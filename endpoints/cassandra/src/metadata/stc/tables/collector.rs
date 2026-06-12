use super::types::{
    CassandraTableColumn, CassandraTableColumnInfo, CassandraTableConfiguration, CassandraTableDetail, CassandraTableHealthIndicators,
    CassandraTableHealthMetrics, CassandraTableIndex, CassandraTableInfo, CassandraTableMaintenanceInfo, CassandraTableMaintenanceMetrics,
    CassandraTablePerformanceDetail, CassandraTablePerformanceMetrics, CassandraTableStorageDistribution, CassandraTableStorageMetrics,
};
use super::utils::{DEFAULT_QUERY_TIMEOUT, get_f64, get_string, get_u64, run_named_query, run_optional_named_query};
use crate::api::lib::QueryUnpagedInput;
use cassandra_core::CassandraAsync;
use chrono::{DateTime, Utc};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection};
use error::ResultEP;
use function_name::named;
use serde_json::Value;
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

impl CassandraTableInfo {
    const BYTES_TO_GB: f64 = 1024.0 * 1024.0 * 1024.0;
    const BYTES_TO_KB: f64 = 1024.0;
    const LARGE_PARTITION_THRESHOLD_MB: f64 = 100.0;
    // Threshold reserved for future health-check reporting
    #[allow(dead_code)]
    const WIDE_PARTITION_THRESHOLD: u64 = 100_000; // Number of cells
    const HIGH_TOMBSTONE_THRESHOLD_PCT: f64 = 20.0;
    const POOR_COMPRESSION_THRESHOLD: f64 = 1.5;
    const HIGH_LATENCY_THRESHOLD_MS: f64 = 10.0;
    const SYSTEM_KEYSPACES: &'static [&'static str] = &[
        "system",
        "system_schema",
        "system_auth",
        "system_distributed",
        "system_traces",
        "system_views",
        "system_virtual_schema",
    ];

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: CassandraAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut table_info = CassandraTableInfo::default();
        let requests = self.request();

        // Execute required queries concurrently; each failure is fatal.
        let (tables_data, columns_data, indexes_data, views_data, size_estimates_data) = tokio::try_join!(
            run_named_query(&requests, "tables", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "columns", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "indexes", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "views", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "size_estimates", context.clone(), DEFAULT_QUERY_TIMEOUT),
        )?;

        // Optional queries: non-standard tables. Failure returns None rather than aborting.
        let compaction_history_data =
            run_optional_named_query(&requests, "compaction_history", context.clone(), DEFAULT_QUERY_TIMEOUT).await;

        // Build detailed table information
        table_info.table_details = Self::build_table_details(
            &tables_data,
            &columns_data,
            &indexes_data,
            &views_data,
            &size_estimates_data,
            compaction_history_data.as_ref(),
        )?;

        Self::calculate_basic_statistics(&mut table_info)?;
        table_info.storage_distribution = Self::build_storage_distribution(&table_info.table_details)?;
        table_info.performance_metrics = Self::build_performance_metrics(&table_info.table_details)?;
        table_info.health_metrics = Self::build_health_metrics(&table_info.table_details)?;
        table_info.maintenance_metrics = Self::build_maintenance_metrics(&table_info.table_details)?;

        Ok(table_info)
    }

    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    fn build_table_details(
        tables_data: &Value,
        columns_data: &Value,
        indexes_data: &Value,
        views_data: &Value,
        size_estimates_data: &Value,
        compaction_history_data: Option<&Value>,
    ) -> ResultEP<Vec<CassandraTableDetail>> {
        let columns_map = Self::build_columns_map(columns_data)?;
        let indexes_map = Self::build_indexes_map(indexes_data)?;
        let views_map = Self::build_views_map(views_data)?;
        let size_estimates_map = Self::build_size_estimates_map(size_estimates_data)?;
        let compaction_map = match compaction_history_data {
            Some(data) => Self::build_compaction_map(data)?,
            None => HashMap::new(),
        };

        let Value::Array(table_rows) = tables_data else {
            return Ok(Vec::new());
        };

        let mut table_details = Vec::with_capacity(table_rows.len());

        for row in table_rows {
            let keyspace_name = get_string(row, "keyspace_name").unwrap_or_default();
            let table_name = get_string(row, "table_name").unwrap_or_default();
            let table_key = format!("{}.{}", keyspace_name, table_name);

            let columns = columns_map.get(&table_key).cloned().unwrap_or_default();
            let column_info = Self::build_column_info(&columns);
            let storage_metrics = Self::build_table_storage_metrics(&table_key, &size_estimates_map);
            let performance_metrics = CassandraTablePerformanceDetail::default();
            let configuration = Self::build_table_configuration(row);
            let indexes = indexes_map.get(&table_key).cloned().unwrap_or_default();
            let materialized_views = views_map.get(&table_key).cloned().unwrap_or_default();
            let health_indicators = Self::build_health_indicators(&column_info, &storage_metrics, &configuration);
            let maintenance_info = Self::build_maintenance_info(&table_key, &compaction_map);

            let table_type = if Self::is_system_keyspace(&keyspace_name) {
                "SYSTEM".to_string()
            } else {
                "USER".to_string()
            };

            table_details.push(CassandraTableDetail {
                keyspace_name,
                table_name,
                table_id: get_string(row, "id").unwrap_or_default(),
                table_type,
                column_info,
                storage_metrics,
                performance_metrics,
                configuration,
                indexes,
                materialized_views,
                health_indicators,
                maintenance_info,
                created_at: None,    // Not available from system_schema.tables
                last_modified: None, // Not available from system_schema.tables
            });
        }

        Ok(table_details)
    }

    fn build_column_info(columns: &[CassandraTableColumn]) -> CassandraTableColumnInfo {
        let mut partition_key_columns = Vec::new();
        let mut clustering_key_columns = Vec::new();
        let mut static_columns = Vec::new();
        let mut regular_columns = Vec::new();
        let mut collection_columns = 0u64;
        let mut udt_columns = 0u64;

        for column in columns {
            match column.kind.as_str() {
                "partition_key" => partition_key_columns.push(column.clone()),
                "clustering" => clustering_key_columns.push(column.clone()),
                "static" => static_columns.push(column.clone()),
                _ => regular_columns.push(column.clone()),
            }
            if column.is_collection {
                collection_columns += 1;
            }
            if column.is_udt {
                udt_columns += 1;
            }
        }

        partition_key_columns.sort_by_key(|c| c.position.unwrap_or(0));
        clustering_key_columns.sort_by_key(|c| c.position.unwrap_or(0));

        let partition_key_complexity = Self::calculate_key_complexity(&partition_key_columns);
        let clustering_key_complexity = Self::calculate_key_complexity(&clustering_key_columns);

        CassandraTableColumnInfo {
            total_columns: columns.len() as u64,
            partition_key_columns,
            clustering_key_columns,
            static_columns,
            regular_columns,
            collection_columns,
            udt_columns,
            partition_key_complexity,
            clustering_key_complexity,
        }
    }

    fn build_table_storage_metrics(
        table_key: &str,
        size_estimates_map: &HashMap<String, (u64, f64)>, // (partitions, avg_size_kb)
    ) -> CassandraTableStorageMetrics {
        let mut metrics = CassandraTableStorageMetrics::default();

        if let Some((partitions, avg_partition_size_kb)) = size_estimates_map.get(table_key) {
            metrics.estimated_partitions = *partitions;
            metrics.avg_partition_size_kb = *avg_partition_size_kb;

            // Estimate logical size from partition count and average size
            let logical_size_bytes = *partitions as f64 * (*avg_partition_size_kb * Self::BYTES_TO_KB);
            metrics.logical_size_gb = logical_size_bytes / Self::BYTES_TO_GB;
            metrics.total_size_gb = metrics.logical_size_gb;
        }

        // growth_rate_gb_per_day remains 0.0: historical data not available here

        metrics
    }

    fn build_table_configuration(row: &Value) -> CassandraTableConfiguration {
        CassandraTableConfiguration {
            compaction_strategy: Self::extract_compaction_strategy(row),
            compaction_options: Self::extract_compaction_options(row),
            compression_algorithm: Self::extract_compression_algorithm(row),
            compression_options: Self::extract_compression_options(row),
            caching_config: Self::extract_caching_config(row),
            bloom_filter_fp_chance: get_f64(row, "bloom_filter_fp_chance").unwrap_or(0.01),
            default_ttl: get_u64(row, "default_time_to_live"),
            gc_grace_seconds: get_u64(row, "gc_grace_seconds").unwrap_or(864_000),
            min_index_interval: get_u64(row, "min_index_interval").unwrap_or(128),
            max_index_interval: get_u64(row, "max_index_interval").unwrap_or(2048),
            crc_check_chance: get_f64(row, "crc_check_chance").unwrap_or(1.0),
            comment: get_string(row, "comment"),
        }
    }

    fn build_health_indicators(
        column_info: &CassandraTableColumnInfo,
        storage_metrics: &CassandraTableStorageMetrics,
        _configuration: &CassandraTableConfiguration,
    ) -> CassandraTableHealthIndicators {
        let has_design_issues =
            column_info.total_columns > 50 || column_info.partition_key_complexity > 3.0 || column_info.clustering_key_complexity > 5.0;

        // Performance issues cannot be detected without JMX data.
        let has_performance_issues = false;

        let has_storage_issues = storage_metrics.compression_ratio > 0.0
            && storage_metrics.compression_ratio < Self::POOR_COMPRESSION_THRESHOLD
            || storage_metrics.avg_sstable_size_mb > 1000.0;

        let large_partitions_count = if storage_metrics.avg_partition_size_kb > Self::LARGE_PARTITION_THRESHOLD_MB * 1024.0 {
            1
        } else {
            0
        };

        let poor_compaction_efficiency = storage_metrics.sstable_count > 20;
        let suboptimal_compression =
            storage_metrics.compression_ratio > 0.0 && storage_metrics.compression_ratio < Self::POOR_COMPRESSION_THRESHOLD;

        let mut score: f64 = 100.0;
        if has_design_issues {
            score -= 20.0;
        }
        if has_storage_issues {
            score -= 15.0;
        }
        if poor_compaction_efficiency {
            score -= 10.0;
        }
        if suboptimal_compression {
            score -= 10.0;
        }

        CassandraTableHealthIndicators {
            health_score: score.max(0.0),
            has_design_issues,
            has_performance_issues,
            has_storage_issues,
            large_partitions_count,
            poor_compaction_efficiency,
            suboptimal_compression,
            // high_tombstone_ratio cannot be determined without JMX
            high_tombstone_ratio: false,
            ..Default::default()
        }
    }

    fn build_maintenance_info(table_key: &str, compaction_map: &HashMap<String, String>) -> CassandraTableMaintenanceInfo {
        let last_compaction = compaction_map.get(table_key).cloned();

        let days_since_major_compaction = match &last_compaction {
            Some(ts) => Self::days_since_timestamp(ts),
            None => f64::INFINITY,
        };

        // Determine if maintenance is needed based on compaction age only.
        // pending_compactions and active_compactions require JMX and are left at 0.
        let needs_maintenance = days_since_major_compaction > 14.0;

        CassandraTableMaintenanceInfo {
            last_compaction,
            last_repair: None,   // Repair history not available from standard CQL tables
            last_snapshot: None, // snapshot table is non-standard; removed from queries
            pending_compactions: 0,
            active_compactions: 0,
            compaction_efficiency: 0.0,
            days_since_major_compaction,
            needs_maintenance,
        }
    }

    fn calculate_basic_statistics(table_info: &mut CassandraTableInfo) -> ResultEP<()> {
        table_info.total_tables = table_info.table_details.len() as u64;

        let (user_count, system_count) = table_info.table_details.iter().fold((0u64, 0u64), |(user, system), table| {
            if table.table_type == "USER" {
                (user + 1, system)
            } else {
                (user, system + 1)
            }
        });

        table_info.user_tables = user_count;
        table_info.system_tables = system_count;

        if table_info.table_details.is_empty() {
            return Ok(());
        }

        let sizes: Vec<f64> = table_info.table_details.iter().map(|t| t.storage_metrics.total_size_gb).collect();

        table_info.total_storage_gb = sizes.iter().sum();
        table_info.avg_table_size_gb = table_info.total_storage_gb / sizes.len() as f64;
        table_info.largest_table_size_gb = sizes.iter().fold(0.0_f64, |max, &size| max.max(size));

        table_info.empty_tables = table_info.table_details.iter().filter(|t| t.storage_metrics.total_size_gb == 0.0).count() as u64;

        table_info.tables_with_issues = table_info
            .table_details
            .iter()
            .filter(|t| {
                t.health_indicators.has_design_issues
                    || t.health_indicators.has_performance_issues
                    || t.health_indicators.has_storage_issues
            })
            .count() as u64;

        let column_counts: Vec<u64> = table_info.table_details.iter().map(|t| t.column_info.total_columns).collect();
        table_info.avg_columns_per_table = column_counts.iter().sum::<u64>() as f64 / column_counts.len() as f64;

        let partition_sizes: Vec<f64> =
            table_info.table_details.iter().map(|t| t.storage_metrics.avg_partition_size_kb).filter(|&size| size > 0.0).collect();
        if !partition_sizes.is_empty() {
            table_info.avg_partition_size_kb = partition_sizes.iter().sum::<f64>() / partition_sizes.len() as f64;
        }

        table_info.total_sstables = table_info.table_details.iter().map(|t| t.storage_metrics.sstable_count).sum();

        Ok(())
    }

    fn build_storage_distribution(table_details: &[CassandraTableDetail]) -> ResultEP<CassandraTableStorageDistribution> {
        let mut distribution = CassandraTableStorageDistribution::default();

        for table in table_details {
            *distribution.storage_by_keyspace.entry(table.keyspace_name.clone()).or_insert(0.0) += table.storage_metrics.total_size_gb;

            *distribution.storage_by_compaction_strategy.entry(table.configuration.compaction_strategy.clone()).or_insert(0.0) +=
                table.storage_metrics.total_size_gb;

            *distribution.storage_by_compression.entry(table.configuration.compression_algorithm.clone()).or_insert(0.0) +=
                table.storage_metrics.total_size_gb;

            let range = match table.storage_metrics.total_size_gb {
                0.0 => "Empty",
                s if s <= 1.0 => "0-1 GB",
                s if s <= 10.0 => "1-10 GB",
                s if s <= 100.0 => "10-100 GB",
                _ => "100+ GB",
            };
            *distribution.storage_by_size_ranges.entry(range.to_string()).or_insert(0) += 1;
        }

        let mut sorted_tables: Vec<&CassandraTableDetail> = table_details.iter().collect();
        sorted_tables.sort_by(|a, b| {
            b.storage_metrics.total_size_gb.partial_cmp(&a.storage_metrics.total_size_gb).unwrap_or(std::cmp::Ordering::Equal)
        });
        distribution.largest_tables = sorted_tables.iter().take(10).map(|t| format!("{}.{}", t.keyspace_name, t.table_name)).collect();

        // fastest_growing_tables cannot be computed without historical data; leave empty.
        distribution.fastest_growing_tables = Vec::new();

        Ok(distribution)
    }

    fn build_performance_metrics(_table_details: &[CassandraTableDetail]) -> ResultEP<CassandraTablePerformanceMetrics> {
        // All JMX-sourced metrics are unavailable; return honest defaults.
        Ok(CassandraTablePerformanceMetrics::default())
    }

    fn build_health_metrics(table_details: &[CassandraTableDetail]) -> ResultEP<CassandraTableHealthMetrics> {
        let mut metrics = CassandraTableHealthMetrics::default();

        for table in table_details {
            if table.health_indicators.has_design_issues {
                metrics.tables_with_design_issues += 1;
            }
            if table.health_indicators.has_performance_issues {
                metrics.tables_with_performance_issues += 1;
            }
            if table.health_indicators.has_storage_issues {
                metrics.tables_with_storage_issues += 1;
            }
            if table.health_indicators.wide_partitions_count > 0 {
                metrics.tables_with_wide_partitions += 1;
            }
            if table.health_indicators.high_tombstone_ratio {
                metrics.tables_with_high_tombstones += 1;
            }
            if table.health_indicators.suboptimal_compression {
                metrics.tables_with_poor_compression += 1;
            }
            if table.health_indicators.missing_indexes {
                metrics.tables_missing_indexes += 1;
            }
            if table.health_indicators.unused_indexes {
                metrics.tables_with_unused_indexes += 1;
            }
        }

        if table_details.is_empty() {
            metrics.overall_health_score = 100.0;
        } else {
            let health_scores: Vec<f64> = table_details.iter().map(|t| t.health_indicators.health_score).collect();
            metrics.overall_health_score = health_scores.iter().sum::<f64>() / health_scores.len() as f64;
        }

        Ok(metrics)
    }

    fn build_maintenance_metrics(table_details: &[CassandraTableDetail]) -> ResultEP<CassandraTableMaintenanceMetrics> {
        let mut metrics = CassandraTableMaintenanceMetrics::default();

        for table in table_details {
            if table.maintenance_info.needs_maintenance {
                metrics.tables_needing_maintenance += 1;
            }

            // Compaction overdue: last compaction was more than 7 days ago and is known.
            if table.maintenance_info.days_since_major_compaction > 7.0
                && table.maintenance_info.days_since_major_compaction != f64::INFINITY
            {
                metrics.tables_overdue_compaction += 1;
            }

            if table.maintenance_info.last_snapshot.is_none() {
                metrics.tables_without_snapshots += 1;
            }
        }

        if !table_details.is_empty() {
            let valid_days: Vec<f64> = table_details
                .iter()
                .map(|t| t.maintenance_info.days_since_major_compaction)
                .filter(|&days| days != f64::INFINITY)
                .collect();

            if !valid_days.is_empty() {
                metrics.avg_days_since_maintenance = valid_days.iter().sum::<f64>() / valid_days.len() as f64;
            }
        }

        Ok(metrics)
    }

    // Lookup-map builders

    fn build_columns_map(columns_data: &Value) -> ResultEP<HashMap<String, Vec<CassandraTableColumn>>> {
        let mut columns_map: HashMap<String, Vec<CassandraTableColumn>> = HashMap::new();

        let Value::Array(column_rows) = columns_data else {
            return Ok(columns_map);
        };

        for row in column_rows {
            let keyspace_name = get_string(row, "keyspace_name").unwrap_or_default();
            let table_name = get_string(row, "table_name").unwrap_or_default();
            let table_key = format!("{}.{}", keyspace_name, table_name);

            let column_type = get_string(row, "type").unwrap_or_default();
            let column = CassandraTableColumn {
                name: get_string(row, "column_name").unwrap_or_default(),
                data_type: column_type.clone(),
                kind: get_string(row, "kind").unwrap_or_default(),
                position: get_u64(row, "position"),
                clustering_order: get_string(row, "clustering_order"),
                is_collection: Self::is_collection_type(&column_type),
                is_udt: Self::is_udt_type(&column_type),
            };

            columns_map.entry(table_key).or_default().push(column);
        }

        Ok(columns_map)
    }

    fn build_indexes_map(indexes_data: &Value) -> ResultEP<HashMap<String, Vec<CassandraTableIndex>>> {
        let mut indexes_map: HashMap<String, Vec<CassandraTableIndex>> = HashMap::new();

        let Value::Array(index_rows) = indexes_data else {
            return Ok(indexes_map);
        };

        for row in index_rows {
            let keyspace_name = get_string(row, "keyspace_name").unwrap_or_default();
            let table_name = get_string(row, "table_name").unwrap_or_default();
            let table_key = format!("{}.{}", keyspace_name, table_name);

            let index = CassandraTableIndex {
                index_name: get_string(row, "index_name").unwrap_or_default(),
                index_type: get_string(row, "kind").unwrap_or_else(|| "SECONDARY".to_string()),
                target_column: String::new(), // Not available; requires parsing options blob
                options: Self::extract_index_options(row),
                is_ready: true,         // Default assumption
                estimated_size_mb: 0.0, // Not available from system_schema.indexes
            };

            indexes_map.entry(table_key).or_default().push(index);
        }

        Ok(indexes_map)
    }

    fn build_views_map(views_data: &Value) -> ResultEP<HashMap<String, Vec<String>>> {
        let mut views_map: HashMap<String, Vec<String>> = HashMap::new();

        let Value::Array(view_rows) = views_data else {
            return Ok(views_map);
        };

        for row in view_rows {
            let keyspace_name = get_string(row, "keyspace_name").unwrap_or_default();
            let base_table_name = get_string(row, "base_table_name").unwrap_or_default();
            let view_name = get_string(row, "view_name").unwrap_or_default();
            let table_key = format!("{}.{}", keyspace_name, base_table_name);
            views_map.entry(table_key).or_default().push(view_name);
        }

        Ok(views_map)
    }

    fn build_size_estimates_map(size_estimates_data: &Value) -> ResultEP<HashMap<String, (u64, f64)>> {
        let mut size_map: HashMap<String, (u64, f64)> = HashMap::new();

        let Value::Array(size_rows) = size_estimates_data else {
            return Ok(size_map);
        };

        for row in size_rows {
            let keyspace_name = get_string(row, "keyspace_name").unwrap_or_default();
            let table_name = get_string(row, "table_name").unwrap_or_default();
            let table_key = format!("{}.{}", keyspace_name, table_name);

            let partitions = get_u64(row, "partitions_count").unwrap_or(0);
            let mean_size_kb = get_u64(row, "mean_partition_size").unwrap_or(0) as f64 / Self::BYTES_TO_KB;

            size_map
                .entry(table_key)
                .and_modify(|(existing_partitions, existing_size)| {
                    // Multiple token ranges per table: accumulate partitions, average size.
                    *existing_size = (*existing_size + mean_size_kb) / 2.0;
                    *existing_partitions += partitions;
                })
                .or_insert((partitions, mean_size_kb));
        }

        Ok(size_map)
    }

    fn build_compaction_map(compaction_data: &Value) -> ResultEP<HashMap<String, String>> {
        let mut compaction_map: HashMap<String, String> = HashMap::new();

        let Value::Array(compaction_rows) = compaction_data else {
            return Ok(compaction_map);
        };

        for row in compaction_rows {
            let keyspace_name = get_string(row, "keyspace_name").unwrap_or_default();
            let table_name = get_string(row, "columnfamily_name").unwrap_or_default();
            let table_key = format!("{}.{}", keyspace_name, table_name);
            let compacted_at = get_string(row, "compacted_at").unwrap_or_default();

            // Keep the most recent compaction time per table.
            let should_update = match compaction_map.get(&table_key) {
                None => true,
                Some(existing) => Self::is_timestamp_more_recent(&compacted_at, existing),
            };

            if should_update && !compacted_at.is_empty() {
                compaction_map.insert(table_key, compacted_at);
            }
        }

        Ok(compaction_map)
    }

    // Timestamp helpers

    fn parse_timestamp(timestamp_str: &str) -> Option<DateTime<Utc>> {
        if timestamp_str.is_empty() {
            return None;
        }

        // ISO 8601 with timezone offset
        if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp_str) {
            return Some(dt.with_timezone(&Utc));
        }
        // ISO 8601 UTC
        if let Ok(dt) = timestamp_str.parse::<DateTime<Utc>>() {
            return Some(dt);
        }
        // Cassandra timestamp (microseconds since epoch)
        if let Ok(micros) = timestamp_str.parse::<i64>() {
            return DateTime::from_timestamp(micros / 1_000_000, ((micros % 1_000_000) * 1000) as u32);
        }

        None
    }

    fn is_timestamp_more_recent(ts1: &str, ts2: &str) -> bool {
        match (Self::parse_timestamp(ts1), Self::parse_timestamp(ts2)) {
            (Some(a), Some(b)) => a > b,
            (Some(_), None) => true,
            _ => false,
        }
    }

    fn days_since_timestamp(timestamp: &str) -> f64 {
        match Self::parse_timestamp(timestamp) {
            Some(ts) => {
                let duration = Utc::now().signed_duration_since(ts);
                duration.num_days() as f64
            }
            None => f64::INFINITY,
        }
    }

    // Configuration extraction helpers

    fn extract_compaction_strategy(row: &Value) -> String {
        if let Some(obj) = row.get("compaction").and_then(|v| v.as_object())
            && let Some(class_str) = obj.get("class").and_then(|v| v.as_str())
        {
            return class_str.split('.').next_back().unwrap_or(class_str).to_string();
        }
        "SizeTieredCompactionStrategy".to_string()
    }

    fn extract_compaction_options(row: &Value) -> HashMap<String, String> {
        let mut options = HashMap::new();
        if let Some(obj) = row.get("compaction").and_then(|v| v.as_object()) {
            for (key, value) in obj {
                if key != "class"
                    && let Some(s) = value.as_str()
                {
                    options.insert(key.clone(), s.to_string());
                }
            }
        }
        options
    }

    fn extract_compression_algorithm(row: &Value) -> String {
        if let Some(obj) = row.get("compression").and_then(|v| v.as_object()) {
            if let Some(algorithm) = obj.get("algorithm").and_then(|v| v.as_str()) {
                return algorithm.to_string();
            }
            // Cassandra 4.x uses 'class' key for the compressor
            if let Some(class_str) = obj.get("class").and_then(|v| v.as_str()) {
                return class_str.split('.').next_back().unwrap_or(class_str).to_string();
            }
        }
        "LZ4Compressor".to_string()
    }

    fn extract_compression_options(row: &Value) -> HashMap<String, String> {
        let mut options = HashMap::new();
        if let Some(obj) = row.get("compression").and_then(|v| v.as_object()) {
            for (key, value) in obj {
                if let Some(s) = value.as_str() {
                    options.insert(key.clone(), s.to_string());
                }
            }
        }
        options
    }

    fn extract_caching_config(row: &Value) -> HashMap<String, String> {
        let mut config = HashMap::new();
        if let Some(obj) = row.get("caching").and_then(|v| v.as_object()) {
            for (key, value) in obj {
                if let Some(s) = value.as_str() {
                    config.insert(key.clone(), s.to_string());
                }
            }
        }
        config
    }

    fn extract_index_options(row: &Value) -> HashMap<String, String> {
        let mut options = HashMap::new();
        if let Some(obj) = row.get("options").and_then(|v| v.as_object()) {
            for (key, value) in obj {
                if let Some(s) = value.as_str() {
                    options.insert(key.clone(), s.to_string());
                }
            }
        }
        options
    }

    // Type-detection helpers

    pub(crate) fn is_collection_type(column_type: &str) -> bool {
        column_type.contains("map<") || column_type.contains("set<") || column_type.contains("list<")
    }

    pub(crate) fn is_udt_type(column_type: &str) -> bool {
        !Self::is_primitive_type(column_type) && !Self::is_collection_type(column_type)
    }

    pub(crate) fn is_primitive_type(column_type: &str) -> bool {
        matches!(
            column_type,
            "text"
                | "varchar"
                | "ascii"
                | "int"
                | "bigint"
                | "smallint"
                | "tinyint"
                | "varint"
                | "float"
                | "double"
                | "decimal"
                | "boolean"
                | "uuid"
                | "timeuuid"
                | "timestamp"
                | "date"
                | "time"
                | "inet"
                | "blob"
                | "counter"
        )
    }

    pub(crate) fn calculate_key_complexity(columns: &[CassandraTableColumn]) -> f64 {
        let mut complexity = columns.len() as f64;
        for column in columns {
            if column.is_collection {
                complexity += 2.0;
            }
            if column.is_udt {
                complexity += 1.5;
            }
        }
        complexity
    }

    pub(crate) fn is_system_keyspace(keyspace_name: &str) -> bool {
        Self::SYSTEM_KEYSPACES.contains(&keyspace_name)
    }
}

impl CassandraTableInfo {
    // Public analytics / utility methods

    /// Checks if there are critical table issues requiring immediate attention.
    pub fn has_critical_table_issues(&self) -> bool {
        self.health_metrics.overall_health_score < 60.0
            || self.tables_with_issues > (self.total_tables / 2)
            || self.maintenance_metrics.tables_needing_maintenance > (self.total_tables / 3)
    }

    /// Gets tables that need immediate attention.
    pub fn tables_needing_attention(&self) -> Vec<&CassandraTableDetail> {
        self.table_details
            .iter()
            .filter(|table| {
                table.health_indicators.health_score < 50.0
                    || table.maintenance_info.needs_maintenance
                    || table.storage_metrics.avg_partition_size_kb > Self::LARGE_PARTITION_THRESHOLD_MB * 1024.0
                    || table.performance_metrics.tombstone_ratio_pct > Self::HIGH_TOMBSTONE_THRESHOLD_PCT
            })
            .collect()
    }

    /// Gets tables with poor performance.
    pub fn tables_with_poor_performance(&self) -> Vec<&CassandraTableDetail> {
        self.table_details
            .iter()
            .filter(|table| {
                table.performance_metrics.avg_read_latency_ms > Self::HIGH_LATENCY_THRESHOLD_MS
                    || table.performance_metrics.avg_write_latency_ms > Self::HIGH_LATENCY_THRESHOLD_MS
                    || table.performance_metrics.cache_hit_ratio_pct < 60.0
                    || table.performance_metrics.has_hot_partitions
            })
            .collect()
    }

    /// Gets tables with storage optimization opportunities.
    pub fn tables_with_storage_issues(&self) -> Vec<&CassandraTableDetail> {
        self.table_details
            .iter()
            .filter(|table| {
                (table.storage_metrics.compression_ratio > 0.0
                    && table.storage_metrics.compression_ratio < Self::POOR_COMPRESSION_THRESHOLD)
                    || table.storage_metrics.sstable_count > 50
                    || table.health_indicators.large_partitions_count > 0
            })
            .collect()
    }

    /// Gets the largest tables by storage.
    pub fn largest_tables(&self, limit: usize) -> Vec<&CassandraTableDetail> {
        let mut tables: Vec<&CassandraTableDetail> = self.table_details.iter().collect();
        tables.sort_by(|a, b| {
            b.storage_metrics.total_size_gb.partial_cmp(&a.storage_metrics.total_size_gb).unwrap_or(std::cmp::Ordering::Equal)
        });
        tables.into_iter().take(limit).collect()
    }

    /// Gets tables with the most complex schemas.
    pub fn most_complex_tables(&self, limit: usize) -> Vec<&CassandraTableDetail> {
        let mut tables: Vec<&CassandraTableDetail> = self.table_details.iter().collect();
        tables.sort_by(|a, b| {
            let ca = a.column_info.total_columns as f64 + a.column_info.partition_key_complexity + a.column_info.clustering_key_complexity;
            let cb = b.column_info.total_columns as f64 + b.column_info.partition_key_complexity + b.column_info.clustering_key_complexity;
            cb.partial_cmp(&ca).unwrap_or(std::cmp::Ordering::Equal)
        });
        tables.into_iter().take(limit).collect()
    }

    /// Gets tables that are growing fastest.
    ///
    /// Note: growth rate is always 0.0 because historical data is not available
    /// from the standard CQL system tables queried by this collector.
    pub fn fastest_growing_tables(&self, limit: usize) -> Vec<&CassandraTableDetail> {
        let mut tables: Vec<&CassandraTableDetail> = self.table_details.iter().collect();
        tables.sort_by(|a, b| {
            b.storage_metrics
                .growth_rate_gb_per_day
                .partial_cmp(&a.storage_metrics.growth_rate_gb_per_day)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        tables.into_iter().take(limit).collect()
    }

    /// Gets overall table health rating (A–F scale).
    pub fn table_health_rating(&self) -> String {
        match self.health_metrics.overall_health_score {
            s if s >= 90.0 => "A",
            s if s >= 80.0 => "B",
            s if s >= 70.0 => "C",
            s if s >= 60.0 => "D",
            _ => "F",
        }
        .to_string()
    }

    /// Calculates storage efficiency score across all tables.
    pub fn storage_efficiency_score(&self) -> f64 {
        if self.table_details.is_empty() {
            return 100.0;
        }

        let compression_scores: Vec<f64> = self.table_details.iter().map(|t| (t.storage_metrics.compression_ratio - 1.0) * 20.0).collect();

        let avg_compression_score = compression_scores.iter().sum::<f64>() / compression_scores.len() as f64;

        let sstable_efficiency = if self.total_tables > 0 {
            100.0 - (self.total_sstables as f64 / self.total_tables as f64).min(20.0) * 3.0
        } else {
            100.0
        };

        (avg_compression_score + sstable_efficiency) / 2.0
    }

    /// Gets recommended table optimization actions.
    pub fn get_table_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.has_critical_table_issues() {
            recommendations.push("CRITICAL: Address table health issues immediately".to_string());
        }
        if self.health_metrics.tables_with_design_issues > 0 {
            recommendations.push(format!(
                "{} tables have design issues - review schema patterns",
                self.health_metrics.tables_with_design_issues
            ));
        }
        if self.health_metrics.tables_with_performance_issues > 0 {
            recommendations.push(format!(
                "{} tables have performance issues - optimize queries and indexes",
                self.health_metrics.tables_with_performance_issues
            ));
        }
        if self.health_metrics.tables_with_high_tombstones > 0 {
            recommendations.push(format!(
                "{} tables have high tombstone ratios - review deletion patterns",
                self.health_metrics.tables_with_high_tombstones
            ));
        }
        if self.health_metrics.tables_with_poor_compression > 0 {
            recommendations.push(format!(
                "{} tables have poor compression - consider compression tuning",
                self.health_metrics.tables_with_poor_compression
            ));
        }
        if self.maintenance_metrics.tables_needing_maintenance > 0 {
            recommendations.push(format!(
                "{} tables need maintenance - schedule compaction and repairs",
                self.maintenance_metrics.tables_needing_maintenance
            ));
        }
        if self.empty_tables > 5 {
            recommendations.push(format!("{} empty tables detected - consider cleanup", self.empty_tables));
        }
        if self.performance_metrics.avg_performance_score < 70.0 && self.performance_metrics.avg_performance_score > 0.0 {
            recommendations.push("Overall table performance is suboptimal - review workload patterns".to_string());
        }
        if self.health_metrics.tables_missing_indexes > 0 {
            recommendations.push("Some tables may benefit from additional indexes".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("Table configuration appears optimal - continue monitoring".to_string());
        }

        recommendations
    }

    /// Gets table distribution statistics.
    pub fn get_table_distribution_stats(&self) -> super::types::CassandraTableDistributionStats {
        super::types::CassandraTableDistributionStats {
            tables_by_keyspace: self.get_tables_by_keyspace(),
            tables_by_size_ranges: self.storage_distribution.storage_by_size_ranges.clone(),
            tables_by_column_count: self.get_tables_by_column_count(),
            tables_by_compaction_strategy: self.get_tables_by_compaction_strategy(),
            tables_by_health_score: self.get_tables_by_health_score(),
        }
    }

    fn get_tables_by_keyspace(&self) -> HashMap<String, u64> {
        let mut counts: HashMap<String, u64> = HashMap::new();
        for table in &self.table_details {
            *counts.entry(table.keyspace_name.clone()).or_insert(0) += 1;
        }
        counts
    }

    fn get_tables_by_column_count(&self) -> HashMap<String, u64> {
        let mut ranges = HashMap::from([
            ("1-10".to_string(), 0u64),
            ("11-25".to_string(), 0),
            ("26-50".to_string(), 0),
            ("51+".to_string(), 0),
        ]);

        for table in &self.table_details {
            let range_key = match table.column_info.total_columns {
                1..=10 => "1-10",
                11..=25 => "11-25",
                26..=50 => "26-50",
                _ => "51+",
            };
            if let Some(count) = ranges.get_mut(range_key) {
                *count += 1;
            }
        }

        ranges
    }

    fn get_tables_by_compaction_strategy(&self) -> HashMap<String, u64> {
        let mut counts: HashMap<String, u64> = HashMap::new();
        for table in &self.table_details {
            *counts.entry(table.configuration.compaction_strategy.clone()).or_insert(0) += 1;
        }
        counts
    }

    fn get_tables_by_health_score(&self) -> HashMap<String, u64> {
        let mut health_ranges = HashMap::from([
            ("90-100".to_string(), 0u64),
            ("70-89".to_string(), 0),
            ("50-69".to_string(), 0),
            ("0-49".to_string(), 0),
        ]);

        for table in &self.table_details {
            let range_key = match table.health_indicators.health_score {
                s if s >= 90.0 => "90-100",
                s if s >= 70.0 => "70-89",
                s if s >= 50.0 => "50-69",
                _ => "0-49",
            };
            if let Some(count) = health_ranges.get_mut(range_key) {
                *count += 1;
            }
        }

        health_ranges
    }

    /// Gets summary for reporting.
    pub fn get_table_summary(&self) -> super::types::CassandraTableSummary {
        super::types::CassandraTableSummary {
            total_tables: self.total_tables,
            user_tables: self.user_tables,
            total_storage_gb: self.total_storage_gb,
            avg_table_size_gb: self.avg_table_size_gb,
            tables_with_issues: self.tables_with_issues,
            health_score: self.health_metrics.overall_health_score,
            health_rating: self.table_health_rating(),
            performance_score: self.performance_metrics.avg_performance_score,
            storage_efficiency_score: self.storage_efficiency_score(),
            tables_needing_maintenance: self.maintenance_metrics.tables_needing_maintenance,
            has_critical_issues: self.has_critical_table_issues(),
        }
    }
}

// Queries needed by MetadataCollection::request(). mod.rs delegates here.

pub(super) fn build_request() -> HashMap<String, QueryUnpagedInput> {
    use super::utils::query;
    HashMap::from([
        (
            "tables".to_string(),
            query(
                "SELECT keyspace_name, table_name, id, bloom_filter_fp_chance, caching, comment,
                 compaction, compression, crc_check_chance, default_time_to_live, extensions,
                 flags, gc_grace_seconds, max_index_interval, min_index_interval
                 FROM system_schema.tables",
            ),
        ),
        (
            "columns".to_string(),
            query(
                "SELECT keyspace_name, table_name, column_name, clustering_order,
                 column_name_bytes, kind, position, type
                 FROM system_schema.columns",
            ),
        ),
        (
            "indexes".to_string(),
            query(
                "SELECT keyspace_name, table_name, index_name, kind, options
                 FROM system_schema.indexes",
            ),
        ),
        (
            "views".to_string(),
            query(
                "SELECT keyspace_name, view_name, base_table_name, base_table_id
                 FROM system_schema.views",
            ),
        ),
        (
            "size_estimates".to_string(),
            query(
                "SELECT keyspace_name, table_name, range_start, range_end,
                 mean_partition_size, partitions_count
                 FROM system.size_estimates",
            ),
        ),
        (
            "compaction_history".to_string(),
            query(
                "SELECT keyspace_name, columnfamily_name, compacted_at, bytes_in, bytes_out,
                 rows_merged
                 FROM system.compaction_history",
            ),
        ),
    ])
}
