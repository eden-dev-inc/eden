use borsh::{BorshDeserialize, BorshSerialize};
use cassandra_core::CassandraAsync;
use endpoint_types::metadata::CapabilityChecker;
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};
use error::ResultEP;
use function_name::named;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

use super::utils::{
    DEFAULT_QUERY_TIMEOUT, get_bool, get_string, get_u64, map_rows, query, query_map, run_named_query, run_optional_named_query,
};
use crate::api::lib::QueryUnpagedInput;

/// Cassandra keyspace information and metrics
///
/// Covers keyspace replication settings, table statistics, storage metrics
/// and performance characteristics.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraKeyspaceInfo {
    /// Total number of keyspaces in the cluster
    pub total_keyspaces: u64,
    /// Number of user-defined keyspaces (excluding system keyspaces)
    pub user_keyspaces: u64,
    /// Number of system keyspaces
    pub system_keyspaces: u64,
    /// Total storage used by all keyspaces (GB)
    pub total_storage_gb: f64,
    /// Average replication factor across user keyspaces
    pub avg_replication_factor: f64,
    /// Replication strategy distribution
    pub replication_strategy_distribution: HashMap<String, u64>,
    /// Detailed keyspace information
    pub keyspaces: Vec<CassandraKeyspaceDetail>,
    /// Storage distribution by keyspace
    pub storage_distribution: Vec<CassandraKeyspaceStorage>,
    /// Performance metrics by keyspace
    pub performance_metrics: Vec<CassandraKeyspacePerformance>,
    /// Consistency level usage statistics
    pub consistency_level_stats: HashMap<String, u64>,
}

/// Detailed information about a specific keyspace
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraKeyspaceDetail {
    /// Keyspace name
    pub keyspace_name: String,
    /// Replication strategy class
    pub replication_strategy: String,
    /// Replication factor (for SimpleStrategy) or data center factors
    pub replication_options: HashMap<String, String>,
    /// Effective replication factor
    pub effective_replication_factor: u64,
    /// Durable writes setting
    pub durable_writes: bool,
    /// Number of tables in this keyspace
    pub table_count: u64,
    /// Number of materialized views
    pub materialized_view_count: u64,
    /// Number of user-defined types
    pub udt_count: u64,
    /// Number of user-defined functions
    pub function_count: u64,
    /// Number of user-defined aggregates
    pub aggregate_count: u64,
    /// Keyspace type (USER, SYSTEM)
    pub keyspace_type: String,
    /// Creation timestamp
    pub created_at: Option<String>,
    /// Last modified timestamp
    pub modified_at: Option<String>,
}

/// Storage metrics for a keyspace
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraKeyspaceStorage {
    /// Keyspace name
    pub keyspace_name: String,
    /// Total size across all replicas (GB)
    pub total_size_gb: f64,
    /// Logical size (before replication) (GB)
    pub logical_size_gb: f64,
    /// Compressed size (GB)
    pub compressed_size_gb: f64,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Number of SSTables across all tables
    pub total_sstables: u64,
    /// Average SSTable size (MB)
    pub avg_sstable_size_mb: f64,
    /// Total number of partitions
    pub total_partitions: u64,
    /// Average partition size (KB)
    pub avg_partition_size_kb: f64,
    /// Bloom filter size (MB)
    pub bloom_filter_size_mb: f64,
    /// Index size (MB)
    pub index_size_mb: f64,
    /// Storage efficiency percentage
    pub storage_efficiency_pct: f64,
}

/// Performance metrics for a keyspace
///
/// All counters default to zero. Cassandra does not expose per-keyspace
/// performance counters via standard CQL system tables; these fields are
/// reserved for future population from JMX or alternative sources.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraKeyspacePerformance {
    /// Keyspace name
    pub keyspace_name: String,
    /// Read requests per second
    pub read_ops_per_sec: f64,
    /// Write requests per second
    pub write_ops_per_sec: f64,
    /// Average read latency (ms)
    pub avg_read_latency_ms: f64,
    /// Average write latency (ms)
    pub avg_write_latency_ms: f64,
    /// 95th percentile read latency (ms)
    pub p95_read_latency_ms: f64,
    /// 95th percentile write latency (ms)
    pub p95_write_latency_ms: f64,
    /// Read timeout count
    pub read_timeouts: u64,
    /// Write timeout count
    pub write_timeouts: u64,
    /// Read failures
    pub read_failures: u64,
    /// Write failures
    pub write_failures: u64,
    /// Cache hit ratio for this keyspace
    pub cache_hit_ratio_pct: f64,
    /// Bloom filter hits
    pub bloom_filter_hits: u64,
    /// Bloom filter false positives
    pub bloom_filter_false_positives: u64,
    /// Compaction activity (tasks per hour)
    pub compaction_activity: f64,
    /// Repair activity (last repair timestamp)
    pub last_repair: Option<String>,
}

/// Table-level information within keyspaces
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableInfo {
    /// Table name
    pub table_name: String,
    /// Keyspace name
    pub keyspace_name: String,
    /// Table ID
    pub table_id: String,
    /// Compaction strategy
    pub compaction_strategy: String,
    /// Compaction strategy options
    pub compaction_options: HashMap<String, String>,
    /// Compression options
    pub compression_options: HashMap<String, String>,
    /// Caching options
    pub caching_options: HashMap<String, String>,
    /// Default time to live (seconds)
    pub default_ttl: Option<u64>,
    /// GC grace seconds
    pub gc_grace_seconds: u64,
    /// Bloom filter FP chance
    pub bloom_filter_fp_chance: f64,
    /// Min index interval
    pub min_index_interval: u64,
    /// Max index interval
    pub max_index_interval: u64,
    /// CRC check chance
    pub crc_check_chance: f64,
    /// Table flags
    pub flags: Vec<String>,
    /// Extensions
    pub extensions: HashMap<String, String>,
}

impl MetadataCollection for CassandraKeyspaceInfo {
    type Request = HashMap<String, QueryUnpagedInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                "keyspaces",
                query(
                    "SELECT keyspace_name, replication, durable_writes
                     FROM system_schema.keyspaces",
                ),
            ),
            (
                "tables",
                query(
                    "SELECT keyspace_name, table_name, id, compaction, compression, caching,
                     default_time_to_live, gc_grace_seconds, bloom_filter_fp_chance,
                     min_index_interval, max_index_interval, crc_check_chance, flags, extensions
                     FROM system_schema.tables",
                ),
            ),
            (
                "views",
                query(
                    "SELECT keyspace_name, view_name, base_table_name, base_table_id
                     FROM system_schema.views",
                ),
            ),
            (
                "types",
                query(
                    "SELECT keyspace_name, type_name, field_names, field_types
                     FROM system_schema.types",
                ),
            ),
            (
                "functions",
                query(
                    "SELECT keyspace_name, function_name, argument_types, argument_names,
                     body, language, return_type, called_on_null_input
                     FROM system_schema.functions",
                ),
            ),
            (
                "aggregates",
                query(
                    "SELECT keyspace_name, aggregate_name, argument_types, final_func,
                     initcond, return_type, state_func, state_type
                     FROM system_schema.aggregates",
                ),
            ),
            (
                "size_estimates",
                query(
                    "SELECT keyspace_name, table_name, range_start, range_end,
                     mean_partition_size, partitions_count
                     FROM system.size_estimates",
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Cassandra keyspace information and metrics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "keyspace"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

impl CassandraKeyspaceInfo {
    const BYTES_TO_KB: f64 = 1024.0;
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

        let mut keyspace_info = CassandraKeyspaceInfo::default();
        let requests = self.request();

        // Execute required queries concurrently. Each of these targets standard
        // system_schema tables present in every Apache Cassandra release.
        let (keyspaces_data, tables_data, views_data, types_data, functions_data, aggregates_data) = tokio::try_join!(
            run_named_query(&requests, "keyspaces", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "tables", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "views", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "types", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "functions", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "aggregates", context.clone(), DEFAULT_QUERY_TIMEOUT),
        )?;

        // system.size_estimates is standard but may be empty on small clusters;
        // treat failure as soft so the rest of the collector still succeeds.
        let size_estimates_data = run_optional_named_query(&requests, "size_estimates", context.clone(), DEFAULT_QUERY_TIMEOUT)
            .await
            .unwrap_or(Value::Array(vec![]));

        // Process keyspace definitions.
        keyspace_info.keyspaces = Self::process_keyspaces(&keyspaces_data)?;

        // Add table and object counts to keyspaces.
        Self::enhance_keyspaces_with_counts(
            &mut keyspace_info.keyspaces,
            &tables_data,
            &views_data,
            &types_data,
            &functions_data,
            &aggregates_data,
        )?;

        // Calculate basic statistics.
        Self::calculate_basic_statistics(&mut keyspace_info)?;

        // Build storage distribution from size_estimates only; the
        // system.sstable_activity table is DSE-specific and not available in
        // standard Apache Cassandra.
        keyspace_info.storage_distribution = Self::build_storage_distribution(&keyspace_info.keyspaces, &size_estimates_data)?;

        // Performance metrics are not available via standard CQL system tables.
        // Populate the vec with honest zero-valued entries so callers have a
        // stable structure to work with, without fabricating data.
        keyspace_info.performance_metrics = Self::build_performance_metrics(&keyspace_info.keyspaces);

        // Build replication strategy distribution.
        Self::build_replication_distribution(&mut keyspace_info)?;

        // Calculate total storage.
        keyspace_info.total_storage_gb = keyspace_info.storage_distribution.iter().map(|s| s.total_size_gb).sum();

        Ok(keyspace_info)
    }

    fn process_keyspaces(keyspaces_data: &Value) -> ResultEP<Vec<CassandraKeyspaceDetail>> {
        let keyspaces = map_rows(keyspaces_data, |row| {
            let keyspace_name = get_string(row, "keyspace_name")?;
            let replication_map = Self::parse_replication_options(row).ok()?;
            let replication_strategy = replication_map.get("class").cloned().unwrap_or_else(|| "Unknown".to_string());
            let effective_rf = Self::calculate_effective_replication_factor(&replication_map);

            Some(CassandraKeyspaceDetail {
                keyspace_type: if Self::is_system_keyspace(&keyspace_name) {
                    "SYSTEM".to_string()
                } else {
                    "USER".to_string()
                },
                keyspace_name,
                replication_strategy: Self::extract_strategy_name(&replication_strategy),
                replication_options: replication_map,
                effective_replication_factor: effective_rf,
                durable_writes: get_bool(row, "durable_writes").unwrap_or(true),
                table_count: 0,
                materialized_view_count: 0,
                udt_count: 0,
                function_count: 0,
                aggregate_count: 0,
                created_at: None,
                modified_at: None,
            })
        });

        Ok(keyspaces)
    }

    fn enhance_keyspaces_with_counts(
        keyspaces: &mut [CassandraKeyspaceDetail],
        tables_data: &Value,
        views_data: &Value,
        types_data: &Value,
        functions_data: &Value,
        aggregates_data: &Value,
    ) -> ResultEP<()> {
        let table_counts = Self::count_by_keyspace(tables_data);
        let view_counts = Self::count_by_keyspace(views_data);
        let type_counts = Self::count_by_keyspace(types_data);
        let function_counts = Self::count_by_keyspace(functions_data);
        let aggregate_counts = Self::count_by_keyspace(aggregates_data);

        for keyspace in keyspaces.iter_mut() {
            let name = &keyspace.keyspace_name;
            keyspace.table_count = table_counts.get(name).copied().unwrap_or(0);
            keyspace.materialized_view_count = view_counts.get(name).copied().unwrap_or(0);
            keyspace.udt_count = type_counts.get(name).copied().unwrap_or(0);
            keyspace.function_count = function_counts.get(name).copied().unwrap_or(0);
            keyspace.aggregate_count = aggregate_counts.get(name).copied().unwrap_or(0);
        }

        Ok(())
    }

    /// Build a `keyspace_name -> count` map from a JSON array of rows.
    fn count_by_keyspace(data: &Value) -> HashMap<String, u64> {
        let mut counts: HashMap<String, u64> = HashMap::new();
        map_rows(data, |row| get_string(row, "keyspace_name")).into_iter().for_each(|ks| {
            *counts.entry(ks).or_insert(0) += 1;
        });
        counts
    }

    fn calculate_basic_statistics(keyspace_info: &mut CassandraKeyspaceInfo) -> ResultEP<()> {
        keyspace_info.total_keyspaces = keyspace_info.keyspaces.len() as u64;

        let (user_count, system_count) = keyspace_info.keyspaces.iter().fold((0u64, 0u64), |(user, system), ks| {
            if ks.keyspace_type == "USER" {
                (user + 1, system)
            } else {
                (user, system + 1)
            }
        });

        keyspace_info.user_keyspaces = user_count;
        keyspace_info.system_keyspaces = system_count;

        let user_rfs: Vec<u64> = keyspace_info
            .keyspaces
            .iter()
            .filter(|ks| ks.keyspace_type == "USER")
            .map(|ks| ks.effective_replication_factor)
            .collect();

        if !user_rfs.is_empty() {
            keyspace_info.avg_replication_factor = user_rfs.iter().sum::<u64>() as f64 / user_rfs.len() as f64;
        }

        Ok(())
    }

    fn build_storage_distribution(
        keyspaces: &[CassandraKeyspaceDetail],
        size_estimates_data: &Value,
    ) -> ResultEP<Vec<CassandraKeyspaceStorage>> {
        let mut storage_map: HashMap<String, CassandraKeyspaceStorage> = keyspaces
            .iter()
            .map(|ks| {
                (
                    ks.keyspace_name.clone(),
                    CassandraKeyspaceStorage {
                        keyspace_name: ks.keyspace_name.clone(),
                        total_size_gb: 0.0,
                        logical_size_gb: 0.0,
                        compressed_size_gb: 0.0,
                        compression_ratio: 1.0,
                        total_sstables: 0,
                        avg_sstable_size_mb: 0.0,
                        total_partitions: 0,
                        avg_partition_size_kb: 0.0,
                        bloom_filter_size_mb: 0.0,
                        index_size_mb: 0.0,
                        storage_efficiency_pct: 100.0,
                    },
                )
            })
            .collect();

        // Process size estimates (standard system.size_estimates table).
        map_rows(size_estimates_data, |row| {
            let keyspace = get_string(row, "keyspace_name")?;
            let partition_count = get_u64(row, "partitions_count");
            let mean_partition_size = get_u64(row, "mean_partition_size");
            Some((keyspace, partition_count, mean_partition_size))
        })
        .into_iter()
        .for_each(|(keyspace, partition_count, mean_partition_size)| {
            if let Some(storage) = storage_map.get_mut(&keyspace) {
                if let Some(count) = partition_count {
                    storage.total_partitions += count;
                }
                if let Some(size) = mean_partition_size {
                    // Not a weighted average; last-write wins across token ranges.
                    storage.avg_partition_size_kb = size as f64 / Self::BYTES_TO_KB;
                }
            }
        });

        Ok(storage_map.into_values().collect())
    }

    /// Build zero-valued performance entries for each user keyspace.
    ///
    /// Cassandra does not expose per-keyspace performance counters through
    /// standard CQL system tables. These fields require JMX or an alternative
    /// metrics source and are intentionally left at their zero defaults until
    /// such a source is integrated.
    fn build_performance_metrics(keyspaces: &[CassandraKeyspaceDetail]) -> Vec<CassandraKeyspacePerformance> {
        keyspaces
            .iter()
            .filter(|ks| ks.keyspace_type == "USER")
            .map(|ks| CassandraKeyspacePerformance {
                keyspace_name: ks.keyspace_name.clone(),
                read_ops_per_sec: 0.0,
                write_ops_per_sec: 0.0,
                avg_read_latency_ms: 0.0,
                avg_write_latency_ms: 0.0,
                p95_read_latency_ms: 0.0,
                p95_write_latency_ms: 0.0,
                read_timeouts: 0,
                write_timeouts: 0,
                read_failures: 0,
                write_failures: 0,
                cache_hit_ratio_pct: 0.0,
                bloom_filter_hits: 0,
                bloom_filter_false_positives: 0,
                compaction_activity: 0.0,
                last_repair: None,
            })
            .collect()
    }

    fn build_replication_distribution(keyspace_info: &mut CassandraKeyspaceInfo) -> ResultEP<()> {
        for keyspace in &keyspace_info.keyspaces {
            if keyspace.keyspace_type == "USER" {
                *keyspace_info.replication_strategy_distribution.entry(keyspace.replication_strategy.clone()).or_insert(0) += 1;
            }
        }
        Ok(())
    }

    fn parse_replication_options(row: &Value) -> ResultEP<HashMap<String, String>> {
        let mut options = HashMap::new();

        let Some(replication_value) = row.get("replication") else {
            return Ok(options);
        };

        match replication_value {
            Value::Object(replication_map) => {
                for (key, value) in replication_map {
                    if let Some(string_value) = value.as_str() {
                        options.insert(key.clone(), string_value.to_string());
                    }
                }
            }
            Value::String(replication_str) => {
                if let Ok(parsed) = Self::parse_replication_string(replication_str) {
                    options = parsed;
                }
            }
            _ => {}
        }

        Ok(options)
    }

    fn parse_replication_string(replication_str: &str) -> ResultEP<HashMap<String, String>> {
        let mut options = HashMap::new();
        let cleaned = replication_str.trim_matches(|c| c == '{' || c == '}');

        for pair in cleaned.split(',') {
            let parts: Vec<&str> = pair.split(':').collect();
            if parts.len() == 2 {
                let key = parts[0].trim().trim_matches('\'').trim_matches('"');
                let value = parts[1].trim().trim_matches('\'').trim_matches('"');
                options.insert(key.to_string(), value.to_string());
            }
        }

        Ok(options)
    }

    fn calculate_effective_replication_factor(replication_options: &HashMap<String, String>) -> u64 {
        let Some(class) = replication_options.get("class") else {
            return 1;
        };

        if class.contains("SimpleStrategy") {
            return replication_options.get("replication_factor").and_then(|rf| rf.parse().ok()).unwrap_or(1);
        }

        if class.contains("NetworkTopologyStrategy") {
            return replication_options
                .iter()
                .filter(|(key, _)| *key != "class")
                .filter_map(|(_, value)| value.parse::<u64>().ok())
                .sum::<u64>()
                .max(1);
        }

        1
    }

    fn extract_strategy_name(full_class_name: &str) -> String {
        full_class_name.split('.').next_back().unwrap_or(full_class_name).to_string()
    }

    fn is_system_keyspace(keyspace_name: &str) -> bool {
        Self::SYSTEM_KEYSPACES.contains(&keyspace_name)
    }
}

impl CassandraKeyspaceInfo {
    /// Gets the keyspace with the highest storage usage
    pub fn keyspace_with_highest_storage(&self) -> Option<&CassandraKeyspaceStorage> {
        self.storage_distribution
            .iter()
            .max_by(|a, b| a.total_size_gb.partial_cmp(&b.total_size_gb).unwrap_or(std::cmp::Ordering::Equal))
    }

    /// Gets the keyspace with the highest performance load
    pub fn keyspace_with_highest_performance_load(&self) -> Option<&CassandraKeyspacePerformance> {
        self.performance_metrics.iter().max_by(|a, b| {
            let load_a = a.read_ops_per_sec + a.write_ops_per_sec;
            let load_b = b.read_ops_per_sec + b.write_ops_per_sec;
            load_a.partial_cmp(&load_b).unwrap_or(std::cmp::Ordering::Equal)
        })
    }

    /// Checks if any keyspace has replication factor below recommended minimum
    pub fn has_low_replication_factor(&self, min_rf: u64) -> bool {
        self.keyspaces.iter().filter(|ks| ks.keyspace_type == "USER").any(|ks| ks.effective_replication_factor < min_rf)
    }

    /// Gets keyspaces with potential storage issues
    pub fn keyspaces_with_storage_issues(&self) -> Vec<&CassandraKeyspaceStorage> {
        self.storage_distribution
            .iter()
            .filter(|storage| {
                storage.compression_ratio < 2.0 || storage.avg_sstable_size_mb > 1000.0 || storage.storage_efficiency_pct < 50.0
            })
            .collect()
    }

    /// Gets keyspaces with performance issues
    pub fn keyspaces_with_performance_issues(&self) -> Vec<&CassandraKeyspacePerformance> {
        self.performance_metrics
            .iter()
            .filter(|perf| {
                perf.avg_read_latency_ms > 10.0
                    || perf.avg_write_latency_ms > 5.0
                    || perf.read_timeouts > 10
                    || perf.write_timeouts > 10
                    || perf.cache_hit_ratio_pct < 70.0
            })
            .collect()
    }

    /// Calculates average compression ratio across all keyspaces
    pub fn average_compression_ratio(&self) -> f64 {
        let ratios: Vec<f64> =
            self.storage_distribution.iter().filter(|s| s.compression_ratio > 0.0).map(|s| s.compression_ratio).collect();

        if ratios.is_empty() {
            1.0
        } else {
            ratios.iter().sum::<f64>() / ratios.len() as f64
        }
    }

    /// Gets the most common replication strategy
    pub fn primary_replication_strategy(&self) -> Option<String> {
        self.replication_strategy_distribution.iter().max_by_key(|&(_, &count)| count).map(|(strategy, _)| strategy.clone())
    }

    /// Checks if cluster uses consistent replication strategies
    pub fn has_consistent_replication_strategy(&self) -> bool {
        self.replication_strategy_distribution.len() <= 1
    }

    /// Gets total number of tables across all keyspaces
    pub fn total_tables(&self) -> u64 {
        self.keyspaces.iter().map(|ks| ks.table_count).sum()
    }

    /// Gets total number of user-defined objects (UDTs, functions, aggregates)
    pub fn total_user_objects(&self) -> u64 {
        self.keyspaces.iter().map(|ks| ks.udt_count + ks.function_count + ks.aggregate_count).sum()
    }

    /// Calculates storage overhead due to replication
    pub fn replication_overhead_gb(&self) -> f64 {
        self.storage_distribution
            .iter()
            .map(|storage| {
                let logical_size = storage.logical_size_gb;
                let total_size = storage.total_size_gb;
                if logical_size > 0.0 { total_size - logical_size } else { 0.0 }
            })
            .sum()
    }

    /// Gets keyspaces sorted by storage usage
    pub fn keyspaces_by_storage(&self) -> Vec<&CassandraKeyspaceStorage> {
        let mut sorted = self.storage_distribution.iter().collect::<Vec<_>>();
        sorted.sort_by(|a, b| b.total_size_gb.partial_cmp(&a.total_size_gb).unwrap_or(std::cmp::Ordering::Equal));
        sorted
    }

    /// Gets keyspaces sorted by performance load
    pub fn keyspaces_by_performance(&self) -> Vec<&CassandraKeyspacePerformance> {
        let mut sorted = self.performance_metrics.iter().collect::<Vec<_>>();
        sorted.sort_by(|a, b| {
            let load_a = a.read_ops_per_sec + a.write_ops_per_sec;
            let load_b = b.read_ops_per_sec + b.write_ops_per_sec;
            load_b.partial_cmp(&load_a).unwrap_or(std::cmp::Ordering::Equal)
        });
        sorted
    }

    /// Checks if any keyspace needs attention based on multiple criteria
    pub fn needs_attention(&self) -> bool {
        !self.keyspaces_with_storage_issues().is_empty()
            || !self.keyspaces_with_performance_issues().is_empty()
            || self.has_low_replication_factor(2)
            || !self.has_consistent_replication_strategy()
    }

    /// Gets summary statistics
    pub fn get_summary_stats(&self) -> CassandraKeyspaceSummary {
        CassandraKeyspaceSummary {
            total_keyspaces: self.total_keyspaces,
            user_keyspaces: self.user_keyspaces,
            total_tables: self.total_tables(),
            total_storage_gb: self.total_storage_gb,
            avg_replication_factor: self.avg_replication_factor,
            avg_compression_ratio: self.average_compression_ratio(),
            primary_replication_strategy: self.primary_replication_strategy(),
            keyspaces_with_issues: self.keyspaces_with_storage_issues().len() as u64
                + self.keyspaces_with_performance_issues().len() as u64,
        }
    }
}

/// Summary statistics for keyspace information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraKeyspaceSummary {
    pub total_keyspaces: u64,
    pub user_keyspaces: u64,
    pub total_tables: u64,
    pub total_storage_gb: f64,
    pub avg_replication_factor: f64,
    pub avg_compression_ratio: f64,
    pub primary_replication_strategy: Option<String>,
    pub keyspaces_with_issues: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_replication_factor_calculation() {
        // Test SimpleStrategy
        let mut simple_options = HashMap::new();
        simple_options.insert("class".to_string(), "org.apache.cassandra.locator.SimpleStrategy".to_string());
        simple_options.insert("replication_factor".to_string(), "3".to_string());

        assert_eq!(CassandraKeyspaceInfo::calculate_effective_replication_factor(&simple_options), 3);

        // Test NetworkTopologyStrategy
        let mut network_options = HashMap::new();
        network_options.insert("class".to_string(), "org.apache.cassandra.locator.NetworkTopologyStrategy".to_string());
        network_options.insert("datacenter1".to_string(), "3".to_string());
        network_options.insert("datacenter2".to_string(), "2".to_string());

        assert_eq!(CassandraKeyspaceInfo::calculate_effective_replication_factor(&network_options), 5);

        // Test unknown strategy
        let mut unknown_options = HashMap::new();
        unknown_options.insert("class".to_string(), "UnknownStrategy".to_string());

        assert_eq!(CassandraKeyspaceInfo::calculate_effective_replication_factor(&unknown_options), 1);
    }

    #[test]
    fn test_strategy_name_extraction() {
        assert_eq!(
            CassandraKeyspaceInfo::extract_strategy_name("org.apache.cassandra.locator.SimpleStrategy"),
            "SimpleStrategy"
        );
        assert_eq!(CassandraKeyspaceInfo::extract_strategy_name("SimpleStrategy"), "SimpleStrategy");
    }

    #[test]
    fn test_system_keyspace_detection() {
        assert!(CassandraKeyspaceInfo::is_system_keyspace("system"));
        assert!(CassandraKeyspaceInfo::is_system_keyspace("system_schema"));
        assert!(CassandraKeyspaceInfo::is_system_keyspace("system_auth"));
        assert!(!CassandraKeyspaceInfo::is_system_keyspace("my_keyspace"));
        assert!(!CassandraKeyspaceInfo::is_system_keyspace("user_data"));
    }

    #[test]
    fn test_replication_string_parsing() {
        let replication_str = "{'class': 'SimpleStrategy', 'replication_factor': '3'}";
        let parsed = CassandraKeyspaceInfo::parse_replication_string(replication_str).unwrap();

        assert_eq!(parsed.get("class"), Some(&"SimpleStrategy".to_string()));
        assert_eq!(parsed.get("replication_factor"), Some(&"3".to_string()));

        let network_str = "{'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '2'}";
        let parsed_network = CassandraKeyspaceInfo::parse_replication_string(network_str).unwrap();

        assert_eq!(parsed_network.get("class"), Some(&"NetworkTopologyStrategy".to_string()));
        assert_eq!(parsed_network.get("dc1"), Some(&"3".to_string()));
        assert_eq!(parsed_network.get("dc2"), Some(&"2".to_string()));
    }

    #[test]
    fn test_process_keyspaces() {
        let keyspaces_data = json!([
            {
                "keyspace_name": "user_ks",
                "replication": {
                    "class": "SimpleStrategy",
                    "replication_factor": "3"
                },
                "durable_writes": true
            },
            {
                "keyspace_name": "system",
                "replication": {
                    "class": "SimpleStrategy",
                    "replication_factor": "1"
                },
                "durable_writes": true
            }
        ]);

        let keyspaces = CassandraKeyspaceInfo::process_keyspaces(&keyspaces_data).unwrap();

        assert_eq!(keyspaces.len(), 2);

        let user_ks = keyspaces.iter().find(|k| k.keyspace_name == "user_ks").unwrap();
        assert_eq!(user_ks.replication_strategy, "SimpleStrategy");
        assert_eq!(user_ks.effective_replication_factor, 3);
        assert_eq!(user_ks.keyspace_type, "USER");
        assert!(user_ks.durable_writes);

        let system_ks = keyspaces.iter().find(|k| k.keyspace_name == "system").unwrap();
        assert_eq!(system_ks.keyspace_type, "SYSTEM");
        assert_eq!(system_ks.effective_replication_factor, 1);
    }

    #[test]
    fn test_storage_issue_detection() {
        let keyspace_info = CassandraKeyspaceInfo {
            storage_distribution: vec![
                CassandraKeyspaceStorage {
                    keyspace_name: "good_ks".to_string(),
                    total_size_gb: 100.0,
                    logical_size_gb: 80.0,
                    compressed_size_gb: 40.0,
                    compression_ratio: 2.5,
                    total_sstables: 100,
                    avg_sstable_size_mb: 500.0,
                    total_partitions: 1000000,
                    avg_partition_size_kb: 50.0,
                    bloom_filter_size_mb: 10.0,
                    index_size_mb: 5.0,
                    storage_efficiency_pct: 80.0,
                },
                CassandraKeyspaceStorage {
                    keyspace_name: "problematic_ks".to_string(),
                    total_size_gb: 200.0,
                    logical_size_gb: 150.0,
                    compressed_size_gb: 140.0,
                    compression_ratio: 1.1, // Poor compression
                    total_sstables: 50,
                    avg_sstable_size_mb: 1500.0, // Large SSTables
                    total_partitions: 500000,
                    avg_partition_size_kb: 200.0,
                    bloom_filter_size_mb: 20.0,
                    index_size_mb: 15.0,
                    storage_efficiency_pct: 30.0, // Poor efficiency
                },
            ],
            ..Default::default()
        };

        let issues = keyspace_info.keyspaces_with_storage_issues();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].keyspace_name, "problematic_ks");
    }

    #[test]
    fn test_performance_issue_detection() {
        let keyspace_info = CassandraKeyspaceInfo {
            performance_metrics: vec![
                CassandraKeyspacePerformance {
                    keyspace_name: "fast_ks".to_string(),
                    read_ops_per_sec: 100.0,
                    write_ops_per_sec: 150.0,
                    avg_read_latency_ms: 2.0,
                    avg_write_latency_ms: 1.5,
                    p95_read_latency_ms: 8.0,
                    p95_write_latency_ms: 5.0,
                    read_timeouts: 1,
                    write_timeouts: 0,
                    read_failures: 0,
                    write_failures: 0,
                    cache_hit_ratio_pct: 90.0,
                    bloom_filter_hits: 1000,
                    bloom_filter_false_positives: 5,
                    compaction_activity: 2.0,
                    last_repair: None,
                },
                CassandraKeyspacePerformance {
                    keyspace_name: "slow_ks".to_string(),
                    read_ops_per_sec: 50.0,
                    write_ops_per_sec: 75.0,
                    avg_read_latency_ms: 15.0, // High latency
                    avg_write_latency_ms: 8.0, // High latency
                    p95_read_latency_ms: 50.0,
                    p95_write_latency_ms: 30.0,
                    read_timeouts: 20, // High timeouts
                    write_timeouts: 15,
                    read_failures: 5,
                    write_failures: 3,
                    cache_hit_ratio_pct: 60.0, // Low cache hit ratio
                    bloom_filter_hits: 500,
                    bloom_filter_false_positives: 50,
                    compaction_activity: 5.0,
                    last_repair: None,
                },
            ],
            ..Default::default()
        };

        let issues = keyspace_info.keyspaces_with_performance_issues();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].keyspace_name, "slow_ks");
    }

    #[test]
    fn test_replication_consistency() {
        let mut keyspace_info = CassandraKeyspaceInfo::default();

        // Test consistent strategies
        keyspace_info.replication_strategy_distribution.insert("SimpleStrategy".to_string(), 5);
        assert!(keyspace_info.has_consistent_replication_strategy());
        assert_eq!(keyspace_info.primary_replication_strategy(), Some("SimpleStrategy".to_string()));

        // Test inconsistent strategies
        keyspace_info.replication_strategy_distribution.insert("NetworkTopologyStrategy".to_string(), 3);
        assert!(!keyspace_info.has_consistent_replication_strategy());
        assert_eq!(keyspace_info.primary_replication_strategy(), Some("SimpleStrategy".to_string()));
    }

    #[test]
    fn test_low_replication_factor_detection() {
        let keyspace_info = CassandraKeyspaceInfo {
            keyspaces: vec![
                CassandraKeyspaceDetail {
                    keyspace_name: "good_ks".to_string(),
                    replication_strategy: "SimpleStrategy".to_string(),
                    replication_options: HashMap::new(),
                    effective_replication_factor: 3,
                    durable_writes: true,
                    table_count: 5,
                    materialized_view_count: 0,
                    udt_count: 0,
                    function_count: 0,
                    aggregate_count: 0,
                    keyspace_type: "USER".to_string(),
                    created_at: None,
                    modified_at: None,
                },
                CassandraKeyspaceDetail {
                    keyspace_name: "risky_ks".to_string(),
                    replication_strategy: "SimpleStrategy".to_string(),
                    replication_options: HashMap::new(),
                    effective_replication_factor: 1, // Low RF
                    durable_writes: true,
                    table_count: 3,
                    materialized_view_count: 0,
                    udt_count: 0,
                    function_count: 0,
                    aggregate_count: 0,
                    keyspace_type: "USER".to_string(),
                    created_at: None,
                    modified_at: None,
                },
            ],
            ..Default::default()
        };

        assert!(keyspace_info.has_low_replication_factor(2));
        assert!(!keyspace_info.has_low_replication_factor(1));
    }

    #[test]
    fn test_build_performance_metrics_zero_defaults() {
        let keyspaces = vec![
            CassandraKeyspaceDetail {
                keyspace_name: "app_ks".to_string(),
                replication_strategy: "SimpleStrategy".to_string(),
                replication_options: HashMap::new(),
                effective_replication_factor: 3,
                durable_writes: true,
                table_count: 2,
                materialized_view_count: 0,
                udt_count: 0,
                function_count: 0,
                aggregate_count: 0,
                keyspace_type: "USER".to_string(),
                created_at: None,
                modified_at: None,
            },
            CassandraKeyspaceDetail {
                keyspace_name: "system".to_string(),
                replication_strategy: "SimpleStrategy".to_string(),
                replication_options: HashMap::new(),
                effective_replication_factor: 1,
                durable_writes: true,
                table_count: 10,
                materialized_view_count: 0,
                udt_count: 0,
                function_count: 0,
                aggregate_count: 0,
                keyspace_type: "SYSTEM".to_string(),
                created_at: None,
                modified_at: None,
            },
        ];

        let metrics = CassandraKeyspaceInfo::build_performance_metrics(&keyspaces);

        // Only user keyspaces get an entry.
        assert_eq!(metrics.len(), 1);
        let m = &metrics[0];
        assert_eq!(m.keyspace_name, "app_ks");
        assert_eq!(m.read_ops_per_sec, 0.0);
        assert_eq!(m.write_ops_per_sec, 0.0);
        assert_eq!(m.read_timeouts, 0);
        assert_eq!(m.bloom_filter_hits, 0);
    }

    #[test]
    fn test_count_by_keyspace() {
        let data = json!([
            {"keyspace_name": "ks1", "table_name": "t1"},
            {"keyspace_name": "ks1", "table_name": "t2"},
            {"keyspace_name": "ks2", "table_name": "t3"},
            {"other_field": "no keyspace_name"},
        ]);

        let counts = CassandraKeyspaceInfo::count_by_keyspace(&data);
        assert_eq!(counts.get("ks1"), Some(&2));
        assert_eq!(counts.get("ks2"), Some(&1));
        assert_eq!(counts.len(), 2);
    }
}
