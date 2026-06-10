use crate::api::lib::QueryUnpagedInput;
use cassandra_core::CassandraAsync;
use endpoint_types::metadata::CapabilityChecker;
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};
use error::ResultEP;
use function_name::named;
use serde_json::Value;
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

use super::utils::{
    DEFAULT_QUERY_TIMEOUT, get_bool_or_false, get_f64, get_string, get_string_or, get_u64, map_rows, row_count, run_named_query,
    run_optional_named_query,
};

pub mod types;
pub use types::*;

// Constants

const EXCESSIVE_COLUMNS_THRESHOLD: u64 = 50;
const WIDE_ROW_CLUSTERING_THRESHOLD: u64 = 10;

// MetadataCollection impl

impl MetadataCollection for CassandraSchemaInfo {
    type Request = HashMap<String, QueryUnpagedInput>;

    fn request(&self) -> Self::Request {
        use super::utils::query;
        HashMap::from([
            ("peers_schema".to_string(), query("SELECT peer, schema_version FROM system.peers")),
            (
                "local_schema".to_string(),
                query("SELECT broadcast_address AS peer, schema_version FROM system.local"),
            ),
            (
                "keyspaces".to_string(),
                query(
                    "SELECT keyspace_name, replication, durable_writes \
                     FROM system_schema.keyspaces",
                ),
            ),
            (
                "tables".to_string(),
                query(
                    "SELECT keyspace_name, table_name, id, bloom_filter_fp_chance, caching, \
                     comment, compaction, compression, crc_check_chance, \
                     default_time_to_live, extensions, flags, gc_grace_seconds, \
                     max_index_interval, min_index_interval \
                     FROM system_schema.tables",
                ),
            ),
            (
                "columns".to_string(),
                query(
                    "SELECT keyspace_name, table_name, column_name, clustering_order, \
                     column_name_bytes, kind, position, type \
                     FROM system_schema.columns",
                ),
            ),
            (
                "indexes".to_string(),
                query(
                    "SELECT keyspace_name, table_name, index_name, kind, options \
                     FROM system_schema.indexes",
                ),
            ),
            (
                "views".to_string(),
                query(
                    "SELECT keyspace_name, view_name, base_table_name, base_table_id, \
                     include_all_columns \
                     FROM system_schema.views",
                ),
            ),
            (
                "types".to_string(),
                query(
                    "SELECT keyspace_name, type_name, field_names, field_types \
                     FROM system_schema.types",
                ),
            ),
            (
                "functions".to_string(),
                query(
                    "SELECT keyspace_name, function_name, argument_types, argument_names, \
                     body, called_on_null_input, language, return_type \
                     FROM system_schema.functions",
                ),
            ),
            (
                "aggregates".to_string(),
                query(
                    "SELECT keyspace_name, aggregate_name, argument_types, final_func, \
                     initcond, return_type, state_func, state_type \
                     FROM system_schema.aggregates",
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Cassandra schema information and analysis"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "schema"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Low // Schema changes less frequently
    }
}

// sync_metadata

impl CassandraSchemaInfo {
    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: CassandraAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut schema_info = CassandraSchemaInfo::default();
        let requests = self.request();

        // Execute critical queries concurrently.
        // peers_schema and local_schema are fetched separately then merged
        // because CQL does not support UNION ALL.
        let (peers_schema_data, local_schema_data, keyspaces_data, tables_data, columns_data, indexes_data, views_data) = tokio::try_join!(
            run_named_query(&requests, "peers_schema", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "local_schema", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "keyspaces", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "tables", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "columns", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "indexes", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "views", context.clone(), DEFAULT_QUERY_TIMEOUT),
        )?;

        // Non-critical queries: missing UDF/aggregate support is acceptable.
        let types_data = run_optional_named_query(&requests, "types", context.clone(), DEFAULT_QUERY_TIMEOUT)
            .await
            .unwrap_or(Value::Array(vec![]));

        let functions_data = run_optional_named_query(&requests, "functions", context.clone(), DEFAULT_QUERY_TIMEOUT)
            .await
            .unwrap_or(Value::Array(vec![]));

        let aggregates_data = run_optional_named_query(&requests, "aggregates", context.clone(), DEFAULT_QUERY_TIMEOUT)
            .await
            .unwrap_or(Value::Array(vec![]));

        // Merge peers + local into a single list for schema-version counting.
        let all_schema_versions = merge_peer_and_local_rows(&peers_schema_data, &local_schema_data);

        process_schema_versions(&mut schema_info, &all_schema_versions);

        process_basic_counts(
            &mut schema_info,
            &keyspaces_data,
            &tables_data,
            &indexes_data,
            &views_data,
            &types_data,
            &functions_data,
            &aggregates_data,
        );

        schema_info.table_details = build_table_details(&tables_data, &columns_data, &indexes_data, &views_data);

        schema_info.index_details = build_index_details(&indexes_data);

        schema_info.type_details = build_type_details(&types_data, &columns_data);

        schema_info.function_details = build_function_details(&functions_data, &aggregates_data);

        schema_info.complexity_metrics = calculate_complexity_metrics(&schema_info.table_details, &schema_info.index_details);

        schema_info.evolution_metrics = calculate_evolution_metrics(&schema_info);

        schema_info.health_metrics = calculate_health_metrics(&schema_info);

        Ok(schema_info)
    }
}

// Row merging (replaces invalid UNION ALL)

/// Append rows from `local_data` onto `peers_data`, returning a combined array.
///
/// Both values are expected to be `Value::Array`.  Non-array values are treated
/// as empty.
fn merge_peer_and_local_rows(peers_data: &Value, local_data: &Value) -> Value {
    let mut rows = match peers_data {
        Value::Array(v) => v.clone(),
        _ => vec![],
    };
    if let Value::Array(local_rows) = local_data {
        rows.extend(local_rows.iter().cloned());
    }
    Value::Array(rows)
}

// Processing helpers

fn process_schema_versions(schema_info: &mut CassandraSchemaInfo, versions_data: &Value) {
    let Value::Array(rows) = versions_data else {
        return;
    };
    for row in rows {
        if let Some(version) = get_string(row, "schema_version") {
            *schema_info.schema_versions.entry(version).or_insert(0) += 1;
        }
    }

    schema_info.schema_agreement = schema_info.schema_versions.len() <= 1;

    if !schema_info.schema_agreement {
        let total_nodes: u64 = schema_info.schema_versions.values().sum();
        let max_version_count = schema_info.schema_versions.values().max().copied().unwrap_or(0);
        schema_info.nodes_with_disagreement = total_nodes - max_version_count;
    }

    schema_info.primary_schema_version =
        schema_info.schema_versions.iter().max_by_key(|&(_, &count)| count).map(|(version, _)| version.clone());
}

// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
fn process_basic_counts(
    schema_info: &mut CassandraSchemaInfo,
    keyspaces_data: &Value,
    tables_data: &Value,
    indexes_data: &Value,
    views_data: &Value,
    types_data: &Value,
    functions_data: &Value,
    aggregates_data: &Value,
) {
    schema_info.total_keyspaces = row_count(keyspaces_data) as u64;
    schema_info.total_tables = row_count(tables_data) as u64;
    schema_info.total_indexes = row_count(indexes_data) as u64;
    schema_info.total_materialized_views = row_count(views_data) as u64;
    schema_info.total_user_types = row_count(types_data) as u64;
    schema_info.total_functions = row_count(functions_data) as u64;
    schema_info.total_aggregates = row_count(aggregates_data) as u64;
}

// Detail builders

fn build_table_details(tables_data: &Value, columns_data: &Value, indexes_data: &Value, views_data: &Value) -> Vec<CassandraTableDetail> {
    let column_map = build_column_map(columns_data);
    let index_count_map = build_index_count_map(indexes_data);
    let view_map = build_view_map(views_data);

    map_rows(tables_data, |row| {
        let keyspace_name = get_string_or(row, "keyspace_name", "");
        let table_name = get_string_or(row, "table_name", "");
        let table_key = format!("{keyspace_name}.{table_name}");

        let columns = column_map.get(&table_key).cloned().unwrap_or_default();
        let (partition_key, clustering_key, static_columns) = extract_key_columns(&columns);

        Some(CassandraTableDetail {
            keyspace_name,
            table_name,
            table_id: get_string_or(row, "id", ""),
            columns,
            partition_key,
            clustering_key,
            static_columns,
            compaction_strategy: extract_compaction_strategy(row),
            compression_algorithm: extract_compression_algorithm(row),
            caching_config: extract_caching_config(row),
            bloom_filter_fp_chance: get_f64(row, "bloom_filter_fp_chance").unwrap_or(0.01),
            default_ttl: get_u64(row, "default_time_to_live"),
            gc_grace_seconds: get_u64(row, "gc_grace_seconds").unwrap_or(864_000),
            comment: get_string(row, "comment"),
            flags: extract_flags(row),
            index_count: index_count_map.get(&table_key).copied().unwrap_or(0),
            has_materialized_views: view_map.contains_key(&table_key),
            schema_version: None,
        })
    })
}

fn build_index_details(indexes_data: &Value) -> Vec<CassandraIndexDetail> {
    map_rows(indexes_data, |row| {
        Some(CassandraIndexDetail {
            keyspace_name: get_string_or(row, "keyspace_name", ""),
            table_name: get_string_or(row, "table_name", ""),
            index_name: get_string_or(row, "index_name", ""),
            index_type: get_string(row, "kind").unwrap_or_else(|| "SECONDARY".to_string()),
            target_column: String::new(), // Parsed from options when needed
            options: extract_index_options(row),
            custom_class: None, // Parsed from options when needed
            is_ready: true,     // Default assumption
            created_at: None,   // Not available in system tables
        })
    })
}

fn build_type_details(types_data: &Value, columns_data: &Value) -> Vec<CassandraTypeDetail> {
    let type_usage_map = build_type_usage_map(columns_data);

    map_rows(types_data, |row| {
        let keyspace_name = get_string_or(row, "keyspace_name", "");
        let type_name = get_string_or(row, "type_name", "");
        let type_key = format!("{keyspace_name}.{type_name}");

        let field_names = extract_string_list(row, "field_names");
        let field_types = extract_string_list(row, "field_types");
        let field_count = field_names.len() as u64;

        Some(CassandraTypeDetail {
            keyspace_name,
            type_name,
            field_names,
            field_types,
            field_count,
            used_by_tables: type_usage_map.get(&type_key).cloned().unwrap_or_default(),
            is_frozen_type: false,
        })
    })
}

fn build_function_details(functions_data: &Value, aggregates_data: &Value) -> Vec<CassandraFunctionDetail> {
    let mut details: Vec<CassandraFunctionDetail> = map_rows(functions_data, |row| {
        Some(CassandraFunctionDetail {
            keyspace_name: get_string_or(row, "keyspace_name", ""),
            function_name: get_string_or(row, "function_name", ""),
            function_type: "FUNCTION".to_string(),
            argument_types: extract_string_list(row, "argument_types"),
            return_type: get_string_or(row, "return_type", ""),
            language: get_string(row, "language"),
            body: get_string(row, "body"),
            called_on_null_input: get_bool_or_false(row, "called_on_null_input"),
            state_function: None,
            final_function: None,
            initial_condition: None,
        })
    });

    let aggregates: Vec<CassandraFunctionDetail> = map_rows(aggregates_data, |row| {
        Some(CassandraFunctionDetail {
            keyspace_name: get_string_or(row, "keyspace_name", ""),
            function_name: get_string_or(row, "aggregate_name", ""),
            function_type: "AGGREGATE".to_string(),
            argument_types: extract_string_list(row, "argument_types"),
            return_type: get_string_or(row, "return_type", ""),
            language: None,
            body: None,
            called_on_null_input: false,
            state_function: get_string(row, "state_func"),
            final_function: get_string(row, "final_func"),
            initial_condition: get_string(row, "initcond"),
        })
    });

    details.extend(aggregates);
    details
}

// Metrics calculators

fn calculate_complexity_metrics(
    table_details: &[CassandraTableDetail],
    index_details: &[CassandraIndexDetail],
) -> CassandraSchemaComplexityMetrics {
    let mut metrics = CassandraSchemaComplexityMetrics::default();

    if table_details.is_empty() {
        return metrics;
    }

    let column_counts: Vec<u64> = table_details.iter().map(|t| t.columns.len() as u64).collect();

    metrics.avg_columns_per_table = column_counts.iter().sum::<u64>() as f64 / column_counts.len() as f64;
    metrics.max_columns_per_table = column_counts.iter().max().copied().unwrap_or(0);
    metrics.tables_with_excessive_columns = column_counts.iter().filter(|&&c| c > EXCESSIVE_COLUMNS_THRESHOLD).count() as u64;

    let pk_sizes: Vec<f64> = table_details.iter().map(|t| t.partition_key.len() as f64).collect();
    let ck_sizes: Vec<f64> = table_details.iter().map(|t| t.clustering_key.len() as f64).collect();

    metrics.avg_partition_key_complexity = pk_sizes.iter().sum::<f64>() / pk_sizes.len() as f64;
    metrics.avg_clustering_key_complexity = ck_sizes.iter().sum::<f64>() / ck_sizes.len() as f64;

    let total_tables = table_details.len() as f64;

    metrics.tables_with_composite_keys =
        table_details.iter().filter(|t| t.partition_key.len() > 1 || t.clustering_key.len() > 1).count() as u64;

    metrics.tables_with_static_columns = table_details.iter().filter(|t| !t.static_columns.is_empty()).count() as u64;

    metrics.tables_with_collections =
        table_details.iter().filter(|t| t.columns.iter().any(|col| is_collection_type(&col.column_type))).count() as u64;

    metrics.avg_indexes_per_table = index_details.len() as f64 / total_tables;

    metrics.tables_with_multiple_indexes = table_details.iter().filter(|t| t.index_count > 1).count() as u64;

    metrics.complexity_score = calculate_complexity_score(&metrics, table_details);

    metrics
}

fn calculate_evolution_metrics(schema_info: &CassandraSchemaInfo) -> CassandraSchemaEvolutionMetrics {
    // CQL system tables do not expose schema change history or timestamps for
    // individual DDL events.  All fields that would require such data are left
    // at their zero/None defaults rather than emitting fabricated numbers.
    let mut metrics = CassandraSchemaEvolutionMetrics::default();

    // Record the first listed table as a reference point when available.
    if let Some(t) = schema_info.table_details.first() {
        metrics.newest_table = Some(format!("{}.{}", t.keyspace_name, t.table_name));
        metrics.recently_modified_table = metrics.newest_table.clone();
    }

    // version_drift_days: non-zero only when nodes disagree on schema version.
    if !schema_info.schema_agreement {
        metrics.version_drift_days = 1.0;
    }

    // stability_score: 100 when all nodes agree, 0 when they disagree.
    metrics.stability_score = if schema_info.schema_agreement { 100.0 } else { 0.0 };

    metrics
}

fn calculate_health_metrics(schema_info: &CassandraSchemaInfo) -> CassandraSchemaHealthMetrics {
    let tables = &schema_info.table_details;
    let total_tables = tables.len() as f64;

    let tables_with_good_naming = tables.iter().filter(|t| has_good_naming(&t.table_name)).count() as u64;

    let tables_with_good_partition_design = tables.iter().filter(|t| has_good_partition_design(t)).count() as u64;

    let tables_with_wide_row_risk = tables.iter().filter(|t| t.clustering_key.len() as u64 > WIDE_ROW_CLUSTERING_THRESHOLD).count() as u64;

    let tables_missing_useful_indexes = tables.iter().filter(|t| t.index_count == 0 && t.columns.len() > 5).count() as u64;

    let tables_with_unused_indexes = tables.iter().filter(|t| t.index_count > 3).count() as u64;

    let tables_with_anti_patterns = tables.iter().filter(|t| has_anti_patterns(t)).count() as u64;

    let (health_score, best_practices_compliance) = if total_tables > 0.0 {
        let good_naming_pct = (tables_with_good_naming as f64 / total_tables) * 100.0;
        let good_design_pct = (tables_with_good_partition_design as f64 / total_tables) * 100.0;
        let low_risk_pct = ((total_tables - tables_with_wide_row_risk as f64) / total_tables) * 100.0;
        let low_anti_patterns_pct = ((total_tables - tables_with_anti_patterns as f64) / total_tables) * 100.0;
        let score = (good_naming_pct + good_design_pct + low_risk_pct + low_anti_patterns_pct) / 4.0;
        (score, score)
    } else {
        (100.0, 100.0)
    };

    // security_issues_count: flag when no keyspace name contains "auth".
    // This is a coarse heuristic only; zero does not imply security is configured.
    let security_issues_count = if schema_info.total_keyspaces > 0 && !tables.iter().any(|t| t.keyspace_name.contains("auth")) {
        1
    } else {
        0
    };

    CassandraSchemaHealthMetrics {
        tables_with_good_naming,
        tables_with_good_partition_design,
        tables_with_wide_row_risk,
        tables_missing_useful_indexes,
        tables_with_unused_indexes,
        tables_with_anti_patterns,
        health_score,
        best_practices_compliance,
        deprecated_features_count: 0,
        security_issues_count,
    }
}

// Map builders

fn build_column_map(columns_data: &Value) -> HashMap<String, Vec<CassandraColumnDetail>> {
    let mut column_map: HashMap<String, Vec<CassandraColumnDetail>> = HashMap::new();

    let Value::Array(rows) = columns_data else {
        return column_map;
    };

    for row in rows {
        let keyspace_name = get_string_or(row, "keyspace_name", "");
        let table_name = get_string_or(row, "table_name", "");
        let table_key = format!("{keyspace_name}.{table_name}");

        let column_detail = CassandraColumnDetail {
            column_name: get_string_or(row, "column_name", ""),
            column_type: get_string_or(row, "type", ""),
            column_kind: get_string_or(row, "kind", ""),
            position: get_u64(row, "position"),
            clustering_order: get_string(row, "clustering_order"),
            is_frozen: false,
            type_arguments: vec![],
        };

        column_map.entry(table_key).or_default().push(column_detail);
    }

    column_map
}

fn build_index_count_map(indexes_data: &Value) -> HashMap<String, u64> {
    let mut index_count_map: HashMap<String, u64> = HashMap::new();

    let Value::Array(rows) = indexes_data else {
        return index_count_map;
    };

    for row in rows {
        let keyspace_name = get_string_or(row, "keyspace_name", "");
        let table_name = get_string_or(row, "table_name", "");
        let table_key = format!("{keyspace_name}.{table_name}");
        *index_count_map.entry(table_key).or_insert(0) += 1;
    }

    index_count_map
}

fn build_view_map(views_data: &Value) -> HashMap<String, Vec<String>> {
    let mut view_map: HashMap<String, Vec<String>> = HashMap::new();

    let Value::Array(rows) = views_data else {
        return view_map;
    };

    for row in rows {
        let keyspace_name = get_string_or(row, "keyspace_name", "");
        let base_table_name = get_string_or(row, "base_table_name", "");
        let view_name = get_string_or(row, "view_name", "");
        let table_key = format!("{keyspace_name}.{base_table_name}");
        view_map.entry(table_key).or_default().push(view_name);
    }

    view_map
}

fn build_type_usage_map(columns_data: &Value) -> HashMap<String, Vec<String>> {
    let mut usage_map: HashMap<String, Vec<String>> = HashMap::new();

    let Value::Array(rows) = columns_data else {
        return usage_map;
    };

    for row in rows {
        let keyspace_name = get_string_or(row, "keyspace_name", "");
        let table_name = get_string_or(row, "table_name", "");
        let column_type = get_string_or(row, "type", "");

        if let Some(udt_name) = extract_udt_from_type(&column_type) {
            let type_key = format!("{keyspace_name}.{udt_name}");
            let table_key = format!("{keyspace_name}.{table_name}");
            usage_map.entry(type_key).or_default().push(table_key);
        }
    }

    usage_map
}

// Row-level field extraction

fn extract_key_columns(columns: &[CassandraColumnDetail]) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut partition_key = Vec::new();
    let mut clustering_key = Vec::new();
    let mut static_columns = Vec::new();

    for column in columns {
        match column.column_kind.as_str() {
            "partition_key" => partition_key.push(column.column_name.clone()),
            "clustering" => clustering_key.push(column.column_name.clone()),
            "static" => static_columns.push(column.column_name.clone()),
            _ => {}
        }
    }

    partition_key.sort();
    clustering_key.sort();

    (partition_key, clustering_key, static_columns)
}

fn extract_compaction_strategy(row: &Value) -> String {
    if let Some(compaction_value) = row.get("compaction")
        && let Some(compaction_map) = compaction_value.as_object()
        && let Some(class_value) = compaction_map.get("class")
        && let Some(class_str) = class_value.as_str()
    {
        return class_str.split('.').next_back().unwrap_or(class_str).to_string();
    }
    "Unknown".to_string()
}

fn extract_compression_algorithm(row: &Value) -> String {
    if let Some(compression_value) = row.get("compression")
        && let Some(compression_map) = compression_value.as_object()
        && let Some(algorithm_value) = compression_map.get("algorithm")
        && let Some(algorithm_str) = algorithm_value.as_str()
    {
        return algorithm_str.to_string();
    }
    "Unknown".to_string()
}

fn extract_caching_config(row: &Value) -> HashMap<String, String> {
    let mut config = HashMap::new();
    if let Some(caching_value) = row.get("caching")
        && let Some(caching_map) = caching_value.as_object()
    {
        for (key, value) in caching_map {
            if let Some(value_str) = value.as_str() {
                config.insert(key.clone(), value_str.to_string());
            }
        }
    }
    config
}

fn extract_flags(row: &Value) -> Vec<String> {
    if let Some(flags_value) = row.get("flags")
        && let Some(flags_array) = flags_value.as_array()
    {
        return flags_array.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
    }
    vec![]
}

fn extract_index_options(row: &Value) -> HashMap<String, String> {
    let mut options = HashMap::new();
    if let Some(options_value) = row.get("options")
        && let Some(options_map) = options_value.as_object()
    {
        for (key, value) in options_map {
            if let Some(value_str) = value.as_str() {
                options.insert(key.clone(), value_str.to_string());
            }
        }
    }
    options
}

fn extract_string_list(row: &Value, field: &str) -> Vec<String> {
    if let Some(array_value) = row.get(field)
        && let Some(array) = array_value.as_array()
    {
        return array.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
    }
    vec![]
}

// Analysis helpers

fn is_collection_type(column_type: &str) -> bool {
    column_type.contains("map<") || column_type.contains("set<") || column_type.contains("list<")
}

fn extract_udt_from_type(column_type: &str) -> Option<String> {
    if !column_type.contains('<') && !is_primitive_type(column_type) {
        Some(column_type.to_string())
    } else {
        None
    }
}

fn is_primitive_type(column_type: &str) -> bool {
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

fn calculate_complexity_score(metrics: &CassandraSchemaComplexityMetrics, tables: &[CassandraTableDetail]) -> f64 {
    let mut score = 0.0;

    score += metrics.avg_columns_per_table * 2.0;
    score += metrics.avg_partition_key_complexity * 10.0;
    score += metrics.avg_clustering_key_complexity * 15.0;

    if !tables.is_empty() {
        let total_tables = tables.len() as f64;
        score += (metrics.tables_with_composite_keys as f64 / total_tables) * 20.0;
        score += (metrics.tables_with_static_columns as f64 / total_tables) * 10.0;
        score += (metrics.tables_with_collections as f64 / total_tables) * 15.0;
    }

    score += metrics.avg_indexes_per_table * 5.0;

    score.min(100.0)
}

fn has_good_naming(table_name: &str) -> bool {
    table_name.chars().all(|c| c.is_lowercase() || c.is_numeric() || c == '_') && !table_name.starts_with('_') && !table_name.ends_with('_')
}

fn has_good_partition_design(table: &CassandraTableDetail) -> bool {
    !table.partition_key.is_empty() && table.partition_key.len() <= 3
}

fn has_anti_patterns(table: &CassandraTableDetail) -> bool {
    table.columns.len() > 100
        || table.clustering_key.len() > 10
        || (table.partition_key.len() == 1 && table.clustering_key.is_empty() && table.columns.len() > 20)
}

// Public analysis methods on CassandraSchemaInfo

impl CassandraSchemaInfo {
    /// Checks if the schema has critical issues requiring immediate attention.
    pub fn has_critical_schema_issues(&self) -> bool {
        !self.schema_agreement
            || self.health_metrics.health_score < 50.0
            || self.complexity_metrics.complexity_score > 80.0
            || self.health_metrics.security_issues_count > 0
    }

    /// Gets tables that need immediate attention.
    pub fn tables_needing_attention(&self) -> Vec<&CassandraTableDetail> {
        self.table_details
            .iter()
            .filter(|table| {
                table.columns.len() > EXCESSIVE_COLUMNS_THRESHOLD as usize
                    || table.clustering_key.len() > WIDE_ROW_CLUSTERING_THRESHOLD as usize
                    || has_anti_patterns(table)
            })
            .collect()
    }

    /// Gets schema complexity rating (A-F scale).
    pub fn complexity_rating(&self) -> String {
        match self.complexity_metrics.complexity_score {
            s if s <= 20.0 => "A".to_string(),
            s if s <= 40.0 => "B".to_string(),
            s if s <= 60.0 => "C".to_string(),
            s if s <= 80.0 => "D".to_string(),
            _ => "F".to_string(),
        }
    }

    /// Gets schema health rating (A-F scale).
    pub fn health_rating(&self) -> String {
        match self.health_metrics.health_score {
            s if s >= 90.0 => "A".to_string(),
            s if s >= 80.0 => "B".to_string(),
            s if s >= 70.0 => "C".to_string(),
            s if s >= 60.0 => "D".to_string(),
            _ => "F".to_string(),
        }
    }

    /// Gets the most complex tables by column count.
    pub fn most_complex_tables(&self) -> Vec<&CassandraTableDetail> {
        let mut tables = self.table_details.iter().collect::<Vec<_>>();
        tables.sort_by(|a, b| b.columns.len().cmp(&a.columns.len()));
        tables.into_iter().take(10).collect()
    }

    /// Gets unused or potentially unused indexes.
    pub fn potentially_unused_indexes(&self) -> Vec<&CassandraIndexDetail> {
        let heavy_indexed_tables: std::collections::HashSet<String> = self
            .table_details
            .iter()
            .filter(|table| table.index_count > 3)
            .map(|table| format!("{}.{}", table.keyspace_name, table.table_name))
            .collect();

        self.index_details
            .iter()
            .filter(|index| {
                let table_key = format!("{}.{}", index.keyspace_name, index.table_name);
                heavy_indexed_tables.contains(&table_key)
            })
            .collect()
    }

    /// Gets recommended schema improvements.
    pub fn get_schema_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if !self.schema_agreement {
            recommendations.push("CRITICAL: Resolve schema disagreement across cluster nodes".to_string());
        }

        if self.complexity_metrics.complexity_score > 70.0 {
            recommendations.push("High schema complexity detected - consider simplification".to_string());
        }

        if self.health_metrics.health_score < 70.0 {
            recommendations.push("Poor schema health - review naming conventions and design patterns".to_string());
        }

        if self.complexity_metrics.tables_with_excessive_columns > 0 {
            recommendations.push(format!(
                "{} tables have excessive columns - consider normalization",
                self.complexity_metrics.tables_with_excessive_columns
            ));
        }

        if self.health_metrics.tables_with_anti_patterns > 0 {
            recommendations.push(format!(
                "{} tables show anti-patterns - review partition design",
                self.health_metrics.tables_with_anti_patterns
            ));
        }

        if self.health_metrics.security_issues_count > 0 {
            recommendations.push("Security issues detected - review authentication setup".to_string());
        }

        if self.evolution_metrics.stability_score < 80.0 {
            recommendations.push("High schema change frequency - consider stabilization".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("Schema appears healthy - continue monitoring".to_string());
        }

        recommendations
    }

    /// Gets statistics about schema distribution.
    pub fn get_schema_distribution_stats(&self) -> CassandraSchemaDistributionStats {
        CassandraSchemaDistributionStats {
            keyspaces_by_table_count: self.get_keyspaces_by_table_count(),
            tables_by_column_count_ranges: self.get_tables_by_column_ranges(),
            index_distribution: self.get_index_distribution(),
            type_usage_distribution: self.get_type_usage_distribution(),
        }
    }

    fn get_keyspaces_by_table_count(&self) -> HashMap<String, u64> {
        let mut counts: HashMap<String, u64> = HashMap::new();
        for table in &self.table_details {
            *counts.entry(table.keyspace_name.clone()).or_insert(0) += 1;
        }
        counts
    }

    fn get_tables_by_column_ranges(&self) -> HashMap<String, u64> {
        let mut ranges = HashMap::from([
            ("1-10".to_string(), 0u64),
            ("11-25".to_string(), 0),
            ("26-50".to_string(), 0),
            ("51+".to_string(), 0),
        ]);
        for table in &self.table_details {
            let range_key = match table.columns.len() {
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

    fn get_index_distribution(&self) -> HashMap<String, u64> {
        let mut distribution = HashMap::new();
        for index in &self.index_details {
            *distribution.entry(index.index_type.clone()).or_insert(0) += 1;
        }
        distribution
    }

    fn get_type_usage_distribution(&self) -> HashMap<String, u64> {
        let mut distribution = HashMap::new();
        for type_detail in &self.type_details {
            let usage_range = match type_detail.used_by_tables.len() as u64 {
                0 => "Unused",
                1 => "Single Use",
                2..=5 => "Low Use",
                6..=10 => "Medium Use",
                _ => "High Use",
            };
            *distribution.entry(usage_range.to_string()).or_insert(0) += 1;
        }
        distribution
    }

    /// Gets summary for reporting.
    pub fn get_schema_summary(&self) -> CassandraSchemaSummary {
        CassandraSchemaSummary {
            schema_agreement: self.schema_agreement,
            total_keyspaces: self.total_keyspaces,
            total_tables: self.total_tables,
            total_indexes: self.total_indexes,
            complexity_score: self.complexity_metrics.complexity_score,
            complexity_rating: self.complexity_rating(),
            health_score: self.health_metrics.health_score,
            health_rating: self.health_rating(),
            tables_needing_attention: self.tables_needing_attention().len() as u64,
            schema_versions_count: self.schema_versions.len() as u64,
            has_critical_issues: self.has_critical_schema_issues(),
        }
    }
}

// Tests

#[cfg(test)]
mod tests;
