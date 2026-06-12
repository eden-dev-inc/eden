use crate::api::{Deserialize, InfoInput, RedisJsonValue, Serialize};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};
use std::collections::HashMap;

/// Redis modules information and statistics
///
/// This struct contains comprehensive module metrics from loaded Redis modules,
/// with specific support for commonly used modules.
/// Data is collected from the "Modules" section of Redis INFO command.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct RedisModulesInfo {
    /// RediSearch module information (if loaded)
    pub redisearch: Option<RediSearchInfo>,

    /// ReJSON module information (if loaded)
    pub rejson: Option<ReJSONInfo>,

    /// RedisTimeSeries module information (if loaded)
    pub timeseries: Option<TimeSeriesInfo>,

    /// RedisBloom module information (if loaded)
    pub redisbloom: Option<RedisBloomInfo>,

    /// RedisGraph module information (if loaded)
    pub redisgraph: Option<RedisGraphInfo>,

    /// Other module information (module_name -> key-value pairs)
    pub other_modules: HashMap<String, HashMap<String, String>>,
}

impl MetadataCollection for RedisModulesInfo {
    type Request = InfoInput;

    fn request(&self) -> Self::Request {
        Self::Request::new(Some(vec![RedisJsonValue::String("modules".to_string())]))
    }

    fn description(&self) -> &'static str {
        "Return the modules information for the Redis database"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "modules"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

impl RedisModulesInfo {
    /// Checks if RediSearch module is loaded
    pub fn has_redisearch(&self) -> bool {
        self.redisearch.is_some()
    }

    /// Checks if ReJSON module is loaded
    pub fn has_rejson(&self) -> bool {
        self.rejson.is_some()
    }

    /// Checks if RedisTimeSeries module is loaded
    pub fn has_timeseries(&self) -> bool {
        self.timeseries.is_some()
    }

    /// Checks if RedisBloom module is loaded
    pub fn has_redisbloom(&self) -> bool {
        self.redisbloom.is_some()
    }

    /// Checks if RedisGraph module is loaded
    pub fn has_redisgraph(&self) -> bool {
        self.redisgraph.is_some()
    }

    /// Gets the list of loaded module names
    pub fn get_loaded_module_names(&self) -> Vec<String> {
        let mut modules = Vec::new();

        if self.redisearch.is_some() {
            modules.push("search".to_string());
        }
        if self.rejson.is_some() {
            modules.push("ReJSON".to_string());
        }
        if self.timeseries.is_some() {
            modules.push("timeseries".to_string());
        }
        if self.redisbloom.is_some() {
            modules.push("bf".to_string());
        }
        if self.redisgraph.is_some() {
            modules.push("graph".to_string());
        }

        modules.extend(self.other_modules.keys().cloned());
        modules
    }

    /// Gets the list of loaded module names (alias for get_loaded_module_names)
    pub fn get_loaded_modules(&self) -> Vec<String> {
        self.get_loaded_module_names()
    }

    /// Gets metrics for a specific module
    pub fn get_module_metrics(&self, module_name: &str) -> Option<&HashMap<String, String>> {
        self.other_modules.get(module_name)
    }

    /// Gets total memory usage by all modules
    pub fn total_module_memory(&self) -> u64 {
        let mut total = 0u64;

        if let Some(ref redisearch) = self.redisearch {
            total += redisearch.search_used_memory_indexes;
        }

        // Add memory from other modules if available
        for metrics in self.other_modules.values() {
            if let Some(memory_str) = metrics.get("memory_usage")
                && let Ok(memory) = memory_str.parse::<u64>()
            {
                total += memory;
            }
        }

        total
    }
}

/// ReJSON module information and statistics
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct ReJSONInfo {
    /// ReJSON module version
    pub version: String,
    /// Number of JSON documents stored
    pub json_docs: u64,
    /// Memory used by JSON documents (bytes)
    pub json_memory_usage: u64,
    /// Number of JSON paths indexed
    pub json_paths: u64,
}

/// RedisTimeSeries module information and statistics
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct TimeSeriesInfo {
    /// TimeSeries module version
    pub version: String,
    /// Number of time series created
    pub ts_num_series: u64,
    /// Number of samples across all time series
    pub ts_num_samples: u64,
    /// Memory used by time series (bytes)
    pub ts_memory_usage: u64,
    /// Number of chunks in memory
    pub ts_num_chunks: u64,
    /// Number of chunks on disk
    pub ts_num_chunks_disk: u64,
    /// Number of duplicate samples
    pub ts_duplicate_samples: u64,
    /// Total number of series (alias for ts_num_series)
    pub ts_number_of_series: u64,
    /// Total number of samples (alias for ts_num_samples)
    pub ts_total_samples: u64,
    /// Total chunks (alias for ts_num_chunks)
    pub ts_total_chunks: u64,
}

/// RedisBloom module information and statistics
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct RedisBloomInfo {
    /// RedisBloom module version
    pub version: String,
    /// Number of Bloom filters
    pub bf_num_filters: u64,
    /// Memory used by Bloom filters (bytes)
    pub bf_memory_usage: u64,
    /// Number of Cuckoo filters
    pub cf_num_filters: u64,
    /// Memory used by Cuckoo filters (bytes)
    pub cf_memory_usage: u64,
    /// Number of Count-Min Sketch filters
    pub cms_num_sketches: u64,
    /// Memory used by Count-Min Sketch filters (bytes)
    pub cms_memory_usage: u64,
    /// Number of Top-K sketches
    pub topk_num_sketches: u64,
    /// Memory used by Top-K sketches (bytes)
    pub topk_memory_usage: u64,
    /// Total number of bloom filters (alias for bf_num_filters)
    pub bf_number_of_filters: u64,
    /// Total capacity of all bloom filters
    pub bf_total_capacity: u64,
    /// Total size in bytes of all bloom filters
    pub bf_total_size_bytes: u64,
    /// Total number of count-min sketches (alias for cms_num_sketches)
    pub cms_number_of_sketches: u64,
    /// Total number of top-k lists (alias for topk_num_sketches)
    pub topk_number_of_lists: u64,
}

/// RedisGraph module information and statistics
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct RedisGraphInfo {
    /// RedisGraph module version
    pub version: String,
    /// Number of graphs created
    pub graph_num_graphs: u64,
    /// Number of nodes across all graphs
    pub graph_num_nodes: u64,
    /// Number of relationships across all graphs
    pub graph_num_relationships: u64,
    /// Memory used by graphs (bytes)
    pub graph_memory_usage: u64,
    /// Number of queries executed
    pub graph_queries_executed: u64,
    /// Total query execution time (milliseconds)
    pub graph_query_execution_time_ms: u64,
    /// Number of cached queries
    pub graph_cached_queries: u64,
    /// Total number of graphs (alias for graph_num_graphs)
    pub graph_number_of_graphs: u64,
    /// Total number of nodes (alias for graph_num_nodes)
    pub graph_total_nodes: u64,
    /// Total number of relationships (alias for graph_num_relationships)
    pub graph_total_relationships: u64,
}

/// RediSearch (Redis Query Engine) module statistics
///
/// Contains comprehensive metrics for RediSearch module including garbage collection,
/// indexing operations, memory usage, query performance, and field statistics.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct RediSearchInfo {
    // Garbage Collection metrics
    /// Total memory freed by garbage collectors from indexes (bytes) - v8.0+
    pub search_gc_bytes_collected: u64,
    /// [Deprecated] Total memory freed by garbage collectors (bytes) - pre-v8.0
    pub search_bytes_collected: u64,
    /// Number of vectors marked as deleted but not yet cleaned - v8.0+
    pub search_gc_marked_deleted_vectors: u64,
    /// [Deprecated] Number of vectors marked as deleted - pre-v8.0
    pub search_marked_deleted_vectors: u64,
    /// Total number of garbage collection cycles executed - v8.0+
    pub search_gc_total_cycles: u64,
    /// [Deprecated] Total GC cycles executed - pre-v8.0
    pub search_total_cycles: u64,
    /// Number of documents marked as deleted, not freed by GC - v8.0+
    pub search_gc_total_docs_not_collected_by_gc: u64,
    /// [Deprecated] Documents not collected by GC - pre-v8.0
    pub search_total_docs_not_collected_by_gc: u64,
    /// Total duration of all GC cycles (milliseconds) - v8.0+
    pub search_gc_total_ms_run: u64,
    /// [Deprecated] Total GC duration (milliseconds) - pre-v8.0
    pub search_total_ms_run: u64,

    // Cursor management
    /// Total coordinator cursors holding pending results - v8.0+
    pub search_cursors_internal_idle: u64,
    /// Total user cursors holding pending results - v8.0+
    pub search_cursors_user_idle: u64,
    /// [Deprecated] Total cursors holding pending results - pre-v8.0
    pub search_global_idle: u64,
    /// Total coordinator cursors (active or holding results) - v8.0+
    pub search_cursors_internal_active: u64,
    /// Total user cursors (active or holding results) - v8.0+
    pub search_cursors_user_active: u64,
    /// [Deprecated] Total cursors (active or holding results) - pre-v8.0
    pub search_global_total: u64,

    // Index statistics
    /// Total number of indexes in the shard
    pub search_number_of_indexes: u32,
    /// Number of indexes running background operations
    pub search_number_of_active_indexes: u32,
    /// Number of indexes running background queries
    pub search_number_of_active_indexes_running_queries: u32,
    /// Number of indexes undergoing background indexing
    pub search_number_of_active_indexes_indexing: u32,
    /// Total count of background write processes running
    pub search_total_active_write_threads: u32,

    // Field type statistics
    /// TEXT field counts by type
    pub search_fields_text: FieldTypeStats,
    /// NUMERIC field counts by type
    pub search_fields_numeric: FieldTypeStats,
    /// TAG field counts by type
    pub search_fields_tag: TagFieldStats,
    /// GEO field counts by type
    pub search_fields_geo: FieldTypeStats,
    /// VECTOR field counts by type
    pub search_fields_vector: VectorFieldStats,
    /// GEOSHAPE field counts by type (RediSearch 2.8+)
    pub search_fields_geoshape: FieldTypeStats,

    // Indexing errors by field
    /// Indexing failures per field type
    pub search_field_errors: HashMap<String, u64>,

    // Memory usage
    /// Total memory allocated by all indexes (bytes)
    pub search_used_memory_indexes: u64,
    /// Total memory allocated by all indexes (human readable)
    pub search_used_memory_indexes_human: String,
    /// Memory usage of smallest index (bytes)
    pub search_smallest_memory_index: u64,
    /// Memory usage of smallest index (human readable)
    pub search_smallest_memory_index_human: String,
    /// Memory usage of largest index (bytes)
    pub search_largest_memory_index: u64,
    /// Memory usage of largest index (human readable)
    pub search_largest_memory_index_human: String,
    /// Total memory usage of all vector indexes
    pub search_used_memory_vector_index: u64,

    // Performance metrics
    /// Total time spent on indexing operations
    pub search_total_indexing_time: u64,
    /// Total number of successful query executions
    pub search_total_queries_processed: u64,
    /// Total number of successful query commands
    pub search_total_query_commands: u64,
    /// Cumulative execution time of all query commands (ms)
    pub search_total_query_execution_time_ms: u64,
    /// Number of background queries currently executing
    pub search_total_active_queries: u32,

    // Error tracking
    /// Total indexing failures across all indexes
    pub search_errors_indexing_failures: u64,
    /// Indexing failures in worst-performing index
    pub search_errors_for_index_with_max_failures: u64,

    // Additional fields for detailed parsing
    /// Number of documents across all indexes
    pub search_number_of_documents: u64,
    /// Maximum document ID across all indexes
    pub search_max_doc_id: u64,
    /// Number of terms across all indexes
    pub search_number_of_terms: u64,
    /// Number of records across all indexes
    pub search_number_of_records: u64,
    /// Size of inverted index in MB
    pub search_inverted_index_mb: f64,
    /// Size of vector index in MB
    pub search_vector_index_mb: f64,
    /// Total number of inverted index blocks
    pub search_total_inverted_index_blocks: u64,
    /// Size of offset vectors in MB
    pub search_offset_vectors_mb: f64,
    /// Size of document table in MB
    pub search_doc_table_size_mb: f64,
    /// Size of sortable values in MB
    pub search_sortable_values_size_mb: f64,
    /// Size of key table in MB
    pub search_key_table_size_mb: f64,
    /// Average records per document
    pub search_records_per_doc_avg: f64,
    /// Average bytes per record
    pub search_bytes_per_record_avg: f64,
    /// Average offsets per term
    pub search_offsets_per_term_avg: f64,
    /// Average offset bits per record
    pub search_offset_bits_per_record_avg: f64,
    /// Hash indexing failures
    pub search_hash_indexing_failures: u64,
    /// Total indexing time in seconds
    pub search_total_indexing_time_sec: f64,
    /// Whether indexing is currently in progress
    pub search_indexing_in_progress: bool,
    /// Indexing completion percentage
    pub search_indexing_percentage: f64,
    /// Number of times indexes have been used
    pub search_number_of_uses: u64,
    /// Whether global stats are available
    pub search_global_stats_available: bool,
    /// Module version
    pub version: String,
}

/// Statistics for basic field types (TEXT, NUMERIC, GEO, GEOSHAPE)
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct FieldTypeStats {
    /// Total number of fields of this type
    pub total: u32,
    /// Number of sortable fields (only if > 0)
    pub sortable: u32,
    /// Number of no-index fields (only if > 0)
    pub no_index: u32,
}

/// Statistics for TAG fields (includes case sensitivity)
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct TagFieldStats {
    /// Total number of TAG fields
    pub total: u32,
    /// Number of sortable TAG fields (only if > 0)
    pub sortable: u32,
    /// Number of no-index TAG fields (only if > 0)
    pub no_index: u32,
    /// Number of case-sensitive TAG fields (only if > 0)
    pub case_sensitive: u32,
}

/// Statistics for VECTOR fields (includes algorithm types)
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct VectorFieldStats {
    /// Total number of VECTOR fields
    pub total: u32,
    /// Number of FLAT VECTOR fields
    pub flat: u32,
    /// Number of HNSW VECTOR fields
    pub hnsw: u32,
}

impl Default for RediSearchInfo {
    fn default() -> Self {
        Self {
            search_gc_bytes_collected: 0,
            search_bytes_collected: 0,
            search_gc_marked_deleted_vectors: 0,
            search_marked_deleted_vectors: 0,
            search_gc_total_cycles: 0,
            search_total_cycles: 0,
            search_gc_total_docs_not_collected_by_gc: 0,
            search_total_docs_not_collected_by_gc: 0,
            search_gc_total_ms_run: 0,
            search_total_ms_run: 0,
            search_cursors_internal_idle: 0,
            search_cursors_user_idle: 0,
            search_global_idle: 0,
            search_cursors_internal_active: 0,
            search_cursors_user_active: 0,
            search_global_total: 0,
            search_number_of_indexes: 0,
            search_number_of_active_indexes: 0,
            search_number_of_active_indexes_running_queries: 0,
            search_number_of_active_indexes_indexing: 0,
            search_total_active_write_threads: 0,
            search_fields_text: FieldTypeStats::default(),
            search_fields_numeric: FieldTypeStats::default(),
            search_fields_tag: TagFieldStats::default(),
            search_fields_geo: FieldTypeStats::default(),
            search_fields_vector: VectorFieldStats::default(),
            search_fields_geoshape: FieldTypeStats::default(),
            search_field_errors: HashMap::new(),
            search_used_memory_indexes: 0,
            search_used_memory_indexes_human: String::new(),
            search_smallest_memory_index: 0,
            search_smallest_memory_index_human: String::new(),
            search_largest_memory_index: 0,
            search_largest_memory_index_human: String::new(),
            search_used_memory_vector_index: 0,
            search_total_indexing_time: 0,
            search_total_queries_processed: 0,
            search_total_query_commands: 0,
            search_total_query_execution_time_ms: 0,
            search_total_active_queries: 0,
            search_errors_indexing_failures: 0,
            search_errors_for_index_with_max_failures: 0,
            search_number_of_documents: 0,
            search_max_doc_id: 0,
            search_number_of_terms: 0,
            search_number_of_records: 0,
            search_inverted_index_mb: 0.0,
            search_vector_index_mb: 0.0,
            search_total_inverted_index_blocks: 0,
            search_offset_vectors_mb: 0.0,
            search_doc_table_size_mb: 0.0,
            search_sortable_values_size_mb: 0.0,
            search_key_table_size_mb: 0.0,
            search_records_per_doc_avg: 0.0,
            search_bytes_per_record_avg: 0.0,
            search_offsets_per_term_avg: 0.0,
            search_offset_bits_per_record_avg: 0.0,
            search_hash_indexing_failures: 0,
            search_total_indexing_time_sec: 0.0,
            search_indexing_in_progress: false,
            search_indexing_percentage: 0.0,
            search_number_of_uses: 0,
            search_global_stats_available: false,
            version: String::new(),
        }
    }
}

impl RediSearchInfo {
    /// Parses RediSearch-specific metrics from key-value pairs
    pub fn parse_from_pairs(&mut self, pairs: &HashMap<String, String>) {
        // Garbage Collection metrics (prefer v8.0+ names, fall back to deprecated)
        self.search_gc_bytes_collected = pairs
            .get("search_gc_bytes_collected")
            .or_else(|| pairs.get("search_bytes_collected"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        self.search_bytes_collected = pairs.get("search_bytes_collected").and_then(|s| s.parse().ok()).unwrap_or(0);

        self.search_gc_marked_deleted_vectors = pairs
            .get("search_gc_marked_deleted_vectors")
            .or_else(|| pairs.get("search_marked_deleted_vectors"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        self.search_marked_deleted_vectors = pairs.get("search_marked_deleted_vectors").and_then(|s| s.parse().ok()).unwrap_or(0);

        self.search_gc_total_cycles = pairs
            .get("search_gc_total_cycles")
            .or_else(|| pairs.get("search_total_cycles"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        self.search_total_cycles = pairs.get("search_total_cycles").and_then(|s| s.parse().ok()).unwrap_or(0);

        self.search_gc_total_docs_not_collected_by_gc = pairs
            .get("search_gc_total_docs_not_collected_by_gc")
            .or_else(|| pairs.get("search_total_docs_not_collected_by_gc"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        self.search_total_docs_not_collected_by_gc =
            pairs.get("search_total_docs_not_collected_by_gc").and_then(|s| s.parse().ok()).unwrap_or(0);

        self.search_gc_total_ms_run = pairs
            .get("search_gc_total_ms_run")
            .or_else(|| pairs.get("search_total_ms_run"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        self.search_total_ms_run = pairs.get("search_total_ms_run").and_then(|s| s.parse().ok()).unwrap_or(0);

        // Cursor management (prefer v8.0+ split fields, fall back to global)
        self.search_cursors_internal_idle = pairs.get("search_cursors_internal_idle").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_cursors_user_idle = pairs.get("search_cursors_user_idle").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_global_idle = pairs.get("search_global_idle").and_then(|s| s.parse().ok()).unwrap_or(0);

        self.search_cursors_internal_active = pairs.get("search_cursors_internal_active").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_cursors_user_active = pairs.get("search_cursors_user_active").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_global_total = pairs.get("search_global_total").and_then(|s| s.parse().ok()).unwrap_or(0);

        // Index statistics
        self.search_number_of_indexes = pairs.get("search_number_of_indexes").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_number_of_active_indexes = pairs.get("search_number_of_active_indexes").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_number_of_active_indexes_running_queries =
            pairs.get("search_number_of_active_indexes_running_queries").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_number_of_active_indexes_indexing =
            pairs.get("search_number_of_active_indexes_indexing").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_total_active_write_threads = pairs.get("search_total_active_write_threads").and_then(|s| s.parse().ok()).unwrap_or(0);

        // Field type statistics
        self.search_fields_text = FieldTypeStats::parse_from_pairs(pairs, "search_fields_text");
        self.search_fields_numeric = FieldTypeStats::parse_from_pairs(pairs, "search_fields_numeric");
        self.search_fields_tag = TagFieldStats::parse_from_pairs(pairs, "search_fields_tag");
        self.search_fields_geo = FieldTypeStats::parse_from_pairs(pairs, "search_fields_geo");
        self.search_fields_vector = VectorFieldStats::parse_from_pairs(pairs, "search_fields_vector");
        self.search_fields_geoshape = FieldTypeStats::parse_from_pairs(pairs, "search_fields_geoshape");

        // Parse field errors
        self.search_field_errors.clear();
        for (key, value) in pairs {
            if key.starts_with("search_fields_")
                && key.ends_with("_IndexErrors")
                && let Some(field_type) = key.strip_prefix("search_fields_").and_then(|s| s.strip_suffix("_IndexErrors"))
                && let Ok(errors) = value.parse::<u64>()
            {
                self.search_field_errors.insert(field_type.to_string(), errors);
            }
        }

        // Memory usage
        self.search_used_memory_indexes = pairs.get("search_used_memory_indexes").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_used_memory_indexes_human = pairs.get("search_used_memory_indexes_human").cloned().unwrap_or_default();
        self.search_smallest_memory_index = pairs.get("search_smallest_memory_index").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_smallest_memory_index_human = pairs.get("search_smallest_memory_index_human").cloned().unwrap_or_default();
        self.search_largest_memory_index = pairs.get("search_largest_memory_index").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_largest_memory_index_human = pairs.get("search_largest_memory_index_human").cloned().unwrap_or_default();
        self.search_used_memory_vector_index = pairs.get("search_used_memory_vector_index").and_then(|s| s.parse().ok()).unwrap_or(0);

        // Performance metrics
        self.search_total_indexing_time = pairs.get("search_total_indexing_time").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_total_queries_processed = pairs.get("search_total_queries_processed").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_total_query_commands = pairs.get("search_total_query_commands").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_total_query_execution_time_ms =
            pairs.get("search_total_query_execution_time_ms").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_total_active_queries = pairs.get("search_total_active_queries").and_then(|s| s.parse().ok()).unwrap_or(0);

        // Error tracking
        self.search_errors_indexing_failures = pairs.get("search_errors_indexing_failures").and_then(|s| s.parse().ok()).unwrap_or(0);
        self.search_errors_for_index_with_max_failures =
            pairs.get("search_errors_for_index_with_max_failures").and_then(|s| s.parse().ok()).unwrap_or(0);
    }

    /// Calculates garbage collection efficiency
    ///
    /// # Returns
    /// * Ratio of memory collected to GC cycles (bytes per cycle)
    pub fn gc_efficiency(&self) -> f64 {
        let cycles = if self.search_gc_total_cycles > 0 {
            self.search_gc_total_cycles
        } else {
            self.search_total_cycles
        };

        let bytes_collected = if self.search_gc_bytes_collected > 0 {
            self.search_gc_bytes_collected
        } else {
            self.search_bytes_collected
        };

        if cycles == 0 { 0.0 } else { bytes_collected as f64 / cycles as f64 }
    }

    /// Calculates average query execution time
    ///
    /// # Returns
    /// * Average query execution time in milliseconds
    pub fn average_query_execution_time_ms(&self) -> f64 {
        if self.search_total_query_commands == 0 {
            0.0
        } else {
            self.search_total_query_execution_time_ms as f64 / self.search_total_query_commands as f64
        }
    }

    /// Calculates indexing error rate
    ///
    /// # Returns
    /// * Percentage of indexing operations that failed
    pub fn indexing_error_rate(&self) -> f64 {
        // This is an approximation since we don't have total indexing operations
        if self.search_total_indexing_time == 0 {
            0.0
        } else {
            // Use failures per time unit as a proxy
            self.search_errors_indexing_failures as f64 / self.search_total_indexing_time as f64 * 1000.0
        }
    }

    /// Checks if garbage collection is keeping up with deletions
    ///
    /// # Returns
    /// * True if GC appears to be falling behind
    pub fn is_gc_lagging(&self) -> bool {
        let pending_docs = if self.search_gc_total_docs_not_collected_by_gc > 0 {
            self.search_gc_total_docs_not_collected_by_gc
        } else {
            self.search_total_docs_not_collected_by_gc
        };

        let pending_vectors = if self.search_gc_marked_deleted_vectors > 0 {
            self.search_gc_marked_deleted_vectors
        } else {
            self.search_marked_deleted_vectors
        };

        // Consider GC lagging if there are many pending deletions
        pending_docs > 1000 || pending_vectors > 1000
    }

    /// Gets total number of fields across all types
    ///
    /// # Returns
    /// * Total count of all indexed fields
    pub fn total_field_count(&self) -> u32 {
        self.search_fields_text.total
            + self.search_fields_numeric.total
            + self.search_fields_tag.total
            + self.search_fields_geo.total
            + self.search_fields_vector.total
            + self.search_fields_geoshape.total
    }

    /// Calculates memory efficiency (memory per index)
    ///
    /// # Returns
    /// * Average memory usage per index in bytes
    pub fn average_memory_per_index(&self) -> f64 {
        if self.search_number_of_indexes == 0 {
            0.0
        } else {
            self.search_used_memory_indexes as f64 / self.search_number_of_indexes as f64
        }
    }

    /// Checks if there are concerning levels of indexing activity
    ///
    /// # Returns
    /// * True if many indexes are actively indexing (might indicate performance impact)
    pub fn has_high_indexing_activity(&self) -> bool {
        if self.search_number_of_indexes == 0 {
            false
        } else {
            let active_ratio = self.search_number_of_active_indexes_indexing as f64 / self.search_number_of_indexes as f64;
            active_ratio > 0.5 // More than 50% of indexes are actively indexing
        }
    }

    /// Gets RediSearch health summary
    ///
    /// # Returns
    /// * Tuple of (is_healthy, has_gc_issues, has_error_issues, has_performance_issues)
    pub fn health_summary(&self) -> (bool, bool, bool, bool) {
        let has_gc_issues = self.is_gc_lagging();
        let has_error_issues = self.search_errors_indexing_failures > 0;
        let has_performance_issues = self.has_high_indexing_activity() || self.average_query_execution_time_ms() > 100.0;
        let is_healthy = !has_gc_issues && !has_error_issues && !has_performance_issues;

        (is_healthy, has_gc_issues, has_error_issues, has_performance_issues)
    }
}

impl FieldTypeStats {
    fn parse_from_pairs(pairs: &HashMap<String, String>, prefix: &str) -> Self {
        Self {
            total: pairs
                .get(&format!("{}_{}", prefix, "Text"))
                .or_else(|| pairs.get(&format!("{}_{}", prefix, "Numeric")))
                .or_else(|| pairs.get(&format!("{}_{}", prefix, "Geo")))
                .or_else(|| pairs.get(&format!("{}_{}", prefix, "Geoshape")))
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            sortable: pairs.get(&format!("{}_Sortable", prefix)).and_then(|s| s.parse().ok()).unwrap_or(0),
            no_index: pairs.get(&format!("{}_NoIndex", prefix)).and_then(|s| s.parse().ok()).unwrap_or(0),
        }
    }
}

impl TagFieldStats {
    fn parse_from_pairs(pairs: &HashMap<String, String>, prefix: &str) -> Self {
        Self {
            total: pairs.get(&format!("{}_Tag", prefix)).and_then(|s| s.parse().ok()).unwrap_or(0),
            sortable: pairs.get(&format!("{}_Sortable", prefix)).and_then(|s| s.parse().ok()).unwrap_or(0),
            no_index: pairs.get(&format!("{}_NoIndex", prefix)).and_then(|s| s.parse().ok()).unwrap_or(0),
            case_sensitive: pairs.get(&format!("{}_CaseSensitive", prefix)).and_then(|s| s.parse().ok()).unwrap_or(0),
        }
    }
}

impl VectorFieldStats {
    fn parse_from_pairs(pairs: &HashMap<String, String>, prefix: &str) -> Self {
        Self {
            total: pairs.get(&format!("{}_Vector", prefix)).and_then(|s| s.parse().ok()).unwrap_or(0),
            flat: pairs.get(&format!("{}_Flat", prefix)).and_then(|s| s.parse().ok()).unwrap_or(0),
            hnsw: pairs.get(&format!("{}_HNSW", prefix)).and_then(|s| s.parse().ok()).unwrap_or(0),
        }
    }
}
