use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Cassandra schema information and version tracking.
///
/// Covers version consistency, table structures, indexes and schema evolution.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraSchemaInfo {
    /// Schema version agreement status
    pub schema_agreement: bool,
    /// Number of nodes with schema disagreement
    pub nodes_with_disagreement: u64,
    /// Schema versions across the cluster
    pub schema_versions: HashMap<String, u64>,
    /// Primary schema version (most common)
    pub primary_schema_version: Option<String>,
    /// Total number of keyspaces
    pub total_keyspaces: u64,
    /// Total number of tables across all keyspaces
    pub total_tables: u64,
    /// Total number of indexes
    pub total_indexes: u64,
    /// Total number of materialized views
    pub total_materialized_views: u64,
    /// Total number of user-defined types
    pub total_user_types: u64,
    /// Total number of user-defined functions
    pub total_functions: u64,
    /// Total number of user-defined aggregates
    pub total_aggregates: u64,
    /// Schema complexity metrics
    pub complexity_metrics: CassandraSchemaComplexityMetrics,
    /// Detailed table information
    pub table_details: Vec<CassandraTableDetail>,
    /// Index information
    pub index_details: Vec<CassandraIndexDetail>,
    /// User-defined type information
    pub type_details: Vec<CassandraTypeDetail>,
    /// Function and aggregate information
    pub function_details: Vec<CassandraFunctionDetail>,
    /// Schema evolution tracking
    pub evolution_metrics: CassandraSchemaEvolutionMetrics,
    /// Schema health and best practices compliance
    pub health_metrics: CassandraSchemaHealthMetrics,
}

/// Schema complexity analysis metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraSchemaComplexityMetrics {
    /// Average number of columns per table
    pub avg_columns_per_table: f64,
    /// Maximum number of columns in any table
    pub max_columns_per_table: u64,
    /// Number of tables with excessive columns (>50)
    pub tables_with_excessive_columns: u64,
    /// Average partition key complexity (number of components)
    pub avg_partition_key_complexity: f64,
    /// Average clustering key complexity
    pub avg_clustering_key_complexity: f64,
    /// Number of tables with composite keys
    pub tables_with_composite_keys: u64,
    /// Number of tables with static columns
    pub tables_with_static_columns: u64,
    /// Number of tables with collections (maps, sets, lists)
    pub tables_with_collections: u64,
    /// Average number of indexes per table
    pub avg_indexes_per_table: f64,
    /// Number of tables with multiple indexes
    pub tables_with_multiple_indexes: u64,
    /// Schema complexity score (0-100, higher is more complex)
    pub complexity_score: f64,
}

/// Detailed information about a table
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableDetail {
    /// Keyspace name
    pub keyspace_name: String,
    /// Table name
    pub table_name: String,
    /// Table ID
    pub table_id: String,
    /// Column definitions
    pub columns: Vec<CassandraColumnDetail>,
    /// Partition key columns
    pub partition_key: Vec<String>,
    /// Clustering key columns
    pub clustering_key: Vec<String>,
    /// Static columns
    pub static_columns: Vec<String>,
    /// Compaction strategy
    pub compaction_strategy: String,
    /// Compression algorithm
    pub compression_algorithm: String,
    /// Caching configuration
    pub caching_config: HashMap<String, String>,
    /// Bloom filter false positive chance
    pub bloom_filter_fp_chance: f64,
    /// Default TTL (seconds)
    pub default_ttl: Option<u64>,
    /// GC grace seconds
    pub gc_grace_seconds: u64,
    /// Table comment/description
    pub comment: Option<String>,
    /// Table flags
    pub flags: Vec<String>,
    /// Number of indexes on this table
    pub index_count: u64,
    /// Has materialized views
    pub has_materialized_views: bool,
    /// Schema version when table was created/modified
    pub schema_version: Option<String>,
}

/// Column definition details
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraColumnDetail {
    /// Column name
    pub column_name: String,
    /// Column type (text, int, uuid etc.)
    pub column_type: String,
    /// Column kind (partition_key, clustering, regular, static)
    pub column_kind: String,
    /// Position in key (if applicable)
    pub position: Option<u64>,
    /// Clustering order (ASC/DESC)
    pub clustering_order: Option<String>,
    /// Is frozen (for collections/UDTs)
    pub is_frozen: bool,
    /// Type arguments (for collections and UDTs)
    pub type_arguments: Vec<String>,
}

/// Index information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraIndexDetail {
    /// Keyspace name
    pub keyspace_name: String,
    /// Table name
    pub table_name: String,
    /// Index name
    pub index_name: String,
    /// Index type (SECONDARY, CUSTOM etc.)
    pub index_type: String,
    /// Indexed column name
    pub target_column: String,
    /// Index options
    pub options: HashMap<String, String>,
    /// Custom index class (if applicable)
    pub custom_class: Option<String>,
    /// Is index ready for use
    pub is_ready: bool,
    /// Index creation timestamp
    pub created_at: Option<String>,
}

/// User-defined type information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTypeDetail {
    /// Keyspace name
    pub keyspace_name: String,
    /// Type name
    pub type_name: String,
    /// Field names in the UDT
    pub field_names: Vec<String>,
    /// Field types corresponding to field names
    pub field_types: Vec<String>,
    /// Number of fields in the type
    pub field_count: u64,
    /// Tables using this type
    pub used_by_tables: Vec<String>,
    /// Is frozen when used
    pub is_frozen_type: bool,
}

/// Function and aggregate information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraFunctionDetail {
    /// Keyspace name
    pub keyspace_name: String,
    /// Function/aggregate name
    pub function_name: String,
    /// Function type (FUNCTION, AGGREGATE)
    pub function_type: String,
    /// Argument types
    pub argument_types: Vec<String>,
    /// Return type
    pub return_type: String,
    /// Programming language (java, javascript etc.)
    pub language: Option<String>,
    /// Function body (for user functions)
    pub body: Option<String>,
    /// Called on null input
    pub called_on_null_input: bool,
    /// For aggregates: state function
    pub state_function: Option<String>,
    /// For aggregates: final function
    pub final_function: Option<String>,
    /// For aggregates: initial condition
    pub initial_condition: Option<String>,
}

/// Schema evolution tracking metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraSchemaEvolutionMetrics {
    /// Number of schema changes in the last 30 days
    pub recent_schema_changes: u64,
    /// Most recently created table
    pub newest_table: Option<String>,
    /// Most recently modified table
    pub recently_modified_table: Option<String>,
    /// Tables created in the last 7 days
    pub tables_created_recently: u64,
    /// Indexes created in the last 7 days
    pub indexes_created_recently: u64,
    /// Types created in the last 7 days
    pub types_created_recently: u64,
    /// Schema change frequency (changes per week)
    pub change_frequency_per_week: f64,
    /// Schema stability score (0-100, higher is more stable)
    pub stability_score: f64,
    /// Version drift between nodes (max days difference)
    pub version_drift_days: f64,
}

/// Schema health and best practices metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraSchemaHealthMetrics {
    /// Tables following naming conventions
    pub tables_with_good_naming: u64,
    /// Tables with appropriate partition key design
    pub tables_with_good_partition_design: u64,
    /// Tables with excessive wide rows potential
    pub tables_with_wide_row_risk: u64,
    /// Tables with missing secondary indexes
    pub tables_missing_useful_indexes: u64,
    /// Tables with potentially unused indexes
    pub tables_with_unused_indexes: u64,
    /// Tables with anti-patterns detected
    pub tables_with_anti_patterns: u64,
    /// Overall schema health score (0-100)
    pub health_score: f64,
    /// Best practices compliance percentage
    pub best_practices_compliance: f64,
    /// Number of deprecated features used
    pub deprecated_features_count: u64,
    /// Security issues detected (e.g., no auth tables)
    pub security_issues_count: u64,
}

/// Schema distribution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CassandraSchemaDistributionStats {
    pub keyspaces_by_table_count: HashMap<String, u64>,
    pub tables_by_column_count_ranges: HashMap<String, u64>,
    pub index_distribution: HashMap<String, u64>,
    pub type_usage_distribution: HashMap<String, u64>,
}

/// Summary statistics for schema information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraSchemaSummary {
    pub schema_agreement: bool,
    pub total_keyspaces: u64,
    pub total_tables: u64,
    pub total_indexes: u64,
    pub complexity_score: f64,
    pub complexity_rating: String,
    pub health_score: f64,
    pub health_rating: String,
    pub tables_needing_attention: u64,
    pub schema_versions_count: u64,
    pub has_critical_issues: bool,
}
