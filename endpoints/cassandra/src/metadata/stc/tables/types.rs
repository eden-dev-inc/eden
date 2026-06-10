use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Cassandra table information and analytics
///
/// Covers table-level metrics: structure analysis, performance characteristics,
/// storage usage and operational health.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableInfo {
    /// Total number of tables across all keyspaces
    pub total_tables: u64,
    /// Number of user tables (excluding system tables)
    pub user_tables: u64,
    /// Number of system tables
    pub system_tables: u64,
    /// Total storage used by all tables (GB)
    pub total_storage_gb: f64,
    /// Average table size (GB)
    pub avg_table_size_gb: f64,
    /// Largest table size (GB)
    pub largest_table_size_gb: f64,
    /// Number of tables with no data
    pub empty_tables: u64,
    /// Number of tables with potential issues
    pub tables_with_issues: u64,
    /// Average number of columns per table
    pub avg_columns_per_table: f64,
    /// Average partition size across all tables (KB)
    pub avg_partition_size_kb: f64,
    /// Total number of SSTables across all tables
    pub total_sstables: u64,
    /// Detailed table information
    pub table_details: Vec<CassandraTableDetail>,
    /// Table storage distribution
    pub storage_distribution: CassandraTableStorageDistribution,
    /// Table performance metrics
    pub performance_metrics: CassandraTablePerformanceMetrics,
    /// Table health and design metrics
    pub health_metrics: CassandraTableHealthMetrics,
    /// Compaction and maintenance metrics
    pub maintenance_metrics: CassandraTableMaintenanceMetrics,
}

/// Detailed information about a specific table
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableDetail {
    /// Keyspace name
    pub keyspace_name: String,
    /// Table name
    pub table_name: String,
    /// Table ID
    pub table_id: String,
    /// Table type (USER, SYSTEM)
    pub table_type: String,
    /// Column information
    pub column_info: CassandraTableColumnInfo,
    /// Storage metrics
    pub storage_metrics: CassandraTableStorageMetrics,
    /// Performance metrics
    pub performance_metrics: CassandraTablePerformanceDetail,
    /// Configuration settings
    pub configuration: CassandraTableConfiguration,
    /// Index information
    pub indexes: Vec<CassandraTableIndex>,
    /// Materialized views based on this table
    pub materialized_views: Vec<String>,
    /// Table health indicators
    pub health_indicators: CassandraTableHealthIndicators,
    /// Last maintenance operations
    pub maintenance_info: CassandraTableMaintenanceInfo,
    /// Table creation timestamp
    pub created_at: Option<String>,
    /// Last modified timestamp
    pub last_modified: Option<String>,
}

/// Column structure information for a table
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableColumnInfo {
    /// Total number of columns
    pub total_columns: u64,
    /// Partition key columns
    pub partition_key_columns: Vec<CassandraTableColumn>,
    /// Clustering key columns
    pub clustering_key_columns: Vec<CassandraTableColumn>,
    /// Static columns
    pub static_columns: Vec<CassandraTableColumn>,
    /// Regular columns
    pub regular_columns: Vec<CassandraTableColumn>,
    /// Number of collection columns (maps, sets, lists)
    pub collection_columns: u64,
    /// Number of UDT columns
    pub udt_columns: u64,
    /// Partition key complexity score
    pub partition_key_complexity: f64,
    /// Clustering key complexity score
    pub clustering_key_complexity: f64,
}

/// Individual column information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableColumn {
    /// Column name
    pub name: String,
    /// Column type
    pub data_type: String,
    /// Column kind (partition_key, clustering, static, regular)
    pub kind: String,
    /// Position in key (if applicable)
    pub position: Option<u64>,
    /// Clustering order (ASC/DESC)
    pub clustering_order: Option<String>,
    /// Is collection type
    pub is_collection: bool,
    /// Is user-defined type
    pub is_udt: bool,
}

/// Storage metrics for a table
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableStorageMetrics {
    /// Total data size including all replicas (GB)
    pub total_size_gb: f64,
    /// Logical data size before replication (GB)
    pub logical_size_gb: f64,
    /// Compressed data size (GB)
    pub compressed_size_gb: f64,
    /// Number of SSTables
    pub sstable_count: u64,
    /// Average SSTable size (MB)
    pub avg_sstable_size_mb: f64,
    /// Largest SSTable size (MB)
    pub largest_sstable_size_mb: f64,
    /// Estimated number of partitions
    pub estimated_partitions: u64,
    /// Average partition size (KB)
    pub avg_partition_size_kb: f64,
    /// Largest partition size (MB)
    pub largest_partition_size_mb: f64,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Bloom filter size (MB)
    pub bloom_filter_size_mb: f64,
    /// Index size (MB)
    pub index_size_mb: f64,
    /// Data growth rate (GB per day). Always 0.0; requires historical data not available here.
    pub growth_rate_gb_per_day: f64,
}

/// Performance metrics for a table
///
/// Fields sourced from `system_schema` and `system.size_estimates`.
/// JMX-only metrics (ops/sec, latencies, cache ratio) are always 0/false
/// because no JMX source is available in this collector.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTablePerformanceDetail {
    /// Read operations per second (0, JMX not available)
    pub read_ops_per_sec: f64,
    /// Write operations per second (0, JMX not available)
    pub write_ops_per_sec: f64,
    /// Average read latency ms (0, JMX not available)
    pub avg_read_latency_ms: f64,
    /// Average write latency ms (0, JMX not available)
    pub avg_write_latency_ms: f64,
    /// 95th percentile read latency ms (0, JMX not available)
    pub p95_read_latency_ms: f64,
    /// 95th percentile write latency ms (0, JMX not available)
    pub p95_write_latency_ms: f64,
    /// Cache hit ratio percentage (0, JMX not available)
    pub cache_hit_ratio_pct: f64,
    /// Bloom filter hit ratio percentage (0, JMX not available)
    pub bloom_filter_hit_ratio_pct: f64,
    /// Number of read timeouts (0, JMX not available)
    pub read_timeouts: u64,
    /// Number of write timeouts (0, JMX not available)
    pub write_timeouts: u64,
    /// Tombstone ratio percentage (0, JMX not available)
    pub tombstone_ratio_pct: f64,
    /// Hot partition detection (false, JMX not available)
    pub has_hot_partitions: bool,
    /// Performance score (0, not computable without JMX data)
    pub performance_score: f64,
}

/// Table configuration settings
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableConfiguration {
    /// Compaction strategy
    pub compaction_strategy: String,
    /// Compaction strategy options
    pub compaction_options: HashMap<String, String>,
    /// Compression algorithm
    pub compression_algorithm: String,
    /// Compression options
    pub compression_options: HashMap<String, String>,
    /// Caching configuration
    pub caching_config: HashMap<String, String>,
    /// Bloom filter false positive chance
    pub bloom_filter_fp_chance: f64,
    /// Default TTL (seconds)
    pub default_ttl: Option<u64>,
    /// GC grace seconds
    pub gc_grace_seconds: u64,
    /// Min index interval
    pub min_index_interval: u64,
    /// Max index interval
    pub max_index_interval: u64,
    /// CRC check chance
    pub crc_check_chance: f64,
    /// Table comment
    pub comment: Option<String>,
}

/// Index information for a table
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableIndex {
    /// Index name
    pub index_name: String,
    /// Index type (SECONDARY, CUSTOM)
    pub index_type: String,
    /// Target column
    pub target_column: String,
    /// Index options
    pub options: HashMap<String, String>,
    /// Is index built and ready
    pub is_ready: bool,
    /// Index size estimate (MB)
    pub estimated_size_mb: f64,
}

/// Health indicators for a table
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableHealthIndicators {
    /// Overall health score (0-100)
    pub health_score: f64,
    /// Has design issues
    pub has_design_issues: bool,
    /// Has performance issues
    pub has_performance_issues: bool,
    /// Has storage issues
    pub has_storage_issues: bool,
    /// Number of wide partitions detected
    pub wide_partitions_count: u64,
    /// Number of large partitions (>100MB)
    pub large_partitions_count: u64,
    /// Excessive tombstone ratio
    pub high_tombstone_ratio: bool,
    /// Poor compaction efficiency
    pub poor_compaction_efficiency: bool,
    /// Suboptimal compression
    pub suboptimal_compression: bool,
    /// Missing beneficial indexes
    pub missing_indexes: bool,
    /// Unused indexes detected
    pub unused_indexes: bool,
}

/// Maintenance operation information
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableMaintenanceInfo {
    /// Last compaction timestamp (from system.compaction_history)
    pub last_compaction: Option<String>,
    /// Last repair timestamp
    pub last_repair: Option<String>,
    /// Last snapshot timestamp
    pub last_snapshot: Option<String>,
    /// Pending compactions (0, not available without JMX)
    pub pending_compactions: u64,
    /// Active compactions (0, not available without JMX)
    pub active_compactions: u64,
    /// Compaction efficiency ratio (0.0, not available without JMX)
    pub compaction_efficiency: f64,
    /// Days since last major compaction
    pub days_since_major_compaction: f64,
    /// Needs maintenance attention
    pub needs_maintenance: bool,
}

/// Storage distribution across tables
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableStorageDistribution {
    /// Storage by keyspace
    pub storage_by_keyspace: HashMap<String, f64>,
    /// Storage by table size ranges
    pub storage_by_size_ranges: HashMap<String, u64>,
    /// Storage by compaction strategy
    pub storage_by_compaction_strategy: HashMap<String, f64>,
    /// Storage by compression algorithm
    pub storage_by_compression: HashMap<String, f64>,
    /// Top 10 largest tables
    pub largest_tables: Vec<String>,
    /// Tables with highest growth rates
    pub fastest_growing_tables: Vec<String>,
}

/// Aggregate performance metrics
///
/// All ops/latency fields are 0 because no JMX source is available.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTablePerformanceMetrics {
    /// Total read operations per second (0, JMX not available)
    pub total_read_ops_per_sec: f64,
    /// Total write operations per second (0, JMX not available)
    pub total_write_ops_per_sec: f64,
    /// Average read latency across all tables ms (0, JMX not available)
    pub avg_read_latency_ms: f64,
    /// Average write latency across all tables ms (0, JMX not available)
    pub avg_write_latency_ms: f64,
    /// Tables with high latency (0, JMX not available)
    pub high_latency_tables: u64,
    /// Tables with poor cache performance (0, JMX not available)
    pub poor_cache_performance_tables: u64,
    /// Tables with hot partitions (0, JMX not available)
    pub tables_with_hot_partitions: u64,
    /// Average performance score (0, not computable without JMX data)
    pub avg_performance_score: f64,
}

/// Health metrics across all tables
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableHealthMetrics {
    /// Tables with design issues
    pub tables_with_design_issues: u64,
    /// Tables with performance issues
    pub tables_with_performance_issues: u64,
    /// Tables with storage issues
    pub tables_with_storage_issues: u64,
    /// Tables with wide partitions
    pub tables_with_wide_partitions: u64,
    /// Tables with high tombstone ratios
    pub tables_with_high_tombstones: u64,
    /// Tables with suboptimal compression
    pub tables_with_poor_compression: u64,
    /// Tables missing beneficial indexes
    pub tables_missing_indexes: u64,
    /// Tables with unused indexes
    pub tables_with_unused_indexes: u64,
    /// Overall table health score
    pub overall_health_score: f64,
}

/// Maintenance metrics across all tables
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableMaintenanceMetrics {
    /// Tables needing immediate maintenance
    pub tables_needing_maintenance: u64,
    /// Total pending compactions (0, not available without JMX)
    pub total_pending_compactions: u64,
    /// Total active compactions (0, not available without JMX)
    pub total_active_compactions: u64,
    /// Average compaction efficiency (0.0, not available without JMX)
    pub avg_compaction_efficiency: f64,
    /// Tables overdue for compaction
    pub tables_overdue_compaction: u64,
    /// Tables without recent snapshots
    pub tables_without_snapshots: u64,
    /// Average days since last maintenance
    pub avg_days_since_maintenance: f64,
}

/// Table distribution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CassandraTableDistributionStats {
    pub tables_by_keyspace: HashMap<String, u64>,
    pub tables_by_size_ranges: HashMap<String, u64>,
    pub tables_by_column_count: HashMap<String, u64>,
    pub tables_by_compaction_strategy: HashMap<String, u64>,
    pub tables_by_health_score: HashMap<String, u64>,
}

/// Summary statistics for table information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraTableSummary {
    pub total_tables: u64,
    pub user_tables: u64,
    pub total_storage_gb: f64,
    pub avg_table_size_gb: f64,
    pub tables_with_issues: u64,
    pub health_score: f64,
    pub health_rating: String,
    pub performance_score: f64,
    pub storage_efficiency_score: f64,
    pub tables_needing_maintenance: u64,
    pub has_critical_issues: bool,
}
