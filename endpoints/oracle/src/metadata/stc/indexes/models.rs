use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleIndexInfo {
    /// Schema owner of the index
    pub owner: String,
    /// Name of the index
    pub index_name: String,
    /// Name of the table the index belongs to
    pub table_name: String,
    /// Tablespace where the index is stored
    pub tablespace_name: String,
    /// Type of index (NORMAL, BITMAP, FUNCTION-BASED etc.)
    pub index_type: String,
    /// Uniqueness constraint (UNIQUE, NONUNIQUE)
    pub uniqueness: String,
    /// Index status (VALID, UNUSABLE, INVISIBLE etc.)
    pub status: String,
    /// Visibility (VISIBLE, INVISIBLE)
    pub visibility: String,
    /// Number of columns in the index
    pub column_count: u32,
    /// Comma-separated list of indexed columns
    pub column_names: String,
    /// Index creation date
    pub created: DateTimeWrapper,
    /// Last time the index was analyzed
    pub last_analyzed: Option<DateTimeWrapper>,

    // Size and Space Statistics
    /// Number of leaf blocks in the index
    pub leaf_blocks: u64,
    /// Number of distinct keys in the index
    pub distinct_keys: u64,
    /// Average leaf blocks per key
    pub avg_leaf_blocks_per_key: f64,
    /// Average data blocks per key
    pub avg_data_blocks_per_key: f64,
    /// Clustering factor (how well ordered the index is relative to table data)
    pub clustering_factor: u64,
    /// Number of rows in the indexed table
    pub num_rows: u64,
    /// Sample size used for statistics
    pub sample_size: u64,
    /// Compression enabled (ENABLED, DISABLED)
    pub compression: String,
    /// Prefix length for compressed indexes
    pub prefix_length: u32,

    // Usage Statistics
    /// Number of times the index has been used for table access
    pub table_scans: u64,
    /// Number of times the index has been used for index scans
    pub index_scans: u64,
    /// Number of times the index has been used for index lookups
    pub index_lookups: u64,
    /// Total number of times the index has been accessed
    pub total_access_count: u64,
    /// Last time the index was used
    pub last_used: Option<DateTimeWrapper>,
    /// Usage frequency score (0-100)
    pub usage_score: f64,

    // Performance Metrics
    /// B-tree height of the index
    pub blevel: u32,
    /// Selectivity of the index (0.0 to 1.0)
    pub selectivity: f64,
    /// Index efficiency ratio
    pub efficiency_ratio: f64,
    /// Average I/O cost for index access
    pub avg_io_cost: f64,
    /// CPU cost for index access
    pub cpu_cost: u64,

    // Space and Storage
    /// Size of the index in bytes
    pub index_size_bytes: u64,
    /// Number of extents allocated to the index
    pub extents: u32,
    /// Initial extent size in bytes
    pub initial_extent: u64,
    /// Next extent size in bytes
    pub next_extent: u64,
    /// Maximum number of extents allowed
    pub max_extents: u32,
    /// Percentage increase for next extent
    pub pct_increase: u32,
    /// Free space percentage in index blocks
    pub pct_free: u32,

    // Maintenance and Health
    /// Fragmentation level (0.0 to 100.0)
    pub fragmentation_level: f64,
    /// Whether the index needs rebuilding
    pub needs_rebuild: bool,
    /// Reason for rebuild recommendation
    pub rebuild_reason: Option<String>,
    /// Estimated space savings from rebuild (bytes)
    pub rebuild_space_savings: u64,
    /// Whether statistics are stale
    pub stale_statistics: bool,
    /// Whether the index is a candidate for dropping (unused)
    pub drop_candidate: bool,

    // Partition Information (for partitioned indexes)
    /// Whether the index is partitioned
    pub is_partitioned: bool,
    /// Partitioning type (RANGE, HASH, LIST etc.)
    pub partitioning_type: Option<String>,
    /// Number of partitions
    pub partition_count: u32,
    /// Partition details (if partitioned)
    pub partitions: Vec<OracleIndexPartitionInfo>,

    // Collection metadata
    pub collection_timestamp: DateTimeWrapper,
}
