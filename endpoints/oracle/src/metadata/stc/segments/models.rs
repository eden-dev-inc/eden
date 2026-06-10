use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSegmentInfo {
    /// Total number of segments in the database
    pub total_segments: u64,
    /// Number of table segments
    pub table_segments: u64,
    /// Number of index segments
    pub index_segments: u64,
    /// Number of lob segments
    pub lob_segments: u64,
    /// Number of temporary segments
    pub temp_segments: u64,
    /// Total allocated space for all segments in bytes
    pub total_allocated_space: u64,
    /// Total used space for all segments in bytes
    pub total_used_space: u64,
    /// Total free space available in bytes
    pub total_free_space: u64,
    /// Space utilization percentage (used/allocated * 100)
    pub space_utilization_pct: f64,
    /// Number of segments with high fragmentation
    pub fragmented_segments_count: u64,
    /// Number of segments that grew in the last 24 hours
    pub growing_segments_count: u64,
    /// Total space allocated in the last 24 hours in bytes
    pub space_allocated_24h: u64,
    /// Largest segment size in bytes
    pub largest_segment_size: u64,
    /// Number of segments larger than 1GB
    pub large_segments_count: u64,
    /// Average extent size across all segments
    pub avg_extent_size: u64,
    /// Total number of extents in the database
    pub total_extents: u64,
    /// Number of tablespaces with space issues
    pub tablespaces_with_issues: u64,
    /// Percentage of tablespace space utilization
    pub tablespace_utilization_pct: f64,
    /// Number of segments with chained rows
    pub chained_segments_count: u64,
    /// Total waste due to fragmentation in bytes
    pub fragmentation_waste: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<OracleSegmentDetailedMetrics>,
}

/// Detailed segment metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSegmentDetailedMetrics {
    /// Top largest segments (only collected when space usage is high)
    pub largest_segments: Vec<OracleSegmentDetails>,
    /// Most fragmented segments (only collected when fragmentation detected)
    pub fragmented_segments: Option<Vec<OracleFragmentedSegment>>,
    /// Fast growing segments (collected when growth detected)
    pub growing_segments: Option<Vec<OracleGrowingSegment>>,
    /// Tablespace space issues (collected when space problems detected)
    pub tablespace_issues: Option<Vec<OracleTablespaceIssue>>,
    /// Segments with high row chaining (collected when performance issues detected)
    pub chained_segments: Option<Vec<OracleChainedSegment>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSegmentDetails {
    /// Schema owner of the segment
    pub owner: String,
    /// Name of the segment
    pub segment_name: String,
    /// Type of segment (TABLE, INDEX, LOB etc.)
    pub segment_type: String,
    /// Tablespace containing the segment
    pub tablespace_name: String,
    /// Total size in bytes
    pub bytes: u64,
    /// Number of extents
    pub extents: u64,
    /// Size of initial extent
    pub initial_extent: u64,
    /// Size of next extent
    pub next_extent: u64,
    /// Maximum number of extents allowed
    pub max_extents: Option<u64>,
    /// Size in megabytes
    pub size_mb: f64,
    /// Fragmentation level (LOW, MEDIUM, HIGH)
    pub fragmentation_level: String,
}

/// Fragmented segment information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleFragmentedSegment {
    /// Schema owner of the segment
    pub owner: String,
    /// Name of the segment
    pub segment_name: String,
    /// Type of segment
    pub segment_type: String,
    /// Tablespace containing the segment
    pub tablespace_name: String,
    /// Total size in bytes
    pub bytes: u64,
    /// Number of extents (high indicates fragmentation)
    pub extents: u64,
    /// Maximum number of extents allowed
    pub max_extents: Option<u64>,
    /// Size of initial extent
    pub initial_extent: u64,
    /// Size of next extent
    pub next_extent: u64,
    /// Average extent size
    pub avg_extent_size: u64,
    /// Estimated wasted space due to fragmentation (MB)
    pub wasted_space_mb: f64,
}

/// Growing segment information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleGrowingSegment {
    /// Schema owner of the segment
    pub owner: String,
    /// Name of the segment
    pub segment_name: String,
    /// Type of segment
    pub segment_type: String,
    /// Tablespace containing the segment
    pub tablespace_name: String,
    /// Current total size in bytes
    pub current_size: u64,
    /// Current number of extents
    pub current_extents: u64,
    /// Number of new extents allocated in last 24 hours
    pub new_extents_24h: u64,
    /// Growth in bytes in last 24 hours
    pub growth_bytes_24h: u64,
    /// Growth in MB in last 24 hours
    pub growth_mb_24h: f64,
}

/// Tablespace space issue information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleTablespaceIssue {
    /// Name of the tablespace
    pub tablespace_name: String,
    /// Total allocated space in MB
    pub total_size_mb: f64,
    /// Used space in MB
    pub used_size_mb: f64,
    /// Free space in MB
    pub free_size_mb: f64,
    /// Space usage percentage
    pub usage_pct: f64,
    /// Size of largest free extent in MB
    pub largest_free_mb: f64,
    /// Issue status (NORMAL, WARNING, CRITICAL)
    pub status: String,
}

/// Chained segment information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleChainedSegment {
    /// Schema owner of the table
    pub owner: String,
    /// Name of the table
    pub table_name: String,
    /// Tablespace containing the table
    pub tablespace_name: String,
    /// Number of rows in the table
    pub num_rows: u64,
    /// Number of chained rows
    pub chain_cnt: u64,
    /// Average row length
    pub avg_row_len: u64,
    /// Number of blocks used
    pub blocks: u64,
    /// Average free space per block
    pub avg_space: u64,
    /// Percentage of rows that are chained
    pub chain_pct: f64,
}

pub type SegmentHealthStatus = HealthStatus;

/// Overall health summary for Oracle segment management
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleSegmentHealthSummary {
    /// Space utilization health
    pub space_health: SegmentHealthStatus,
    /// Fragmentation health
    pub fragmentation_health: SegmentHealthStatus,
    /// Growth pattern health
    pub growth_health: SegmentHealthStatus,
    /// Tablespace health
    pub tablespace_health: SegmentHealthStatus,
    /// Performance health (chaining etc.)
    pub performance_health: SegmentHealthStatus,
}
