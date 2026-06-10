use super::*;

/// Large table information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseLargeTable {
    /// Database name
    pub database: String,
    /// Table name
    pub table_name: String,
    /// Table engine
    pub engine: String,
    /// Total bytes used
    pub total_bytes: u64,
    /// Total number of rows
    pub total_rows: u64,
    /// Uncompressed data size
    pub uncompressed_bytes: u64,
    /// Compressed data size
    pub compressed_bytes: u64,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Human-readable size
    pub readable_size: String,
    /// Partition key definition
    pub partition_key: Option<String>,
    /// Sorting key definition
    pub sorting_key: Option<String>,
    /// Primary key definition
    pub primary_key: Option<String>,
}

/// Table with poor compression ratio
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhousePoorCompressionTable {
    /// Database name
    pub database: String,
    /// Table name
    pub table_name: String,
    /// Table engine
    pub engine: String,
    /// Total bytes used
    pub total_bytes: u64,
    /// Total number of rows
    pub total_rows: u64,
    /// Uncompressed data size
    pub uncompressed_bytes: u64,
    /// Compressed data size
    pub compressed_bytes: u64,
    /// Current compression ratio
    pub compression_ratio: f64,
    /// Human-readable size
    pub readable_size: String,
    /// Current compression codec
    pub compression_codec: Option<String>,
    /// Potential space savings with better compression
    pub potential_savings: u64,
    /// Recommended compression codec
    pub recommended_codec: String,
}

/// Fragmented table information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseFragmentedTable {
    /// Database name
    pub database: String,
    /// Table name
    pub table_name: String,
    /// Number of parts in the table
    pub parts_count: u64,
    /// Total size of the table
    pub total_size: u64,
    /// Total number of rows
    pub total_rows: u64,
    /// Last modification time
    pub last_modification: Option<DateTimeWrapper>,
    /// Oldest partition date
    pub oldest_partition: Option<String>,
    /// Newest partition date
    pub newest_partition: Option<String>,
    /// Level of fragmentation
    pub fragmentation_level: FragmentationLevel,
    /// Urgency of optimization needed
    pub optimization_urgency: OptimizationUrgency,
}

/// Currently active merge operation
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseActiveMerge {
    /// Database name
    pub database: String,
    /// Table name
    pub table_name: String,
    /// Elapsed time in seconds
    pub elapsed_seconds: f64,
    /// Progress percentage (0.0 to 1.0)
    pub progress: f64,
    /// Number of parts being merged
    pub num_parts: u64,
    /// Name of the resulting part
    pub result_part_name: Option<String>,
    /// Bytes read (uncompressed)
    pub bytes_read_uncompressed: u64,
    /// Bytes written (uncompressed)
    pub bytes_written_uncompressed: u64,
    /// Rows read during merge
    pub rows_read: u64,
    /// Rows written during merge
    pub rows_written: u64,
    /// Columns written during merge
    pub columns_written: u64,
    /// Memory usage of the merge process
    pub memory_usage: u64,
    /// Thread ID performing the merge
    pub thread_id: u64,
    /// Estimated completion time in seconds
    pub estimated_completion_time: Option<f64>,
}

/// Failed merge operation information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseFailedMerge {
    /// Database name
    pub database: String,
    /// Table name
    pub table_name: String,
    /// Failure time
    pub failure_time: DateTimeWrapper,
    /// Exception message
    pub exception_message: String,
    /// Parts that were being merged
    pub parts_involved: Vec<String>,
    /// Bytes that were being processed
    pub bytes_being_processed: u64,
    /// Duration before failure
    pub duration_before_failure: f64,
    /// Failure category
    pub failure_category: MergeFailureCategory,
}

/// Table optimization candidate
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseOptimizationCandidate {
    /// Database name
    pub database: String,
    /// Table name
    pub table_name: String,
    /// Type of optimization needed
    pub optimization_type: OptimizationType,
    /// Description of current issue
    pub current_issue: String,
    /// Recommended action to take
    pub recommended_action: String,
    /// Expected benefit from optimization
    pub expected_benefit: String,
    /// Urgency of the optimization
    pub urgency: OptimizationUrgency,
    /// Estimated time for optimization in minutes
    pub estimated_time_minutes: u64,
    /// Potential space savings in bytes
    pub potential_space_savings: u64,
}

/// Database storage statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDatabaseStorageStats {
    /// Database name
    pub database: String,
    /// Number of tables in database
    pub table_count: u64,
    /// Total size of database
    pub total_size: u64,
    /// Total rows in database
    pub total_rows: u64,
    /// Average compression ratio
    pub avg_compression_ratio: f64,
    /// Total uncompressed size
    pub total_uncompressed: u64,
    /// Total compressed size
    pub total_compressed: u64,
    /// Human-readable size
    pub readable_size: String,
    /// Average table size
    pub avg_table_size: u64,
    /// Storage efficiency score
    pub storage_efficiency: f64,
}

/// Storage growth trend information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseStorageGrowthTrend {
    /// Database name
    pub database: String,
    /// Table name (optional, for table-level trends)
    pub table_name: Option<String>,
    /// Growth period in days
    pub period_days: u64,
    /// Size at start of period
    pub start_size: u64,
    /// Size at end of period
    pub end_size: u64,
    /// Growth rate as percentage
    pub growth_rate_percent: f64,
    /// Projected size in 30 days
    pub projected_size_30d: u64,
    /// Growth trend classification
    pub growth_trend: GrowthTrend,
}

/// Partition information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhousePartitionInfo {
    /// Database name
    pub database: String,
    /// Table name
    pub table_name: String,
    /// Partition identifier
    pub partition: String,
    /// Number of parts in partition
    pub parts_in_partition: u64,
    /// Size of the partition
    pub partition_size: u64,
    /// Number of rows in partition
    pub partition_rows: u64,
    /// Minimum date in partition
    pub partition_min_date: Option<String>,
    /// Maximum date in partition
    pub partition_max_date: Option<String>,
    /// Last modification time
    pub last_modified: Option<DateTimeWrapper>,
    /// Health status of the partition
    pub partition_health: PartitionHealth,
}

/// Storage efficiency analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseStorageEfficiencyAnalysis {
    /// Type of efficiency analysis
    pub analysis_type: EfficiencyAnalysisType,
    /// Name of the metric being analyzed
    pub metric_name: String,
    /// Current value of the metric
    pub current_value: f64,
    /// Optimal value for the metric
    pub optimal_value: f64,
    /// Efficiency score (0.0 to 1.0)
    pub efficiency_score: f64,
    /// Recommendations for improvement
    pub recommendations: Vec<String>,
    /// Impact level of the efficiency issue
    pub impact_level: EfficiencyImpactLevel,
}

/// Fragmentation level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum FragmentationLevel {
    /// Low fragmentation
    Low,
    /// Medium fragmentation
    Medium,
    /// High fragmentation requiring attention
    High,
    /// Critical fragmentation requiring immediate action
    Critical,
}

/// Optimization urgency classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum OptimizationUrgency {
    /// Low priority optimization
    Low,
    /// Medium priority optimization
    Medium,
    /// High priority optimization
    High,
    /// Critical optimization needed immediately
    Critical,
}

/// Type of optimization needed
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum OptimizationType {
    /// Defragmentation through part merging
    Defragmentation,
    /// Compression improvement
    CompressionImprovement,
    /// Partition optimization
    PartitionOptimization,
    /// Index optimization
    IndexOptimization,
    /// Schema optimization
    SchemaOptimization,
    /// Data archival
    DataArchival,
}

/// Merge failure category classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum MergeFailureCategory {
    /// Memory-related failure
    Memory,
    /// Disk space-related failure
    DiskSpace,
    /// Timeout-related failure
    Timeout,
    /// Corruption-related failure
    Corruption,
    /// Resource contention
    ResourceContention,
    /// Other/unknown failure
    Other,
}

/// Growth trend classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum GrowthTrend {
    /// Stable growth
    Stable,
    /// Moderate growth
    Moderate,
    /// Rapid growth requiring attention
    Rapid,
    /// Exponential growth requiring immediate action
    Exponential,
    /// Declining storage usage
    Declining,
}

/// Partition health classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum PartitionHealth {
    /// Partition is healthy
    Healthy,
    /// Partition has minor issues
    Warning,
    /// Partition has critical issues
    Critical,
}

/// Efficiency analysis type classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum EfficiencyAnalysisType {
    /// Overall storage efficiency
    Overall,
    /// Compression efficiency
    Compression,
    /// Fragmentation efficiency
    Fragmentation,
    /// Partition efficiency
    Partition,
    /// Query performance efficiency
    QueryPerformance,
}

/// Efficiency impact level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum EfficiencyImpactLevel {
    /// Low impact on efficiency
    Low,
    /// Medium impact on efficiency
    Medium,
    /// High impact on efficiency
    High,
    /// Critical impact on efficiency
    Critical,
}

/// Storage health status classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum StorageHealthStatus {
    /// Storage is healthy and well-optimized
    Healthy,
    /// Minor storage issues that should be monitored
    Attention,
    /// Storage issues that require investigation
    Warning,
    /// Critical storage issues requiring immediate attention
    Critical,
}

/// Storage utilization level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum StorageUtilizationLevel {
    /// Minimal storage usage
    Minimal,
    /// Low storage usage
    Low,
    /// Medium storage usage
    Medium,
    /// High storage usage
    High,
    /// Very high storage usage
    VeryHigh,
}

/// Storage maintenance burden classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum StorageMaintenanceBurden {
    /// Minimal maintenance required
    Minimal,
    /// Low maintenance burden
    Low,
    /// Medium maintenance burden
    Medium,
    /// High maintenance burden
    High,
    /// Very high maintenance burden
    VeryHigh,
}

/// Merge activity level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum MergeActivityLevel {
    /// No merge activity
    Idle,
    /// Low merge activity
    Low,
    /// Moderate merge activity
    Moderate,
    /// High merge activity
    High,
    /// Very high merge activity
    VeryHigh,
}

/// Storage performance impact classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum StoragePerformanceImpact {
    /// Minimal impact on performance
    Minimal,
    /// Low impact on performance
    Low,
    /// Medium impact on performance
    Medium,
    /// High impact on performance
    High,
    /// Critical impact on performance
    Critical,
}
