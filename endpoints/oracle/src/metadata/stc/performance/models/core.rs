use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
/// Oracle database performance statistics and analysis.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OraclePerformanceStats {
    /// System statistics
    pub system_stats: SystemStatistics,
    /// Wait event statistics
    pub wait_events: Vec<WaitEventStat>,
    /// SQL performance metrics
    pub sql_performance: SqlPerformanceMetrics,
    /// Memory utilization
    pub memory_utilization: MemoryUtilization,
    /// I/O statistics
    pub io_statistics: IoStatistics,
    /// Session statistics
    pub session_statistics: SessionStatistics,
    /// Performance analysis
    pub performance_analysis: PerformanceAnalysis,
    /// Alert conditions
    pub alerts: Vec<PerformanceAlert>,
    /// Collection timestamp
    pub collection_timestamp: DateTimeWrapper,
}

/// Overall performance statistics collection
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OraclePerformanceStatsCollection {
    /// Performance statistics
    pub stats: OraclePerformanceStats,
    /// Historical trend data
    pub trends: PerformanceTrends,
    /// Performance recommendations
    pub recommendations: Vec<PerformanceRecommendation>,
    /// System health score
    pub health_score: f64,
    /// Collection metadata
    pub collection_metadata: CollectionMetadata,
    /// Collection timestamp
    pub collection_timestamp: DateTimeWrapper,
}
