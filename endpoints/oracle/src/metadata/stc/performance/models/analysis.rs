use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PerformanceAnalysis {
    /// Overall performance score (0-100)
    pub overall_score: f64,
    /// CPU performance score
    pub cpu_score: f64,
    /// Memory performance score
    pub memory_score: f64,
    /// I/O performance score
    pub io_score: f64,
    /// SQL performance score
    pub sql_score: f64,
    /// Wait events analysis
    pub wait_events_analysis: WaitEventsAnalysis,
    /// Resource bottlenecks
    pub bottlenecks: Vec<PerformanceBottleneck>,
    /// Performance trends
    pub trends: Vec<PerformanceTrend>,
    /// Key performance indicators
    pub kpis: PerformanceKpis,
}

/// Wait events analysis
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct WaitEventsAnalysis {
    /// Top wait events by time
    pub top_wait_events: Vec<String>,
    /// Wait classes distribution
    pub wait_classes_distribution: HashMap<String, f64>,
    /// Critical wait events
    pub critical_wait_events: Vec<String>,
    /// Wait events trend
    pub wait_trend: WaitEventsTrend,
}

/// Performance bottleneck identification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PerformanceBottleneck {
    /// Bottleneck type
    pub bottleneck_type: BottleneckType,
    /// Severity level
    pub severity: BottleneckSeverity,
    /// Description
    pub description: String,
    /// Impact assessment
    pub impact: String,
    /// Recommendation
    pub recommendation: String,
    /// Affected components
    pub affected_components: Vec<String>,
    /// Metrics
    pub metrics: HashMap<String, f64>,
}

/// Performance trend information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PerformanceTrend {
    /// Metric name
    pub metric_name: String,
    /// Current value
    pub current_value: f64,
    /// Previous value
    pub previous_value: f64,
    /// Change percentage
    pub change_pct: f64,
    /// Trend direction
    pub trend_direction: TrendDirection,
    /// Trend severity
    pub trend_severity: TrendSeverity,
}

/// Key Performance Indicators
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PerformanceKpis {
    /// Average response time (ms)
    pub avg_response_time_ms: f64,
    /// Transactions per second
    pub transactions_per_second: f64,
    /// SQL executions per second
    pub sql_executions_per_second: f64,
    /// Error rate percentage
    pub error_rate_pct: f64,
    /// Availability percentage
    pub availability_pct: f64,
    /// Resource utilization percentage
    pub resource_utilization_pct: f64,
    /// Concurrency level
    pub concurrency_level: f64,
    /// Throughput (operations/sec)
    pub throughput_ops_per_sec: f64,
}

/// Performance trends over time
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PerformanceTrends {
    /// CPU utilization trend
    pub cpu_trend: Vec<TrendDataPoint>,
    /// Memory utilization trend
    pub memory_trend: Vec<TrendDataPoint>,
    /// I/O throughput trend
    pub io_trend: Vec<TrendDataPoint>,
    /// Response time trend
    pub response_time_trend: Vec<TrendDataPoint>,
    /// Session count trend
    pub session_count_trend: Vec<TrendDataPoint>,
    /// Wait events trend
    pub wait_events_trend: HashMap<String, Vec<TrendDataPoint>>,
}

/// Trend data point
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct TrendDataPoint {
    /// Timestamp
    pub timestamp: DateTimeWrapper,
    /// Value
    pub value: f64,
    /// Metadata
    pub metadata: HashMap<String, String>,
}

/// Performance recommendation
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PerformanceRecommendation {
    /// Recommendation category
    pub category: RecommendationCategory,
    /// Priority level
    pub priority: RecommendationPriority,
    /// Title
    pub title: String,
    /// Description
    pub description: String,
    /// Rationale
    pub rationale: String,
    /// Expected benefit
    pub expected_benefit: String,
    /// Implementation effort
    pub implementation_effort: ImplementationEffort,
    /// Risk level
    pub risk_level: RiskLevel,
    /// Affected metrics
    pub affected_metrics: Vec<String>,
    /// Action items
    pub action_items: Vec<String>,
}

/// Performance alert
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PerformanceAlert {
    /// Alert type
    pub alert_type: AlertType,
    /// Severity level
    pub severity: AlertSeverity,
    /// Alert message
    pub message: String,
    /// Metric name
    pub metric_name: String,
    /// Current value
    pub current_value: f64,
    /// Threshold value
    pub threshold_value: f64,
    /// Duration (seconds)
    pub duration_seconds: u64,
    /// First occurrence
    pub first_occurrence: DateTimeWrapper,
    /// Recommended action
    pub recommended_action: String,
}

/// Collection metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CollectionMetadata {
    /// Collection duration (milliseconds)
    pub collection_duration_ms: u64,
    /// Number of queries executed
    pub queries_executed: u32,
    /// Data quality score
    pub data_quality_score: f64,
    /// Collection warnings
    pub warnings: Vec<String>,
    /// Collection errors
    pub errors: Vec<String>,
}
