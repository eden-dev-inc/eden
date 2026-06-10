use crate::api::lib::QueryInput;
use borsh::{BorshDeserialize, BorshSerialize};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

mod core_sync;
mod detailed_sync;
mod parsers;

/// Clickhouse replication information and cluster synchronization metrics.
///
/// Covers replica health and replication lag.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseReplicationInfo {
    /// Total number of replicated tables
    pub total_replicated_tables: u64,
    /// Number of tables that are fully synchronized
    pub synchronized_tables: u64,
    /// Number of tables with replication lag
    pub lagging_tables: u64,
    /// Number of tables with replication errors
    pub tables_with_errors: u64,
    /// Number of readonly tables (unable to accept writes)
    pub readonly_tables: u64,
    /// Total entries in replication queue across all tables
    pub total_queue_size: u64,
    /// Number of replication queue entries currently being processed
    pub active_queue_entries: u64,
    /// Maximum replication lag across all tables in seconds
    pub max_replication_lag: f64,
    /// Average replication lag across all tables in seconds
    pub avg_replication_lag: f64,
    /// Number of replicas that are out of sync
    pub out_of_sync_replicas: u64,
    /// Number of failed replication operations in the last hour
    pub failed_operations_last_hour: u64,
    /// Total number of zookeeper sessions
    pub total_zookeeper_sessions: u64,
    /// Number of active zookeeper sessions
    pub active_zookeeper_sessions: u64,
    /// Average queue processing time in seconds
    pub avg_queue_processing_time: f64,
    /// Number of tables in recovery mode
    pub tables_in_recovery: u64,
    /// Detailed metrics collected when problems are detected
    pub detailed_metrics: Option<ClickhouseReplicationDetailedMetrics>,
}

/// Detailed replication metrics collected when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseReplicationDetailedMetrics {
    /// Tables with high replication lag
    pub high_lag_tables: Vec<ClickhouseHighLagTable>,
    /// Tables with replication errors
    pub error_tables: Vec<ClickhouseReplicationError>,
    /// Readonly tables unable to accept writes
    pub readonly_table_details: Vec<ClickhouseReadonlyTable>,
    /// Large replication queue entries
    pub large_queue_entries: Vec<ClickhouseLargeQueueEntry>,
    /// Failed replication operations
    pub failed_operations: Vec<ClickhouseFailedReplication>,
    /// Replica synchronization status
    pub replica_sync_status: Vec<ClickhouseReplicaStatus>,
    /// Zookeeper connection details
    pub zookeeper_status: Vec<ClickhouseZookeeperStatus>,
    /// Recovery operations in progress
    pub recovery_operations: Vec<ClickhouseRecoveryOperation>,
}

impl MetadataCollection for ClickhouseReplicationInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                Self::QUERY_REPLICATION_OVERVIEW,
                query(
                    "SELECT
                    count() as total_replicated_tables,
                    countIf(is_session_expired = 0 AND queue_size = 0) as synchronized_tables,
                    countIf(absolute_delay > 60) as lagging_tables,
                    countIf(last_queue_update_exception != '') as tables_with_errors,
                    countIf(is_readonly = 1) as readonly_tables,
                    sum(queue_size) as total_queue_size,
                    sumIf(queue_size, inserts_in_queue > 0 OR merges_in_queue > 0) as active_queue_entries,
                    max(absolute_delay) as max_replication_lag,
                    avg(absolute_delay) as avg_replication_lag
                FROM system.replicas"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_REPLICA_STATUS,
                query(
                    "SELECT
                    countIf(is_session_expired = 1) as out_of_sync_replicas,
                    countIf(is_readonly = 1 AND last_queue_update_exception LIKE '%recovery%') as tables_in_recovery
                FROM system.replicas"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_ZOOKEEPER_SESSIONS,
                query(
                    "SELECT
                    count(DISTINCT zookeeper_path) as total_zookeeper_sessions,
                    countIf(is_session_expired = 0) as active_zookeeper_sessions
                FROM system.replicas"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_QUEUE_PERFORMANCE,
                query(
                    "SELECT
                    0 as avg_queue_processing_time
                FROM system.replicas
                LIMIT 1"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_RECENT_FAILURES,
                query(
                    "SELECT
                    countIf(last_queue_update_exception != '') as failed_operations_last_hour
                FROM system.replicas"
                        .to_string(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential Clickhouse replication and cluster synchronization metrics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "replication"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use crate::metadata::stc::utils::{query, query_map};
use function_name::named;
use std::time::Duration;

impl ClickhouseReplicationInfo {
    const QUERY_REPLICATION_OVERVIEW: &'static str = "replication_overview";
    const QUERY_REPLICA_STATUS: &'static str = "replica_status";
    const QUERY_ZOOKEEPER_SESSIONS: &'static str = "zookeeper_sessions";
    const QUERY_QUEUE_PERFORMANCE: &'static str = "queue_performance";
    const QUERY_RECENT_FAILURES: &'static str = "recent_failures";
    const DETAIL_QUERY_HIGH_LAG_TABLES: &'static str = "high_lag_tables";
    const DETAIL_QUERY_ERROR_TABLES: &'static str = "error_tables";
    const DETAIL_QUERY_READONLY_TABLES: &'static str = "readonly_tables";
    const DETAIL_QUERY_LARGE_QUEUE: &'static str = "large_queue";
    const DETAIL_QUERY_FAILED_OPERATIONS: &'static str = "failed_operations";
    const DETAIL_QUERY_REPLICA_STATUS: &'static str = "replica_status";
    const DETAIL_QUERY_ZOOKEEPER_STATUS: &'static str = "zookeeper_status";
    const DETAIL_QUERY_RECOVERY_OPERATIONS: &'static str = "recovery_operations";
    const HIGH_LAG_THRESHOLD: f64 = 300.0; // 5 minutes
    const LARGE_QUEUE_THRESHOLD: u64 = 100;
    // Threshold reserved for future health-check reporting
    #[allow(dead_code)]
    const LONG_RECOVERY_THRESHOLD: f64 = 3600.0; // 1 hour
    const QUERY_TIMEOUT: Duration = Duration::from_secs(8);
    const MAX_DETAILED_RESULTS: usize = 50;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: ClickhouseAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        core_sync::sync_metadata(self, context).await
    }

    fn should_collect_detailed_metrics(core_info: &ClickhouseReplicationInfo) -> bool {
        core_info.max_replication_lag > Self::HIGH_LAG_THRESHOLD
            || core_info.tables_with_errors > 0
            || core_info.readonly_tables > 0
            || core_info.total_queue_size > Self::LARGE_QUEUE_THRESHOLD
            || core_info.failed_operations_last_hour > 0
            || core_info.out_of_sync_replicas > 0
            || core_info.tables_in_recovery > 0
    }
}

/// Table with high replication lag
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseHighLagTable {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Replica name
    pub replica_name: String,
    /// Replication delay in seconds
    pub absolute_delay: f64,
    /// Maximum log index
    pub log_max_index: u64,
    /// Current log pointer
    pub log_pointer: u64,
    /// Queue size
    pub queue_size: u64,
    /// Number of inserts in queue
    pub inserts_in_queue: u64,
    /// Number of merges in queue
    pub merges_in_queue: u64,
    /// Last queue update time
    pub last_queue_update: Option<DateTimeWrapper>,
    /// Whether ZooKeeper session is expired
    pub is_session_expired: bool,
    /// ZooKeeper path
    pub zookeeper_path: String,
    /// Number of active replicas
    pub active_replicas: u64,
    /// Total number of replicas
    pub total_replicas: u64,
}

/// Replication error information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseReplicationError {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Replica name
    pub replica_name: String,
    /// Last exception message
    pub last_exception: String,
    /// When the exception occurred
    pub last_exception_time: Option<DateTimeWrapper>,
    /// Current queue size
    pub queue_size: u64,
    /// Replication delay in seconds
    pub absolute_delay: f64,
    /// Whether replica is readonly
    pub is_readonly: bool,
    /// Whether ZooKeeper session is expired
    pub is_session_expired: bool,
    /// ZooKeeper path
    pub zookeeper_path: String,
    /// Replica path
    pub replica_path: String,
    /// Maximum log index
    pub log_max_index: u64,
    /// Current log pointer
    pub log_pointer: u64,
}

/// Readonly table information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseReadonlyTable {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Replica name
    pub replica_name: String,
    /// Last exception if any
    pub last_exception: Option<String>,
    /// When the exception occurred
    pub last_exception_time: Option<DateTimeWrapper>,
    /// Replication delay in seconds
    pub absolute_delay: f64,
    /// Current queue size
    pub queue_size: u64,
    /// Whether ZooKeeper session is expired
    pub is_session_expired: bool,
    /// ZooKeeper path
    pub zookeeper_path: String,
    /// Maximum log index
    pub log_max_index: u64,
    /// Current log pointer
    pub log_pointer: u64,
    /// Number of active replicas
    pub active_replicas: u64,
    /// Total number of replicas
    pub total_replicas: u64,
}

/// Large replication queue entry
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseLargeQueueEntry {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Operation type
    pub operation_type: String,
    /// When the operation was created
    pub create_time: DateTimeWrapper,
    /// Required quorum for the operation
    pub required_quorum: u64,
    /// Source replica
    pub source_replica: Option<String>,
    /// New part name being created
    pub new_part_name: Option<String>,
    /// Parts being merged
    pub parts_to_merge: Option<String>,
    /// Whether currently executing
    pub is_currently_executing: bool,
    /// Number of execution attempts
    pub num_tries: u64,
    /// Last attempt time
    pub last_attempt_time: Option<DateTimeWrapper>,
    /// Last exception if any
    pub last_exception: Option<String>,
    /// Reason for postponement
    pub postpone_reason: Option<String>,
}

/// Failed replication operation
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseFailedReplication {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Replica name
    pub replica_name: String,
    /// Exception message
    pub last_exception: String,
    /// When the failure occurred
    pub last_exception_time: DateTimeWrapper,
    /// Current queue size
    pub queue_size: u64,
    /// Replication delay
    pub absolute_delay: f64,
    /// Last queue update time
    pub last_queue_update: Option<DateTimeWrapper>,
    /// ZooKeeper path
    pub zookeeper_path: String,
}

/// Replica synchronization status
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseReplicaStatus {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Replica name
    pub replica_name: String,
    /// Whether this replica is the leader
    pub is_leader: bool,
    /// Whether replica is readonly
    pub is_readonly: bool,
    /// Whether ZooKeeper session is expired
    pub is_session_expired: bool,
    /// Replication delay in seconds
    pub absolute_delay: f64,
    /// Current queue size
    pub queue_size: u64,
    /// Number of active replicas
    pub active_replicas: u64,
    /// Total number of replicas
    pub total_replicas: u64,
    /// ZooKeeper path
    pub zookeeper_path: String,
    /// Replica path
    pub replica_path: String,
    /// Maximum log index
    pub log_max_index: u64,
    /// Current log pointer
    pub log_pointer: u64,
    /// Last queue update time
    pub last_queue_update: Option<DateTimeWrapper>,
}

/// ZooKeeper cluster status
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseZookeeperStatus {
    /// ZooKeeper path
    pub zookeeper_path: String,
    /// Total number of replicas
    pub replica_count: u64,
    /// Number of active replicas
    pub active_replicas: u64,
    /// Number of readonly replicas
    pub readonly_replicas: u64,
    /// Maximum lag among replicas
    pub max_lag: f64,
    /// Total queue size across replicas
    pub total_queue_size: u64,
    /// Number of replicas with errors
    pub error_count: u64,
}

/// Recovery operation information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseRecoveryOperation {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Replica name
    pub replica_name: String,
    /// Recovery exception message
    pub last_exception: String,
    /// When recovery started
    pub last_exception_time: DateTimeWrapper,
    /// Current replication delay
    pub absolute_delay: f64,
    /// Current queue size
    pub queue_size: u64,
    /// Whether replica is readonly
    pub is_readonly: bool,
    /// Whether ZooKeeper session is expired
    pub is_session_expired: bool,
    /// ZooKeeper path
    pub zookeeper_path: String,
    /// Duration of recovery in seconds
    pub recovery_duration: f64,
}

impl ClickhouseReplicationInfo {
    /// Checks if there are tables with high replication lag
    pub fn has_high_replication_lag(&self, threshold_seconds: f64) -> bool {
        self.max_replication_lag > threshold_seconds
    }

    /// Checks if there are replication errors
    pub fn has_replication_errors(&self) -> bool {
        self.tables_with_errors > 0
    }

    /// Checks if there are readonly tables
    pub fn has_readonly_tables(&self) -> bool {
        self.readonly_tables > 0
    }

    /// Checks if there's a large replication queue
    pub fn has_large_replication_queue(&self, threshold: u64) -> bool {
        self.total_queue_size > threshold
    }

    /// Checks if there are out of sync replicas
    pub fn has_out_of_sync_replicas(&self) -> bool {
        self.out_of_sync_replicas > 0
    }

    /// Checks if there are tables in recovery mode
    pub fn has_tables_in_recovery(&self) -> bool {
        self.tables_in_recovery > 0
    }

    /// Checks if there were recent replication failures
    pub fn has_recent_failures(&self) -> bool {
        self.failed_operations_last_hour > 0
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets replication synchronization rate (0.0 to 1.0)
    pub fn get_synchronization_rate(&self) -> f64 {
        if self.total_replicated_tables == 0 {
            return 0.0;
        }
        self.synchronized_tables as f64 / self.total_replicated_tables as f64
    }

    /// Gets replication error rate (0.0 to 1.0)
    pub fn get_error_rate(&self) -> f64 {
        if self.total_replicated_tables == 0 {
            return 0.0;
        }
        self.tables_with_errors as f64 / self.total_replicated_tables as f64
    }

    /// Gets lag rate (proportion of tables with lag)
    pub fn get_lag_rate(&self) -> f64 {
        if self.total_replicated_tables == 0 {
            return 0.0;
        }
        self.lagging_tables as f64 / self.total_replicated_tables as f64
    }

    /// Gets max replication lag in minutes
    pub fn get_max_replication_lag_minutes(&self) -> f64 {
        self.max_replication_lag / 60.0
    }

    /// Gets average replication lag in minutes
    pub fn get_avg_replication_lag_minutes(&self) -> f64 {
        self.avg_replication_lag / 60.0
    }

    /// Gets queue processing efficiency (active vs total queue)
    pub fn get_queue_processing_efficiency(&self) -> f64 {
        if self.total_queue_size == 0 {
            return 1.0; // No queue means 100% efficiency
        }
        self.active_queue_entries as f64 / self.total_queue_size as f64
    }

    /// Gets ZooKeeper session health rate
    pub fn get_zookeeper_session_health_rate(&self) -> f64 {
        if self.total_zookeeper_sessions == 0 {
            return 0.0;
        }
        self.active_zookeeper_sessions as f64 / self.total_zookeeper_sessions as f64
    }

    /// Gets average queue processing time in minutes
    pub fn get_avg_queue_processing_time_minutes(&self) -> f64 {
        self.avg_queue_processing_time / 60.0
    }

    /// Gets replication health status
    pub fn get_replication_health_status(&self) -> ReplicationHealthStatus {
        let sync_rate = self.get_synchronization_rate();
        let error_rate = self.get_error_rate();
        let has_high_lag = self.max_replication_lag > 600.0; // 10 minutes
        let has_many_readonly = self.readonly_tables > self.total_replicated_tables / 4; // >25%
        let zk_health = self.get_zookeeper_session_health_rate();

        if sync_rate < 0.5 || error_rate > 0.3 || (has_high_lag && has_many_readonly) || zk_health < 0.5 {
            ReplicationHealthStatus::Critical
        } else if sync_rate < 0.8 || error_rate > 0.1 || has_high_lag || has_many_readonly || zk_health < 0.8 {
            ReplicationHealthStatus::Warning
        } else if sync_rate < 0.95 || error_rate > 0.05 || self.failed_operations_last_hour > 0 {
            ReplicationHealthStatus::Attention
        } else {
            ReplicationHealthStatus::Healthy
        }
    }

    /// Gets replication activity level
    pub fn get_replication_activity_level(&self) -> ReplicationActivityLevel {
        let total_activity = self.total_queue_size + self.active_queue_entries;

        if total_activity == 0 {
            ReplicationActivityLevel::Idle
        } else if total_activity <= 10 {
            ReplicationActivityLevel::Low
        } else if total_activity <= 50 {
            ReplicationActivityLevel::Moderate
        } else if total_activity <= 200 {
            ReplicationActivityLevel::High
        } else {
            ReplicationActivityLevel::VeryHigh
        }
    }

    /// Gets readonly table ratio
    pub fn get_readonly_table_ratio(&self) -> f64 {
        if self.total_replicated_tables == 0 {
            return 0.0;
        }
        self.readonly_tables as f64 / self.total_replicated_tables as f64
    }

    /// Gets replica availability (active vs total replicas estimate)
    pub fn get_replica_availability_estimate(&self) -> f64 {
        // Estimate based on out of sync replicas vs total sessions
        if self.total_zookeeper_sessions == 0 {
            return 0.0;
        }
        let healthy_sessions = self.total_zookeeper_sessions.saturating_sub(self.out_of_sync_replicas);
        healthy_sessions as f64 / self.total_zookeeper_sessions as f64
    }

    /// Gets replication lag severity
    pub fn get_lag_severity(&self) -> ReplicationLagSeverity {
        if self.max_replication_lag <= 60.0 {
            ReplicationLagSeverity::Low
        } else if self.max_replication_lag <= 300.0 {
            ReplicationLagSeverity::Moderate
        } else if self.max_replication_lag <= 1800.0 {
            ReplicationLagSeverity::High
        } else {
            ReplicationLagSeverity::Critical
        }
    }
}

/// Replication health status classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ReplicationHealthStatus {
    /// All replicas are healthy and synchronized
    Healthy,
    /// Minor issues that should be monitored
    Attention,
    /// Issues that require investigation
    Warning,
    /// Critical issues requiring immediate attention
    Critical,
}

/// Replication activity level classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ReplicationActivityLevel {
    /// No replication activity
    Idle,
    /// Low replication activity
    Low,
    /// Moderate replication activity
    Moderate,
    /// High replication activity
    High,
    /// Very high replication activity
    VeryHigh,
}

/// Replication lag severity classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum ReplicationLagSeverity {
    /// Lag under 1 minute
    Low,
    /// Lag between 1-5 minutes
    Moderate,
    /// Lag between 5-30 minutes
    High,
    /// Lag over 30 minutes
    Critical,
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::test_utils::database_test_utils::connect_to_clickhouse;
//     use crate::test_utils::database_manager_test_utils::create_database_manager;
//
//     #[tokio::test]
//     async fn test_clickhouse_replication_info() {
//         let (_redis, _clickhouse, _db_manager) = create_database_manager().await;
//
//         let (_clickhouse, endpoint_cache_uuid, clickhouse_ep, telemetry_wrapper) =
//             connect_to_clickhouse().await;
//
//         let replication_info = ClickhouseReplicationInfo::default();
//
//         let result = replication_info
//             .sync_metadata(
//                 clickhouse_ep
//                     .0
//                     .read_conn_async(&endpoint_cache_uuid, telemetry_wrapper)
//                     .await?;
//                     .expect("failed to get connection")
//                     .to_owned(),
//                 telemetry_wrapper,
//             )
//             .await;
//
//         assert!(result.is_ok());
//         let info = result.unwrap_or_default();
//
//         // Verify core metrics are collected
//         assert!(info.get_synchronization_rate() >= 0.0);
//         assert!(info.get_synchronization_rate() <= 1.0);
//         assert!(info.get_error_rate() >= 0.0);
//         assert!(info.get_error_rate() <= 1.0);
//     }
//
//     #[test]
//     fn test_clickhouse_replication_calculations() {
//         let mut replication_info = ClickhouseReplicationInfo::default();
//         replication_info.total_replicated_tables = 20;
//         replication_info.synchronized_tables = 16;
//         replication_info.lagging_tables = 3;
//         replication_info.tables_with_errors = 1;
//         replication_info.readonly_tables = 2;
//         replication_info.max_replication_lag = 420.0; // 7 minutes
//         replication_info.avg_replication_lag = 180.0; // 3 minutes
//         replication_info.total_queue_size = 75;
//         replication_info.active_queue_entries = 15;
//         replication_info.total_zookeeper_sessions = 10;
//         replication_info.active_zookeeper_sessions = 9;
//         replication_info.out_of_sync_replicas = 1;
//         replication_info.failed_operations_last_hour = 2;
//
//         assert_eq!(replication_info.get_synchronization_rate(), 0.8);
//         assert_eq!(replication_info.get_error_rate(), 0.05);
//         assert_eq!(replication_info.get_lag_rate(), 0.15);
//         assert_eq!(replication_info.get_max_replication_lag_minutes(), 7.0);
//         assert_eq!(replication_info.get_avg_replication_lag_minutes(), 3.0);
//         assert_eq!(replication_info.get_queue_processing_efficiency(), 0.2);
//         assert_eq!(replication_info.get_zookeeper_session_health_rate(), 0.9);
//         assert_eq!(replication_info.get_readonly_table_ratio(), 0.1);
//
//         assert!(replication_info.has_high_replication_lag(300.0)); // 5 minutes threshold
//         assert!(replication_info.has_replication_errors());
//         assert!(replication_info.has_readonly_tables());
//         assert!(replication_info.has_large_replication_queue(50));
//         assert!(replication_info.has_out_of_sync_replicas());
//         assert!(replication_info.has_recent_failures());
//
//         let activity_level = replication_info.get_replication_activity_level();
//         assert!(matches!(activity_level, ReplicationActivityLevel::Moderate));
//
//         let health_status = replication_info.get_replication_health_status();
//         assert!(matches!(health_status, ReplicationHealthStatus::Warning));
//
//         let lag_severity = replication_info.get_lag_severity();
//         assert!(matches!(lag_severity, ReplicationLagSeverity::Moderate));
//     }
//
//     #[test]
//     fn test_replication_health_classification() {
//         // Test healthy status
//         let mut healthy_replication = ClickhouseReplicationInfo::default();
//         healthy_replication.total_replicated_tables = 10;
//         healthy_replication.synchronized_tables = 10;
//         healthy_replication.tables_with_errors = 0;
//         healthy_replication.readonly_tables = 0;
//         healthy_replication.max_replication_lag = 30.0; // 30 seconds
//         healthy_replication.total_zookeeper_sessions = 5;
//         healthy_replication.active_zookeeper_sessions = 5;
//         healthy_replication.failed_operations_last_hour = 0;
//
//         assert!(matches!(healthy_replication.get_replication_health_status(), ReplicationHealthStatus::Healthy));
//
//         // Test critical status
//         let mut critical_replication = ClickhouseReplicationInfo::default();
//         critical_replication.total_replicated_tables = 20;
//         critical_replication.synchronized_tables = 8; // 40% sync rate
//         critical_replication.tables_with_errors = 7; // 35% error rate
//         critical_replication.readonly_tables = 10;
//         critical_replication.max_replication_lag = 1800.0; // 30 minutes
//         critical_replication.total_zookeeper_sessions = 10;
//         critical_replication.active_zookeeper_sessions = 4; // 40% ZK health
//
//         assert!(matches!(critical_replication.get_replication_health_status(), ReplicationHealthStatus::Critical));
//     }
// }

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::ClickhouseReplicationInfo;

    #[test]
    fn replication_detailed_gate_false_for_healthy_baseline() {
        let info = ClickhouseReplicationInfo::default();
        assert!(!ClickhouseReplicationInfo::should_collect_detailed_metrics(&info));
    }

    #[test]
    fn replication_detailed_gate_true_for_lag() {
        let info = ClickhouseReplicationInfo {
            max_replication_lag: ClickhouseReplicationInfo::HIGH_LAG_THRESHOLD + 1.0,
            ..ClickhouseReplicationInfo::default()
        };
        assert!(ClickhouseReplicationInfo::should_collect_detailed_metrics(&info));
    }
}
