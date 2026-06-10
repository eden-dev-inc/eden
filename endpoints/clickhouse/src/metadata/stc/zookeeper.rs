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

/// Clickhouse ZooKeeper coordination and cluster health information.
///
/// Covers ZooKeeper connectivity and coordination metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseZooKeeperInfo {
    /// Number of active ZooKeeper connections
    pub active_connections: u64,
    /// Total number of ZooKeeper operations in the last minute
    pub operations_last_minute: u64,
    /// Number of failed ZooKeeper operations in the last minute
    pub failed_operations_last_minute: u64,
    /// Average ZooKeeper operation latency in milliseconds
    pub avg_operation_latency_ms: f64,
    /// Number of pending ZooKeeper operations
    pub pending_operations: u64,
    /// Number of ZooKeeper sessions
    pub active_sessions: u64,
    /// Number of replication queue entries across all tables
    pub replication_queue_size: u64,
    /// Number of tables with replication lag
    pub tables_with_replication_lag: u64,
    /// Maximum replication lag across all tables in seconds
    pub max_replication_lag_seconds: f64,
    /// Number of detached replica tables
    pub detached_replicas: u64,
    /// Number of read-only replica tables
    pub readonly_replicas: u64,
    /// Number of ZooKeeper coordination errors in the last hour
    pub coordination_errors_last_hour: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<ClickhouseZooKeeperDetailedMetrics>,
}

/// Detailed ZooKeeper metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseZooKeeperDetailedMetrics {
    /// Tables with high replication lag (collected when replication lag detected)
    pub lagging_replicas: Vec<ClickhouseLaggingReplica>,
    /// Failed ZooKeeper operations details (collected when failures > 0)
    pub failed_operations: Vec<ClickhouseFailedZooKeeperOperation>,
    /// Detached replica details (collected when detached_replicas > 0)
    pub detached_replica_details: Vec<ClickhouseDetachedReplica>,
    /// ZooKeeper session information (collected when connection issues detected)
    pub session_details: Option<Vec<ClickhouseZooKeeperSession>>,
    /// Replication queue analysis by table
    pub replication_queue_analysis: Option<Vec<ClickhouseReplicationQueueInfo>>,
}

impl MetadataCollection for ClickhouseZooKeeperInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                Self::QUERY_ZK_CONNECTIONS,
                query(
                    "SELECT
                    (SELECT value FROM system.metrics WHERE metric = 'ZooKeeperSessions') as active_connections,
                    0 as avg_operation_latency_ms"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_ZK_OPERATIONS,
                query(
                    "SELECT
                    0 as operations_last_minute,
                    0 as failed_operations_last_minute,
                    0 as coordination_errors_last_hour"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_REPLICATION_STATUS,
                query(
                    "SELECT
                    sum(queue_size) as replication_queue_size,
                    count() as total_replicated_tables,
                    countIf(is_session_expired = 1) as detached_replicas,
                    countIf(is_readonly = 1) as readonly_replicas,
                    countIf(log_max_index - log_pointer > 100) as tables_with_replication_lag,
                    max(log_max_index - log_pointer) as max_replication_lag_entries
                FROM system.replicas"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_ZK_SESSIONS,
                query(
                    "SELECT
                    (SELECT value FROM system.metrics WHERE metric = 'ZooKeeperSessions') as active_sessions,
                    (SELECT value FROM system.metrics WHERE metric = 'ZooKeeperRequest') as pending_operations"
                        .to_string(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential Clickhouse ZooKeeper coordination metrics with minimal overhead"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "coordination"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use crate::metadata::stc::utils::{query, query_map};
use function_name::named;
use std::time::Duration;

impl ClickhouseZooKeeperInfo {
    const QUERY_ZK_CONNECTIONS: &'static str = "zk_connections";
    const QUERY_ZK_OPERATIONS: &'static str = "zk_operations";
    const QUERY_REPLICATION_STATUS: &'static str = "replication_status";
    const QUERY_ZK_SESSIONS: &'static str = "zk_sessions";
    const HIGH_REPLICATION_LAG_THRESHOLD: u64 = 100; // entries
    const HIGH_LATENCY_THRESHOLD: f64 = 100.0; // milliseconds
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
}

impl ClickhouseZooKeeperInfo {
    /// Checks if there are replication issues
    pub fn has_replication_issues(&self) -> bool {
        self.tables_with_replication_lag > 0 || self.detached_replicas > 0 || self.readonly_replicas > 0
    }

    /// Checks if there are ZooKeeper connectivity issues
    pub fn has_connectivity_issues(&self) -> bool {
        self.failed_operations_last_minute > 0 || self.coordination_errors_last_hour > 0
    }

    /// Checks if ZooKeeper latency is high
    pub fn has_high_latency(&self, threshold_ms: f64) -> bool {
        self.avg_operation_latency_ms > threshold_ms
    }

    /// Checks if replication queue is backed up
    pub fn has_queue_backlog(&self, threshold: u64) -> bool {
        self.replication_queue_size > threshold
    }

    /// Checks if there are detached replicas
    pub fn has_detached_replicas(&self) -> bool {
        self.detached_replicas > 0
    }

    /// Checks if there are read-only replicas
    pub fn has_readonly_replicas(&self) -> bool {
        self.readonly_replicas > 0
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets operations per second rate
    pub fn get_operations_per_second(&self) -> f64 {
        self.operations_last_minute as f64 / 60.0
    }

    /// Gets failure rate percentage
    pub fn get_failure_rate_percentage(&self) -> f64 {
        if self.operations_last_minute == 0 {
            0.0
        } else {
            (self.failed_operations_last_minute as f64 / self.operations_last_minute as f64) * 100.0
        }
    }

    /// Gets maximum replication lag in minutes
    pub fn get_max_replication_lag_minutes(&self) -> f64 {
        self.max_replication_lag_seconds / 60.0
    }

    /// Checks if the cluster coordination is healthy
    pub fn is_coordination_healthy(&self) -> bool {
        self.active_connections > 0
            && self.failed_operations_last_minute == 0
            && self.coordination_errors_last_hour == 0
            && self.detached_replicas == 0
    }

    /// Gets the replica health percentage
    pub fn get_replica_health_percentage(&self, total_expected_replicas: u64) -> f64 {
        if total_expected_replicas == 0 {
            100.0
        } else {
            let unhealthy_replicas = self.detached_replicas + self.readonly_replicas + self.tables_with_replication_lag;
            let healthy_replicas = total_expected_replicas.saturating_sub(unhealthy_replicas);
            (healthy_replicas as f64 / total_expected_replicas as f64) * 100.0
        }
    }
}

/// Information about replicas with high replication lag
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseLaggingReplica {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Replication lag in log entries
    pub replication_lag_entries: u64,
    /// Current queue size
    pub queue_size: u64,
    /// Whether the replica is read-only
    pub is_readonly: bool,
    /// Whether the ZooKeeper session is expired
    pub is_session_expired: bool,
    /// Last time the queue was updated
    pub last_queue_update: DateTimeWrapper,
    /// Absolute delay in seconds
    pub absolute_delay: u64,
    /// Total number of replicas for this table
    pub total_replicas: u64,
    /// Number of active replicas
    pub active_replicas: u64,
}

/// Information about failed ZooKeeper operations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseFailedZooKeeperOperation {
    /// Type of ZooKeeper operation
    pub operation_type: String,
    /// ZooKeeper path involved in the operation
    pub path: String,
    /// ZooKeeper error code
    pub error_code: u64,
    /// When the operation failed
    pub event_time: DateTimeWrapper,
    /// ZooKeeper session ID
    pub session_id: u64,
    /// Request index
    pub request_idx: u64,
    /// Response index
    pub response_idx: u64,
    /// Operation duration in milliseconds
    pub duration_ms: f64,
}

/// Information about detached replica tables
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDetachedReplica {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Whether the ZooKeeper session is expired
    pub is_session_expired: bool,
    /// Whether the replica is read-only
    pub is_readonly: bool,
    /// Current queue size
    pub queue_size: u64,
    /// Replication lag in entries
    pub replication_lag: u64,
    /// Last time the queue was updated
    pub last_queue_update: DateTimeWrapper,
    /// ZooKeeper path for this replica
    pub zookeeper_path: String,
    /// Replica name/identifier
    pub replica_name: String,
}

/// ZooKeeper session information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseZooKeeperSession {
    /// ZooKeeper session ID
    pub session_id: u64,
    /// ZooKeeper host
    pub host: String,
    /// ZooKeeper port
    pub port: u64,
    /// Current latency in milliseconds
    pub latency: f64,
    /// Whether the session is expired
    pub is_expired: bool,
    /// Session uptime in seconds
    pub session_uptime_seconds: u64,
    /// Number of queries executed in this session
    pub queries: u64,
    /// Bytes sent in this session
    pub bytes_sent: u64,
    /// Bytes received in this session
    pub bytes_received: u64,
}

/// Replication queue analysis information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseReplicationQueueInfo {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Total queue size
    pub queue_size: u64,
    /// Number of insert operations in queue
    pub inserts_in_queue: u64,
    /// Number of merge operations in queue
    pub merges_in_queue: u64,
    /// Number of mutation operations in queue
    pub mutations_in_queue: u64,
    /// Total number of replicas for this table
    pub total_replicas: u64,
    /// Number of active replicas
    pub active_replicas: u64,
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::test_utils::database_test_utils::connect_to_clickhouse;
//     use crate::test_utils::database_manager_test_utils::create_database_manager;
//
//     #[tokio::test]
//     async fn test_clickhouse_metadata_zookeeper_info() {
//         let (_redis, _clickhouse, _db_manager) = create_database_manager().await;
//
//         let (_clickhouse, endpoint_cache_uuid, clickhouse_ep, telemetry_wrapper) =
//             connect_to_clickhouse().await;
//
//         let zookeeper_info = ClickhouseZooKeeperInfo::default();
//
//         let result = zookeeper_info
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
//         assert!(info.active_connections >= 0);
//         assert!(info.avg_operation_latency_ms >= 0.0);
//         assert!(info.replication_queue_size >= 0);
//     }
//
//     #[test]
//     fn test_clickhouse_zookeeper_health_checks() {
//         let mut zk_info = ClickhouseZooKeeperInfo::default();
//
//         // Test healthy state
//         zk_info.active_connections = 3;
//         zk_info.operations_last_minute = 100;
//         zk_info.failed_operations_last_minute = 0;
//         zk_info.coordination_errors_last_hour = 0;
//         zk_info.detached_replicas = 0;
//         zk_info.readonly_replicas = 0;
//         zk_info.avg_operation_latency_ms = 50.0;
//
//         assert!(zk_info.is_coordination_healthy());
//         assert!(!zk_info.has_replication_issues());
//         assert!(!zk_info.has_connectivity_issues());
//         assert!(!zk_info.has_high_latency(100.0));
//         assert_eq!(zk_info.get_failure_rate_percentage(), 0.0);
//
//         // Test problematic state
//         zk_info.failed_operations_last_minute = 5;
//         zk_info.detached_replicas = 2;
//         zk_info.readonly_replicas = 1;
//         zk_info.tables_with_replication_lag = 3;
//         zk_info.avg_operation_latency_ms = 150.0;
//         zk_info.coordination_errors_last_hour = 10;
//
//         assert!(!zk_info.is_coordination_healthy());
//         assert!(zk_info.has_replication_issues());
//         assert!(zk_info.has_connectivity_issues());
//         assert!(zk_info.has_high_latency(100.0));
//         assert!(zk_info.has_detached_replicas());
//         assert!(zk_info.has_readonly_replicas());
//         assert_eq!(zk_info.get_failure_rate_percentage(), 5.0);
//     }
//
//     #[test]
//     fn test_zookeeper_calculations() {
//         let mut zk_info = ClickhouseZooKeeperInfo::default();
//         zk_info.operations_last_minute = 120;
//         zk_info.failed_operations_last_minute = 6;
//         zk_info.max_replication_lag_seconds = 300.0; // 5 minutes
//         zk_info.replication_queue_size = 1000;
//
//         assert_eq!(zk_info.get_operations_per_second(), 2.0);
//         assert_eq!(zk_info.get_failure_rate_percentage(), 5.0);
//         assert_eq!(zk_info.get_max_replication_lag_minutes(), 5.0);
//         assert!(zk_info.has_queue_backlog(500));
//         assert!(!zk_info.has_queue_backlog(2000));
//     }
//
//     #[test]
//     fn test_replica_health_percentage() {
//         let mut zk_info = ClickhouseZooKeeperInfo::default();
//         zk_info.detached_replicas = 1;
//         zk_info.readonly_replicas = 1;
//         zk_info.tables_with_replication_lag = 2;
//
//         // 4 unhealthy out of 10 total = 60% healthy
//         assert_eq!(zk_info.get_replica_health_percentage(10), 60.0);
//
//         // Edge case: no expected replicas
//         assert_eq!(zk_info.get_replica_health_percentage(0), 100.0);
//
//         // Edge case: more issues than expected replicas
//         assert_eq!(zk_info.get_replica_health_percentage(3), 0.0);
//     }
//
//     #[test]
//     fn test_edge_cases() {
//         let zk_info = ClickhouseZooKeeperInfo::default();
//
//         // Test division by zero cases
//         assert_eq!(zk_info.get_operations_per_second(), 0.0);
//         assert_eq!(zk_info.get_failure_rate_percentage(), 0.0);
//         assert_eq!(zk_info.get_max_replication_lag_minutes(), 0.0);
//
//         // Test healthy defaults
//         assert!(zk_info.is_coordination_healthy());
//         assert!(!zk_info.has_replication_issues());
//         assert!(!zk_info.has_connectivity_issues());
//         assert!(!zk_info.has_detailed_metrics());
//     }
//
//     #[test]
//     fn test_coordination_health_conditions() {
//         let mut zk_info = ClickhouseZooKeeperInfo::default();
//
//         // Test each condition that breaks coordination health
//         zk_info.active_connections = 0;
//         assert!(!zk_info.is_coordination_healthy());
//
//         zk_info.active_connections = 1;
//         zk_info.failed_operations_last_minute = 1;
//         assert!(!zk_info.is_coordination_healthy());
//
//         zk_info.failed_operations_last_minute = 0;
//         zk_info.coordination_errors_last_hour = 1;
//         assert!(!zk_info.is_coordination_healthy());
//
//         zk_info.coordination_errors_last_hour = 0;
//         zk_info.detached_replicas = 1;
//         assert!(!zk_info.is_coordination_healthy());
//
//         // Reset to healthy state
//         zk_info.detached_replicas = 0;
//         assert!(zk_info.is_coordination_healthy());
//     }
//
//     #[test]
//     fn test_queue_backlog_detection() {
//         let mut zk_info = ClickhouseZooKeeperInfo::default();
//
//         // Test no backlog
//         zk_info.replication_queue_size = 50;
//         assert!(!zk_info.has_queue_backlog(100));
//
//         // Test backlog detected
//         zk_info.replication_queue_size = 150;
//         assert!(zk_info.has_queue_backlog(100));
//
//         // Test edge case
//         zk_info.replication_queue_size = 100;
//         assert!(!zk_info.has_queue_backlog(100));
//     }
//
//     #[test]
//     fn test_latency_threshold_detection() {
//         let mut zk_info = ClickhouseZooKeeperInfo::default();
//
//         // Test low latency
//         zk_info.avg_operation_latency_ms = 50.0;
//         assert!(!zk_info.has_high_latency(100.0));
//
//         // Test high latency
//         zk_info.avg_operation_latency_ms = 150.0;
//         assert!(zk_info.has_high_latency(100.0));
//
//         // Test edge case
//         zk_info.avg_operation_latency_ms = 100.0;
//         assert!(!zk_info.has_high_latency(100.0));
//     }
// }
