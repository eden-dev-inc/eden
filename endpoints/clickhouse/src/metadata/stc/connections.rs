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

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JsonValue(serde_json::Value);

/// Clickhouse connection information and pool statistics.
///
/// Covers connection pools, protocol usage, user sessions and
/// connection health.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseConnectionInfo {
    /// Total number of active connections
    pub total_connections: u64,
    /// HTTP protocol connections
    pub http_connections: u64,
    /// TCP (native) protocol connections
    pub tcp_connections: u64,
    /// MySQL protocol connections
    pub mysql_connections: u64,
    /// PostgreSQL protocol connections
    pub postgres_connections: u64,
    /// gRPC protocol connections
    pub grpc_connections: u64,
    /// InterServer connections (cluster communication)
    pub interserver_connections: u64,
    /// Maximum allowed connections from configuration
    pub max_connections: u64,
    /// Connection pool utilization percentage (0.0 to 100.0)
    pub connection_utilization_pct: f64,
    /// Average connection duration in seconds
    pub avg_connection_duration: f64,
    /// Longest active connection duration in seconds
    pub longest_connection_duration: f64,
    /// Number of connections in the last minute
    pub connections_last_minute: u64,
    /// Number of connection failures in the last minute
    pub connection_failures_last_minute: u64,
    /// Connection success rate percentage (0.0 to 100.0)
    pub connection_success_rate_pct: f64,
    /// Memory usage by all connections in bytes
    pub total_connection_memory: u64,
    /// Average memory per connection in bytes
    pub avg_memory_per_connection: u64,
    /// Number of unique users currently connected
    pub active_users_count: u64,
    /// Number of unique databases being accessed
    pub active_databases_count: u64,
    /// Detailed connection breakdown by user and database
    pub user_connections: Vec<ClickhouseUserConnection>,
    /// Protocol-specific statistics
    pub protocol_stats: Vec<ClickhouseProtocolStats>,
    /// Detailed metrics collected when connection issues are detected
    pub detailed_metrics: Option<ClickhouseConnectionDetailedMetrics>,
}

/// Detailed connection metrics collected when problems are detected
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseConnectionDetailedMetrics {
    /// Long-running connections
    pub long_running_connections: Vec<ClickhouseLongConnection>,
    /// Failed connection attempts
    pub recent_connection_failures: Vec<ClickhouseConnectionFailure>,
    /// High memory usage connections
    pub high_memory_connections: Vec<ClickhouseHighMemoryConnection>,
    /// Connection distribution by client
    pub client_distribution: Vec<ClickhouseClientStats>,
    /// Idle connections that might need cleanup
    pub idle_connections: Vec<ClickhouseIdleConnection>,
}

impl MetadataCollection for ClickhouseConnectionInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                Self::QUERY_CONNECTION_STATS,
                query(
                    "SELECT
                    multiIf(interface = 1, 'TCP', interface = 2, 'HTTP', interface = 3, 'MySQL', interface = 4, 'PostgreSQL', interface = 5, 'gRPC', interface = 6, 'InterServer', 'Unknown') as protocol,
                    count() as connection_count,
                    avg(elapsed) as avg_duration,
                    max(elapsed) as max_duration,
                    sum(memory_usage) as total_memory,
                    avg(memory_usage) as avg_memory
                FROM system.processes
                GROUP BY interface
                ORDER BY connection_count DESC"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_USER_CONNECTIONS,
                query(
                    "SELECT
                    user, current_database as database,
                    multiIf(interface = 1, 'TCP', interface = 2, 'HTTP', interface = 3, 'MySQL', interface = 4, 'PostgreSQL', interface = 5, 'gRPC', interface = 6, 'InterServer', 'Unknown') as protocol,
                    count() as connection_count,
                    sum(memory_usage) as total_memory,
                    avg(elapsed) as avg_duration,
                    countIf(query = '') as idle_connections
                FROM system.processes
                GROUP BY user, current_database, interface
                ORDER BY connection_count DESC"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_CONNECTION_SUMMARY,
                query(
                    "SELECT
                    count() as total_connections,
                    countDistinct(user) as unique_users,
                    countDistinct(current_database) as unique_databases,
                    sum(memory_usage) as total_memory,
                    avg(elapsed) as avg_duration,
                    max(elapsed) as max_duration,
                    (SELECT value FROM system.settings WHERE name = 'max_connections') as max_connections
                FROM system.processes"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_CONNECTION_HISTORY,
                query(
                    "SELECT
                    countIf(event_time >= now() - INTERVAL 1 MINUTE) as connections_last_minute,
                    countIf(event_time >= now() - INTERVAL 1 MINUTE AND exception != '') as failures_last_minute
                FROM system.query_log
                WHERE event_time >= now() - INTERVAL 1 MINUTE"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_CONNECTION_SETTINGS,
                query(
                    "SELECT name, value
                FROM system.settings
                WHERE name IN ('max_connections', 'max_concurrent_queries', 'max_threads',
                              'keep_alive_timeout', 'tcp_keep_alive_timeout', 'http_keep_alive_timeout')"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_CLIENT_INFO,
                query(
                    "SELECT
                    client_name, client_hostname,
                    count() as connection_count,
                    sum(memory_usage) as total_memory
                FROM system.processes
                WHERE client_name != ''
                GROUP BY client_name, client_hostname
                ORDER BY connection_count DESC
                LIMIT 20"
                        .to_string(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return comprehensive Clickhouse connection pool and protocol statistics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "connection"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use crate::metadata::stc::utils::{query, query_map};
use function_name::named;
use std::time::Duration;

impl ClickhouseConnectionInfo {
    const QUERY_CONNECTION_STATS: &'static str = "connection_stats";
    const QUERY_USER_CONNECTIONS: &'static str = "user_connections";
    const QUERY_CONNECTION_SUMMARY: &'static str = "connection_summary";
    const QUERY_CONNECTION_HISTORY: &'static str = "connection_history";
    const QUERY_CONNECTION_SETTINGS: &'static str = "connection_settings";
    const QUERY_CLIENT_INFO: &'static str = "client_info";
    const DETAIL_QUERY_LONG_RUNNING_CONNECTIONS: &'static str = "long_running_connections";
    const DETAIL_QUERY_HIGH_MEMORY_CONNECTIONS: &'static str = "high_memory_connections";
    const DETAIL_QUERY_CONNECTION_FAILURES: &'static str = "connection_failures";
    const DETAIL_QUERY_CLIENT_DISTRIBUTION: &'static str = "client_distribution";
    const DETAIL_QUERY_IDLE_CONNECTIONS: &'static str = "idle_connections";
    const LONG_CONNECTION_THRESHOLD: f64 = 3600.0; // 1 hour
    const HIGH_MEMORY_THRESHOLD: u64 = 1_073_741_824; // 1GB
    const IDLE_THRESHOLD: f64 = 1800.0; // 30 minutes
    const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
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

    fn should_collect_detailed_metrics(core_info: &ClickhouseConnectionInfo) -> bool {
        core_info.longest_connection_duration > Self::LONG_CONNECTION_THRESHOLD
            || core_info.connection_utilization_pct > 80.0
            || core_info.connection_failures_last_minute > 0
            || core_info.avg_memory_per_connection > Self::HIGH_MEMORY_THRESHOLD
    }

    fn update_protocol_counts(info: &mut ClickhouseConnectionInfo) {
        for stat in info.protocol_stats.as_slice() {
            match stat.protocol.as_str() {
                "HTTP" => info.http_connections = stat.connection_count,
                "TCP" => info.tcp_connections = stat.connection_count,
                "MySQL" => info.mysql_connections = stat.connection_count,
                "PostgreSQL" => info.postgres_connections = stat.connection_count,
                "gRPC" => info.grpc_connections = stat.connection_count,
                "InterServer" => info.interserver_connections = stat.connection_count,
                _ => {} // Other protocols not tracked individually
            }
        }
    }
}

/// Connection statistics by protocol
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseProtocolStats {
    /// Protocol name (HTTP, TCP, MySQL etc.)
    pub protocol: String,
    /// Number of connections using this protocol
    pub connection_count: u64,
    /// Average connection duration for this protocol
    pub avg_duration: f64,
    /// Maximum connection duration for this protocol
    pub max_duration: f64,
    /// Total memory usage by connections of this protocol
    pub total_memory: u64,
    /// Average memory per connection for this protocol
    pub avg_memory: u64,
}

/// Connection information by user and database
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseUserConnection {
    /// Username
    pub user: String,
    /// Database name
    pub database: String,
    /// Protocol used
    pub protocol: String,
    /// Number of connections for this user/database/protocol combination
    pub connection_count: u64,
    /// Total memory usage by these connections
    pub total_memory: u64,
    /// Average connection duration
    pub avg_duration: f64,
    /// Number of idle connections
    pub idle_connections: u64,
}

/// Information about long-running connections
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseLongConnection {
    /// Username
    pub user: String,
    /// Database name
    pub database: String,
    /// Protocol used
    pub protocol: String,
    /// Query ID
    pub query_id: String,
    /// Query text (truncated)
    pub query_text: String,
    /// Connection duration in seconds
    pub duration: f64,
    /// Memory usage
    pub memory_usage: u64,
    /// Rows read
    pub read_rows: u64,
    /// Bytes read
    pub read_bytes: u64,
    /// Client application name
    pub client_name: Option<String>,
    /// Client hostname
    pub client_hostname: Option<String>,
    /// Client version
    pub client_version: Option<String>,
    /// When the connection started
    pub start_time: DateTimeWrapper,
}

/// Information about high memory usage connections
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseHighMemoryConnection {
    /// Username
    pub user: String,
    /// Database name
    pub database: String,
    /// Protocol used
    pub protocol: String,
    /// Query ID
    pub query_id: String,
    /// Query text (truncated)
    pub query_text: String,
    /// Memory usage in bytes
    pub memory_usage: u64,
    /// Connection duration in seconds
    pub duration: f64,
    /// Rows read
    pub read_rows: u64,
    /// Bytes read
    pub read_bytes: u64,
    /// Client application name
    pub client_name: Option<String>,
}

/// Information about connection failures
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseConnectionFailure {
    /// Username that attempted connection
    pub user: String,
    /// Database that was being accessed
    pub database: String,
    /// Client application name
    pub client_name: Option<String>,
    /// Client hostname
    pub client_hostname: Option<String>,
    /// Exception/error message
    pub exception: String,
    /// When the failure occurred
    pub failure_time: DateTimeWrapper,
    /// Duration before failure
    pub duration: f64,
    /// Query that was being executed
    pub query_text: String,
}

/// Client application statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseClientStats {
    /// Client application name
    pub client_name: String,
    /// Client version
    pub client_version: Option<String>,
    /// Client hostname
    pub client_hostname: Option<String>,
    /// Number of connections from this client
    pub connection_count: u64,
    /// Total memory usage by this client
    pub total_memory: u64,
    /// Average connection duration for this client
    pub avg_duration: f64,
}

/// Information about idle connections
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseIdleConnection {
    /// Username
    pub user: String,
    /// Database name
    pub database: String,
    /// Protocol used
    pub protocol: String,
    /// Query ID (if any)
    pub query_id: String,
    /// How long the connection has been idle
    pub idle_duration: f64,
    /// Memory usage while idle
    pub memory_usage: u64,
    /// Client application name
    pub client_name: Option<String>,
    /// Client hostname
    pub client_hostname: Option<String>,
}

impl ClickhouseConnectionInfo {
    /// Checks if connection pool utilization is high
    pub fn is_high_utilization(&self, threshold_pct: f64) -> bool {
        self.connection_utilization_pct > threshold_pct
    }

    /// Checks if there are connection failures
    pub fn has_connection_failures(&self) -> bool {
        self.connection_failures_last_minute > 0
    }

    /// Checks if connection success rate is below threshold
    pub fn is_low_success_rate(&self, threshold_pct: f64) -> bool {
        self.connection_success_rate_pct < threshold_pct
    }

    /// Checks if there are long-running connections
    pub fn has_long_connections(&self, threshold_seconds: f64) -> bool {
        self.longest_connection_duration > threshold_seconds
    }

    /// Gets the most used protocol
    pub fn get_primary_protocol(&self) -> String {
        let mut max_count = 0u64;
        let mut primary = "Unknown".to_string();

        for stat in &self.protocol_stats {
            if stat.connection_count > max_count {
                max_count = stat.connection_count;
                primary = stat.protocol.clone();
            }
        }

        primary
    }

    /// Gets memory usage in a human-readable format
    pub fn get_memory_usage_formatted(&self) -> String {
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
        let mut size = self.total_connection_memory as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < UNITS.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        format!("{:.2} {}", size, UNITS[unit_index])
    }

    /// Gets average memory per connection in MB
    pub fn get_avg_memory_per_connection_mb(&self) -> f64 {
        self.avg_memory_per_connection as f64 / 1_048_576.0
    }

    /// Gets connection distribution summary
    pub fn get_connection_distribution(&self) -> HashMap<String, u64> {
        let mut distribution = HashMap::new();

        for stat in &self.protocol_stats {
            distribution.insert(stat.protocol.clone(), stat.connection_count);
        }

        distribution
    }

    /// Gets the user with most connections
    pub fn get_top_user(&self) -> Option<&ClickhouseUserConnection> {
        self.user_connections.iter().max_by_key(|conn| conn.connection_count)
    }

    /// Gets total idle connections across all users
    pub fn get_total_idle_connections(&self) -> u64 {
        self.user_connections.iter().map(|conn| conn.idle_connections).sum()
    }

    /// Calculates connection pool efficiency
    pub fn get_connection_efficiency(&self) -> f64 {
        let idle_count = self.get_total_idle_connections();
        if self.total_connections == 0 {
            100.0
        } else {
            let active_count = self.total_connections - idle_count;
            (active_count as f64 / self.total_connections as f64) * 100.0
        }
    }

    /// Checks if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets connection health status
    pub fn get_connection_health_status(&self) -> String {
        if self.connection_failures_last_minute > 10 || self.connection_success_rate_pct < 95.0 {
            "Critical".to_string()
        } else if self.connection_utilization_pct > 90.0 || self.connection_success_rate_pct < 98.0 {
            "Warning".to_string()
        } else if self.connection_failures_last_minute > 0 || self.connection_utilization_pct > 80.0 {
            "Caution".to_string()
        } else {
            "Healthy".to_string()
        }
    }

    /// Gets protocol with highest memory usage
    pub fn get_highest_memory_protocol(&self) -> Option<&ClickhouseProtocolStats> {
        self.protocol_stats.iter().max_by_key(|stat| stat.avg_memory)
    }

    /// Estimates connection capacity remaining
    pub fn get_remaining_capacity(&self) -> u64 {
        self.max_connections.saturating_sub(self.total_connections)
    }

    /// Calculates connections per user ratio
    pub fn get_avg_connections_per_user(&self) -> f64 {
        if self.active_users_count == 0 {
            0.0
        } else {
            self.total_connections as f64 / self.active_users_count as f64
        }
    }
}
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::test_utils::database_test_utils::connect_to_clickhouse;
//     use crate::test_utils::database_manager_test_utils::create_database_manager;
//
//     #[tokio::test]
//     async fn test_clickhouse_connection_metadata() {
//         let (_redis, _clickhouse, _db_manager) = create_database_manager().await;
//
//         let (_clickhouse, endpoint_cache_uuid, clickhouse_ep, telemetry_wrapper) =
//             connect_to_clickhouse().await;
//
//         let connection_info = ClickhouseConnectionInfo::default();
//
//         let result = connection_info
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
//         assert!(info.connection_utilization_pct >= 0.0);
//         assert!(info.connection_utilization_pct <= 100.0);
//         assert!(info.connection_success_rate_pct >= 0.0);
//         assert!(info.connection_success_rate_pct <= 100.0);
//     }
//
//     #[test]
//     fn test_connection_health_calculations() {
//         let mut conn_info = ClickhouseConnectionInfo::default();
//         conn_info.total_connections = 80;
//         conn_info.max_connections = 100;
//         conn_info.connection_failures_last_minute = 0;
//         conn_info.connection_success_rate_pct = 99.5;
//         conn_info.connection_utilization_pct = 80.0;
//
//         assert_eq!(conn_info.get_remaining_capacity(), 20);
//         assert!(!conn_info.is_high_utilization(85.0));
//         assert!(conn_info.is_high_utilization(75.0));
//         assert!(!conn_info.has_connection_failures());
//         assert_eq!(conn_info.get_connection_health_status(), "Caution");
//     }
//
//     #[test]
//     fn test_memory_formatting() {
//         let mut conn_info = ClickhouseConnectionInfo::default();
//
//         // Test bytes
//         conn_info.total_connection_memory = 512;
//         assert_eq!(conn_info.get_memory_usage_formatted(), "512.00 B");
//
//         // Test MB
//         conn_info.total_connection_memory = 1_572_864; // 1.5 MB
//         assert_eq!(conn_info.get_memory_usage_formatted(), "1.50 MB");
//
//         // Test GB
//         conn_info.total_connection_memory = 2_147_483_648; // 2 GB
//         assert_eq!(conn_info.get_memory_usage_formatted(), "2.00 GB");
//     }
//
//     #[test]
//     fn test_connection_efficiency() {
//         let mut conn_info = ClickhouseConnectionInfo::default();
//         conn_info.total_connections = 10;
//
//         // Add user connections with idle connections
//         conn_info.user_connections = vec![
//             ClickhouseUserConnection {
//                 user: "user1".to_string(),
//                 database: "db1".to_string(),
//                 protocol: "HTTP".to_string(),
//                 connection_count: 5,
//                 total_memory: 1000,
//                 avg_duration: 10.0,
//                 idle_connections: 2,
//             },
//             ClickhouseUserConnection {
//                 user: "user2".to_string(),
//                 database: "db2".to_string(),
//                 protocol: "TCP".to_string(),
//                 connection_count: 5,
//                 total_memory: 2000,
//                 avg_duration: 15.0,
//                 idle_connections: 1,
//             },
//         ];
//
//         assert_eq!(conn_info.get_total_idle_connections(), 3);
//         assert_eq!(conn_info.get_connection_efficiency(), 70.0); // 7 active out of 10 total
//     }
//
//     #[test]
//     fn test_protocol_distribution() {
//         let mut conn_info = ClickhouseConnectionInfo::default();
//         conn_info.protocol_stats = vec![
//             ClickhouseProtocolStats {
//                 protocol: "HTTP".to_string(),
//                 connection_count: 15,
//                 avg_duration: 10.0,
//                 max_duration: 30.0,
//                 total_memory: 1000,
//                 avg_memory: 66,
//             },
//             ClickhouseProtocolStats {
//                 protocol: "TCP".to_string(),
//                 connection_count: 5,
//                 avg_duration: 20.0,
//                 max_duration: 60.0,
//                 total_memory: 2000,
//                 avg_memory: 400,
//             },
//         ];
//
//         assert_eq!(conn_info.get_primary_protocol(), "HTTP");
//
//         let distribution = conn_info.get_connection_distribution();
//         assert_eq!(distribution.get("HTTP"), Some(&15));
//         assert_eq!(distribution.get("TCP"), Some(&5));
//
//         let highest_memory = conn_info.get_highest_memory_protocol().unwrap_or_default();
//         assert_eq!(highest_memory.protocol, "TCP");
//     }
//
//     #[test]
//     fn test_user_analytics() {
//         let mut conn_info = ClickhouseConnectionInfo::default();
//         conn_info.total_connections = 20;
//         conn_info.active_users_count = 4;
//
//         conn_info.user_connections = vec![
//             ClickhouseUserConnection {
//                 user: "power_user".to_string(),
//                 database: "analytics".to_string(),
//                 protocol: "HTTP".to_string(),
//                 connection_count: 12,
//                 total_memory: 5000,
//                 avg_duration: 30.0,
//                 idle_connections: 1,
//             },
//             ClickhouseUserConnection {
//                 user: "regular_user".to_string(),
//                 database: "app".to_string(),
//                 protocol: "TCP".to_string(),
//                 connection_count: 8,
//                 total_memory: 2000,
//                 avg_duration: 10.0,
//                 idle_connections: 2,
//             },
//         ];
//
//         assert_eq!(conn_info.get_avg_connections_per_user(), 5.0);
//
//         let top_user = conn_info.get_top_user().unwrap_or_default();
//         assert_eq!(top_user.user, "power_user");
//         assert_eq!(top_user.connection_count, 12);
//     }
// }

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::ClickhouseConnectionInfo;

    #[test]
    fn connections_detailed_gate_false_for_healthy_baseline() {
        let info = ClickhouseConnectionInfo::default();
        assert!(!ClickhouseConnectionInfo::should_collect_detailed_metrics(&info));
    }

    #[test]
    fn connections_detailed_gate_true_for_failures() {
        let info = ClickhouseConnectionInfo {
            connection_failures_last_minute: 1,
            ..ClickhouseConnectionInfo::default()
        };
        assert!(ClickhouseConnectionInfo::should_collect_detailed_metrics(&info));
    }
}
