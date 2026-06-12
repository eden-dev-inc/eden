use crate::api::lib::QueryInput;
use borsh::{BorshDeserialize, BorshSerialize};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// Clickhouse cluster information and health metrics
///
/// Provides comprehensive monitoring of cluster topology, shard health,
/// replica status and distributed table operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseClusterInfo {
    /// Basic cluster topology information
    pub cluster_name: String,
    /// Total number of shards in the cluster
    pub total_shards: u64,
    /// Total number of replicas across all shards
    pub total_replicas: u64,
    /// Number of healthy shards (all replicas online)
    pub healthy_shards: u64,
    /// Number of degraded shards (some replicas offline)
    pub degraded_shards: u64,
    /// Number of failed shards (all replicas offline)
    pub failed_shards: u64,
    /// Overall cluster health percentage (0.0 to 100.0)
    pub cluster_health_pct: f64,
    /// Number of distributed tables in the cluster
    pub distributed_tables_count: u64,
    /// Number of replicated tables across all nodes
    pub replicated_tables_count: u64,
    /// Total data size across all shards in bytes
    pub total_data_size: u64,
    /// Number of active distributed queries
    pub active_distributed_queries: u64,
    /// Average replication lag across all replicas in seconds
    pub avg_replication_lag: f64,
    /// Maximum replication lag in the cluster in seconds
    pub max_replication_lag: f64,
    /// Detailed shard and replica information
    pub shards: Vec<ClickhouseShardInfo>,
    /// ZooKeeper cluster coordination status
    pub zookeeper_status: ClickhouseZooKeeperStatus,
    /// Detailed metrics collected when issues are detected
    pub detailed_metrics: Option<ClickhouseClusterDetailedMetrics>,
}

/// Detailed cluster metrics collected when problems are detected
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseClusterDetailedMetrics {
    /// Offline or problematic replicas
    pub problematic_replicas: Vec<ClickhouseReplicaStatus>,
    /// Tables with replication issues
    pub replication_issues: Vec<ClickhouseReplicationIssue>,
    /// Failed distributed queries
    pub failed_distributed_queries: Vec<ClickhouseDistributedQueryFailure>,
    /// Shard imbalance information
    pub shard_imbalance: Option<ClickhouseShardImbalance>,
}

impl MetadataCollection for ClickhouseClusterInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                Self::QUERY_CLUSTER_TOPOLOGY,
                query(
                    "SELECT
                    cluster, shard_num, replica_num, host_name, host_address, port,
                    is_local, user, default_database, errors_count, slowdowns_count,
                    estimated_recovery_time
                FROM system.clusters
                ORDER BY cluster, shard_num, replica_num"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_CLUSTER_SUMMARY,
                query(
                    "SELECT
                    cluster,
                    count() as total_nodes,
                    countDistinct(shard_num) as total_shards,
                    count() as total_replicas,
                    countIf(errors_count = 0) as healthy_replicas,
                    countIf(errors_count > 0) as unhealthy_replicas
                FROM system.clusters
                GROUP BY cluster"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_REPLICATION_STATUS,
                query(
                    "SELECT
                    database, table, is_leader, is_readonly, is_session_expired,
                    future_parts, parts_to_check, columns_version, queue_size,
                    inserts_in_queue, merges_in_queue, part_mutations_in_queue,
                    queue_oldest_time, inserts_oldest_time, merges_oldest_time,
                    oldest_part_to_get, oldest_part_to_merge_to, oldest_part_to_mutate_to,
                    log_max_index, log_pointer, last_queue_update, absolute_delay,
                    total_replicas, active_replicas, lost_part_count, last_queue_update_exception
                FROM system.replicas"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_DISTRIBUTED_TABLES,
                query(
                    "SELECT
                    database, name as table_name, engine, engine_full,
                    total_rows, total_bytes
                FROM system.tables
                WHERE engine LIKE '%Distributed%'"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_ZOOKEEPER_STATUS,
                query(
                    "SELECT
                    name, value
                FROM system.zookeeper
                WHERE path = '/'
                LIMIT 1"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_ACTIVE_DISTRIBUTED_QUERIES,
                query(
                    "SELECT
                    count() as active_distributed_count,
                    avg(elapsed) as avg_duration
                FROM system.processes
                WHERE query LIKE '%Distributed%' OR query LIKE '%GLOBAL%'"
                        .to_string(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return comprehensive Clickhouse cluster health and topology information"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "cluster"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use crate::metadata::capabilities::{CLICKHOUSE_HAS_REPLICATION, CLICKHOUSE_HAS_ZOOKEEPER};
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt, query, query_map};
use crate::output::ClickhouseRow;
use function_name::named;
use std::time::Duration;

impl ClickhouseClusterInfo {
    const QUERY_CLUSTER_TOPOLOGY: &'static str = "cluster_topology";
    const QUERY_CLUSTER_SUMMARY: &'static str = "cluster_summary";
    const QUERY_REPLICATION_STATUS: &'static str = "replication_status";
    const QUERY_DISTRIBUTED_TABLES: &'static str = "distributed_tables";
    const QUERY_ZOOKEEPER_STATUS: &'static str = "zookeeper_status";
    const QUERY_ACTIVE_DISTRIBUTED_QUERIES: &'static str = "active_distributed_queries";
    const REPLICATION_LAG_THRESHOLD: f64 = 300.0; // 5 minutes
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    #[allow(dead_code)]
    const MAX_DETAILED_RESULTS: usize = 100;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: ClickhouseAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut cluster_info = ClickhouseClusterInfo::default();
        let requests = self.request();

        let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, Self::QUERY_TIMEOUT);

        let (topology_rows, cluster_summary_row, distributed_rows, active_distributed_row) = tokio::try_join!(
            metadata_queries.rows(Self::QUERY_CLUSTER_TOPOLOGY),
            metadata_queries.row(Self::QUERY_CLUSTER_SUMMARY),
            metadata_queries.rows(Self::QUERY_DISTRIBUTED_TABLES),
            metadata_queries.row(Self::QUERY_ACTIVE_DISTRIBUTED_QUERIES),
        )?;

        // Get cluster topology
        cluster_info.shards = Self::parse_cluster_topology(topology_rows)?;

        // Get cluster summary
        if let Some(row) = cluster_summary_row {
            cluster_info.cluster_name = row.string_or_empty("cluster")?;
            cluster_info.total_shards = row.u64_or_zero("total_shards")?;
            cluster_info.total_replicas = row.u64_or_zero("total_replicas")?;

            let healthy_replicas = row.u64_or_zero("healthy_replicas")?;
            let unhealthy_replicas = row.u64_or_zero("unhealthy_replicas")?;

            // Calculate shard health based on replica health
            cluster_info.healthy_shards = healthy_replicas.min(cluster_info.total_shards);
            cluster_info.degraded_shards = if unhealthy_replicas > 0 && healthy_replicas > 0 {
                unhealthy_replicas.min(cluster_info.total_shards)
            } else {
                0
            };
            cluster_info.failed_shards = if healthy_replicas == 0 { cluster_info.total_shards } else { 0 };

            // Calculate overall health percentage
            cluster_info.cluster_health_pct = if cluster_info.total_replicas > 0 {
                (healthy_replicas as f64 / cluster_info.total_replicas as f64) * 100.0
            } else {
                100.0
            };
        }

        // Get replication status only when replicas exist
        let replication_rows = if capabilities.has(&CLICKHOUSE_HAS_REPLICATION) {
            metadata_queries.optional_rows(Self::QUERY_REPLICATION_STATUS).await.unwrap_or_default()
        } else {
            Vec::new()
        };
        let (avg_lag, max_lag, replicated_count) = Self::analyze_replication_status(&replication_rows)?;
        cluster_info.avg_replication_lag = avg_lag;
        cluster_info.max_replication_lag = max_lag;
        cluster_info.replicated_tables_count = replicated_count;

        // Get distributed tables info
        cluster_info.distributed_tables_count = distributed_rows.len() as u64;
        cluster_info.total_data_size = Self::calculate_total_data_size(&distributed_rows)?;

        // Get ZooKeeper status only when ZooKeeper is available
        cluster_info.zookeeper_status = if capabilities.has(&CLICKHOUSE_HAS_ZOOKEEPER) {
            let zk_rows = metadata_queries.optional_rows(Self::QUERY_ZOOKEEPER_STATUS).await;
            let zk_connected = matches!(zk_rows, Some(rows) if !rows.is_empty());
            if zk_connected {
                ClickhouseZooKeeperStatus {
                    is_connected: true,
                    connection_status: "Connected".to_string(),
                    last_error: None,
                }
            } else {
                ClickhouseZooKeeperStatus {
                    is_connected: false,
                    connection_status: "Disconnected".to_string(),
                    last_error: Some("Unable to query ZooKeeper".to_string()),
                }
            }
        } else {
            ClickhouseZooKeeperStatus::default()
        };

        // Get active distributed queries
        if let Some(row) = active_distributed_row {
            cluster_info.active_distributed_queries = row.u64_or_zero("active_distributed_count")?;
        }

        // Conditionally collect detailed metrics when problems are detected
        cluster_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&cluster_info, &replication_rows, context).await?;

        Ok(cluster_info)
    }

    async fn collect_detailed_metrics_if_needed(
        core_info: &ClickhouseClusterInfo,
        replication_rows: &[ClickhouseRow],
        _context: ClickhouseAsync,
    ) -> ResultEP<Option<ClickhouseClusterDetailedMetrics>> {
        let has_replication_issues = core_info.max_replication_lag > Self::REPLICATION_LAG_THRESHOLD;
        let has_unhealthy_shards = core_info.degraded_shards > 0 || core_info.failed_shards > 0;
        let has_zk_issues = !core_info.zookeeper_status.is_connected;

        if !has_replication_issues && !has_unhealthy_shards && !has_zk_issues {
            return Ok(None);
        }

        let mut detailed_metrics = ClickhouseClusterDetailedMetrics {
            problematic_replicas: Vec::new(),
            replication_issues: Vec::new(),
            failed_distributed_queries: Vec::new(),
            shard_imbalance: None,
        };

        // Collect problematic replicas if needed
        if has_unhealthy_shards {
            detailed_metrics.problematic_replicas = Self::identify_problematic_replicas(&core_info.shards);
        }

        // Collect replication issues if needed
        if has_replication_issues {
            detailed_metrics.replication_issues = Self::parse_replication_issues(replication_rows)?;
        }

        // Check for shard imbalance
        if core_info.shards.len() > 1 {
            detailed_metrics.shard_imbalance = Self::analyze_shard_imbalance(&core_info.shards);
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_cluster_topology(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseShardInfo>> {
        let mut shards_map: HashMap<u64, ClickhouseShardInfo> = HashMap::new();

        for row in rows {
            let shard_num = row.u64_or_zero("shard_num")?;
            let replica_num = row.u64_or_zero("replica_num")?;

            let replica = ClickhouseReplicaInfo {
                replica_num,
                host_name: row.string_or_empty("host_name")?,
                host_address: row.string_or_empty("host_address")?,
                port: row.u64_or_zero("port")? as u16,
                is_local: row.bool_or_false("is_local")?,
                user: row.string_or_empty("user")?,
                default_database: row.string_or_empty("default_database")?,
                errors_count: row.u64_or_zero("errors_count")?,
                slowdowns_count: row.u64_or_zero("slowdowns_count")?,
                estimated_recovery_time: row.u64_or_zero("estimated_recovery_time")?,
                is_healthy: row.u64_or_zero("errors_count")? == 0,
            };

            let shard = shards_map.entry(shard_num).or_insert_with(|| ClickhouseShardInfo {
                shard_num,
                replicas: Vec::new(),
                is_healthy: true,
                total_errors: 0,
                total_slowdowns: 0,
            });

            shard.total_errors += replica.errors_count;
            shard.total_slowdowns += replica.slowdowns_count;
            shard.is_healthy = shard.is_healthy && replica.is_healthy;
            shard.replicas.push(replica);
        }

        Ok(shards_map.into_values().collect())
    }

    fn analyze_replication_status(rows: &[ClickhouseRow]) -> ResultEP<(f64, f64, u64)> {
        if rows.is_empty() {
            return Ok((0.0, 0.0, 0));
        }

        let mut total_delay = 0.0;
        let mut max_delay: f64 = 0.0;
        let count = rows.len() as u64;

        for row in rows {
            let delay = row.f64_or_zero("absolute_delay")?;
            total_delay += delay;
            max_delay = max_delay.max(delay);
        }

        let avg_delay = if count > 0 { total_delay / count as f64 } else { 0.0 };

        Ok((avg_delay, max_delay, count))
    }

    fn calculate_total_data_size(rows: &[ClickhouseRow]) -> ResultEP<u64> {
        let mut total_size = 0u64;
        for row in rows {
            total_size += row.u64_or_zero("total_bytes")?;
        }
        Ok(total_size)
    }

    fn identify_problematic_replicas(shards: &[ClickhouseShardInfo]) -> Vec<ClickhouseReplicaStatus> {
        let mut problematic = Vec::new();

        for shard in shards {
            for replica in &shard.replicas {
                if !replica.is_healthy || replica.errors_count > 0 {
                    problematic.push(ClickhouseReplicaStatus {
                        shard_num: shard.shard_num,
                        replica_num: replica.replica_num,
                        host_name: replica.host_name.clone(),
                        status: if replica.is_healthy {
                            "Degraded".to_string()
                        } else {
                            "Failed".to_string()
                        },
                        errors_count: replica.errors_count,
                        last_error: None, // Would need additional query to get specific errors
                        recovery_time_estimate: replica.estimated_recovery_time,
                    });
                }
            }
        }

        problematic
    }

    fn parse_replication_issues(rows: &[ClickhouseRow]) -> ResultEP<Vec<ClickhouseReplicationIssue>> {
        let mut issues = Vec::new();

        for row in rows {
            let absolute_delay = row.f64_or_zero("absolute_delay")?;
            let queue_size = row.u64_or_zero("queue_size")?;

            if absolute_delay > 300.0 || queue_size > 100 {
                // Thresholds for issues
                issues.push(ClickhouseReplicationIssue {
                    database: row.string_or_empty("database")?,
                    table: row.string_or_empty("table")?,
                    issue_type: if absolute_delay > 300.0 {
                        "High Lag".to_string()
                    } else {
                        "Large Queue".to_string()
                    },
                    description: format!("Replication lag: {:.2}s, Queue size: {}", absolute_delay, queue_size),
                    severity: if absolute_delay > 600.0 || queue_size > 1000 {
                        "Critical".to_string()
                    } else {
                        "Warning".to_string()
                    },
                    absolute_delay,
                    queue_size,
                    parts_to_check: row.u64_or_zero("parts_to_check")?,
                });
            }
        }

        Ok(issues)
    }

    fn analyze_shard_imbalance(shards: &[ClickhouseShardInfo]) -> Option<ClickhouseShardImbalance> {
        if shards.len() < 2 {
            return None;
        }

        // Calculate replica count variance
        let replica_counts: Vec<usize> = shards.iter().map(|s| s.replicas.len()).collect();
        let avg_replicas = replica_counts.iter().sum::<usize>() as f64 / replica_counts.len() as f64;

        let variance = replica_counts.iter().map(|&count| (count as f64 - avg_replicas).powi(2)).sum::<f64>() / replica_counts.len() as f64;

        let std_dev = variance.sqrt();

        // Consider significant if std dev > 10% of average
        if std_dev > avg_replicas * 0.1 {
            Some(ClickhouseShardImbalance {
                imbalance_type: "Replica Count".to_string(),
                severity: if std_dev > avg_replicas * 0.3 {
                    "High".to_string()
                } else {
                    "Medium".to_string()
                },
                description: format!("Uneven replica distribution across shards. Std dev: {:.2}", std_dev),
                affected_shards: shards
                    .iter()
                    .filter(|s| (s.replicas.len() as f64 - avg_replicas).abs() > std_dev)
                    .map(|s| s.shard_num)
                    .collect(),
            })
        } else {
            None
        }
    }
}

/// Information about a specific shard in the cluster
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseShardInfo {
    /// Shard number/identifier
    pub shard_num: u64,
    /// List of replicas in this shard
    pub replicas: Vec<ClickhouseReplicaInfo>,
    /// Whether all replicas in this shard are healthy
    pub is_healthy: bool,
    /// Total error count across all replicas in this shard
    pub total_errors: u64,
    /// Total slowdown count across all replicas in this shard
    pub total_slowdowns: u64,
}

/// Information about a specific replica
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseReplicaInfo {
    /// Replica number within the shard
    pub replica_num: u64,
    /// Hostname of the replica
    pub host_name: String,
    /// IP address of the replica
    pub host_address: String,
    /// Port number
    pub port: u16,
    /// Whether this is the local replica
    pub is_local: bool,
    /// Username for connection
    pub user: String,
    /// Default database
    pub default_database: String,
    /// Number of connection errors
    pub errors_count: u64,
    /// Number of slowdowns
    pub slowdowns_count: u64,
    /// Estimated recovery time if replica is down
    pub estimated_recovery_time: u64,
    /// Overall health status of this replica
    pub is_healthy: bool,
}

/// ZooKeeper coordination status
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseZooKeeperStatus {
    /// Whether Clickhouse is connected to ZooKeeper
    pub is_connected: bool,
    /// Connection status description
    pub connection_status: String,
    /// Last error message if any
    pub last_error: Option<String>,
}

/// Status of a problematic replica
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseReplicaStatus {
    /// Shard number this replica belongs to
    pub shard_num: u64,
    /// Replica number
    pub replica_num: u64,
    /// Hostname of the problematic replica
    pub host_name: String,
    /// Current status (Failed, Degraded etc.)
    pub status: String,
    /// Number of errors
    pub errors_count: u64,
    /// Last error message
    pub last_error: Option<String>,
    /// Estimated time to recovery in seconds
    pub recovery_time_estimate: u64,
}

/// Replication issue details
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseReplicationIssue {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Type of replication issue
    pub issue_type: String,
    /// Detailed description of the issue
    pub description: String,
    /// Severity level (Critical, Warning etc.)
    pub severity: String,
    /// Absolute replication delay in seconds
    pub absolute_delay: f64,
    /// Size of replication queue
    pub queue_size: u64,
    /// Number of parts to check
    pub parts_to_check: u64,
}

/// Information about failed distributed queries
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDistributedQueryFailure {
    /// Query ID
    pub query_id: String,
    /// Failed shard information
    pub failed_shard: String,
    /// Error message
    pub error_message: String,
    /// When the failure occurred
    pub failure_time: DateTimeWrapper,
    /// Query that failed
    pub query_text: String,
}

/// Shard imbalance analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseShardImbalance {
    /// Type of imbalance (Data Size, Replica Count etc.)
    pub imbalance_type: String,
    /// Severity level
    pub severity: String,
    /// Description of the imbalance
    pub description: String,
    /// List of affected shard numbers
    pub affected_shards: Vec<u64>,
}

impl ClickhouseClusterInfo {
    /// Checks if the cluster is healthy
    pub fn is_cluster_healthy(&self) -> bool {
        self.failed_shards == 0 && self.degraded_shards == 0 && self.zookeeper_status.is_connected
    }

    /// Checks if there are replication issues
    pub fn has_replication_issues(&self) -> bool {
        self.max_replication_lag > 300.0 || // 5 minutes
            self.avg_replication_lag > 60.0 // 1 minute average
    }

    /// Gets the overall cluster status as a string
    pub fn get_cluster_status(&self) -> String {
        if self.failed_shards > 0 {
            "Critical".to_string()
        } else if self.degraded_shards > 0 || !self.zookeeper_status.is_connected {
            "Degraded".to_string()
        } else if self.has_replication_issues() {
            "Warning".to_string()
        } else {
            "Healthy".to_string()
        }
    }

    /// Gets total data size in a human-readable format
    pub fn get_data_size_formatted(&self) -> String {
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
        let mut size = self.total_data_size as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < UNITS.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        format!("{:.2} {}", size, UNITS[unit_index])
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets the number of healthy replicas across all shards
    pub fn get_healthy_replica_count(&self) -> u64 {
        self.shards.iter().map(|shard| shard.replicas.iter().filter(|r| r.is_healthy).count() as u64).sum()
    }

    /// Gets the number of unhealthy replicas across all shards
    pub fn get_unhealthy_replica_count(&self) -> u64 {
        self.total_replicas - self.get_healthy_replica_count()
    }

    /// Checks if ZooKeeper coordination is working
    pub fn is_zookeeper_healthy(&self) -> bool {
        self.zookeeper_status.is_connected
    }

    /// Gets replication lag status
    pub fn get_replication_lag_status(&self) -> String {
        if self.max_replication_lag > 600.0 {
            "Critical".to_string()
        } else if self.max_replication_lag > 300.0 {
            "Warning".to_string()
        } else {
            "Normal".to_string()
        }
    }

    /// Gets the most problematic shard (highest error count)
    pub fn get_most_problematic_shard(&self) -> Option<&ClickhouseShardInfo> {
        self.shards.iter().max_by_key(|shard| shard.total_errors + shard.total_slowdowns)
    }

    /// Calculates cluster capacity utilization
    pub fn get_capacity_utilization(&self) -> f64 {
        if self.total_replicas == 0 {
            0.0
        } else {
            (self.active_distributed_queries as f64 / self.total_replicas as f64) * 100.0
        }
    }
}

impl Default for ClickhouseZooKeeperStatus {
    fn default() -> Self {
        Self {
            is_connected: false,
            connection_status: "Unknown".to_string(),
            last_error: None,
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
//     async fn test_clickhouse_cluster_metadata() {
//         let (_redis, _clickhouse, _db_manager) = create_database_manager().await;
//
//         let (_clickhouse, endpoint_cache_uuid, clickhouse_ep, telemetry_wrapper) =
//             connect_to_clickhouse().await;
//
//         let cluster_info = ClickhouseClusterInfo::default();
//
//         let result = cluster_info
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
//         assert!(info.cluster_health_pct >= 0.0);
//         assert!(info.cluster_health_pct <= 100.0);
//     }
//
//     #[test]
//     fn test_cluster_health_calculations() {
//         let mut cluster_info = ClickhouseClusterInfo::default();
//         cluster_info.total_shards = 3;
//         cluster_info.healthy_shards = 2;
//         cluster_info.degraded_shards = 1;
//         cluster_info.failed_shards = 0;
//         cluster_info.max_replication_lag = 120.0;
//         cluster_info.zookeeper_status.is_connected = true;
//
//         assert!(!cluster_info.is_cluster_healthy()); // Has degraded shards
//         assert!(!cluster_info.has_replication_issues()); // Lag under threshold
//         assert_eq!(cluster_info.get_cluster_status(), "Degraded");
//         assert!(cluster_info.is_zookeeper_healthy());
//     }
//
//     #[test]
//     fn test_data_size_formatting() {
//         let mut cluster_info = ClickhouseClusterInfo::default();
//
//         // Test bytes
//         cluster_info.total_data_size = 512;
//         assert_eq!(cluster_info.get_data_size_formatted(), "512.00 B");
//
//         // Test KB
//         cluster_info.total_data_size = 1536; // 1.5 KB
//         assert_eq!(cluster_info.get_data_size_formatted(), "1.50 KB");
//
//         // Test GB
//         cluster_info.total_data_size = 2_147_483_648; // 2 GB
//         assert_eq!(cluster_info.get_data_size_formatted(), "2.00 GB");
//     }
//
//     #[test]
//     fn test_replica_counts() {
//         let mut cluster_info = ClickhouseClusterInfo::default();
//         cluster_info.total_replicas = 6;
//
//         // Create mock shards with replicas
//         cluster_info.shards = vec![ClickhouseShardInfo {
//             shard_num: 1,
//             replicas: vec![
//                 ClickhouseReplicaInfo {
//                     replica_num: 1,
//                     host_name: "host1".to_string(),
//                     host_address: "192.168.1.1".to_string(),
//                     port: 9000,
//                     is_local: true,
//                     user: "default".to_string(),
//                     default_database: "default".to_string(),
//                     errors_count: 0,
//                     slowdowns_count: 0,
//                     estimated_recovery_time: 0,
//                     is_healthy: true,
//                 },
//                 ClickhouseReplicaInfo {
//                     replica_num: 2,
//                     host_name: "host2".to_string(),
//                     host_address: "192.168.1.2".to_string(),
//                     port: 9000,
//                     is_local: false,
//                     user: "default".to_string(),
//                     default_database: "default".to_string(),
//                     errors_count: 1,
//                     slowdowns_count: 0,
//                     estimated_recovery_time: 0,
//                     is_healthy: false,
//                 },
//             ],
//             is_healthy: false,
//             total_errors: 1,
//             total_slowdowns: 0,
//         }];
//
//         assert_eq!(cluster_info.get_healthy_replica_count(), 1);
//         assert_eq!(cluster_info.get_unhealthy_replica_count(), 5);
//     }
//
//     #[test]
//     fn test_replication_lag_status() {
//         let mut cluster_info = ClickhouseClusterInfo::default();
//
//         // Normal lag
//         cluster_info.max_replication_lag = 30.0;
//         assert_eq!(cluster_info.get_replication_lag_status(), "Normal");
//
//         // Warning lag
//         cluster_info.max_replication_lag = 400.0;
//         assert_eq!(cluster_info.get_replication_lag_status(), "Warning");
//
//         // Critical lag
//         cluster_info.max_replication_lag = 700.0;
//         assert_eq!(cluster_info.get_replication_lag_status(), "Critical");
//     }
// }
