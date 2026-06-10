use super::utils::{
    DEFAULT_QUERY_TIMEOUT, get_string, get_string_or, map_rows, query, query_map, row_count, run_named_query, run_optional_query,
};
use crate::api::lib::QueryUnpagedInput;
use crate::metadata::capabilities::CASSANDRA_HAS_VIRTUAL_TABLES;
use borsh::{BorshDeserialize, BorshSerialize};
use cassandra_core::CassandraAsync;
use endpoint_types::metadata::CapabilityChecker;
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};
use error::ResultEP;
use function_name::named;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// Cassandra cluster health and status: node counts, schema agreement and
/// client connections.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraClusterInfo {
    /// Total number of nodes in the cluster
    pub total_nodes: u64,
    /// Number of nodes that are currently up and available
    pub up_nodes: u64,
    /// Number of nodes that are down or unreachable
    pub down_nodes: u64,
    /// Number of nodes in joining state
    pub joining_nodes: u64,
    /// Number of nodes in leaving state
    pub leaving_nodes: u64,
    /// Number of nodes in moving state
    pub moving_nodes: u64,
    /// Cluster health percentage (0.0 to 100.0)
    pub cluster_health_pct: f64,
    /// Current schema agreement status across the cluster
    pub schema_agreement: bool,
    /// Number of nodes with schema disagreement
    pub schema_disagreement_count: u64,
    /// Total number of active client connections across cluster
    pub total_client_connections: u64,
    /// Maximum allowed connections per node
    pub max_connections_per_node: u64,
    /// Average connection utilization percentage across nodes
    pub avg_connection_utilization_pct: f64,
    /// Number of pending compactions across the cluster
    pub pending_compactions: u64,
    /// Number of active repairs in progress
    pub active_repairs: u64,
    /// Average load across all nodes (in GB)
    pub avg_load_gb: f64,
    /// Cluster consistency level warnings
    pub consistency_warnings: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<CassandraDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraDetailedMetrics {
    /// Individual node status details (only collected when nodes are down)
    pub node_status_details: Vec<CassandraNodeStatus>,
    /// Schema version details (only collected during schema disagreement)
    pub schema_versions: Vec<CassandraSchemaVersion>,
    /// Connection details by datacenter (collected less frequently)
    pub connections_by_datacenter: Option<Vec<CassandraConnectionsByDatacenter>>,
    /// Pending compaction details (only when compactions are high)
    pub pending_compaction_details: Vec<CassandraPendingCompaction>,
}

impl MetadataCollection for CassandraClusterInfo {
    type Request = HashMap<String, QueryUnpagedInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                "cluster_status",
                query(
                    "SELECT peer, data_center, rack, release_version, host_id, preferred_ip, \
                     rpc_address, schema_version, tokens FROM system.peers",
                ),
            ),
            (
                "local_node",
                query(
                    "SELECT cluster_name, data_center, rack, release_version, schema_version, \
                     partitioner, broadcast_address, listen_address, rpc_address \
                     FROM system.local",
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential Cassandra cluster health metrics with minimal overhead"
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

impl CassandraClusterInfo {
    #[allow(dead_code)]
    const SCHEMA_CHECK_THRESHOLD: u64 = 1;
    #[allow(dead_code)]
    const MAX_DETAILED_RESULTS: usize = 50;
    const HIGH_COMPACTION_THRESHOLD: u64 = 10;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: CassandraAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let requests = self.request();

        let cluster_status_data = run_named_query(&requests, "cluster_status", context.clone(), DEFAULT_QUERY_TIMEOUT).await?;
        let local_node_data = run_named_query(&requests, "local_node", context.clone(), DEFAULT_QUERY_TIMEOUT).await?;

        let total_nodes = Self::count_total_nodes(&cluster_status_data)?;
        let (schema_agreement, schema_disagreement_count) = Self::check_schema_agreement(&cluster_status_data, &local_node_data)?;

        // All peers visible in system.peers are assumed reachable; true up/down state
        // requires gossip data not available through CQL.
        let cluster_health_pct = if total_nodes > 0 { 100.0 } else { 0.0 };

        let total_client_connections = if capabilities.has(&CASSANDRA_HAS_VIRTUAL_TABLES) {
            run_optional_query(
                "SELECT address, port, connection_stage, keyspace_name, username \
                 FROM system_views.clients",
                context.clone(),
                DEFAULT_QUERY_TIMEOUT,
                "clients_virtual_table",
            )
            .await
            .map(|rows| row_count(&rows) as u64)
            .unwrap_or(0)
        } else {
            0
        };

        let mut cluster_info = CassandraClusterInfo {
            total_nodes,
            up_nodes: 0,
            down_nodes: 0,
            joining_nodes: 0,
            leaving_nodes: 0,
            moving_nodes: 0,
            cluster_health_pct,
            schema_agreement,
            schema_disagreement_count,
            total_client_connections,
            max_connections_per_node: 0,
            avg_connection_utilization_pct: 0.0,
            pending_compactions: 0,
            active_repairs: 0,
            avg_load_gb: 0.0,
            consistency_warnings: 0,
            detailed_metrics: None,
        };

        cluster_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&cluster_info, context).await?;

        Ok(cluster_info)
    }

    async fn collect_detailed_metrics_if_needed(
        core_info: &CassandraClusterInfo,
        context: CassandraAsync,
    ) -> ResultEP<Option<CassandraDetailedMetrics>> {
        let needs_node_details = core_info.down_nodes > 0 || core_info.joining_nodes > 0 || core_info.leaving_nodes > 0;
        let needs_schema_details = !core_info.schema_agreement;
        let needs_compaction_details = core_info.pending_compactions > Self::HIGH_COMPACTION_THRESHOLD;

        if !needs_node_details && !needs_schema_details && !needs_compaction_details {
            return Ok(None);
        }

        let mut detailed_metrics = CassandraDetailedMetrics {
            node_status_details: Vec::new(),
            schema_versions: Vec::new(),
            connections_by_datacenter: None,
            pending_compaction_details: Vec::new(),
        };

        if needs_node_details
            && let Some(result) = run_optional_query(
                "SELECT peer, data_center, rack, release_version, host_id, schema_version, tokens \
                 FROM system.peers",
                context.clone(),
                DEFAULT_QUERY_TIMEOUT,
                "node_status_details",
            )
            .await
        {
            detailed_metrics.node_status_details = Self::parse_node_status_details(&result);
        }

        if needs_schema_details {
            let mut schema_versions = Vec::new();

            if let Some(peers_result) = run_optional_query(
                "SELECT peer, schema_version FROM system.peers",
                context.clone(),
                DEFAULT_QUERY_TIMEOUT,
                "schema_versions_peers",
            )
            .await
            {
                schema_versions.extend(Self::parse_schema_versions_peers(&peers_result));
            }

            if let Some(local_result) = run_optional_query(
                "SELECT broadcast_address, schema_version FROM system.local",
                context.clone(),
                DEFAULT_QUERY_TIMEOUT,
                "schema_versions_local",
            )
            .await
            {
                schema_versions.extend(Self::parse_schema_versions_local(&local_result));
            }

            detailed_metrics.schema_versions = schema_versions;
        }

        Ok(Some(detailed_metrics))
    }

    fn count_total_nodes(cluster_data: &Value) -> ResultEP<u64> {
        // +1 accounts for the local node, which does not appear in system.peers
        Ok(row_count(cluster_data) as u64 + 1)
    }

    fn check_schema_agreement(cluster_data: &Value, local_data: &Value) -> ResultEP<(bool, u64)> {
        let mut schema_versions = std::collections::HashSet::new();

        if let Value::Array(local_rows) = local_data
            && let Some(local_row) = local_rows.first()
            && let Some(v) = get_string(local_row, "schema_version")
        {
            schema_versions.insert(v);
        }

        if let Value::Array(cluster_rows) = cluster_data {
            for row in cluster_rows {
                if let Some(v) = get_string(row, "schema_version") {
                    schema_versions.insert(v);
                }
            }
        }

        let disagreement_count = schema_versions.len().saturating_sub(1) as u64;
        Ok((schema_versions.len() <= 1, disagreement_count))
    }

    fn parse_node_status_details(query_result: &Value) -> Vec<CassandraNodeStatus> {
        map_rows(query_result, |row| {
            Some(CassandraNodeStatus {
                peer_address: get_string_or(row, "peer", ""),
                data_center: get_string_or(row, "data_center", ""),
                rack: get_string_or(row, "rack", ""),
                release_version: get_string_or(row, "release_version", ""),
                host_id: get_string_or(row, "host_id", ""),
                schema_version: get_string_or(row, "schema_version", ""),
                status: "UP".to_string(),
                load_gb: 0.0,
                token_count: 0,
            })
        })
    }

    fn parse_schema_versions_peers(query_result: &Value) -> Vec<CassandraSchemaVersion> {
        map_rows(query_result, |row| {
            Some(CassandraSchemaVersion {
                node_address: get_string_or(row, "peer", ""),
                schema_version: get_string_or(row, "schema_version", ""),
            })
        })
    }

    fn parse_schema_versions_local(query_result: &Value) -> Vec<CassandraSchemaVersion> {
        map_rows(query_result, |row| {
            Some(CassandraSchemaVersion {
                node_address: get_string_or(row, "broadcast_address", ""),
                schema_version: get_string_or(row, "schema_version", ""),
            })
        })
    }
}

/// Individual node status information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraNodeStatus {
    /// IP address of the peer node
    pub peer_address: String,
    /// Data center where the node is located
    pub data_center: String,
    /// Rack where the node is located
    pub rack: String,
    /// Cassandra version running on the node
    pub release_version: String,
    /// Unique identifier for the host
    pub host_id: String,
    /// Schema version on this node
    pub schema_version: String,
    /// Current status of the node (UP, DOWN, JOINING etc.)
    pub status: String,
    /// Current load on the node in GB
    pub load_gb: f64,
    /// Number of tokens owned by this node
    pub token_count: u64,
}

/// Schema version information for cluster agreement tracking
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraSchemaVersion {
    /// Node address
    pub node_address: String,
    /// Schema version UUID
    pub schema_version: String,
}

/// Connection statistics grouped by datacenter
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraConnectionsByDatacenter {
    /// Datacenter name
    pub datacenter_name: String,
    /// Total connections to this datacenter
    pub total_connections: u64,
    /// Active connections to this datacenter
    pub active_connections: u64,
    /// Number of nodes in this datacenter
    pub node_count: u64,
    /// Average connections per node
    pub avg_connections_per_node: f64,
}

/// Pending compaction information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraPendingCompaction {
    /// Keyspace name
    pub keyspace_name: String,
    /// Table name
    pub table_name: String,
    /// Number of pending compactions
    pub pending_tasks: u64,
    /// Estimated size of data to compact (MB)
    pub estimated_size_mb: u64,
    /// Compaction type (MAJOR, MINOR etc.)
    pub compaction_type: String,
}

impl CassandraClusterInfo {
    /// Calculates the percentage of nodes that are healthy (up)
    pub fn healthy_node_percentage(&self) -> f64 {
        if self.total_nodes == 0 {
            0.0
        } else {
            (self.up_nodes as f64 / self.total_nodes as f64) * 100.0
        }
    }

    /// Checks if the cluster has any unhealthy nodes
    pub fn has_unhealthy_nodes(&self) -> bool {
        self.down_nodes > 0 || self.joining_nodes > 0 || self.leaving_nodes > 0
    }

    /// Checks if schema agreement is maintained across the cluster
    pub fn has_schema_agreement(&self) -> bool {
        self.schema_agreement
    }

    /// Checks if there are too many pending compactions
    pub fn has_high_compaction_load(&self, threshold: u64) -> bool {
        self.pending_compactions > threshold
    }

    /// Checks if connection utilization is approaching limits
    pub fn is_approaching_connection_limit(&self, threshold_percentage: f64) -> bool {
        self.avg_connection_utilization_pct > threshold_percentage
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Checks if the cluster is in a critical state
    pub fn is_critical_state(&self) -> bool {
        self.cluster_health_pct < 50.0 || !self.schema_agreement || self.down_nodes > 0
    }

    /// Gets the number of nodes in transitional states
    pub fn transitional_nodes(&self) -> u64 {
        self.joining_nodes + self.leaving_nodes + self.moving_nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_cluster_health_calculations() {
        let cluster_info = CassandraClusterInfo {
            total_nodes: 5,
            up_nodes: 4,
            down_nodes: 1,
            ..Default::default()
        };

        assert_eq!(cluster_info.healthy_node_percentage(), 80.0);
        assert!(cluster_info.has_unhealthy_nodes());
        // down_nodes > 0 triggers critical state regardless of health percentage
        assert!(cluster_info.is_critical_state());

        // Non-critical: all nodes up, schema agreed, health above 50%
        let healthy_cluster = CassandraClusterInfo {
            total_nodes: 5,
            up_nodes: 5,
            cluster_health_pct: 100.0,
            schema_agreement: true,
            ..Default::default()
        };
        assert!(!healthy_cluster.is_critical_state());
    }

    #[test]
    fn test_critical_state_detection() {
        let cluster_info = CassandraClusterInfo {
            total_nodes: 4,
            up_nodes: 1,
            down_nodes: 3,
            cluster_health_pct: 25.0,
            ..Default::default()
        };

        assert!(cluster_info.is_critical_state());
        assert!(cluster_info.has_unhealthy_nodes());
    }

    #[test]
    fn test_schema_agreement_check() {
        let cluster_data = json!([
            {"schema_version": "abc123"},
            {"schema_version": "abc123"}
        ]);
        let local_data = json!([
            {"schema_version": "abc123"}
        ]);

        let (agreement, disagreement_count) = CassandraClusterInfo::check_schema_agreement(&cluster_data, &local_data).unwrap_or_default();

        assert!(agreement);
        assert_eq!(disagreement_count, 0);

        let cluster_data_disagreed = json!([
            {"schema_version": "abc123"},
            {"schema_version": "def456"}
        ]);

        let (agreement, disagreement_count) =
            CassandraClusterInfo::check_schema_agreement(&cluster_data_disagreed, &local_data).unwrap_or_default();

        assert!(!agreement);
        assert_eq!(disagreement_count, 1);
    }

    #[test]
    fn test_count_total_nodes() {
        let cluster_data = json!([
            {"peer": "192.168.1.2"},
            {"peer": "192.168.1.3"},
            {"peer": "192.168.1.4"}
        ]);

        let count = CassandraClusterInfo::count_total_nodes(&cluster_data).unwrap_or_default();
        assert_eq!(count, 4); // 3 peers + 1 local node
    }

    #[test]
    fn test_parse_node_status_details() {
        let data = json!([
            {
                "peer": "10.0.0.1",
                "data_center": "dc1",
                "rack": "rack1",
                "release_version": "4.0.0",
                "host_id": "host-uuid-1",
                "schema_version": "schema-uuid-1"
            },
            {
                "peer": "10.0.0.2",
                "data_center": "dc1",
                "rack": "rack2",
                "release_version": "4.0.0",
                "host_id": "host-uuid-2",
                "schema_version": "schema-uuid-1"
            }
        ]);

        let statuses = CassandraClusterInfo::parse_node_status_details(&data);
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0].peer_address, "10.0.0.1");
        assert_eq!(statuses[0].data_center, "dc1");
        assert_eq!(statuses[1].peer_address, "10.0.0.2");
        assert_eq!(statuses[1].rack, "rack2");
    }

    #[test]
    fn test_parse_schema_versions_peers() {
        let data = json!([
            {"peer": "10.0.0.1", "schema_version": "schema-a"},
            {"peer": "10.0.0.2", "schema_version": "schema-b"}
        ]);

        let versions = CassandraClusterInfo::parse_schema_versions_peers(&data);
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].node_address, "10.0.0.1");
        assert_eq!(versions[0].schema_version, "schema-a");
        assert_eq!(versions[1].node_address, "10.0.0.2");
        assert_eq!(versions[1].schema_version, "schema-b");
    }

    #[test]
    fn test_parse_schema_versions_local() {
        let data = json!([
            {"broadcast_address": "10.0.0.3", "schema_version": "schema-a"}
        ]);

        let versions = CassandraClusterInfo::parse_schema_versions_local(&data);
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].node_address, "10.0.0.3");
        assert_eq!(versions[0].schema_version, "schema-a");
    }
}
