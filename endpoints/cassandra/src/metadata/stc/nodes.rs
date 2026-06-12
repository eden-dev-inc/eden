use crate::api::lib::QueryUnpagedInput;
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

use super::utils::{DEFAULT_QUERY_TIMEOUT, get_string, get_string_or, map_rows, run_named_query};

/// Cassandra node information and health metrics
///
/// Provides detailed information about individual nodes in the cluster,
/// including health status, resource utilization and performance metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraNodeInfo {
    /// Local node information
    pub local_node: CassandraLocalNode,
    /// Information about all peer nodes
    pub peer_nodes: Vec<CassandraPeerNode>,
    /// Total number of nodes in the cluster
    pub total_nodes: u64,
    /// Number of nodes that are up
    pub up_nodes: u64,
    /// Number of nodes that are down
    pub down_nodes: u64,
    /// Number of nodes in joining state
    pub joining_nodes: u64,
    /// Number of nodes in leaving state
    pub leaving_nodes: u64,
    /// Average node load across the cluster (GB)
    pub avg_cluster_load_gb: f64,
    /// Maximum node load in the cluster (GB)
    pub max_cluster_load_gb: f64,
    /// Minimum node load in the cluster (GB)
    pub min_cluster_load_gb: f64,
    /// Node resource utilization metrics
    pub resource_metrics: CassandraNodeResourceMetrics,
    /// Node performance metrics
    pub performance_metrics: CassandraNodePerformanceMetrics,
    /// Data center distribution
    pub datacenter_distribution: HashMap<String, u64>,
    /// Rack distribution
    pub rack_distribution: HashMap<String, u64>,
    /// Version distribution across nodes
    pub version_distribution: HashMap<String, u64>,
}

/// Information about the local Cassandra node
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraLocalNode {
    /// Node host ID
    pub host_id: String,
    /// Node IP address
    pub listen_address: String,
    /// RPC address for client connections
    pub rpc_address: String,
    /// Broadcast address
    pub broadcast_address: String,
    /// Data center name
    pub data_center: String,
    /// Rack name
    pub rack: String,
    /// Cassandra version
    pub release_version: String,
    /// Cluster name
    pub cluster_name: String,
    /// Partitioner being used
    pub partitioner: String,
    /// Schema version
    pub schema_version: String,
    /// Native protocol version
    pub native_protocol_version: String,
    /// CQL version
    pub cql_version: String,
    /// Thrift API version
    pub thrift_version: String,
    /// Node uptime in seconds (not available via CQL; requires JMX)
    pub uptime_seconds: u64,
    /// Current load on this node in GB (not available via CQL; requires JMX)
    pub load_gb: f64,
    /// Number of tokens owned by this node
    pub token_count: u64,
}

/// Information about a peer node in the cluster
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraPeerNode {
    /// Peer host ID
    pub host_id: String,
    /// Peer IP address
    pub peer_address: String,
    /// Preferred IP address
    pub preferred_ip: Option<String>,
    /// RPC address
    pub rpc_address: String,
    /// Data center name
    pub data_center: String,
    /// Rack name
    pub rack: String,
    /// Cassandra version
    pub release_version: String,
    /// Schema version
    pub schema_version: String,
    /// Node status (not available via CQL; requires gossip/JMX)
    pub status: String,
    /// Node state (not available via CQL; requires gossip/JMX)
    pub state: String,
    /// Current load on this node in GB (not available via CQL; requires JMX)
    pub load_gb: f64,
    /// Number of tokens owned by this node
    pub token_count: u64,
    /// Last seen timestamp
    pub last_seen: Option<String>,
    /// Network latency to this node (ms)
    pub network_latency_ms: Option<f64>,
}

/// Resource utilization metrics for nodes
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraNodeResourceMetrics {
    /// Current heap memory usage (MB)
    pub heap_memory_used_mb: f64,
    /// Maximum heap memory (MB)
    pub heap_memory_max_mb: f64,
    /// Heap memory utilization percentage
    pub heap_memory_utilization_pct: f64,
    /// Off-heap memory usage (MB)
    pub off_heap_memory_used_mb: f64,
    /// Current CPU utilization percentage
    pub cpu_utilization_pct: f64,
    /// Disk space used (GB)
    pub disk_used_gb: f64,
    /// Total disk space (GB)
    pub disk_total_gb: f64,
    /// Disk utilization percentage
    pub disk_utilization_pct: f64,
    /// Number of active client connections
    pub active_connections: u64,
    /// Maximum allowed connections
    pub max_connections: u64,
    /// Connection utilization percentage
    pub connection_utilization_pct: f64,
}

/// Performance metrics for nodes
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CassandraNodePerformanceMetrics {
    /// Read requests per second
    pub read_requests_per_sec: f64,
    /// Write requests per second
    pub write_requests_per_sec: f64,
    /// Average read latency (ms)
    pub avg_read_latency_ms: f64,
    /// Average write latency (ms)
    pub avg_write_latency_ms: f64,
    /// 99th percentile read latency (ms)
    pub p99_read_latency_ms: f64,
    /// 99th percentile write latency (ms)
    pub p99_write_latency_ms: f64,
    /// Number of timeouts in the last period
    pub timeout_count: u64,
    /// Number of dropped messages
    pub dropped_messages: u64,
    /// Compaction tasks running
    pub active_compactions: u64,
    /// Pending compaction tasks
    pub pending_compactions: u64,
    /// Cache hit ratio percentage
    pub cache_hit_ratio_pct: f64,
    /// Bloom filter false positive ratio
    pub bloom_filter_false_positive_ratio: f64,
}

impl MetadataCollection for CassandraNodeInfo {
    type Request = HashMap<String, QueryUnpagedInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "local_node".to_string(),
                QueryUnpagedInput::new(
                    "SELECT cluster_name, data_center, rack, release_version, schema_version,
                 partitioner, broadcast_address, listen_address, rpc_address, host_id,
                 native_protocol_version, cql_version
                 FROM system.local"
                        .to_string(),
                ),
            ),
            (
                "peer_nodes".to_string(),
                QueryUnpagedInput::new(
                    "SELECT host_id, peer, preferred_ip, rpc_address, data_center, rack,
                 release_version, schema_version, tokens
                 FROM system.peers"
                        .to_string(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return detailed Cassandra node information and health metrics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "node"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

impl CassandraNodeInfo {
    const DEFAULT_TOKEN_COUNT: u64 = 256;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: CassandraAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let requests = self.request();

        let (local_node_data, peer_nodes_data) = tokio::try_join!(
            run_named_query(&requests, "local_node", context.clone(), DEFAULT_QUERY_TIMEOUT),
            run_named_query(&requests, "peer_nodes", context.clone(), DEFAULT_QUERY_TIMEOUT),
        )?;

        let local_node = Self::process_local_node(&local_node_data);
        let peer_nodes = Self::process_peer_nodes(&peer_nodes_data);

        let mut node_info = CassandraNodeInfo { local_node, peer_nodes, ..Default::default() };

        Self::calculate_cluster_statistics(&mut node_info);
        Self::build_distribution_maps(&mut node_info);

        // resource_metrics and performance_metrics are not available via CQL;
        // they remain at zero-defaults until a JMX/metrics source is wired in.

        Ok(node_info)
    }

    fn process_local_node(local_data: &Value) -> CassandraLocalNode {
        let Value::Array(rows) = local_data else {
            return CassandraLocalNode { token_count: Self::DEFAULT_TOKEN_COUNT, ..Default::default() };
        };
        let Some(row) = rows.first() else {
            return CassandraLocalNode { token_count: Self::DEFAULT_TOKEN_COUNT, ..Default::default() };
        };

        CassandraLocalNode {
            host_id: get_string_or(row, "host_id", ""),
            listen_address: get_string_or(row, "listen_address", ""),
            rpc_address: get_string_or(row, "rpc_address", ""),
            broadcast_address: get_string_or(row, "broadcast_address", ""),
            data_center: get_string_or(row, "data_center", ""),
            rack: get_string_or(row, "rack", ""),
            release_version: get_string_or(row, "release_version", ""),
            cluster_name: get_string_or(row, "cluster_name", ""),
            partitioner: get_string_or(row, "partitioner", ""),
            schema_version: get_string_or(row, "schema_version", ""),
            native_protocol_version: get_string_or(row, "native_protocol_version", ""),
            cql_version: get_string_or(row, "cql_version", ""),
            thrift_version: get_string_or(row, "thrift_version", ""),
            // uptime_seconds and load_gb require JMX; default to 0/0.0.
            uptime_seconds: 0,
            load_gb: 0.0,
            token_count: Self::DEFAULT_TOKEN_COUNT,
        }
    }

    fn process_peer_nodes(peer_data: &Value) -> Vec<CassandraPeerNode> {
        map_rows(peer_data, |row| {
            let peer_address = get_string_or(row, "peer", "");
            Some(CassandraPeerNode {
                host_id: get_string_or(row, "host_id", ""),
                peer_address,
                preferred_ip: get_string(row, "preferred_ip"),
                rpc_address: get_string_or(row, "rpc_address", ""),
                data_center: get_string_or(row, "data_center", ""),
                rack: get_string_or(row, "rack", ""),
                release_version: get_string_or(row, "release_version", ""),
                schema_version: get_string_or(row, "schema_version", ""),
                // status and state are not available in system.peers (gossip/JMX only).
                status: "unknown".to_string(),
                state: "unknown".to_string(),
                // load_gb is not available in system.peers (JMX only).
                load_gb: 0.0,
                token_count: Self::count_tokens(row),
                last_seen: None,
                network_latency_ms: None,
            })
        })
    }

    fn calculate_cluster_statistics(node_info: &mut CassandraNodeInfo) {
        // +1 for the local node, which we connected to successfully.
        node_info.total_nodes = node_info.peer_nodes.len() as u64 + 1;

        // Status/state for peer nodes is not available via CQL.
        // Counts remain at 0 until a gossip/JMX source is integrated.
        node_info.up_nodes = 0;
        node_info.down_nodes = 0;
        node_info.joining_nodes = 0;
        node_info.leaving_nodes = 0;

        // All load values are 0.0 (JMX not yet wired in), so cluster load
        // statistics are also 0.0; this is an honest representation.
        node_info.avg_cluster_load_gb = 0.0;
        node_info.max_cluster_load_gb = 0.0;
        node_info.min_cluster_load_gb = 0.0;
    }

    fn build_distribution_maps(node_info: &mut CassandraNodeInfo) {
        // Data center distribution
        *node_info.datacenter_distribution.entry(node_info.local_node.data_center.clone()).or_insert(0) += 1;
        for peer in &node_info.peer_nodes {
            *node_info.datacenter_distribution.entry(peer.data_center.clone()).or_insert(0) += 1;
        }

        // Rack distribution (keyed as "datacenter.rack")
        let local_rack_key = format!("{}.{}", node_info.local_node.data_center, node_info.local_node.rack);
        *node_info.rack_distribution.entry(local_rack_key).or_insert(0) += 1;
        for peer in &node_info.peer_nodes {
            let rack_key = format!("{}.{}", peer.data_center, peer.rack);
            *node_info.rack_distribution.entry(rack_key).or_insert(0) += 1;
        }

        // Version distribution
        *node_info.version_distribution.entry(node_info.local_node.release_version.clone()).or_insert(0) += 1;
        for peer in &node_info.peer_nodes {
            *node_info.version_distribution.entry(peer.release_version.clone()).or_insert(0) += 1;
        }
    }

    fn count_tokens(row: &Value) -> u64 {
        match row.get("tokens") {
            Some(Value::Array(token_array)) => token_array.len() as u64,
            Some(Value::String(token_str)) => token_str.split(',').count() as u64,
            _ => Self::DEFAULT_TOKEN_COUNT,
        }
    }
}

impl CassandraNodeInfo {
    /// Gets the cluster health percentage based on up nodes
    pub fn cluster_health_percentage(&self) -> f64 {
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

    /// Gets the node with the highest load
    pub fn node_with_highest_load(&self) -> Option<String> {
        let local_load = self.local_node.load_gb;
        let local_address = &self.local_node.listen_address;

        let max_peer = self.peer_nodes.iter().max_by(|a, b| a.load_gb.partial_cmp(&b.load_gb).unwrap_or(std::cmp::Ordering::Equal));

        match max_peer {
            Some(peer) if peer.load_gb > local_load => Some(peer.peer_address.clone()),
            _ => Some(local_address.clone()),
        }
    }

    /// Gets the node with the lowest load
    pub fn node_with_lowest_load(&self) -> Option<String> {
        let local_load = self.local_node.load_gb;
        let local_address = &self.local_node.listen_address;

        let min_peer = self.peer_nodes.iter().min_by(|a, b| a.load_gb.partial_cmp(&b.load_gb).unwrap_or(std::cmp::Ordering::Equal));

        match min_peer {
            Some(peer) if peer.load_gb < local_load => Some(peer.peer_address.clone()),
            _ => Some(local_address.clone()),
        }
    }

    /// Checks if load distribution is balanced (within 20% variance)
    pub fn is_load_balanced(&self) -> bool {
        if self.avg_cluster_load_gb == 0.0 {
            return true;
        }

        let variance_threshold = 0.2;
        let max_allowed_load = self.avg_cluster_load_gb * (1.0 + variance_threshold);
        let min_allowed_load = self.avg_cluster_load_gb * (1.0 - variance_threshold);

        self.max_cluster_load_gb <= max_allowed_load && self.min_cluster_load_gb >= min_allowed_load
    }

    /// Gets the number of data centers in the cluster
    pub fn datacenter_count(&self) -> usize {
        self.datacenter_distribution.len()
    }

    /// Gets the number of racks in the cluster
    pub fn rack_count(&self) -> usize {
        self.rack_distribution.len()
    }

    /// Checks if all nodes are running the same version
    pub fn has_version_consistency(&self) -> bool {
        self.version_distribution.len() <= 1
    }

    /// Gets the most common Cassandra version
    pub fn primary_version(&self) -> Option<String> {
        self.version_distribution.iter().max_by_key(|&(_, &count)| count).map(|(version, _)| version.clone())
    }

    /// Gets nodes in a specific data center
    pub fn nodes_in_datacenter(&self, datacenter: &str) -> Vec<String> {
        let mut nodes = Vec::new();

        if self.local_node.data_center == datacenter {
            nodes.push(self.local_node.listen_address.clone());
        }

        for peer in &self.peer_nodes {
            if peer.data_center == datacenter {
                nodes.push(peer.peer_address.clone());
            }
        }

        nodes
    }

    /// Checks if resource utilization is healthy
    pub fn is_resource_healthy(&self) -> bool {
        self.resource_metrics.heap_memory_utilization_pct < 80.0
            && self.resource_metrics.cpu_utilization_pct < 80.0
            && self.resource_metrics.disk_utilization_pct < 80.0
            && self.resource_metrics.connection_utilization_pct < 80.0
    }

    /// Checks if performance metrics indicate healthy operation
    pub fn is_performance_healthy(&self) -> bool {
        self.performance_metrics.avg_read_latency_ms < 10.0
            && self.performance_metrics.avg_write_latency_ms < 5.0
            && self.performance_metrics.timeout_count < 100
            && self.performance_metrics.cache_hit_ratio_pct > 70.0
    }

    /// Gets the total cluster load in GB
    pub fn total_cluster_load_gb(&self) -> f64 {
        self.avg_cluster_load_gb * self.total_nodes as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_cluster_health_calculation() {
        let node_info = CassandraNodeInfo {
            total_nodes: 5,
            up_nodes: 4,
            down_nodes: 1,
            ..Default::default()
        };

        assert_eq!(node_info.cluster_health_percentage(), 80.0);
        assert!(node_info.has_unhealthy_nodes());
    }

    #[test]
    fn test_load_balance_check() {
        let mut node_info = CassandraNodeInfo {
            avg_cluster_load_gb: 100.0,
            max_cluster_load_gb: 110.0, // 10% variance
            min_cluster_load_gb: 95.0,  // 5% variance
            ..Default::default()
        };

        assert!(node_info.is_load_balanced());

        node_info.max_cluster_load_gb = 130.0; // 30% variance - too high
        assert!(!node_info.is_load_balanced());
    }

    #[test]
    fn test_version_consistency() {
        let mut node_info = CassandraNodeInfo::default();

        node_info.version_distribution.insert("4.0.0".to_string(), 5);
        assert!(node_info.has_version_consistency());
        assert_eq!(node_info.primary_version(), Some("4.0.0".to_string()));

        node_info.version_distribution.insert("3.11.0".to_string(), 2);
        assert!(!node_info.has_version_consistency());
        assert_eq!(node_info.primary_version(), Some("4.0.0".to_string()));
    }

    #[test]
    fn test_datacenter_operations() {
        let mut node_info = CassandraNodeInfo::default();
        node_info.local_node.data_center = "dc1".to_string();
        node_info.local_node.listen_address = "192.168.1.1".to_string();

        node_info.peer_nodes = vec![
            CassandraPeerNode {
                host_id: "host2".to_string(),
                peer_address: "192.168.1.2".to_string(),
                preferred_ip: None,
                rpc_address: "192.168.1.2".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                release_version: "4.0.0".to_string(),
                schema_version: "schema1".to_string(),
                status: "unknown".to_string(),
                state: "unknown".to_string(),
                load_gb: 0.0,
                token_count: 256,
                last_seen: None,
                network_latency_ms: None,
            },
            CassandraPeerNode {
                host_id: "host3".to_string(),
                peer_address: "192.168.1.3".to_string(),
                preferred_ip: None,
                rpc_address: "192.168.1.3".to_string(),
                data_center: "dc2".to_string(),
                rack: "rack1".to_string(),
                release_version: "4.0.0".to_string(),
                schema_version: "schema1".to_string(),
                status: "unknown".to_string(),
                state: "unknown".to_string(),
                load_gb: 0.0,
                token_count: 256,
                last_seen: None,
                network_latency_ms: None,
            },
        ];

        node_info.datacenter_distribution.insert("dc1".to_string(), 2);
        node_info.datacenter_distribution.insert("dc2".to_string(), 1);

        assert_eq!(node_info.datacenter_count(), 2);

        let dc1_nodes = node_info.nodes_in_datacenter("dc1");
        assert_eq!(dc1_nodes.len(), 2);
        assert!(dc1_nodes.contains(&"192.168.1.1".to_string()));
        assert!(dc1_nodes.contains(&"192.168.1.2".to_string()));

        let dc2_nodes = node_info.nodes_in_datacenter("dc2");
        assert_eq!(dc2_nodes.len(), 1);
        assert!(dc2_nodes.contains(&"192.168.1.3".to_string()));
    }

    #[test]
    fn test_load_extremes() {
        let mut node_info = CassandraNodeInfo::default();
        node_info.local_node.load_gb = 100.0;
        node_info.local_node.listen_address = "192.168.1.1".to_string();

        node_info.peer_nodes = vec![
            CassandraPeerNode {
                host_id: "host2".to_string(),
                peer_address: "192.168.1.2".to_string(),
                preferred_ip: None,
                rpc_address: "192.168.1.2".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                release_version: "4.0.0".to_string(),
                schema_version: "schema1".to_string(),
                status: "unknown".to_string(),
                state: "unknown".to_string(),
                load_gb: 150.0,
                token_count: 256,
                last_seen: None,
                network_latency_ms: None,
            },
            CassandraPeerNode {
                host_id: "host3".to_string(),
                peer_address: "192.168.1.3".to_string(),
                preferred_ip: None,
                rpc_address: "192.168.1.3".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack2".to_string(),
                release_version: "4.0.0".to_string(),
                schema_version: "schema1".to_string(),
                status: "unknown".to_string(),
                state: "unknown".to_string(),
                load_gb: 50.0,
                token_count: 256,
                last_seen: None,
                network_latency_ms: None,
            },
        ];

        assert_eq!(node_info.node_with_highest_load(), Some("192.168.1.2".to_string()));
        assert_eq!(node_info.node_with_lowest_load(), Some("192.168.1.3".to_string()));
    }

    #[test]
    fn test_health_checks() {
        let mut node_info = CassandraNodeInfo {
            resource_metrics: CassandraNodeResourceMetrics {
                heap_memory_utilization_pct: 60.0,
                cpu_utilization_pct: 50.0,
                disk_utilization_pct: 40.0,
                connection_utilization_pct: 30.0,
                ..Default::default()
            },
            performance_metrics: CassandraNodePerformanceMetrics {
                avg_read_latency_ms: 5.0,
                avg_write_latency_ms: 3.0,
                timeout_count: 10,
                cache_hit_ratio_pct: 85.0,
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(node_info.is_resource_healthy());
        assert!(node_info.is_performance_healthy());

        node_info.resource_metrics.heap_memory_utilization_pct = 90.0;
        assert!(!node_info.is_resource_healthy());

        node_info.performance_metrics.avg_read_latency_ms = 50.0;
        assert!(!node_info.is_performance_healthy());
    }

    #[test]
    fn test_process_local_node() {
        let local_data = json!([
            {
                "host_id": "12345-abcde",
                "listen_address": "192.168.1.1",
                "rpc_address": "192.168.1.1",
                "broadcast_address": "192.168.1.1",
                "data_center": "datacenter1",
                "rack": "rack1",
                "release_version": "4.0.0",
                "cluster_name": "test_cluster",
                "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
                "schema_version": "abc123-def456",
                "native_protocol_version": "v5",
                "cql_version": "3.4.5",
                "thrift_version": "20.1.0"
            }
        ]);

        let local_node = CassandraNodeInfo::process_local_node(&local_data);

        assert_eq!(local_node.host_id, "12345-abcde");
        assert_eq!(local_node.listen_address, "192.168.1.1");
        assert_eq!(local_node.data_center, "datacenter1");
        assert_eq!(local_node.rack, "rack1");
        assert_eq!(local_node.release_version, "4.0.0");
        assert_eq!(local_node.cluster_name, "test_cluster");
        assert_eq!(local_node.uptime_seconds, 0);
        assert_eq!(local_node.load_gb, 0.0);
        assert_eq!(local_node.token_count, 256);
    }

    #[test]
    fn test_process_local_node_empty() {
        let empty = json!([]);
        let local_node = CassandraNodeInfo::process_local_node(&empty);
        assert_eq!(local_node.host_id, "");
        assert_eq!(local_node.token_count, 256);
    }

    #[test]
    fn test_process_peer_nodes() {
        let peer_data = json!([
            {
                "host_id": "peer1-host-id",
                "peer": "192.168.1.2",
                "preferred_ip": "192.168.1.2",
                "rpc_address": "192.168.1.2",
                "data_center": "datacenter1",
                "rack": "rack1",
                "release_version": "4.0.0",
                "schema_version": "abc123-def456",
                "tokens": ["token1", "token2", "token3"]
            },
            {
                "host_id": "peer2-host-id",
                "peer": "192.168.1.3",
                "rpc_address": "192.168.1.3",
                "data_center": "datacenter2",
                "rack": "rack1",
                "release_version": "3.11.0",
                "schema_version": "abc123-def456",
                "tokens": "token1,token2,token3,token4"
            }
        ]);

        let peer_nodes = CassandraNodeInfo::process_peer_nodes(&peer_data);

        assert_eq!(peer_nodes.len(), 2);

        let peer1 = &peer_nodes[0];
        assert_eq!(peer1.host_id, "peer1-host-id");
        assert_eq!(peer1.peer_address, "192.168.1.2");
        assert_eq!(peer1.data_center, "datacenter1");
        assert_eq!(peer1.release_version, "4.0.0");
        assert_eq!(peer1.status, "unknown");
        assert_eq!(peer1.state, "unknown");
        assert_eq!(peer1.load_gb, 0.0);
        assert_eq!(peer1.token_count, 3);

        let peer2 = &peer_nodes[1];
        assert_eq!(peer2.host_id, "peer2-host-id");
        assert_eq!(peer2.peer_address, "192.168.1.3");
        assert_eq!(peer2.data_center, "datacenter2");
        assert_eq!(peer2.release_version, "3.11.0");
        assert_eq!(peer2.status, "unknown");
        assert_eq!(peer2.state, "unknown");
        assert_eq!(peer2.load_gb, 0.0);
        assert_eq!(peer2.token_count, 4);
    }

    #[test]
    fn test_token_counting() {
        let row_with_array = json!({"tokens": ["token1", "token2", "token3", "token4"]});
        assert_eq!(CassandraNodeInfo::count_tokens(&row_with_array), 4);

        let row_with_string = json!({"tokens": "token1,token2,token3"});
        assert_eq!(CassandraNodeInfo::count_tokens(&row_with_string), 3);

        let row_without_tokens = json!({"peer": "192.168.1.2"});
        assert_eq!(CassandraNodeInfo::count_tokens(&row_without_tokens), 256);
    }

    #[test]
    fn test_total_cluster_load() {
        let node_info = CassandraNodeInfo {
            total_nodes: 3,
            avg_cluster_load_gb: 75.0,
            ..Default::default()
        };

        assert_eq!(node_info.total_cluster_load_gb(), 225.0);
    }

    #[test]
    fn test_distribution_building() {
        let mut node_info = CassandraNodeInfo::default();
        node_info.local_node.data_center = "dc1".to_string();
        node_info.local_node.rack = "rack1".to_string();
        node_info.local_node.release_version = "4.0.0".to_string();

        node_info.peer_nodes = vec![
            CassandraPeerNode {
                host_id: "host2".to_string(),
                peer_address: "192.168.1.2".to_string(),
                preferred_ip: None,
                rpc_address: "192.168.1.2".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack2".to_string(),
                release_version: "4.0.0".to_string(),
                schema_version: "schema1".to_string(),
                status: "unknown".to_string(),
                state: "unknown".to_string(),
                load_gb: 0.0,
                token_count: 256,
                last_seen: None,
                network_latency_ms: None,
            },
            CassandraPeerNode {
                host_id: "host3".to_string(),
                peer_address: "192.168.1.3".to_string(),
                preferred_ip: None,
                rpc_address: "192.168.1.3".to_string(),
                data_center: "dc2".to_string(),
                rack: "rack1".to_string(),
                release_version: "3.11.0".to_string(),
                schema_version: "schema1".to_string(),
                status: "unknown".to_string(),
                state: "unknown".to_string(),
                load_gb: 0.0,
                token_count: 256,
                last_seen: None,
                network_latency_ms: None,
            },
        ];

        CassandraNodeInfo::build_distribution_maps(&mut node_info);

        assert_eq!(node_info.datacenter_distribution.get("dc1"), Some(&2));
        assert_eq!(node_info.datacenter_distribution.get("dc2"), Some(&1));

        assert_eq!(node_info.rack_distribution.get("dc1.rack1"), Some(&1));
        assert_eq!(node_info.rack_distribution.get("dc1.rack2"), Some(&1));
        assert_eq!(node_info.rack_distribution.get("dc2.rack1"), Some(&1));

        assert_eq!(node_info.version_distribution.get("4.0.0"), Some(&2));
        assert_eq!(node_info.version_distribution.get("3.11.0"), Some(&1));
    }
}
