//! Benchmark-facing Redis Cluster validation helpers.

use super::response::ClusterResponseRewriter;
use super::topology::ClusterNodesParser;
use super::*;

pub struct ClusterValidation;

impl ClusterValidation {
    pub fn parse_nodes_counts(raw: &str) -> (usize, usize) {
        let parsed = ClusterNodesParser::parse_with_warnings(raw);
        (parsed.nodes.len(), parsed.warnings.len())
    }

    pub fn rewrite_redirect_response(resp: &[u8]) -> Bytes {
        ClusterResponseRewriter::redirect_response(Bytes::copy_from_slice(resp), &Self::sample_topology())
    }

    pub fn rewrite_nodes_payload_len(raw: &str) -> usize {
        ClusterResponseRewriter::nodes_payload(raw, &Self::sample_topology()).len()
    }

    fn sample_topology() -> VirtualClusterTopology {
        let endpoint_uuid = EndpointCacheUuid::new(None, EndpointUuid::new_uuid());
        VirtualClusterTopology {
            endpoint_uuid,
            redis_config: RedisConfig::default(),
            advertise_host: "proxy.example.com".to_string(),
            nodes: vec![
                VirtualClusterNode {
                    listener_id: "n1".to_string(),
                    bind_port: 7000,
                    advertise_port: 17000,
                    stable_node_id: "eden-n1".to_string(),
                    role: ClusterProxyNodeRole::Master,
                    effective_slot_ranges: vec![(0, 8191)],
                    backend: ClusterProxyNode {
                        node_id: "node-1".to_string(),
                        host: "10.0.0.1".to_string(),
                        port: 6379,
                        bus_port: Some(16379),
                        role: ClusterProxyNodeRole::Master,
                        master_id: None,
                        flags: vec!["master".to_string()],
                        slot_ranges: vec![(0, 8191)],
                        connected: true,
                    },
                },
                VirtualClusterNode {
                    listener_id: "n2".to_string(),
                    bind_port: 7001,
                    advertise_port: 17001,
                    stable_node_id: "eden-n2".to_string(),
                    role: ClusterProxyNodeRole::Master,
                    effective_slot_ranges: vec![(8192, 16383)],
                    backend: ClusterProxyNode {
                        node_id: "node-2".to_string(),
                        host: "10.0.0.2".to_string(),
                        port: 6380,
                        bus_port: Some(16380),
                        role: ClusterProxyNodeRole::Master,
                        master_id: None,
                        flags: vec!["master".to_string()],
                        slot_ranges: vec![(8192, 16383)],
                        connected: true,
                    },
                },
            ],
        }
    }
}
