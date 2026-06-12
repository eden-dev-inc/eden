use crate::api::{Deserialize, InfoInput, RedisJsonValue, Serialize};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct ClusterNodeInfo {
    pub id: String,
    pub ip: String,
    pub port: u32,
    pub flags: Vec<String>,
    pub master_id: Option<String>,
    pub ping_sent: u64,
    pub pong_recv: u64,
    pub config_epoch: u64,
    pub link_state: String,
    pub slots: Vec<RedisSlotRange>,
    /// Node health status
    pub health_status: NodeHealthStatus,
    /// Current latency in milliseconds
    pub latency_ms: Option<f64>,
    /// Memory usage in bytes
    pub memory_usage_bytes: Option<u64>,
    /// Number of connected clients
    pub connected_clients: Option<u32>,
    /// Node role in the cluster
    pub role: ClusterNodeRole,
    /// Last seen timestamp
    pub last_seen: Option<u64>,
}

/// Health status of a cluster node
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq)]
pub enum NodeHealthStatus {
    /// Node is healthy and responding normally
    Healthy,
    /// Node is unreachable or not responding
    Unreachable,
    /// Node has failed
    Failed,
    /// Node is joining the cluster
    Joining,
    /// Node is initializing
    Initializing,
    /// Node is overloaded (high memory, CPU, etc.)
    Overloaded,
}

/// Role of a node in the cluster
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq)]
pub enum ClusterNodeRole {
    /// Master node that owns slots
    Master,
    /// Replica node that replicates a master
    Replica,
    /// Unknown role
    Unknown,
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default)]
pub struct RedisClusterInfo {
    pub cluster_enabled: bool,
    pub cluster_state: String,
    pub cluster_slots_assigned: u32,
    pub cluster_slots_ok: u32,
    pub cluster_slots_pfail: u32,
    pub cluster_slots_fail: u32,
    pub cluster_known_nodes: u32,
    pub cluster_size: u32,
    pub cluster_current_epoch: u64,
    pub cluster_my_epoch: u64,
    pub cluster_stats_messages_ping_sent: u64,
    pub cluster_stats_messages_pong_sent: u64,
    pub cluster_stats_messages_meet_sent: u64,
    pub cluster_stats_messages_fail_sent: u64,
    pub cluster_stats_messages_publish_sent: u64,
    pub cluster_stats_messages_auth_req_sent: u64,
    pub cluster_stats_messages_auth_ack_sent: u64,
    pub cluster_stats_messages_sent: u64,
    pub cluster_stats_messages_ping_received: u64,
    pub cluster_stats_messages_pong_received: u64,
    pub cluster_stats_messages_meet_received: u64,
    pub cluster_stats_messages_fail_received: u64,
    pub cluster_stats_messages_publish_received: u64,
    pub cluster_stats_messages_auth_req_received: u64,
    pub cluster_stats_messages_auth_ack_received: u64,
    pub cluster_stats_messages_received: u64,
    pub cluster_node_id: String,
    pub cluster_node_slots: Vec<RedisSlotRange>,
    pub cluster_nodes: Vec<ClusterNodeInfo>,
}

impl RedisClusterInfo {
    /// Get all master nodes in the cluster
    pub fn get_master_nodes(&self) -> Vec<&ClusterNodeInfo> {
        self.cluster_nodes.iter().filter(|node| node.role == ClusterNodeRole::Master).collect()
    }

    /// Get all replica nodes in the cluster
    pub fn get_replica_nodes(&self) -> Vec<&ClusterNodeInfo> {
        self.cluster_nodes.iter().filter(|node| node.role == ClusterNodeRole::Replica).collect()
    }

    /// Get unhealthy nodes in the cluster
    pub fn get_unhealthy_nodes(&self) -> Vec<&ClusterNodeInfo> {
        self.cluster_nodes.iter().filter(|node| node.health_status != NodeHealthStatus::Healthy).collect()
    }

    /// Get the total number of slots assigned across all masters
    pub fn get_total_assigned_slots(&self) -> u32 {
        self.get_master_nodes()
            .iter()
            .map(|node| {
                node.slots.iter().map(|range| if range.end >= range.start { range.end - range.start + 1 } else { 0 }).sum::<u16>() as u32
            })
            .sum()
    }

    /// Check if the cluster has a quorum of healthy masters
    pub fn has_master_quorum(&self) -> bool {
        let masters = self.get_master_nodes();
        let healthy_masters = masters.iter().filter(|node| node.health_status == NodeHealthStatus::Healthy).count();

        // Need majority of masters to be healthy
        !masters.is_empty() && (healthy_masters as f32 / masters.len() as f32) > 0.5
    }

    /// Get cluster health summary
    pub fn get_health_summary(&self) -> ClusterHealthSummary {
        let total_nodes = self.cluster_nodes.len();
        let unhealthy_nodes = self.get_unhealthy_nodes().len();
        let masters = self.get_master_nodes().len();
        let replicas = self.get_replica_nodes().len();

        let health_percentage = if total_nodes > 0 {
            ((total_nodes - unhealthy_nodes) as f32 / total_nodes as f32) * 100.0
        } else {
            0.0
        };

        ClusterHealthSummary {
            total_nodes: total_nodes as u32,
            unhealthy_nodes: unhealthy_nodes as u32,
            healthy_percentage: health_percentage,
            master_nodes: masters as u32,
            replica_nodes: replicas as u32,
            cluster_state: self.cluster_state.clone(),
            slots_assigned: self.cluster_slots_assigned,
            slots_ok: self.cluster_slots_ok,
            slots_pfail: self.cluster_slots_pfail,
            slots_fail: self.cluster_slots_fail,
        }
    }
}

/// Summary of cluster health status
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct ClusterHealthSummary {
    pub total_nodes: u32,
    pub unhealthy_nodes: u32,
    pub healthy_percentage: f32,
    pub master_nodes: u32,
    pub replica_nodes: u32,
    pub cluster_state: String,
    pub slots_assigned: u32,
    pub slots_ok: u32,
    pub slots_pfail: u32,
    pub slots_fail: u32,
}

impl MetadataCollection for RedisClusterInfo {
    type Request = InfoInput;

    fn request(&self) -> Self::Request {
        Self::Request::new(Some(vec![RedisJsonValue::String("cluster".to_string())]))
    }
    fn description(&self) -> &'static str {
        "Return the cluster information for the Redis database"
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn category(&self) -> &'static str {
        "cluster"
    }
    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct RedisSlotRange {
    pub start: u16,
    pub end: u16,
}
