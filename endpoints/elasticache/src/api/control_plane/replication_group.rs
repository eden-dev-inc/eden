use serde::{Deserialize, Serialize};

use super::ElasticacheTag;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateReplicationGroupInput {
    pub replication_group_id: String,
    pub replication_group_description: String,
    pub engine: Option<String>,
    pub cache_node_type: Option<String>,
    pub num_cache_clusters: Option<i32>,
    pub num_node_groups: Option<i32>,
    pub replicas_per_node_group: Option<i32>,
    pub automatic_failover_enabled: Option<bool>,
    pub multi_az_enabled: Option<bool>,
    pub user_group_ids: Option<Vec<String>>,
    pub security_group_ids: Option<Vec<String>>,
    pub cache_subnet_group_name: Option<String>,
    pub engine_version: Option<String>,
    pub snapshot_name: Option<String>,
    pub snapshot_arns: Option<Vec<String>>,
    pub transit_encryption_enabled: Option<bool>,
    pub at_rest_encryption_enabled: Option<bool>,
    pub tags: Option<Vec<ElasticacheTag>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeReplicationGroupsInput {
    pub replication_group_id: Option<String>,
    pub marker: Option<String>,
    pub max_records: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyReplicationGroupInput {
    pub replication_group_id: String,
    pub apply_immediately: Option<bool>,
    pub cache_node_type: Option<String>,
    pub engine_version: Option<String>,
    pub preferred_maintenance_window: Option<String>,
    pub notification_topic_arn: Option<String>,
    pub automatic_failover_enabled: Option<bool>,
    pub multi_az_enabled: Option<bool>,
    pub snapshotting_cluster_id: Option<String>,
    pub user_group_ids_to_add: Option<Vec<String>>,
    pub user_group_ids_to_remove: Option<Vec<String>>,
    pub security_group_ids: Option<Vec<String>>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteReplicationGroupInput {
    pub replication_group_id: String,
    pub retain_primary_cluster: Option<bool>,
    pub final_snapshot_identifier: Option<String>,
}
