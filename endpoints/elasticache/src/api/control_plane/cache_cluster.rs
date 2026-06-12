use serde::{Deserialize, Serialize};

use super::ElasticacheTag;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCacheClusterInput {
    pub cache_cluster_id: String,
    pub engine: String,
    pub cache_node_type: Option<String>,
    pub num_cache_nodes: Option<i32>,
    pub replication_group_id: Option<String>,
    pub snapshot_name: Option<String>,
    pub snapshot_arns: Option<Vec<String>>,
    pub preferred_availability_zone: Option<String>,
    pub preferred_availability_zones: Option<Vec<String>>,
    pub security_group_ids: Option<Vec<String>>,
    pub cache_subnet_group_name: Option<String>,
    pub engine_version: Option<String>,
    pub auto_minor_version_upgrade: Option<bool>,
    pub port: Option<i32>,
    pub notification_topic_arn: Option<String>,
    pub tags: Option<Vec<ElasticacheTag>>,
    pub user_group_ids: Option<Vec<String>>,
    pub transit_encryption_enabled: Option<bool>,
    pub at_rest_encryption_enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeCacheClustersInput {
    pub cache_cluster_id: Option<String>,
    pub marker: Option<String>,
    pub max_records: Option<i32>,
    pub show_cache_node_info: Option<bool>,
    pub show_cache_clusters_not_in_replication_groups: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyCacheClusterInput {
    pub cache_cluster_id: String,
    pub num_cache_nodes: Option<i32>,
    pub cache_node_type: Option<String>,
    pub engine_version: Option<String>,
    pub preferred_maintenance_window: Option<String>,
    pub notification_topic_arn: Option<String>,
    pub security_group_ids: Option<Vec<String>>,
    pub apply_immediately: Option<bool>,
    pub snapshot_retention_limit: Option<i32>,
    pub snapshot_window: Option<String>,
    pub user_group_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteCacheClusterInput {
    pub cache_cluster_id: String,
    pub final_snapshot_identifier: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebootCacheClusterInput {
    pub cache_cluster_id: String,
    pub cache_node_ids_to_reboot: Option<Vec<String>>,
}
