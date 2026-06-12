use serde::{Deserialize, Serialize};

use super::ElasticacheTag;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSnapshotInput {
    pub snapshot_name: String,
    pub cache_cluster_id: Option<String>,
    pub replication_group_id: Option<String>,
    pub tags: Option<Vec<ElasticacheTag>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeSnapshotsInput {
    pub snapshot_name: Option<String>,
    pub cache_cluster_id: Option<String>,
    pub replication_group_id: Option<String>,
    pub snapshot_source: Option<String>,
    pub marker: Option<String>,
    pub max_records: Option<i32>,
    pub show_node_group_config: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteSnapshotInput {
    pub snapshot_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopySnapshotInput {
    pub source_snapshot_name: String,
    pub target_snapshot_name: String,
    pub target_bucket: Option<String>,
    pub kms_key_id: Option<String>,
    pub tags: Option<Vec<ElasticacheTag>>,
}
