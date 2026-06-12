use serde::{Deserialize, Serialize};

use super::RdsTag;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDbSnapshotInput {
    pub db_snapshot_identifier: String,
    pub db_instance_identifier: String,
    pub tags: Option<Vec<RdsTag>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeDbSnapshotsInput {
    pub db_snapshot_identifier: Option<String>,
    pub db_instance_identifier: Option<String>,
    pub snapshot_type: Option<String>,
    pub marker: Option<String>,
    pub max_records: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteDbSnapshotInput {
    pub db_snapshot_identifier: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyDbSnapshotInput {
    pub source_db_snapshot_identifier: String,
    pub target_db_snapshot_identifier: String,
    pub kms_key_id: Option<String>,
    pub copy_tags: Option<bool>,
    pub tags: Option<Vec<RdsTag>>,
}
