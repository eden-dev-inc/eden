use serde::{Deserialize, Serialize};

use super::RdsTag;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDbClusterInput {
    pub db_cluster_identifier: String,
    pub engine: String,
    pub master_username: Option<String>,
    pub master_user_password: Option<String>,
    pub db_subnet_group_name: Option<String>,
    pub vpc_security_group_ids: Option<Vec<String>>,
    pub availability_zones: Option<Vec<String>>,
    pub engine_version: Option<String>,
    pub port: Option<i32>,
    pub database_name: Option<String>,
    pub backup_retention_period: Option<i32>,
    pub preferred_backup_window: Option<String>,
    pub preferred_maintenance_window: Option<String>,
    pub storage_encrypted: Option<bool>,
    pub kms_key_id: Option<String>,
    pub deletion_protection: Option<bool>,
    pub storage_type: Option<String>,
    pub tags: Option<Vec<RdsTag>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeDbClustersInput {
    pub db_cluster_identifier: Option<String>,
    pub marker: Option<String>,
    pub max_records: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyDbClusterInput {
    pub db_cluster_identifier: String,
    pub engine_version: Option<String>,
    pub master_user_password: Option<String>,
    pub vpc_security_group_ids: Option<Vec<String>>,
    pub backup_retention_period: Option<i32>,
    pub preferred_backup_window: Option<String>,
    pub preferred_maintenance_window: Option<String>,
    pub apply_immediately: Option<bool>,
    pub deletion_protection: Option<bool>,
    pub storage_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteDbClusterInput {
    pub db_cluster_identifier: String,
    pub skip_final_snapshot: Option<bool>,
    pub final_db_snapshot_identifier: Option<String>,
    pub delete_automated_backups: Option<bool>,
}
