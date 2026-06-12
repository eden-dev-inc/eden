use serde::{Deserialize, Serialize};

use super::RdsTag;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDbInstanceInput {
    pub db_instance_identifier: String,
    pub db_instance_class: String,
    pub engine: String,
    pub master_username: Option<String>,
    pub master_user_password: Option<String>,
    pub allocated_storage: Option<i32>,
    pub db_name: Option<String>,
    pub vpc_security_group_ids: Option<Vec<String>>,
    pub db_subnet_group_name: Option<String>,
    pub availability_zone: Option<String>,
    pub multi_az: Option<bool>,
    pub engine_version: Option<String>,
    pub auto_minor_version_upgrade: Option<bool>,
    pub publicly_accessible: Option<bool>,
    pub storage_type: Option<String>,
    pub port: Option<i32>,
    pub db_cluster_identifier: Option<String>,
    pub storage_encrypted: Option<bool>,
    pub kms_key_id: Option<String>,
    pub backup_retention_period: Option<i32>,
    pub preferred_backup_window: Option<String>,
    pub preferred_maintenance_window: Option<String>,
    pub db_parameter_group_name: Option<String>,
    pub deletion_protection: Option<bool>,
    pub tags: Option<Vec<RdsTag>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeDbInstancesInput {
    pub db_instance_identifier: Option<String>,
    pub marker: Option<String>,
    pub max_records: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyDbInstanceInput {
    pub db_instance_identifier: String,
    pub db_instance_class: Option<String>,
    pub allocated_storage: Option<i32>,
    pub master_user_password: Option<String>,
    pub vpc_security_group_ids: Option<Vec<String>>,
    pub multi_az: Option<bool>,
    pub engine_version: Option<String>,
    pub auto_minor_version_upgrade: Option<bool>,
    pub backup_retention_period: Option<i32>,
    pub preferred_backup_window: Option<String>,
    pub preferred_maintenance_window: Option<String>,
    pub apply_immediately: Option<bool>,
    pub storage_type: Option<String>,
    pub deletion_protection: Option<bool>,
    pub db_parameter_group_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteDbInstanceInput {
    pub db_instance_identifier: String,
    pub skip_final_snapshot: Option<bool>,
    pub final_db_snapshot_identifier: Option<String>,
    pub delete_automated_backups: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebootDbInstanceInput {
    pub db_instance_identifier: String,
    pub force_failover: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartDbInstanceInput {
    pub db_instance_identifier: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopDbInstanceInput {
    pub db_instance_identifier: String,
    pub db_snapshot_identifier: Option<String>,
}
