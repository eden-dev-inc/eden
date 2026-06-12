use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_rds::Client;
use aws_sdk_rds::types::{Parameter, Tag};
use error::EpError;
use serde_json::{Value, json};
use tokio::sync::OnceCell;

use super::{
    CopyDbSnapshotInput, CreateDbClusterInput, CreateDbInstanceInput, CreateDbParameterGroupInput, CreateDbSnapshotInput,
    CreateDbSubnetGroupInput, DeleteDbClusterInput, DeleteDbInstanceInput, DeleteDbParameterGroupInput, DeleteDbSnapshotInput,
    DeleteDbSubnetGroupInput, DescribeDbClustersInput, DescribeDbInstancesInput, DescribeDbParameterGroupsInput, DescribeDbSnapshotsInput,
    DescribeDbSubnetGroupsInput, ModifyDbClusterInput, ModifyDbInstanceInput, ModifyDbParameterGroupInput, ModifyDbSubnetGroupInput,
    RdsTag, RebootDbInstanceInput, StartDbInstanceInput, StopDbInstanceInput,
};

#[derive(Clone)]
pub struct RdsControlPlaneClient {
    client: Client,
}

static CONTROL_PLANE_CLIENT: OnceCell<RdsControlPlaneClient> = OnceCell::const_new();

impl RdsControlPlaneClient {
    pub async fn from_env() -> Result<Self, EpError> {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        Ok(Self { client: Client::new(&config) })
    }

    pub fn from_sdk_config(config: &SdkConfig) -> Self {
        Self { client: Client::new(config) }
    }

    pub fn inner(&self) -> &Client {
        &self.client
    }

    // ── DB Instances ─────────────────────────────────────────────────────

    pub async fn create_db_instance(&self, input: CreateDbInstanceInput) -> Result<Value, EpError> {
        let mut req = self
            .client
            .create_db_instance()
            .db_instance_identifier(input.db_instance_identifier)
            .db_instance_class(input.db_instance_class)
            .engine(input.engine);

        if let Some(value) = input.master_username {
            req = req.master_username(value);
        }
        if let Some(value) = input.master_user_password {
            req = req.master_user_password(value);
        }
        if let Some(value) = input.allocated_storage {
            req = req.allocated_storage(value);
        }
        if let Some(value) = input.db_name {
            req = req.db_name(value);
        }
        if let Some(value) = input.vpc_security_group_ids {
            req = req.set_vpc_security_group_ids(Some(value));
        }
        if let Some(value) = input.db_subnet_group_name {
            req = req.db_subnet_group_name(value);
        }
        if let Some(value) = input.availability_zone {
            req = req.availability_zone(value);
        }
        if let Some(value) = input.multi_az {
            req = req.multi_az(value);
        }
        if let Some(value) = input.engine_version {
            req = req.engine_version(value);
        }
        if let Some(value) = input.auto_minor_version_upgrade {
            req = req.auto_minor_version_upgrade(value);
        }
        if let Some(value) = input.publicly_accessible {
            req = req.publicly_accessible(value);
        }
        if let Some(value) = input.storage_type {
            req = req.storage_type(value);
        }
        if let Some(value) = input.port {
            req = req.port(value);
        }
        if let Some(value) = input.db_cluster_identifier {
            req = req.db_cluster_identifier(value);
        }
        if let Some(value) = input.storage_encrypted {
            req = req.storage_encrypted(value);
        }
        if let Some(value) = input.kms_key_id {
            req = req.kms_key_id(value);
        }
        if let Some(value) = input.backup_retention_period {
            req = req.backup_retention_period(value);
        }
        if let Some(value) = input.preferred_backup_window {
            req = req.preferred_backup_window(value);
        }
        if let Some(value) = input.preferred_maintenance_window {
            req = req.preferred_maintenance_window(value);
        }
        if let Some(value) = input.db_parameter_group_name {
            req = req.db_parameter_group_name(value);
        }
        if let Some(value) = input.deletion_protection {
            req = req.deletion_protection(value);
        }
        if let Some(value) = map_tags(input.tags) {
            req = req.set_tags(Some(value));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("create_db_instance failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn describe_db_instances(&self, input: DescribeDbInstancesInput) -> Result<Value, EpError> {
        let mut req = self.client.describe_db_instances();

        if let Some(value) = input.db_instance_identifier {
            req = req.db_instance_identifier(value);
        }
        if let Some(value) = input.marker {
            req = req.marker(value);
        }
        if let Some(value) = input.max_records {
            req = req.max_records(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("describe_db_instances failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn modify_db_instance(&self, input: ModifyDbInstanceInput) -> Result<Value, EpError> {
        let mut req = self.client.modify_db_instance().db_instance_identifier(input.db_instance_identifier);

        if let Some(value) = input.db_instance_class {
            req = req.db_instance_class(value);
        }
        if let Some(value) = input.allocated_storage {
            req = req.allocated_storage(value);
        }
        if let Some(value) = input.master_user_password {
            req = req.master_user_password(value);
        }
        if let Some(value) = input.vpc_security_group_ids {
            req = req.set_vpc_security_group_ids(Some(value));
        }
        if let Some(value) = input.multi_az {
            req = req.multi_az(value);
        }
        if let Some(value) = input.engine_version {
            req = req.engine_version(value);
        }
        if let Some(value) = input.auto_minor_version_upgrade {
            req = req.auto_minor_version_upgrade(value);
        }
        if let Some(value) = input.backup_retention_period {
            req = req.backup_retention_period(value);
        }
        if let Some(value) = input.preferred_backup_window {
            req = req.preferred_backup_window(value);
        }
        if let Some(value) = input.preferred_maintenance_window {
            req = req.preferred_maintenance_window(value);
        }
        if let Some(value) = input.apply_immediately {
            req = req.apply_immediately(value);
        }
        if let Some(value) = input.storage_type {
            req = req.storage_type(value);
        }
        if let Some(value) = input.deletion_protection {
            req = req.deletion_protection(value);
        }
        if let Some(value) = input.db_parameter_group_name {
            req = req.db_parameter_group_name(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("modify_db_instance failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn delete_db_instance(&self, input: DeleteDbInstanceInput) -> Result<Value, EpError> {
        let mut req = self.client.delete_db_instance().db_instance_identifier(input.db_instance_identifier);

        if let Some(value) = input.skip_final_snapshot {
            req = req.skip_final_snapshot(value);
        }
        if let Some(value) = input.final_db_snapshot_identifier {
            req = req.final_db_snapshot_identifier(value);
        }
        if let Some(value) = input.delete_automated_backups {
            req = req.delete_automated_backups(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("delete_db_instance failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn reboot_db_instance(&self, input: RebootDbInstanceInput) -> Result<Value, EpError> {
        let mut req = self.client.reboot_db_instance().db_instance_identifier(input.db_instance_identifier);

        if let Some(value) = input.force_failover {
            req = req.force_failover(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("reboot_db_instance failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn start_db_instance(&self, input: StartDbInstanceInput) -> Result<Value, EpError> {
        let resp = self
            .client
            .start_db_instance()
            .db_instance_identifier(input.db_instance_identifier)
            .send()
            .await
            .map_err(|e| EpError::request(format!("start_db_instance failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn stop_db_instance(&self, input: StopDbInstanceInput) -> Result<Value, EpError> {
        let mut req = self.client.stop_db_instance().db_instance_identifier(input.db_instance_identifier);

        if let Some(value) = input.db_snapshot_identifier {
            req = req.db_snapshot_identifier(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("stop_db_instance failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    // ── DB Clusters ──────────────────────────────────────────────────────

    pub async fn create_db_cluster(&self, input: CreateDbClusterInput) -> Result<Value, EpError> {
        let mut req = self.client.create_db_cluster().db_cluster_identifier(input.db_cluster_identifier).engine(input.engine);

        if let Some(value) = input.master_username {
            req = req.master_username(value);
        }
        if let Some(value) = input.master_user_password {
            req = req.master_user_password(value);
        }
        if let Some(value) = input.db_subnet_group_name {
            req = req.db_subnet_group_name(value);
        }
        if let Some(value) = input.vpc_security_group_ids {
            req = req.set_vpc_security_group_ids(Some(value));
        }
        if let Some(value) = input.availability_zones {
            req = req.set_availability_zones(Some(value));
        }
        if let Some(value) = input.engine_version {
            req = req.engine_version(value);
        }
        if let Some(value) = input.port {
            req = req.port(value);
        }
        if let Some(value) = input.database_name {
            req = req.database_name(value);
        }
        if let Some(value) = input.backup_retention_period {
            req = req.backup_retention_period(value);
        }
        if let Some(value) = input.preferred_backup_window {
            req = req.preferred_backup_window(value);
        }
        if let Some(value) = input.preferred_maintenance_window {
            req = req.preferred_maintenance_window(value);
        }
        if let Some(value) = input.storage_encrypted {
            req = req.storage_encrypted(value);
        }
        if let Some(value) = input.kms_key_id {
            req = req.kms_key_id(value);
        }
        if let Some(value) = input.deletion_protection {
            req = req.deletion_protection(value);
        }
        if let Some(value) = input.storage_type {
            req = req.storage_type(value);
        }
        if let Some(value) = map_tags(input.tags) {
            req = req.set_tags(Some(value));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("create_db_cluster failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn describe_db_clusters(&self, input: DescribeDbClustersInput) -> Result<Value, EpError> {
        let mut req = self.client.describe_db_clusters();

        if let Some(value) = input.db_cluster_identifier {
            req = req.db_cluster_identifier(value);
        }
        if let Some(value) = input.marker {
            req = req.marker(value);
        }
        if let Some(value) = input.max_records {
            req = req.max_records(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("describe_db_clusters failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn modify_db_cluster(&self, input: ModifyDbClusterInput) -> Result<Value, EpError> {
        let mut req = self.client.modify_db_cluster().db_cluster_identifier(input.db_cluster_identifier);

        if let Some(value) = input.engine_version {
            req = req.engine_version(value);
        }
        if let Some(value) = input.master_user_password {
            req = req.master_user_password(value);
        }
        if let Some(value) = input.vpc_security_group_ids {
            req = req.set_vpc_security_group_ids(Some(value));
        }
        if let Some(value) = input.backup_retention_period {
            req = req.backup_retention_period(value);
        }
        if let Some(value) = input.preferred_backup_window {
            req = req.preferred_backup_window(value);
        }
        if let Some(value) = input.preferred_maintenance_window {
            req = req.preferred_maintenance_window(value);
        }
        if let Some(value) = input.apply_immediately {
            req = req.apply_immediately(value);
        }
        if let Some(value) = input.deletion_protection {
            req = req.deletion_protection(value);
        }
        if let Some(value) = input.storage_type {
            req = req.storage_type(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("modify_db_cluster failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn delete_db_cluster(&self, input: DeleteDbClusterInput) -> Result<Value, EpError> {
        let mut req = self.client.delete_db_cluster().db_cluster_identifier(input.db_cluster_identifier);

        if let Some(value) = input.skip_final_snapshot {
            req = req.skip_final_snapshot(value);
        }
        if let Some(value) = input.final_db_snapshot_identifier {
            req = req.final_db_snapshot_identifier(value);
        }
        if let Some(value) = input.delete_automated_backups {
            req = req.delete_automated_backups(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("delete_db_cluster failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    // ── DB Snapshots ─────────────────────────────────────────────────────

    pub async fn create_db_snapshot(&self, input: CreateDbSnapshotInput) -> Result<Value, EpError> {
        let mut req = self
            .client
            .create_db_snapshot()
            .db_snapshot_identifier(input.db_snapshot_identifier)
            .db_instance_identifier(input.db_instance_identifier);

        if let Some(value) = map_tags(input.tags) {
            req = req.set_tags(Some(value));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("create_db_snapshot failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn describe_db_snapshots(&self, input: DescribeDbSnapshotsInput) -> Result<Value, EpError> {
        let mut req = self.client.describe_db_snapshots();

        if let Some(value) = input.db_snapshot_identifier {
            req = req.db_snapshot_identifier(value);
        }
        if let Some(value) = input.db_instance_identifier {
            req = req.db_instance_identifier(value);
        }
        if let Some(value) = input.snapshot_type {
            req = req.snapshot_type(value);
        }
        if let Some(value) = input.marker {
            req = req.marker(value);
        }
        if let Some(value) = input.max_records {
            req = req.max_records(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("describe_db_snapshots failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn delete_db_snapshot(&self, input: DeleteDbSnapshotInput) -> Result<Value, EpError> {
        let resp = self
            .client
            .delete_db_snapshot()
            .db_snapshot_identifier(input.db_snapshot_identifier)
            .send()
            .await
            .map_err(|e| EpError::request(format!("delete_db_snapshot failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn copy_db_snapshot(&self, input: CopyDbSnapshotInput) -> Result<Value, EpError> {
        let mut req = self
            .client
            .copy_db_snapshot()
            .source_db_snapshot_identifier(input.source_db_snapshot_identifier)
            .target_db_snapshot_identifier(input.target_db_snapshot_identifier);

        if let Some(value) = input.kms_key_id {
            req = req.kms_key_id(value);
        }
        if let Some(value) = input.copy_tags {
            req = req.copy_tags(value);
        }
        if let Some(value) = map_tags(input.tags) {
            req = req.set_tags(Some(value));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("copy_db_snapshot failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    // ── DB Subnet Groups ─────────────────────────────────────────────────

    pub async fn create_db_subnet_group(&self, input: CreateDbSubnetGroupInput) -> Result<Value, EpError> {
        let mut req = self
            .client
            .create_db_subnet_group()
            .db_subnet_group_name(input.db_subnet_group_name)
            .db_subnet_group_description(input.db_subnet_group_description)
            .set_subnet_ids(Some(input.subnet_ids));

        if let Some(value) = map_tags(input.tags) {
            req = req.set_tags(Some(value));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("create_db_subnet_group failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn describe_db_subnet_groups(&self, input: DescribeDbSubnetGroupsInput) -> Result<Value, EpError> {
        let mut req = self.client.describe_db_subnet_groups();

        if let Some(value) = input.db_subnet_group_name {
            req = req.db_subnet_group_name(value);
        }
        if let Some(value) = input.marker {
            req = req.marker(value);
        }
        if let Some(value) = input.max_records {
            req = req.max_records(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("describe_db_subnet_groups failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn modify_db_subnet_group(&self, input: ModifyDbSubnetGroupInput) -> Result<Value, EpError> {
        let mut req = self
            .client
            .modify_db_subnet_group()
            .db_subnet_group_name(input.db_subnet_group_name)
            .set_subnet_ids(Some(input.subnet_ids));

        if let Some(value) = input.db_subnet_group_description {
            req = req.db_subnet_group_description(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("modify_db_subnet_group failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn delete_db_subnet_group(&self, input: DeleteDbSubnetGroupInput) -> Result<Value, EpError> {
        let resp = self
            .client
            .delete_db_subnet_group()
            .db_subnet_group_name(input.db_subnet_group_name)
            .send()
            .await
            .map_err(|e| EpError::request(format!("delete_db_subnet_group failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    // ── DB Parameter Groups ──────────────────────────────────────────────

    pub async fn create_db_parameter_group(&self, input: CreateDbParameterGroupInput) -> Result<Value, EpError> {
        let mut req = self
            .client
            .create_db_parameter_group()
            .db_parameter_group_name(input.db_parameter_group_name)
            .db_parameter_group_family(input.db_parameter_group_family)
            .description(input.description);

        if let Some(value) = map_tags(input.tags) {
            req = req.set_tags(Some(value));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("create_db_parameter_group failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn describe_db_parameter_groups(&self, input: DescribeDbParameterGroupsInput) -> Result<Value, EpError> {
        let mut req = self.client.describe_db_parameter_groups();

        if let Some(value) = input.db_parameter_group_name {
            req = req.db_parameter_group_name(value);
        }
        if let Some(value) = input.marker {
            req = req.marker(value);
        }
        if let Some(value) = input.max_records {
            req = req.max_records(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("describe_db_parameter_groups failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn modify_db_parameter_group(&self, input: ModifyDbParameterGroupInput) -> Result<Value, EpError> {
        let params: Vec<Parameter> = input
            .parameters
            .into_iter()
            .map(|p| {
                let mut builder = Parameter::builder().parameter_name(p.parameter_name).parameter_value(p.parameter_value);
                if let Some(method) = p.apply_method {
                    builder = builder.apply_method(method.as_str().into());
                }
                builder.build()
            })
            .collect();

        let resp = self
            .client
            .modify_db_parameter_group()
            .db_parameter_group_name(input.db_parameter_group_name)
            .set_parameters(Some(params))
            .send()
            .await
            .map_err(|e| EpError::request(format!("modify_db_parameter_group failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn delete_db_parameter_group(&self, input: DeleteDbParameterGroupInput) -> Result<Value, EpError> {
        let resp = self
            .client
            .delete_db_parameter_group()
            .db_parameter_group_name(input.db_parameter_group_name)
            .send()
            .await
            .map_err(|e| EpError::request(format!("delete_db_parameter_group failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }
}

pub async fn shared_client() -> Result<&'static RdsControlPlaneClient, EpError> {
    CONTROL_PLANE_CLIENT.get_or_try_init(|| async { RdsControlPlaneClient::from_env().await }).await
}

fn map_tags(tags: Option<Vec<RdsTag>>) -> Option<Vec<Tag>> {
    tags.map(|tags| tags.into_iter().map(|tag| Tag::builder().key(tag.key).value(tag.value).build()).collect())
}

fn to_debug_json<T: std::fmt::Debug>(value: &T) -> Value {
    json!({ "debug": format!("{value:?}") })
}
