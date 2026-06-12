use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_elasticache::{Client, types::Tag};
use error::EpError;
use serde_json::{Value, json};
use tokio::sync::OnceCell;

use super::{
    CopySnapshotInput, CreateCacheClusterInput, CreateReplicationGroupInput, CreateSnapshotInput, CreateUserGroupInput, CreateUserInput,
    DeleteCacheClusterInput, DeleteReplicationGroupInput, DeleteSnapshotInput, DeleteUserGroupInput, DeleteUserInput,
    DescribeCacheClustersInput, DescribeReplicationGroupsInput, DescribeSnapshotsInput, DescribeUserGroupsInput, DescribeUsersInput,
    ElasticacheTag, ModifyCacheClusterInput, ModifyReplicationGroupInput, ModifyUserGroupInput, ModifyUserInput, RebootCacheClusterInput,
};

#[derive(Clone)]
pub struct ElasticacheControlPlaneClient {
    client: Client,
}

static CONTROL_PLANE_CLIENT: OnceCell<ElasticacheControlPlaneClient> = OnceCell::const_new();

impl ElasticacheControlPlaneClient {
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

    pub async fn create_cache_cluster(&self, input: CreateCacheClusterInput) -> Result<Value, EpError> {
        let mut req = self.client.create_cache_cluster().cache_cluster_id(input.cache_cluster_id).engine(input.engine);

        if let Some(value) = input.cache_node_type {
            req = req.cache_node_type(value);
        }
        if let Some(value) = input.num_cache_nodes {
            req = req.num_cache_nodes(value);
        }
        if let Some(value) = input.replication_group_id {
            req = req.replication_group_id(value);
        }
        if let Some(value) = input.snapshot_name {
            req = req.snapshot_name(value);
        }
        if let Some(value) = input.snapshot_arns {
            req = req.set_snapshot_arns(Some(value));
        }
        if let Some(value) = input.preferred_availability_zone {
            req = req.preferred_availability_zone(value);
        }
        if let Some(value) = input.preferred_availability_zones {
            req = req.set_preferred_availability_zones(Some(value));
        }
        if let Some(value) = input.security_group_ids {
            req = req.set_security_group_ids(Some(value));
        }
        if let Some(value) = input.cache_subnet_group_name {
            req = req.cache_subnet_group_name(value);
        }
        if let Some(value) = input.engine_version {
            req = req.engine_version(value);
        }
        if let Some(value) = input.auto_minor_version_upgrade {
            req = req.auto_minor_version_upgrade(value);
        }
        if let Some(value) = input.port {
            req = req.port(value);
        }
        if let Some(value) = input.notification_topic_arn {
            req = req.notification_topic_arn(value);
        }
        if let Some(value) = map_tags(input.tags) {
            req = req.set_tags(Some(value));
        }
        if input.user_group_ids.is_some() {
            return Err(EpError::request("user_group_ids not supported by CreateCacheCluster in aws-sdk-elasticache"));
        }
        if let Some(value) = input.transit_encryption_enabled {
            req = req.transit_encryption_enabled(value);
        }
        if input.at_rest_encryption_enabled.is_some() {
            return Err(EpError::request(
                "at_rest_encryption_enabled not supported by CreateCacheCluster in aws-sdk-elasticache",
            ));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("create_cache_cluster failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn describe_cache_clusters(&self, input: DescribeCacheClustersInput) -> Result<Value, EpError> {
        let mut req = self.client.describe_cache_clusters();

        if let Some(value) = input.cache_cluster_id {
            req = req.cache_cluster_id(value);
        }
        if let Some(value) = input.marker {
            req = req.marker(value);
        }
        if let Some(value) = input.max_records {
            req = req.max_records(value);
        }
        if let Some(value) = input.show_cache_node_info {
            req = req.show_cache_node_info(value);
        }
        if let Some(value) = input.show_cache_clusters_not_in_replication_groups {
            req = req.show_cache_clusters_not_in_replication_groups(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("describe_cache_clusters failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn modify_cache_cluster(&self, input: ModifyCacheClusterInput) -> Result<Value, EpError> {
        let mut req = self.client.modify_cache_cluster().cache_cluster_id(input.cache_cluster_id);

        if let Some(value) = input.num_cache_nodes {
            req = req.num_cache_nodes(value);
        }
        if let Some(value) = input.cache_node_type {
            req = req.cache_node_type(value);
        }
        if let Some(value) = input.engine_version {
            req = req.engine_version(value);
        }
        if let Some(value) = input.preferred_maintenance_window {
            req = req.preferred_maintenance_window(value);
        }
        if let Some(value) = input.notification_topic_arn {
            req = req.notification_topic_arn(value);
        }
        if let Some(value) = input.security_group_ids {
            req = req.set_security_group_ids(Some(value));
        }
        if let Some(value) = input.apply_immediately {
            req = req.apply_immediately(value);
        }
        if let Some(value) = input.snapshot_retention_limit {
            req = req.snapshot_retention_limit(value);
        }
        if let Some(value) = input.snapshot_window {
            req = req.snapshot_window(value);
        }
        if input.user_group_ids.is_some() {
            return Err(EpError::request("user_group_ids not supported by ModifyCacheCluster in aws-sdk-elasticache"));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("modify_cache_cluster failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn delete_cache_cluster(&self, input: DeleteCacheClusterInput) -> Result<Value, EpError> {
        let mut req = self.client.delete_cache_cluster().cache_cluster_id(input.cache_cluster_id);

        if let Some(value) = input.final_snapshot_identifier {
            req = req.final_snapshot_identifier(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("delete_cache_cluster failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn reboot_cache_cluster(&self, input: RebootCacheClusterInput) -> Result<Value, EpError> {
        let mut req = self.client.reboot_cache_cluster().cache_cluster_id(input.cache_cluster_id);

        if let Some(value) = input.cache_node_ids_to_reboot {
            req = req.set_cache_node_ids_to_reboot(Some(value));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("reboot_cache_cluster failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn create_replication_group(&self, input: CreateReplicationGroupInput) -> Result<Value, EpError> {
        let mut req = self
            .client
            .create_replication_group()
            .replication_group_id(input.replication_group_id)
            .replication_group_description(input.replication_group_description);

        if let Some(value) = input.engine {
            req = req.engine(value);
        }
        if let Some(value) = input.cache_node_type {
            req = req.cache_node_type(value);
        }
        if let Some(value) = input.num_cache_clusters {
            req = req.num_cache_clusters(value);
        }
        if let Some(value) = input.num_node_groups {
            req = req.num_node_groups(value);
        }
        if let Some(value) = input.replicas_per_node_group {
            req = req.replicas_per_node_group(value);
        }
        if let Some(value) = input.automatic_failover_enabled {
            req = req.automatic_failover_enabled(value);
        }
        if let Some(value) = input.multi_az_enabled {
            req = req.multi_az_enabled(value);
        }
        if let Some(value) = input.user_group_ids {
            req = req.set_user_group_ids(Some(value));
        }
        if let Some(value) = input.security_group_ids {
            req = req.set_security_group_ids(Some(value));
        }
        if let Some(value) = input.cache_subnet_group_name {
            req = req.cache_subnet_group_name(value);
        }
        if let Some(value) = input.engine_version {
            req = req.engine_version(value);
        }
        if let Some(value) = input.snapshot_name {
            req = req.snapshot_name(value);
        }
        if let Some(value) = input.snapshot_arns {
            req = req.set_snapshot_arns(Some(value));
        }
        if let Some(value) = input.transit_encryption_enabled {
            req = req.transit_encryption_enabled(value);
        }
        if let Some(value) = input.at_rest_encryption_enabled {
            req = req.at_rest_encryption_enabled(value);
        }
        if let Some(value) = map_tags(input.tags) {
            req = req.set_tags(Some(value));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("create_replication_group failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn describe_replication_groups(&self, input: DescribeReplicationGroupsInput) -> Result<Value, EpError> {
        let mut req = self.client.describe_replication_groups();

        if let Some(value) = input.replication_group_id {
            req = req.replication_group_id(value);
        }
        if let Some(value) = input.marker {
            req = req.marker(value);
        }
        if let Some(value) = input.max_records {
            req = req.max_records(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("describe_replication_groups failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn modify_replication_group(&self, input: ModifyReplicationGroupInput) -> Result<Value, EpError> {
        let mut req = self.client.modify_replication_group().replication_group_id(input.replication_group_id);

        if let Some(value) = input.apply_immediately {
            req = req.apply_immediately(value);
        }
        if let Some(value) = input.cache_node_type {
            req = req.cache_node_type(value);
        }
        if let Some(value) = input.engine_version {
            req = req.engine_version(value);
        }
        if let Some(value) = input.preferred_maintenance_window {
            req = req.preferred_maintenance_window(value);
        }
        if let Some(value) = input.notification_topic_arn {
            req = req.notification_topic_arn(value);
        }
        if let Some(value) = input.automatic_failover_enabled {
            req = req.automatic_failover_enabled(value);
        }
        if let Some(value) = input.multi_az_enabled {
            req = req.multi_az_enabled(value);
        }
        if let Some(value) = input.snapshotting_cluster_id {
            req = req.snapshotting_cluster_id(value);
        }
        if let Some(value) = input.user_group_ids_to_add {
            req = req.set_user_group_ids_to_add(Some(value));
        }
        if let Some(value) = input.user_group_ids_to_remove {
            req = req.set_user_group_ids_to_remove(Some(value));
        }
        if let Some(value) = input.security_group_ids {
            req = req.set_security_group_ids(Some(value));
        }
        if let Some(value) = input.description {
            req = req.replication_group_description(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("modify_replication_group failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn delete_replication_group(&self, input: DeleteReplicationGroupInput) -> Result<Value, EpError> {
        let mut req = self.client.delete_replication_group().replication_group_id(input.replication_group_id);

        if let Some(value) = input.retain_primary_cluster {
            req = req.retain_primary_cluster(value);
        }
        if let Some(value) = input.final_snapshot_identifier {
            req = req.final_snapshot_identifier(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("delete_replication_group failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn create_user(&self, input: CreateUserInput) -> Result<Value, EpError> {
        let mut req = self
            .client
            .create_user()
            .user_id(input.user_id)
            .user_name(input.user_name)
            .engine(input.engine)
            .access_string(input.access_string);

        if let Some(value) = input.passwords {
            req = req.set_passwords(Some(value));
        }
        if let Some(value) = input.no_password_required {
            req = req.no_password_required(value);
        }
        if let Some(value) = map_tags(input.tags) {
            req = req.set_tags(Some(value));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("create_user failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn describe_users(&self, input: DescribeUsersInput) -> Result<Value, EpError> {
        let mut req = self.client.describe_users();

        if let Some(value) = input.user_id {
            req = req.user_id(value);
        }
        if let Some(value) = input.marker {
            req = req.marker(value);
        }
        if let Some(value) = input.max_records {
            req = req.max_records(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("describe_users failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn modify_user(&self, input: ModifyUserInput) -> Result<Value, EpError> {
        let mut req = self.client.modify_user().user_id(input.user_id);

        if let Some(value) = input.access_string {
            req = req.access_string(value);
        }
        if input.append_passwords.is_some() || input.remove_passwords.is_some() {
            return Err(EpError::request(
                "append_passwords/remove_passwords not supported by ModifyUser in aws-sdk-elasticache",
            ));
        }
        if let Some(value) = input.no_password_required {
            req = req.no_password_required(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("modify_user failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn delete_user(&self, input: DeleteUserInput) -> Result<Value, EpError> {
        let resp = self
            .client
            .delete_user()
            .user_id(input.user_id)
            .send()
            .await
            .map_err(|e| EpError::request(format!("delete_user failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn create_user_group(&self, input: CreateUserGroupInput) -> Result<Value, EpError> {
        let mut req = self.client.create_user_group().user_group_id(input.user_group_id).engine(input.engine);

        if let Some(value) = input.user_ids {
            req = req.set_user_ids(Some(value));
        }
        if let Some(value) = map_tags(input.tags) {
            req = req.set_tags(Some(value));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("create_user_group failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn describe_user_groups(&self, input: DescribeUserGroupsInput) -> Result<Value, EpError> {
        let mut req = self.client.describe_user_groups();

        if let Some(value) = input.user_group_id {
            req = req.user_group_id(value);
        }
        if let Some(value) = input.marker {
            req = req.marker(value);
        }
        if let Some(value) = input.max_records {
            req = req.max_records(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("describe_user_groups failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn modify_user_group(&self, input: ModifyUserGroupInput) -> Result<Value, EpError> {
        let mut req = self.client.modify_user_group().user_group_id(input.user_group_id);

        if let Some(value) = input.user_ids_to_add {
            req = req.set_user_ids_to_add(Some(value));
        }
        if let Some(value) = input.user_ids_to_remove {
            req = req.set_user_ids_to_remove(Some(value));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("modify_user_group failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn delete_user_group(&self, input: DeleteUserGroupInput) -> Result<Value, EpError> {
        let resp = self
            .client
            .delete_user_group()
            .user_group_id(input.user_group_id)
            .send()
            .await
            .map_err(|e| EpError::request(format!("delete_user_group failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn create_snapshot(&self, input: CreateSnapshotInput) -> Result<Value, EpError> {
        let mut req = self.client.create_snapshot().snapshot_name(input.snapshot_name);

        if let Some(value) = input.cache_cluster_id {
            req = req.cache_cluster_id(value);
        }
        if let Some(value) = input.replication_group_id {
            req = req.replication_group_id(value);
        }
        if let Some(value) = map_tags(input.tags) {
            req = req.set_tags(Some(value));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("create_snapshot failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn describe_snapshots(&self, input: DescribeSnapshotsInput) -> Result<Value, EpError> {
        let mut req = self.client.describe_snapshots();

        if let Some(value) = input.snapshot_name {
            req = req.snapshot_name(value);
        }
        if let Some(value) = input.cache_cluster_id {
            req = req.cache_cluster_id(value);
        }
        if let Some(value) = input.replication_group_id {
            req = req.replication_group_id(value);
        }
        if let Some(value) = input.snapshot_source {
            req = req.snapshot_source(value);
        }
        if let Some(value) = input.marker {
            req = req.marker(value);
        }
        if let Some(value) = input.max_records {
            req = req.max_records(value);
        }
        if let Some(value) = input.show_node_group_config {
            req = req.show_node_group_config(value);
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("describe_snapshots failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn delete_snapshot(&self, input: DeleteSnapshotInput) -> Result<Value, EpError> {
        let resp = self
            .client
            .delete_snapshot()
            .snapshot_name(input.snapshot_name)
            .send()
            .await
            .map_err(|e| EpError::request(format!("delete_snapshot failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }

    pub async fn copy_snapshot(&self, input: CopySnapshotInput) -> Result<Value, EpError> {
        let mut req = self
            .client
            .copy_snapshot()
            .source_snapshot_name(input.source_snapshot_name)
            .target_snapshot_name(input.target_snapshot_name);

        if let Some(value) = input.target_bucket {
            req = req.target_bucket(value);
        }
        if let Some(value) = input.kms_key_id {
            req = req.kms_key_id(value);
        }
        if let Some(value) = map_tags(input.tags) {
            req = req.set_tags(Some(value));
        }

        let resp = req.send().await.map_err(|e| EpError::request(format!("copy_snapshot failed: {e}")))?;

        Ok(to_debug_json(&resp))
    }
}

pub async fn shared_client() -> Result<&'static ElasticacheControlPlaneClient, EpError> {
    CONTROL_PLANE_CLIENT.get_or_try_init(|| async { ElasticacheControlPlaneClient::from_env().await }).await
}

fn map_tags(tags: Option<Vec<ElasticacheTag>>) -> Option<Vec<Tag>> {
    tags.map(|tags| tags.into_iter().map(|tag| Tag::builder().key(tag.key).value(tag.value).build()).collect())
}

fn to_debug_json<T: std::fmt::Debug>(value: &T) -> Value {
    json!({ "debug": format!("{value:?}") })
}
