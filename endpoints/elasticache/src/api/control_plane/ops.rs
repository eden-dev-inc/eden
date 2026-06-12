use std::any::Any;

use async_trait::async_trait;
use ctor::ctor;
use endpoint_types::{EndpointOperation, EpRequest, Operation, OperationExecutor, OperationKind, RequestConstructor, RunOutput};
use ep_core::ReqType;
use error::EpError;
use redis_core::{RedisAsync, RedisTx};
use serde::{Deserialize, Serialize};
use telemetry::TelemetryWrapper;

use crate::api::control_plane::{
    CopySnapshotInput, CreateCacheClusterInput, CreateReplicationGroupInput, CreateSnapshotInput, CreateUserGroupInput, CreateUserInput,
    DeleteCacheClusterInput, DeleteReplicationGroupInput, DeleteSnapshotInput, DeleteUserGroupInput, DeleteUserInput,
    DescribeCacheClustersInput, DescribeReplicationGroupsInput, DescribeSnapshotsInput, DescribeUserGroupsInput, DescribeUsersInput,
    ElasticacheApi, ModifyCacheClusterInput, ModifyReplicationGroupInput, ModifyUserGroupInput, ModifyUserInput, RebootCacheClusterInput,
};
use crate::api::control_plane::{ElasticacheControlPlaneClient, shared_client};
use crate::output::ElasticacheControlPlaneOutput;
use crate::request::ElasticacheRequest;
use crate::serde::register_operation;
use ep_core::EpOutput;
use ep_core::ToOutput;

#[async_trait]
pub trait ControlPlaneCall: Clone + Send + Sync + std::fmt::Debug + Serialize + for<'de> Deserialize<'de> + 'static {
    const API: ElasticacheApi;
    const REQUEST_TYPE: ReqType;

    async fn call(&self, client: &ElasticacheControlPlaneClient) -> Result<serde_json::Value, EpError>;
}

macro_rules! impl_control_plane_call {
    ($input:ty, $api:expr, $req_type:expr, $client_method:ident, $register_name:ident) => {
        #[async_trait]
        impl ControlPlaneCall for $input {
            const API: ElasticacheApi = $api;
            const REQUEST_TYPE: ReqType = $req_type;

            async fn call(&self, client: &ElasticacheControlPlaneClient) -> Result<serde_json::Value, EpError> {
                client.$client_method(self.clone()).await
            }
        }

        impl EndpointOperation for $input {}

        impl OperationKind<ElasticacheApi> for $input {
            fn operation_kind() -> ElasticacheApi {
                $api
            }
        }

        impl Operation<RedisAsync, ElasticacheApi, RedisTx> for $input {
            fn kind(&self) -> ElasticacheApi {
                $api
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn request_type(&self) -> ReqType {
                $req_type
            }

            fn as_operation(self: Box<Self>) -> Box<dyn Operation<RedisAsync, ElasticacheApi, RedisTx>> {
                self
            }

            fn as_exec(&self) -> Option<&dyn OperationExecutor<RedisAsync, ElasticacheApi, RedisTx>> {
                Some(self)
            }

            fn clone_box(&self) -> Box<dyn Operation<RedisAsync, ElasticacheApi, RedisTx>> {
                Box::new(self.clone())
            }
        }

        impl OperationExecutor<RedisAsync, ElasticacheApi, RedisTx> for $input {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn run_operation_request(&self, _context: RedisAsync, mut telemetry_wrapper: TelemetryWrapper) -> RunOutput<'_> {
                let input = self.clone();
                Box::pin(async move {
                    let _ = &mut telemetry_wrapper;
                    let client = shared_client().await?;
                    let value = input.call(client).await?;
                    Ok(Box::new(ElasticacheControlPlaneOutput(value).to_output()) as Box<dyn EpOutput>)
                })
            }

            fn run_operation_transaction(&self, _tx_context: &mut RedisTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
        }

        impl TryInto<endpoint_types::request::EndpointRequestInput> for $input {
            type Error = EpError;

            fn try_into(self) -> Result<endpoint_types::request::EndpointRequestInput, Self::Error> {
                Ok(endpoint_types::request::EndpointRequestInput::new(
                    serde_json::to_value(Box::new(ElasticacheRequest::new(Box::new(self))).as_request()).map_err(EpError::serde)?,
                ))
            }
        }

        #[ctor]
        fn $register_name() {
            register_operation::<$input>();
        }
    };
}

impl_control_plane_call!(
    CreateCacheClusterInput,
    ElasticacheApi::CreateCacheCluster,
    ReqType::Write,
    create_cache_cluster,
    register_create_cache_cluster
);
impl_control_plane_call!(
    DescribeCacheClustersInput,
    ElasticacheApi::DescribeCacheClusters,
    ReqType::Read,
    describe_cache_clusters,
    register_describe_cache_clusters
);
impl_control_plane_call!(
    ModifyCacheClusterInput,
    ElasticacheApi::ModifyCacheCluster,
    ReqType::Write,
    modify_cache_cluster,
    register_modify_cache_cluster
);
impl_control_plane_call!(
    DeleteCacheClusterInput,
    ElasticacheApi::DeleteCacheCluster,
    ReqType::Write,
    delete_cache_cluster,
    register_delete_cache_cluster
);
impl_control_plane_call!(
    RebootCacheClusterInput,
    ElasticacheApi::RebootCacheCluster,
    ReqType::Write,
    reboot_cache_cluster,
    register_reboot_cache_cluster
);
impl_control_plane_call!(
    CreateReplicationGroupInput,
    ElasticacheApi::CreateReplicationGroup,
    ReqType::Write,
    create_replication_group,
    register_create_replication_group
);
impl_control_plane_call!(
    DescribeReplicationGroupsInput,
    ElasticacheApi::DescribeReplicationGroups,
    ReqType::Read,
    describe_replication_groups,
    register_describe_replication_groups
);
impl_control_plane_call!(
    ModifyReplicationGroupInput,
    ElasticacheApi::ModifyReplicationGroup,
    ReqType::Write,
    modify_replication_group,
    register_modify_replication_group
);
impl_control_plane_call!(
    DeleteReplicationGroupInput,
    ElasticacheApi::DeleteReplicationGroup,
    ReqType::Write,
    delete_replication_group,
    register_delete_replication_group
);
impl_control_plane_call!(CreateUserInput, ElasticacheApi::CreateUser, ReqType::Write, create_user, register_create_user);
impl_control_plane_call!(
    DescribeUsersInput,
    ElasticacheApi::DescribeUsers,
    ReqType::Read,
    describe_users,
    register_describe_users
);
impl_control_plane_call!(ModifyUserInput, ElasticacheApi::ModifyUser, ReqType::Write, modify_user, register_modify_user);
impl_control_plane_call!(DeleteUserInput, ElasticacheApi::DeleteUser, ReqType::Write, delete_user, register_delete_user);
impl_control_plane_call!(
    CreateUserGroupInput,
    ElasticacheApi::CreateUserGroup,
    ReqType::Write,
    create_user_group,
    register_create_user_group
);
impl_control_plane_call!(
    DescribeUserGroupsInput,
    ElasticacheApi::DescribeUserGroups,
    ReqType::Read,
    describe_user_groups,
    register_describe_user_groups
);
impl_control_plane_call!(
    ModifyUserGroupInput,
    ElasticacheApi::ModifyUserGroup,
    ReqType::Write,
    modify_user_group,
    register_modify_user_group
);
impl_control_plane_call!(
    DeleteUserGroupInput,
    ElasticacheApi::DeleteUserGroup,
    ReqType::Write,
    delete_user_group,
    register_delete_user_group
);
impl_control_plane_call!(
    CreateSnapshotInput,
    ElasticacheApi::CreateSnapshot,
    ReqType::Write,
    create_snapshot,
    register_create_snapshot
);
impl_control_plane_call!(
    DescribeSnapshotsInput,
    ElasticacheApi::DescribeSnapshots,
    ReqType::Read,
    describe_snapshots,
    register_describe_snapshots
);
impl_control_plane_call!(
    DeleteSnapshotInput,
    ElasticacheApi::DeleteSnapshot,
    ReqType::Write,
    delete_snapshot,
    register_delete_snapshot
);
impl_control_plane_call!(
    CopySnapshotInput,
    ElasticacheApi::CopySnapshot,
    ReqType::Write,
    copy_snapshot,
    register_copy_snapshot
);
