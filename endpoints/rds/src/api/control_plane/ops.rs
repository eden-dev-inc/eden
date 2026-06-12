use std::any::Any;

use async_trait::async_trait;
use ctor::ctor;
use endpoint_types::{EndpointOperation, EpRequest, Operation, OperationExecutor, OperationKind, RequestConstructor, RunOutput};
use ep_core::ReqType;
use error::EpError;
use postgres_core::{PostgresAsync, PostgresTx};
use serde::{Deserialize, Serialize};
use telemetry::TelemetryWrapper;

use crate::api::control_plane::{
    CopyDbSnapshotInput, CreateDbClusterInput, CreateDbInstanceInput, CreateDbParameterGroupInput, CreateDbSnapshotInput,
    CreateDbSubnetGroupInput, DeleteDbClusterInput, DeleteDbInstanceInput, DeleteDbParameterGroupInput, DeleteDbSnapshotInput,
    DeleteDbSubnetGroupInput, DescribeDbClustersInput, DescribeDbInstancesInput, DescribeDbParameterGroupsInput, DescribeDbSnapshotsInput,
    DescribeDbSubnetGroupsInput, ModifyDbClusterInput, ModifyDbInstanceInput, ModifyDbParameterGroupInput, ModifyDbSubnetGroupInput,
    RdsApi, RebootDbInstanceInput, StartDbInstanceInput, StopDbInstanceInput,
};
use crate::api::control_plane::{RdsControlPlaneClient, shared_client};
use crate::output::RdsControlPlaneOutput;
use crate::request::RdsRequest;
use crate::serde::register_operation;
use ep_core::EpOutput;
use ep_core::ToOutput;

#[async_trait]
pub trait ControlPlaneCall: Clone + Send + Sync + std::fmt::Debug + Serialize + for<'de> Deserialize<'de> + 'static {
    const API: RdsApi;
    const REQUEST_TYPE: ReqType;

    async fn call(&self, client: &RdsControlPlaneClient) -> Result<serde_json::Value, EpError>;
}

macro_rules! impl_control_plane_call {
    ($input:ty, $api:expr, $req_type:expr, $client_method:ident, $register_name:ident) => {
        #[async_trait]
        impl ControlPlaneCall for $input {
            const API: RdsApi = $api;
            const REQUEST_TYPE: ReqType = $req_type;

            async fn call(&self, client: &RdsControlPlaneClient) -> Result<serde_json::Value, EpError> {
                client.$client_method(self.clone()).await
            }
        }

        impl EndpointOperation for $input {}

        impl OperationKind<RdsApi> for $input {
            fn operation_kind() -> RdsApi {
                $api
            }
        }

        impl Operation<PostgresAsync, RdsApi, PostgresTx> for $input {
            fn kind(&self) -> RdsApi {
                $api
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn request_type(&self) -> ReqType {
                $req_type
            }

            fn as_operation(self: Box<Self>) -> Box<dyn Operation<PostgresAsync, RdsApi, PostgresTx>> {
                self
            }

            fn as_exec(&self) -> Option<&dyn OperationExecutor<PostgresAsync, RdsApi, PostgresTx>> {
                Some(self)
            }

            fn clone_box(&self) -> Box<dyn Operation<PostgresAsync, RdsApi, PostgresTx>> {
                Box::new(self.clone())
            }
        }

        impl OperationExecutor<PostgresAsync, RdsApi, PostgresTx> for $input {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn run_operation_request(&self, _context: PostgresAsync, mut telemetry_wrapper: TelemetryWrapper) -> RunOutput<'_> {
                let input = self.clone();
                Box::pin(async move {
                    let _ = &mut telemetry_wrapper;
                    let client = shared_client().await?;
                    let value = input.call(client).await?;
                    Ok(Box::new(RdsControlPlaneOutput(value).to_output()) as Box<dyn EpOutput>)
                })
            }

            fn run_operation_transaction(&self, _tx_context: &mut PostgresTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
        }

        impl TryInto<endpoint_types::request::EndpointRequestInput> for $input {
            type Error = EpError;

            fn try_into(self) -> Result<endpoint_types::request::EndpointRequestInput, Self::Error> {
                Ok(endpoint_types::request::EndpointRequestInput::new(
                    serde_json::to_value(Box::new(RdsRequest::new(Box::new(self))).as_request()).map_err(EpError::serde)?,
                ))
            }
        }

        #[ctor]
        fn $register_name() {
            register_operation::<$input>();
        }
    };
}

// ── DB Instances ─────────────────────────────────────────────────────────────
impl_control_plane_call!(
    CreateDbInstanceInput,
    RdsApi::CreateDbInstance,
    ReqType::Write,
    create_db_instance,
    register_create_db_instance
);
impl_control_plane_call!(
    DescribeDbInstancesInput,
    RdsApi::DescribeDbInstances,
    ReqType::Read,
    describe_db_instances,
    register_describe_db_instances
);
impl_control_plane_call!(
    ModifyDbInstanceInput,
    RdsApi::ModifyDbInstance,
    ReqType::Write,
    modify_db_instance,
    register_modify_db_instance
);
impl_control_plane_call!(
    DeleteDbInstanceInput,
    RdsApi::DeleteDbInstance,
    ReqType::Write,
    delete_db_instance,
    register_delete_db_instance
);
impl_control_plane_call!(
    RebootDbInstanceInput,
    RdsApi::RebootDbInstance,
    ReqType::Write,
    reboot_db_instance,
    register_reboot_db_instance
);
impl_control_plane_call!(
    StartDbInstanceInput,
    RdsApi::StartDbInstance,
    ReqType::Write,
    start_db_instance,
    register_start_db_instance
);
impl_control_plane_call!(
    StopDbInstanceInput,
    RdsApi::StopDbInstance,
    ReqType::Write,
    stop_db_instance,
    register_stop_db_instance
);

// ── DB Clusters ──────────────────────────────────────────────────────────────
impl_control_plane_call!(
    CreateDbClusterInput,
    RdsApi::CreateDbCluster,
    ReqType::Write,
    create_db_cluster,
    register_create_db_cluster
);
impl_control_plane_call!(
    DescribeDbClustersInput,
    RdsApi::DescribeDbClusters,
    ReqType::Read,
    describe_db_clusters,
    register_describe_db_clusters
);
impl_control_plane_call!(
    ModifyDbClusterInput,
    RdsApi::ModifyDbCluster,
    ReqType::Write,
    modify_db_cluster,
    register_modify_db_cluster
);
impl_control_plane_call!(
    DeleteDbClusterInput,
    RdsApi::DeleteDbCluster,
    ReqType::Write,
    delete_db_cluster,
    register_delete_db_cluster
);

// ── DB Snapshots ─────────────────────────────────────────────────────────────
impl_control_plane_call!(
    CreateDbSnapshotInput,
    RdsApi::CreateDbSnapshot,
    ReqType::Write,
    create_db_snapshot,
    register_create_db_snapshot
);
impl_control_plane_call!(
    DescribeDbSnapshotsInput,
    RdsApi::DescribeDbSnapshots,
    ReqType::Read,
    describe_db_snapshots,
    register_describe_db_snapshots
);
impl_control_plane_call!(
    DeleteDbSnapshotInput,
    RdsApi::DeleteDbSnapshot,
    ReqType::Write,
    delete_db_snapshot,
    register_delete_db_snapshot
);
impl_control_plane_call!(
    CopyDbSnapshotInput,
    RdsApi::CopyDbSnapshot,
    ReqType::Write,
    copy_db_snapshot,
    register_copy_db_snapshot
);

// ── DB Subnet Groups ────────────────────────────────────────────────────────
impl_control_plane_call!(
    CreateDbSubnetGroupInput,
    RdsApi::CreateDbSubnetGroup,
    ReqType::Write,
    create_db_subnet_group,
    register_create_db_subnet_group
);
impl_control_plane_call!(
    DescribeDbSubnetGroupsInput,
    RdsApi::DescribeDbSubnetGroups,
    ReqType::Read,
    describe_db_subnet_groups,
    register_describe_db_subnet_groups
);
impl_control_plane_call!(
    ModifyDbSubnetGroupInput,
    RdsApi::ModifyDbSubnetGroup,
    ReqType::Write,
    modify_db_subnet_group,
    register_modify_db_subnet_group
);
impl_control_plane_call!(
    DeleteDbSubnetGroupInput,
    RdsApi::DeleteDbSubnetGroup,
    ReqType::Write,
    delete_db_subnet_group,
    register_delete_db_subnet_group
);

// ── DB Parameter Groups ─────────────────────────────────────────────────────
impl_control_plane_call!(
    CreateDbParameterGroupInput,
    RdsApi::CreateDbParameterGroup,
    ReqType::Write,
    create_db_parameter_group,
    register_create_db_parameter_group
);
impl_control_plane_call!(
    DescribeDbParameterGroupsInput,
    RdsApi::DescribeDbParameterGroups,
    ReqType::Read,
    describe_db_parameter_groups,
    register_describe_db_parameter_groups
);
impl_control_plane_call!(
    ModifyDbParameterGroupInput,
    RdsApi::ModifyDbParameterGroup,
    ReqType::Write,
    modify_db_parameter_group,
    register_modify_db_parameter_group
);
impl_control_plane_call!(
    DeleteDbParameterGroupInput,
    RdsApi::DeleteDbParameterGroup,
    ReqType::Write,
    delete_db_parameter_group,
    register_delete_db_parameter_group
);
