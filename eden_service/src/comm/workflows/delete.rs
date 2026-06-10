use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::lib::{ClickhouseConn, PgConn, RedisConn};
use database::db::methods::delete::DeleteMethod;
use database::db::methods::delete::workflow::DeleteWorkflow;
use database::methods::delete::UuidsToUpdate;
use eden_core::auth::ParsedJwt;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::WorkflowCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, WorkflowCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, WorkflowId, WorkflowUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::workflow::WorkflowSchema;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Delete (disconnect) a Workflow
/// **Permissions**: `ControlPerms::CONFIGURE` on the Workflow or Organization
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Workflows"],
    path="/workflows/{workflow}",
    operation_id = "delete_workflow",
    responses((status = OK, body = String))
)]
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    workflow: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    let workflow_object = CacheObjectType::from((Some(org_key), workflow.clone()));

    let _workflow_uuid =
        <EdenDb as CacheFunctions<WorkflowSchema, WorkflowCacheUuid, WorkflowUuid, WorkflowCacheId, WorkflowId>>::get_uuid(
            &database,
            &workflow_object,
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let _ = delete_workflow(&database, workflow_object, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::<String>::ok("success").into()
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Response {}

pub(crate) async fn delete_workflow(
    db_manager: &EdenDb,
    cache_object: CacheObjectType<WorkflowCacheUuid, WorkflowCacheId>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<UuidsToUpdate> {
    let delete_workflow = <DeleteWorkflow as DeleteMethod<
        WorkflowSchema,
        WorkflowCacheUuid,
        WorkflowUuid,
        WorkflowCacheId,
        WorkflowId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::new(cache_object);

    <DeleteWorkflow as DeleteMethod<
        WorkflowSchema,
        WorkflowCacheUuid,
        WorkflowUuid,
        WorkflowCacheId,
        WorkflowId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::delete(&delete_workflow, db_manager, telemetry_wrapper)
    .await
}
