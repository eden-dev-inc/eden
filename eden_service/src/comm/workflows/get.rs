use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
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

/// Get a Workflow
/// **Permissions**: `ControlPerms::CONFIGURE` on the Workflow or Organization
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Workflows"],
    path="/workflows/{workflow}",
    operation_id = "get_workflow",
    responses((status = OK, body = WorkflowSchema))
)]
#[allow(clippy::too_many_arguments)]
pub async fn get(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    workflow: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    let output = get_workflow(&database, &CacheObjectType::from((Some(org_key.clone()), workflow.clone())), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(Response::new(output)).into()
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct Response(WorkflowSchema);

impl Response {
    fn new(schema: WorkflowSchema) -> Self {
        Self(schema)
    }
}

pub(crate) async fn get_workflow(
    db_manager: &EdenDb,
    cache_object: &CacheObjectType<WorkflowCacheUuid, WorkflowCacheId>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<WorkflowSchema> {
    <EdenDb as CacheFunctions<WorkflowSchema, WorkflowCacheUuid, WorkflowUuid, WorkflowCacheId, WorkflowId>>::get_from_cache(
        db_manager,
        cache_object,
        telemetry_wrapper,
    )
    .await
}
