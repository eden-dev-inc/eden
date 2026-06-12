use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::WorkflowCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, WorkflowCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, IdKind, WorkflowId, WorkflowUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::workflow::WorkflowSchema;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: `ControlPerms::GRANT | ControlPerms::DESTROY` on Organization
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["RBAC"],
    path="/iam/control/workflows/{workflow}",
    operation_id = "delete_rbac_workflow",
    responses((status = OK, body = String))
)]
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let entity = input.into_inner();

    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let workflow_cache =
        <EdenDb as CacheFunctions<WorkflowSchema, WorkflowCacheUuid, WorkflowUuid, WorkflowCacheId, WorkflowId>>::get_cache_uuid(
            &database,
            &CacheObjectType::from((Some(org_key.clone()), entity)),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT | ControlPerms::DESTROY, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let version_ms = chrono::Utc::now().timestamp_millis();

    database
        .control_plane_remove_entity(org_key.uuid(), IdKind::Workflow, workflow_cache.uuid(), version_ms, 0i64)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::<String>::ok("success").into()
}

#[allow(dead_code)]
#[derive(Debug, Serialize, ToSchema)]
struct Response {}
