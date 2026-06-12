use crate::EdenDb;
use crate::comm::iam::rbac::resolve_subject_for_org;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use serde::{Deserialize, Serialize};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: `ControlPerms::GRANT | ControlPerms::DESTROY` on Organization
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["RBAC"],
    path="/iam/control/subjects/{subject}",
    operation_id = "delete_rbac_subject",
    responses((status = OK, body = String))
)]
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let subject = input.into_inner();

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT | ControlPerms::DESTROY, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_cache = OrganizationCacheUuid::from(auth.org_uuid().to_owned());

    let resolved_subject = resolve_subject_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let version_ms = chrono::Utc::now().timestamp_millis();

    database
        .control_plane_remove_subject(org_cache.uuid(), resolved_subject.kind, resolved_subject.uuid, version_ms, 0i64)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::<String>::ok("success").into()
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response(String);
