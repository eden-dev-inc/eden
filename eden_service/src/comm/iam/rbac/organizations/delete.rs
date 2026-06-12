use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::IdKind;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use telemetry_extensions_macro::with_telemetry;

/// **Permissions**: `ControlPerms::GRANT | ControlPerms::DESTROY` on Organization
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["RBAC"],
    path="/iam/control/organizations",
    operation_id = "delete_rbac_organization",
    responses((status = OK, body = String))
)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned()).uuid();

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT | ControlPerms::DESTROY, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let version_ms = chrono::Utc::now().timestamp_millis();

    database
        .control_plane_remove_entity(org_uuid, IdKind::Organization, org_uuid, version_ms, 0i64)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::<String>::ok("success").into()
}
