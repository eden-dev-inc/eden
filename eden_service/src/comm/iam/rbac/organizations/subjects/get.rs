use crate::EdenDb;
use crate::comm::iam::rbac::resolve_subject_for_org;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::IdKind;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use serde::Deserialize;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: `ControlPerms::GRANT` on Organization
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["RBAC"],
    path="/iam/control/organizations/subjects/{subject}",
    operation_id = "get_rbac_organization_subject",
    responses((status = OK, body = ControlPerms))
)]
pub async fn get(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let subject = input.into_inner();

    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let resolved_subject = resolve_subject_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = org_cache.uuid();
    let perms = database
        .control_plane_get(org_uuid, IdKind::Organization, org_uuid, resolved_subject.kind, resolved_subject.uuid)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(Response::new(perms)).into()
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response(ControlPerms);

impl Response {
    fn new(perms: ControlPerms) -> Self {
        Self(perms)
    }
}
