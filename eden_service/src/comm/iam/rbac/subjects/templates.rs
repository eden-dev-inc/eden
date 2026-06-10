use crate::EdenDb;
use crate::comm::iam::rbac::resolve_subject_for_org;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::IdKind;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::{ControlPerms, ControlPlaneRbacData};
use eden_core::response::EdenResponse;
use serde::Deserialize;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: `ControlPerms::GRANT` on Organization
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["RBAC"],
    path="/iam/control/subjects/{subject}/templates",
    operation_id = "get_rbac_subject_templates",
    responses((status = OK, body = Response))
)]
#[allow(clippy::too_many_arguments)]
pub async fn get(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let subject = input.into_inner();

    // only admins can view RBAC info
    verify_control_perms(&database, &auth, None, ControlPerms::GRANT, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_cache = OrganizationCacheUuid::from(auth.org_uuid().to_owned());

    let resolved_subject = resolve_subject_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let all_entries = database
        .control_plane_list_by_subject(org_cache.uuid(), resolved_subject.kind, resolved_subject.uuid)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let entries: Vec<ControlPlaneRbacData> = all_entries.into_iter().filter(|e| e.entity_kind == IdKind::Template.as_str()).collect();

    EdenResponse::response(Response::new(entries)).into()
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response {
    entries: Vec<ControlPlaneRbacData>,
}

impl Response {
    fn new(entries: Vec<ControlPlaneRbacData>) -> Self {
        Self { entries }
    }
}
