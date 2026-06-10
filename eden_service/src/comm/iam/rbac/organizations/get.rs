use crate::EdenDb;
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
    path="/iam/control/organizations",
    operation_id = "get_rbac_organization",
    responses((status = OK, body = String))
)]
pub async fn get(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::GRANT, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned()).uuid();

    let subjects = database
        .control_plane_list_by_entity(org_uuid, IdKind::Organization, org_uuid)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(Response::new(subjects)).into()
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response(Vec<ControlPlaneRbacData>);

impl Response {
    fn new(data: Vec<ControlPlaneRbacData>) -> Self {
        Self(data)
    }
}
