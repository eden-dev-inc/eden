use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::{ControlPerms, ControlPlaneRbacData};
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid, IdKind};
use eden_core::response::EdenResponse;
use endpoint_schema::endpoint::EndpointSchema;
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
    path="/iam/control/endpoints/{endpoint}",
    operation_id = "get_rbac_endpoint",
    responses((status = OK, body = Response))
)]
pub async fn get(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let entity = input.into_inner();

    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());

    let endpoint_cache =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_cache_uuid(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), entity.clone())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let entries = database
        .control_plane_list_by_entity(org_cache.uuid(), IdKind::Endpoint, endpoint_cache.uuid())
        .await
        .map_err(|e| error_handling(e, &mut span))?;

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
