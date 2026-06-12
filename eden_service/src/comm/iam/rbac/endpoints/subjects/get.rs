use crate::EdenDb;
use crate::comm::iam::rbac::resolve_subject_for_org;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::{CacheId, EndpointCacheId};
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid, UserCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EdenId, EndpointId, EndpointUuid, IdKind};
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
    path="/iam/control/endpoints/{endpoint}/subjects/{subject}",
    operation_id = "get_rbac_endpoint_subject",
    responses((status = OK, body = ControlPerms))
)]
pub async fn get(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<(String, String)>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let (entity, subject) = input.into_inner();

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT, telemetry_wrapper).await?;

    let org_key = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());

    let endpoint_cache =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_cache_uuid(
            &database,
            &CacheObjectType::new(None, Some(EndpointCacheId::new(Some(org_key.clone()), EndpointId::new(entity.to_string())))),
            telemetry_wrapper,
        )
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let resolved_subject = resolve_subject_for_org(&database, &org_key, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let perms = database
        .control_plane_get(
            org_key.uuid(),
            IdKind::Endpoint,
            endpoint_cache.uuid(),
            resolved_subject.kind,
            resolved_subject.uuid,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(Response::new(perms)).into()
}

#[with_telemetry]
/// Get the endpoints for the user that makes the request
#[allow(clippy::too_many_arguments)]
pub async fn get_self(
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
            &CacheObjectType::from((Some(org_cache.clone()), entity)),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let user_cache = UserCacheUuid::new(Some(org_cache.clone()), auth.user_uuid().to_owned());

    let perms = database
        .control_plane_get(org_cache.uuid(), IdKind::Endpoint, endpoint_cache.uuid(), IdKind::User, user_cache.uuid())
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
