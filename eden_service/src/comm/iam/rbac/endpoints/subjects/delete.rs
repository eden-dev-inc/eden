use crate::EdenDb;
use crate::comm::iam::rbac::resolve_subject_for_org;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid, IdKind};
use eden_core::response::EdenResponse;
use endpoint_schema::endpoint::EndpointSchema;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: `ControlPerms::GRANT | subject_bits` on Organization
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["RBAC"],
    path="/iam/control/endpoints/{endpoint}/subjects/{subject}",
    operation_id = "delete_rbac_endpoint_subject",
    responses((status = OK, body = ControlPerms))
)]
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<(String, String)>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let (entity, subject) = input.into_inner();

    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    let endpoint_cache =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_cache_uuid(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), entity)),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let resolved_subject = resolve_subject_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Get perms to determine required auth level
    let perms = database
        .control_plane_get(
            org_cache.uuid(),
            IdKind::Endpoint,
            endpoint_cache.uuid(),
            resolved_subject.kind,
            resolved_subject.uuid,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let required_perms = ControlPerms::GRANT | perms;
    verify_control_perms(&database, &auth, None, required_perms, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    database
        .control_plane_revoke(
            org_cache.uuid(),
            IdKind::Endpoint,
            endpoint_cache.uuid(),
            resolved_subject.kind,
            resolved_subject.uuid,
            chrono::Utc::now().timestamp_millis(),
            0i64,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(Response::new(perms)).into()
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct Response(ControlPerms);

impl Response {
    fn new(perms: ControlPerms) -> Self {
        Self(perms)
    }
}
