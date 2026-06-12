use crate::EdenDb;
use crate::comm::iam::SubjectInput;
use crate::comm::iam::rbac::resolve_user_cache_uuid_for_org;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::CacheUuid;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPlaneRbacData;
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid, IdKind};
use eden_core::response::EdenResponse;
use endpoint_schema::endpoint::EndpointSchema;
use serde::Deserialize;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: `ControlPerms::GRANT | granted_bits` on Organization
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
pub async fn post(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<String>,
    database: web::Data<EdenDb>,
    body: web::Json<SubjectInput>,
) -> Result<impl Responder, actix_web::Error> {
    let entity = input.into_inner();

    let subject_input = body.into_inner();

    verify_control_perms(&database, &auth, None, subject_input.required_grant_perms(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = auth.org_uuid();

    let org_cache = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), entity)),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    let endpoint_cache = EndpointCacheUuid::new(Some(org_cache.clone()), endpoint_schema.endpoint_uuid());

    let version_ms = chrono::Utc::now().timestamp_millis();

    let mut grants = Vec::with_capacity(subject_input.subjects.len());
    for (subject, relation) in subject_input.to_vec() {
        let user_cache = resolve_user_cache_uuid_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
            .await
            .map_err(|e| error_handling(e, &mut span))?;

        grants.push((user_cache.uuid(), relation));
    }

    for (subject_uuid, perms) in grants {
        let data = ControlPlaneRbacData {
            org_uuid: org_cache.uuid(),
            entity_kind: IdKind::Endpoint.as_str().to_owned(),
            entity_uuid: endpoint_cache.uuid(),
            subject_kind: IdKind::User.as_str().to_owned(),
            subject_uuid,
            perms,
        };

        database.control_plane_grant(&data, version_ms, 0i64).await.map_err(|e| error_handling(e, &mut span))?;
    }

    EdenResponse::<String>::ok("added rbac rule for endpoint").into()
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response(String);
