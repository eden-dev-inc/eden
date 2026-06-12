use crate::EdenDb;
use crate::comm::iam::DataPermInput;
use crate::comm::iam::rbac::resolve_subject_for_org;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::els::UserPolicyAssignmentRedacted;
use database::db::rbac::DataPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::{ControlPerms, DataPerms, DataPlaneRbacData};
use eden_core::format::{CacheObjectType, EdenUuid, EndpointId, EndpointUuid, IdKind, UserUuid};
use eden_core::response::EdenResponse;
use endpoint_schema::endpoint::EndpointSchema;
use serde::{Deserialize, Serialize};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct ListResponse {
    pub entries: Vec<DataPlaneRbacData>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataPlaneMode {
    None,
    SharedRbac,
    Els,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct ControlPlaneAccess {
    pub organization_perms: ControlPerms,
    pub endpoint_perms: ControlPerms,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct DataPlaneAccess {
    pub mode: DataPlaneMode,
    pub shared_perms: DataPerms,
    pub els_assignment: Option<UserPolicyAssignmentRedacted>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct AccessResponse {
    pub control_plane: ControlPlaneAccess,
    pub data_plane: DataPlaneAccess,
}

fn data_plane_response(perms: DataPerms) -> Result<actix_web::HttpResponse, actix_web::Error> {
    EdenResponse::response(perms).into()
}

/// List all shared runtime grants on an endpoint.
/// **Permissions**: `ControlPerms::GRANT` on Organization
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["IAM"],
    path="/iam/data/endpoints/{endpoint}",
    operation_id = "get_data_endpoint",
    responses((status = OK, body = ListResponse))
)]
pub async fn get_endpoint(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), endpoint.into_inner())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let entries = database
        .data_plane_list_by_endpoint(org_cache.uuid(), endpoint_schema.endpoint_uuid().uuid())
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(ListResponse { entries }).into()
}

/// Get one subject's exact shared runtime perms on an endpoint.
/// **Permissions**: `ControlPerms::GRANT` on Organization
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["IAM"],
    path="/iam/data/endpoints/{endpoint}/subjects/{subject}",
    operation_id = "get_data_endpoint_subject",
    responses((status = OK, body = DataPerms))
)]
pub async fn get_endpoint_subject(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<(String, String)>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint, subject) = input.into_inner();
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), endpoint)),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let resolved_subject = resolve_subject_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let perms = database
        .data_plane_get(
            org_cache.uuid(),
            endpoint_schema.endpoint_uuid().uuid(),
            resolved_subject.kind,
            resolved_subject.uuid,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    data_plane_response(perms)
}

/// Set one subject's exact shared runtime perms on an endpoint.
/// **Permissions**: `ControlPerms::GRANT` on Organization
#[with_telemetry]
#[utoipa::path(
    put,
    tags = ["IAM"],
    path="/iam/data/endpoints/{endpoint}/subjects/{subject}",
    operation_id = "put_data_endpoint_subject",
    request_body = DataPermInput,
    responses((status = OK, body = DataPerms))
)]
pub async fn put_endpoint_subject(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<(String, String)>,
    database: web::Data<EdenDb>,
    body: web::Json<DataPermInput>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint, subject) = input.into_inner();
    let input = body.into_inner();
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    input.validate_for_put().map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, input.required_grant_perms(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), endpoint)),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    let endpoint_cache = EndpointCacheUuid::new(Some(org_cache.clone()), endpoint_schema.endpoint_uuid());

    let resolved_subject = resolve_subject_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    if resolved_subject.kind == IdKind::User {
        let user_uuid = UserUuid::from(resolved_subject.uuid);
        database
            .data_plane_grant_endpoint_users_exclusive(&endpoint_cache, &[(user_uuid, input.perms)], chrono::Utc::now().timestamp_millis())
            .await
            .map_err(|e| error_handling(e, &mut span))?;
    } else {
        database
            .data_plane_grant(
                &DataPlaneRbacData {
                    org_uuid: org_cache.uuid(),
                    endpoint_uuid: endpoint_cache.uuid(),
                    subject_kind: resolved_subject.kind.as_str().to_owned(),
                    subject_uuid: resolved_subject.uuid,
                    perms: input.perms,
                },
                chrono::Utc::now().timestamp_millis(),
                0,
            )
            .await
            .map_err(|e| error_handling(e, &mut span))?;
    }

    data_plane_response(input.perms)
}

/// Revoke one subject's shared runtime perms on an endpoint.
/// **Permissions**: `ControlPerms::GRANT` on Organization
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["IAM"],
    path="/iam/data/endpoints/{endpoint}/subjects/{subject}",
    operation_id = "delete_data_endpoint_subject",
    responses((status = OK, body = DataPerms))
)]
pub async fn delete_endpoint_subject(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<(String, String)>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let (endpoint, subject) = input.into_inner();
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), endpoint)),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let resolved_subject = resolve_subject_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    let endpoint_uuid = endpoint_schema.endpoint_uuid().uuid();

    let perms = database
        .data_plane_get(org_cache.uuid(), endpoint_uuid, resolved_subject.kind, resolved_subject.uuid)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    database
        .data_plane_revoke(
            org_cache.uuid(),
            endpoint_uuid,
            resolved_subject.kind,
            resolved_subject.uuid,
            chrono::Utc::now().timestamp_millis(),
            0,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    data_plane_response(perms)
}

/// Remove all shared runtime grants on an endpoint.
/// **Permissions**: `ControlPerms::GRANT | ControlPerms::DESTROY` on Organization
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["IAM"],
    path="/iam/data/endpoints/{endpoint}",
    operation_id = "delete_data_endpoint",
    responses((status = OK, body = String))
)]
pub async fn delete_endpoint(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT | ControlPerms::DESTROY, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), endpoint.into_inner())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    database
        .data_plane_remove_endpoint(org_cache.uuid(), endpoint_schema.endpoint_uuid().uuid(), chrono::Utc::now().timestamp_millis(), 0)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::<String>::ok("removed all shared runtime grants for endpoint").into()
}

/// List all shared runtime grants for one subject across endpoints.
/// **Permissions**: `ControlPerms::GRANT` on Organization
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["IAM"],
    path="/iam/data/subjects/{subject}/endpoints",
    operation_id = "get_data_subject_endpoints",
    responses((status = OK, body = ListResponse))
)]
pub async fn get_subject_endpoints(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    subject: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let subject = subject.into_inner();
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let resolved_subject = resolve_subject_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let entries = database
        .data_plane_list_by_subject(org_cache.uuid(), resolved_subject.kind, resolved_subject.uuid)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(ListResponse { entries }).into()
}

/// Remove all shared runtime grants for a subject across endpoints.
/// **Permissions**: `ControlPerms::GRANT | ControlPerms::DESTROY` on Organization
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["IAM"],
    path="/iam/data/subjects/{subject}",
    operation_id = "delete_data_subject",
    responses((status = OK, body = String))
)]
pub async fn delete_subject(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    subject: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let subject = subject.into_inner();
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT | ControlPerms::DESTROY, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let resolved_subject = resolve_subject_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    database
        .data_plane_remove_subject(
            org_cache.uuid(),
            resolved_subject.kind,
            resolved_subject.uuid,
            chrono::Utc::now().timestamp_millis(),
            0,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::<String>::ok("removed all shared runtime grants for subject").into()
}
