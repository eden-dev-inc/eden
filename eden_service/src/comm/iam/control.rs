use crate::EdenDb;
use crate::comm::iam::ControlPermInput;
use crate::comm::iam::rbac::resolve_subject_for_org;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::{EndpointCacheId, TemplateCacheId, WorkflowCacheId};
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid, TemplateCacheUuid, WorkflowCacheUuid};
use eden_core::format::rbac::{ControlPerms, ControlPlaneRbacData};
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid, IdKind, TemplateId, TemplateUuid, WorkflowId, WorkflowUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::template::TemplateSchema;
use endpoint_core::ep_core::database::schema::workflow::WorkflowSchema;
use endpoint_schema::endpoint::EndpointSchema;
use telemetry_extensions_macro::with_telemetry;

fn control_plane_response(perms: ControlPerms) -> Result<actix_web::HttpResponse, actix_web::Error> {
    EdenResponse::response(perms).into()
}

/// Set one subject's exact organization control-plane permissions.
/// **Permissions**: `ControlPerms::GRANT | granted_bits` on Organization
#[with_telemetry]
#[utoipa::path(
    put,
    tags = ["IAM"],
    path="/iam/control/organizations/subjects/{subject}",
    operation_id = "put_control_organization_subject",
    request_body = ControlPermInput,
    responses((status = OK, body = ControlPerms))
)]
pub async fn put_organization_subject(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    subject: web::Path<String>,
    database: web::Data<EdenDb>,
    body: web::Json<ControlPermInput>,
) -> Result<impl Responder, actix_web::Error> {
    let subject = subject.into_inner();
    let input = body.into_inner();
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());
    let org_uuid = org_cache.uuid();

    input.validate_for_put().map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, input.required_grant_perms(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let resolved_subject = resolve_subject_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    database
        .control_plane_grant(
            &ControlPlaneRbacData {
                org_uuid,
                entity_kind: IdKind::Organization.as_str().to_owned(),
                entity_uuid: org_uuid,
                subject_kind: resolved_subject.kind.as_str().to_owned(),
                subject_uuid: resolved_subject.uuid,
                perms: input.perms,
            },
            chrono::Utc::now().timestamp_millis(),
            0,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    control_plane_response(input.perms)
}

/// Set one subject's exact endpoint control-plane permissions.
/// **Permissions**: `ControlPerms::GRANT | granted_bits` on Organization
#[with_telemetry]
#[utoipa::path(
    put,
    tags = ["IAM"],
    path="/iam/control/endpoints/{endpoint}/subjects/{subject}",
    operation_id = "put_control_endpoint_subject",
    request_body = ControlPermInput,
    responses((status = OK, body = ControlPerms))
)]
pub async fn put_endpoint_subject(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<(String, String)>,
    database: web::Data<EdenDb>,
    body: web::Json<ControlPermInput>,
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

    database
        .control_plane_grant(
            &ControlPlaneRbacData {
                org_uuid: org_cache.uuid(),
                entity_kind: IdKind::Endpoint.as_str().to_owned(),
                entity_uuid: endpoint_cache.uuid(),
                subject_kind: resolved_subject.kind.as_str().to_owned(),
                subject_uuid: resolved_subject.uuid,
                perms: input.perms,
            },
            chrono::Utc::now().timestamp_millis(),
            0,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    control_plane_response(input.perms)
}

/// Set one subject's exact template control-plane permissions.
/// **Permissions**: `ControlPerms::GRANT | granted_bits` on Organization
#[with_telemetry]
#[utoipa::path(
    put,
    tags = ["IAM"],
    path="/iam/control/templates/{template}/subjects/{subject}",
    operation_id = "put_control_template_subject",
    request_body = ControlPermInput,
    responses((status = OK, body = ControlPerms))
)]
pub async fn put_template_subject(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<(String, String)>,
    database: web::Data<EdenDb>,
    body: web::Json<ControlPermInput>,
) -> Result<impl Responder, actix_web::Error> {
    let (template, subject) = input.into_inner();
    let input = body.into_inner();
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    input.validate_for_put().map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, input.required_grant_perms(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let template_cache =
        <EdenDb as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::get_cache_uuid(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), template)),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let resolved_subject = resolve_subject_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    database
        .control_plane_grant(
            &ControlPlaneRbacData {
                org_uuid: org_cache.uuid(),
                entity_kind: IdKind::Template.as_str().to_owned(),
                entity_uuid: template_cache.uuid(),
                subject_kind: resolved_subject.kind.as_str().to_owned(),
                subject_uuid: resolved_subject.uuid,
                perms: input.perms,
            },
            chrono::Utc::now().timestamp_millis(),
            0,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    control_plane_response(input.perms)
}

/// Set one subject's exact workflow control-plane permissions.
/// **Permissions**: `ControlPerms::GRANT | granted_bits` on Organization
#[with_telemetry]
#[utoipa::path(
    put,
    tags = ["IAM"],
    path="/iam/control/workflows/{workflow}/subjects/{subject}",
    operation_id = "put_control_workflow_subject",
    request_body = ControlPermInput,
    responses((status = OK, body = ControlPerms))
)]
pub async fn put_workflow_subject(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<(String, String)>,
    database: web::Data<EdenDb>,
    body: web::Json<ControlPermInput>,
) -> Result<impl Responder, actix_web::Error> {
    let (workflow, subject) = input.into_inner();
    let input = body.into_inner();
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    input.validate_for_put().map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, input.required_grant_perms(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let workflow_cache =
        <EdenDb as CacheFunctions<WorkflowSchema, WorkflowCacheUuid, WorkflowUuid, WorkflowCacheId, WorkflowId>>::get_cache_uuid(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), workflow)),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let resolved_subject = resolve_subject_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    database
        .control_plane_grant(
            &ControlPlaneRbacData {
                org_uuid: org_cache.uuid(),
                entity_kind: IdKind::Workflow.as_str().to_owned(),
                entity_uuid: workflow_cache.uuid(),
                subject_kind: resolved_subject.kind.as_str().to_owned(),
                subject_uuid: resolved_subject.uuid,
                perms: input.perms,
            },
            chrono::Utc::now().timestamp_millis(),
            0,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    control_plane_response(input.perms)
}
