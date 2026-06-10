use crate::EdenDb;
use crate::comm::iam::rbac::resolve_subject_for_org;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::TemplateCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, TemplateCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, IdKind, TemplateId, TemplateUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::template::TemplateSchema;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: `ControlPerms::GRANT | subject_bits` on Organization
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["RBAC"],
    path="/iam/control/templates/{template}/subjects/{subject}",
    operation_id = "delete_rbac_template_subject",
    responses((status = OK, body = String))
)]
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<(String, String)>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let (entity, subject) = input.into_inner();

    let org_key = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    let template_cache =
        <EdenDb as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::get_cache_uuid(
            &database,
            &CacheObjectType::from((Some(org_key.clone()), entity)),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let resolved_subject = resolve_subject_for_org(&database, &org_key, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let existing_perms = database
        .control_plane_get(
            org_key.uuid(),
            IdKind::Template,
            template_cache.uuid(),
            resolved_subject.kind,
            resolved_subject.uuid,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT | existing_perms, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let version_ms = chrono::Utc::now().timestamp_millis();

    database
        .control_plane_revoke(
            org_key.uuid(),
            IdKind::Template,
            template_cache.uuid(),
            resolved_subject.kind,
            resolved_subject.uuid,
            version_ms,
            0i64,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::<String>::ok("success").into()
}

#[allow(dead_code)]
#[derive(Debug, Serialize, ToSchema)]
struct Response {}
