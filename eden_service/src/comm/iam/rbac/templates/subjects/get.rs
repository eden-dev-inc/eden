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

/// **Permissions**: `ControlPerms::GRANT` on Organization
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["RBAC"],
    path="/iam/control/templates/{template}/subjects/{subject}",
    operation_id = "get_rbac_template_subject",
    responses((status = OK, body = ControlPerms))
)]
#[allow(clippy::too_many_arguments)]
pub async fn get(
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
            &CacheObjectType::from((Some(org_key.clone()), entity.to_string())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let resolved_subject = resolve_subject_for_org(&database, &org_key, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let perms = database
        .control_plane_get(
            org_key.uuid(),
            IdKind::Template,
            template_cache.uuid(),
            resolved_subject.kind,
            resolved_subject.uuid,
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
