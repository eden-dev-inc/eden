use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::TemplateCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, TemplateCacheUuid};
use eden_core::format::rbac::{ControlPerms, ControlPlaneRbacData};
use eden_core::format::{CacheObjectType, IdKind, TemplateId, TemplateUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::template::TemplateSchema;
use serde::Deserialize;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: `ControlPerms::GRANT` on Organization
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["RBAC"],
    path="/iam/control/templates/{template}",
    operation_id = "get_rbac_template",
    responses((status = OK, body = Response))
)]
#[allow(clippy::too_many_arguments)]
pub async fn get(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let entity = input.into_inner();

    verify_control_perms(&database, &auth, None, ControlPerms::GRANT, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    let template_cache =
        <EdenDb as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::get_cache_uuid(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), entity.to_string())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let entries = database
        .control_plane_list_by_entity(org_cache.uuid(), IdKind::Template, template_cache.uuid())
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
