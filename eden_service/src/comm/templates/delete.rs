use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::cache::CacheFunctions;
use database::db::lib::{ClickhouseConn, PgConn, RedisConn};
use database::db::methods::delete::DeleteMethod;
use database::db::methods::delete::template::DeleteTemplate;
use database::methods::delete::UuidsToUpdate;
use eden_core::auth::ParsedJwt;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::TemplateCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, TemplateCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, TemplateId, TemplateUuid};
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::template::TemplateSchema;
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Delete (disconnect) a Template
/// **Permissions**: `ControlPerms::CONFIGURE` on the Template or Organization
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Templates"],
    path="/templates/{template}",
    operation_id = "delete_template",
    responses((status = OK, body = String))
)]
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    template: web::Path<String>,
    template_registry: web::Data<TemplateRegistry>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let template_object = CacheObjectType::from((Some(org_key), template.to_string()));

    let template_uuid = <EdenDb as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::get_uuid(
        &database,
        &template_object,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    delete_template(&database, template_object, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    template_registry.remove(&template_uuid, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::<String>::ok("success").into()
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Response {}

pub(crate) async fn delete_template(
    db_manager: &EdenDb,
    cache_object: CacheObjectType<TemplateCacheUuid, TemplateCacheId>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<UuidsToUpdate> {
    let delete_template = <DeleteTemplate as DeleteMethod<
        TemplateSchema,
        TemplateCacheUuid,
        TemplateUuid,
        TemplateCacheId,
        TemplateId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::new(cache_object);

    <DeleteTemplate as DeleteMethod<
        TemplateSchema,
        TemplateCacheUuid,
        TemplateUuid,
        TemplateCacheId,
        TemplateId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::delete(&delete_template, db_manager, telemetry_wrapper)
    .await
}
