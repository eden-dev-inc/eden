use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::{TemplateRegistry, error_handling};
use actix_web::{HttpRequest, Responder, web};
use chrono::{DateTime, Utc};
use database::db::cache::CacheFunctions;
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::cache_id::TemplateCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, TemplateCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::timestamp::DateTimeWrapper;
use eden_core::format::{CacheObjectType, TemplateId, TemplateUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::template::{TemplateSchema, TemplateSchemaIds};
use endpoint_core::ep_core::database::template::JsonTemplate;
use endpoint_core::ep_core::settings::EdenSettings;
use serde::Deserialize;
use serde::Serialize;
use std::str::FromStr;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Get a Template
/// **Permissions**: `ControlPerms::CONFIGURE` on the Template or Organization
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Templates"],
    path="/templates/{template}",
    operation_id = "get_template",
    responses((status = OK, body = Option<JsonTemplate>))
)]
#[allow(clippy::too_many_arguments)]
pub async fn get(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    template: web::Path<String>,
    template_registry: web::Data<TemplateRegistry>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    let template_uuid = <EdenDb as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::get_uuid(
        &database,
        &CacheObjectType::from((Some(org_key), template.to_string())),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let template = template_registry.get(&template_uuid, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(Response::new(template)).into()
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response(Option<JsonTemplate>);

impl Response {
    fn new(schema: Option<JsonTemplate>) -> Self {
        Self(schema)
    }

    pub fn into_inner(self) -> Option<JsonTemplate> {
        self.0
    }
}

/// Get a Template
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Templates"],
    path="/templates",
    operation_id = "list_templates",
    responses((status = OK, body = Vec<TemplateSchemaIds>))
)]
pub async fn get_all(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();

    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    match EdenSettings::from(req.headers()).verbose() {
        true => EdenResponse::response(database.select_all_templates(org_uuid, telemetry_wrapper).await?).into(),
        false => EdenResponse::response(database.select_all_templates_ids(org_uuid, telemetry_wrapper).await?).into(),
    }
}

/// Get a Template
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Templates"],
    path="/templates/updated",
    operation_id = "list_templates_updated",
    responses((status = OK, body = Vec<TemplateSchemaIds>))
)]
#[allow(clippy::too_many_arguments)]
pub async fn get_all_updated(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    timestamp: web::Json<String>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();

    let time: DateTime<Utc> = DateTime::from_str(timestamp.as_str()).map_err(EpError::parse)?;
    let date_time_wrapper = DateTimeWrapper::from(time);

    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    match EdenSettings::from(req.headers()).verbose() {
        true => {
            EdenResponse::response(database.select_all_templates_updated(org_uuid, &date_time_wrapper, telemetry_wrapper).await?).into()
        }
        false => {
            EdenResponse::response(database.select_all_templates_ids_updated(org_uuid, &date_time_wrapper, telemetry_wrapper).await?).into()
        }
    }
}
