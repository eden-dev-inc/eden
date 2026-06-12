use crate::EdenDb;
use crate::comm::apis::ApiResponse;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use chrono::{DateTime, Utc};
use database::db::cache::CacheFunctions;
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::cache_id::{ApiCacheId, TemplateCacheId};
use eden_core::format::cache_uuid::{ApiCacheUuid, CacheUuid, OrganizationCacheUuid, TemplateCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::timestamp::DateTimeWrapper;
use eden_core::format::{ApiId, ApiUuid, CacheObjectType, TemplateId, TemplateUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::api::ApiSchema;
use endpoint_core::ep_core::database::schema::template::TemplateSchema;
use endpoint_core::ep_core::settings::EdenSettings;
use std::str::FromStr;
use telemetry_extensions_macro::with_telemetry;

/// Get an Api
/// **Permissions**: `ControlPerms::CONFIGURE` on the Api or Organization
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Apis"],
    path="/apis/{api}",
    operation_id = "get_api",
        responses((status = OK, body = ApiResponse))
)]
pub async fn get(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    api: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    let settings = EdenSettings::from(req.headers());

    let api_schema = <EdenDb as CacheFunctions<ApiSchema, ApiCacheUuid, ApiUuid, ApiCacheId, ApiId>>::get_from_cache(
        &database,
        &CacheObjectType::from((Some(org_key.clone()), api.into_inner())),
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let mut templates = vec![];
    for uuid in api_schema.templates() {
        templates.push(
            <EdenDb as CacheFunctions<TemplateSchema, TemplateCacheUuid, TemplateUuid, TemplateCacheId, TemplateId>>::get_from_cache(
                &database,
                &CacheObjectType::new(Some(TemplateCacheUuid::new(Some(org_key.clone()), uuid.clone())), None),
                telemetry_wrapper,
            )
            .await
            .map_err(|e| error_handling(e, &mut span))?,
        );
    }

    match settings.verbose() {
        true => EdenResponse::response(api_schema).into(),
        false => EdenResponse::response(ApiResponse::new(
            api_schema.id(),
            api_schema.uuid(),
            templates,
            api_schema.response_logic().cloned(),
            api_schema.created_at(),
            api_schema.updated_at(),
        ))
        .into(),
    }
}

/// Get an Api
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Apis"],
    path="/apis",
    operation_id = "list_apis",
        responses((status = OK, body = ApiResponse))
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
        true => EdenResponse::response(database.select_all_apis(org_uuid, telemetry_wrapper).await?).into(),
        false => EdenResponse::response(database.select_all_apis_ids(org_uuid, telemetry_wrapper).await?).into(),
    }
}

/// Get an Api
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Apis"],
    path="/apis/updated",
    operation_id = "list_apis_updated",
        responses((status = OK, body = ApiResponse))
)]
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
        true => EdenResponse::response(database.select_all_apis_updated(org_uuid, &date_time_wrapper, telemetry_wrapper).await?).into(),
        false => {
            EdenResponse::response(database.select_all_apis_ids_updated(org_uuid, &date_time_wrapper, telemetry_wrapper).await?).into()
        }
    }
}
