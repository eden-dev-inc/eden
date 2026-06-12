use crate::EdenDb;
use crate::comm::interlays::InterlayResponse;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use database::db::cache::CacheFunctions;
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::cache_id::InterlayCacheId;
use eden_core::format::cache_uuid::{CacheUuid, InterlayCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::timestamp::DateTimeWrapper;
use eden_core::format::{CacheObjectType, InterlayId, InterlayUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::interlay::{InterlaySchema, InterlayState};
use endpoint_core::ep_core::settings::EdenSettings;
use std::str::FromStr;
use telemetry_extensions_macro::with_telemetry;

/// Get an Interlay
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Interlays"],
    path="/interlays/{interlay}",
    operation_id = "get_interlay",
        responses((status = OK, body = InterlayResponse))
)]
#[allow(clippy::too_many_arguments)]
pub async fn get(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    interlay: web::Path<String>,
    database: web::Data<EdenDb>,
    interlay_endpoints: web::Data<DashMap<InterlayCacheUuid, InterlayState>>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    let settings = EdenSettings::from(req.headers());

    let interlay_schema =
        <EdenDb as CacheFunctions<InterlaySchema, InterlayCacheUuid, InterlayUuid, InterlayCacheId, InterlayId>>::get_from_cache(
            &database,
            &CacheObjectType::from((Some(org_key.clone()), interlay.into_inner())),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Check if interlay is running
    let interlay_cache_uuid = InterlayCacheUuid::new(Some(org_key), interlay_schema.uuid());
    let _is_running = interlay_endpoints.get(&interlay_cache_uuid).map(|state| state.is_running()).unwrap_or(false);

    match settings.verbose() {
        true => {
            let mut response_body = serde_json::to_value(&interlay_schema).unwrap_or_else(|_| serde_json::json!({}));
            if let serde_json::Value::Object(ref mut map) = response_body {
                map.insert("running".to_string(), serde_json::Value::Bool(_is_running));
            }
            EdenResponse::response(response_body).into()
        }
        false => EdenResponse::response(InterlayResponse::from(interlay_schema).with_running(_is_running)).into(),
    }
}

/// Get an Interlay
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Interlays"],
    path="/interlays",
    operation_id = "list_interlays",
        responses((status = OK, body = InterlayResponse))
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
        true => EdenResponse::response(database.select_all_interlays(org_uuid, telemetry_wrapper).await?).into(),
        false => EdenResponse::response(database.select_all_interlays_ids(org_uuid, telemetry_wrapper).await?).into(),
    }
}

/// Get an Interlay
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Interlays"],
    path="/interlays/updated",
    operation_id = "list_interlays_updated",
        responses((status = OK, body = InterlayResponse))
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
            EdenResponse::response(database.select_all_interlays_updated(org_uuid, &date_time_wrapper, telemetry_wrapper).await?).into()
        }
        false => {
            EdenResponse::response(database.select_all_interlays_ids_updated(org_uuid, &date_time_wrapper, telemetry_wrapper).await?).into()
        }
    }
}
