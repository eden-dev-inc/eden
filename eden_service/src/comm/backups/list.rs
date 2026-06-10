use crate::EdenDb;
#[cfg(external_db)]
use crate::comm::rbac::verify_control_perms;
#[cfg(external_db)]
use crate::error_handling;
#[cfg(all(feature = "openapi", not(external_db)))]
use actix_web::HttpResponse;
use actix_web::{HttpRequest, Responder, web};
#[cfg(external_db)]
use database::backups::list_backups;
use eden_core::auth::ParsedJwt;
#[cfg(external_db)]
use eden_core::format::rbac::ControlPerms;
#[cfg(external_db)]
use eden_core::response::EdenResponse;
#[cfg(external_db)]
use eden_logger_internal::{LogAudience, ctx_with_trace, log_error, log_info};
#[cfg(external_db)]
use function_name::named;
#[cfg(external_db)]
use serde_json::json;
#[cfg(external_db)]
use telemetry_extensions_macro::with_telemetry;

#[cfg(external_db)]
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Backup"],
    path="/backups",
    security(),
    responses((status = OK, body = serde_json::Value))
)]
#[named]
pub async fn list(_req: HttpRequest, auth: web::ReqData<ParsedJwt>, db: web::Data<EdenDb>) -> Result<impl Responder, actix_web::Error> {
    let ctx = ctx_with_trace!();

    verify_control_perms(&db, &auth, None, ControlPerms::DESTROY, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    log_info!(ctx.clone(), "Listing backups", audience = LogAudience::Internal);

    let backup_dir = eden_config::backup().dir.clone().unwrap_or_else(|| "backups".to_string());

    let backups = list_backups(&backup_dir).await.map_err(|e| {
        log_error!(
            ctx.clone(),
            "Failed to list backups",
            audience = LogAudience::Internal,
            error = e.to_string(),
            backup_dir = &backup_dir
        );
        error_handling(e, &mut span)
    })?;

    log_info!(ctx, "Successfully listed backups", audience = LogAudience::Internal, count = backups.len());

    let response_data = json!({
        "backups": backups,
        "count": backups.len()
    });

    EdenResponse::response(response_data).into()
}

#[cfg(all(feature = "openapi", not(external_db)))]
#[utoipa::path(
    get,
    tags = ["Backup"],
    path="/backups",
    security(),
    responses((status = OK, body = serde_json::Value))
)]
pub async fn list(_req: HttpRequest, _auth: web::ReqData<ParsedJwt>, _db: web::Data<EdenDb>) -> Result<impl Responder, actix_web::Error> {
    Ok(HttpResponse::NotImplemented().finish())
}
