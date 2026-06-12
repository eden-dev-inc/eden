use crate::EdenDb;
#[cfg(external_db)]
use crate::comm::rbac::verify_control_perms;
#[cfg(external_db)]
use crate::error_handling;
#[cfg(all(feature = "openapi", not(external_db)))]
use actix_web::HttpResponse;
use actix_web::{HttpRequest, Responder, web};
#[cfg(external_db)]
use database::backups::delete_backup;
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
use std::path::PathBuf;
#[cfg(external_db)]
use telemetry_extensions_macro::with_telemetry;

#[cfg(external_db)]
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Backup"],
    path="/backups/{timestamp}",
    security(),
    params(
        ("timestamp" = i64, Path, description = "Backup timestamp (created_at value)")
    ),
    responses((status = OK, body = serde_json::Value))
)]
#[named]
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    timestamp: web::Path<i64>,
    db: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let ctx = ctx_with_trace!();
    let timestamp = timestamp.into_inner();

    verify_control_perms(&db, &auth, None, ControlPerms::DESTROY, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    log_info!(ctx.clone(), "Deleting backup", audience = LogAudience::Internal, timestamp = timestamp);

    let backup_dir = eden_config::backup().dir.clone().unwrap_or_else(|| "backups".to_string());

    let metadata_filename = format!("backup-{}.metadata.json", timestamp);
    let metadata_path = PathBuf::from(&backup_dir).join(&metadata_filename);

    delete_backup(&metadata_path).await.map_err(|e| {
        log_error!(
            ctx.clone(),
            "Failed to delete backup",
            audience = LogAudience::Internal,
            error = e.to_string(),
            timestamp = timestamp
        );
        error_handling(e, &mut span)
    })?;

    log_info!(ctx, "Successfully deleted backup", audience = LogAudience::Internal, timestamp = timestamp);

    let response_data = json!({
        "message": "Backup deleted successfully",
        "timestamp": timestamp
    });

    EdenResponse::response(response_data).into()
}

#[cfg(all(feature = "openapi", not(external_db)))]
#[utoipa::path(
    delete,
    tags = ["Backup"],
    path="/backups/{timestamp}",
    security(),
    params(
        ("timestamp" = i64, Path, description = "Backup timestamp (created_at value)")
    ),
    responses((status = OK, body = serde_json::Value))
)]
pub async fn delete(
    _req: HttpRequest,
    _auth: web::ReqData<ParsedJwt>,
    _timestamp: web::Path<i64>,
    _db: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    Ok(HttpResponse::NotImplemented().finish())
}
