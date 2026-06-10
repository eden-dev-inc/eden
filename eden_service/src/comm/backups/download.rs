use crate::EdenDb;
#[cfg(external_db)]
use crate::comm::rbac::verify_control_perms;
#[cfg(external_db)]
use crate::error_handling;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
#[cfg(external_db)]
use database::backups::load_backup_metadata;
use eden_core::auth::ParsedJwt;
#[cfg(external_db)]
use eden_core::format::rbac::ControlPerms;
#[cfg(external_db)]
use eden_logger_internal::{LogAudience, ctx_with_trace, log_error, log_info};
#[cfg(external_db)]
use function_name::named;
use serde::Deserialize;
#[cfg(external_db)]
use std::path::PathBuf;
#[cfg(external_db)]
use telemetry_extensions_macro::with_telemetry;
use utoipa::IntoParams;

#[derive(Debug, Deserialize, IntoParams)]
pub struct DownloadQuery {
    /// Which component to download: postgres, redis_cache, redis_rbac, or metadata
    pub component: String,
}

#[cfg(external_db)]
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Backup"],
    path="/backups/{timestamp}/download",
    security(),
    params(
        ("timestamp" = i64, Path, description = "Backup timestamp (created_at value)"),
        DownloadQuery
    ),
    responses((status = OK, body = Vec<u8>))
)]
#[named]
#[allow(clippy::too_many_arguments)]
pub async fn download(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    db: web::Data<EdenDb>,
    timestamp: web::Path<i64>,
    query: web::Query<DownloadQuery>,
) -> Result<impl Responder, actix_web::Error> {
    let ctx = ctx_with_trace!();
    let timestamp = timestamp.into_inner();

    verify_control_perms(&db, &auth, None, ControlPerms::DESTROY, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    log_info!(
        ctx.clone(),
        "Downloading backup component",
        audience = LogAudience::Internal,
        timestamp = timestamp,
        component = &query.component
    );

    let backup_dir = eden_config::backup().dir.clone().unwrap_or_else(|| "backups".to_string());

    let metadata_filename = format!("backup-{}.metadata.json", timestamp);
    let metadata_path = PathBuf::from(&backup_dir).join(&metadata_filename);

    // If requesting metadata, just return the JSON file
    if query.component == "metadata" {
        let metadata_bytes = tokio::fs::read(&metadata_path).await.map_err(|e| {
            log_error!(
                ctx.clone(),
                "Failed to read metadata file",
                audience = LogAudience::Internal,
                error = e.to_string(),
                path = metadata_path.display().to_string()
            );
            error_handling(eden_core::error::EpError::fs(format!("Backup not found: {}", e)), &mut span)
        })?;

        log_info!(
            ctx,
            "Successfully downloaded metadata",
            audience = LogAudience::Internal,
            timestamp = timestamp,
            size_bytes = metadata_bytes.len()
        );

        return Ok(HttpResponse::Ok().content_type("application/json").body(metadata_bytes));
    }

    // Load metadata to get dump file paths
    let backup = load_backup_metadata(&metadata_path).await.map_err(|e| {
        log_error!(
            ctx.clone(),
            "Failed to load backup metadata",
            audience = LogAudience::Internal,
            error = e.to_string(),
            path = metadata_path.display().to_string()
        );
        error_handling(e, &mut span)
    })?;

    // Determine which dump file to download
    let (dump_path, component_name) = match query.component.as_str() {
        "postgres" => (&backup.postgres.dump_path, "PostgreSQL"),
        "redis_cache" => (&backup.redis_cache.dump_path, "Redis Cache"),
        "redis_rbac" => (&backup.redis_rbac.dump_path, "Redis RBAC"),
        invalid => {
            log_error!(ctx, "Invalid component requested", audience = LogAudience::Internal, component = invalid);
            return Err(error_handling(
                eden_core::error::EpError::parse(format!(
                    "Invalid component '{}'. Valid options: postgres, redis_cache, redis_rbac, metadata",
                    invalid
                )),
                &mut span,
            ));
        }
    };

    // Read the dump file
    let dump_bytes = tokio::fs::read(dump_path).await.map_err(|e| {
        log_error!(
            ctx.clone(),
            "Failed to read dump file",
            audience = LogAudience::Internal,
            error = e.to_string(),
            path = dump_path.display().to_string(),
            component = component_name
        );
        error_handling(eden_core::error::EpError::fs(format!("Dump file not found: {}", e)), &mut span)
    })?;

    log_info!(
        ctx,
        "Successfully downloaded dump file",
        audience = LogAudience::Internal,
        timestamp = timestamp,
        component = component_name,
        size_bytes = dump_bytes.len()
    );

    // Return the encrypted dump file
    let filename = dump_path.file_name().and_then(|n| n.to_str()).unwrap_or("dump.bin");

    Ok(HttpResponse::Ok()
        .content_type("application/octet-stream")
        .append_header(("Content-Disposition", format!("attachment; filename=\"{}\"", filename)))
        .body(dump_bytes))
}

#[cfg(all(feature = "openapi", not(external_db)))]
#[utoipa::path(
    get,
    tags = ["Backup"],
    path="/backups/{timestamp}/download",
    security(),
    params(
        ("timestamp" = i64, Path, description = "Backup timestamp (created_at value)"),
        DownloadQuery
    ),
    responses((status = OK, body = Vec<u8>))
)]
pub async fn download(
    _req: HttpRequest,
    _auth: web::ReqData<ParsedJwt>,
    _db: web::Data<EdenDb>,
    _timestamp: web::Path<i64>,
    _query: web::Query<DownloadQuery>,
) -> Result<impl Responder, actix_web::Error> {
    Ok(HttpResponse::NotImplemented().finish())
}
