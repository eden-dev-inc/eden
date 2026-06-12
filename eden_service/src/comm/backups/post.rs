use crate::EdenDb;
#[cfg(external_db)]
use crate::comm::rbac::verify_control_perms;
#[cfg(external_db)]
use crate::error_handling;
#[cfg(all(feature = "openapi", not(external_db)))]
use actix_web::HttpResponse;
use actix_web::{HttpRequest, Responder, web};
#[cfg(external_db)]
use database::backups::BackupConfig;
use eden_core::auth::ParsedJwt;
#[cfg(external_db)]
use eden_core::format::rbac::ControlPerms;
#[cfg(external_db)]
use eden_core::response::EdenResponse;
#[cfg(external_db)]
use eden_logger_internal::{LogAudience, ctx_with_trace, log_error, log_info};
#[cfg(external_db)]
use function_name::named;
use serde::{Deserialize, Serialize};
#[cfg(external_db)]
use std::path::PathBuf;
#[cfg(external_db)]
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateBackupInput {
    pub encrypt_password: String,
    pub description: Option<String>,
    pub source_node: Option<String>,
}

#[derive(Debug, Serialize, PartialEq, ToSchema)]
pub struct CreateBackupResponse {
    pub created_at: i64,
    pub metadata_path: String,
    pub description: Option<String>,
    pub source_node: Option<String>,
}

#[cfg(external_db)]
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Backup"],
    path="/backups",
    request_body = CreateBackupInput,
    security(),
    responses((status = OK, body = CreateBackupResponse))
)]
#[named]
#[allow(clippy::too_many_arguments)]
pub async fn post(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    db: web::Data<EdenDb>,
    input: web::Json<CreateBackupInput>,
) -> Result<impl Responder, actix_web::Error> {
    let ctx = ctx_with_trace!();

    verify_control_perms(&db, &auth, None, ControlPerms::DESTROY, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    log_info!(
        ctx.clone(),
        "Creating backup",
        audience = LogAudience::Internal,
        description = input.description.as_deref().unwrap_or("(none)")
    );

    let backup_dir = eden_config::backup().dir.clone().unwrap_or_else(|| "backups".to_string());

    let pg_password = eden_config::databases().postgres.password.clone();
    if pg_password.is_empty() {
        log_error!(ctx.clone(), "POSTGRES_PASSWORD not configured", audience = LogAudience::Internal);
        return Err(error_handling(
            eden_core::error::EpError::init("POSTGRES_PASSWORD is required for backup creation"),
            &mut span,
        ));
    }

    // Build backup configuration
    let mut config = BackupConfig::persistent(PathBuf::from(&backup_dir));

    if let Some(desc) = &input.description {
        config = config.with_description(desc.clone());
    }

    if let Some(node) = &input.source_node {
        config = config.with_source_node(node.clone());
    }

    // Create backup
    let backup = db.create_backup_with_config(&pg_password, &input.encrypt_password, config).await.map_err(|e| {
        log_error!(ctx.clone(), "Failed to create backup", audience = LogAudience::Internal, error = e.to_string());
        error_handling(e, &mut span)
    })?;

    let metadata_path = backup.metadata_path.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "unknown".to_string());

    log_info!(
        ctx,
        "Successfully created backup",
        audience = LogAudience::Internal,
        created_at = backup.created_at,
        metadata_path = &metadata_path
    );

    let response_data = CreateBackupResponse {
        created_at: backup.created_at,
        metadata_path,
        description: backup.config.description,
        source_node: backup.config.source_node,
    };

    EdenResponse::response(response_data).into()
}

#[cfg(all(feature = "openapi", not(external_db)))]
#[utoipa::path(
    post,
    tags = ["Backup"],
    path="/backups",
    request_body = CreateBackupInput,
    security(),
    responses((status = OK, body = CreateBackupResponse))
)]
pub async fn post(
    _req: HttpRequest,
    _auth: web::ReqData<ParsedJwt>,
    _db: web::Data<EdenDb>,
    _input: web::Json<CreateBackupInput>,
) -> Result<impl Responder, actix_web::Error> {
    Ok(HttpResponse::NotImplemented().finish())
}
