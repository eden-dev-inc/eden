use crate::EdenDb;
use crate::EpError;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::org_transfer::{ImportConflictStrategy, ImportResult};
use eden_core::auth::ParsedJwt;
use eden_core::format::EdenNodeUuid;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_error, log_info};
use function_name::named;
use serde::Deserialize;
use std::path::PathBuf;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

#[derive(Deserialize, ToSchema)]
pub struct ImportOrganizationInput {
    pub artifact_path: String,
    pub encrypt_password: String,
    pub target_eden_node_uuid: EdenNodeUuid,
    #[serde(default = "default_conflict_strategy")]
    pub conflict_strategy: ImportConflictStrategy,
}

impl std::fmt::Debug for ImportOrganizationInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportOrganizationInput")
            .field("artifact_path", &self.artifact_path)
            .field("encrypt_password", &"[REDACTED]")
            .field("target_eden_node_uuid", &self.target_eden_node_uuid)
            .field("conflict_strategy", &self.conflict_strategy)
            .finish()
    }
}

fn default_conflict_strategy() -> ImportConflictStrategy {
    ImportConflictStrategy::Abort
}

/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Organization Transfer"],
    path = "/organizations/import",
    request_body = ImportOrganizationInput,
    security(),
    responses((status = OK, body = ImportResult))
)]
#[named]
#[allow(clippy::too_many_arguments)]
pub async fn post_import(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    db: web::Data<EdenDb>,
    input: web::Json<ImportOrganizationInput>,
) -> Result<impl Responder, actix_web::Error> {
    let ctx = ctx_with_trace!();

    verify_control_perms(&db, &auth, None, ControlPerms::DESTROY, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    log_info!(
        ctx.clone(),
        "Importing organization from artifact",
        audience = LogAudience::Internal,
        artifact_path = &input.artifact_path
    );

    // Validate artifact_path is within the configured transfer directory to prevent path traversal
    let transfer_dir = PathBuf::from(eden_config::org_transfer().dir.clone().unwrap_or_else(|| "transfers".to_string()));
    let transfer_dir_canonical = tokio::fs::canonicalize(&transfer_dir).await.map_err(|e| {
        log_error!(ctx.clone(), "Transfer directory not found", audience = LogAudience::Internal, error = e.to_string());
        error_handling(EpError::init(format!("Transfer directory not accessible: {e}")), &mut span)
    })?;
    let artifact_path = transfer_dir_canonical.join(&input.artifact_path);
    let artifact_path = tokio::fs::canonicalize(&artifact_path).await.map_err(|e| {
        log_error!(ctx.clone(), "Artifact path not found", audience = LogAudience::Internal, error = e.to_string());
        error_handling(EpError::init(format!("Artifact not found: {e}")), &mut span)
    })?;
    if !artifact_path.starts_with(&transfer_dir_canonical) {
        return Err(error_handling(
            EpError::auth("Artifact path must be within the configured transfer directory".to_string()),
            &mut span,
        ));
    }

    let result = db
        .import_organization(&artifact_path, &input.encrypt_password, &input.target_eden_node_uuid, input.conflict_strategy)
        .await
        .map_err(|e| {
            log_error!(
                ctx.clone(),
                "Failed to import organization",
                audience = LogAudience::Internal,
                error = e.to_string()
            );
            error_handling(e, &mut span)
        })?;

    log_info!(
        ctx,
        "Successfully imported organization",
        audience = LogAudience::Internal,
        organization_uuid = result.organization_uuid.to_string(),
        users = result.users_imported,
        endpoints = result.endpoints_imported
    );

    EdenResponse::response(result).into()
}
