use crate::EdenDb;
use crate::EpError;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::lib::{ClickhouseConn, PgConn, RedisConn};
use database::org_transfer::{ExportMode, OrgTransferConfig, OrgTransferMetadata};
use eden_core::auth::ParsedJwt;
use eden_core::format::OrganizationUuid;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_error, log_info};
use function_name::named;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Deserialize, ToSchema)]
pub struct ExportOrganizationInput {
    pub organization_uuid: OrganizationUuid,
    pub encrypt_password: String,
    #[serde(default = "default_export_mode")]
    pub mode: ExportMode,
    pub description: Option<String>,
}

impl std::fmt::Debug for ExportOrganizationInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportOrganizationInput")
            .field("organization_uuid", &self.organization_uuid)
            .field("encrypt_password", &"[REDACTED]")
            .field("mode", &self.mode)
            .field("description", &self.description)
            .finish()
    }
}

fn default_export_mode() -> ExportMode {
    ExportMode::Copy
}

#[derive(Debug, Serialize, PartialEq, ToSchema)]
pub struct ExportOrganizationResponse {
    pub created_at: i64,
    pub organization_uuid: Uuid,
    pub metadata_path: String,
    pub mode: ExportMode,
    pub description: Option<String>,
    pub source_node: Option<String>,
}

/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Organization Transfer"],
    path = "/organizations/export",
    request_body = ExportOrganizationInput,
    security(),
    responses((status = OK, body = ExportOrganizationResponse))
)]
#[named]
#[allow(clippy::too_many_arguments)]
pub async fn post_export(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    db: web::Data<EdenDb>,
    input: web::Json<ExportOrganizationInput>,
) -> Result<impl Responder, actix_web::Error> {
    let ctx = ctx_with_trace!();

    verify_control_perms(&db, &auth, None, ControlPerms::DESTROY, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Verify the caller belongs to the organization being exported
    if auth.org_uuid() != &input.organization_uuid {
        return Err(error_handling(EpError::auth("Not authorized to export this organization".to_string()), &mut span));
    }

    log_info!(
        ctx.clone(),
        "Exporting organization",
        audience = LogAudience::Internal,
        organization_uuid = input.organization_uuid.to_string()
    );

    let transfer_dir = eden_config::org_transfer().dir.clone().unwrap_or_else(|| "transfers".to_string());

    let mut config = OrgTransferConfig::new(PathBuf::from(&transfer_dir));

    if let Some(desc) = &input.description {
        config = config.with_description(desc.clone());
    }

    let metadata = db.export_organization(&input.organization_uuid, &input.encrypt_password, config).await.map_err(|e| {
        log_error!(
            ctx.clone(),
            "Failed to export organization",
            audience = LogAudience::Internal,
            error = e.to_string()
        );
        error_handling(e, &mut span)
    })?;

    // If mode is Move, delete the source organization after successful export
    if input.mode == ExportMode::Move {
        use crate::comm::organization::delete::delete_organization;
        use database::db::methods::delete::DeleteMethod;
        use database::db::methods::delete::organization::DeleteOrganization;
        use eden_core::format::cache_id::OrganizationCacheId;
        use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
        use eden_core::format::{CacheObjectType, OrganizationId};
        use endpoint_core::ep_core::database::schema::organization::OrganizationSchema;

        let del_organization =
            <DeleteOrganization as DeleteMethod<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
                RedisConn,
                PgConn,
                ClickhouseConn,
            >>::new(CacheObjectType::new(Some(OrganizationCacheUuid::new(None, input.organization_uuid.clone())), None));

        delete_organization(&db, telemetry_wrapper, &del_organization).await.map_err(|e| {
            log_error!(
                ctx.clone(),
                "Export succeeded but failed to delete source organization (move mode)",
                audience = LogAudience::Internal,
                error = e.to_string()
            );
            error_handling(e, &mut span)
        })?;

        log_info!(
            ctx.clone(),
            "Source organization deleted (move mode)",
            audience = LogAudience::Internal,
            organization_uuid = input.organization_uuid.to_string()
        );
    }

    // Return only the metadata filename, not the full server-side path
    let metadata_filename = OrgTransferMetadata::metadata_filename(metadata.created_at, &metadata.organization_uuid);

    log_info!(
        ctx,
        "Successfully exported organization",
        audience = LogAudience::Internal,
        organization_uuid = metadata.organization_uuid.to_string(),
        created_at = metadata.created_at
    );

    let response_data = ExportOrganizationResponse {
        created_at: metadata.created_at,
        organization_uuid: metadata.organization_uuid,
        metadata_path: metadata_filename,
        mode: input.into_inner().mode,
        description: metadata.description,
        source_node: metadata.source_node,
    };

    EdenResponse::response(response_data).into()
}
