use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use crate::pipeline::manager::CdcManager;
use actix_web::{HttpRequest, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::pipeline::PipelineStatus;
use serde::Serialize;
use std::str::FromStr;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;
use uuid::Uuid;

/// Pause a running pipeline (stops the WAL consumer but keeps the replication slot).
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Pipelines"],
    path="/pipelines/{pipeline}/pause",
    operation_id = "pause_pipeline",
    responses((status = OK, body = PausePipelineResponse))
)]
pub async fn pause(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    pipeline: web::Path<String>,
    database: web::Data<EdenDb>,
    cdc_manager: web::Data<CdcManager>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = auth.org_uuid();
    let pipeline_ref = pipeline.into_inner();

    // Org-scoped lookup (SQL joins organization_pipelines)
    let schema = match Uuid::from_str(&pipeline_ref) {
        Ok(uuid) => database.select_pipeline_uuid(&uuid, org_uuid, telemetry_wrapper).await,
        Err(_) => database.select_pipeline_id(&pipeline_ref, org_uuid, telemetry_wrapper).await,
    }
    .map_err(|e| error_handling(e, &mut span))?;

    if *schema.status() != PipelineStatus::Running {
        return Err(error_handling(
            EpError::api(format!("Cannot pause pipeline in {} state", schema.status())),
            &mut span,
        ));
    }

    // Stop the CDC worker (keeps replication slot intact for resume)
    let pipeline_uuid = *schema.uuid();
    cdc_manager.pause(&pipeline_uuid).await.map_err(|e| error_handling(e, &mut span))?;

    // Update status to Paused
    database
        .update_pipeline_status(&pipeline_uuid, &PipelineStatus::Paused.to_string(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(PausePipelineResponse {
        uuid: pipeline_uuid.to_string(),
        status: "Paused".to_string(),
    })
    .into()
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct PausePipelineResponse {
    pub uuid: String,
    pub status: String,
}
