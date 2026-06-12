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
use serde_json::json;
use std::str::FromStr;
use telemetry_extensions_macro::with_telemetry;
use uuid::Uuid;

/// Delete a pipeline. Stops the CDC worker if running and drops the replication slot/publication.
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Pipelines"],
    path="/pipelines/{pipeline}",
    operation_id = "delete_pipeline",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn delete(
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

    // Stop the CDC worker if running
    if *schema.status() == PipelineStatus::Running {
        let _ = cdc_manager.stop(schema.uuid()).await;
    }

    // Drop the replication slot and publication
    {
        use crate::pipeline::cdc::postgres::ReplicationCommands;

        let conn = database.pg_connection().await.map_err(|e| error_handling(e, &mut span))?;
        let cdc_config = schema.cdc_config();

        // Drop replication slot (if it exists)
        if let Some(slot_name) = cdc_config.effective_slot_name() {
            let drop_slot = ReplicationCommands::drop_replication_slot(slot_name);
            if let Err(e) = conn.execute(&drop_slot, &[]).await {
                let msg = e.to_string();
                if !msg.contains("does not exist") {
                    return Err(error_handling(EpError::database(e), &mut span));
                }
            }
        }

        // Drop publication (if it exists)
        if let Some(pub_name) = cdc_config.effective_publication_name() {
            let drop_pub = ReplicationCommands::drop_publication(pub_name);
            if let Err(e) = conn.execute(&drop_pub, &[]).await {
                let msg = e.to_string();
                if !msg.contains("does not exist") {
                    return Err(error_handling(EpError::database(e), &mut span));
                }
            }
        }
    }

    // Delete the pipeline from the database
    let conn = database.pg_connection().await.map_err(|e| error_handling(e, &mut span))?;

    conn.execute("DELETE FROM organization_pipelines WHERE pipeline_uuid = $1", &[schema.uuid()])
        .await
        .map_err(|e| error_handling(EpError::database(e), &mut span))?;

    conn.execute("DELETE FROM pipelines WHERE uuid = $1", &[schema.uuid()])
        .await
        .map_err(|e| error_handling(EpError::database(e), &mut span))?;

    EdenResponse::response(json!({
        "message": "Pipeline deleted successfully",
        "uuid": schema.uuid().to_string()
    }))
    .into()
}
