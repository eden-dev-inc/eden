use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use crate::pipeline::manager::CdcManager;
use actix_web::{HttpRequest, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use serde::Serialize;
use std::str::FromStr;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;
use uuid::Uuid;

/// Get real-time CDC status for a pipeline.
///
/// Returns CDC-specific stats: whether the worker is active, the last confirmed LSN,
/// replication slot lag, and buffer depth.
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Pipelines"],
    path="/pipelines/{pipeline}/status",
    operation_id = "get_pipeline_status",
    responses((status = OK, body = PipelineStatusResponse))
)]
pub async fn status(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    pipeline: web::Path<String>,
    database: web::Data<EdenDb>,
    cdc_manager: web::Data<CdcManager>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
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

    let pipeline_uuid = *schema.uuid();
    let worker_active = cdc_manager.is_active(&pipeline_uuid).await;

    // Get replication slot lag
    let slot_lag = {
        let cdc_config = schema.cdc_config();
        if let Some(slot_name) = cdc_config.effective_slot_name() {
            get_slot_lag(&database, slot_name).await.ok()
        } else {
            None
        }
    };

    let response = PipelineStatusResponse {
        uuid: pipeline_uuid.to_string(),
        status: schema.status().to_string(),
        worker_active,
        last_lsn: schema.last_lsn().clone(),
        slot_lag,
    };

    EdenResponse::response(response).into()
}

/// Get replication slot lag info.
async fn get_slot_lag(database: &EdenDb, slot_name: &str) -> Result<SlotLagInfo, EpError> {
    use crate::pipeline::cdc::postgres::ReplicationCommands;

    let conn = database.pg_connection().await?;
    let lag_sql = ReplicationCommands::slot_lag(slot_name);
    let row = conn.query_opt(&lag_sql, &[]).await.map_err(EpError::database)?;

    match row {
        Some(row) => {
            let confirmed_lsn: Option<String> = row.try_get("confirmed_flush_lsn").ok();
            let lag_bytes: Option<i64> = row.try_get("lag_bytes").ok();

            Ok(SlotLagInfo {
                slot_name: slot_name.to_string(),
                confirmed_flush_lsn: confirmed_lsn,
                lag_bytes: lag_bytes.map(|b| b as u64),
            })
        }
        None => Ok(SlotLagInfo {
            slot_name: slot_name.to_string(),
            confirmed_flush_lsn: None,
            lag_bytes: None,
        }),
    }
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct PipelineStatusResponse {
    pub uuid: String,
    pub status: String,
    pub worker_active: bool,
    pub last_lsn: Option<String>,
    pub slot_lag: Option<SlotLagInfo>,
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct SlotLagInfo {
    pub slot_name: String,
    pub confirmed_flush_lsn: Option<String>,
    pub lag_bytes: Option<u64>,
}
