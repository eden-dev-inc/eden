use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use crate::pipeline::manager::CdcManager;
use actix_web::{HttpRequest, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::snapshot::{SnapshotStatus, SourceMode};
use serde_json::json;
use std::str::FromStr;
use telemetry_extensions_macro::with_telemetry;
use uuid::Uuid;

/// Delete a snapshot (only if not currently running)
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["Snapshots"],
    path="/snapshots/{snapshot}",
    operation_id = "delete_snapshot",
    responses((status = OK, body = serde_json::Value))
)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    snapshot: web::Path<String>,
    database: web::Data<EdenDb>,
    cdc_manager: web::Data<CdcManager>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = auth.org_uuid();
    let snapshot_ref = snapshot.into_inner();

    // Org-scoped lookup (SQL joins organization_snapshots)
    let schema = match Uuid::from_str(&snapshot_ref) {
        Ok(uuid) => database.select_snapshot_uuid(&uuid, org_uuid, telemetry_wrapper).await,
        Err(_) => database.select_snapshot_id(&snapshot_ref, org_uuid, telemetry_wrapper).await,
    }
    .map_err(|e| error_handling(e, &mut span))?;

    if *schema.status() == SnapshotStatus::Running {
        // For CDC snapshots, stop the worker before deleting
        if matches!(schema.source_mode(), SourceMode::Cdc) {
            let _ = cdc_manager.stop(schema.uuid()).await;
        } else {
            return Err(error_handling(EpError::api("Cannot delete a running snapshot. Cancel it first."), &mut span));
        }
    }

    // For CDC snapshots, drop the replication slot and publication
    if matches!(schema.source_mode(), SourceMode::Cdc) {
        if let Some(cdc_config) = schema.cdc_config().as_ref() {
            use crate::pipeline::cdc::postgres::ReplicationCommands;

            let conn = database.pg_connection().await.map_err(|e| error_handling(e, &mut span))?;

            // Drop replication slot (if it exists)
            if let Some(slot_name) = cdc_config.effective_slot_name() {
                let drop_slot = ReplicationCommands::drop_replication_slot(slot_name);
                if let Err(e) = conn.execute(&drop_slot, &[]).await {
                    let msg = e.to_string();
                    // Ignore "does not exist" errors
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
    }

    // Atomically delete the snapshot only if it is not Running (prevents TOCTOU race).
    // For CDC snapshots we already stopped the worker above, so status should be Paused/Failed.
    let conn = database.pg_connection().await.map_err(|e| error_handling(e, &mut span))?;

    let deleted = conn
        .query_opt(
            "WITH deleted AS (
                DELETE FROM snapshots WHERE uuid = $1 AND status != 'Running' RETURNING uuid
            )
            DELETE FROM organization_snapshots WHERE snapshot_uuid IN (SELECT uuid FROM deleted)
            RETURNING (SELECT uuid FROM deleted)",
            &[schema.uuid()],
        )
        .await
        .map_err(|e| error_handling(EpError::database(e), &mut span))?;

    if deleted.is_none() {
        return Err(error_handling(EpError::api("Snapshot is currently running. Stop it before deleting."), &mut span));
    }

    EdenResponse::response(json!({
        "message": "Snapshot deleted successfully",
        "uuid": schema.uuid().to_string()
    }))
    .into()
}
