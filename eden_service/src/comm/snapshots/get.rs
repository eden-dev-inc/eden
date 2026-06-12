use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use std::str::FromStr;
use telemetry_extensions_macro::with_telemetry;
use uuid::Uuid;

/// Get a single snapshot by id or uuid
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Snapshots"],
    path="/snapshots/{snapshot}",
    operation_id = "get_snapshot",
    responses((status = OK, body = endpoint_core::ep_core::database::schema::snapshot::SnapshotSchema))
)]
pub async fn get(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    snapshot: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = auth.org_uuid();
    let snapshot_ref = snapshot.into_inner();

    let schema = match Uuid::from_str(&snapshot_ref) {
        Ok(uuid) => database.select_snapshot_uuid(&uuid, org_uuid, telemetry_wrapper).await,
        Err(_) => database.select_snapshot_id(&snapshot_ref, org_uuid, telemetry_wrapper).await,
    }
    .map_err(|e| error_handling(e, &mut span))?;

    // Verify snapshot belongs to the caller's organization
    let conn = database.pg_connection().await.map_err(|e| error_handling(e, &mut span))?;
    let ownership = conn
        .query_opt(
            "SELECT 1 FROM organization_snapshots WHERE organization_uuid = $1 AND snapshot_uuid = $2",
            &[org_uuid, schema.uuid()],
        )
        .await
        .map_err(|e| error_handling(EpError::database(e), &mut span))?;
    if ownership.is_none() {
        return Err(error_handling(EpError::auth("Snapshot not found in this organization"), &mut span));
    }

    EdenResponse::response(schema).into()
}

/// List all snapshots for the organization
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Snapshots"],
    path="/snapshots",
    operation_id = "list_snapshots",
    responses((status = OK, body = Vec<endpoint_core::ep_core::database::schema::snapshot::SnapshotSchema>))
)]
pub async fn get_all(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid();

    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let schemas = database.select_all_snapshots(org_uuid, telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(schemas).into()
}
