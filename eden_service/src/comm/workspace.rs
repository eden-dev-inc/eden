use crate::{EdenDb, error_handling};

use actix_web::{HttpResponse, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::EdenUuid;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use telemetry_extensions_macro::with_telemetry;

const WORKSPACE_KEY: &str = "dashboard-workspace";

#[derive(Debug, Deserialize, Serialize)]
pub struct WorkspaceDocument {
    pub schema: Value,
    #[serde(default)]
    pub saved_views: Vec<Value>,
}

fn validate_workspace_document(document: &WorkspaceDocument) -> Result<(), EpError> {
    let schema = document.schema.as_object().ok_or_else(|| EpError::request("workspace schema must be an object"))?;

    if schema.get("version").and_then(Value::as_i64) != Some(4) {
        return Err(EpError::request("workspace schema version must be 4"));
    }

    if !schema.get("views").is_some_and(Value::is_array) {
        return Err(EpError::request("workspace schema views must be an array"));
    }

    Ok(())
}

#[with_telemetry]
pub async fn get_workspace(auth: web::ReqData<ParsedJwt>, database: web::Data<EdenDb>) -> Result<impl Responder, actix_web::Error> {
    let org_uuid = auth.org_uuid().uuid();
    let user_uuid = auth.user_uuid().uuid();
    let conn = database.pg_connection().await.map_err(|e| error_handling(e, &mut span))?;
    let row = conn
        .query_opt(
            "SELECT workspace_schema, saved_views FROM workspace_views WHERE organization_uuid = $1 AND user_uuid = $2 AND workspace_key = $3",
            &[&org_uuid, &user_uuid, &WORKSPACE_KEY],
        )
        .await
        .map_err(|e| error_handling(EpError::database(e), &mut span))?;

    let Some(row) = row else {
        return Ok(HttpResponse::NoContent().finish());
    };

    let schema: Value = row.get("workspace_schema");
    let saved_views: Value = row.get("saved_views");
    let saved_views = saved_views.as_array().cloned().unwrap_or_default();

    Ok(HttpResponse::Ok().json(WorkspaceDocument { schema, saved_views }))
}

#[with_telemetry]
pub async fn put_workspace(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    body: web::Json<WorkspaceDocument>,
) -> Result<impl Responder, actix_web::Error> {
    let document = body.into_inner();
    validate_workspace_document(&document).map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = auth.org_uuid().uuid();
    let user_uuid = auth.user_uuid().uuid();
    let saved_views = Value::Array(document.saved_views.clone());
    let conn = database.pg_connection().await.map_err(|e| error_handling(e, &mut span))?;
    conn.execute(
        "INSERT INTO workspace_views (organization_uuid, user_uuid, workspace_key, workspace_schema, saved_views, updated_at)
         VALUES ($1, $2, $3, $4, $5, NOW())
         ON CONFLICT (organization_uuid, user_uuid, workspace_key)
         DO UPDATE SET workspace_schema = EXCLUDED.workspace_schema, saved_views = EXCLUDED.saved_views, updated_at = NOW()",
        &[&org_uuid, &user_uuid, &WORKSPACE_KEY, &document.schema, &saved_views],
    )
    .await
    .map_err(|e| error_handling(EpError::database(e), &mut span))?;

    Ok(HttpResponse::Ok().json(document))
}
