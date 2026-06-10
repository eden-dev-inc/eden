//! Per-user analytics dashboard preferences (saved y-axis ranges, named color
//! ranges, etc.). The body is an opaque JSON blob owned by the dashboard; the
//! backend only scopes it to the authenticated `(user, organization)` pair and
//! persists it durably (see `database::db::analytics_prefs`).

use crate::EdenDb;
use crate::error_handling;
use actix_web::{Responder, web};
use database::db::analytics_prefs::AnalyticsPrefsStore;
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::EdenUuid;
use eden_core::response::EdenResponse;
use serde::{Deserialize, Serialize};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Current epoch-millis as a string (opaque updated-at stamp, no chrono needed).
fn now_millis() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_default()
}

#[derive(Serialize, PartialEq, ToSchema)]
pub struct GetResponse {
    /// The saved preference blob, or `null` if the user has none yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefs: Option<serde_json::Value>,
}

/// Get the authenticated user's saved analytics dashboard preferences.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["IAM"],
    path = "/iam/humans/me/analytics-prefs",
    operation_id = "get_analytics_prefs",
    responses((status = OK, body = GetResponse))
)]
pub async fn get(auth: web::ReqData<ParsedJwt>, database: web::Data<EdenDb>) -> Result<impl Responder, actix_web::Error> {
    let stored = database
        .get_analytics_prefs(auth.user_uuid().uuid(), auth.org_uuid().uuid())
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    // Stored as text; parse back to JSON so the client receives a real object.
    let prefs = stored.and_then(|s| serde_json::from_str(&s).ok());
    EdenResponse::response(GetResponse { prefs }).into()
}

#[derive(Deserialize, ToSchema)]
pub struct PutInput {
    /// The full preference blob to persist (replaces any prior value).
    pub prefs: serde_json::Value,
}

#[derive(Serialize, PartialEq, ToSchema)]
pub struct PutResponse {
    pub saved: bool,
    pub updated_at: String,
}

/// Replace the authenticated user's analytics dashboard preferences.
#[with_telemetry]
#[utoipa::path(
    put,
    tags = ["IAM"],
    path = "/iam/humans/me/analytics-prefs",
    operation_id = "put_analytics_prefs",
    request_body = PutInput,
    responses((status = OK, body = PutResponse))
)]
pub async fn put(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    input: web::Json<PutInput>,
) -> Result<impl Responder, actix_web::Error> {
    let prefs_str = serde_json::to_string(&input.prefs)
        .map_err(|e| error_handling(EpError::parse(format!("invalid analytics prefs json: {e}")), &mut span))?;
    let updated_at = now_millis();
    database
        .upsert_analytics_prefs(auth.user_uuid().uuid(), auth.org_uuid().uuid(), &prefs_str, &updated_at)
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    EdenResponse::response(PutResponse { saved: true, updated_at }).into()
}
