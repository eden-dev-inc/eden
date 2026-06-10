//! Get active sessions for the current user.

use crate::user_sessions::SESSION_STORE;
use actix_web::{HttpRequest, Responder, web};
use chrono::{DateTime, Utc};
use eden_core::auth::ParsedJwt;
use eden_core::response::EdenResponse;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Session response object.
#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct SessionResponse {
    pub uuid: String,
    pub device: String,
    pub ip_address: String,
    pub last_active: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub is_current: bool,
    pub request_count: u64,
}

/// List of active sessions response.
#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct SessionsResponse {
    pub sessions: Vec<SessionResponse>,
}

/// Get Active Sessions
///
/// Returns a list of all active sessions for the current user.
/// Sessions are identified by JTI (JWT ID) when using bearer auth, or by IP/UA for legacy sessions.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["IAM"],
    path = "/iam/sessions",
    operation_id = "list_sessions",
    responses((status = OK, body = SessionsResponse))
)]
pub async fn list_sessions(req: HttpRequest, auth: web::ReqData<ParsedJwt>) -> Result<impl Responder, actix_web::Error> {
    let organization_uuid = auth.org_uuid().to_string();
    let user_uuid = auth.user_uuid().to_string();

    // Get current request's JTI to identify current session (preferred)
    let current_jti = auth.jti().map(|s| s.to_string());

    // Fallback: IP/UA for legacy sessions without JTI
    let current_ip = req.connection_info().realip_remote_addr().unwrap_or("unknown").to_string();
    let current_ua = req.headers().get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();

    let entries = SESSION_STORE.get_user_sessions(&organization_uuid, &user_uuid);

    let sessions: Vec<SessionResponse> = entries
        .into_iter()
        .map(|entry| {
            // Identify current session: by JTI if available, otherwise by IP/UA
            let is_current = if let (Some(entry_jti), Some(req_jti)) = (&entry.jti, &current_jti) {
                entry_jti == req_jti
            } else if entry.jti.is_none() && current_jti.is_none() {
                // Legacy fallback: both have no JTI, match by IP/UA
                entry.ip_address == current_ip && entry.user_agent == current_ua
            } else {
                false
            };
            SessionResponse {
                uuid: entry.session_uuid,
                device: entry.device,
                ip_address: entry.ip_address,
                last_active: entry.last_active_at,
                started_at: entry.started_at,
                is_current,
                request_count: entry.request_count.load(std::sync::atomic::Ordering::Relaxed),
            }
        })
        .collect();

    EdenResponse::response(SessionsResponse { sessions }).into()
}
