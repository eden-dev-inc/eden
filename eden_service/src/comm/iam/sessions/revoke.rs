//! Revoke user sessions.

use crate::EdenDb;
use crate::user_sessions::SESSION_STORE;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use eden_core::auth::ParsedJwt;
use serde::{Deserialize, Serialize};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// Request to revoke sessions other than the current one.
#[derive(Debug, Deserialize, ToSchema)]
pub struct RevokeOthersRequest {
    /// If true, confirms the revocation (required).
    pub confirm: bool,
}

/// Response after revoking sessions.
#[derive(Debug, Serialize, ToSchema)]
pub struct RevokeResponse {
    pub revoked_count: usize,
    pub message: String,
}

/// Revoke Other Sessions
///
/// Revokes all active sessions for the current user except the current session.
/// This is useful for security purposes when a user suspects unauthorized access.
/// The JWT tokens for revoked sessions are blacklisted until they expire.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["IAM"],
    path = "/iam/sessions/revoke-others",
    operation_id = "revoke_other_sessions",
    request_body = RevokeOthersRequest,
    responses((status = OK, body = RevokeResponse))
)]
pub async fn revoke_others(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    body: web::Json<RevokeOthersRequest>,
    db: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    if !body.confirm {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Must confirm revocation by setting confirm: true"
        })));
    }

    let organization_uuid = auth.org_uuid().to_string();
    let user_uuid = auth.user_uuid().to_string();

    // Get current request's IP and user agent to preserve current session
    let current_ip = req.connection_info().realip_remote_addr().unwrap_or("unknown").to_string();
    let current_ua = req.headers().get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();

    // Get current session's JTI for exclusion
    let current_jti = auth.jti();

    // Get JTIs of other sessions to blacklist before revoking
    let other_jtis = SESSION_STORE.get_other_session_jtis(&organization_uuid, &user_uuid, &current_ip, &current_ua, current_jti);

    // Get all sessions before revocation
    let sessions_before = SESSION_STORE.get_user_sessions(&organization_uuid, &user_uuid);
    // Count sessions that will be revoked (excluding current session by JTI or IP/UA)
    let other_sessions_count = sessions_before
        .iter()
        .filter(|s| {
            if let Some(cjti) = current_jti {
                s.jti.as_deref() != Some(cjti)
            } else {
                !(s.ip_address == current_ip && s.user_agent == current_ua)
            }
        })
        .count();

    // Revoke all sessions for this user
    SESSION_STORE.revoke_user_sessions(&organization_uuid, &user_uuid);

    // Blacklist the JTIs of other sessions
    for jti in &other_jtis {
        crate::jwt_blacklist::blacklist_jti(&**db, jti).await;
    }

    // Re-register current session (effectively keeping it active)
    use analytics_schema::events::AuthMethod;
    SESSION_STORE.record_session_with_jti(
        &organization_uuid,
        &user_uuid,
        auth.user_id().as_str(),
        &current_ip,
        &current_ua,
        AuthMethod::Bearer,
        auth.jti(),
    );

    Ok(HttpResponse::Ok().json(RevokeResponse {
        revoked_count: other_sessions_count,
        message: format!("Successfully revoked {} other session(s)", other_sessions_count),
    }))
}

/// Revoke All Sessions
///
/// Revokes all active sessions for the current user, including the current session.
/// After this call, the user will need to log in again. The JWT token will be
/// blacklisted until it expires, preventing further use.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["IAM"],
    path = "/iam/sessions/revoke-all",
    operation_id = "revoke_all_sessions",
    request_body = RevokeOthersRequest,
    responses((status = OK, body = RevokeResponse))
)]
pub async fn revoke_all(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    body: web::Json<RevokeOthersRequest>,
    db: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    if !body.confirm {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Must confirm revocation by setting confirm: true"
        })));
    }

    let organization_uuid = auth.org_uuid().to_string();
    let user_uuid = auth.user_uuid().to_string();

    // Get count before revocation
    let sessions_before = SESSION_STORE.get_user_sessions(&organization_uuid, &user_uuid);
    let count = sessions_before.len();

    // Revoke all sessions
    SESSION_STORE.revoke_user_sessions(&organization_uuid, &user_uuid);

    // Blacklist the user's JWT tokens until they expire
    crate::jwt_blacklist::blacklist_user(&**db, &organization_uuid, &user_uuid).await;

    Ok(HttpResponse::Ok().json(RevokeResponse {
        revoked_count: count,
        message: format!("Successfully revoked all {} session(s). Please log in again.", count),
    }))
}
