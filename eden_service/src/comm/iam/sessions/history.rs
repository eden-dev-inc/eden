//! Session history query endpoint.

use actix_web::{Responder, web};
use chrono::{DateTime, Utc};
use eden_core::auth::ParsedJwt;
use eden_core::response::EdenResponse;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct SessionHistoryItem {
    pub session_uuid: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub last_active_at: DateTime<Utc>,
    pub device: String,
    pub ip_address: String,
    pub auth_method: String,
    pub status: String,
    pub request_count: u64,
    pub error_count: u64,
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct SessionHistoryResponse {
    pub sessions: Vec<SessionHistoryItem>,
    pub total: usize,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct SessionHistoryQuery {
    #[serde(default = "default_days")]
    pub days: u32,
    #[serde(default = "default_limit")]
    pub limit: usize,
    pub status: Option<String>,
}

fn default_days() -> u32 {
    30
}

fn default_limit() -> usize {
    100
}

#[utoipa::path(
    get,
    tags = ["IAM"],
    path = "/iam/sessions/history",
    operation_id = "get_session_history",
    params(SessionHistoryQuery),
    responses((status = OK, body = SessionHistoryResponse))
)]
pub async fn get_session_history(
    _auth: web::ReqData<ParsedJwt>,
    _query: web::Query<SessionHistoryQuery>,
) -> Result<impl Responder, actix_web::Error> {
    EdenResponse::response(SessionHistoryResponse { sessions: vec![], total: 0 }).into()
}
