//! API usage history query endpoint.

use actix_web::{Responder, web};
use chrono::{DateTime, Utc};
use eden_core::auth::ParsedJwt;
use eden_core::response::EdenResponse;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct ApiUsageItem {
    pub request_time: DateTime<Utc>,
    pub request_id: String,
    pub http_method: String,
    pub http_path: String,
    pub http_status: u16,
    pub latency_us: u64,
    pub endpoint_uuid: Option<String>,
    pub client_ip: String,
    pub error_code: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct ApiUsageSummary {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub avg_latency_us: f64,
    pub p95_latency_us: u64,
    pub p99_latency_us: u64,
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct ApiUsageResponse {
    pub requests: Vec<ApiUsageItem>,
    pub summary: ApiUsageSummary,
    pub total: usize,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ApiUsageQuery {
    #[serde(default = "default_days")]
    pub days: u32,
    #[serde(default = "default_limit")]
    pub limit: usize,
    pub method: Option<String>,
    pub path_prefix: Option<String>,
    pub status: Option<String>,
    pub endpoint_uuid: Option<String>,
}

fn default_days() -> u32 {
    7
}

fn default_limit() -> usize {
    100
}

#[utoipa::path(
    get,
    tags = ["IAM"],
    path = "/iam/usage",
    operation_id = "get_api_usage",
    params(ApiUsageQuery),
    responses((status = OK, body = ApiUsageResponse))
)]
pub async fn get_api_usage(_auth: web::ReqData<ParsedJwt>, _query: web::Query<ApiUsageQuery>) -> Result<impl Responder, actix_web::Error> {
    let response = ApiUsageResponse {
        requests: vec![],
        summary: ApiUsageSummary {
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            avg_latency_us: 0.0,
            p95_latency_us: 0,
            p99_latency_us: 0,
        },
        total: 0,
    };
    EdenResponse::response(response).into()
}
