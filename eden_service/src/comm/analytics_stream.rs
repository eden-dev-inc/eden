//! Analytics Server-Sent Events endpoints.
//!
//! The verbose request stream was removed from this distribution. The routes
//! remain so clients receive a stable explicit response instead of a
//! missing-route fallback.

use crate::analytics::AnalyticsState;
use actix_web::{HttpResponse, Responder, web};
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub(crate) struct StreamQuery {
    endpoint_uuid: Option<String>,
    endpoint_uuids: Option<String>,
    organization_uuid: Option<String>,
    ep_kind: Option<String>,
    #[serde(default)]
    anomalies_only: bool,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub(crate) struct QueryStreamQuery {
    endpoint_uuid: Option<String>,
    endpoint_uuids: Option<String>,
    organization_uuid: Option<String>,
    ep_kind: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub(crate) struct CaptureAllRequest {
    endpoint_uuid: String,
    duration_secs: u64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub(crate) struct CaptureAllQuery {
    endpoint_uuid: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CaptureAllEndpointStatusResponse {
    endpoint_uuid: String,
    active: bool,
    remaining_secs: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CaptureAllStatusListResponse {
    captures: Vec<CaptureAllEndpointStatusResponse>,
}

#[derive(Debug, Serialize)]
struct AnalyticsUnavailable {
    error: &'static str,
    message: &'static str,
}

fn unavailable() -> HttpResponse {
    HttpResponse::Gone().json(AnalyticsUnavailable {
        error: "analytics_stream_unavailable",
        message: "Verbose request stream analytics are not included in this build.",
    })
}

pub(crate) async fn stream_sse(_: web::Data<AnalyticsState>, _: web::Query<StreamQuery>) -> impl Responder {
    unavailable()
}

pub(crate) async fn query_stream_sse(_: web::Data<AnalyticsState>, _: web::Query<QueryStreamQuery>) -> impl Responder {
    unavailable()
}

pub(crate) async fn activate_capture_all(
    _: web::Data<AnalyticsState>,
    _: web::Json<CaptureAllRequest>,
) -> Result<impl Responder, actix_web::Error> {
    Ok(unavailable())
}

pub(crate) async fn capture_all_status(
    _: web::Data<AnalyticsState>,
    _: web::Query<CaptureAllQuery>,
) -> Result<impl Responder, actix_web::Error> {
    Ok(HttpResponse::Ok().json(CaptureAllStatusListResponse { captures: Vec::new() }))
}

pub(crate) async fn deactivate_capture_all(
    _: web::Data<AnalyticsState>,
    _: web::Json<CaptureAllRequest>,
) -> Result<impl Responder, actix_web::Error> {
    Ok(unavailable())
}
