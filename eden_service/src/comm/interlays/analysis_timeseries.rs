use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::format::rbac::ControlPerms;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

#[allow(dead_code)]
#[derive(Debug, serde::Deserialize)]
pub struct TimeseriesQuery {
    pub range: Option<String>,
    pub since: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, ToSchema, PartialEq)]
pub struct AnalysisTimeseriesPointResponse {
    pub timestamp: String,
    pub ops_per_sec_avg: Option<f64>,
    pub ops_per_sec_min: Option<f64>,
    pub ops_per_sec_max: Option<f64>,
    pub memory_avg: Option<f64>,
    pub memory_min: Option<f64>,
    pub memory_max: Option<f64>,
    pub cpu_avg: Option<f64>,
    pub cpu_min: Option<f64>,
    pub cpu_max: Option<f64>,
}

#[derive(Debug, Clone, serde::Serialize, ToSchema, PartialEq)]
pub struct InterlayTimeseriesResponse {
    pub interlay_id: String,
    pub endpoint_uuid: String,
    pub range: String,
    pub points: Vec<AnalysisTimeseriesPointResponse>,
}

#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["Interlays"],
    path = "/interlays/{interlay}/analysis/timeseries",
    operation_id = "get_interlay_analysis_timeseries",
    responses((status = GONE, body = serde_json::Value))
)]
pub async fn get_analysis_timeseries(
    _req: HttpRequest,
    database: web::Data<EdenDb>,
    auth: web::ReqData<ParsedJwt>,
    _interlay: web::Path<String>,
    _query: web::Query<TimeseriesQuery>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    Ok(actix_web::HttpResponse::Gone().json(serde_json::json!({
        "error": "interlay_analysis_timeseries_unavailable",
        "message": "Interlay analysis timeseries are not included in this build."
    })))
}
