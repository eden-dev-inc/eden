use actix_web::{Responder, web};
use eden_core::json::unflatten::unflatten_json;
use serde_json::Value;
use std::collections::HashMap;

/// Unflatten JSON
#[utoipa::path(
    post,
    tags = ["Json"],
    path="/json/unflatten",
    request_body = HashMap<String, Value>,
    operation_id = "json_unflatten",
        responses((status = OK, body = String))
)]
pub async fn unflatten(req: web::Json<HashMap<String, Value>>) -> impl Responder {
    let flat_map = req.into_inner();
    serde_json::to_string(&unflatten_json(&flat_map)).map_err(actix_web::error::ErrorInternalServerError)
}
