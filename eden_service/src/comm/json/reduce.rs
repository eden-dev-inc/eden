use actix_web::{Responder, web};
use eden_core::json::reduce::reduce_json;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct JsonReduceInput {
    pub json: Value,
    pub paths: Vec<String>,
}

/// Reduce JSON
#[utoipa::path(
    post,
    tags = ["Json"],
    path="/json/reduce",
    request_body = JsonReduceInput,
    operation_id = "json_reduce",
        responses((status = OK, body = String))
)]
pub async fn reduce(req: web::Json<JsonReduceInput>) -> impl Responder {
    let json_reduce_input = req.into_inner();
    reduce_json(&json_reduce_input.json, json_reduce_input.paths).map_err(actix_web::error::ErrorInternalServerError)
}
