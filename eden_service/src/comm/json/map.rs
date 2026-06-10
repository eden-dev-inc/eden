use actix_web::{Responder, web};
use eden_core::json::map::map_json;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct JsonMapInput {
    pub json: Value,
    pub schema: Value,
    pub mappings: HashMap<String, String>,
}

/// Map JSON
#[utoipa::path(
    post,
    tags = ["Json"],
    path="/json/map",
    request_body = JsonMapInput,
    operation_id = "json_map",
        responses((status = OK, body = String))
)]
pub async fn map(req: web::Json<JsonMapInput>) -> impl Responder {
    let json_map_input = req.into_inner();
    map_json(&json_map_input.json, &json_map_input.schema, &json_map_input.mappings).map_err(actix_web::error::ErrorInternalServerError)
}
