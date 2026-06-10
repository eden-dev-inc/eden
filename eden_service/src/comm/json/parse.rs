use actix_web::{Responder, web};
use eden_core::json::parse::parse_json;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct JsonParseInput {
    pub json: Value,
    pub paths: Vec<String>,
}

/// Parse JSON
#[utoipa::path(
    post,
    tags = ["Json"],
    path="/json/parse",
    request_body = JsonParseInput,
    operation_id = "json_parse",
        responses((status = OK, body = String))
)]
pub async fn parse(req: web::Json<JsonParseInput>) -> impl Responder {
    let json_parse_input = req.into_inner();
    parse_json(&json_parse_input.json, json_parse_input.paths).map_err(actix_web::error::ErrorInternalServerError)
}
