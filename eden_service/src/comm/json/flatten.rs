use actix_web::{Responder, web};
use eden_core::json::flatten::flatten_json;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, ToSchema)]
pub struct JsonFlattenInput {
    pub json: Value,
    pub prefix: Option<String>,
}

/// Flatten JSON
#[utoipa::path(
    post,
    tags = ["Json"],
    path="/json/flatten",
    request_body = JsonFlattenInput,
    operation_id = "json_flatten",
        responses((status = OK, body = String))
)]
pub async fn flatten(input: web::Json<JsonFlattenInput>, // Changed to accept any JSON Value
) -> impl Responder {
    let input = input.into_inner();

    let mut flattened = Map::new();

    flatten_json(&input.json, &input.prefix.unwrap_or_default(), &mut flattened);

    serde_json::to_string(&Value::Object(flattened)).map_err(actix_web::error::ErrorInternalServerError)
}
