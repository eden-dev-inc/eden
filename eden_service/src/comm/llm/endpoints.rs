use crate::EdenDb;
use crate::comm::rbac::verify_endpoint_access;
use crate::error_handling;
use actix_web::{HttpResponse, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::error::ResultEP;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::endpoint::EpKind;
use eden_core::format::rbac::DataPerms;
use eden_core::response::EdenResponse;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_schema::endpoint::EndpointSchema;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

#[derive(Debug, Serialize, ToSchema, PartialEq)]
pub struct LlmEndpointInfo {
    pub id: String,
    pub uuid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

fn resolve_provider_and_model(config: &Value) -> (Option<String>, Option<String>) {
    let conn = config.get("read_conn").or_else(|| config.get("write_conn")).or_else(|| config.get("target"));

    let provider = conn.and_then(|value| value.get("provider")).and_then(Value::as_str).map(ToString::to_string);

    let model = conn
        .and_then(|value| value.get("defaults").and_then(|defaults| defaults.get("model")).or_else(|| value.get("model")))
        .and_then(Value::as_str)
        .map(ToString::to_string);

    (provider, model)
}

async fn append_chat_endpoint_info(
    results: &mut Vec<LlmEndpointInfo>,
    seen: &mut BTreeSet<String>,
    database: &EdenDb,
    auth: &ParsedJwt,
    org_cache: &OrganizationCacheUuid,
    endpoint_schema: EndpointSchema,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<()> {
    if endpoint_schema.kind() != EpKind::Llm {
        return Ok(());
    }

    let endpoint_uuid = endpoint_schema.uuid().to_string();
    if seen.contains(&endpoint_uuid) {
        return Ok(());
    }

    let endpoint_cache_uuid = endpoint_schema.cache_key(org_cache.clone());
    if verify_endpoint_access(
        database,
        auth,
        &endpoint_cache_uuid,
        endpoint_schema.endpoint_uuid(),
        DataPerms::READ,
        telemetry_wrapper,
    )
    .await
    .is_err()
    {
        return Ok(());
    }

    let config = endpoint_schema.config().serialize()?;
    let (provider, model) = resolve_provider_and_model(&config);

    seen.insert(endpoint_uuid);
    results.push(LlmEndpointInfo {
        id: endpoint_schema.id().to_string(),
        uuid: endpoint_schema.uuid().to_string(),
        description: endpoint_schema.description(),
        provider,
        model,
    });

    Ok(())
}

/// List LLM endpoints that the caller can use through the LLM or agent gateway.
/// **Permissions**: same data-plane access check as the gateway execution path.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["LLM"],
    path = "/llm/endpoints",
    responses((status = 200, body = EdenResponse<Vec<LlmEndpointInfo>>))
)]
pub async fn list_chat_endpoints(auth: web::ReqData<ParsedJwt>, database: web::Data<EdenDb>) -> Result<impl Responder, actix_web::Error> {
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());
    let endpoint_schemas =
        database.select_all_endpoints(auth.org_uuid(), telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;

    let mut results = Vec::new();
    let mut seen = BTreeSet::new();

    for endpoint_schema in endpoint_schemas {
        append_chat_endpoint_info(&mut results, &mut seen, &database, &auth, &org_cache, endpoint_schema, telemetry_wrapper)
            .await
            .map_err(|e| error_handling(e, &mut span))?;
    }

    let response: Result<HttpResponse, actix_web::error::Error> = EdenResponse::response(results).into();
    response
}
