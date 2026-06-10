use actix_web::http::header::HeaderMap;
use actix_web::{HttpResponse, Responder};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::{CacheId, OrganizationCacheId};
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::{CacheObjectType, EdenId, EdenUuid, OrganizationId, OrganizationUuid};
use eden_core::request::headers::{ORG_ID_HEADER, ORG_UUID_HEADER};
use serde_json::json;
use uuid::Uuid;

#[utoipa::path(
    get,
    tags = ["Help"],
    path="/help",
    responses((status = OK, body = Value))
)]
pub async fn help() -> impl Responder {
    // Build the help JSON dynamically so we can conditionally include
    // feature-gated endpoints (e.g. migration) at compile time.
    use serde_json::{Map, Value};

    let mut apis = Map::new();

    // Base
    apis.insert("/".to_string(), Value::String("Health check endpoint".to_string()));

    // Auth
    apis.insert("POST /api/v1/auth/login".to_string(), Value::String("User login".to_string()));
    apis.insert("POST /api/v1/auth/refresh".to_string(), Value::String("Refresh authentication token".to_string()));
    apis.insert("POST /api/v1/auth/robots/login".to_string(), Value::String("Robot login".to_string()));

    // Organization
    apis.insert(
        "POST /api/v1/organizations".to_string(),
        Value::String("Create organization (no auth required)".to_string()),
    );
    apis.insert("GET /api/v1/organizations/{org}".to_string(), Value::String("Get organization details".to_string()));
    apis.insert("PATCH /api/v1/organizations/{org}".to_string(), Value::String("Update organization".to_string()));
    apis.insert("DELETE /api/v1/organizations/{org}".to_string(), Value::String("Delete organization".to_string()));
    apis.insert(
        "POST /api/v1/organizations/{org}/export".to_string(),
        Value::String("Export organization".to_string()),
    );
    apis.insert(
        "POST /api/v1/organizations/{org}/import".to_string(),
        Value::String("Import organization".to_string()),
    );

    // IAM - Humans
    apis.insert("POST /api/v1/iam/humans".to_string(), Value::String("Create human".to_string()));
    apis.insert("GET /api/v1/iam/humans".to_string(), Value::String("List organization humans".to_string()));
    apis.insert("GET /api/v1/iam/humans/me".to_string(), Value::String("Get current human".to_string()));
    apis.insert("PATCH /api/v1/iam/humans/me".to_string(), Value::String("Update current human".to_string()));
    apis.insert("GET /api/v1/iam/humans/{human}".to_string(), Value::String("Get human details".to_string()));
    apis.insert("PATCH /api/v1/iam/humans/{human}".to_string(), Value::String("Update human".to_string()));
    apis.insert("DELETE /api/v1/iam/humans/{human}".to_string(), Value::String("Delete human".to_string()));
    apis.insert(
        "GET /api/v1/iam/access/endpoints/{endpoint}".to_string(),
        Value::String("Get resolved control-plane and data-plane access for the caller on an endpoint".to_string()),
    );
    apis.insert(
        "GET /api/v1/iam/security/endpoints/{endpoint}".to_string(),
        Value::String("Get a redacted security summary for an endpoint".to_string()),
    );

    // IAM - Agents
    apis.insert("POST /api/v1/iam/agents".to_string(), Value::String("Create agent".to_string()));
    apis.insert("GET /api/v1/iam/agents".to_string(), Value::String("List agents".to_string()));
    apis.insert("GET /api/v1/iam/agents/{agent}".to_string(), Value::String("Get agent".to_string()));
    apis.insert("PATCH /api/v1/iam/agents/{agent}".to_string(), Value::String("Update agent".to_string()));
    apis.insert(
        "POST /api/v1/iam/agents/{agent}/rotate-key".to_string(),
        Value::String("Rotate agent key".to_string()),
    );
    apis.insert("DELETE /api/v1/iam/agents/{agent}".to_string(), Value::String("Delete agent".to_string()));

    // IAM - Control plane
    apis.insert(
        "GET /api/v1/iam/control/subjects/{subject}".to_string(),
        Value::String("List control-plane grants for a subject".to_string()),
    );
    apis.insert(
        "DELETE /api/v1/iam/control/subjects/{subject}".to_string(),
        Value::String("Remove all control-plane grants for a subject".to_string()),
    );
    apis.insert(
        "GET /api/v1/iam/control/organizations".to_string(),
        Value::String("List organization control-plane grants".to_string()),
    );
    apis.insert(
        "PUT /api/v1/iam/control/organizations/subjects/{subject}".to_string(),
        Value::String("Set exact organization control-plane perms for a subject".to_string()),
    );
    apis.insert(
        "GET /api/v1/iam/control/organizations/subjects/{subject}".to_string(),
        Value::String("Get exact organization control-plane perms for a subject".to_string()),
    );
    apis.insert(
        "DELETE /api/v1/iam/control/organizations/subjects/{subject}".to_string(),
        Value::String("Revoke organization control-plane perms for a subject".to_string()),
    );
    apis.insert(
        "GET /api/v1/iam/control/endpoints/{endpoint}".to_string(),
        Value::String("List endpoint control-plane grants".to_string()),
    );
    apis.insert(
        "PUT /api/v1/iam/control/endpoints/{endpoint}/subjects/{subject}".to_string(),
        Value::String("Set exact endpoint control-plane perms for a subject".to_string()),
    );
    apis.insert(
        "GET /api/v1/iam/control/endpoints/{endpoint}/subjects/{subject}".to_string(),
        Value::String("Get exact endpoint control-plane perms for a subject".to_string()),
    );
    apis.insert(
        "DELETE /api/v1/iam/control/endpoints/{endpoint}/subjects/{subject}".to_string(),
        Value::String("Revoke endpoint control-plane perms for a subject".to_string()),
    );
    apis.insert(
        "GET /api/v1/iam/control/templates/{template}".to_string(),
        Value::String("List template control-plane grants".to_string()),
    );
    apis.insert(
        "PUT /api/v1/iam/control/templates/{template}/subjects/{subject}".to_string(),
        Value::String("Set exact template control-plane perms for a subject".to_string()),
    );
    apis.insert(
        "GET /api/v1/iam/control/workflows/{workflow}".to_string(),
        Value::String("List workflow control-plane grants".to_string()),
    );
    apis.insert(
        "PUT /api/v1/iam/control/workflows/{workflow}/subjects/{subject}".to_string(),
        Value::String("Set exact workflow control-plane perms for a subject".to_string()),
    );

    // IAM - Shared data plane
    apis.insert(
        "GET /api/v1/iam/data/endpoints/{endpoint}".to_string(),
        Value::String("List shared runtime grants on an endpoint".to_string()),
    );
    apis.insert(
        "PUT /api/v1/iam/data/endpoints/{endpoint}/subjects/{subject}".to_string(),
        Value::String("Set exact shared runtime perms for a subject on an endpoint".to_string()),
    );
    apis.insert(
        "GET /api/v1/iam/data/endpoints/{endpoint}/subjects/{subject}".to_string(),
        Value::String("Get exact shared runtime perms for a subject on an endpoint".to_string()),
    );
    apis.insert(
        "DELETE /api/v1/iam/data/endpoints/{endpoint}/subjects/{subject}".to_string(),
        Value::String("Revoke shared runtime perms for a subject on an endpoint".to_string()),
    );
    apis.insert(
        "GET /api/v1/iam/data/subjects/{subject}/endpoints".to_string(),
        Value::String("List shared runtime grants for a subject across endpoints".to_string()),
    );

    // IAM - ELS
    apis.insert(
        "GET /api/v1/iam/els/endpoints/{endpoint}/policies".to_string(),
        Value::String("List ELS policies for an endpoint".to_string()),
    );
    apis.insert(
        "GET /api/v1/iam/els/endpoints/{endpoint}/users".to_string(),
        Value::String("List ELS user assignments for an endpoint".to_string()),
    );

    // Endpoints
    apis.insert("POST /api/v1/endpoints".to_string(), Value::String("Create endpoint".to_string()));
    apis.insert("GET /api/v1/endpoints".to_string(), Value::String("List endpoints".to_string()));
    apis.insert("GET /api/v1/endpoints/{endpoint}".to_string(), Value::String("Get endpoint details".to_string()));
    apis.insert("PATCH /api/v1/endpoints/{endpoint}".to_string(), Value::String("Update endpoint".to_string()));
    apis.insert("DELETE /api/v1/endpoints/{endpoint}".to_string(), Value::String("Delete endpoint".to_string()));
    apis.insert(
        "POST /api/v1/endpoints/{endpoint}/read".to_string(),
        Value::String("Read from endpoint".to_string()),
    );
    apis.insert(
        "POST /api/v1/endpoints/{endpoint}/write".to_string(),
        Value::String("Write to endpoint".to_string()),
    );
    apis.insert(
        "POST /api/v1/endpoints/{endpoint}/transaction".to_string(),
        Value::String("Start transaction".to_string()),
    );
    apis.insert(
        "GET /api/v1/endpoints/{endpoint}/metadata".to_string(),
        Value::String("Get endpoint metadata".to_string()),
    );
    apis.insert(
        "POST /api/v1/endpoints/{endpoint}/metadata/collect".to_string(),
        Value::String("Collect endpoint metadata".to_string()),
    );
    apis.insert(
        "POST /api/v1/endpoints/google_workspace/oauth/exchange".to_string(),
        Value::String("Exchange a Google OAuth authorization code for a refresh token".to_string()),
    );

    // Endpoint Groups
    apis.insert("POST /api/v1/endpoint-groups".to_string(), Value::String("Create endpoint group".to_string()));
    apis.insert("GET /api/v1/endpoint-groups".to_string(), Value::String("List endpoint groups".to_string()));
    apis.insert("GET /api/v1/endpoint-groups/{group}".to_string(), Value::String("Get endpoint group".to_string()));
    apis.insert(
        "PATCH /api/v1/endpoint-groups/{group}".to_string(),
        Value::String("Update endpoint group".to_string()),
    );
    apis.insert(
        "DELETE /api/v1/endpoint-groups/{group}".to_string(),
        Value::String("Delete endpoint group".to_string()),
    );
    apis.insert(
        "POST /api/v1/endpoint-groups/{group}/members".to_string(),
        Value::String("Add member to endpoint group".to_string()),
    );
    apis.insert(
        "DELETE /api/v1/endpoint-groups/{group}/members/{endpoint}".to_string(),
        Value::String("Remove member from endpoint group".to_string()),
    );

    // Interlays
    apis.insert("POST /api/v1/interlays".to_string(), Value::String("Create interlay".to_string()));
    apis.insert("GET /api/v1/interlays".to_string(), Value::String("List interlays".to_string()));
    apis.insert("GET /api/v1/interlays/{interlay}".to_string(), Value::String("Get interlay".to_string()));
    apis.insert("PATCH /api/v1/interlays/{interlay}".to_string(), Value::String("Update interlay".to_string()));
    apis.insert("DELETE /api/v1/interlays/{interlay}".to_string(), Value::String("Delete interlay".to_string()));
    apis.insert("POST /api/v1/interlays/{interlay}/start".to_string(), Value::String("Start interlay".to_string()));
    apis.insert("POST /api/v1/interlays/{interlay}/stop".to_string(), Value::String("Stop interlay".to_string()));

    // APIs
    apis.insert("POST /api/v1/apis".to_string(), Value::String("Create API".to_string()));
    apis.insert("GET /api/v1/apis".to_string(), Value::String("List APIs".to_string()));
    apis.insert("GET /api/v1/apis/{api}".to_string(), Value::String("Get API".to_string()));
    apis.insert("DELETE /api/v1/apis/{api}".to_string(), Value::String("Delete API".to_string()));
    apis.insert("POST /api/v1/apis/{api}".to_string(), Value::String("Run API".to_string()));

    // Functions
    apis.insert("POST /api/v1/functions".to_string(), Value::String("Create function".to_string()));
    apis.insert("POST /api/v1/functions/{endpoint}/invoke".to_string(), Value::String("Invoke function".to_string()));

    // Templates
    apis.insert("POST /api/v1/templates".to_string(), Value::String("Create template".to_string()));
    apis.insert("GET /api/v1/templates".to_string(), Value::String("List templates".to_string()));
    apis.insert("GET /api/v1/templates/{template}".to_string(), Value::String("Get template".to_string()));
    apis.insert("PATCH /api/v1/templates/{template}".to_string(), Value::String("Update template".to_string()));
    apis.insert("DELETE /api/v1/templates/{template}".to_string(), Value::String("Delete template".to_string()));
    apis.insert("POST /api/v1/templates/{template}".to_string(), Value::String("Run template".to_string()));
    apis.insert("POST /api/v1/templates/{template}/render".to_string(), Value::String("Render template".to_string()));

    // Workflows
    apis.insert("POST /api/v1/workflows".to_string(), Value::String("Create workflow".to_string()));
    apis.insert("GET /api/v1/workflows/{workflow}".to_string(), Value::String("Get workflow".to_string()));
    apis.insert("PATCH /api/v1/workflows/{workflow}".to_string(), Value::String("Update workflow".to_string()));
    apis.insert("DELETE /api/v1/workflows/{workflow}".to_string(), Value::String("Delete workflow".to_string()));

    // JSON Operations
    apis.insert("POST /api/v1/json/flatten".to_string(), Value::String("Flatten JSON structure".to_string()));
    apis.insert("POST /api/v1/json/map".to_string(), Value::String("Map JSON data".to_string()));
    apis.insert("POST /api/v1/json/parse".to_string(), Value::String("Parse JSON".to_string()));
    apis.insert("POST /api/v1/json/reduce".to_string(), Value::String("Reduce JSON data".to_string()));
    apis.insert("POST /api/v1/json/unflatten".to_string(), Value::String("Unflatten JSON structure".to_string()));

    // Analytics
    apis.insert("GET /api/v1/analytics/status".to_string(), Value::String("Get analytics status".to_string()));
    apis.insert(
        "GET /api/v1/analytics/clickhouse".to_string(),
        Value::String("Export ClickHouse telemetry rows for metrics, traces, or logs".to_string()),
    );
    apis.insert(
        "GET /api/v1/analytics/clickhouse/metrics".to_string(),
        Value::String("Export ClickHouse metric rows".to_string()),
    );
    apis.insert(
        "GET /api/v1/analytics/clickhouse/traces".to_string(),
        Value::String("Export ClickHouse trace rows".to_string()),
    );
    apis.insert(
        "GET /api/v1/analytics/clickhouse/logs".to_string(),
        Value::String("Export ClickHouse log rows".to_string()),
    );
    apis.insert(
        "GET /api/v1/analytics/telemetry".to_string(),
        Value::String("Export ClickHouse telemetry rows for metrics, traces, or logs".to_string()),
    );
    apis.insert(
        "GET /api/v1/analytics/telemetry/metrics".to_string(),
        Value::String("Export ClickHouse metric rows".to_string()),
    );
    apis.insert(
        "GET /api/v1/analytics/telemetry/traces".to_string(),
        Value::String("Export ClickHouse trace rows".to_string()),
    );
    apis.insert(
        "GET /api/v1/analytics/telemetry/logs".to_string(),
        Value::String("Export ClickHouse log rows".to_string()),
    );
    apis.insert("POST /api/v1/analytics/enable".to_string(), Value::String("Enable analytics".to_string()));
    apis.insert("POST /api/v1/analytics/disable".to_string(), Value::String("Disable analytics".to_string()));
    apis.insert(
        "GET /api/v1/analytics/connections/stream".to_string(),
        Value::String("Stream connection metrics (SSE)".to_string()),
    );
    apis.insert("GET /api/v1/analytics/stream".to_string(), Value::String("Stream analytics (SSE)".to_string()));

    // Backups
    apis.insert("POST /api/v1/backups".to_string(), Value::String("Create backup".to_string()));
    apis.insert("GET /api/v1/backups".to_string(), Value::String("List backups".to_string()));
    apis.insert("GET /api/v1/backups/{timestamp}/download".to_string(), Value::String("Download backup".to_string()));
    apis.insert("DELETE /api/v1/backups/{timestamp}".to_string(), Value::String("Delete backup".to_string()));

    // Snapshots
    apis.insert("POST /api/v1/snapshots".to_string(), Value::String("Create snapshot".to_string()));
    apis.insert("GET /api/v1/snapshots".to_string(), Value::String("List snapshots".to_string()));
    apis.insert("GET /api/v1/snapshots/{snapshot}".to_string(), Value::String("Get snapshot".to_string()));
    apis.insert("DELETE /api/v1/snapshots/{snapshot}".to_string(), Value::String("Delete snapshot".to_string()));
    apis.insert(
        "GET /api/v1/snapshots/{snapshot}/status".to_string(),
        Value::String("Get snapshot status".to_string()),
    );

    // Pipelines
    apis.insert("POST /api/v1/pipelines".to_string(), Value::String("Create pipeline".to_string()));
    apis.insert("GET /api/v1/pipelines".to_string(), Value::String("List pipelines".to_string()));
    apis.insert("GET /api/v1/pipelines/{pipeline}".to_string(), Value::String("Get pipeline".to_string()));
    apis.insert("DELETE /api/v1/pipelines/{pipeline}".to_string(), Value::String("Delete pipeline".to_string()));
    apis.insert(
        "GET /api/v1/pipelines/{pipeline}/status".to_string(),
        Value::String("Get pipeline status".to_string()),
    );
    apis.insert("POST /api/v1/pipelines/{pipeline}/run".to_string(), Value::String("Run pipeline".to_string()));
    apis.insert("POST /api/v1/pipelines/{pipeline}/pause".to_string(), Value::String("Pause pipeline".to_string()));

    // LLM / Agent gateway
    apis.insert("GET /api/v1/llm/endpoints".to_string(), Value::String("List LLM endpoints".to_string()));
    apis.insert(
        "GET /api/v1/llm/gateway/dashboard".to_string(),
        Value::String("Get AI gateway usage, routing, cache, budget, and agent summary".to_string()),
    );
    apis.insert(
        "GET /api/v1/llm/gateway_snapshot".to_string(),
        Value::String("Get LLM gateway control-plane snapshot".to_string()),
    );
    apis.insert(
        "POST /api/v1/llm/agent-gateway/connections".to_string(),
        Value::String("Register an agent gateway connection".to_string()),
    );
    apis.insert(
        "GET /api/v1/llm/agent-gateway/connections".to_string(),
        Value::String("List active agent gateway connections".to_string()),
    );
    apis.insert(
        "POST /api/v1/llm/agent-gateway/connections/{session_id}/heartbeat".to_string(),
        Value::String("Heartbeat an agent gateway connection".to_string()),
    );
    apis.insert(
        "POST /api/v1/llm/agent-gateway/connections/{session_id}/usage".to_string(),
        Value::String("Record and rate-limit agent gateway usage".to_string()),
    );
    apis.insert(
        "POST /api/v1/llm/agent-gateway/connections/{session_id}/drain".to_string(),
        Value::String("Mark an agent gateway connection as draining".to_string()),
    );
    apis.insert(
        "DELETE /api/v1/llm/agent-gateway/connections/{session_id}".to_string(),
        Value::String("Disconnect an agent gateway connection".to_string()),
    );
    apis.insert(
        "GET /api/v1/llm/agent-gateway/usage".to_string(),
        Value::String("List agent gateway usage windows by agent fingerprint".to_string()),
    );
    apis.insert(
        "GET /api/v1/llm/agent-gateway/agents/{agent_id}/route".to_string(),
        Value::String("Resolve the active route for an agent".to_string()),
    );
    apis.insert(
        "POST /proxy/v1/chat/completions".to_string(),
        Value::String("OpenAI-compatible LLM gateway proxy; x-eden-agent-id invokes the agent handler".to_string()),
    );
    apis.insert(
        "GET /proxy/v1/models".to_string(),
        Value::String("OpenAI-compatible LLM gateway model list for API keys".to_string()),
    );
    apis.insert("GET /api/v1/llm/credentials".to_string(), Value::String("List LLM credentials".to_string()));
    apis.insert("POST /api/v1/llm/credentials".to_string(), Value::String("Create LLM credential".to_string()));
    apis.insert(
        "PATCH /api/v1/llm/credentials/{credential_id}".to_string(),
        Value::String("Update LLM credential".to_string()),
    );
    apis.insert(
        "DELETE /api/v1/llm/credentials/{credential_id}".to_string(),
        Value::String("Delete LLM credential".to_string()),
    );
    apis.insert(
        "GET /api/v1/llm/marketplace/search".to_string(),
        Value::String("Search skill marketplace".to_string()),
    );

    // User Notifications
    apis.insert(
        "GET /api/v1/notifications".to_string(),
        Value::String("List user notifications (alerts, updates, recommendations)".to_string()),
    );
    apis.insert(
        "POST /api/v1/notifications/{notification_id}/read".to_string(),
        Value::String("Mark notification as read".to_string()),
    );

    // Admin - LLM
    apis.insert("GET /api/v1/admin/llm/system-prompts".to_string(), Value::String("List system prompts".to_string()));
    apis.insert(
        "PUT /api/v1/admin/llm/system-prompts/{prompt_key}".to_string(),
        Value::String("Upsert system prompt".to_string()),
    );
    apis.insert("GET /api/v1/admin/llm/skills".to_string(), Value::String("List admin skills".to_string()));
    apis.insert("POST /api/v1/admin/llm/skills".to_string(), Value::String("Create skill".to_string()));
    apis.insert("PATCH /api/v1/admin/llm/skills/{skill_id}".to_string(), Value::String("Update skill".to_string()));
    apis.insert("DELETE /api/v1/admin/llm/skills/{skill_id}".to_string(), Value::String("Delete skill".to_string()));
    apis.insert(
        "POST /api/v1/admin/llm/marketplace/import".to_string(),
        Value::String("Import marketplace skill".to_string()),
    );

    HttpResponse::Ok().json(json!({ "apis": Value::Object(apis) }))
}

// #[named]
// pub(crate) async fn get_uuid<T, U, I>(
//     database: &EdenDb,
//     org_cache_pointer: Option<OrganizationCached>,
//     input: I,
//     telemetry_wrapper: &mut TelemetryWrapper,
// ) -> ResultEP<U>
// where
//     T: Table + FromRow + Serialize + DeserializeOwned,
//     U: CacheUuid + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
//     I: CacheId + Clone,
// {
//     let span_context = telemetry_wrapper
//         .client_tracer(format!("{}", function_name!()))
//         .await;
//
//
//     Ok(U::new(
//         org_cache_pointer,
//
//         match Uuid::parse_str(input.to_string().as_str()) {
//         Ok(uuid) => {
//             span.add_event(
//                 "successfully parsed `uuid from input`",
//                 vec![FastSpanAttribute::new("uuid", uuid.to_string())],
//             );
//             uuid
//         }
//         Err(_) => {
//             span.add_event(
//                 "failed to parse `uuid` from input",
//                 vec![FastSpanAttribute::new("input", input.to_string())],
//             );
//             <EdenDb as CacheFunctions<T, U, I>>::get_cache_uuid(
//                 database,
//                 &CacheObjectType::new(P::new(org_cache_pointer, input.to_string())),
//                 telemetry_wrapper,
//             )
//             .await?
//         }
//     }))
// }
//
// #[named]
// pub(crate) async fn get_input_uuid<T, U, I>(
//     database: &EdenDb,
//     org_id: &OrganizationId,
//     input: I,
//     telemetry_wrapper: &mut TelemetryWrapper,
// ) -> Result<U, actix_web::error::Error>
// where
//     T: Table + FromRow + Serialize + DeserializeOwned,
//     U: CacheUuid + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
//     I: CacheId + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
// {
//     let span_context = telemetry_wrapper
//         .client_tracer(format!("{}", function_name!()))
//         .await;
//
//
//     let input_uuid = get_uuid::<T, U, I>(
//         database,
//         Some(OrganizationCacheId::new(None, org_id.to_string())),
//         input,
//         telemetry_wrapper,
//     )
//     .await
//     .map_err(|e| {
//         span.set_status(Status::Error {
//             description: Cow::Owned(e.to_string()),
//         });
//         actix_web::error::ErrorBadRequest(e)
//     })?;
//
//     Ok(input_uuid)
// }
//
// #[named]
// pub(crate) async fn get_cache_id<T, U, I>(
//     database: &EdenDb,
//     headers: &HeaderMap,
//     input: I,
//     telemetry_wrapper: &mut TelemetryWrapper,
// ) -> Result<(OrganizationId, OrganizationUuid, U), actix_web::error::Error>
// where
//     T: Table + FromRow + Serialize + DeserializeOwned,
//     U: CacheUuid + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
//     I: CacheId + Clone,
// {
//     let span_context = telemetry_wrapper
//         .client_tracer(format!("{}", function_name!()))
//         .await;
//
//
//     log::debug!("getting org cache object from header");
//     span.add_event("getting org cache object from header", vec![]);
//     let org_cache_object = get_org_from_header(headers).map_err(|e| {
//         span.set_status(Status::Error {
//             description: Cow::Owned(e.to_string()),
//         });
//         actix_web::error::ErrorBadRequest(e)
//     })?;
//
//     log::debug!("getting org uuid from cache");
//     span.add_event(
//         "getting org_uuid from cache",
//         vec![FastSpanAttribute::new(
//             "org_cache_object",
//             org_cache_object.to_string(),
//         )],
//     );
//     let org_uuid = <EdenDb as CacheFunctions<
//         OrganizationSchema,
//         OrganizationCacheUuid,
//         OrganizationCacheId,
//     >>::get_cache_uuid(database, &org_cache_object, telemetry_wrapper)
//     .await
//     .map_err(|e| {
//         span.set_status(Status::Error {
//             description: Cow::Owned(e.to_string()),
//         });
//         actix_web::error::ErrorInternalServerError(e)
//     })?;
//
//     log::debug!("getting org id from cache");
//     span.add_event(
//         "getting org_id from cache",
//         vec![FastSpanAttribute::new(
//             "org_cache_object",
//             org_cache_object.to_string(),
//         )],
//     );
//
//     let org_id = <EdenDb as CacheFunctions<
//         OrganizationSchema,
//         OrganizationCacheUuid,
//         OrganizationCacheId,
//     >>::get_cache_id(database, &org_cache_object, telemetry_wrapper)
//     .await
//     .map_err(|e| {
//         span.set_status(Status::Error {
//             description: Cow::Owned(e.to_string()),
//         });
//         actix_web::error::ErrorInternalServerError(e)
//     })?;
//
//     span.add_event(
//         "getting input_uuid from cache",
//         vec![FastSpanAttribute::new(
//             "org_cache_object",
//             org_cache_object.to_string(),
//         )],
//     );
//     let input_uuid = get_uuid::<T, U, I>(
//         database,
//         Some(OrganizationCacheId::new(None, org_id.clone())),
//         input,
//         telemetry_wrapper,
//     )
//     .await
//     .map_err(|e| {
//         span.set_status(Status::Error {
//             description: Cow::Owned(e.to_string()),
//         });
//         actix_web::error::ErrorBadRequest(e)
//     })?;
//
//     span.add_event(
//         "collected id from cache",
//         vec![
//             FastSpanAttribute::new("org_id", org_id.clone()),
//             FastSpanAttribute::new("org_uuid", org_uuid.to_string()),
//             FastSpanAttribute::new("input_uuid", input_uuid.uuid().to_string()),
//         ],
//     );
//
//     Ok((
//         OrganizationId::new(org_id),
//         OrganizationUuid::new(org_uuid),
//         input_uuid,
//     ))
// }
//
// #[named]
// pub(crate) async fn get_cache_uuid<T, U, I>(
//     database: &EdenDb,
//     auth: &ParsedJwt,
//     input: I,
//     telemetry_wrapper: &mut TelemetryWrapper,
// ) -> Result<U, actix_web::error::Error>
// where
//     T: Table + FromRow + Serialize + DeserializeOwned,
//     U: CacheUuid + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
//     I: CacheId + Clone,
// {
//     let span_context = telemetry_wrapper
//         .client_tracer(format!("{}", function_name!()))
//         .await;
//
//
//     Ok(U::new(match Uuid::parse_str(&input.to_string()) {
//         Ok(uuid) => uuid,
//         Err(_) => <EdenDb as CacheFunctions<T, U, I>>::get_cache_uuid(
//             database,
//             &CacheObjectType::Pointer(P::new(
//                 Some(OrganizationCacheId::new(None, auth.org_id().to_string())),
//                 input.to_string(),
//             )),
//             telemetry_wrapper,
//         )
//         .await
//         .map_err(|e| {
//             span.set_status(Status::Error {
//                 description: Cow::Owned(e.to_string()),
//             });
//             actix_web::error::ErrorInternalServerError(e)
//         })?,
//     }))
// }

pub(crate) fn get_org_from_header(headers: &HeaderMap) -> ResultEP<CacheObjectType<OrganizationCacheUuid, OrganizationCacheId>> {
    Ok(if let Some(org_uuid) = headers.get(ORG_UUID_HEADER) {
        CacheObjectType::new(
            Some(OrganizationCacheUuid::new(
                None,
                OrganizationUuid::new(Uuid::parse_str(org_uuid.to_str().map_err(EpError::parse)?).map_err(EpError::parse)?),
            )),
            None,
        )
    } else if let Some(org_id) = headers.get(ORG_ID_HEADER) {
        CacheObjectType::new(
            None,
            Some(OrganizationCacheId::new(
                None,
                OrganizationId::new(org_id.to_str().map_err(EpError::parse)?.to_string()),
            )),
        )
    } else {
        return Err(EpError::database("missing organization in header"));
    })
}
