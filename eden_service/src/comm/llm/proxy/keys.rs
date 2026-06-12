use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpResponse, Responder, web};
use chrono::{DateTime, Utc};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::{EdenUuid, EndpointUuid, OrganizationUuid};
use eden_core::response::EdenResponse;
use endpoint_core::llm_core::{
    CustomPiiMatcher, CustomPiiTerm, LLM_GATEWAY_KEY_PREFIX, LlmGatewayControlPlaneSnapshot, LlmGatewayCredential, LlmGatewayPolicy,
    LlmKvCacheMode, LlmRouteOptimizationMode, PolicyAction, PriceArbitrageMode,
};
use rand::{TryRngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;
use uuid::Uuid;

use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use database::db::methods::llm::StoredLlmGatewayApiKey;

use super::state::ProxyGatewayState;
use super::{fetch_llm_endpoint_schema, parse_uuid_path};

#[derive(Debug, Clone)]
pub struct ApiKey {
    pub id: Uuid,
    pub org_uuid: Uuid,
    pub name: String,
    pub key_hash: String,
    pub key_prefix: String,
    pub endpoint_uuid: Uuid,
    pub agent_uuid: Uuid,
    pub model_allowlist: Option<Vec<String>>,
    pub rate_limit_rpm: Option<u32>,
    pub budget_tokens_monthly: Option<u64>,
    pub pii_policy: PolicyAction,
    /// User-defined PII dictionary terms with per-term enforcement actions.
    pub custom_pii_terms: Vec<CustomPiiTerm>,
    /// Compiled, reusable matcher for `custom_pii_terms` (derived runtime cache;
    /// built once per key load/update, not persisted). `None` when empty.
    pub pii_matcher: Option<Arc<CustomPiiMatcher>>,
    pub price_arbitrage_mode: PriceArbitrageMode,
    pub response_cache_ttl_secs: Option<u64>,
    pub route_optimization_mode: LlmRouteOptimizationMode,
    pub kv_cache_mode: LlmKvCacheMode,
    pub kv_cache_ttl_secs: Option<u64>,
    pub route_switch_threshold_percent: Option<u8>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

impl ApiKey {
    pub(super) fn gateway_policy(&self) -> LlmGatewayPolicy {
        LlmGatewayPolicy {
            model_allowlist: self.model_allowlist.clone(),
            allowed_tools: None,
            max_prompt_characters: None,
            max_tool_definitions: None,
            request_pii_action: self.pii_policy,
            response_pii_action: self.pii_policy,
            prompt_security_action: PolicyAction::AuditOnly,
            rate_limit_rpm: self.rate_limit_rpm,
            budget_tokens_monthly: self.budget_tokens_monthly,
            price_arbitrage_mode: self.price_arbitrage_mode,
            response_cache_ttl_secs: self.response_cache_ttl_secs,
            route_optimization_mode: self.route_optimization_mode,
            kv_cache_mode: self.kv_cache_mode,
            kv_cache_ttl_secs: self.kv_cache_ttl_secs,
            route_switch_threshold_percent: self.route_switch_threshold_percent,
        }
    }

    pub(super) fn to_stored(&self) -> StoredLlmGatewayApiKey {
        StoredLlmGatewayApiKey {
            id: self.id,
            organization_uuid: OrganizationUuid::from(self.org_uuid),
            name: self.name.clone(),
            key_hash: self.key_hash.clone(),
            key_prefix: self.key_prefix.clone(),
            endpoint_uuid: self.endpoint_uuid,
            agent_uuid: self.agent_uuid,
            model_allowlist: self.model_allowlist.clone(),
            rate_limit_rpm: self.rate_limit_rpm,
            budget_tokens_monthly: self.budget_tokens_monthly,
            pii_policy: self.pii_policy,
            custom_pii_terms: self.custom_pii_terms.clone(),
            price_arbitrage_mode: self.price_arbitrage_mode,
            response_cache_ttl_secs: self.response_cache_ttl_secs,
            route_optimization_mode: self.route_optimization_mode,
            kv_cache_mode: self.kv_cache_mode,
            kv_cache_ttl_secs: self.kv_cache_ttl_secs,
            route_switch_threshold_percent: self.route_switch_threshold_percent,
            enabled: self.enabled,
            created_at: self.created_at,
            updated_at: self.updated_at,
            last_used_at: self.last_used_at,
        }
    }
}

impl From<StoredLlmGatewayApiKey> for ApiKey {
    fn from(value: StoredLlmGatewayApiKey) -> Self {
        let pii_matcher = CustomPiiMatcher::compile(&value.custom_pii_terms);
        Self {
            id: value.id,
            org_uuid: value.organization_uuid.uuid(),
            name: value.name,
            key_hash: value.key_hash,
            key_prefix: value.key_prefix,
            endpoint_uuid: value.endpoint_uuid,
            agent_uuid: value.agent_uuid,
            model_allowlist: value.model_allowlist,
            rate_limit_rpm: value.rate_limit_rpm,
            budget_tokens_monthly: value.budget_tokens_monthly,
            pii_policy: value.pii_policy,
            custom_pii_terms: value.custom_pii_terms,
            pii_matcher,
            price_arbitrage_mode: value.price_arbitrage_mode,
            response_cache_ttl_secs: value.response_cache_ttl_secs,
            route_optimization_mode: value.route_optimization_mode,
            kv_cache_mode: value.kv_cache_mode,
            kv_cache_ttl_secs: value.kv_cache_ttl_secs,
            route_switch_threshold_percent: value.route_switch_threshold_percent,
            enabled: value.enabled,
            created_at: value.created_at,
            updated_at: value.updated_at,
            last_used_at: value.last_used_at,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateApiKeyRequest {
    pub name: String,
    pub endpoint_uuid: Uuid,
    /// Immutable registry agent (`llm_agents.id`) that owns this key (1:1).
    pub agent_uuid: Uuid,
    #[serde(default)]
    pub model_allowlist: Option<Vec<String>>,
    #[serde(default)]
    pub rate_limit_rpm: Option<u32>,
    #[serde(default)]
    pub budget_tokens_monthly: Option<u64>,
    #[serde(default)]
    pub pii_policy: Option<PolicyAction>,
    /// Custom PII dictionary (replaces the existing set when provided).
    #[serde(default)]
    pub custom_pii_terms: Option<Vec<CustomPiiTerm>>,
    #[serde(default)]
    pub price_arbitrage_mode: Option<PriceArbitrageMode>,
    #[serde(default)]
    pub response_cache_ttl_secs: Option<u64>,
    #[serde(default)]
    pub route_optimization_mode: Option<LlmRouteOptimizationMode>,
    #[serde(default)]
    pub kv_cache_mode: Option<LlmKvCacheMode>,
    #[serde(default)]
    pub kv_cache_ttl_secs: Option<u64>,
    #[serde(default)]
    pub route_switch_threshold_percent: Option<u8>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateApiKeyRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub endpoint_uuid: Option<Uuid>,
    #[serde(default)]
    pub model_allowlist: Option<Vec<String>>,
    #[serde(default)]
    pub rate_limit_rpm: Option<u32>,
    #[serde(default)]
    pub budget_tokens_monthly: Option<u64>,
    #[serde(default)]
    pub pii_policy: Option<PolicyAction>,
    /// Custom PII dictionary (replaces the existing set when provided).
    #[serde(default)]
    pub custom_pii_terms: Option<Vec<CustomPiiTerm>>,
    #[serde(default)]
    pub price_arbitrage_mode: Option<PriceArbitrageMode>,
    #[serde(default)]
    pub response_cache_ttl_secs: Option<u64>,
    #[serde(default)]
    pub route_optimization_mode: Option<LlmRouteOptimizationMode>,
    #[serde(default)]
    pub kv_cache_mode: Option<LlmKvCacheMode>,
    #[serde(default)]
    pub kv_cache_ttl_secs: Option<u64>,
    #[serde(default)]
    pub route_switch_threshold_percent: Option<u8>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ApiKeyResponse {
    pub id: String,
    pub org_uuid: String,
    pub name: String,
    pub key_prefix: String,
    pub endpoint_uuid: String,
    /// Immutable owning agent (`llm_agents.id`) of this key (1:1).
    pub agent_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_allowlist: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_rpm: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_tokens_monthly: Option<u64>,
    pub pii_policy: PolicyAction,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_pii_terms: Vec<CustomPiiTerm>,
    pub price_arbitrage_mode: PriceArbitrageMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_cache_ttl_secs: Option<u64>,
    pub route_optimization_mode: LlmRouteOptimizationMode,
    pub kv_cache_mode: LlmKvCacheMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_cache_ttl_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_switch_threshold_percent: Option<u8>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct CreateApiKeyResponse {
    pub api_key: ApiKeyResponse,
    /// One-time plaintext secret, returned only at creation.
    pub secret: String,
}

#[derive(Debug, Clone)]
pub(super) struct ApiKeyUpdate {
    pub name: Option<String>,
    pub endpoint_uuid: Option<Uuid>,
    pub model_allowlist: Option<Option<Vec<String>>>,
    pub rate_limit_rpm: Option<Option<u32>>,
    pub budget_tokens_monthly: Option<Option<u64>>,
    pub pii_policy: Option<PolicyAction>,
    pub custom_pii_terms: Option<Vec<CustomPiiTerm>>,
    pub price_arbitrage_mode: Option<PriceArbitrageMode>,
    pub response_cache_ttl_secs: Option<Option<u64>>,
    pub route_optimization_mode: Option<LlmRouteOptimizationMode>,
    pub kv_cache_mode: Option<LlmKvCacheMode>,
    pub kv_cache_ttl_secs: Option<Option<u64>>,
    pub route_switch_threshold_percent: Option<Option<u8>>,
    pub enabled: Option<bool>,
}

impl ProxyGatewayState {
    pub(super) fn create_key(
        &self,
        org_uuid: Uuid,
        name: String,
        endpoint_uuid: Uuid,
        agent_uuid: Uuid,
        model_allowlist: Option<Vec<String>>,
        rate_limit_rpm: Option<u32>,
        budget_tokens_monthly: Option<u64>,
        pii_policy: PolicyAction,
        custom_pii_terms: Vec<CustomPiiTerm>,
        price_arbitrage_mode: PriceArbitrageMode,
        response_cache_ttl_secs: Option<u64>,
        route_optimization_mode: LlmRouteOptimizationMode,
        kv_cache_mode: LlmKvCacheMode,
        kv_cache_ttl_secs: Option<u64>,
        route_switch_threshold_percent: Option<u8>,
        enabled: bool,
    ) -> Result<(ApiKey, String), EpError> {
        let now = Utc::now();

        for _ in 0..8 {
            let (plaintext_key, key_prefix) = generate_api_key_material()?;
            let key_hash = hash_api_key(&plaintext_key);

            if self.ids_by_hash.contains_key(&key_hash) {
                continue;
            }

            let api_key = ApiKey {
                id: Uuid::new_v4(),
                org_uuid,
                name: name.clone(),
                key_hash: key_hash.clone(),
                key_prefix,
                endpoint_uuid,
                agent_uuid,
                model_allowlist: model_allowlist.clone(),
                rate_limit_rpm,
                budget_tokens_monthly,
                pii_policy,
                pii_matcher: CustomPiiMatcher::compile(&custom_pii_terms),
                custom_pii_terms: custom_pii_terms.clone(),
                price_arbitrage_mode,
                response_cache_ttl_secs,
                route_optimization_mode,
                kv_cache_mode,
                kv_cache_ttl_secs,
                route_switch_threshold_percent,
                enabled,
                created_at: now,
                updated_at: now,
                last_used_at: None,
            };

            self.ids_by_hash.insert(key_hash, api_key.id);
            self.keys_by_id.insert(api_key.id, api_key.clone());
            return Ok((api_key, plaintext_key));
        }

        Err(EpError::auth("failed to generate a unique API key"))
    }

    pub(super) fn list_keys(&self, org_uuid: Uuid) -> Vec<ApiKey> {
        let mut keys = self
            .keys_by_id
            .iter()
            .filter(|entry| entry.value().org_uuid == org_uuid)
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();

        keys.sort_by(|a, b| b.created_at.cmp(&a.created_at).then_with(|| a.name.cmp(&b.name)));
        keys
    }

    /// True if this org already has an api key bound to `agent_uuid`. The DB
    /// enforces 1:1 via a UNIQUE constraint; this lets the create handler return
    /// a clean message instead of surfacing a raw constraint violation.
    pub(super) fn agent_has_key(&self, org_uuid: Uuid, agent_uuid: Uuid) -> bool {
        self.keys_by_id.iter().any(|entry| entry.value().org_uuid == org_uuid && entry.value().agent_uuid == agent_uuid)
    }

    pub(super) fn get_key(&self, org_uuid: Uuid, key_id: Uuid) -> Option<ApiKey> {
        let entry = self.keys_by_id.get(&key_id)?;
        if entry.org_uuid != org_uuid {
            return None;
        }
        Some(entry.clone())
    }

    pub(super) fn upsert_key(&self, api_key: ApiKey) -> Result<(), EpError> {
        if let Some(existing_id) = self.ids_by_hash.get(&api_key.key_hash)
            && *existing_id != api_key.id
        {
            return Err(EpError::request("API key hash already exists"));
        }

        if let Some(old_hash) = self.keys_by_id.get(&api_key.id).map(|entry| entry.key_hash.clone())
            && old_hash != api_key.key_hash
        {
            self.ids_by_hash.remove(&old_hash);
        }

        self.ids_by_hash.insert(api_key.key_hash.clone(), api_key.id);
        self.keys_by_id.insert(api_key.id, api_key);
        Ok(())
    }

    pub(super) fn hydrate_keys(&self, keys: impl IntoIterator<Item = ApiKey>) -> Result<usize, EpError> {
        let mut count = 0;
        for key in keys {
            self.upsert_key(key)?;
            count += 1;
        }
        Ok(count)
    }

    pub(super) fn delete_key(&self, org_uuid: Uuid, key_id: Uuid) -> bool {
        let Some((_, removed)) = self.keys_by_id.remove(&key_id) else {
            return false;
        };

        if removed.org_uuid != org_uuid {
            self.keys_by_id.insert(key_id, removed);
            return false;
        }

        self.ids_by_hash.remove(&removed.key_hash);
        self.rate_limits.remove(&key_id);
        self.budget_usage.remove(&key_id);
        true
    }

    pub(super) fn updated_key(&self, org_uuid: Uuid, key_id: Uuid, update: ApiKeyUpdate) -> Result<Option<ApiKey>, EpError> {
        let Some(mut api_key) = self.get_key(org_uuid, key_id) else {
            return Ok(None);
        };

        apply_api_key_update(&mut api_key, update);
        Ok(Some(api_key))
    }
}

fn apply_api_key_update(api_key: &mut ApiKey, update: ApiKeyUpdate) {
    if let Some(name) = update.name {
        api_key.name = name;
    }
    if let Some(endpoint_uuid) = update.endpoint_uuid {
        api_key.endpoint_uuid = endpoint_uuid;
    }
    if let Some(model_allowlist) = update.model_allowlist {
        api_key.model_allowlist = model_allowlist;
    }
    if let Some(rate_limit_rpm) = update.rate_limit_rpm {
        api_key.rate_limit_rpm = rate_limit_rpm;
    }
    if let Some(budget_tokens_monthly) = update.budget_tokens_monthly {
        api_key.budget_tokens_monthly = budget_tokens_monthly;
    }
    if let Some(pii_policy) = update.pii_policy {
        api_key.pii_policy = pii_policy;
    }
    if let Some(custom_pii_terms) = update.custom_pii_terms {
        api_key.custom_pii_terms = custom_pii_terms;
    }
    if let Some(price_arbitrage_mode) = update.price_arbitrage_mode {
        api_key.price_arbitrage_mode = price_arbitrage_mode;
    }
    if let Some(response_cache_ttl_secs) = update.response_cache_ttl_secs {
        api_key.response_cache_ttl_secs = response_cache_ttl_secs;
    }
    if let Some(route_optimization_mode) = update.route_optimization_mode {
        api_key.route_optimization_mode = route_optimization_mode;
    }
    if let Some(kv_cache_mode) = update.kv_cache_mode {
        api_key.kv_cache_mode = kv_cache_mode;
    }
    if let Some(kv_cache_ttl_secs) = update.kv_cache_ttl_secs {
        api_key.kv_cache_ttl_secs = kv_cache_ttl_secs;
    }
    if let Some(route_switch_threshold_percent) = update.route_switch_threshold_percent {
        api_key.route_switch_threshold_percent = route_switch_threshold_percent;
    }
    if let Some(enabled) = update.enabled {
        api_key.enabled = enabled;
    }
    api_key.updated_at = Utc::now();
}

#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["LLM"],
    path = "/llm/api_keys",
    operation_id = "llm_list_api_keys",
    responses((status = 200, body = EdenResponse<Vec<ApiKeyResponse>>))
)]
pub async fn list_api_keys(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    proxy_state: web::Data<ProxyGatewayState>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, eden_core::format::rbac::ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let keys = proxy_state.list_keys(auth.org_uuid().uuid()).into_iter().map(ApiKeyResponse::from).collect::<Vec<_>>();

    let response: Result<HttpResponse, actix_web::Error> = EdenResponse::response(keys).into();
    response
}

#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["LLM"],
    path = "/llm/gateway_snapshot",
    operation_id = "llm_gateway_control_plane_snapshot",
    responses((status = 200, body = EdenResponse<LlmGatewayControlPlaneSnapshot>))
)]
pub async fn gateway_control_plane_snapshot(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    proxy_state: web::Data<ProxyGatewayState>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, eden_core::format::rbac::ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let snapshot = proxy_state.control_plane_snapshot(auth.org_uuid().uuid());
    EdenResponse::response(snapshot).into()
}

#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["LLM"],
    path = "/llm/api_keys",
    operation_id = "llm_create_api_key",
    request_body = CreateApiKeyRequest,
    responses((status = 201, body = EdenResponse<CreateApiKeyResponse>))
)]
pub async fn create_api_key(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    proxy_state: web::Data<ProxyGatewayState>,
    payload: web::Json<CreateApiKeyRequest>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, eden_core::format::rbac::ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let request = payload.into_inner();
    let name = normalize_required_name(&request.name).map_err(|e| error_handling(e, &mut span))?;

    let endpoint_schema =
        fetch_llm_endpoint_schema(&database, auth.org_uuid(), EndpointUuid::from(request.endpoint_uuid), telemetry_wrapper)
            .await
            .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(
        &database,
        &auth,
        Some(endpoint_schema.endpoint_uuid()),
        eden_core::format::rbac::ControlPerms::CONFIGURE,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    // The owning agent must exist in this org (api keys are 1:1 with agents).
    // Validate up front so the caller gets a clear message rather than a raw
    // foreign-key / unique-constraint violation from the insert.
    let agent = database
        .load_agent(request.agent_uuid, telemetry_wrapper)
        .await
        .map_err(|_| error_handling(EpError::parse(format!("owning agent {} does not exist", request.agent_uuid)), &mut span))?;
    if agent.organization_uuid != auth.org_uuid().uuid() {
        return Err(error_handling(
            EpError::parse(format!("owning agent {} does not exist", request.agent_uuid)),
            &mut span,
        ));
    }
    if proxy_state.agent_has_key(auth.org_uuid().uuid(), request.agent_uuid) {
        return Err(error_handling(
            EpError::parse(format!("agent {} already has an api key (one key per agent)", request.agent_uuid)),
            &mut span,
        ));
    }

    let (api_key, plaintext_key) = proxy_state
        .create_key(
            auth.org_uuid().uuid(),
            name,
            request.endpoint_uuid,
            request.agent_uuid,
            normalize_allowlist(request.model_allowlist),
            normalize_optional_limit_u32(request.rate_limit_rpm),
            normalize_optional_limit_u64(request.budget_tokens_monthly),
            // Default new agents to redact so PII is enforced out of the box;
            // operators can relax this per agent via the update endpoint.
            request.pii_policy.unwrap_or(PolicyAction::Redact),
            normalize_custom_pii_terms(request.custom_pii_terms.unwrap_or_default()),
            request.price_arbitrage_mode.unwrap_or_default(),
            normalize_optional_limit_u64(request.response_cache_ttl_secs),
            request.route_optimization_mode.unwrap_or_default(),
            request.kv_cache_mode.unwrap_or_default(),
            normalize_optional_limit_u64(request.kv_cache_ttl_secs),
            normalize_route_switch_threshold(request.route_switch_threshold_percent),
            request.enabled,
        )
        .map_err(|e| error_handling(e, &mut span))?;

    let stored_api_key = match database.upsert_llm_gateway_api_key(&api_key.to_stored(), telemetry_wrapper).await {
        Ok(stored_api_key) => stored_api_key,
        Err(error) => {
            proxy_state.delete_key(auth.org_uuid().uuid(), api_key.id);
            return Err(error_handling(error, &mut span));
        }
    };
    let api_key = ApiKey::from(stored_api_key);
    proxy_state.upsert_key(api_key.clone()).map_err(|e| error_handling(e, &mut span))?;

    let response = CreateApiKeyResponse {
        api_key: ApiKeyResponse::from(api_key),
        secret: plaintext_key,
    };

    Ok::<HttpResponse, actix_web::Error>(HttpResponse::Created().json(EdenResponse::response(response)))
}

#[with_telemetry]
#[utoipa::path(
    patch,
    tags = ["LLM"],
    path = "/llm/api_keys/{key_id}",
    operation_id = "llm_update_api_key",
    params(("key_id" = String, Path, description = "API key identifier")),
    request_body = UpdateApiKeyRequest,
    responses((status = 200, body = EdenResponse<ApiKeyResponse>))
)]
pub async fn update_api_key(
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<String>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    proxy_state: web::Data<ProxyGatewayState>,
    payload: web::Json<UpdateApiKeyRequest>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, eden_core::format::rbac::ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let key_id = parse_uuid_path(path.as_str(), "key_id").map_err(|e| error_handling(e, &mut span))?;
    let request = payload.into_inner();

    if let Some(endpoint_uuid) = request.endpoint_uuid {
        let endpoint_schema = fetch_llm_endpoint_schema(&database, auth.org_uuid(), EndpointUuid::from(endpoint_uuid), telemetry_wrapper)
            .await
            .map_err(|e| error_handling(e, &mut span))?;

        verify_control_perms(
            &database,
            &auth,
            Some(endpoint_schema.endpoint_uuid()),
            eden_core::format::rbac::ControlPerms::CONFIGURE,
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    }

    let update = ApiKeyUpdate {
        name: request.name.as_deref().map(normalize_required_name).transpose().map_err(|e| error_handling(e, &mut span))?,
        endpoint_uuid: request.endpoint_uuid,
        model_allowlist: request.model_allowlist.map(|allowlist| normalize_allowlist(Some(allowlist))),
        rate_limit_rpm: request.rate_limit_rpm.map(|value| normalize_optional_limit_u32(Some(value))),
        budget_tokens_monthly: request.budget_tokens_monthly.map(|value| normalize_optional_limit_u64(Some(value))),
        pii_policy: request.pii_policy,
        custom_pii_terms: request.custom_pii_terms.map(normalize_custom_pii_terms),
        price_arbitrage_mode: request.price_arbitrage_mode,
        response_cache_ttl_secs: request.response_cache_ttl_secs.map(|value| normalize_optional_limit_u64(Some(value))),
        route_optimization_mode: request.route_optimization_mode,
        kv_cache_mode: request.kv_cache_mode,
        kv_cache_ttl_secs: request.kv_cache_ttl_secs.map(|value| normalize_optional_limit_u64(Some(value))),
        route_switch_threshold_percent: request.route_switch_threshold_percent.map(|value| normalize_route_switch_threshold(Some(value))),
        enabled: request.enabled,
    };

    let updated = proxy_state.updated_key(auth.org_uuid().uuid(), key_id, update).map_err(|e| error_handling(e, &mut span))?;

    let Some(updated) = updated else {
        return Err(error_handling(EpError::request("API key not found"), &mut span));
    };

    let stored_updated = database
        .upsert_llm_gateway_api_key(&updated.to_stored(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    let updated = ApiKey::from(stored_updated);
    proxy_state.upsert_key(updated.clone()).map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(ApiKeyResponse::from(updated)).into()
}

#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["LLM"],
    path = "/llm/api_keys/{key_id}",
    operation_id = "llm_delete_api_key",
    params(("key_id" = String, Path, description = "API key identifier")),
    responses((status = 204))
)]
pub async fn delete_api_key(
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<String>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    proxy_state: web::Data<ProxyGatewayState>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, eden_core::format::rbac::ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let key_id = parse_uuid_path(path.as_str(), "key_id").map_err(|e| error_handling(e, &mut span))?;
    if proxy_state.get_key(auth.org_uuid().uuid(), key_id).is_none() {
        return Err(error_handling(EpError::request("API key not found"), &mut span));
    }
    let deleted = database
        .delete_llm_gateway_api_key(auth.org_uuid(), key_id, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    if !deleted {
        return Err(error_handling(EpError::request("API key not found"), &mut span));
    }
    proxy_state.delete_key(auth.org_uuid().uuid(), key_id);

    Ok::<HttpResponse, actix_web::Error>(HttpResponse::NoContent().finish())
}

#[derive(Debug, Serialize, ToSchema, Default, PartialEq, Eq)]
pub struct OrgPiiDictionaryResponse {
    #[serde(default)]
    pub terms: Vec<CustomPiiTerm>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateOrgPiiDictionaryRequest {
    #[serde(default)]
    pub terms: Vec<CustomPiiTerm>,
}

#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["LLM"],
    path = "/llm/pii_dictionary",
    operation_id = "llm_get_org_pii_dictionary",
    responses((status = 200, body = EdenResponse<OrgPiiDictionaryResponse>))
)]
pub async fn get_org_pii_dictionary(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, eden_core::format::rbac::ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let stored = database.load_org_pii_dictionary(auth.org_uuid(), telemetry_wrapper).await.map_err(|e| error_handling(e, &mut span))?;
    let response = stored.map(|d| OrgPiiDictionaryResponse { terms: d.terms, updated_at: Some(d.updated_at) }).unwrap_or_default();
    EdenResponse::response(response).into()
}

#[with_telemetry]
#[utoipa::path(
    put,
    tags = ["LLM"],
    path = "/llm/pii_dictionary",
    operation_id = "llm_update_org_pii_dictionary",
    request_body = UpdateOrgPiiDictionaryRequest,
    responses((status = 200, body = EdenResponse<OrgPiiDictionaryResponse>))
)]
pub async fn update_org_pii_dictionary(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    proxy_state: web::Data<ProxyGatewayState>,
    payload: web::Json<UpdateOrgPiiDictionaryRequest>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, eden_core::format::rbac::ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let terms = normalize_custom_pii_terms(payload.into_inner().terms);
    let stored = database
        .upsert_org_pii_dictionary(auth.org_uuid(), &terms, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    // Refresh the in-memory compiled matcher so enforcement picks it up immediately.
    proxy_state.set_org_pii_dictionary(auth.org_uuid().uuid(), &stored.terms);

    EdenResponse::response(OrgPiiDictionaryResponse { terms: stored.terms, updated_at: Some(stored.updated_at) }).into()
}

impl From<ApiKey> for ApiKeyResponse {
    fn from(value: ApiKey) -> Self {
        Self {
            id: value.id.to_string(),
            org_uuid: value.org_uuid.to_string(),
            name: value.name,
            key_prefix: value.key_prefix,
            endpoint_uuid: value.endpoint_uuid.to_string(),
            agent_uuid: value.agent_uuid.to_string(),
            model_allowlist: value.model_allowlist,
            rate_limit_rpm: value.rate_limit_rpm,
            budget_tokens_monthly: value.budget_tokens_monthly,
            pii_policy: value.pii_policy,
            custom_pii_terms: value.custom_pii_terms,
            price_arbitrage_mode: value.price_arbitrage_mode,
            response_cache_ttl_secs: value.response_cache_ttl_secs,
            route_optimization_mode: value.route_optimization_mode,
            kv_cache_mode: value.kv_cache_mode,
            kv_cache_ttl_secs: value.kv_cache_ttl_secs,
            route_switch_threshold_percent: value.route_switch_threshold_percent,
            enabled: value.enabled,
            created_at: value.created_at,
            updated_at: value.updated_at,
            last_used_at: value.last_used_at,
        }
    }
}

fn default_true() -> bool {
    true
}

/// Trim and de-duplicate dictionary terms, dropping blanks. Later entries win
/// on a case-insensitive term collision so an explicit action overrides a
/// duplicate.
fn normalize_custom_pii_terms(terms: Vec<CustomPiiTerm>) -> Vec<CustomPiiTerm> {
    let mut normalized: Vec<CustomPiiTerm> = Vec::new();
    for mut term in terms {
        term.term = term.term.trim().to_string();
        if term.term.is_empty() {
            continue;
        }
        term.label = term.label.map(|label| label.trim().to_string()).filter(|label| !label.is_empty());
        let key = term.term.to_ascii_lowercase();
        if let Some(existing) = normalized.iter_mut().find(|entry| entry.term.to_ascii_lowercase() == key) {
            *existing = term;
        } else {
            normalized.push(term);
        }
    }
    normalized
}

fn normalize_required_name(value: &str) -> Result<String, EpError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(EpError::request("name must not be empty"));
    }

    Ok(trimmed.to_string())
}

fn normalize_allowlist(values: Option<Vec<String>>) -> Option<Vec<String>> {
    let values = values?;

    let mut normalized = values.into_iter().map(|value| value.trim().to_string()).filter(|value| !value.is_empty()).collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();

    if normalized.is_empty() { None } else { Some(normalized) }
}

fn normalize_optional_limit_u32(value: Option<u32>) -> Option<u32> {
    value.filter(|value| *value > 0)
}

fn normalize_optional_limit_u64(value: Option<u64>) -> Option<u64> {
    value.filter(|value| *value > 0)
}

fn normalize_route_switch_threshold(value: Option<u8>) -> Option<u8> {
    value.filter(|value| *value > 0).map(|value| value.min(100))
}

fn generate_api_key_material() -> Result<(String, String), EpError> {
    let mut random_bytes = [0_u8; 24];
    OsRng.try_fill_bytes(&mut random_bytes).map_err(|e| EpError::auth(format!("failed to generate API key: {e}")))?;

    let suffix = hex::encode(random_bytes);
    let key_prefix = suffix.chars().take(8).collect::<String>();
    Ok((format!("{LLM_GATEWAY_KEY_PREFIX}{suffix}"), key_prefix))
}

pub(super) fn hash_api_key(plaintext_key: &str) -> String {
    LlmGatewayCredential::hash_api_key(plaintext_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use endpoint_core::llm_core::LlmGatewayControlPlaneAuthMode;

    #[test]
    fn api_key_generation_matches_expected_format() {
        let (key, prefix) = generate_api_key_material().expect("API key generation should succeed");
        assert!(key.starts_with(LLM_GATEWAY_KEY_PREFIX));
        assert_eq!(prefix.len(), 8);
        assert_eq!(hash_api_key(&key).len(), 64);
    }

    #[test]
    fn agent_has_key_enforces_one_to_one() {
        let org = Uuid::new_v4();
        let agent = Uuid::new_v4();
        let state = ProxyGatewayState::new();
        assert!(!state.agent_has_key(org, agent), "no key bound yet");

        state
            .create_key(
                org,
                "agent-key".to_string(),
                Uuid::new_v4(),
                agent,
                None,
                None,
                None,
                PolicyAction::AuditOnly,
                Vec::new(),
                PriceArbitrageMode::Disabled,
                None,
                LlmRouteOptimizationMode::Cost,
                LlmKvCacheMode::Disabled,
                None,
                None,
                true,
            )
            .expect("key should be created");

        assert!(state.agent_has_key(org, agent), "agent is now keyed (1:1)");
        assert!(!state.agent_has_key(org, Uuid::new_v4()), "a different agent is unaffected");
        assert!(!state.agent_has_key(Uuid::new_v4(), agent), "a different org is unaffected");
    }

    #[test]
    fn org_pii_dictionary_compiles_caches_and_clears() {
        let state = ProxyGatewayState::new();
        let org = Uuid::new_v4();
        assert!(state.org_pii_matcher(org).is_none(), "no org dictionary yet");

        state.set_org_pii_dictionary(
            org,
            &[CustomPiiTerm {
                term: "Project Titan".to_string(),
                action: PolicyAction::Block,
                label: None,
            }],
        );
        let matcher = state.org_pii_matcher(org).expect("compiled org matcher is cached");
        // The cached matcher actually detects the org term.
        let scanner = endpoint_core::llm_core::LlmPiiScanner::with_compiled_dictionary(Some(matcher));
        assert!(scanner.scan_text("notes about project titan").detected);

        // An empty dictionary clears the cache.
        state.set_org_pii_dictionary(org, &[]);
        assert!(state.org_pii_matcher(org).is_none(), "empty dictionary removes the cached matcher");
    }

    #[test]
    fn normalize_custom_pii_terms_trims_dedupes_and_drops_blanks() {
        let terms = normalize_custom_pii_terms(vec![
            CustomPiiTerm {
                term: "  Project Titan  ".to_string(),
                action: PolicyAction::Redact,
                label: Some("  ".to_string()),
            },
            CustomPiiTerm {
                term: "".to_string(),
                action: PolicyAction::Block,
                label: None,
            },
            CustomPiiTerm {
                term: "project titan".to_string(),
                action: PolicyAction::Block,
                label: Some("project".to_string()),
            },
        ]);

        // Blank dropped; case-insensitive duplicate collapses to the last entry.
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].term, "project titan");
        assert_eq!(terms[0].action, PolicyAction::Block);
        // A blank label is normalized away.
        assert_eq!(terms[0].label.as_deref(), Some("project"));
        assert!(normalize_custom_pii_terms(Vec::new()).is_empty());
    }

    #[test]
    fn api_key_materializes_shared_gateway_policy() {
        let now = Utc::now();
        let key = ApiKey {
            id: Uuid::new_v4(),
            org_uuid: Uuid::new_v4(),
            name: "test".to_string(),
            key_hash: "hash".to_string(),
            key_prefix: "prefix".to_string(),
            endpoint_uuid: Uuid::new_v4(),
            agent_uuid: Uuid::new_v4(),
            model_allowlist: Some(vec!["gpt-allowed".to_string()]),
            rate_limit_rpm: Some(60),
            budget_tokens_monthly: Some(1_000_000),
            pii_policy: PolicyAction::Redact,
            custom_pii_terms: Vec::new(),
            pii_matcher: None,
            price_arbitrage_mode: PriceArbitrageMode::AllowedModelsCheapest,
            response_cache_ttl_secs: Some(300),
            route_optimization_mode: LlmRouteOptimizationMode::Balanced,
            kv_cache_mode: LlmKvCacheMode::Adaptive,
            kv_cache_ttl_secs: Some(1_800),
            route_switch_threshold_percent: Some(20),
            enabled: true,
            created_at: now,
            updated_at: now,
            last_used_at: None,
        };

        let policy = key.gateway_policy();

        assert_eq!(policy.model_allowlist.as_deref(), Some(&["gpt-allowed".to_string()][..]));
        assert_eq!(policy.request_pii_action, PolicyAction::Redact);
        assert_eq!(policy.response_pii_action, PolicyAction::Redact);
        assert_eq!(policy.rate_limit_rpm, Some(60));
        assert_eq!(policy.price_arbitrage_mode, PriceArbitrageMode::AllowedModelsCheapest);
        assert_eq!(policy.kv_cache_mode, LlmKvCacheMode::Adaptive);
    }

    #[test]
    fn api_key_round_trips_through_stored_gateway_shape() {
        let now = Utc::now();
        let key = ApiKey {
            id: Uuid::new_v4(),
            org_uuid: Uuid::new_v4(),
            name: "persisted".to_string(),
            key_hash: hash_api_key("eden-gateway-test-key"),
            key_prefix: "test-key".to_string(),
            endpoint_uuid: Uuid::new_v4(),
            agent_uuid: Uuid::new_v4(),
            model_allowlist: Some(vec!["gpt-4.1".to_string(), "gpt-4.1-mini".to_string()]),
            rate_limit_rpm: Some(90),
            budget_tokens_monthly: Some(2_000_000),
            pii_policy: PolicyAction::Block,
            custom_pii_terms: vec![CustomPiiTerm {
                term: "Project Titan".to_string(),
                action: PolicyAction::Block,
                label: Some("project".to_string()),
            }],
            pii_matcher: None,
            price_arbitrage_mode: PriceArbitrageMode::SameModelCheapest,
            response_cache_ttl_secs: Some(120),
            route_optimization_mode: LlmRouteOptimizationMode::Throughput,
            kv_cache_mode: LlmKvCacheMode::Affinity,
            kv_cache_ttl_secs: Some(900),
            route_switch_threshold_percent: Some(15),
            enabled: true,
            created_at: now,
            updated_at: now,
            last_used_at: Some(now),
        };

        let stored = key.to_stored();
        let restored = ApiKey::from(stored);

        assert_eq!(restored.id, key.id);
        assert_eq!(restored.org_uuid, key.org_uuid);
        assert_eq!(restored.agent_uuid, key.agent_uuid, "owning agent_uuid survives the stored round-trip");
        assert_eq!(restored.key_hash, key.key_hash);
        assert_eq!(restored.model_allowlist, key.model_allowlist);
        assert_eq!(
            restored.custom_pii_terms, key.custom_pii_terms,
            "custom PII dictionary survives the stored round-trip"
        );
        assert_eq!(restored.custom_pii_terms[0].action, PolicyAction::Block);
        assert_eq!(restored.price_arbitrage_mode, PriceArbitrageMode::SameModelCheapest);
        assert_eq!(restored.route_optimization_mode, LlmRouteOptimizationMode::Throughput);
        assert_eq!(restored.kv_cache_ttl_secs, Some(900));
    }

    #[test]
    fn proxy_state_hydrates_persisted_keys_for_auth_lookup() {
        let plaintext = "eden-gateway-hydrated-key";
        let now = Utc::now();
        let key = ApiKey {
            id: Uuid::new_v4(),
            org_uuid: Uuid::new_v4(),
            name: "hydrated".to_string(),
            key_hash: hash_api_key(plaintext),
            key_prefix: "hydrated".to_string(),
            endpoint_uuid: Uuid::new_v4(),
            agent_uuid: Uuid::new_v4(),
            model_allowlist: None,
            rate_limit_rpm: None,
            budget_tokens_monthly: None,
            pii_policy: PolicyAction::AuditOnly,
            custom_pii_terms: Vec::new(),
            pii_matcher: None,
            price_arbitrage_mode: PriceArbitrageMode::Disabled,
            response_cache_ttl_secs: None,
            route_optimization_mode: LlmRouteOptimizationMode::Cost,
            kv_cache_mode: LlmKvCacheMode::Disabled,
            kv_cache_ttl_secs: None,
            route_switch_threshold_percent: None,
            enabled: true,
            created_at: now,
            updated_at: now,
            last_used_at: None,
        };
        let state = ProxyGatewayState::new();

        let count = state.hydrate_keys(vec![key.clone()]).expect("hydration should succeed");
        let resolved = state.resolve_plaintext_key(plaintext).expect("hydrated key should resolve");

        assert_eq!(count, 1);
        assert_eq!(resolved.id, key.id);
        assert_eq!(resolved.org_uuid, key.org_uuid);
    }

    #[test]
    fn proxy_state_exports_gateway_control_plane_snapshot() {
        let org_uuid = Uuid::new_v4();
        let endpoint_uuid = Uuid::new_v4();
        let state = ProxyGatewayState::new();
        let (key, _) = state
            .create_key(
                org_uuid,
                "gateway".to_string(),
                endpoint_uuid,
                Uuid::new_v4(),
                Some(vec!["gpt-4.1".to_string()]),
                Some(120),
                Some(10_000),
                PolicyAction::AuditOnly,
                Vec::new(),
                PriceArbitrageMode::AllowedModelsCheapest,
                Some(300),
                LlmRouteOptimizationMode::Latency,
                LlmKvCacheMode::Affinity,
                Some(600),
                Some(10),
                true,
            )
            .expect("key should be created");

        let snapshot = state.control_plane_snapshot(org_uuid);

        assert_eq!(snapshot.auth_mode, LlmGatewayControlPlaneAuthMode::Enforce);
        assert_eq!(snapshot.key_policies.len(), 1);
        assert_eq!(snapshot.key_policies[0].key_hash, key.key_hash);
        assert_eq!(snapshot.key_policies[0].policy.model_allowlist.as_deref(), Some(&["gpt-4.1".to_string()][..]));
        assert!(snapshot.model_catalog.is_some());
    }

    #[test]
    fn proxy_state_exports_all_orgs_gateway_control_plane_snapshot_for_deployment_handoff() {
        let org_a = Uuid::new_v4();
        let org_b = Uuid::new_v4();
        let state = ProxyGatewayState::new();
        state
            .create_key(
                org_a,
                "gateway-a".to_string(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                None,
                None,
                None,
                PolicyAction::AuditOnly,
                Vec::new(),
                PriceArbitrageMode::Disabled,
                None,
                LlmRouteOptimizationMode::Cost,
                LlmKvCacheMode::Disabled,
                None,
                None,
                true,
            )
            .expect("first key should be created");
        state
            .create_key(
                org_b,
                "gateway-b".to_string(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                None,
                None,
                None,
                PolicyAction::AuditOnly,
                Vec::new(),
                PriceArbitrageMode::Disabled,
                None,
                LlmRouteOptimizationMode::Cost,
                LlmKvCacheMode::Disabled,
                None,
                None,
                true,
            )
            .expect("second key should be created");

        let org_a_snapshot = state.control_plane_snapshot(org_a);
        let all_org_snapshot = state.control_plane_snapshot_all_orgs();
        let org_a_string = org_a.to_string();
        let org_b_string = org_b.to_string();
        let exported_orgs = all_org_snapshot
            .key_policies
            .iter()
            .filter_map(|policy| policy.org_uuid.as_deref())
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(org_a_snapshot.key_policies.len(), 1);
        assert_eq!(all_org_snapshot.auth_mode, LlmGatewayControlPlaneAuthMode::Enforce);
        assert_eq!(all_org_snapshot.key_policies.len(), 2);
        assert!(exported_orgs.contains(org_a_string.as_str()));
        assert!(exported_orgs.contains(org_b_string.as_str()));
    }
}
