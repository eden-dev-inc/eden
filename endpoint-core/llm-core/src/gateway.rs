//! Shared LLM/agent gateway control-plane and data-plane primitives.
//!
//! `eden_service` owns management APIs and persistence for these shapes, while
//! `eden_gateway` owns hot-path enforcement. Keeping the contract here prevents
//! either side from growing a private copy of key, policy, identity, or
//! telemetry vocabulary.

use crate::pricing::{
    ModelPricing, PriceArbitrageMode, PriceRouteCandidate, PriceSource, choose_openrouter_price_route, estimate_price,
    openrouter_price_route_candidates, static_model_pricings,
};
use crate::types::{LlmKvCacheMode, LlmRouteOptimizationMode, PolicyAction};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{cmp::Ordering, hash::Hash, sync::RwLock};
use utoipa::ToSchema;

/// Preferred prefix for API keys accepted by the LLM data-plane gateway.
pub const LLM_GATEWAY_KEY_PREFIX: &str = "eden-gateway-";

/// Legacy key prefix emitted by the service proxy before the gateway split was
/// made explicit. Runtime auth should continue accepting this during migration.
pub const LEGACY_LLM_PROXY_KEY_PREFIX: &str = "eden-proxy-";

pub const LLM_GATEWAY_AGENT_ID_HEADER: &str = "x-eden-agent-id";
pub const LLM_GATEWAY_AGENT_FINGERPRINT_HEADER: &str = "x-eden-agent-fingerprint";
pub const LLM_GATEWAY_AGENT_SESSION_HEADER: &str = "x-eden-agent-session";
pub const LLM_GATEWAY_AGENT_PRINCIPAL_HEADER: &str = "x-eden-agent-principal";
pub const LLM_GATEWAY_AGENT_TAGS_HEADER: &str = "x-eden-agent-tags";

const MAX_AGENT_IDENTITY_LEN: usize = 256;
const MAX_AGENT_TAGS: usize = 32;
const MAX_AGENT_TAG_LEN: usize = 128;
const MAX_ROUTE_STATS_ENTRIES: usize = 2_048;

static LLM_GATEWAY_ROUTE_STATS: Lazy<RwLock<HashMap<LlmGatewayRouteStatsKey, LlmGatewayRouteStats>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LlmGatewayRouteStatsKey {
    provider: String,
    model: String,
    route_class: String,
}

impl LlmGatewayRouteStatsKey {
    fn from_parts(provider: &str, model: &str, route_class: &str) -> Option<Self> {
        let provider = normalize_route_stats_component(provider);
        let model = normalize_route_stats_model(model);
        let route_class = normalize_route_stats_component(route_class);

        if provider.is_empty() || model.is_empty() || route_class.is_empty() || provider == "unknown" || model == "unknown" {
            return None;
        }

        Some(Self { provider, model, route_class })
    }
}

/// Supported API-key families for the LLM gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LlmGatewayKeyKind {
    Gateway,
    LegacyProxy,
}

impl LlmGatewayKeyKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gateway => "gateway",
            Self::LegacyProxy => "legacy_proxy",
        }
    }
}

impl std::fmt::Display for LlmGatewayKeyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Normalized authentication scheme labels used by gateway telemetry and auth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LlmGatewayAuthScheme {
    None,
    GatewayKey,
    LegacyProxyKey,
    ApiKey,
    Bearer,
    Basic,
    Other,
}

impl LlmGatewayAuthScheme {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::GatewayKey => "gateway_key",
            Self::LegacyProxyKey => "legacy_proxy_key",
            Self::ApiKey => "api_key",
            Self::Bearer => "bearer",
            Self::Basic => "basic",
            Self::Other => "other",
        }
    }

    /// Classify the auth shape without validating the secret.
    pub fn classify(authorization: Option<&str>, x_api_key: Option<&str>, api_key: Option<&str>) -> Self {
        if let Some(key) = x_api_key.or(api_key).map(str::trim).filter(|value| !value.is_empty()) {
            return match LlmGatewayCredential::classify_api_key(key) {
                Some(LlmGatewayKeyKind::Gateway) => Self::GatewayKey,
                Some(LlmGatewayKeyKind::LegacyProxy) => Self::LegacyProxyKey,
                None => Self::ApiKey,
            };
        }

        let Some(value) = authorization.map(str::trim).filter(|value| !value.is_empty()) else {
            return Self::None;
        };

        if let Some(token) = LlmGatewayCredential::bearer_api_key(value) {
            return match LlmGatewayCredential::classify_api_key(token) {
                Some(LlmGatewayKeyKind::Gateway) => Self::GatewayKey,
                Some(LlmGatewayKeyKind::LegacyProxy) => Self::LegacyProxyKey,
                None => Self::Bearer,
            };
        }

        match value.split_once(' ').map(|(scheme, _)| scheme).unwrap_or(value).to_ascii_lowercase().as_str() {
            "basic" => Self::Basic,
            "bearer" => Self::Bearer,
            _ => Self::Other,
        }
    }
}

impl std::fmt::Display for LlmGatewayAuthScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Helpers for recognizing LLM gateway API-key material.
pub struct LlmGatewayCredential;

impl LlmGatewayCredential {
    /// Classify a plaintext API key by stable public prefix.
    pub fn classify_api_key(value: &str) -> Option<LlmGatewayKeyKind> {
        let trimmed = value.trim();
        if trimmed.starts_with(LLM_GATEWAY_KEY_PREFIX) {
            Some(LlmGatewayKeyKind::Gateway)
        } else if trimmed.starts_with(LEGACY_LLM_PROXY_KEY_PREFIX) {
            Some(LlmGatewayKeyKind::LegacyProxy)
        } else {
            None
        }
    }

    /// Return true when the key uses a supported gateway key prefix.
    pub fn is_supported_api_key(value: &str) -> bool {
        Self::classify_api_key(value).is_some()
    }

    /// Extract a supported gateway API key from an Authorization header.
    pub fn bearer_api_key(authorization: &str) -> Option<&str> {
        let (scheme, token) = authorization.trim().split_once(' ')?;
        if !scheme.eq_ignore_ascii_case("bearer") {
            return None;
        }
        let token = token.trim();
        Self::is_supported_api_key(token).then_some(token)
    }

    /// Extract a supported gateway API key from common inbound auth locations.
    pub fn api_key_from_parts<'a>(authorization: Option<&'a str>, x_api_key: Option<&'a str>, api_key: Option<&'a str>) -> Option<&'a str> {
        x_api_key
            .or(api_key)
            .map(str::trim)
            .filter(|value| Self::is_supported_api_key(value))
            .or_else(|| authorization.and_then(Self::bearer_api_key))
    }

    /// Stable SHA-256 hex digest for storing and comparing gateway API keys.
    pub fn hash_api_key(plaintext_key: &str) -> String {
        let digest = Sha256::digest(plaintext_key.as_bytes());
        hex_digest(&digest)
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

/// One completed gateway request observation used for adaptive route selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LlmGatewayRouteObservation {
    pub provider: String,
    pub model: String,
    pub route_class: String,
    pub latency_ms: u64,
    pub output_tokens: u64,
    pub success: bool,
}

/// Rolling in-memory route stats for one provider/model/route-class tuple.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LlmGatewayRouteStats {
    pub provider: String,
    pub model: String,
    pub route_class: String,
    pub success_count: u64,
    pub error_count: u64,
    pub total_latency_ms: u64,
    pub min_latency_ms: u64,
    pub max_latency_ms: u64,
    pub total_output_tokens: u64,
    pub total_duration_ms: u64,
    pub first_observed_unix_ms: i64,
    pub last_observed_unix_ms: i64,
}

impl LlmGatewayRouteStats {
    fn new(key: &LlmGatewayRouteStatsKey, observed_at_unix_ms: i64) -> Self {
        Self {
            provider: key.provider.clone(),
            model: key.model.clone(),
            route_class: key.route_class.clone(),
            success_count: 0,
            error_count: 0,
            total_latency_ms: 0,
            min_latency_ms: 0,
            max_latency_ms: 0,
            total_output_tokens: 0,
            total_duration_ms: 0,
            first_observed_unix_ms: observed_at_unix_ms,
            last_observed_unix_ms: observed_at_unix_ms,
        }
    }

    fn record(&mut self, observation: &LlmGatewayRouteObservation, observed_at_unix_ms: i64) {
        if observation.success {
            self.success_count = self.success_count.saturating_add(1);
        } else {
            self.error_count = self.error_count.saturating_add(1);
        }

        let latency_ms = observation.latency_ms;
        self.total_latency_ms = self.total_latency_ms.saturating_add(latency_ms);
        self.total_output_tokens = self.total_output_tokens.saturating_add(observation.output_tokens);
        self.total_duration_ms = self.total_duration_ms.saturating_add(latency_ms.max(1));
        self.last_observed_unix_ms = observed_at_unix_ms;

        if self.min_latency_ms == 0 || latency_ms < self.min_latency_ms {
            self.min_latency_ms = latency_ms;
        }
        if latency_ms > self.max_latency_ms {
            self.max_latency_ms = latency_ms;
        }
    }

    pub fn observation_count(&self) -> u64 {
        self.success_count.saturating_add(self.error_count)
    }

    pub fn average_latency_ms(&self) -> Option<u64> {
        let count = self.observation_count();
        (count > 0).then_some(self.total_latency_ms / count)
    }

    pub fn output_tokens_per_second_milli(&self) -> Option<u64> {
        (self.total_duration_ms > 0).then(|| self.total_output_tokens.saturating_mul(1_000_000) / self.total_duration_ms)
    }

    pub fn error_rate_per_million(&self) -> Option<u64> {
        let count = self.observation_count();
        (count > 0).then_some(self.error_count.saturating_mul(1_000_000) / count)
    }
}

/// Record one completed gateway route observation.
pub fn record_llm_gateway_route_observation(observation: LlmGatewayRouteObservation) {
    let Some(key) = LlmGatewayRouteStatsKey::from_parts(&observation.provider, &observation.model, &observation.route_class) else {
        return;
    };
    let observed_at_unix_ms = unix_millis_now();
    let Ok(mut stats) = LLM_GATEWAY_ROUTE_STATS.write() else {
        return;
    };

    stats
        .entry(key.clone())
        .or_insert_with(|| LlmGatewayRouteStats::new(&key, observed_at_unix_ms))
        .record(&observation, observed_at_unix_ms);
    trim_route_stats(&mut stats);
}

/// Return a point-in-time snapshot of route stats for control-plane sync or tests.
pub fn llm_gateway_route_stats_snapshot() -> Vec<LlmGatewayRouteStats> {
    let Ok(stats) = LLM_GATEWAY_ROUTE_STATS.read() else {
        return Vec::new();
    };

    stats.values().cloned().collect()
}

/// Clear route stats. Intended for tests and process lifecycle resets.
pub fn clear_llm_gateway_route_stats() {
    if let Ok(mut stats) = LLM_GATEWAY_ROUTE_STATS.write() {
        stats.clear();
    }
}

/// Merge service-managed route stats into the process-local route-stat cache.
pub fn hydrate_llm_gateway_route_stats(route_stats: Vec<LlmGatewayRouteStats>) {
    let Ok(mut stats) = LLM_GATEWAY_ROUTE_STATS.write() else {
        return;
    };

    for route_stat in route_stats {
        let Some(key) = LlmGatewayRouteStatsKey::from_parts(&route_stat.provider, &route_stat.model, &route_stat.route_class) else {
            continue;
        };
        if route_stat.observation_count() == 0 {
            continue;
        }
        stats.insert(
            key.clone(),
            LlmGatewayRouteStats {
                provider: key.provider,
                model: key.model,
                route_class: key.route_class,
                ..route_stat
            },
        );
    }
    trim_route_stats(&mut stats);
}

fn trim_route_stats(stats: &mut HashMap<LlmGatewayRouteStatsKey, LlmGatewayRouteStats>) {
    if stats.len() <= MAX_ROUTE_STATS_ENTRIES {
        return;
    }

    let overflow = stats.len().saturating_sub(MAX_ROUTE_STATS_ENTRIES);
    let mut oldest = stats.iter().map(|(key, value)| (key.clone(), value.last_observed_unix_ms)).collect::<Vec<_>>();
    oldest.sort_by_key(|(_, last_observed_unix_ms)| *last_observed_unix_ms);

    for (key, _) in oldest.into_iter().take(overflow) {
        stats.remove(&key);
    }
}

fn unix_millis_now() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(i64::MAX as u128) as i64,
        Err(_) => 0,
    }
}

fn normalize_route_stats_component(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn normalize_route_stats_model(model: &str) -> String {
    let normalized = normalize_route_stats_component(model);
    if let Some((_, suffix)) = normalized.split_once('/') {
        return suffix.to_string();
    }
    normalized
}

/// Runtime policy managed by the service and enforced by the gateway.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LlmGatewayPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_allowlist: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_tools: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_prompt_characters: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_definitions: Option<u32>,
    #[serde(default)]
    pub request_pii_action: PolicyAction,
    #[serde(default)]
    pub response_pii_action: PolicyAction,
    #[serde(default)]
    pub prompt_security_action: PolicyAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_rpm: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_tokens_monthly: Option<u64>,
    #[serde(default)]
    pub price_arbitrage_mode: PriceArbitrageMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_cache_ttl_secs: Option<u64>,
    #[serde(default)]
    pub route_optimization_mode: LlmRouteOptimizationMode,
    #[serde(default)]
    pub kv_cache_mode: LlmKvCacheMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_cache_ttl_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_switch_threshold_percent: Option<u8>,
}

/// Authentication mode hydrated by the LLM gateway control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum LlmGatewayControlPlaneAuthMode {
    #[default]
    Disabled,
    Observe,
    Enforce,
}

impl LlmGatewayControlPlaneAuthMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Observe => "observe",
            Self::Enforce => "enforce",
        }
    }
}

impl std::fmt::Display for LlmGatewayControlPlaneAuthMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Service-managed key policy record used by the data-plane gateway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LlmGatewayKeyPolicy {
    pub key_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_uuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_uuid: Option<String>,
    #[serde(default)]
    pub policy: LlmGatewayPolicy,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl LlmGatewayKeyPolicy {
    pub fn normalized(mut self) -> Option<Self> {
        self.key_hash = self.key_hash.trim().to_ascii_lowercase();
        if !is_sha256_hex(self.key_hash.as_str()) {
            return None;
        }
        self.key_prefix = normalize_non_empty(self.key_prefix);
        self.org_uuid = normalize_non_empty(self.org_uuid);
        self.endpoint_uuid = normalize_non_empty(self.endpoint_uuid);
        Some(self)
    }
}

/// Hot-path control-plane snapshot consumed by `eden_gateway`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LlmGatewayControlPlaneSnapshot {
    #[serde(default)]
    pub version: u64,
    #[serde(default)]
    pub auth_mode: LlmGatewayControlPlaneAuthMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_policy: Option<LlmGatewayPolicy>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_policies: Vec<LlmGatewayKeyPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_catalog: Option<LlmGatewayModelCatalog>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub route_stats: Vec<LlmGatewayRouteStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_unix_ms: Option<i64>,
}

impl Default for LlmGatewayControlPlaneSnapshot {
    fn default() -> Self {
        Self {
            version: 0,
            auth_mode: LlmGatewayControlPlaneAuthMode::Disabled,
            default_policy: None,
            key_policies: Vec::new(),
            model_catalog: None,
            route_stats: Vec::new(),
            updated_at_unix_ms: None,
        }
    }
}

impl LlmGatewayControlPlaneSnapshot {
    pub fn normalized(mut self) -> Self {
        self.key_policies = self.key_policies.into_iter().filter_map(LlmGatewayKeyPolicy::normalized).collect();
        self.route_stats.retain(|stats| {
            LlmGatewayRouteStatsKey::from_parts(&stats.provider, &stats.model, &stats.route_class).is_some()
                && stats.observation_count() > 0
        });
        self
    }

    pub fn enabled_key_hashes(&self) -> BTreeSet<String> {
        self.key_policies.iter().filter(|policy| policy.enabled).map(|policy| policy.key_hash.clone()).collect()
    }

    pub fn policy_for_key_hash(&self, key_hash: &str) -> Option<&LlmGatewayPolicy> {
        let key_hash = key_hash.trim().to_ascii_lowercase();
        self.key_policies
            .iter()
            .find(|policy| policy.enabled && policy.key_hash == key_hash)
            .map(|policy| &policy.policy)
            .or(self.default_policy.as_ref())
    }

    pub fn model_catalog_or_builtin(&self) -> LlmGatewayModelCatalog {
        self.model_catalog.clone().unwrap_or_else(LlmGatewayModelCatalog::builtin)
    }
}

fn default_true() -> bool {
    true
}

fn normalize_non_empty(value: Option<String>) -> Option<String> {
    value.map(|value| value.trim().to_string()).filter(|value| !value.is_empty())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Route selection metadata for one gateway LLM request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LlmGatewayRouteDecision {
    pub requested_provider: String,
    pub requested_model: String,
    pub selected_provider: String,
    pub selected_model: String,
    pub route_class: String,
    pub price_arbitrage_mode: PriceArbitrageMode,
    pub route_optimization_mode: LlmRouteOptimizationMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_source: Option<PriceSource>,
    pub baseline_estimated_cost_micros: u64,
    pub selected_estimated_cost_micros: u64,
    pub estimated_savings_micros: u64,
    #[serde(default)]
    pub route_stats_sample_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_average_latency_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_output_tokens_per_second_milli: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_error_rate_per_million: Option<u64>,
    pub model_rewritten: bool,
    pub reason: String,
}

impl LlmGatewayRouteDecision {
    pub fn selected_model_changed(&self) -> bool {
        self.model_rewritten && self.selected_model != self.requested_model
    }
}

/// Stateless route selector shared by the service proxy and data-plane gateway.
pub struct LlmGatewayRouteSelector;

struct RouteStatsChoice {
    candidate: PriceRouteCandidate,
    stats: LlmGatewayRouteStats,
    reason: &'static str,
}

struct RouteDecisionContext<'a> {
    policy: &'a LlmGatewayPolicy,
    requested_provider: &'a str,
    requested_model: &'a str,
    route_class: &'a str,
    baseline_cost: u64,
}

impl LlmGatewayRouteSelector {
    pub fn select(
        policy: &LlmGatewayPolicy,
        requested_provider: &str,
        requested_model: &str,
        route_class: &str,
        prompt_tokens: u32,
        completion_tokens: u32,
    ) -> LlmGatewayRouteDecision {
        let route_stats_storage;
        let route_stats = if Self::requires_route_stats(policy, requested_provider) {
            route_stats_storage = llm_gateway_route_stats_snapshot();
            route_stats_storage.as_slice()
        } else {
            &[]
        };

        Self::select_with_route_stats(
            policy,
            route_stats,
            requested_provider,
            requested_model,
            route_class,
            prompt_tokens,
            completion_tokens,
        )
    }

    pub fn select_with_route_stats(
        policy: &LlmGatewayPolicy,
        route_stats: &[LlmGatewayRouteStats],
        requested_provider: &str,
        requested_model: &str,
        route_class: &str,
        prompt_tokens: u32,
        completion_tokens: u32,
    ) -> LlmGatewayRouteDecision {
        let requested_provider = requested_provider.trim().to_ascii_lowercase();
        let requested_model = requested_model.trim();
        let route_class = route_class.trim();

        if requested_provider == "openrouter" && policy.price_arbitrage_mode != PriceArbitrageMode::Disabled {
            return Self::openrouter_price_route(
                policy,
                &requested_provider,
                requested_model,
                route_class,
                prompt_tokens,
                completion_tokens,
                route_stats,
            );
        }

        let estimate = estimate_price(&requested_provider, requested_model, prompt_tokens, completion_tokens);
        let estimated_cost = estimate.as_ref().map(|estimate| estimate.estimated_cost_micros).unwrap_or_default();
        LlmGatewayRouteDecision {
            requested_provider: requested_provider.clone(),
            requested_model: requested_model.to_string(),
            selected_provider: requested_provider,
            selected_model: requested_model.to_string(),
            route_class: route_class.to_string(),
            price_arbitrage_mode: policy.price_arbitrage_mode,
            route_optimization_mode: policy.route_optimization_mode,
            price_source: estimate.map(|estimate| estimate.source),
            baseline_estimated_cost_micros: estimated_cost,
            selected_estimated_cost_micros: estimated_cost,
            estimated_savings_micros: 0,
            route_stats_sample_count: 0,
            selected_average_latency_ms: None,
            selected_output_tokens_per_second_milli: None,
            selected_error_rate_per_million: None,
            model_rewritten: false,
            reason: if policy.price_arbitrage_mode == PriceArbitrageMode::Disabled {
                "price_arbitrage_disabled".to_string()
            } else {
                "provider_not_arbitrageable".to_string()
            },
        }
    }

    fn requires_route_stats(policy: &LlmGatewayPolicy, requested_provider: &str) -> bool {
        requested_provider.trim().eq_ignore_ascii_case("openrouter")
            && policy.price_arbitrage_mode != PriceArbitrageMode::Disabled
            && policy.route_optimization_mode != LlmRouteOptimizationMode::Cost
    }

    fn openrouter_price_route(
        policy: &LlmGatewayPolicy,
        requested_provider: &str,
        requested_model: &str,
        route_class: &str,
        prompt_tokens: u32,
        completion_tokens: u32,
        route_stats: &[LlmGatewayRouteStats],
    ) -> LlmGatewayRouteDecision {
        let decision = choose_openrouter_price_route(
            policy.price_arbitrage_mode,
            requested_model,
            policy.model_allowlist.as_deref(),
            prompt_tokens,
            completion_tokens,
        );
        let model_rewritten = decision.selected_model != requested_model;
        if policy.route_optimization_mode != LlmRouteOptimizationMode::Cost
            && let Some(route_choice) = Self::choose_route_stats_candidate(
                policy,
                route_stats,
                requested_provider,
                requested_model,
                route_class,
                prompt_tokens,
                completion_tokens,
            )
        {
            return Self::route_decision_from_candidate(
                &RouteDecisionContext {
                    policy,
                    requested_provider,
                    requested_model,
                    route_class,
                    baseline_cost: decision.baseline_estimated_cost_micros,
                },
                &route_choice.candidate,
                Some(&route_choice.stats),
                route_choice.reason,
            );
        }

        LlmGatewayRouteDecision {
            requested_provider: requested_provider.to_string(),
            requested_model: requested_model.to_string(),
            selected_provider: requested_provider.to_string(),
            selected_model: decision.selected_model,
            route_class: route_class.to_string(),
            price_arbitrage_mode: policy.price_arbitrage_mode,
            route_optimization_mode: policy.route_optimization_mode,
            price_source: decision.price_source,
            baseline_estimated_cost_micros: decision.baseline_estimated_cost_micros,
            selected_estimated_cost_micros: decision.selected_estimated_cost_micros,
            estimated_savings_micros: decision.estimated_savings_micros,
            route_stats_sample_count: 0,
            selected_average_latency_ms: None,
            selected_output_tokens_per_second_milli: None,
            selected_error_rate_per_million: None,
            model_rewritten,
            reason: decision.reason,
        }
    }

    fn choose_route_stats_candidate(
        policy: &LlmGatewayPolicy,
        route_stats: &[LlmGatewayRouteStats],
        requested_provider: &str,
        requested_model: &str,
        route_class: &str,
        prompt_tokens: u32,
        completion_tokens: u32,
    ) -> Option<RouteStatsChoice> {
        let candidates =
            openrouter_price_route_candidates(requested_model, policy.model_allowlist.as_deref(), prompt_tokens, completion_tokens);
        Self::choose_from_route_stats(policy.route_optimization_mode, requested_provider, route_class, candidates.as_slice(), route_stats)
    }

    fn choose_from_route_stats(
        mode: LlmRouteOptimizationMode,
        provider: &str,
        route_class: &str,
        candidates: &[PriceRouteCandidate],
        route_stats: &[LlmGatewayRouteStats],
    ) -> Option<RouteStatsChoice> {
        let mut observed = candidates
            .iter()
            .filter_map(|candidate| {
                let stats = Self::find_route_stats(route_stats, provider, &candidate.model, route_class)?;
                (stats.success_count > 0).then_some(RouteStatsChoice {
                    candidate: candidate.clone(),
                    stats,
                    reason: match mode {
                        LlmRouteOptimizationMode::Cost => "cost_lowest_price",
                        LlmRouteOptimizationMode::Latency => "route_stats_lowest_latency",
                        LlmRouteOptimizationMode::Throughput => "route_stats_highest_throughput",
                        LlmRouteOptimizationMode::Balanced => "route_stats_balanced",
                    },
                })
            })
            .collect::<Vec<_>>();

        if observed.is_empty() {
            return None;
        }

        match mode {
            LlmRouteOptimizationMode::Cost => observed.sort_by(Self::compare_cost_choice),
            LlmRouteOptimizationMode::Latency => observed.sort_by(Self::compare_latency_choice),
            LlmRouteOptimizationMode::Throughput => observed.sort_by(Self::compare_throughput_choice),
            LlmRouteOptimizationMode::Balanced => observed.sort_by(Self::compare_balanced_choice),
        }

        observed.into_iter().next()
    }

    fn find_route_stats(
        route_stats: &[LlmGatewayRouteStats],
        provider: &str,
        model: &str,
        route_class: &str,
    ) -> Option<LlmGatewayRouteStats> {
        let provider = normalize_route_stats_component(provider);
        let model = normalize_route_stats_model(model);
        let route_class = normalize_route_stats_component(route_class);

        route_stats
            .iter()
            .filter(|stats| stats.provider == provider && stats.model == model && stats.route_class == route_class)
            .max_by_key(|stats| (stats.observation_count(), stats.last_observed_unix_ms))
            .cloned()
    }

    fn route_decision_from_candidate(
        context: &RouteDecisionContext<'_>,
        candidate: &PriceRouteCandidate,
        stats: Option<&LlmGatewayRouteStats>,
        reason: &str,
    ) -> LlmGatewayRouteDecision {
        let selected_cost = candidate.estimated_cost_micros;
        LlmGatewayRouteDecision {
            requested_provider: context.requested_provider.to_string(),
            requested_model: context.requested_model.to_string(),
            selected_provider: context.requested_provider.to_string(),
            selected_model: candidate.model.clone(),
            route_class: context.route_class.to_string(),
            price_arbitrage_mode: context.policy.price_arbitrage_mode,
            route_optimization_mode: context.policy.route_optimization_mode,
            price_source: Some(candidate.source),
            baseline_estimated_cost_micros: context.baseline_cost,
            selected_estimated_cost_micros: selected_cost,
            estimated_savings_micros: context.baseline_cost.saturating_sub(selected_cost),
            route_stats_sample_count: stats.map(LlmGatewayRouteStats::observation_count).unwrap_or_default(),
            selected_average_latency_ms: stats.and_then(LlmGatewayRouteStats::average_latency_ms),
            selected_output_tokens_per_second_milli: stats.and_then(LlmGatewayRouteStats::output_tokens_per_second_milli),
            selected_error_rate_per_million: stats.and_then(LlmGatewayRouteStats::error_rate_per_million),
            model_rewritten: candidate.model != context.requested_model,
            reason: reason.to_string(),
        }
    }

    fn compare_cost_choice(left: &RouteStatsChoice, right: &RouteStatsChoice) -> Ordering {
        left.candidate
            .estimated_cost_micros
            .cmp(&right.candidate.estimated_cost_micros)
            .then_with(|| left.candidate.model.cmp(&right.candidate.model))
    }

    fn compare_latency_choice(left: &RouteStatsChoice, right: &RouteStatsChoice) -> Ordering {
        let left_key = (
            left.stats.average_latency_ms().unwrap_or(u64::MAX),
            left.stats.error_rate_per_million().unwrap_or(u64::MAX),
            left.candidate.estimated_cost_micros,
            left.candidate.model.as_str(),
        );
        let right_key = (
            right.stats.average_latency_ms().unwrap_or(u64::MAX),
            right.stats.error_rate_per_million().unwrap_or(u64::MAX),
            right.candidate.estimated_cost_micros,
            right.candidate.model.as_str(),
        );
        left_key.cmp(&right_key)
    }

    fn compare_throughput_choice(left: &RouteStatsChoice, right: &RouteStatsChoice) -> Ordering {
        let left_key = (
            left.stats.output_tokens_per_second_milli().unwrap_or_default(),
            u64::MAX.saturating_sub(left.stats.error_rate_per_million().unwrap_or(u64::MAX)),
            u64::MAX.saturating_sub(left.candidate.estimated_cost_micros),
        );
        let right_key = (
            right.stats.output_tokens_per_second_milli().unwrap_or_default(),
            u64::MAX.saturating_sub(right.stats.error_rate_per_million().unwrap_or(u64::MAX)),
            u64::MAX.saturating_sub(right.candidate.estimated_cost_micros),
        );
        right_key.cmp(&left_key).then_with(|| left.candidate.model.cmp(&right.candidate.model))
    }

    fn compare_balanced_choice(left: &RouteStatsChoice, right: &RouteStatsChoice) -> Ordering {
        let left_key = (
            Self::balanced_score(left),
            left.stats.error_rate_per_million().unwrap_or(u64::MAX),
            left.candidate.estimated_cost_micros,
            left.candidate.model.as_str(),
        );
        let right_key = (
            Self::balanced_score(right),
            right.stats.error_rate_per_million().unwrap_or(u64::MAX),
            right.candidate.estimated_cost_micros,
            right.candidate.model.as_str(),
        );
        left_key.cmp(&right_key)
    }

    fn balanced_score(choice: &RouteStatsChoice) -> u128 {
        let latency_score = u128::from(choice.stats.average_latency_ms().unwrap_or(u64::MAX / 2)).saturating_mul(1_000);
        let cost_score = u128::from(choice.candidate.estimated_cost_micros / 100);
        let error_score = u128::from(choice.stats.error_rate_per_million().unwrap_or(1_000_000)).saturating_mul(10);
        let throughput = choice.stats.output_tokens_per_second_milli().unwrap_or_default();
        let throughput_penalty = if throughput == 0 {
            1_000_000_000
        } else {
            1_000_000_000 / throughput
        };
        latency_score.saturating_add(cost_score).saturating_add(error_score).saturating_add(u128::from(throughput_penalty))
    }
}

/// Lifecycle state for a model exposed through the LLM gateway catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LlmModelLifecycle {
    Active,
    Preview,
    Deprecated,
    Retired,
    Unknown,
}

impl LlmModelLifecycle {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Preview => "preview",
            Self::Deprecated => "deprecated",
            Self::Retired => "retired",
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for LlmModelLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Payload modality advertised by a model catalog entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LlmModelModality {
    Text,
    Image,
    Audio,
    Embedding,
}

impl LlmModelModality {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Image => "image",
            Self::Audio => "audio",
            Self::Embedding => "embedding",
        }
    }
}

impl std::fmt::Display for LlmModelModality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Operation families supported by a model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LlmModelOperation {
    ChatCompletions,
    Embeddings,
}

impl LlmModelOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ChatCompletions => "chat.completions",
            Self::Embeddings => "embeddings",
        }
    }
}

impl std::fmt::Display for LlmModelOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Pricing metadata attached to a gateway model catalog entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LlmModelCatalogPricing {
    pub source: PriceSource,
    pub input_micros_per_million: u64,
    pub output_micros_per_million: u64,
}

/// Stable model catalog entry shared by service management and gateway runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LlmModelCatalogEntry {
    pub id: String,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub regions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modalities: Vec<LlmModelModality>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<LlmModelOperation>,
    pub supports_tools: bool,
    pub supports_streaming: bool,
    pub supports_json_schema: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_group: Option<String>,
    pub lifecycle: LlmModelLifecycle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing: Option<LlmModelCatalogPricing>,
}

impl LlmModelCatalogEntry {
    pub fn matches_model(&self, model: &str) -> bool {
        let model = model.trim();
        self.id.eq_ignore_ascii_case(model)
            || self.model.eq_ignore_ascii_case(model)
            || self.aliases.iter().any(|alias| alias.eq_ignore_ascii_case(model))
    }

    fn from_static_pricing(pricing: &ModelPricing) -> Self {
        let model = pricing.canonical_model.to_string();
        let provider = pricing.provider.to_string();
        let operations = model_operations(&model);
        let modalities = model_modalities(pricing.provider, &model);
        let supports_tools = operations.contains(&LlmModelOperation::ChatCompletions) && !model.contains("embedding");
        let supports_json_schema = supports_tools && pricing.provider == "openai";

        Self {
            id: model.clone(),
            provider: provider.clone(),
            model,
            aliases: pricing.aliases.iter().map(|alias| (*alias).to_string()).collect(),
            regions: vec!["global".to_string()],
            context_window_tokens: context_window_tokens(pricing.provider, pricing.canonical_model),
            modalities,
            operations,
            supports_tools,
            supports_streaming: true,
            supports_json_schema,
            fallback_group: fallback_group(pricing.provider, pricing.canonical_model),
            lifecycle: lifecycle(pricing.canonical_model),
            pricing: Some(LlmModelCatalogPricing {
                source: PriceSource::StaticFallback,
                input_micros_per_million: pricing.input_micros_per_million,
                output_micros_per_million: pricing.output_micros_per_million,
            }),
        }
    }
}

/// Model catalog used by the gateway runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LlmGatewayModelCatalog {
    entries: Vec<LlmModelCatalogEntry>,
}

impl LlmGatewayModelCatalog {
    pub fn builtin() -> Self {
        let mut entries = static_model_pricings().map(LlmModelCatalogEntry::from_static_pricing).collect::<Vec<_>>();
        entries.sort_by(|left, right| left.provider.cmp(&right.provider).then_with(|| left.id.cmp(&right.id)));
        entries.dedup_by(|left, right| left.provider == right.provider && left.id == right.id);
        Self { entries }
    }

    pub fn new(entries: Vec<LlmModelCatalogEntry>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[LlmModelCatalogEntry] {
        &self.entries
    }

    pub fn filter_allowed(&self, allowed_models: Option<&BTreeSet<String>>) -> Self {
        let Some(allowed_models) = allowed_models else {
            return self.clone();
        };

        let entries =
            self.entries.iter().filter(|entry| allowed_models.iter().any(|allowed| entry.matches_model(allowed))).cloned().collect();
        Self { entries }
    }
}

fn model_operations(model: &str) -> Vec<LlmModelOperation> {
    if model.contains("embedding") {
        vec![LlmModelOperation::Embeddings]
    } else {
        vec![LlmModelOperation::ChatCompletions]
    }
}

fn model_modalities(provider: &str, model: &str) -> Vec<LlmModelModality> {
    if model.contains("embedding") {
        return vec![LlmModelModality::Embedding];
    }

    let mut modalities = BTreeSet::from([LlmModelModality::Text]);
    if provider == "anthropic"
        || model.starts_with("gpt-4o")
        || model.starts_with("gpt-4.1")
        || model.starts_with("gpt-5")
        || model.starts_with("o3")
        || model.starts_with("o4")
    {
        modalities.insert(LlmModelModality::Image);
    }
    modalities.into_iter().collect()
}

fn context_window_tokens(provider: &str, model: &str) -> Option<u32> {
    if model.contains("embedding") {
        return Some(8_192);
    }
    if provider == "anthropic" {
        return Some(200_000);
    }
    if model.starts_with("gpt-4.1") {
        return Some(1_047_576);
    }
    if model.starts_with("gpt-5") || model.starts_with("gpt-4o") || model.starts_with("o3") || model.starts_with("o4") {
        return Some(128_000);
    }
    if model.starts_with("gpt-4") {
        return Some(128_000);
    }
    None
}

fn fallback_group(provider: &str, model: &str) -> Option<String> {
    let family = if model.contains("embedding") {
        "embedding"
    } else if model.contains("opus") {
        "opus"
    } else if model.contains("sonnet") {
        "sonnet"
    } else if model.contains("haiku") {
        "haiku"
    } else if model.starts_with("gpt-5") {
        "gpt-5"
    } else if model.starts_with("gpt-4.1") {
        "gpt-4.1"
    } else if model.starts_with("gpt-4o") {
        "gpt-4o"
    } else if model.starts_with("gpt-4") {
        "gpt-4"
    } else if model.starts_with("o") {
        "reasoning"
    } else {
        return None;
    };

    Some(format!("{provider}:{family}"))
}

fn lifecycle(model: &str) -> LlmModelLifecycle {
    if model.contains("preview") {
        LlmModelLifecycle::Preview
    } else if model == "gpt-4" || model == "gpt-4-turbo" || model == "claude-3-opus" {
        LlmModelLifecycle::Deprecated
    } else {
        LlmModelLifecycle::Active
    }
}

/// Optional agent identity attached to a gateway LLM request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LlmGatewayAgentIdentity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

impl LlmGatewayAgentIdentity {
    pub fn from_parts(
        agent_id: Option<&str>,
        fingerprint: Option<&str>,
        session_id: Option<&str>,
        principal: Option<&str>,
        tags: Option<&str>,
    ) -> Self {
        Self {
            agent_id: Self::normalize_id(agent_id),
            fingerprint: Self::normalize_id(fingerprint),
            session_id: Self::normalize_id(session_id),
            principal: Self::normalize_id(principal),
            tags: Self::parse_tags(tags),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.agent_id.is_none()
            && self.fingerprint.is_none()
            && self.session_id.is_none()
            && self.principal.is_none()
            && self.tags.is_empty()
    }

    fn normalize_id(value: Option<&str>) -> Option<String> {
        value.map(str::trim).filter(|value| !value.is_empty() && value.len() <= MAX_AGENT_IDENTITY_LEN).map(ToOwned::to_owned)
    }

    fn parse_tags(value: Option<&str>) -> BTreeMap<String, String> {
        let Some(value) = value else {
            return BTreeMap::new();
        };

        value
            .split(',')
            .filter_map(|part| {
                let (key, value) = part.split_once('=')?;
                let key = key.trim();
                let value = value.trim();
                if key.is_empty() || value.is_empty() || key.len() > MAX_AGENT_TAG_LEN || value.len() > MAX_AGENT_TAG_LEN {
                    return None;
                }
                Some((key.to_string(), value.to_string()))
            })
            .take(MAX_AGENT_TAGS)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_gateway_and_legacy_keys() {
        assert_eq!(LlmGatewayCredential::classify_api_key("eden-gateway-abc"), Some(LlmGatewayKeyKind::Gateway));
        assert_eq!(LlmGatewayCredential::classify_api_key("eden-proxy-abc"), Some(LlmGatewayKeyKind::LegacyProxy));
        assert_eq!(LlmGatewayCredential::classify_api_key("sk-abc"), None);
    }

    #[test]
    fn classifies_authorization_header_without_validating_secret() {
        assert_eq!(
            LlmGatewayAuthScheme::classify(Some("Bearer eden-gateway-abc"), None, None),
            LlmGatewayAuthScheme::GatewayKey
        );
        assert_eq!(
            LlmGatewayAuthScheme::classify(Some("Bearer eden-proxy-abc"), None, None),
            LlmGatewayAuthScheme::LegacyProxyKey
        );
        assert_eq!(LlmGatewayAuthScheme::classify(Some("Bearer sk-abc"), None, None), LlmGatewayAuthScheme::Bearer);
        assert_eq!(
            LlmGatewayAuthScheme::classify(None, Some("eden-gateway-abc"), None),
            LlmGatewayAuthScheme::GatewayKey
        );
    }

    #[test]
    fn extracts_and_hashes_supported_api_keys() {
        assert_eq!(
            LlmGatewayCredential::api_key_from_parts(Some("Bearer eden-gateway-abc"), None, None),
            Some("eden-gateway-abc")
        );
        assert_eq!(
            LlmGatewayCredential::api_key_from_parts(Some("Bearer sk-abc"), None, Some("eden-proxy-abc")),
            Some("eden-proxy-abc")
        );
        assert_eq!(
            LlmGatewayCredential::hash_api_key("eden-gateway-abc"),
            "6a55b754521ca93501c2f755df86561ae5db493dd1b56ef4170964a0d9b966cb"
        );
    }

    #[test]
    fn builds_agent_identity_from_headers_without_accepting_empty_values() {
        let identity = LlmGatewayAgentIdentity::from_parts(
            Some("agent-1"),
            Some("sha256:abc"),
            Some("session-1"),
            Some("principal-1"),
            Some("tier=prod, region=us-east-1, empty= "),
        );

        assert_eq!(identity.agent_id.as_deref(), Some("agent-1"));
        assert_eq!(identity.fingerprint.as_deref(), Some("sha256:abc"));
        assert_eq!(identity.session_id.as_deref(), Some("session-1"));
        assert_eq!(identity.principal.as_deref(), Some("principal-1"));
        assert_eq!(identity.tags.get("tier").map(String::as_str), Some("prod"));
        assert_eq!(identity.tags.get("region").map(String::as_str), Some("us-east-1"));
        assert!(!identity.tags.contains_key("empty"));
    }

    #[test]
    fn builds_builtin_model_catalog_from_static_pricing() {
        let catalog = LlmGatewayModelCatalog::builtin();
        let gpt_4_1 = catalog.entries().iter().find(|entry| entry.id == "gpt-4.1").expect("gpt-4.1 should be cataloged");

        assert_eq!(gpt_4_1.provider, "openai");
        assert_eq!(gpt_4_1.context_window_tokens, Some(1_047_576));
        assert!(gpt_4_1.modalities.contains(&LlmModelModality::Text));
        assert!(gpt_4_1.modalities.contains(&LlmModelModality::Image));
        assert!(gpt_4_1.operations.contains(&LlmModelOperation::ChatCompletions));
        assert!(gpt_4_1.supports_tools);
        assert_eq!(gpt_4_1.pricing.as_ref().map(|pricing| pricing.source), Some(PriceSource::StaticFallback));
    }

    #[test]
    fn model_catalog_filters_by_ids_and_aliases() {
        let catalog = LlmGatewayModelCatalog::builtin();
        let filtered = catalog.filter_allowed(Some(&BTreeSet::from(["claude-opus-4-6".to_string(), "gpt-4.1".to_string()])));

        assert!(filtered.entries().iter().any(|entry| entry.id == "claude-opus-4.6"));
        assert!(filtered.entries().iter().any(|entry| entry.id == "gpt-4.1"));
        assert!(!filtered.entries().iter().any(|entry| entry.id == "gpt-4o"));
    }

    #[test]
    fn route_selector_rewrites_openrouter_model_when_price_arbitrage_is_enabled() {
        let policy = LlmGatewayPolicy {
            model_allowlist: Some(vec!["gpt-4.1".to_string(), "gpt-4.1-mini".to_string()]),
            price_arbitrage_mode: PriceArbitrageMode::AllowedModelsCheapest,
            ..LlmGatewayPolicy::default()
        };

        let decision = LlmGatewayRouteSelector::select(&policy, "openrouter", "gpt-4.1", "default", 10_000, 10_000);

        assert_eq!(decision.selected_model, "gpt-4.1-mini");
        assert!(decision.selected_model_changed());
        assert_eq!(decision.estimated_savings_micros, 90_000);
        assert_eq!(decision.reason, "allowed_model_cheaper");
    }

    #[test]
    fn route_selector_observes_non_openrouter_without_rewrite() {
        let policy = LlmGatewayPolicy {
            price_arbitrage_mode: PriceArbitrageMode::AllowedModelsCheapest,
            ..LlmGatewayPolicy::default()
        };

        let decision = LlmGatewayRouteSelector::select(&policy, "openai", "gpt-4.1", "default", 10_000, 10_000);

        assert_eq!(decision.selected_model, "gpt-4.1");
        assert!(!decision.selected_model_changed());
        assert_eq!(decision.reason, "provider_not_arbitrageable");
        assert_eq!(decision.baseline_estimated_cost_micros, 100_000);
    }

    #[test]
    fn route_selector_only_requires_stats_for_openrouter_non_cost_optimization() {
        let mut policy = LlmGatewayPolicy {
            price_arbitrage_mode: PriceArbitrageMode::AllowedModelsCheapest,
            ..LlmGatewayPolicy::default()
        };

        assert!(!LlmGatewayRouteSelector::requires_route_stats(&policy, "openai"));
        assert!(!LlmGatewayRouteSelector::requires_route_stats(&policy, "openrouter"));

        policy.route_optimization_mode = LlmRouteOptimizationMode::Latency;

        assert!(LlmGatewayRouteSelector::requires_route_stats(&policy, " openrouter "));
        assert!(!LlmGatewayRouteSelector::requires_route_stats(&policy, "openai"));

        policy.price_arbitrage_mode = PriceArbitrageMode::Disabled;

        assert!(!LlmGatewayRouteSelector::requires_route_stats(&policy, "openrouter"));
    }

    #[test]
    fn route_selector_uses_latency_stats_when_enabled() {
        let policy = LlmGatewayPolicy {
            model_allowlist: Some(vec!["gpt-4.1".to_string(), "gpt-4.1-mini".to_string()]),
            price_arbitrage_mode: PriceArbitrageMode::AllowedModelsCheapest,
            route_optimization_mode: LlmRouteOptimizationMode::Latency,
            ..LlmGatewayPolicy::default()
        };
        let route_stats = vec![
            observed_route_stats("openrouter", "gpt-4.1", "default", 5, 20, 10),
            observed_route_stats("openrouter", "gpt-4.1-mini", "default", 5, 200, 200),
        ];

        let decision =
            LlmGatewayRouteSelector::select_with_route_stats(&policy, &route_stats, "openrouter", "gpt-4.1", "default", 10_000, 10_000);

        assert_eq!(decision.selected_model, "gpt-4.1");
        assert!(!decision.selected_model_changed());
        assert_eq!(decision.reason, "route_stats_lowest_latency");
        assert_eq!(decision.route_stats_sample_count, 5);
        assert_eq!(decision.selected_average_latency_ms, Some(20));
        assert_eq!(decision.estimated_savings_micros, 0);
    }

    #[test]
    fn route_stats_observations_roll_up_latency_and_throughput() {
        clear_llm_gateway_route_stats();
        record_llm_gateway_route_observation(LlmGatewayRouteObservation {
            provider: "openrouter".to_string(),
            model: "openai/gpt-4.1".to_string(),
            route_class: "default".to_string(),
            latency_ms: 100,
            output_tokens: 50,
            success: true,
        });
        record_llm_gateway_route_observation(LlmGatewayRouteObservation {
            provider: "openrouter".to_string(),
            model: "gpt-4.1".to_string(),
            route_class: "default".to_string(),
            latency_ms: 300,
            output_tokens: 0,
            success: false,
        });

        let snapshot = llm_gateway_route_stats_snapshot();
        let stats = snapshot.iter().find(|stats| stats.model == "gpt-4.1").expect("route stats should be recorded");

        assert_eq!(stats.observation_count(), 2);
        assert_eq!(stats.average_latency_ms(), Some(200));
        assert_eq!(stats.error_rate_per_million(), Some(500_000));
        assert_eq!(stats.output_tokens_per_second_milli(), Some(125_000));
        clear_llm_gateway_route_stats();
    }

    #[test]
    fn control_plane_snapshot_normalizes_key_policies_and_falls_back_to_default_policy() {
        let key = "eden-gateway-test";
        let key_hash = LlmGatewayCredential::hash_api_key(key);
        let key_policy = LlmGatewayPolicy {
            price_arbitrage_mode: PriceArbitrageMode::AllowedModelsCheapest,
            ..LlmGatewayPolicy::default()
        };
        let default_policy = LlmGatewayPolicy {
            route_optimization_mode: LlmRouteOptimizationMode::Balanced,
            ..LlmGatewayPolicy::default()
        };
        let snapshot = LlmGatewayControlPlaneSnapshot {
            auth_mode: LlmGatewayControlPlaneAuthMode::Enforce,
            default_policy: Some(default_policy),
            key_policies: vec![
                LlmGatewayKeyPolicy {
                    key_hash: key_hash.to_ascii_uppercase(),
                    key_prefix: Some(" test ".to_string()),
                    org_uuid: None,
                    endpoint_uuid: None,
                    policy: key_policy,
                    enabled: true,
                },
                LlmGatewayKeyPolicy {
                    key_hash: "not-a-hash".to_string(),
                    key_prefix: None,
                    org_uuid: None,
                    endpoint_uuid: None,
                    policy: LlmGatewayPolicy::default(),
                    enabled: true,
                },
            ],
            ..LlmGatewayControlPlaneSnapshot::default()
        }
        .normalized();

        assert_eq!(snapshot.enabled_key_hashes(), BTreeSet::from([key_hash.clone()]));
        assert_eq!(
            snapshot.policy_for_key_hash(&key_hash).map(|policy| policy.price_arbitrage_mode),
            Some(PriceArbitrageMode::AllowedModelsCheapest)
        );
        assert_eq!(
            snapshot
                .policy_for_key_hash("0000000000000000000000000000000000000000000000000000000000000000")
                .map(|policy| policy.route_optimization_mode),
            Some(LlmRouteOptimizationMode::Balanced)
        );
    }

    fn observed_route_stats(
        provider: &str,
        model: &str,
        route_class: &str,
        success_count: u64,
        average_latency_ms: u64,
        output_tokens_per_request: u64,
    ) -> LlmGatewayRouteStats {
        LlmGatewayRouteStats {
            provider: provider.to_string(),
            model: model.to_string(),
            route_class: route_class.to_string(),
            success_count,
            error_count: 0,
            total_latency_ms: success_count.saturating_mul(average_latency_ms),
            min_latency_ms: average_latency_ms,
            max_latency_ms: average_latency_ms,
            total_output_tokens: success_count.saturating_mul(output_tokens_per_request),
            total_duration_ms: success_count.saturating_mul(average_latency_ms.max(1)),
            first_observed_unix_ms: 1,
            last_observed_unix_ms: 1,
        }
    }
}
