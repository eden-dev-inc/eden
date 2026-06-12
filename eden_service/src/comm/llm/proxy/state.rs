use crate::comm::llm::analytics_helpers::LlmAnalyticsContext;
use chrono::{DateTime, Datelike, Duration, Utc};
use dashmap::DashMap;
use eden_core::format::{EndpointUuid, OrganizationUuid};
use endpoint_core::llm_core::{
    CustomPiiMatcher, CustomPiiTerm, LlmCacheStatus, LlmGatewayControlPlaneAuthMode, LlmGatewayControlPlaneSnapshot, LlmGatewayKeyPolicy,
    LlmGatewayModelCatalog, LlmGatewayRouteStats, LlmKvCacheMode, LlmKvCacheStatus, LlmRouteOptimizationMode, OpenAiChatCompletionResponse,
    TrafficSource,
};
use serde::Serialize;
use std::collections::BTreeSet;
use std::sync::Arc;
use uuid::Uuid;

use super::auth::{ProxyBudgetWindow, ProxyRateLimitWindow};
use super::keys::ApiKey;

#[derive(Debug, Default)]
pub struct ProxyGatewayState {
    pub(super) keys_by_id: DashMap<Uuid, ApiKey>,
    pub(super) ids_by_hash: DashMap<String, Uuid>,
    pub(super) rate_limits: DashMap<Uuid, ProxyRateLimitWindow>,
    pub(super) budget_usage: DashMap<Uuid, ProxyBudgetWindow>,
    pub(super) response_cache: DashMap<String, ProxyResponseCacheEntry>,
    pub(super) conversation_routes: DashMap<String, ProxyConversationRouteEntry>,
    pub(super) route_stats: DashMap<String, ProxyRouteStats>,
    /// Compiled organization-wide PII dictionaries, keyed by org. Applied to
    /// every agent in the org; per-agent dictionaries add to these. Absent entry
    /// = no org dictionary.
    pub(super) org_pii_matchers: DashMap<Uuid, Arc<CustomPiiMatcher>>,
}

impl ProxyGatewayState {
    pub fn new() -> Self {
        Self::default()
    }

    /// The compiled organization-wide PII dictionary, if any.
    pub(super) fn org_pii_matcher(&self, org_uuid: Uuid) -> Option<Arc<CustomPiiMatcher>> {
        self.org_pii_matchers.get(&org_uuid).map(|entry| Arc::clone(entry.value()))
    }

    /// Compile and cache an organization's PII dictionary, replacing any prior
    /// one. An empty dictionary removes the entry.
    pub(super) fn set_org_pii_dictionary(&self, org_uuid: Uuid, terms: &[CustomPiiTerm]) {
        match CustomPiiMatcher::compile(terms) {
            Some(matcher) => {
                self.org_pii_matchers.insert(org_uuid, matcher);
            }
            None => {
                self.org_pii_matchers.remove(&org_uuid);
            }
        }
    }

    /// Compile and cache many organizations' dictionaries (startup hydration).
    pub(super) fn hydrate_org_pii_dictionaries(&self, dictionaries: impl IntoIterator<Item = (Uuid, Vec<CustomPiiTerm>)>) -> usize {
        let mut count = 0;
        for (org_uuid, terms) in dictionaries {
            self.set_org_pii_dictionary(org_uuid, &terms);
            count += 1;
        }
        count
    }

    pub(super) fn lookup_response_cache(
        &self,
        org_uuid: Uuid,
        cache_key: &str,
        now: DateTime<Utc>,
    ) -> Option<OpenAiChatCompletionResponse> {
        let entry = self.response_cache.get(cache_key)?;
        if entry.org_uuid != org_uuid {
            return None;
        }
        if entry.expires_at <= now {
            drop(entry);
            self.response_cache.remove(cache_key);
            return None;
        }
        Some(entry.response.clone())
    }

    pub(super) fn store_response_cache(
        &self,
        cache_key: String,
        org_uuid: Uuid,
        response: OpenAiChatCompletionResponse,
        ttl_secs: u64,
        now: DateTime<Utc>,
    ) {
        let ttl_secs = i64::try_from(ttl_secs.min(86_400)).unwrap_or(86_400);
        let expires_at = now + Duration::seconds(ttl_secs.max(1));
        self.response_cache.insert(cache_key, ProxyResponseCacheEntry { org_uuid, response, expires_at });
        self.prune_expired_response_cache(now);
    }

    fn prune_expired_response_cache(&self, now: DateTime<Utc>) {
        if self.response_cache.len() <= 10_000 {
            return;
        }
        let expired =
            self.response_cache.iter().filter(|entry| entry.expires_at <= now).map(|entry| entry.key().clone()).collect::<Vec<_>>();
        for key in expired {
            self.response_cache.remove(&key);
        }
    }

    pub(super) fn lookup_conversation_route(&self, route_key: &str, now: DateTime<Utc>) -> Option<ProxyConversationRouteEntry> {
        let mut entry = self.conversation_routes.get_mut(route_key)?;
        if entry.expires_at <= now {
            drop(entry);
            self.conversation_routes.remove(route_key);
            return None;
        }
        entry.last_used_at = now;
        Some(entry.clone())
    }

    pub(super) fn store_conversation_route(
        &self,
        route_key: String,
        org_uuid: Uuid,
        provider: String,
        model: String,
        ttl_secs: u64,
        now: DateTime<Utc>,
    ) {
        let ttl_secs = i64::try_from(ttl_secs.min(86_400)).unwrap_or(86_400);
        self.conversation_routes.insert(
            route_key,
            ProxyConversationRouteEntry {
                org_uuid,
                provider,
                model,
                last_used_at: now,
                expires_at: now + Duration::seconds(ttl_secs.max(1)),
            },
        );
        self.prune_expired_conversation_routes(now);
    }

    fn prune_expired_conversation_routes(&self, now: DateTime<Utc>) {
        if self.conversation_routes.len() <= 10_000 {
            return;
        }
        let expired = self
            .conversation_routes
            .iter()
            .filter(|entry| entry.expires_at <= now)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in expired {
            self.conversation_routes.remove(&key);
        }
    }

    pub(super) fn record_route_observation(
        &self,
        endpoint_uuid: Uuid,
        provider: &str,
        model: &str,
        latency_ms: u64,
        total_tokens: u32,
        now: DateTime<Utc>,
    ) -> ProxyRouteStatsSnapshot {
        let key = route_stats_key(endpoint_uuid, provider, model);
        let tokens_per_second = if latency_ms == 0 {
            f64::from(total_tokens)
        } else {
            f64::from(total_tokens) / (latency_ms as f64 / 1_000.0)
        };
        // Hold the entry across just the mutation + snapshot; drop before
        // prune. prune_stale_route_stats reads `route_stats.len()` which can
        // touch the same shard the entry guard is locking, and an aborted
        // request leaves the shard locked → next request to the same route
        // key deadlocks forever.
        let snapshot = {
            let mut entry = self.route_stats.entry(key).or_default();
            entry.record_observation(latency_ms, total_tokens, tokens_per_second, now);
            entry.snapshot()
        };
        self.prune_stale_route_stats(now);
        snapshot
    }

    pub(super) fn route_stats(&self, endpoint_uuid: Uuid, provider: &str, model: &str) -> Option<ProxyRouteStatsSnapshot> {
        self.route_stats.get(&route_stats_key(endpoint_uuid, provider, model)).map(|entry| entry.snapshot())
    }

    pub(super) fn hydrate_route_rollups<I>(&self, rollups: I) -> usize
    where
        I: IntoIterator<Item = ProxyRouteRollupSeed>,
    {
        let mut hydrated = 0;
        for rollup in rollups {
            if rollup.success_count == 0 {
                continue;
            }
            self.route_stats.insert(
                route_stats_key(rollup.endpoint_uuid, &rollup.provider, &rollup.model),
                ProxyRouteStats::from_seed(rollup),
            );
            hydrated += 1;
        }
        hydrated
    }

    pub(super) fn set_budget_usage(&self, key_id: Uuid, month_bucket: i32, used_tokens: u64) {
        self.budget_usage.insert(key_id, ProxyBudgetWindow { month_bucket, used_tokens });
    }

    pub fn runtime_summary(&self, org_uuid: Uuid, now: DateTime<Utc>) -> ProxyGatewayRuntimeSummary {
        let keys = self.keys_by_id.iter().filter(|entry| entry.org_uuid == org_uuid).map(|entry| entry.clone()).collect::<Vec<_>>();
        let active_api_key_count = keys.iter().filter(|key| key.enabled).count();
        let endpoint_uuids = keys.iter().map(|key| key.endpoint_uuid).collect::<BTreeSet<_>>();
        let key_ids = keys.iter().map(|key| key.id).collect::<BTreeSet<_>>();
        let month_bucket = current_budget_month_bucket(now);

        let response_cache_entries =
            self.response_cache.iter().filter(|entry| entry.org_uuid == org_uuid && entry.expires_at > now).count();
        let conversation_route_entries =
            self.conversation_routes.iter().filter(|entry| entry.org_uuid == org_uuid && entry.expires_at > now).count();
        let mut route_stats = self
            .route_stats
            .iter()
            .filter_map(|entry| {
                let (endpoint_uuid, provider, model) = parse_route_stats_key(entry.key())?;
                if !endpoint_uuids.contains(&endpoint_uuid) {
                    return None;
                }
                let snapshot = entry.snapshot();
                Some(ProxyGatewayRouteSummary {
                    endpoint_uuid: endpoint_uuid.to_string(),
                    provider,
                    model,
                    request_count: snapshot.request_count,
                    avg_latency_ms: snapshot.avg_latency_ms,
                    avg_tokens_per_second: snapshot.avg_tokens_per_second,
                    min_latency_ms: snapshot.min_latency_ms,
                    max_latency_ms: snapshot.max_latency_ms,
                    last_seen_at: snapshot.last_seen_at,
                })
            })
            .collect::<Vec<_>>();
        route_stats.sort_by(|left, right| right.last_seen_at.cmp(&left.last_seen_at));

        let mut budget_usage = self
            .budget_usage
            .iter()
            .filter(|entry| key_ids.contains(entry.key()) && entry.month_bucket == month_bucket)
            .map(|entry| ProxyGatewayBudgetSummary {
                key_id: entry.key().to_string(),
                month_bucket: entry.month_bucket,
                used_tokens: entry.used_tokens,
            })
            .collect::<Vec<_>>();
        budget_usage.sort_by(|left, right| right.used_tokens.cmp(&left.used_tokens).then_with(|| left.key_id.cmp(&right.key_id)));

        ProxyGatewayRuntimeSummary {
            api_key_count: keys.len(),
            active_api_key_count,
            in_memory_response_cache_entries: response_cache_entries,
            in_memory_conversation_route_entries: conversation_route_entries,
            in_memory_route_stat_entries: route_stats.len(),
            current_month_budget_windows: budget_usage.len(),
            current_month_budget_tokens_used: budget_usage.iter().map(|entry| entry.used_tokens).sum(),
            route_stats,
            budget_usage,
        }
    }

    pub(super) fn control_plane_snapshot(&self, org_uuid: Uuid) -> LlmGatewayControlPlaneSnapshot {
        self.control_plane_snapshot_matching(Some(org_uuid))
    }

    pub(super) fn control_plane_snapshot_all_orgs(&self) -> LlmGatewayControlPlaneSnapshot {
        self.control_plane_snapshot_matching(None)
    }

    fn control_plane_snapshot_matching(&self, org_uuid: Option<Uuid>) -> LlmGatewayControlPlaneSnapshot {
        let mut key_policies = self
            .keys_by_id
            .iter()
            .filter(|entry| org_uuid.is_none_or(|org_uuid| entry.org_uuid == org_uuid))
            .map(|entry| LlmGatewayKeyPolicy {
                key_hash: entry.key_hash.clone(),
                key_prefix: Some(entry.key_prefix.clone()),
                org_uuid: Some(entry.org_uuid.to_string()),
                endpoint_uuid: Some(entry.endpoint_uuid.to_string()),
                policy: entry.gateway_policy(),
                enabled: entry.enabled,
            })
            .collect::<Vec<_>>();
        key_policies.sort_by(|left, right| {
            left.org_uuid
                .cmp(&right.org_uuid)
                .then_with(|| left.endpoint_uuid.cmp(&right.endpoint_uuid))
                .then_with(|| left.key_prefix.cmp(&right.key_prefix))
        });

        let mut route_stats =
            self.route_stats.iter().filter_map(|entry| proxy_route_stats_to_gateway(entry.key(), entry.value())).collect::<Vec<_>>();
        route_stats.sort_by(|left, right| {
            left.provider
                .cmp(&right.provider)
                .then_with(|| left.model.cmp(&right.model))
                .then_with(|| left.route_class.cmp(&right.route_class))
        });

        LlmGatewayControlPlaneSnapshot {
            version: Utc::now().timestamp_millis().max(0) as u64,
            auth_mode: if key_policies.iter().any(|policy| policy.enabled) {
                LlmGatewayControlPlaneAuthMode::Enforce
            } else {
                LlmGatewayControlPlaneAuthMode::Disabled
            },
            default_policy: None,
            key_policies,
            model_catalog: Some(LlmGatewayModelCatalog::builtin()),
            route_stats,
            updated_at_unix_ms: Some(Utc::now().timestamp_millis()),
        }
    }

    fn prune_stale_route_stats(&self, now: DateTime<Utc>) {
        if self.route_stats.len() <= 50_000 {
            return;
        }
        let cutoff = now - Duration::days(7);
        let stale = self
            .route_stats
            .iter()
            .filter(|entry| entry.last_seen_at <= cutoff)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in stale {
            self.route_stats.remove(&key);
        }
    }
}

#[derive(Clone)]
pub struct ProxyAnalyticsContext {
    pub endpoint_uuid: EndpointUuid,
    pub organization_uuid: OrganizationUuid,
    pub credential_id: Option<String>,
    pub consumer_id: String,
    pub agent_uuid: Uuid,
    pub message_count: u32,
    pub prompt_fingerprint: Option<String>,
    pub request_bytes: u32,
    pub temperature: Option<f32>,
    pub max_tokens_requested: Option<u32>,
    pub requested_provider: Option<String>,
    pub requested_model: Option<String>,
    pub baseline_estimated_cost_micros: u64,
    pub selected_estimated_cost_micros: u64,
    pub estimated_arbitrage_savings_micros: u64,
    pub arbitrage_reason: Option<String>,
    pub price_source: Option<String>,
    pub cache_status: LlmCacheStatus,
    pub estimated_cache_savings_micros: u64,
    pub route_optimization_mode: LlmRouteOptimizationMode,
    pub kv_cache_mode: LlmKvCacheMode,
    pub kv_cache_status: LlmKvCacheStatus,
    pub estimated_kv_cache_savings_micros: u64,
    pub route_move_reason: Option<String>,
    pub conversation_route_key: Option<String>,
}

impl ProxyAnalyticsContext {
    pub fn to_common_context(&self) -> LlmAnalyticsContext {
        LlmAnalyticsContext {
            endpoint_uuid: self.endpoint_uuid.clone(),
            organization_uuid: self.organization_uuid.clone(),
            user_uuid: None,
            credential_id: self.credential_id.clone(),
            consumer_id: Some(self.consumer_id.clone()),
            agent_uuid: Some(self.agent_uuid),
            message_count: self.message_count,
            prompt_fingerprint: self.prompt_fingerprint.clone(),
            request_bytes: self.request_bytes,
            temperature: self.temperature,
            max_tokens_requested: self.max_tokens_requested,
            traffic_source: TrafficSource::ProxyApp,
            requested_provider: self.requested_provider.clone(),
            requested_model: self.requested_model.clone(),
            baseline_estimated_cost_micros: self.baseline_estimated_cost_micros,
            selected_estimated_cost_micros: self.selected_estimated_cost_micros,
            estimated_arbitrage_savings_micros: self.estimated_arbitrage_savings_micros,
            arbitrage_reason: self.arbitrage_reason.clone(),
            price_source: self.price_source.clone(),
            cache_status: self.cache_status,
            estimated_cache_savings_micros: self.estimated_cache_savings_micros,
            route_optimization_mode: self.route_optimization_mode,
            kv_cache_mode: self.kv_cache_mode,
            kv_cache_status: self.kv_cache_status,
            estimated_kv_cache_savings_micros: self.estimated_kv_cache_savings_micros,
            route_move_reason: self.route_move_reason.clone(),
            conversation_route_key: self.conversation_route_key.clone(),
        }
    }

    pub fn with_cache_status(&self, cache_status: LlmCacheStatus, estimated_cache_savings_micros: u64) -> Self {
        let mut next = self.clone();
        next.cache_status = cache_status;
        next.estimated_cache_savings_micros = estimated_cache_savings_micros;
        next
    }
}

pub(super) fn current_budget_month_bucket(now: DateTime<Utc>) -> i32 {
    now.year().saturating_mul(100).saturating_add(now.month() as i32)
}

#[derive(Debug, Clone)]
pub(super) struct ProxyResponseCacheEntry {
    pub org_uuid: Uuid,
    pub response: OpenAiChatCompletionResponse,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(super) struct ProxyConversationRouteEntry {
    pub org_uuid: Uuid,
    pub provider: String,
    pub model: String,
    pub last_used_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ProxyRouteStatsSnapshot {
    pub request_count: u64,
    pub avg_latency_ms: f64,
    pub avg_tokens_per_second: f64,
    pub total_latency_ms: u64,
    pub min_latency_ms: u64,
    pub max_latency_ms: u64,
    pub total_output_tokens: u64,
    pub total_duration_ms: u64,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(super) struct ProxyRouteStats {
    pub request_count: u64,
    pub avg_latency_ms: f64,
    pub avg_tokens_per_second: f64,
    pub total_latency_ms: u64,
    pub min_latency_ms: u64,
    pub max_latency_ms: u64,
    pub total_output_tokens: u64,
    pub total_duration_ms: u64,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
}

impl Default for ProxyRouteStats {
    fn default() -> Self {
        Self {
            request_count: 0,
            avg_latency_ms: 0.0,
            avg_tokens_per_second: 0.0,
            total_latency_ms: 0,
            min_latency_ms: 0,
            max_latency_ms: 0,
            total_output_tokens: 0,
            total_duration_ms: 0,
            first_seen_at: Utc::now(),
            last_seen_at: Utc::now(),
        }
    }
}

impl ProxyRouteStats {
    fn record_observation(&mut self, latency_ms: u64, total_tokens: u32, tokens_per_second: f64, now: DateTime<Utc>) {
        self.request_count = self.request_count.saturating_add(1);
        if self.request_count == 1 {
            self.first_seen_at = now;
            self.min_latency_ms = latency_ms;
            self.max_latency_ms = latency_ms;
        } else {
            self.min_latency_ms = self.min_latency_ms.min(latency_ms);
            self.max_latency_ms = self.max_latency_ms.max(latency_ms);
        }
        self.total_latency_ms = self.total_latency_ms.saturating_add(latency_ms);
        self.total_duration_ms = self.total_duration_ms.saturating_add(latency_ms.max(1));
        self.total_output_tokens = self.total_output_tokens.saturating_add(u64::from(total_tokens));
        let latency_ms = latency_ms as f64;
        let alpha = if self.request_count <= 5 {
            1.0 / self.request_count as f64
        } else {
            0.2
        };
        self.avg_latency_ms = smooth(self.avg_latency_ms, latency_ms, alpha);
        self.avg_tokens_per_second = smooth(self.avg_tokens_per_second, tokens_per_second, alpha);
        self.last_seen_at = now;
    }

    fn from_seed(seed: ProxyRouteRollupSeed) -> Self {
        let avg_latency_ms = if seed.success_count == 0 {
            0.0
        } else {
            seed.total_latency_ms as f64 / seed.success_count as f64
        };
        let avg_tokens_per_second = if seed.total_duration_ms == 0 {
            0.0
        } else {
            seed.total_output_tokens as f64 / (seed.total_duration_ms as f64 / 1_000.0)
        };
        Self {
            request_count: seed.success_count,
            avg_latency_ms,
            avg_tokens_per_second,
            total_latency_ms: seed.total_latency_ms,
            min_latency_ms: seed.min_latency_ms,
            max_latency_ms: seed.max_latency_ms,
            total_output_tokens: seed.total_output_tokens,
            total_duration_ms: seed.total_duration_ms,
            first_seen_at: seed.first_observed_at,
            last_seen_at: seed.last_observed_at,
        }
    }

    fn snapshot(&self) -> ProxyRouteStatsSnapshot {
        ProxyRouteStatsSnapshot {
            request_count: self.request_count,
            avg_latency_ms: self.avg_latency_ms,
            avg_tokens_per_second: self.avg_tokens_per_second,
            total_latency_ms: self.total_latency_ms,
            min_latency_ms: self.min_latency_ms,
            max_latency_ms: self.max_latency_ms,
            total_output_tokens: self.total_output_tokens,
            total_duration_ms: self.total_duration_ms,
            first_seen_at: self.first_seen_at,
            last_seen_at: self.last_seen_at,
        }
    }
}

fn smooth(current: f64, observed: f64, alpha: f64) -> f64 {
    if current <= 0.0 {
        observed
    } else {
        (current * (1.0 - alpha)) + (observed * alpha)
    }
}

fn route_stats_key(endpoint_uuid: Uuid, provider: &str, model: &str) -> String {
    format!("{endpoint_uuid}:{}:{}", provider.trim().to_ascii_lowercase(), model.trim().to_ascii_lowercase())
}

fn parse_route_stats_key(key: &str) -> Option<(Uuid, String, String)> {
    let mut parts = key.splitn(3, ':');
    let endpoint_uuid = Uuid::parse_str(parts.next()?).ok()?;
    let provider = parts.next()?.to_string();
    let model = parts.next()?.to_string();
    Some((endpoint_uuid, provider, model))
}

fn proxy_route_stats_to_gateway(key: &str, stats: &ProxyRouteStats) -> Option<LlmGatewayRouteStats> {
    let (_endpoint_uuid, provider, model) = parse_route_stats_key(key)?;
    let success_count = stats.request_count;
    if success_count == 0 {
        return None;
    }

    Some(LlmGatewayRouteStats {
        provider,
        model,
        route_class: "default".to_string(),
        success_count,
        error_count: 0,
        total_latency_ms: stats.total_latency_ms,
        min_latency_ms: stats.min_latency_ms,
        max_latency_ms: stats.max_latency_ms,
        total_output_tokens: stats.total_output_tokens,
        total_duration_ms: stats.total_duration_ms.max(1),
        first_observed_unix_ms: stats.first_seen_at.timestamp_millis(),
        last_observed_unix_ms: stats.last_seen_at.timestamp_millis(),
    })
}

#[derive(Debug, Clone)]
pub(super) struct ProxyRouteRollupSeed {
    pub endpoint_uuid: Uuid,
    pub provider: String,
    pub model: String,
    pub success_count: u64,
    pub total_latency_ms: u64,
    pub min_latency_ms: u64,
    pub max_latency_ms: u64,
    pub total_output_tokens: u64,
    pub total_duration_ms: u64,
    pub first_observed_at: DateTime<Utc>,
    pub last_observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ProxyGatewayRuntimeSummary {
    pub api_key_count: usize,
    pub active_api_key_count: usize,
    pub in_memory_response_cache_entries: usize,
    pub in_memory_conversation_route_entries: usize,
    pub in_memory_route_stat_entries: usize,
    pub current_month_budget_windows: usize,
    pub current_month_budget_tokens_used: u64,
    pub route_stats: Vec<ProxyGatewayRouteSummary>,
    pub budget_usage: Vec<ProxyGatewayBudgetSummary>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ProxyGatewayRouteSummary {
    pub endpoint_uuid: String,
    pub provider: String,
    pub model: String,
    pub request_count: u64,
    pub avg_latency_ms: f64,
    pub avg_tokens_per_second: f64,
    pub min_latency_ms: u64,
    pub max_latency_ms: u64,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ProxyGatewayBudgetSummary {
    pub key_id: String,
    pub month_bucket: i32,
    pub used_tokens: u64,
}
