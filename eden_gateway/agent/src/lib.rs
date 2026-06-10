//! Agent gateway connection registry and route selection.
//!
//! This module is intentionally transport-neutral. HTTP, WebSocket, A2A, MCP,
//! and future wire transports can all register the same session shape, while
//! callers use `route_to_agent` to find the current best network path.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_SESSION_TTL_SECS: u64 = 60;
const DEFAULT_MAX_CONNECTIONS_PER_AGENT: usize = 16;
const DEFAULT_USAGE_WINDOW_MS: u64 = 60_000;
const MAX_URL_LEN: usize = 2_048;
const MAX_ID_LEN: usize = 256;
const MAX_TAGS: usize = 32;
const MAX_TAG_LEN: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentGatewayTransport {
    InProcess,
    A2aHttp,
    Http,
    WebSocket,
    Mcp,
}

impl AgentGatewayTransport {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InProcess => "in_process",
            Self::A2aHttp => "a2a_http",
            Self::Http => "http",
            Self::WebSocket => "websocket",
            Self::Mcp => "mcp",
        }
    }
}

impl std::fmt::Display for AgentGatewayTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentConnectionStatus {
    Active,
    Draining,
}

impl AgentConnectionStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Draining => "draining",
        }
    }
}

impl std::fmt::Display for AgentConnectionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentGatewayNetworkEndpoint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertise_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AgentGatewayIdentity {
    #[serde(default)]
    pub fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

impl AgentGatewayIdentity {
    fn normalized(mut self, registration: &AgentConnectionRegistration) -> Result<Self, AgentGatewayError> {
        self.fingerprint = Self::normalize_optional_id(Some(self.fingerprint)).unwrap_or_else(|| Self::derived_fingerprint(registration));
        self.instance_id = Self::normalize_optional_id(self.instance_id);
        self.principal = Self::normalize_optional_id(self.principal);
        self.tags = Self::normalize_tags(self.tags)?;
        Ok(self)
    }

    fn derived_fingerprint(registration: &AgentConnectionRegistration) -> String {
        let endpoint = &registration.endpoint;
        let mut input = String::new();
        input.push_str(registration.org_id.trim());
        input.push('\n');
        input.push_str(registration.agent_id.trim());
        input.push('\n');
        input.push_str(registration.transport.as_str());
        input.push('\n');
        input.push_str(endpoint.node_id.as_deref().unwrap_or_default().trim());
        input.push('\n');
        input.push_str(endpoint.region.as_deref().unwrap_or_default().trim());
        input.push('\n');
        input.push_str(endpoint.callback_url.as_deref().unwrap_or_default().trim());
        input.push('\n');
        input.push_str(endpoint.advertise_url.as_deref().unwrap_or_default().trim());

        let digest = Sha256::digest(input.as_bytes());
        let mut fingerprint = String::with_capacity(70);
        fingerprint.push_str("sha256:");
        Self::append_hex(&mut fingerprint, &digest);
        fingerprint
    }

    fn normalize_optional_id(value: Option<String>) -> Option<String> {
        value.map(|value| value.trim().to_string()).filter(|value| !value.is_empty())
    }

    fn normalize_tags(tags: BTreeMap<String, String>) -> Result<BTreeMap<String, String>, AgentGatewayError> {
        if tags.len() > MAX_TAGS {
            return Err(AgentGatewayError::TooManyAgentTags);
        }

        let mut normalized = BTreeMap::new();
        for (key, value) in tags {
            let key = key.trim();
            let value = value.trim();
            if key.is_empty() || value.is_empty() {
                continue;
            }
            if key.len() > MAX_TAG_LEN || value.len() > MAX_TAG_LEN {
                return Err(AgentGatewayError::AgentIdentityTooLong);
            }
            normalized.insert(key.to_string(), value.to_string());
        }
        Ok(normalized)
    }

    fn append_hex(output: &mut String, bytes: &[u8]) {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in bytes {
            output.push(HEX[(byte >> 4) as usize] as char);
            output.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }

    fn validate(&self) -> Result<(), AgentGatewayError> {
        if self.fingerprint.len() > MAX_ID_LEN
            || self.instance_id.as_ref().is_some_and(|value| value.len() > MAX_ID_LEN)
            || self.principal.as_ref().is_some_and(|value| value.len() > MAX_ID_LEN)
        {
            return Err(AgentGatewayError::AgentIdentityTooLong);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentConnectionMetrics {
    #[serde(default)]
    pub active_streams: u32,
    #[serde(default)]
    pub queued_messages: u32,
    #[serde(default)]
    pub avg_latency_ms: Option<u64>,
    #[serde(default)]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AgentGatewayRateLimit {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requests_per_minute: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_tokens_per_minute: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_tokens_per_minute: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tokens_per_minute: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_active_streams: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_queued_messages: Option<u32>,
}

impl AgentGatewayRateLimit {
    fn with_defaults(mut self, defaults: &Self) -> Self {
        self.requests_per_minute = self.requests_per_minute.or(defaults.requests_per_minute);
        self.prompt_tokens_per_minute = self.prompt_tokens_per_minute.or(defaults.prompt_tokens_per_minute);
        self.completion_tokens_per_minute = self.completion_tokens_per_minute.or(defaults.completion_tokens_per_minute);
        self.total_tokens_per_minute = self.total_tokens_per_minute.or(defaults.total_tokens_per_minute);
        self.max_active_streams = self.max_active_streams.or(defaults.max_active_streams);
        self.max_queued_messages = self.max_queued_messages.or(defaults.max_queued_messages);
        self
    }

    fn accepts_metrics(&self, metrics: &AgentConnectionMetrics) -> bool {
        self.max_active_streams.is_none_or(|limit| metrics.active_streams < limit)
            && self.max_queued_messages.is_none_or(|limit| metrics.queued_messages < limit)
    }

    fn check_usage(&self, window: &AgentGatewayUsageSnapshot, event: &AgentGatewayUsageEvent) -> Option<AgentGatewayRateLimitReason> {
        let requests = event.effective_request_count();
        if self.requests_per_minute.is_some_and(|limit| window.requests.saturating_add(requests) > limit) {
            return Some(AgentGatewayRateLimitReason::RequestsPerMinute);
        }
        if self.prompt_tokens_per_minute.is_some_and(|limit| window.prompt_tokens.saturating_add(event.prompt_tokens) > limit) {
            return Some(AgentGatewayRateLimitReason::PromptTokensPerMinute);
        }
        if self
            .completion_tokens_per_minute
            .is_some_and(|limit| window.completion_tokens.saturating_add(event.completion_tokens) > limit)
        {
            return Some(AgentGatewayRateLimitReason::CompletionTokensPerMinute);
        }
        if self
            .total_tokens_per_minute
            .is_some_and(|limit| window.total_tokens.saturating_add(event.effective_total_tokens()) > limit)
        {
            return Some(AgentGatewayRateLimitReason::TotalTokensPerMinute);
        }
        None
    }

    fn remaining_requests(&self, window: &AgentGatewayUsageSnapshot) -> Option<u64> {
        self.requests_per_minute.map(|limit| limit.saturating_sub(window.requests))
    }

    fn remaining_prompt_tokens(&self, window: &AgentGatewayUsageSnapshot) -> Option<u64> {
        self.prompt_tokens_per_minute.map(|limit| limit.saturating_sub(window.prompt_tokens))
    }

    fn remaining_completion_tokens(&self, window: &AgentGatewayUsageSnapshot) -> Option<u64> {
        self.completion_tokens_per_minute.map(|limit| limit.saturating_sub(window.completion_tokens))
    }

    fn remaining_total_tokens(&self, window: &AgentGatewayUsageSnapshot) -> Option<u64> {
        self.total_tokens_per_minute.map(|limit| limit.saturating_sub(window.total_tokens))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentGatewayRateLimitReason {
    RequestsPerMinute,
    PromptTokensPerMinute,
    CompletionTokensPerMinute,
    TotalTokensPerMinute,
}

impl AgentGatewayRateLimitReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RequestsPerMinute => "requests_per_minute",
            Self::PromptTokensPerMinute => "prompt_tokens_per_minute",
            Self::CompletionTokensPerMinute => "completion_tokens_per_minute",
            Self::TotalTokensPerMinute => "total_tokens_per_minute",
        }
    }
}

impl std::fmt::Display for AgentGatewayRateLimitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentGatewayUsageEvent {
    #[serde(default)]
    pub request_count: u64,
    #[serde(default)]
    pub prompt_tokens: u64,
    #[serde(default)]
    pub completion_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub cost_microdollars: u64,
}

impl AgentGatewayUsageEvent {
    fn effective_request_count(&self) -> u64 {
        self.request_count.max(1)
    }

    fn effective_total_tokens(&self) -> u64 {
        if self.total_tokens > 0 {
            self.total_tokens
        } else {
            self.prompt_tokens.saturating_add(self.completion_tokens)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentGatewayUsageSnapshot {
    pub org_id: String,
    pub agent_id: String,
    pub fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    pub window_started_ms: u64,
    pub window_ends_ms: u64,
    pub requests: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub cost_microdollars: u64,
    pub rate_limited_requests: u64,
    pub last_seen_ms: u64,
}

impl AgentGatewayUsageSnapshot {
    fn new(session: &AgentConnectionSession, now_ms: u64, window_ms: u64) -> Self {
        Self {
            org_id: session.org_id.clone(),
            agent_id: session.agent_id.clone(),
            fingerprint: session.identity.fingerprint.clone(),
            instance_id: session.identity.instance_id.clone(),
            principal: session.identity.principal.clone(),
            tags: session.identity.tags.clone(),
            window_started_ms: now_ms,
            window_ends_ms: now_ms.saturating_add(window_ms),
            requests: 0,
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
            cost_microdollars: 0,
            rate_limited_requests: 0,
            last_seen_ms: now_ms,
        }
    }

    fn reset_window(&mut self, now_ms: u64, window_ms: u64) {
        self.window_started_ms = now_ms;
        self.window_ends_ms = now_ms.saturating_add(window_ms);
        self.requests = 0;
        self.prompt_tokens = 0;
        self.completion_tokens = 0;
        self.total_tokens = 0;
        self.cost_microdollars = 0;
        self.rate_limited_requests = 0;
        self.last_seen_ms = now_ms;
    }

    fn refresh_identity(&mut self, session: &AgentConnectionSession) {
        self.instance_id = session.identity.instance_id.clone();
        self.principal = session.identity.principal.clone();
        self.tags = session.identity.tags.clone();
    }

    fn record_allowed(&mut self, event: &AgentGatewayUsageEvent, now_ms: u64) {
        self.requests = self.requests.saturating_add(event.effective_request_count());
        self.prompt_tokens = self.prompt_tokens.saturating_add(event.prompt_tokens);
        self.completion_tokens = self.completion_tokens.saturating_add(event.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(event.effective_total_tokens());
        self.cost_microdollars = self.cost_microdollars.saturating_add(event.cost_microdollars);
        self.last_seen_ms = now_ms;
    }

    fn record_limited(&mut self, event: &AgentGatewayUsageEvent, now_ms: u64) {
        self.rate_limited_requests = self.rate_limited_requests.saturating_add(event.effective_request_count());
        self.last_seen_ms = now_ms;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentGatewayRateLimitDecision {
    pub allowed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<AgentGatewayRateLimitReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining_requests: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining_prompt_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining_completion_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining_total_tokens: Option<u64>,
    pub usage: AgentGatewayUsageSnapshot,
}

impl AgentGatewayRateLimitDecision {
    fn from_window(
        allowed: bool,
        reason: Option<AgentGatewayRateLimitReason>,
        limit: &AgentGatewayRateLimit,
        usage: AgentGatewayUsageSnapshot,
    ) -> Self {
        let retry_after_ms = reason.map(|_| usage.window_ends_ms.saturating_sub(usage.last_seen_ms));
        Self {
            allowed,
            reason,
            retry_after_ms,
            remaining_requests: limit.remaining_requests(&usage),
            remaining_prompt_tokens: limit.remaining_prompt_tokens(&usage),
            remaining_completion_tokens: limit.remaining_completion_tokens(&usage),
            remaining_total_tokens: limit.remaining_total_tokens(&usage),
            usage,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConnectionRegistration {
    pub org_id: String,
    pub agent_id: String,
    pub transport: AgentGatewayTransport,
    #[serde(default)]
    pub identity: AgentGatewayIdentity,
    #[serde(default)]
    pub endpoint: AgentGatewayNetworkEndpoint,
    #[serde(default)]
    pub metrics: AgentConnectionMetrics,
    #[serde(default)]
    pub rate_limit: AgentGatewayRateLimit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConnectionSession {
    pub session_id: String,
    pub org_id: String,
    pub agent_id: String,
    pub identity: AgentGatewayIdentity,
    pub transport: AgentGatewayTransport,
    pub endpoint: AgentGatewayNetworkEndpoint,
    pub metrics: AgentConnectionMetrics,
    pub rate_limit: AgentGatewayRateLimit,
    pub status: AgentConnectionStatus,
    pub connected_at_ms: u64,
    pub last_heartbeat_ms: u64,
    pub expires_at_ms: u64,
}

impl AgentConnectionSession {
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.expires_at_ms <= now_ms
    }

    pub fn is_routable(&self, now_ms: u64) -> bool {
        self.status == AgentConnectionStatus::Active && !self.is_expired(now_ms) && self.rate_limit.accepts_metrics(&self.metrics)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentGatewayRoute {
    pub org_id: String,
    pub agent_id: String,
    pub identity: AgentGatewayIdentity,
    pub session_id: String,
    pub transport: AgentGatewayTransport,
    pub endpoint: AgentGatewayNetworkEndpoint,
    pub rate_limit: AgentGatewayRateLimit,
    pub active_streams: u32,
    pub queued_messages: u32,
    pub last_heartbeat_ms: u64,
}

#[derive(Debug, Clone)]
pub struct AgentGatewayConfig {
    pub session_ttl_secs: u64,
    pub max_connections_per_agent: usize,
    pub usage_window_ms: u64,
    pub default_rate_limit: AgentGatewayRateLimit,
    pub network_policy: AgentGatewayNetworkPolicy,
}

impl Default for AgentGatewayConfig {
    fn default() -> Self {
        Self {
            session_ttl_secs: DEFAULT_SESSION_TTL_SECS,
            max_connections_per_agent: DEFAULT_MAX_CONNECTIONS_PER_AGENT,
            usage_window_ms: DEFAULT_USAGE_WINDOW_MS,
            default_rate_limit: AgentGatewayRateLimit::default(),
            network_policy: AgentGatewayNetworkPolicy::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AgentGatewayNetworkPolicy {
    pub allowed_transports: Vec<AgentGatewayTransport>,
    pub allow_remote_urls: bool,
}

impl AgentGatewayNetworkPolicy {
    fn validate_url(&self, url: Option<&str>) -> Result<(), AgentGatewayError> {
        let Some(url) = url.map(str::trim).filter(|url| !url.is_empty()) else {
            return Ok(());
        };
        if url.len() > MAX_URL_LEN {
            return Err(AgentGatewayError::UrlTooLong);
        }
        if !self.allow_remote_urls && !Self::is_local_url(url) {
            return Err(AgentGatewayError::RemoteUrlNotAllowed);
        }
        Ok(())
    }

    fn is_local_url(url: &str) -> bool {
        let lower = url.to_ascii_lowercase();
        lower.starts_with("/")
            || lower.starts_with("http://127.")
            || lower.starts_with("http://localhost")
            || lower.starts_with("http://[::1]")
            || lower.starts_with("ws://127.")
            || lower.starts_with("ws://localhost")
            || lower.starts_with("ws://[::1]")
    }
}

impl Default for AgentGatewayNetworkPolicy {
    fn default() -> Self {
        Self {
            allowed_transports: vec![
                AgentGatewayTransport::InProcess,
                AgentGatewayTransport::A2aHttp,
                AgentGatewayTransport::Http,
                AgentGatewayTransport::WebSocket,
                AgentGatewayTransport::Mcp,
            ],
            allow_remote_urls: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentGatewayError {
    MissingOrgId,
    MissingAgentId,
    AgentIdentityTooLong,
    TooManyAgentTags,
    TransportNotAllowed,
    RemoteUrlNotAllowed,
    UrlTooLong,
    TooManyConnections,
    SessionNotFound,
    OrgMismatch,
}

impl std::fmt::Display for AgentGatewayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingOrgId => f.write_str("org_id must not be empty"),
            Self::MissingAgentId => f.write_str("agent_id must not be empty"),
            Self::AgentIdentityTooLong => f.write_str("agent gateway identity values are too long"),
            Self::TooManyAgentTags => f.write_str("too many agent gateway identity tags"),
            Self::TransportNotAllowed => f.write_str("agent gateway transport is not allowed"),
            Self::RemoteUrlNotAllowed => f.write_str("remote agent gateway URLs are not allowed by policy"),
            Self::UrlTooLong => f.write_str("agent gateway URL is too long"),
            Self::TooManyConnections => f.write_str("too many active agent gateway connections"),
            Self::SessionNotFound => f.write_str("agent gateway session not found"),
            Self::OrgMismatch => f.write_str("agent gateway session belongs to a different organization"),
        }
    }
}

impl std::error::Error for AgentGatewayError {}

#[derive(Debug)]
pub struct AgentGatewayState {
    config: AgentGatewayConfig,
    sessions: DashMap<String, AgentConnectionSession>,
    routes: DashMap<String, String>,
    usage_windows: DashMap<String, AgentGatewayUsageSnapshot>,
    next_session: AtomicU64,
}

impl Default for AgentGatewayState {
    fn default() -> Self {
        Self::new(AgentGatewayConfig::default())
    }
}

impl AgentGatewayState {
    pub fn new(config: AgentGatewayConfig) -> Self {
        Self {
            config,
            sessions: DashMap::new(),
            routes: DashMap::new(),
            usage_windows: DashMap::new(),
            next_session: AtomicU64::new(1),
        }
    }

    pub fn register_connection(&self, registration: AgentConnectionRegistration) -> Result<AgentConnectionSession, AgentGatewayError> {
        let now_ms = Self::now_millis();
        self.prune_expired(now_ms);
        self.validate_registration(&registration)?;
        let identity = registration.identity.clone().normalized(&registration)?;
        identity.validate()?;

        let active_count = self
            .sessions
            .iter()
            .filter(|entry| entry.org_id == registration.org_id && entry.agent_id == registration.agent_id && entry.is_routable(now_ms))
            .count();
        if self.config.max_connections_per_agent > 0 && active_count >= self.config.max_connections_per_agent {
            return Err(AgentGatewayError::TooManyConnections);
        }

        let session_id = self.next_session_id(now_ms);
        let ttl_ms = self.config.session_ttl_secs.saturating_mul(1_000).max(1_000);
        let session = AgentConnectionSession {
            session_id: session_id.clone(),
            org_id: registration.org_id,
            agent_id: registration.agent_id,
            identity,
            transport: registration.transport,
            endpoint: registration.endpoint,
            metrics: registration.metrics,
            rate_limit: registration.rate_limit.with_defaults(&self.config.default_rate_limit),
            status: AgentConnectionStatus::Active,
            connected_at_ms: now_ms,
            last_heartbeat_ms: now_ms,
            expires_at_ms: now_ms.saturating_add(ttl_ms),
        };

        let route_key = Self::route_key(&session.org_id, &session.agent_id);
        self.sessions.insert(session_id.clone(), session.clone());
        self.routes.remove(&route_key);
        Ok(session)
    }

    pub fn heartbeat(
        &self,
        org_id: &str,
        session_id: &str,
        metrics: AgentConnectionMetrics,
    ) -> Result<AgentConnectionSession, AgentGatewayError> {
        let now_ms = Self::now_millis();
        let Some(mut session) = self.sessions.get_mut(session_id) else {
            return Err(AgentGatewayError::SessionNotFound);
        };
        if session.org_id != org_id {
            return Err(AgentGatewayError::OrgMismatch);
        }

        session.metrics = metrics;
        session.status = AgentConnectionStatus::Active;
        session.last_heartbeat_ms = now_ms;
        session.expires_at_ms = now_ms.saturating_add(self.config.session_ttl_secs.saturating_mul(1_000).max(1_000));
        let route_key = Self::route_key(&session.org_id, &session.agent_id);
        let updated = session.clone();
        drop(session);

        self.routes.remove(&route_key);
        Ok(updated)
    }

    pub fn mark_draining(&self, org_id: &str, session_id: &str) -> Result<AgentConnectionSession, AgentGatewayError> {
        let Some(mut session) = self.sessions.get_mut(session_id) else {
            return Err(AgentGatewayError::SessionNotFound);
        };
        if session.org_id != org_id {
            return Err(AgentGatewayError::OrgMismatch);
        }
        session.status = AgentConnectionStatus::Draining;
        Ok(session.clone())
    }

    pub fn disconnect(&self, org_id: &str, session_id: &str) -> Result<AgentConnectionSession, AgentGatewayError> {
        let Some((_, session)) = self.sessions.remove(session_id) else {
            return Err(AgentGatewayError::SessionNotFound);
        };
        if session.org_id != org_id {
            self.sessions.insert(session.session_id.clone(), session);
            return Err(AgentGatewayError::OrgMismatch);
        }

        let key = Self::route_key(&session.org_id, &session.agent_id);
        if self.routes.get(&key).is_some_and(|active| active.as_str() == session_id) {
            self.routes.remove(&key);
        }
        Ok(session)
    }

    pub fn route_to_agent(&self, org_id: &str, agent_id: &str) -> Option<AgentGatewayRoute> {
        let now_ms = Self::now_millis();
        let key = Self::route_key(org_id, agent_id);

        if let Some(session_id) = self.routes.get(&key).map(|entry| entry.value().clone()) {
            if let Some(route) = self.route_from_cached_session(org_id, agent_id, &session_id, now_ms) {
                return Some(route);
            }
            self.routes.remove_if(&key, |_, active| active.as_str() == session_id);
        }

        self.prune_expired(now_ms);
        let best = self
            .sessions
            .iter()
            .filter(|entry| entry.org_id == org_id && entry.agent_id == agent_id && entry.is_routable(now_ms))
            .min_by(|left, right| {
                left.metrics
                    .active_streams
                    .cmp(&right.metrics.active_streams)
                    .then_with(|| left.metrics.queued_messages.cmp(&right.metrics.queued_messages))
                    .then_with(|| right.last_heartbeat_ms.cmp(&left.last_heartbeat_ms))
            })
            .map(|entry| entry.value().clone())?;

        self.routes.insert(key, best.session_id.clone());
        Some(Self::route_from_session(&best))
    }

    fn route_from_cached_session(&self, org_id: &str, agent_id: &str, session_id: &str, now_ms: u64) -> Option<AgentGatewayRoute> {
        let session = self.sessions.get(session_id)?;
        if session.org_id != org_id || session.agent_id != agent_id || !session.is_routable(now_ms) {
            return None;
        }
        Some(Self::route_from_session(session.value()))
    }

    pub fn list_connections(&self, org_id: &str) -> Vec<AgentConnectionSession> {
        let now_ms = Self::now_millis();
        self.prune_expired(now_ms);
        let mut sessions =
            self.sessions.iter().filter(|entry| entry.org_id == org_id).map(|entry| entry.value().clone()).collect::<Vec<_>>();
        sessions.sort_by(|left, right| {
            left.agent_id
                .cmp(&right.agent_id)
                .then_with(|| right.last_heartbeat_ms.cmp(&left.last_heartbeat_ms))
                .then_with(|| left.session_id.cmp(&right.session_id))
        });
        sessions
    }

    pub fn record_usage(
        &self,
        org_id: &str,
        session_id: &str,
        event: AgentGatewayUsageEvent,
    ) -> Result<AgentGatewayRateLimitDecision, AgentGatewayError> {
        let now_ms = Self::now_millis();
        self.prune_expired(now_ms);
        let Some(session) = self.sessions.get(session_id).map(|entry| entry.value().clone()) else {
            return Err(AgentGatewayError::SessionNotFound);
        };
        if session.org_id != org_id {
            return Err(AgentGatewayError::OrgMismatch);
        }

        let window_ms = self.config.usage_window_ms.max(1_000);
        let key = Self::usage_key(&session.org_id, &session.agent_id, &session.identity.fingerprint);
        let mut window = self.usage_windows.entry(key).or_insert_with(|| AgentGatewayUsageSnapshot::new(&session, now_ms, window_ms));
        if window.window_ends_ms <= now_ms {
            window.reset_window(now_ms, window_ms);
        }
        window.refresh_identity(&session);

        let reason = session.rate_limit.check_usage(&window, &event);
        if reason.is_some() {
            window.record_limited(&event, now_ms);
            return Ok(AgentGatewayRateLimitDecision::from_window(false, reason, &session.rate_limit, window.clone()));
        }

        window.record_allowed(&event, now_ms);
        Ok(AgentGatewayRateLimitDecision::from_window(true, None, &session.rate_limit, window.clone()))
    }

    pub fn list_usage(&self, org_id: &str) -> Vec<AgentGatewayUsageSnapshot> {
        let now_ms = Self::now_millis();
        self.prune_expired(now_ms);
        let mut usage =
            self.usage_windows.iter().filter(|entry| entry.org_id == org_id).map(|entry| entry.value().clone()).collect::<Vec<_>>();
        usage.sort_by(|left, right| {
            left.agent_id
                .cmp(&right.agent_id)
                .then_with(|| left.fingerprint.cmp(&right.fingerprint))
                .then_with(|| right.last_seen_ms.cmp(&left.last_seen_ms))
        });
        usage
    }

    pub fn prune_expired(&self, now_ms: u64) {
        let expired = self
            .sessions
            .iter()
            .filter(|entry| entry.is_expired(now_ms))
            .map(|entry| (entry.session_id.clone(), Self::route_key(&entry.org_id, &entry.agent_id)))
            .collect::<Vec<_>>();

        for (session_id, key) in expired {
            self.sessions.remove(&session_id);
            if self.routes.get(&key).is_some_and(|active| active.as_str() == session_id) {
                self.routes.remove(&key);
            }
        }
    }

    fn validate_registration(&self, registration: &AgentConnectionRegistration) -> Result<(), AgentGatewayError> {
        if registration.org_id.trim().is_empty() {
            return Err(AgentGatewayError::MissingOrgId);
        }
        if registration.agent_id.trim().is_empty() {
            return Err(AgentGatewayError::MissingAgentId);
        }
        if !self.config.network_policy.allowed_transports.contains(&registration.transport) {
            return Err(AgentGatewayError::TransportNotAllowed);
        }
        self.config.network_policy.validate_url(registration.endpoint.advertise_url.as_deref())?;
        self.config.network_policy.validate_url(registration.endpoint.callback_url.as_deref())?;
        Ok(())
    }

    fn next_session_id(&self, now_ms: u64) -> String {
        let sequence = self.next_session.fetch_add(1, Ordering::Relaxed);
        format!("ags_{now_ms:x}_{sequence:x}")
    }

    fn route_from_session(session: &AgentConnectionSession) -> AgentGatewayRoute {
        AgentGatewayRoute {
            org_id: session.org_id.clone(),
            agent_id: session.agent_id.clone(),
            identity: session.identity.clone(),
            session_id: session.session_id.clone(),
            transport: session.transport,
            endpoint: session.endpoint.clone(),
            rate_limit: session.rate_limit.clone(),
            active_streams: session.metrics.active_streams,
            queued_messages: session.metrics.queued_messages,
            last_heartbeat_ms: session.last_heartbeat_ms,
        }
    }

    fn route_key(org_id: &str, agent_id: &str) -> String {
        format!("{}:{}", org_id.trim(), agent_id.trim())
    }

    fn usage_key(org_id: &str, agent_id: &str, fingerprint: &str) -> String {
        format!("{}:{}:{}", org_id.trim(), agent_id.trim(), fingerprint.trim())
    }

    fn now_millis() -> u64 {
        let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registration(agent_id: &str) -> AgentConnectionRegistration {
        AgentConnectionRegistration {
            org_id: "org-1".to_string(),
            agent_id: agent_id.to_string(),
            transport: AgentGatewayTransport::A2aHttp,
            endpoint: AgentGatewayNetworkEndpoint {
                callback_url: Some("https://agent.example.com/a2a".to_string()),
                ..Default::default()
            },
            metrics: AgentConnectionMetrics::default(),
            identity: AgentGatewayIdentity::default(),
            rate_limit: AgentGatewayRateLimit::default(),
        }
    }

    fn cached_session_id(gateway: &AgentGatewayState, org_id: &str, agent_id: &str) -> Option<String> {
        let key = AgentGatewayState::route_key(org_id, agent_id);
        gateway.routes.get(&key).map(|entry| entry.value().clone())
    }

    #[test]
    fn registers_and_routes_agent_connection() {
        let gateway = AgentGatewayState::default();
        let session = gateway.register_connection(registration("agent-1")).expect("registration should succeed");

        let route = gateway.route_to_agent("org-1", "agent-1").expect("route should exist");
        assert_eq!(route.session_id, session.session_id);
        assert_eq!(route.transport, AgentGatewayTransport::A2aHttp);
        assert_eq!(route.identity.fingerprint, session.identity.fingerprint);
    }

    #[test]
    fn heartbeat_updates_metrics() {
        let gateway = AgentGatewayState::default();
        let session = gateway.register_connection(registration("agent-1")).expect("registration should succeed");
        let updated = gateway
            .heartbeat(
                "org-1",
                &session.session_id,
                AgentConnectionMetrics {
                    active_streams: 3,
                    queued_messages: 1,
                    avg_latency_ms: Some(42),
                    last_error: None,
                },
            )
            .expect("heartbeat should succeed");

        assert_eq!(updated.metrics.active_streams, 3);
        let route = gateway.route_to_agent("org-1", "agent-1").expect("route should exist");
        assert_eq!(route.active_streams, 3);
    }

    #[test]
    fn route_uses_least_loaded_session() {
        let gateway = AgentGatewayState::default();
        let busy = gateway.register_connection(registration("agent-1")).expect("busy registration should succeed");
        let quiet = gateway.register_connection(registration("agent-1")).expect("quiet registration should succeed");

        let _ = gateway.heartbeat(
            "org-1",
            &busy.session_id,
            AgentConnectionMetrics {
                active_streams: 10,
                queued_messages: 4,
                avg_latency_ms: None,
                last_error: None,
            },
        );
        let _ = gateway.heartbeat("org-1", &quiet.session_id, AgentConnectionMetrics::default());

        let route = gateway.route_to_agent("org-1", "agent-1").expect("route should exist");
        assert_eq!(route.session_id, quiet.session_id);
    }

    #[test]
    fn route_uses_cached_session_when_routable() {
        let gateway = AgentGatewayState::default();
        let busy = gateway.register_connection(registration("agent-1")).expect("busy registration should succeed");
        let quiet = gateway.register_connection(registration("agent-1")).expect("quiet registration should succeed");
        gateway
            .heartbeat(
                "org-1",
                &busy.session_id,
                AgentConnectionMetrics {
                    active_streams: 5,
                    queued_messages: 2,
                    avg_latency_ms: None,
                    last_error: None,
                },
            )
            .expect("busy heartbeat should succeed");
        gateway
            .heartbeat("org-1", &quiet.session_id, AgentConnectionMetrics::default())
            .expect("quiet heartbeat should succeed");

        let route = gateway.route_to_agent("org-1", "agent-1").expect("route should exist");
        assert_eq!(route.session_id, quiet.session_id);
        assert_eq!(cached_session_id(&gateway, "org-1", "agent-1").as_deref(), Some(quiet.session_id.as_str()));

        let cached_route = gateway.route_to_agent("org-1", "agent-1").expect("cached route should exist");
        assert_eq!(cached_route.session_id, quiet.session_id);
    }

    #[test]
    fn register_connection_invalidates_cached_route_for_new_capacity() {
        let gateway = AgentGatewayState::default();
        let first = gateway.register_connection(registration("agent-1")).expect("first registration should succeed");
        gateway
            .heartbeat(
                "org-1",
                &first.session_id,
                AgentConnectionMetrics {
                    active_streams: 10,
                    queued_messages: 4,
                    avg_latency_ms: None,
                    last_error: None,
                },
            )
            .expect("first heartbeat should succeed");
        let initial = gateway.route_to_agent("org-1", "agent-1").expect("initial route should exist");
        assert_eq!(initial.session_id, first.session_id);

        let second = gateway.register_connection(registration("agent-1")).expect("second registration should succeed");
        let route = gateway.route_to_agent("org-1", "agent-1").expect("route should exist");

        assert_eq!(route.session_id, second.session_id);
        assert_eq!(cached_session_id(&gateway, "org-1", "agent-1").as_deref(), Some(second.session_id.as_str()));
    }

    #[test]
    fn heartbeat_invalidates_cached_route_when_load_changes() {
        let gateway = AgentGatewayState::default();
        let first = gateway.register_connection(registration("agent-1")).expect("first registration should succeed");
        let second = gateway.register_connection(registration("agent-1")).expect("second registration should succeed");
        gateway
            .heartbeat("org-1", &first.session_id, AgentConnectionMetrics::default())
            .expect("first heartbeat should succeed");
        gateway
            .heartbeat(
                "org-1",
                &second.session_id,
                AgentConnectionMetrics {
                    active_streams: 10,
                    queued_messages: 4,
                    avg_latency_ms: None,
                    last_error: None,
                },
            )
            .expect("second heartbeat should succeed");
        let initial = gateway.route_to_agent("org-1", "agent-1").expect("initial route should exist");
        assert_eq!(initial.session_id, first.session_id);

        gateway
            .heartbeat(
                "org-1",
                &first.session_id,
                AgentConnectionMetrics {
                    active_streams: 20,
                    queued_messages: 4,
                    avg_latency_ms: None,
                    last_error: None,
                },
            )
            .expect("first heartbeat should invalidate route");
        let route = gateway.route_to_agent("org-1", "agent-1").expect("route should exist");

        assert_eq!(route.session_id, second.session_id);
        assert_eq!(cached_session_id(&gateway, "org-1", "agent-1").as_deref(), Some(second.session_id.as_str()));
    }

    #[test]
    fn route_falls_back_when_cached_session_is_expired() {
        let gateway = AgentGatewayState::default();
        let fallback = gateway.register_connection(registration("agent-1")).expect("fallback registration should succeed");
        let cached = gateway.register_connection(registration("agent-1")).expect("cached registration should succeed");
        {
            let mut session = gateway.sessions.get_mut(&cached.session_id).expect("cached session should exist");
            session.expires_at_ms = 0;
        }

        let route = gateway.route_to_agent("org-1", "agent-1").expect("route should exist");

        assert_eq!(route.session_id, fallback.session_id);
        assert!(gateway.sessions.get(&cached.session_id).is_none());
        assert_eq!(cached_session_id(&gateway, "org-1", "agent-1").as_deref(), Some(fallback.session_id.as_str()));
    }

    #[test]
    fn route_falls_back_when_cached_session_is_draining() {
        let gateway = AgentGatewayState::default();
        let fallback = gateway.register_connection(registration("agent-1")).expect("fallback registration should succeed");
        let cached = gateway.register_connection(registration("agent-1")).expect("cached registration should succeed");
        gateway.mark_draining("org-1", &cached.session_id).expect("cached session should drain");

        let route = gateway.route_to_agent("org-1", "agent-1").expect("route should exist");

        assert_eq!(route.session_id, fallback.session_id);
        assert_eq!(cached_session_id(&gateway, "org-1", "agent-1").as_deref(), Some(fallback.session_id.as_str()));
    }

    #[test]
    fn route_falls_back_after_cached_session_disconnects() {
        let gateway = AgentGatewayState::default();
        let fallback = gateway.register_connection(registration("agent-1")).expect("fallback registration should succeed");
        let cached = gateway.register_connection(registration("agent-1")).expect("cached registration should succeed");
        gateway.disconnect("org-1", &cached.session_id).expect("cached session should disconnect");

        let route = gateway.route_to_agent("org-1", "agent-1").expect("route should exist");

        assert_eq!(route.session_id, fallback.session_id);
        assert_eq!(cached_session_id(&gateway, "org-1", "agent-1").as_deref(), Some(fallback.session_id.as_str()));
    }

    #[test]
    fn route_falls_back_when_cached_session_has_no_capacity() {
        let gateway = AgentGatewayState::default();
        let fallback = gateway.register_connection(registration("agent-1")).expect("fallback registration should succeed");
        let mut registration = registration("agent-1");
        registration.rate_limit.max_active_streams = Some(1);
        let cached = gateway.register_connection(registration).expect("cached registration should succeed");
        gateway
            .heartbeat(
                "org-1",
                &cached.session_id,
                AgentConnectionMetrics {
                    active_streams: 1,
                    queued_messages: 0,
                    avg_latency_ms: None,
                    last_error: None,
                },
            )
            .expect("cached heartbeat should succeed");

        let route = gateway.route_to_agent("org-1", "agent-1").expect("route should exist");

        assert_eq!(route.session_id, fallback.session_id);
        assert_eq!(cached_session_id(&gateway, "org-1", "agent-1").as_deref(), Some(fallback.session_id.as_str()));
    }

    #[test]
    fn local_only_policy_blocks_remote_urls() {
        let gateway = AgentGatewayState::new(AgentGatewayConfig {
            network_policy: AgentGatewayNetworkPolicy {
                allow_remote_urls: false,
                ..AgentGatewayNetworkPolicy::default()
            },
            ..AgentGatewayConfig::default()
        });

        let err = gateway.register_connection(registration("agent-1")).expect_err("remote URL should be blocked");
        assert_eq!(err, AgentGatewayError::RemoteUrlNotAllowed);
    }

    #[test]
    fn derives_stable_agent_fingerprint_when_not_supplied() {
        let gateway = AgentGatewayState::default();
        let first = gateway.register_connection(registration("agent-1")).expect("first registration should succeed");
        let second = gateway.register_connection(registration("agent-1")).expect("second registration should succeed");

        assert!(first.identity.fingerprint.starts_with("sha256:"));
        assert_eq!(first.identity.fingerprint, second.identity.fingerprint);
    }

    #[test]
    fn preserves_supplied_agent_identity_tags() {
        let gateway = AgentGatewayState::default();
        let mut registration = registration("agent-1");
        registration.identity = AgentGatewayIdentity {
            fingerprint: "agent-seat-42".to_string(),
            instance_id: Some(" worker-a ".to_string()),
            principal: Some(" planner ".to_string()),
            tags: BTreeMap::from([
                (" tier ".to_string(), " prod ".to_string()),
                (" ".to_string(), "ignored".to_string()),
            ]),
        };

        let session = gateway.register_connection(registration).expect("registration should succeed");

        assert_eq!(session.identity.fingerprint, "agent-seat-42");
        assert_eq!(session.identity.instance_id.as_deref(), Some("worker-a"));
        assert_eq!(session.identity.principal.as_deref(), Some("planner"));
        assert_eq!(session.identity.tags.get("tier").map(String::as_str), Some("prod"));
    }

    #[test]
    fn records_usage_by_agent_fingerprint() {
        let gateway = AgentGatewayState::default();
        let mut registration = registration("agent-1");
        registration.identity.fingerprint = "agent-seat-42".to_string();
        let session = gateway.register_connection(registration).expect("registration should succeed");

        let decision = gateway
            .record_usage(
                "org-1",
                &session.session_id,
                AgentGatewayUsageEvent {
                    prompt_tokens: 10,
                    completion_tokens: 5,
                    cost_microdollars: 25,
                    ..Default::default()
                },
            )
            .expect("usage should record");

        assert!(decision.allowed);
        assert_eq!(decision.usage.fingerprint, "agent-seat-42");
        assert_eq!(decision.usage.requests, 1);
        assert_eq!(decision.usage.total_tokens, 15);
        assert_eq!(gateway.list_usage("org-1").len(), 1);
    }

    #[test]
    fn rate_limits_usage_per_agent_fingerprint() {
        let gateway = AgentGatewayState::default();
        let mut registration = registration("agent-1");
        registration.identity.fingerprint = "agent-seat-42".to_string();
        registration.rate_limit.requests_per_minute = Some(1);
        let session = gateway.register_connection(registration).expect("registration should succeed");

        let first = gateway
            .record_usage("org-1", &session.session_id, AgentGatewayUsageEvent::default())
            .expect("first request should record");
        let second = gateway
            .record_usage("org-1", &session.session_id, AgentGatewayUsageEvent::default())
            .expect("second request should return a decision");

        assert!(first.allowed);
        assert!(!second.allowed);
        assert_eq!(second.reason, Some(AgentGatewayRateLimitReason::RequestsPerMinute));
        assert_eq!(second.usage.rate_limited_requests, 1);
    }
}
