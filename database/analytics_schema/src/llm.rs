//! Durable LLM analytics storage types for ClickHouse.

use chrono::{DateTime, Utc};
use clickhouse::Row;
use eden_core::format::EdenUuid;
use llm_core::LlmOperationEvent;
use llm_core::pricing::LlmPriceSnapshot;
use serde::{Deserialize, Serialize};

/// Row for `analytics.llm_operation_rollups`.
#[derive(Debug, Clone, Deserialize, Serialize, Row)]
pub struct LlmOperationRollupRow {
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub provider: String,
    pub model: String,
    pub operation: String,
    pub traffic_source: String,
    pub consumer_id: String,
    pub credential_id: String,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub timestamp: DateTime<Utc>,
    pub request_count: u64,
    pub prompt_tokens_sum: u64,
    pub completion_tokens_sum: u64,
    pub total_tokens_sum: u64,
    pub estimated_cost_micros_sum: u64,
    pub estimated_arbitrage_savings_micros_sum: u64,
    pub estimated_cache_savings_micros_sum: u64,
    pub estimated_kv_cache_savings_micros_sum: u64,
    pub latency_sum_ms: u64,
    pub error_count: u64,
    pub cache_hit_count: u64,
    pub cache_miss_count: u64,
    pub kv_cache_hit_count: u64,
    pub route_move_count: u64,
    pub arbitrage_switch_count: u64,
    pub tool_use_count: u64,
    pub pii_detected_count: u64,
    pub streaming_count: u64,
}

/// Row for `analytics.llm_operation_events`.
#[derive(Debug, Clone, Deserialize, Serialize, Row)]
pub struct LlmOperationEventRow {
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub provider: String,
    pub model: String,
    pub operation: String,
    pub traffic_source: String,
    pub consumer_id: String,
    pub credential_id: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
    pub request_bytes: u32,
    pub response_bytes: u32,
    pub estimated_cost_micros: u64,
    pub requested_provider: String,
    pub requested_model: String,
    pub baseline_estimated_cost_micros: u64,
    pub selected_estimated_cost_micros: u64,
    pub estimated_arbitrage_savings_micros: u64,
    pub arbitrage_reason: String,
    pub price_source: String,
    pub cache_status: String,
    pub estimated_cache_savings_micros: u64,
    pub route_optimization_mode: String,
    pub kv_cache_mode: String,
    pub kv_cache_status: String,
    pub estimated_kv_cache_savings_micros: u64,
    pub route_move_reason: String,
    pub conversation_route_key: String,
    pub latency_ms: u64,
    pub success: u8,
    pub error_message: String,
    pub streaming: u8,
    pub tool_used: u8,
    pub tool_call_count: u32,
    pub message_count: u32,
    pub policy_action: String,
    pub pii_detected: u8,
    pub pii_types: Vec<String>,
    pub prompt_fingerprint: String,
    pub agent_uuid: String,
}

/// Row for `analytics.llm_price_snapshots`.
#[derive(Debug, Clone, Deserialize, Serialize, Row)]
pub struct LlmPriceSnapshotRow {
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub fetched_at: DateTime<Utc>,
    pub provider: String,
    pub model: String,
    pub source: String,
    pub input_micros_per_million: u64,
    pub output_micros_per_million: u64,
}

impl From<&LlmOperationEvent> for LlmOperationRollupRow {
    fn from(event: &LlmOperationEvent) -> Self {
        Self {
            organization_uuid: event.organization_uuid.uuid().to_string(),
            endpoint_uuid: event.endpoint_uuid.uuid().to_string(),
            provider: event.provider.clone(),
            model: event.model.clone(),
            operation: event.operation.clone(),
            traffic_source: event.traffic_source.to_string(),
            consumer_id: option_string_or_default(event.consumer_id.as_deref()),
            credential_id: option_string_or_default(event.credential_id.as_deref()),
            timestamp: event.timestamp,
            request_count: 1,
            prompt_tokens_sum: event.prompt_tokens as u64,
            completion_tokens_sum: event.completion_tokens as u64,
            total_tokens_sum: event.total_tokens as u64,
            estimated_cost_micros_sum: event.estimated_provider_cost_micros,
            estimated_arbitrage_savings_micros_sum: event.estimated_arbitrage_savings_micros,
            estimated_cache_savings_micros_sum: event.estimated_cache_savings_micros,
            estimated_kv_cache_savings_micros_sum: event.estimated_kv_cache_savings_micros,
            latency_sum_ms: event.latency_ms,
            error_count: u64::from(!event.success),
            cache_hit_count: u64::from(event.cache_status == llm_core::LlmCacheStatus::Hit),
            cache_miss_count: u64::from(event.cache_status == llm_core::LlmCacheStatus::Miss),
            kv_cache_hit_count: u64::from(event.kv_cache_status == llm_core::LlmKvCacheStatus::Hit),
            route_move_count: u64::from(event.kv_cache_status == llm_core::LlmKvCacheStatus::Move),
            arbitrage_switch_count: u64::from(event.requested_model.as_deref().is_some_and(|model| model != event.model)),
            tool_use_count: u64::from(event.tool_used),
            pii_detected_count: u64::from(event.pii_detected),
            streaming_count: u64::from(event.streaming),
        }
    }
}

impl From<LlmOperationEvent> for LlmOperationRollupRow {
    fn from(event: LlmOperationEvent) -> Self {
        Self::from(&event)
    }
}

impl From<&LlmOperationEvent> for LlmOperationEventRow {
    fn from(event: &LlmOperationEvent) -> Self {
        Self {
            organization_uuid: event.organization_uuid.uuid().to_string(),
            endpoint_uuid: event.endpoint_uuid.uuid().to_string(),
            provider: event.provider.clone(),
            model: event.model.clone(),
            operation: event.operation.clone(),
            traffic_source: event.traffic_source.to_string(),
            consumer_id: option_string_or_default(event.consumer_id.as_deref()),
            credential_id: option_string_or_default(event.credential_id.as_deref()),
            timestamp: event.timestamp,
            prompt_tokens: event.prompt_tokens,
            completion_tokens: event.completion_tokens,
            total_tokens: event.total_tokens,
            request_bytes: event.request_bytes,
            response_bytes: event.response_bytes,
            estimated_cost_micros: event.estimated_provider_cost_micros,
            requested_provider: option_string_or_default(event.requested_provider.as_deref()),
            requested_model: option_string_or_default(event.requested_model.as_deref()),
            baseline_estimated_cost_micros: event.baseline_estimated_cost_micros,
            selected_estimated_cost_micros: event.selected_estimated_cost_micros,
            estimated_arbitrage_savings_micros: event.estimated_arbitrage_savings_micros,
            arbitrage_reason: option_string_or_default(event.arbitrage_reason.as_deref()),
            price_source: option_string_or_default(event.price_source.as_deref()),
            cache_status: event.cache_status.to_string(),
            estimated_cache_savings_micros: event.estimated_cache_savings_micros,
            route_optimization_mode: event.route_optimization_mode.to_string(),
            kv_cache_mode: event.kv_cache_mode.to_string(),
            kv_cache_status: event.kv_cache_status.to_string(),
            estimated_kv_cache_savings_micros: event.estimated_kv_cache_savings_micros,
            route_move_reason: option_string_or_default(event.route_move_reason.as_deref()),
            conversation_route_key: option_string_or_default(event.conversation_route_key.as_deref()),
            latency_ms: event.latency_ms,
            success: u8::from(event.success),
            error_message: option_string_or_default(event.error_message.as_deref()),
            streaming: u8::from(event.streaming),
            tool_used: u8::from(event.tool_used),
            tool_call_count: event.tool_call_count,
            message_count: event.message_count,
            policy_action: event.policy_action.to_string(),
            pii_detected: u8::from(event.pii_detected),
            pii_types: event.pii_types.clone(),
            prompt_fingerprint: option_string_or_default(event.prompt_fingerprint.as_deref()),
            agent_uuid: event.agent_uuid.map(|u| u.to_string()).unwrap_or_default(),
        }
    }
}

impl From<LlmPriceSnapshot> for LlmPriceSnapshotRow {
    fn from(snapshot: LlmPriceSnapshot) -> Self {
        Self {
            fetched_at: snapshot.fetched_at,
            provider: snapshot.provider,
            model: snapshot.model,
            source: snapshot.source.to_string(),
            input_micros_per_million: snapshot.input_micros_per_million,
            output_micros_per_million: snapshot.output_micros_per_million,
        }
    }
}

impl From<LlmOperationEvent> for LlmOperationEventRow {
    fn from(event: LlmOperationEvent) -> Self {
        Self::from(&event)
    }
}

fn option_string_or_default(value: Option<&str>) -> String {
    value.unwrap_or_default().to_string()
}

/// Table names for durable LLM analytics storage.
pub mod tables {
    pub const LLM_OPERATION_ROLLUPS: &str = "analytics.llm_operation_rollups";
    pub const LLM_OPERATION_EVENTS: &str = "analytics.llm_operation_events";
    pub const LLM_PRICE_SNAPSHOTS: &str = "analytics.llm_price_snapshots";
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use eden_core::format::{EndpointUuid, OrganizationUuid};
    use llm_core::{PolicyAction, TrafficSource};
    use uuid::Uuid;

    fn sample_event() -> LlmOperationEvent {
        let timestamp = Utc.timestamp_opt(1_731_972_645, 123_000_000).single().unwrap_or_else(Utc::now);
        LlmOperationEvent {
            timestamp,
            organization_uuid: OrganizationUuid::new(Uuid::parse_str("12345678-1234-5678-1234-567812345678").expect("valid uuid")),
            endpoint_uuid: EndpointUuid::new(Uuid::parse_str("87654321-4321-8765-4321-876543218765").expect("valid uuid")),
            provider: "openai".to_string(),
            model: "gpt-4o".to_string(),
            requested_provider: Some("openrouter".to_string()),
            requested_model: Some("openai/gpt-4o".to_string()),
            operation: "chat.completions".to_string(),
            traffic_source: TrafficSource::AgentGateway,
            consumer_id: None,
            credential_id: Some("cred-1".to_string()),
            agent_uuid: Some(Uuid::parse_str("aaaaaaaa-1111-2222-3333-444444444444").expect("valid uuid")),
            streaming: true,
            tool_used: true,
            tool_call_count: 2,
            message_count: 4,
            prompt_tokens: 120,
            completion_tokens: 30,
            total_tokens: 150,
            request_bytes: 4096,
            response_bytes: 8192,
            estimated_provider_cost_micros: 600,
            baseline_estimated_cost_micros: 700,
            selected_estimated_cost_micros: 600,
            estimated_arbitrage_savings_micros: 100,
            arbitrage_reason: Some("same_model_cheaper_provider".to_string()),
            price_source: Some("dynamic_openrouter".to_string()),
            cache_status: llm_core::LlmCacheStatus::Hit,
            estimated_cache_savings_micros: 600,
            route_optimization_mode: llm_core::LlmRouteOptimizationMode::Balanced,
            kv_cache_mode: llm_core::LlmKvCacheMode::Adaptive,
            kv_cache_status: llm_core::LlmKvCacheStatus::Move,
            estimated_kv_cache_savings_micros: 120,
            route_move_reason: Some("cost_threshold_exceeded".to_string()),
            conversation_route_key: Some("route-key-1".to_string()),
            latency_ms: 900,
            success: false,
            error_message: Some("timeout".to_string()),
            policy_action: PolicyAction::AuditOnly,
            pii_detected: true,
            pii_types: vec!["email".to_string()],
            prompt_fingerprint: Some("fp-1".to_string()),
        }
    }

    #[test]
    fn converts_event_rows() {
        let event = sample_event();
        let row = LlmOperationEventRow::from(&event);

        assert_eq!(row.organization_uuid, "12345678-1234-5678-1234-567812345678");
        assert_eq!(row.traffic_source, "agent_gateway");
        assert_eq!(row.credential_id, "cred-1");
        assert_eq!(row.success, 0);
        assert_eq!(row.policy_action, "audit_only");
        assert_eq!(row.prompt_fingerprint, "fp-1");
        assert_eq!(row.requested_provider, "openrouter");
        assert_eq!(row.cache_status, "hit");
        assert_eq!(row.estimated_cache_savings_micros, 600);
        assert_eq!(row.route_optimization_mode, "balanced");
        assert_eq!(row.kv_cache_mode, "adaptive");
        assert_eq!(row.kv_cache_status, "move");
        assert_eq!(row.estimated_kv_cache_savings_micros, 120);
        assert_eq!(row.route_move_reason, "cost_threshold_exceeded");
        assert_eq!(row.conversation_route_key, "route-key-1");
        assert_eq!(row.agent_uuid, "aaaaaaaa-1111-2222-3333-444444444444");
        assert_eq!(row.request_bytes, 4096);
        assert_eq!(row.response_bytes, 8192);
    }

    #[test]
    fn converts_rollup_rows() {
        let event = sample_event();
        let row = LlmOperationRollupRow::from(&event);

        assert_eq!(row.organization_uuid, "12345678-1234-5678-1234-567812345678");
        assert_eq!(row.request_count, 1);
        assert_eq!(row.error_count, 1);
        assert_eq!(row.cache_hit_count, 1);
        assert_eq!(row.route_move_count, 1);
        assert_eq!(row.estimated_arbitrage_savings_micros_sum, 100);
        assert_eq!(row.estimated_kv_cache_savings_micros_sum, 120);
        assert_eq!(row.tool_use_count, 1);
        assert_eq!(row.pii_detected_count, 1);
        assert_eq!(row.streaming_count, 1);
    }
}
