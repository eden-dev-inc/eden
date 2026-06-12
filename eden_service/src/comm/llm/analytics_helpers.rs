use eden_core::format::{EndpointUuid, OrganizationUuid, UserUuid};
use eden_core::telemetry::metrics::AllMetrics;
use endpoint_core::llm_core::analytics::record_llm_operation;
use endpoint_core::llm_core::{
    LlmCacheStatus, LlmKvCacheMode, LlmKvCacheStatus, LlmOperationEvent, LlmProviderMetadata, LlmRouteOptimizationMode, LlmUsage,
    PolicyAction, TrafficSource, estimate_cost_micros, estimate_price,
};

#[derive(Clone)]
pub struct LlmAnalyticsContext {
    pub endpoint_uuid: EndpointUuid,
    pub organization_uuid: OrganizationUuid,
    pub user_uuid: Option<UserUuid>,
    pub credential_id: Option<String>,
    pub consumer_id: Option<String>,
    /// Immutable owning agent (`llm_agents.id`) of the resolved API key; `None`
    /// for non-agent traffic. Canonical attribution key.
    pub agent_uuid: Option<uuid::Uuid>,
    pub message_count: u32,
    pub prompt_fingerprint: Option<String>,
    pub request_bytes: u32,
    pub temperature: Option<f32>,
    pub max_tokens_requested: Option<u32>,
    pub traffic_source: TrafficSource,
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

pub struct LlmAnalyticsRecord<'a> {
    pub usage: Option<&'a LlmUsage>,
    pub provider: &'a LlmProviderMetadata,
    pub operation: &'a str,
    pub tool_used: bool,
    pub tool_call_count: u32,
    pub streaming: bool,
    pub latency_ms: u64,
    pub response_bytes: u32,
    pub success: bool,
    pub error_message: Option<String>,
    pub policy_action: PolicyAction,
    pub pii_detected: bool,
    pub pii_types: &'a [String],
}

pub fn record_llm_analytics(metrics: &AllMetrics, context: &LlmAnalyticsContext, record: LlmAnalyticsRecord<'_>) {
    let prompt_tokens = record.usage.map(|usage| usage.prompt_tokens).unwrap_or_default();
    let completion_tokens = record.usage.map(|usage| usage.completion_tokens).unwrap_or_default();
    let total_tokens = record.usage.map(|usage| usage.total_tokens).unwrap_or_default();
    let selected_price = estimate_price(&record.provider.provider, &record.provider.model, prompt_tokens, completion_tokens);
    let estimated_provider_cost_micros = if context.cache_status == LlmCacheStatus::Hit {
        0
    } else {
        selected_price
            .as_ref()
            .map(|price| price.estimated_cost_micros)
            .unwrap_or_else(|| estimate_cost_micros(&record.provider.provider, &record.provider.model, prompt_tokens, completion_tokens))
    };
    let price_source = context.price_source.clone().or_else(|| selected_price.map(|price| price.source.to_string()));

    record_llm_operation(
        metrics,
        LlmOperationEvent {
            timestamp: chrono::Utc::now(),
            organization_uuid: context.organization_uuid.clone(),
            endpoint_uuid: context.endpoint_uuid.clone(),
            provider: record.provider.provider.clone(),
            model: record.provider.model.clone(),
            requested_provider: context.requested_provider.clone(),
            requested_model: context.requested_model.clone(),
            operation: record.operation.to_string(),
            traffic_source: context.traffic_source,
            consumer_id: context.consumer_id.clone(),
            credential_id: context.credential_id.clone(),
            // Stamped from the resolved API key's owning agent (Phase 1c).
            agent_uuid: context.agent_uuid,
            streaming: record.streaming,
            tool_used: record.tool_used,
            tool_call_count: record.tool_call_count,
            message_count: context.message_count,
            prompt_tokens,
            completion_tokens,
            total_tokens,
            request_bytes: context.request_bytes,
            response_bytes: record.response_bytes,
            estimated_provider_cost_micros,
            baseline_estimated_cost_micros: context.baseline_estimated_cost_micros,
            selected_estimated_cost_micros: context.selected_estimated_cost_micros,
            estimated_arbitrage_savings_micros: context.estimated_arbitrage_savings_micros,
            arbitrage_reason: context.arbitrage_reason.clone(),
            price_source,
            cache_status: context.cache_status,
            estimated_cache_savings_micros: context.estimated_cache_savings_micros,
            route_optimization_mode: context.route_optimization_mode,
            kv_cache_mode: context.kv_cache_mode,
            kv_cache_status: context.kv_cache_status,
            estimated_kv_cache_savings_micros: context.estimated_kv_cache_savings_micros,
            route_move_reason: context.route_move_reason.clone(),
            conversation_route_key: context.conversation_route_key.clone(),
            latency_ms: record.latency_ms,
            success: record.success,
            error_message: record.error_message.clone(),
            policy_action: record.policy_action,
            pii_detected: record.pii_detected,
            pii_types: record.pii_types.to_vec(),
            prompt_fingerprint: context.prompt_fingerprint.clone(),
        },
    );
}
