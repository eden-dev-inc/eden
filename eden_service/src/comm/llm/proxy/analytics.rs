use crate::comm::llm::analytics_helpers::{LlmAnalyticsRecord, record_llm_analytics};
use crate::comm::llm::utils::prompt_fingerprint;
use eden_core::format::{EdenUuid, EndpointUuid, OrganizationUuid};
use eden_core::telemetry::metrics::AllMetrics;
use endpoint_core::llm_core::{
    LlmCacheStatus, LlmInvocation, LlmKvCacheMode, LlmKvCacheStatus, LlmProviderMetadata, LlmRouteOptimizationMode, LlmUsage,
    OpenAiChatCompletionRequest, PolicyAction,
};

use super::keys::ApiKey;
use super::state::ProxyAnalyticsContext;

pub(super) fn build_proxy_analytics_context(
    endpoint_uuid: EndpointUuid,
    org_uuid: &OrganizationUuid,
    api_key: &ApiKey,
    credential_id: Option<String>,
    request: &OpenAiChatCompletionRequest,
    invocation: &LlmInvocation,
    body: &[u8],
) -> ProxyAnalyticsContext {
    let request_bytes = u32::try_from(body.len()).unwrap_or(u32::MAX);
    let prompt_fingerprint = prompt_fingerprint(&invocation.conversation);
    let message_count = u32::try_from(invocation.conversation.len()).unwrap_or(u32::MAX);

    ProxyAnalyticsContext {
        endpoint_uuid,
        organization_uuid: org_uuid.clone(),
        credential_id,
        consumer_id: api_key.id.to_string(),
        agent_uuid: api_key.agent_uuid,
        message_count,
        prompt_fingerprint,
        request_bytes,
        temperature: request.temperature,
        max_tokens_requested: request.max_tokens,
        requested_provider: None,
        requested_model: Some(request.model.trim().to_string()),
        baseline_estimated_cost_micros: 0,
        selected_estimated_cost_micros: 0,
        estimated_arbitrage_savings_micros: 0,
        arbitrage_reason: None,
        price_source: None,
        cache_status: LlmCacheStatus::Bypass,
        estimated_cache_savings_micros: 0,
        route_optimization_mode: LlmRouteOptimizationMode::Cost,
        kv_cache_mode: LlmKvCacheMode::Disabled,
        kv_cache_status: LlmKvCacheStatus::Bypass,
        estimated_kv_cache_savings_micros: 0,
        route_move_reason: None,
        conversation_route_key: None,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn record_proxy_llm_usage_metrics(
    metrics: &AllMetrics,
    usage: Option<&LlmUsage>,
    provider: &LlmProviderMetadata,
    operation: &str,
    context: &ProxyAnalyticsContext,
    tool_used: bool,
    tool_call_count: u32,
    streaming: bool,
    latency_ms: u64,
    response_bytes: u32,
    success: bool,
    error_message: Option<String>,
    policy_action: PolicyAction,
    pii_detected: bool,
    pii_types: &[String],
) {
    record_llm_analytics(
        metrics,
        &context.to_common_context(),
        LlmAnalyticsRecord {
            usage,
            provider,
            operation,
            tool_used,
            tool_call_count,
            streaming,
            latency_ms,
            response_bytes,
            success,
            error_message,
            policy_action,
            pii_detected,
            pii_types,
        },
    );

    record_general_proxy_metrics(metrics, provider, operation, context, streaming, latency_ms, response_bytes, success, policy_action);
}

fn record_general_proxy_metrics(
    metrics: &AllMetrics,
    provider: &LlmProviderMetadata,
    operation: &str,
    context: &ProxyAnalyticsContext,
    streaming: bool,
    latency_ms: u64,
    response_bytes: u32,
    success: bool,
    policy_action: PolicyAction,
) {
    let org_uuid = context.organization_uuid.uuid().to_string();
    let endpoint_uuid = context.endpoint_uuid.uuid().to_string();
    let cache_status = context.cache_status.to_string();
    let kv_cache_status = context.kv_cache_status.to_string();
    let policy_action = policy_action.to_string();
    let success = if success { "true" } else { "false" };
    let streaming = if streaming { "true" } else { "false" };
    let latency_us = latency_ms.saturating_mul(1_000);

    let labels = [
        ("org_uuid", org_uuid.as_str()),
        ("endpoint_uuid", endpoint_uuid.as_str()),
        ("endpoint_kind", "llm"),
        ("interlay_uuid", "llm_gateway"),
        ("operation", operation),
        ("provider", provider.provider.as_str()),
        ("model", provider.model.as_str()),
        ("traffic_source", "proxy_app"),
        ("streaming", streaming),
        ("success", success),
        ("cache_status", cache_status.as_str()),
        ("kv_cache_status", kv_cache_status.as_str()),
        ("policy_action", policy_action.as_str()),
    ];

    metrics.proxy().record_request(&labels);
    metrics.proxy().record_commands(1, &labels);
    metrics.proxy().record_duration(latency_us, &labels);
    metrics.proxy().record_endpoint_duration(latency_us, &labels);
    metrics.proxy().record_bytes_read(u64::from(context.request_bytes), &labels);
    metrics.proxy().record_bytes_written(u64::from(response_bytes), &labels);

    if success == "false" {
        let error_type = if policy_action == PolicyAction::Block.as_str() {
            "policy_block"
        } else {
            "llm_proxy_error"
        };
        let error_labels = [
            ("org_uuid", org_uuid.as_str()),
            ("endpoint_uuid", endpoint_uuid.as_str()),
            ("endpoint_kind", "llm"),
            ("interlay_uuid", "llm_gateway"),
            ("operation", operation),
            ("provider", provider.provider.as_str()),
            ("model", provider.model.as_str()),
            ("traffic_source", "proxy_app"),
            ("streaming", streaming),
            ("cache_status", cache_status.as_str()),
            ("kv_cache_status", kv_cache_status.as_str()),
            ("policy_action", policy_action.as_str()),
            ("error_type", error_type),
        ];
        metrics.proxy().record_error(&error_labels);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eden_core::telemetry::metrics::FastMetricsExportState;

    fn test_context() -> ProxyAnalyticsContext {
        ProxyAnalyticsContext {
            endpoint_uuid: EndpointUuid::new_uuid(),
            organization_uuid: OrganizationUuid::new_uuid(),
            credential_id: Some("openrouter-credential".to_string()),
            consumer_id: "proxy-key-1".to_string(),
            agent_uuid: uuid::Uuid::new_v4(),
            message_count: 2,
            prompt_fingerprint: Some("prompt-fingerprint".to_string()),
            request_bytes: 512,
            temperature: Some(0.2),
            max_tokens_requested: Some(256),
            requested_provider: Some("openrouter".to_string()),
            requested_model: Some("anthropic/claude-3.5-sonnet".to_string()),
            baseline_estimated_cost_micros: 0,
            selected_estimated_cost_micros: 0,
            estimated_arbitrage_savings_micros: 0,
            arbitrage_reason: None,
            price_source: None,
            cache_status: LlmCacheStatus::Bypass,
            estimated_cache_savings_micros: 0,
            route_optimization_mode: LlmRouteOptimizationMode::Cost,
            kv_cache_mode: LlmKvCacheMode::Disabled,
            kv_cache_status: LlmKvCacheStatus::Bypass,
            estimated_kv_cache_savings_micros: 0,
            route_move_reason: None,
            conversation_route_key: None,
        }
    }

    #[test]
    fn llm_proxy_usage_records_general_proxy_metrics() {
        let metrics = AllMetrics::new();
        let context = test_context();
        let provider =
            LlmProviderMetadata::new("openrouter", "anthropic/claude-3.5-sonnet", Some("https://openrouter.ai/api/v1".to_string()));
        let usage = LlmUsage {
            prompt_tokens: 100,
            completion_tokens: 50,
            total_tokens: 150,
            completion_tokens_details: None,
            prompt_tokens_details: None,
        };

        record_proxy_llm_usage_metrics(
            &metrics,
            Some(&usage),
            &provider,
            "chat.completions",
            &context,
            false,
            0,
            false,
            42,
            1024,
            true,
            None,
            PolicyAction::Allow,
            false,
            &[],
        );

        assert_eq!(metrics.proxy().get_requests_total(), 1);
        assert_eq!(metrics.proxy().get_commands_total(), 1);
        assert_eq!(metrics.proxy().get_bytes_read_total(), 512);
        assert_eq!(metrics.proxy().get_bytes_written_total(), 1024);
        assert_eq!(metrics.proxy().get_errors_total(), 0);

        let mut output = String::new();
        let mut state = FastMetricsExportState::new();
        metrics.export_dogstatsd_delta(&mut output, &[("service", "eden")], &mut state);
        let proxy_output = output.lines().filter(|line| line.starts_with("gateway.")).collect::<Vec<_>>().join("\n");

        assert!(proxy_output.contains("gateway.requests_total:1|c"));
        assert!(proxy_output.contains("gateway.endpoint_duration_microseconds:"));
        assert!(proxy_output.contains("org_uuid:"));
        assert!(proxy_output.contains("endpoint_uuid:"));
        assert!(proxy_output.contains("endpoint_kind:llm"));
        assert!(proxy_output.contains("provider:openrouter"));
        assert!(proxy_output.contains("operation:chat.completions"));
        assert!(!proxy_output.contains("org_uuid:org:"));
        assert!(!proxy_output.contains("endpoint_uuid:endpoint:"));
    }

    #[test]
    fn failed_llm_proxy_usage_records_proxy_error() {
        let metrics = AllMetrics::new();
        let context = test_context();
        let provider = LlmProviderMetadata::new("openrouter", "openai/gpt-4o-mini", None);

        record_proxy_llm_usage_metrics(
            &metrics,
            None,
            &provider,
            "chat.completions",
            &context,
            false,
            0,
            false,
            12,
            0,
            false,
            Some("upstream failed".to_string()),
            PolicyAction::Allow,
            false,
            &[],
        );

        assert_eq!(metrics.proxy().get_requests_total(), 1);
        assert_eq!(metrics.proxy().get_errors_total(), 1);
    }
}
