use crate::types::LlmOperationEvent;
use once_cell::sync::Lazy;
use std::collections::VecDeque;
use std::sync::RwLock;
use telemetry::metrics::AllMetrics;
use tokio::sync::mpsc;

static LLM_OPERATION_SENDER: Lazy<RwLock<Option<mpsc::Sender<LlmOperationEvent>>>> = Lazy::new(|| RwLock::new(None));
static RECENT_LLM_OPERATIONS: Lazy<RwLock<VecDeque<LlmOperationEvent>>> = Lazy::new(|| RwLock::new(VecDeque::new()));
const RECENT_LLM_OPERATION_LIMIT: usize = 512;

/// Install or replace the bounded sender used for durable LLM analytics events.
pub fn set_llm_operation_sender(sender: Option<mpsc::Sender<LlmOperationEvent>>) {
    if let Ok(mut guard) = LLM_OPERATION_SENDER.write() {
        *guard = sender;
    }
}

/// Remove the currently configured durable LLM analytics sender.
pub fn clear_llm_operation_sender() {
    set_llm_operation_sender(None);
}

/// Return the most recent in-process LLM analytics events, newest first.
pub fn recent_llm_operations() -> Vec<LlmOperationEvent> {
    match RECENT_LLM_OPERATIONS.read() {
        Ok(guard) => guard.iter().rev().cloned().collect(),
        Err(_) => Vec::new(),
    }
}

fn push_recent_llm_operation(event: &LlmOperationEvent) {
    let Ok(mut guard) = RECENT_LLM_OPERATIONS.write() else {
        return;
    };
    while guard.len() >= RECENT_LLM_OPERATION_LIMIT {
        guard.pop_front();
    }
    guard.push_back(event.clone());
}

/// Record a durable LLM analytics event.
///
/// This always updates the fast in-memory counters and best-effort enqueues the
/// event for async ClickHouse persistence when a sender has been installed.
pub fn record_llm_operation(metrics: &AllMetrics, event: LlmOperationEvent) {
    push_recent_llm_operation(&event);

    metrics.eden().record_llm_usage(
        Some(event.prompt_tokens as u64),
        Some(event.completion_tokens as u64),
        Some(event.total_tokens as u64),
        &event.provider,
        &event.model,
        Some(&event.endpoint_uuid),
        &event.organization_uuid,
        event.tool_used,
        event.streaming,
    );

    let sender = match LLM_OPERATION_SENDER.read() {
        Ok(guard) => guard.clone(),
        Err(_) => None,
    };

    if let Some(sender) = sender {
        let _ = sender.try_send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PolicyAction, TrafficSource};
    use chrono::Utc;
    use format::{EndpointUuid, OrganizationUuid};

    fn sample_event() -> LlmOperationEvent {
        LlmOperationEvent {
            timestamp: Utc::now(),
            organization_uuid: OrganizationUuid::new_uuid(),
            endpoint_uuid: EndpointUuid::new_uuid(),
            provider: "openai".to_string(),
            model: "gpt-4o".to_string(),
            requested_provider: None,
            requested_model: None,
            operation: "chat.completions".to_string(),
            traffic_source: TrafficSource::AgentGateway,
            consumer_id: None,
            credential_id: None,
            agent_uuid: None,
            streaming: false,
            tool_used: true,
            tool_call_count: 2,
            message_count: 3,
            prompt_tokens: 100,
            completion_tokens: 50,
            total_tokens: 150,
            request_bytes: 0,
            response_bytes: 0,
            estimated_provider_cost_micros: 750,
            baseline_estimated_cost_micros: 0,
            selected_estimated_cost_micros: 0,
            estimated_arbitrage_savings_micros: 0,
            arbitrage_reason: None,
            price_source: None,
            cache_status: crate::types::LlmCacheStatus::Bypass,
            estimated_cache_savings_micros: 0,
            route_optimization_mode: crate::types::LlmRouteOptimizationMode::Cost,
            kv_cache_mode: crate::types::LlmKvCacheMode::Disabled,
            kv_cache_status: crate::types::LlmKvCacheStatus::Bypass,
            estimated_kv_cache_savings_micros: 0,
            route_move_reason: None,
            conversation_route_key: None,
            latency_ms: 42,
            success: true,
            error_message: None,
            policy_action: PolicyAction::Allow,
            pii_detected: false,
            pii_types: Vec::new(),
            prompt_fingerprint: Some("abc123".to_string()),
        }
    }

    #[test]
    fn recent_operations_tracks_events_newest_first() {
        clear_llm_operation_sender();
        let metrics = AllMetrics::new();
        let organization_uuid = OrganizationUuid::new_uuid();
        let endpoint_uuid = EndpointUuid::new_uuid();
        let mut first = sample_event();
        first.organization_uuid = organization_uuid.clone();
        first.endpoint_uuid = endpoint_uuid.clone();
        first.prompt_fingerprint = Some("first".to_string());
        let mut second = sample_event();
        second.organization_uuid = organization_uuid.clone();
        second.endpoint_uuid = endpoint_uuid.clone();
        second.prompt_fingerprint = Some("second".to_string());

        record_llm_operation(&metrics, first);
        record_llm_operation(&metrics, second);

        let recent = recent_llm_operations()
            .into_iter()
            .filter(|event| event.organization_uuid == organization_uuid && event.endpoint_uuid == endpoint_uuid)
            .collect::<Vec<_>>();
        assert_eq!(recent.first().and_then(|event| event.prompt_fingerprint.as_deref()), Some("second"));
        assert!(recent.iter().any(|event| event.prompt_fingerprint.as_deref() == Some("first")));
    }

    #[tokio::test]
    async fn records_metrics_and_enqueues_event() {
        clear_llm_operation_sender();
        let (tx, mut rx) = mpsc::channel(4);
        set_llm_operation_sender(Some(tx));

        let metrics = AllMetrics::new();
        record_llm_operation(&metrics, sample_event());

        let event = rx.recv().await.expect("event should be enqueued");
        assert_eq!(event.provider, "openai");
        assert_eq!(metrics.eden().get_llm_requests(), 1);
        assert_eq!(metrics.eden().get_llm_prompt_tokens(), 100);
        assert_eq!(metrics.eden().get_llm_completion_tokens(), 50);
        assert_eq!(metrics.eden().get_llm_total_tokens(), 150);

        clear_llm_operation_sender();
    }
}
