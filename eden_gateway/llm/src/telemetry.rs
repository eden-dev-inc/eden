use super::HttpRequest;
use super::analysis::LlmPayloadAnalysis;
use super::auth::LlmGatewayAuthDecision;
use super::features::{LlmGatewayFeatureDecision, LlmResponseInspection};
use crate::gateway_telemetry::{GatewayHttpTelemetrySource, GatewayTelemetry, GatewayTelemetryContext};
use eden_core::format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use eden_core::format::endpoint::EpKind;
use eden_core::telemetry::metrics::AllMetrics;
use eden_core::telemetry::{FastSpan, FastSpanAttribute, FastSpanStatus, TelemetryWrapper};
use eden_logger_internal::{LogAudience, LogContext, log_error, log_warn};
use endpoint_core::llm_core::{
    LlmGatewayAgentIdentity, LlmGatewayRouteDecision, LlmGatewayRouteObservation, record_llm_gateway_route_observation,
};
use endpoints::endpoint::llm::{LlmProviderMetadata, LlmUsage};
use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

impl GatewayHttpTelemetrySource for HttpRequest {
    fn method(&self) -> &str {
        self.method.as_str()
    }

    fn path_without_query(&self) -> &str {
        self.path_without_query()
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.header(name)
    }
}

pub(super) struct LlmGatewayParseTelemetry;

impl LlmGatewayParseTelemetry {
    pub(super) fn record(
        telemetry_wrapper: &mut TelemetryWrapper,
        status: u16,
        message: &str,
        request_received_at: Instant,
        ctx: &LogContext,
    ) {
        let mut span = telemetry_wrapper.server_tracer("llm.gateway.request".to_string());
        span.set_attribute("eden.llm.gateway.route", "parse_error");
        span.set_attribute("http.response.status_code", status.to_string());
        span.set_status(FastSpanStatus::Error { message: Cow::Owned(message.to_string()) });

        let status_code = status.to_string();
        let org_uuid = telemetry_wrapper.labels().org_uuid().unwrap_or(eden_core::telemetry::labels::SYSTEM_ORG_UUID).to_string();
        let labels = [
            ("org_uuid", org_uuid.as_str()),
            ("method", "unknown"),
            ("route", "parse_error"),
            ("status_code", status_code.as_str()),
            ("status_class", GatewayTelemetry::status_class(status)),
            ("error_type", "parse_error"),
            ("streaming", "false"),
            ("tool_used", "false"),
            ("auth_scheme", "unknown"),
            ("provider", "unknown"),
            ("model", "unknown"),
        ];

        let metrics = telemetry_wrapper.metrics().eden();
        metrics.record_llm_gateway_request(GatewayTelemetry::elapsed_since_us(request_received_at), &labels);
        metrics.record_llm_gateway_error(&labels);
        log_warn!(
            ctx.clone(),
            "LLM gateway HTTP request parse failed",
            audience = LogAudience::Internal,
            status_code = status,
            status_class = GatewayTelemetry::status_class(status),
            error = message,
            org_uuid = org_uuid.as_str()
        );
    }
}

pub(super) struct LlmGatewayRequestTelemetry {
    metrics: Arc<AllMetrics>,
    span: FastSpan,
    context: GatewayTelemetryContext,
    ctx: LogContext,
    started_at: Instant,
    provider: String,
    model: String,
    route_class: String,
    streaming: bool,
    tool_used: bool,
    finished: bool,
}

impl LlmGatewayRequestTelemetry {
    pub(super) fn new(
        telemetry_wrapper: &mut TelemetryWrapper,
        request: &HttpRequest,
        interlay_cache_uuid: &InterlayCacheUuid,
        client_addr: SocketAddr,
        started_at: Instant,
        ctx: LogContext,
    ) -> Self {
        let context =
            GatewayTelemetryContext::from_http_request(telemetry_wrapper, request, interlay_cache_uuid, EpKind::Llm, client_addr, |path| {
                LlmGatewayRoute::name(path)
            });

        let mut span = telemetry_wrapper.server_tracer("llm.gateway.request".to_string());
        context.set_request_span_attributes(&mut span, "eden.llm.gateway.route", "eden.llm.gateway.auth.scheme");

        Self {
            metrics: Arc::clone(telemetry_wrapper.metrics()),
            span,
            context,
            ctx,
            started_at,
            provider: "unknown".to_string(),
            model: "unknown".to_string(),
            route_class: "unknown".to_string(),
            streaming: false,
            tool_used: false,
            finished: false,
        }
    }

    pub(super) fn set_streaming(&mut self, streaming: bool) {
        self.streaming = streaming;
        self.span.set_attribute("gen_ai.request.stream", streaming.to_string());
    }

    pub(super) fn set_requested_model(&mut self, model: Option<&str>) {
        if let Some(model) = model.filter(|model| !model.is_empty()) {
            self.model = model.to_string();
            self.span.set_attribute("gen_ai.request.model", model.to_string());
        }
    }

    pub(super) fn set_tool_used(&mut self, tool_used: bool) {
        self.tool_used = tool_used;
        self.span.set_attribute("eden.llm.tool_used", tool_used.to_string());
    }

    pub(super) fn set_payload_analysis(&mut self, analysis: &LlmPayloadAnalysis) {
        self.span.set_attribute("eden.llm.payload.message_count", analysis.message_count.to_string());
        self.span.set_attribute("eden.llm.payload.system_message_count", analysis.system_message_count.to_string());
        self.span.set_attribute("eden.llm.payload.user_message_count", analysis.user_message_count.to_string());
        self.span.set_attribute("eden.llm.payload.assistant_message_count", analysis.assistant_message_count.to_string());
        self.span.set_attribute("eden.llm.payload.tool_message_count", analysis.tool_message_count.to_string());
        self.span.set_attribute("eden.llm.payload.text_part_count", analysis.text_part_count.to_string());
        self.span.set_attribute("eden.llm.payload.image_part_count", analysis.image_part_count.to_string());
        self.span.set_attribute("eden.llm.payload.tool_definition_count", analysis.tool_definition_count.to_string());
        self.span.set_attribute("eden.llm.payload.tool_call_count", analysis.tool_call_count.to_string());
        self.span.set_attribute("eden.llm.payload.prompt_characters", analysis.prompt_characters.to_string());
        self.span.set_attribute("eden.llm.payload.contains_pii", analysis.contains_pii().to_string());

        if analysis.contains_pii() {
            self.span.add_event(
                "llm payload pii detected",
                vec![
                    FastSpanAttribute::new("eden.llm.payload.pii.email_count", analysis.pii.email_count.to_string()),
                    FastSpanAttribute::new("eden.llm.payload.pii.phone_count", analysis.pii.phone_count.to_string()),
                    FastSpanAttribute::new("eden.llm.payload.pii.us_ssn_count", analysis.pii.us_ssn_count.to_string()),
                    FastSpanAttribute::new("eden.llm.payload.pii.payment_card_count", analysis.pii.payment_card_count.to_string()),
                ],
            );
        }
    }

    pub(super) fn set_auth_decision(&mut self, decision: &LlmGatewayAuthDecision) {
        self.span.set_attribute("eden.llm.gateway.auth.mode", decision.mode);
        self.span.set_attribute("eden.llm.gateway.auth.action", decision.action);
        self.span.set_attribute("eden.llm.gateway.auth.reason", decision.reason);
        if let Some(key_kind) = decision.key_kind {
            self.span.set_attribute("eden.llm.gateway.auth.key_kind", key_kind);
        }
    }

    pub(super) fn set_agent_identity(&mut self, identity: &LlmGatewayAgentIdentity) {
        if identity.is_empty() {
            return;
        }

        if let Some(agent_id) = identity.agent_id.as_deref() {
            self.span.set_attribute("eden.llm.gateway.agent.id", agent_id.to_string());
        }
        if let Some(fingerprint) = identity.fingerprint.as_deref() {
            self.span.set_attribute("eden.llm.gateway.agent.fingerprint", fingerprint.to_string());
        }
        if let Some(session_id) = identity.session_id.as_deref() {
            self.span.set_attribute("eden.llm.gateway.agent.session_id", session_id.to_string());
        }
        if let Some(principal) = identity.principal.as_deref() {
            self.span.set_attribute("eden.llm.gateway.agent.principal", principal.to_string());
        }
        self.span.set_attribute("eden.llm.gateway.agent.tag_count", identity.tags.len().to_string());
    }

    pub(super) fn set_control_plane_source(&mut self, source: &'static str) {
        self.span.set_attribute("eden.llm.gateway.control_plane.source", source);
    }

    pub(super) fn set_feature_decision(&mut self, decision: &LlmGatewayFeatureDecision) {
        self.span.set_attribute("eden.llm.gateway.policy.action", decision.action_name());
        self.span.set_attribute("eden.llm.gateway.policy.model", decision.model_policy_action);
        self.span.set_attribute("eden.llm.gateway.policy.budget", decision.budget_action);
        self.span.set_attribute("eden.llm.gateway.policy.request_pii", decision.request_pii_action);
        self.span.set_attribute("eden.llm.gateway.policy.prompt_security", decision.prompt_security_action);
        self.span.set_attribute("eden.llm.gateway.policy.tool", decision.tool_policy_action);
        self.span.set_attribute("eden.llm.gateway.routing.class", decision.routing_class.clone());
        self.span.set_attribute("eden.llm.gateway.response_cache", decision.response_cache_action);
        self.span.set_attribute("eden.llm.gateway.eval", decision.eval_action);
        self.span.set_attribute("eden.llm.gateway.observability", decision.observability_mode);
        self.span.set_attribute("eden.llm.gateway.streaming_inspection", decision.streaming_inspection_mode);
        self.span.set_attribute("eden.llm.gateway.prompt_security.risk_level", decision.prompt_security_risk.level());
        self.route_class = decision.routing_class.clone();

        if decision.request_pii_redactions.contains_pii() {
            self.span.add_event(
                "llm request pii redacted",
                vec![
                    FastSpanAttribute::new(
                        "eden.llm.gateway.request_pii.redacted.email_count",
                        decision.request_pii_redactions.email_count.to_string(),
                    ),
                    FastSpanAttribute::new(
                        "eden.llm.gateway.request_pii.redacted.phone_count",
                        decision.request_pii_redactions.phone_count.to_string(),
                    ),
                    FastSpanAttribute::new(
                        "eden.llm.gateway.request_pii.redacted.us_ssn_count",
                        decision.request_pii_redactions.us_ssn_count.to_string(),
                    ),
                    FastSpanAttribute::new(
                        "eden.llm.gateway.request_pii.redacted.payment_card_count",
                        decision.request_pii_redactions.payment_card_count.to_string(),
                    ),
                ],
            );
        }
    }

    pub(super) fn set_route_decision(&mut self, decision: &LlmGatewayRouteDecision) {
        self.route_class = decision.route_class.clone();
        self.span.set_attribute("eden.llm.gateway.routing.requested_provider", decision.requested_provider.clone());
        self.span.set_attribute("eden.llm.gateway.routing.requested_model", decision.requested_model.clone());
        self.span.set_attribute("eden.llm.gateway.routing.selected_provider", decision.selected_provider.clone());
        self.span.set_attribute("eden.llm.gateway.routing.selected_model", decision.selected_model.clone());
        self.span.set_attribute("eden.llm.gateway.routing.reason", decision.reason.clone());
        self.span.set_attribute("eden.llm.gateway.routing.model_rewritten", decision.model_rewritten.to_string());
        self.span.set_attribute("eden.llm.gateway.routing.price_arbitrage_mode", decision.price_arbitrage_mode.to_string());
        self.span.set_attribute("eden.llm.gateway.routing.optimization_mode", decision.route_optimization_mode.to_string());
        self.span.set_attribute(
            "eden.llm.gateway.routing.baseline_estimated_cost_micros",
            decision.baseline_estimated_cost_micros.to_string(),
        );
        self.span.set_attribute(
            "eden.llm.gateway.routing.selected_estimated_cost_micros",
            decision.selected_estimated_cost_micros.to_string(),
        );
        self.span
            .set_attribute("eden.llm.gateway.routing.estimated_savings_micros", decision.estimated_savings_micros.to_string());
        self.span.set_attribute("eden.llm.gateway.routing.stats_sample_count", decision.route_stats_sample_count.to_string());
        if let Some(average_latency_ms) = decision.selected_average_latency_ms {
            self.span.set_attribute("eden.llm.gateway.routing.selected_average_latency_ms", average_latency_ms.to_string());
        }
        if let Some(output_tokens_per_second_milli) = decision.selected_output_tokens_per_second_milli {
            self.span.set_attribute(
                "eden.llm.gateway.routing.selected_output_tokens_per_second_milli",
                output_tokens_per_second_milli.to_string(),
            );
        }
        if let Some(error_rate_per_million) = decision.selected_error_rate_per_million {
            self.span.set_attribute("eden.llm.gateway.routing.selected_error_rate_per_million", error_rate_per_million.to_string());
        }
        if let Some(price_source) = decision.price_source {
            self.span.set_attribute("eden.llm.gateway.routing.price_source", price_source.to_string());
        }
    }

    pub(super) fn set_response_inspection(&mut self, inspection: &LlmResponseInspection) {
        self.span.set_attribute("eden.llm.gateway.response_policy.action", inspection.action_name());
        self.span.set_attribute("eden.llm.gateway.policy.response_pii", inspection.pii_action);
        self.span.set_attribute("eden.llm.response.contains_pii", inspection.pii.contains_pii().to_string());

        if inspection.pii.contains_pii() {
            self.span.add_event(
                "llm response pii detected",
                vec![
                    FastSpanAttribute::new("eden.llm.response.pii.email_count", inspection.pii.email_count.to_string()),
                    FastSpanAttribute::new("eden.llm.response.pii.phone_count", inspection.pii.phone_count.to_string()),
                    FastSpanAttribute::new("eden.llm.response.pii.us_ssn_count", inspection.pii.us_ssn_count.to_string()),
                    FastSpanAttribute::new("eden.llm.response.pii.payment_card_count", inspection.pii.payment_card_count.to_string()),
                ],
            );
        }

        if inspection.pii_redactions.contains_pii() {
            self.span.add_event(
                "llm response pii redacted",
                vec![
                    FastSpanAttribute::new("eden.llm.response.pii.redacted.email_count", inspection.pii_redactions.email_count.to_string()),
                    FastSpanAttribute::new("eden.llm.response.pii.redacted.phone_count", inspection.pii_redactions.phone_count.to_string()),
                    FastSpanAttribute::new(
                        "eden.llm.response.pii.redacted.us_ssn_count",
                        inspection.pii_redactions.us_ssn_count.to_string(),
                    ),
                    FastSpanAttribute::new(
                        "eden.llm.response.pii.redacted.payment_card_count",
                        inspection.pii_redactions.payment_card_count.to_string(),
                    ),
                ],
            );
        }
    }

    pub(super) fn tool_used(&self) -> bool {
        self.tool_used
    }

    pub(super) fn set_endpoint_uuid(&mut self, endpoint_cache_uuid: &EndpointCacheUuid) {
        self.context.set_endpoint_uuid(endpoint_cache_uuid);
        self.span.set_attribute("eden.endpoint_uuid", self.context.endpoint_uuid_label());
    }

    pub(super) fn set_provider_metadata(&mut self, metadata: Option<&LlmProviderMetadata>) {
        let Some(metadata) = metadata else {
            return;
        };

        if !metadata.provider.is_empty() {
            self.provider = metadata.provider.clone();
            self.span.set_attribute("gen_ai.provider.name", metadata.provider.clone());
        }
        if !metadata.model.is_empty() {
            self.model = metadata.model.clone();
            self.span.set_attribute("gen_ai.response.model", metadata.model.clone());
        }
        if let Some(base_url) = &metadata.base_url {
            self.span.set_attribute("server.address", base_url.clone());
        }
    }

    pub(super) fn response_model(&self) -> Option<&str> {
        (self.model != "unknown").then_some(self.model.as_str())
    }

    pub(super) fn record_time_per_output_chunk(&self, duration_us: u64) {
        let streaming = GatewayTelemetry::bool_label(self.streaming);
        let tool_used = GatewayTelemetry::bool_label(self.tool_used);
        let endpoint_uuid = self.context.endpoint_uuid_label();
        let org_uuid = self.context.org_uuid_label();
        let labels = [
            ("method", self.context.method()),
            ("route", self.context.route()),
            ("streaming", streaming),
            ("tool_used", tool_used),
            ("auth_scheme", self.context.auth_scheme()),
            ("provider", self.provider.as_str()),
            ("model", self.model.as_str()),
            ("endpoint_uuid", endpoint_uuid.as_str()),
            ("org_uuid", org_uuid.as_str()),
        ];

        self.metrics.eden().record_llm_gateway_time_per_output_chunk(duration_us, &labels);
    }

    pub(super) fn finish(&mut self, outcome: LlmGatewayOutcome<'_>) {
        if self.finished {
            return;
        }
        self.finished = true;

        let duration_us = GatewayTelemetry::elapsed_since_us(self.started_at);
        self.record_route_observation(duration_us, &outcome);
        let status_code = outcome.status.to_string();
        let status_class = GatewayTelemetry::status_class(outcome.status);
        let error_type = outcome.error_type.unwrap_or("none");
        let streaming = GatewayTelemetry::bool_label(self.streaming);
        let tool_used = GatewayTelemetry::bool_label(self.tool_used);
        let endpoint_uuid = self.context.endpoint_uuid_label();
        let org_uuid = self.context.org_uuid_label();

        let labels = [
            ("method", self.context.method()),
            ("route", self.context.route()),
            ("status_code", status_code.as_str()),
            ("status_class", status_class),
            ("error_type", error_type),
            ("streaming", streaming),
            ("tool_used", tool_used),
            ("auth_scheme", self.context.auth_scheme()),
            ("provider", self.provider.as_str()),
            ("model", self.model.as_str()),
            ("endpoint_uuid", endpoint_uuid.as_str()),
            ("org_uuid", org_uuid.as_str()),
        ];

        if outcome.status >= 500 {
            log_error!(
                self.ctx.clone(),
                "LLM gateway request failed",
                audience = LogAudience::Internal,
                status_code = outcome.status,
                status_class = status_class,
                error_type = error_type,
                method = self.context.method(),
                route = self.context.route(),
                duration_us = duration_us,
                streaming = self.streaming,
                tool_used = self.tool_used,
                auth_scheme = self.context.auth_scheme(),
                provider = self.provider.as_str(),
                model = self.model.as_str(),
                endpoint_uuid = endpoint_uuid.as_str(),
                org_uuid = org_uuid.as_str()
            );
        } else if outcome.status >= 400 {
            log_warn!(
                self.ctx.clone(),
                "LLM gateway request rejected",
                audience = LogAudience::Internal,
                status_code = outcome.status,
                status_class = status_class,
                error_type = error_type,
                method = self.context.method(),
                route = self.context.route(),
                duration_us = duration_us,
                streaming = self.streaming,
                tool_used = self.tool_used,
                auth_scheme = self.context.auth_scheme(),
                provider = self.provider.as_str(),
                model = self.model.as_str(),
                endpoint_uuid = endpoint_uuid.as_str(),
                org_uuid = org_uuid.as_str()
            );
        }

        let eden_metrics = self.metrics.eden();
        eden_metrics.record_llm_gateway_request(duration_us, &labels);
        eden_metrics.record_llm_gateway_stream_chunks(outcome.stream_chunks, &labels);
        if let Some(first_chunk_at) = outcome.first_chunk_at {
            eden_metrics
                .record_llm_gateway_time_to_first_chunk(GatewayTelemetry::elapsed_between_us(self.started_at, first_chunk_at), &labels);
        }

        if outcome.status >= 400 {
            eden_metrics.record_llm_gateway_error(&labels);
            self.span.set_status(FastSpanStatus::Error {
                message: Cow::Owned(outcome.error_type.unwrap_or("llm_gateway_error").to_string()),
            });
        } else {
            self.span.set_status(FastSpanStatus::Ok);
        }

        self.context.set_response_span_attributes(&mut self.span, &status_code);
        self.span.set_attribute("gen_ai.provider.name", self.provider.clone());
        self.span.set_attribute("gen_ai.response.model", self.model.clone());
        let operation = if self.context.route() == "chat.completions" {
            "chat"
        } else {
            "http"
        };
        self.span.set_attribute("gen_ai.operation.name", operation);
        self.span.set_attribute("eden.llm.gateway.duration_us", duration_us.to_string());
        if outcome.stream_chunks > 0 {
            self.span.set_attribute("eden.llm.gateway.stream_chunks", outcome.stream_chunks.to_string());
        }

        if self.context.route() == "chat.completions"
            && self.provider != "unknown"
            && self.model != "unknown"
            && let Some(org_uuid) = self.context.org_uuid()
        {
            let usage = outcome.usage;
            eden_metrics.record_llm_usage(
                usage.map(|usage| u64::from(usage.prompt_tokens)),
                usage.map(|usage| u64::from(usage.completion_tokens)),
                usage.map(|usage| u64::from(usage.total_tokens)),
                &self.provider,
                &self.model,
                self.context.endpoint_uuid(),
                org_uuid,
                self.tool_used,
                self.streaming,
            );
            if let Some(usage) = usage {
                eden_metrics.record_llm_token_details(
                    usage.prompt_tokens_details.as_ref().and_then(|details| details.cached_tokens.map(u64::from)),
                    usage.prompt_tokens_details.as_ref().and_then(|details| details.audio_tokens.map(u64::from)),
                    usage.completion_tokens_details.as_ref().and_then(|details| details.reasoning_tokens.map(u64::from)),
                    usage.completion_tokens_details.as_ref().and_then(|details| details.audio_tokens.map(u64::from)),
                    &self.provider,
                    &self.model,
                    self.context.endpoint_uuid(),
                    org_uuid,
                    self.tool_used,
                    self.streaming,
                );
                self.span.add_event(
                    "llm usage",
                    vec![
                        FastSpanAttribute::new("gen_ai.usage.input_tokens", usage.prompt_tokens.to_string()),
                        FastSpanAttribute::new("gen_ai.usage.output_tokens", usage.completion_tokens.to_string()),
                        FastSpanAttribute::new("gen_ai.usage.total_tokens", usage.total_tokens.to_string()),
                    ],
                );
            }
        }
    }

    fn record_route_observation(&self, duration_us: u64, outcome: &LlmGatewayOutcome<'_>) {
        if self.context.route() != "chat.completions" || self.provider == "unknown" || self.model == "unknown" {
            return;
        }

        record_llm_gateway_route_observation(LlmGatewayRouteObservation {
            provider: self.provider.clone(),
            model: self.model.clone(),
            route_class: self.route_class.clone(),
            latency_ms: duration_us.saturating_add(999) / 1_000,
            output_tokens: outcome.usage.map(|usage| u64::from(usage.completion_tokens)).unwrap_or_default(),
            success: outcome.status < 500,
        });
    }
}

pub(super) struct LlmGatewayOutcome<'a> {
    status: u16,
    error_type: Option<&'static str>,
    usage: Option<&'a LlmUsage>,
    stream_chunks: u64,
    first_chunk_at: Option<Instant>,
}

impl<'a> LlmGatewayOutcome<'a> {
    pub(super) fn status(status: u16) -> Self {
        Self {
            status,
            error_type: None,
            usage: None,
            stream_chunks: 0,
            first_chunk_at: None,
        }
    }

    pub(super) fn error(status: u16, error_type: &'static str) -> Self {
        Self {
            status,
            error_type: Some(error_type),
            usage: None,
            stream_chunks: 0,
            first_chunk_at: None,
        }
    }

    pub(super) fn success_with_usage(usage: Option<&'a LlmUsage>) -> Self {
        Self {
            status: 200,
            error_type: None,
            usage,
            stream_chunks: 0,
            first_chunk_at: None,
        }
    }

    pub(super) fn with_usage(mut self, usage: Option<&'a LlmUsage>) -> Self {
        self.usage = usage;
        self
    }

    pub(super) fn with_stream_chunks(mut self, stream_chunks: u64) -> Self {
        self.stream_chunks = stream_chunks;
        self
    }

    pub(super) fn with_first_chunk_at(mut self, first_chunk_at: Option<Instant>) -> Self {
        self.first_chunk_at = first_chunk_at;
        self
    }
}

pub(super) struct LlmUsageAccumulator;

impl LlmUsageAccumulator {
    pub(super) fn accumulate(accumulator: &mut Option<LlmUsage>, usage: LlmUsage) {
        match accumulator {
            Some(existing) => {
                existing.prompt_tokens = existing.prompt_tokens.saturating_add(usage.prompt_tokens);
                existing.completion_tokens = existing.completion_tokens.saturating_add(usage.completion_tokens);
                existing.total_tokens = existing.prompt_tokens.saturating_add(existing.completion_tokens);
                if let Some(incoming) = usage.completion_tokens_details {
                    match &mut existing.completion_tokens_details {
                        Some(existing) => {
                            existing.reasoning_tokens = Self::add_optional_u32(existing.reasoning_tokens, incoming.reasoning_tokens);
                            existing.audio_tokens = Self::add_optional_u32(existing.audio_tokens, incoming.audio_tokens);
                        }
                        None => existing.completion_tokens_details = Some(incoming),
                    }
                }
                if let Some(incoming) = usage.prompt_tokens_details {
                    match &mut existing.prompt_tokens_details {
                        Some(existing) => {
                            existing.cached_tokens = Self::add_optional_u32(existing.cached_tokens, incoming.cached_tokens);
                            existing.audio_tokens = Self::add_optional_u32(existing.audio_tokens, incoming.audio_tokens);
                        }
                        None => existing.prompt_tokens_details = Some(incoming),
                    }
                }
            }
            None => *accumulator = Some(usage),
        }
    }

    fn add_optional_u32(left: Option<u32>, right: Option<u32>) -> Option<u32> {
        match (left, right) {
            (Some(left), Some(right)) => Some(left.saturating_add(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        }
    }
}

struct LlmGatewayRoute;

impl LlmGatewayRoute {
    fn name(path: &str) -> &'static str {
        match path {
            "/health" | "/v1/health" => "health",
            "/v1/models" => "models",
            "/v1/chat/completions" | "/chat/completions" => "chat.completions",
            _ => "unknown",
        }
    }
}
