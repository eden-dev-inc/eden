use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use eden_core::format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use eden_core::format::endpoint::EpKind;
use eden_core::telemetry::TelemetryWrapper;
use eden_gateway_core::response::{GatewayQueueResponseSender as LlmResponseSender, GatewayResponsePolicySpec, GatewayResponseProfile};
use eden_gateway_core::traits::{BytesQueueSender, DatabaseProtocolProcessor, ProxyRequestChunk};
use eden_logger_internal::{LogAudience, LogContext, log_debug, log_info};
use endpoint_core::llm_core::{
    LLM_GATEWAY_AGENT_FINGERPRINT_HEADER, LLM_GATEWAY_AGENT_ID_HEADER, LLM_GATEWAY_AGENT_PRINCIPAL_HEADER,
    LLM_GATEWAY_AGENT_SESSION_HEADER, LLM_GATEWAY_AGENT_TAGS_HEADER, LlmGatewayAgentIdentity,
};
use endpoints::endpoint::llm::ep::LlmEp;
use endpoints::endpoint::llm::{
    LlmChatResponse, LlmFunctionCall, LlmInvocation, LlmMessage, LlmMessageKind, LlmMessageRole, LlmRequestOverrides,
    LlmStructuredOutputFormat, LlmToolCall, LlmToolChoice, LlmToolDefinition, LlmUsage,
};
use ep_core::GetPool;
use ep_core::database::schema::interlay::InterlayState;
use ep_core::settings::EdenSettings;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::UnboundedReceiver;

mod analysis;
mod auth;
mod catalog;
mod control_plane;
mod features;
mod gateway_telemetry;
mod telemetry;
use self::analysis::LlmPayloadInspector;
use self::catalog::LlmGatewayModelCatalogResponder;
use self::control_plane::LlmGatewayControlPlane;
use self::features::LlmGatewayFeatureEngine;
use self::telemetry::{LlmGatewayOutcome, LlmGatewayParseTelemetry, LlmGatewayRequestTelemetry, LlmUsageAccumulator};
use crate::gateway_telemetry::GatewayTelemetry;

const MAX_HTTP_HEADERS: usize = 64;
const MAX_LLM_HTTP_REQUEST_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct LlmProtocolProcessor {
    ep: LlmEp,
}

impl LlmProtocolProcessor {
    pub fn new(ep: LlmEp) -> Self {
        Self { ep }
    }
}

impl GatewayResponseProfile for LlmProtocolProcessor {
    type Observer = ();

    fn response_policy_spec(&self) -> GatewayResponsePolicySpec {
        GatewayResponsePolicySpec::new("llm", None)
    }
}

impl DatabaseProtocolProcessor for LlmProtocolProcessor {
    #[allow(clippy::too_many_arguments)]
    fn process(
        &self,
        receiver: UnboundedReceiver<ProxyRequestChunk>,
        sender: BytesQueueSender,
        _settings: EdenSettings,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        telemetry_wrapper: TelemetryWrapper,
        ctx: LogContext,
        client_addr: SocketAddr,
        _listener_id: String,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let ep = self.ep.clone();
        Box::pin(async move {
            LlmHttpConnection::process(ep, receiver, sender, interlay_cache_uuid, interlay_endpoints, telemetry_wrapper, ctx, client_addr)
                .await;
        })
    }
}

struct LlmHttpConnection;

impl LlmHttpConnection {
    #[allow(clippy::too_many_arguments)]
    async fn process(
        ep: LlmEp,
        mut receiver: UnboundedReceiver<ProxyRequestChunk>,
        sender: BytesQueueSender,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        mut telemetry_wrapper: TelemetryWrapper,
        ctx: LogContext,
        client_addr: SocketAddr,
    ) {
        log_info!(
            ctx.clone(),
            "LLM gateway connection established",
            audience = LogAudience::Internal,
            client_addr = client_addr.to_string()
        );

        let mut buffer = BytesMut::new();
        let mut buffered_request_received_at: Option<Instant> = None;

        while let Some(chunk) = receiver.recv().await {
            if chunk.is_empty() {
                continue;
            }

            if buffer.is_empty() {
                buffered_request_received_at = Some(Instant::now());
            }

            buffer.extend_from_slice(&chunk.into_bytes());

            loop {
                let request_received_at = buffered_request_received_at.unwrap_or_else(Instant::now);
                let parsed = match HttpRequestParser::try_parse(&buffer) {
                    Ok(Some(parsed)) => parsed,
                    Ok(None) => break,
                    Err(err) => {
                        LlmGatewayParseTelemetry::record(&mut telemetry_wrapper, err.status, &err.message, request_received_at, &ctx);
                        let response = HttpResponseBuilder::json_error(err.status, &err.message, true);
                        let _ = LlmResponseSender::send(&sender, response, request_received_at, &ctx);
                        return;
                    }
                };

                let request_bytes = buffer.split_to(parsed.total_len).freeze();
                let request = parsed.into_request(request_bytes);

                let close_after_response = request.should_close();
                let should_close = LlmHttpRouter::handle_request(
                    &ep,
                    request,
                    &sender,
                    request_received_at,
                    &interlay_cache_uuid,
                    &interlay_endpoints,
                    &mut telemetry_wrapper,
                    &ctx,
                    client_addr,
                )
                .await
                    || close_after_response;

                if should_close {
                    return;
                }

                if buffer.is_empty() {
                    buffered_request_received_at = None;
                    break;
                }

                buffered_request_received_at = Some(request_received_at);
            }
        }

        log_debug!(ctx, "LLM gateway connection closed", audience = LogAudience::Internal);
    }
}

struct LlmHttpRouter;

impl LlmHttpRouter {
    #[allow(clippy::too_many_arguments)]
    async fn handle_request(
        ep: &LlmEp,
        request: HttpRequest,
        sender: &BytesQueueSender,
        request_received_at: Instant,
        interlay_cache_uuid: &InterlayCacheUuid,
        interlay_endpoints: &DashMap<InterlayCacheUuid, InterlayState>,
        telemetry_wrapper: &mut TelemetryWrapper,
        ctx: &LogContext,
        client_addr: SocketAddr,
    ) -> bool {
        let path = request.path_without_query();
        let close = request.should_close();
        let mut request_telemetry = LlmGatewayRequestTelemetry::new(
            telemetry_wrapper,
            &request,
            interlay_cache_uuid,
            client_addr,
            request_received_at,
            ctx.clone(),
        );
        let resolved_control_plane = LlmGatewayControlPlane::global().resolve(&request);
        request_telemetry.set_control_plane_source(resolved_control_plane.source);
        let agent_identity = LlmGatewayAgentIdentity::from_parts(
            request.header(LLM_GATEWAY_AGENT_ID_HEADER),
            request.header(LLM_GATEWAY_AGENT_FINGERPRINT_HEADER),
            request.header(LLM_GATEWAY_AGENT_SESSION_HEADER),
            request.header(LLM_GATEWAY_AGENT_PRINCIPAL_HEADER),
            request.header(LLM_GATEWAY_AGENT_TAGS_HEADER),
        );
        request_telemetry.set_agent_identity(&agent_identity);

        if request.method.eq_ignore_ascii_case("OPTIONS") {
            request_telemetry.finish(LlmGatewayOutcome::status(204));
            let _ = LlmResponseSender::send(sender, HttpResponseBuilder::empty(204, close), request_received_at, ctx);
            return close;
        }

        if request.method.eq_ignore_ascii_case("GET") && matches!(path, "/health" | "/v1/health") {
            let body = json!({
                "status": "ok",
                "service": "eden-gateway",
                "protocol": "llm"
            });
            request_telemetry.finish(LlmGatewayOutcome::status(200));
            let _ = LlmResponseSender::send(sender, HttpResponseBuilder::json(200, &body, close), request_received_at, ctx);
            return close;
        }

        let auth_decision = resolved_control_plane.auth_policy.evaluate(&request);
        request_telemetry.set_auth_decision(&auth_decision);
        if let Some(block) = auth_decision.block_reason() {
            request_telemetry.finish(LlmGatewayOutcome::error(block.status, block.error_type));
            let response = HttpResponseBuilder::json_error(block.status, block.message, close);
            let _ = LlmResponseSender::send(sender, response, request_received_at, ctx);
            return close;
        }

        if request.method.eq_ignore_ascii_case("GET") && path == "/v1/models" {
            let body = LlmGatewayModelCatalogResponder::openai_models_response(
                &resolved_control_plane.feature_engine,
                &resolved_control_plane.model_catalog,
            );
            request_telemetry.finish(LlmGatewayOutcome::status(200));
            let _ = LlmResponseSender::send(sender, HttpResponseBuilder::json(200, &body, close), request_received_at, ctx);
            return close;
        }

        if !request.method.eq_ignore_ascii_case("POST") {
            request_telemetry.finish(LlmGatewayOutcome::error(405, "method_not_allowed"));
            let response = HttpResponseBuilder::json_error(405, "method not allowed", close);
            let _ = LlmResponseSender::send(sender, response, request_received_at, ctx);
            return close;
        }

        if !matches!(path, "/v1/chat/completions" | "/chat/completions") {
            request_telemetry.finish(LlmGatewayOutcome::error(404, "route_not_found"));
            let response = HttpResponseBuilder::json_error(404, "route not found", close);
            let _ = LlmResponseSender::send(sender, response, request_received_at, ctx);
            return close;
        }

        let feature_engine = resolved_control_plane.feature_engine;
        if feature_engine.allows_openai_passthrough() {
            let fast_request = match OpenAiPassthroughRequest::parse(request.body.as_ref()) {
                Ok(request) => request,
                Err(err) => {
                    request_telemetry.finish(LlmGatewayOutcome::error(400, "invalid_json"));
                    let response = HttpResponseBuilder::json_error(400, &format!("invalid JSON request body: {err}"), close);
                    let _ = LlmResponseSender::send(sender, response, request_received_at, ctx);
                    return close;
                }
            };

            if let Some(request_model) = fast_request.passthrough_model() {
                let stream = fast_request.stream();
                request_telemetry.set_streaming(stream);
                request_telemetry.set_requested_model(Some(request_model));
                if Self::try_handle_openai_passthrough(
                    ep,
                    request.body.clone(),
                    request_model,
                    stream,
                    sender,
                    request_received_at,
                    interlay_cache_uuid,
                    interlay_endpoints,
                    close,
                    &mut request_telemetry,
                    ctx,
                )
                .await
                {
                    return stream || close;
                }
            }
        }

        let mut openai_body = match serde_json::from_slice::<Value>(&request.body) {
            Ok(body) => body,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(400, "invalid_json"));
                let response = HttpResponseBuilder::json_error(400, &format!("invalid JSON request body: {err}"), close);
                let _ = LlmResponseSender::send(sender, response, request_received_at, ctx);
                return close;
            }
        };
        let payload_analysis = LlmPayloadInspector::inspect_openai_chat_value(&openai_body);
        request_telemetry.set_payload_analysis(&payload_analysis);
        let feature_decision = feature_engine.evaluate_request(&mut openai_body, &payload_analysis);
        request_telemetry.set_feature_decision(&feature_decision);
        if let Some(block) = feature_decision.block_reason() {
            request_telemetry.finish(LlmGatewayOutcome::error(block.status, block.error_type));
            let response = HttpResponseBuilder::json_error(block.status, &block.message, close);
            let _ = LlmResponseSender::send(sender, response, request_received_at, ctx);
            return close;
        }

        let openai_request = match serde_json::from_value::<OpenAiChatCompletionRequest>(openai_body) {
            Ok(request) => request,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(400, "invalid_request"));
                let response = HttpResponseBuilder::json_error(400, &format!("invalid JSON request body: {err}"), close);
                let _ = LlmResponseSender::send(sender, response, request_received_at, ctx);
                return close;
            }
        };

        let stream = openai_request.stream.unwrap_or(false);
        let prompt_token_estimate = LlmGatewayTokenEstimator::prompt_tokens(&payload_analysis);
        let completion_token_estimate = openai_request.max_completion_tokens.or(openai_request.max_tokens).unwrap_or(1_024);
        let route_class = feature_decision.routing_class.clone();
        request_telemetry.set_streaming(stream);
        request_telemetry.set_requested_model(openai_request.model.as_deref());
        request_telemetry.set_tool_used(!openai_request.tools.is_empty());
        let invocation = match openai_request.into_invocation() {
            Ok(invocation) => invocation,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(err.status, "invalid_request"));
                let response = HttpResponseBuilder::json_error(err.status, &err.message, close);
                let _ = LlmResponseSender::send(sender, response, request_received_at, ctx);
                return close;
            }
        };

        if stream {
            Self::handle_streaming_completion(
                ep,
                invocation,
                sender,
                request_received_at,
                interlay_cache_uuid,
                interlay_endpoints,
                &mut request_telemetry,
                &feature_engine,
                route_class,
                prompt_token_estimate,
                completion_token_estimate,
                ctx,
            )
            .await;
            return true;
        }

        Self::handle_completion(
            ep,
            invocation,
            sender,
            request_received_at,
            interlay_cache_uuid,
            interlay_endpoints,
            close,
            &mut request_telemetry,
            &feature_engine,
            route_class,
            prompt_token_estimate,
            completion_token_estimate,
            ctx,
        )
        .await;
        close
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_handle_openai_passthrough(
        ep: &LlmEp,
        request_body: Bytes,
        request_model: &str,
        stream: bool,
        sender: &BytesQueueSender,
        request_received_at: Instant,
        interlay_cache_uuid: &InterlayCacheUuid,
        interlay_endpoints: &DashMap<InterlayCacheUuid, InterlayState>,
        close: bool,
        request_telemetry: &mut LlmGatewayRequestTelemetry,
        ctx: &LogContext,
    ) -> bool {
        let endpoint_cache_uuid = match LlmEndpointResolver::resolve(interlay_cache_uuid, interlay_endpoints) {
            Ok(endpoint_cache_uuid) => endpoint_cache_uuid,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(err.status, "endpoint_resolve"));
                let _ = LlmResponseSender::send(
                    sender,
                    HttpResponseBuilder::json_error(err.status, &err.message, true),
                    request_received_at,
                    ctx,
                );
                return true;
            }
        };
        request_telemetry.set_endpoint_uuid(&endpoint_cache_uuid);

        let pool = match ep.pool().read_conn_async(&endpoint_cache_uuid).await {
            Ok(pool) => pool,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(502, "pool_read"));
                let _ =
                    LlmResponseSender::send(sender, HttpResponseBuilder::json_error(502, &err.to_string(), true), request_received_at, ctx);
                return true;
            }
        };

        let client = match pool.get().await {
            Ok(client) => client,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(502, "pool_checkout"));
                let _ =
                    LlmResponseSender::send(sender, HttpResponseBuilder::json_error(502, &err.to_string(), true), request_received_at, ctx);
                return true;
            }
        };

        if stream {
            return match client.chat_stream_openai_passthrough(request_body, request_model).await {
                Ok(Some(response)) => {
                    request_telemetry.set_provider_metadata(Some(&response.provider));
                    Self::send_openai_passthrough_stream(response, sender, request_received_at, request_telemetry, ctx).await;
                    true
                }
                Ok(None) => false,
                Err(err) => {
                    request_telemetry.finish(LlmGatewayOutcome::error(502, "upstream_request"));
                    let _ = LlmResponseSender::send(
                        sender,
                        HttpResponseBuilder::json_error(502, &err.to_string(), true),
                        request_received_at,
                        ctx,
                    );
                    true
                }
            };
        }

        match client.chat_openai_passthrough(request_body, request_model).await {
            Ok(Some(response)) => {
                let status = response.status;
                request_telemetry.set_provider_metadata(Some(&response.provider));
                request_telemetry.finish(Self::passthrough_outcome(status, 0, None));
                let _ = LlmResponseSender::send(
                    sender,
                    HttpResponseBuilder::raw(status, response.content_type.as_deref(), response.body, close),
                    request_received_at,
                    ctx,
                );
                true
            }
            Ok(None) => false,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(502, "upstream_request"));
                let _ =
                    LlmResponseSender::send(sender, HttpResponseBuilder::json_error(502, &err.to_string(), true), request_received_at, ctx);
                true
            }
        }
    }

    async fn send_openai_passthrough_stream(
        response: endpoint_core::llm_core::comm::LlmRawStreamResponse,
        sender: &BytesQueueSender,
        request_received_at: Instant,
        request_telemetry: &mut LlmGatewayRequestTelemetry,
        ctx: &LogContext,
    ) {
        let status = response.status;
        if !LlmResponseSender::send(
            sender,
            HttpResponseBuilder::raw_streaming_headers(status, response.content_type.as_deref()),
            request_received_at,
            ctx,
        ) {
            request_telemetry.finish(LlmGatewayOutcome::error(499, "client_disconnect"));
            return;
        }

        let mut stream = response.stream;
        let mut chunk_count = 0_u64;
        let mut first_chunk_at = None;
        let mut previous_chunk_at = None;

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => {
                    if chunk.is_empty() {
                        continue;
                    }
                    chunk_count = chunk_count.saturating_add(1);
                    let chunk_at = Instant::now();
                    if first_chunk_at.is_none() {
                        first_chunk_at = Some(chunk_at);
                    } else if let Some(previous) = previous_chunk_at {
                        request_telemetry.record_time_per_output_chunk(GatewayTelemetry::elapsed_between_us(previous, chunk_at));
                    }
                    previous_chunk_at = Some(chunk_at);
                    if !LlmResponseSender::send(sender, HttpResponseBuilder::raw_chunk(&chunk), request_received_at, ctx) {
                        request_telemetry.finish(
                            LlmGatewayOutcome::error(499, "client_disconnect")
                                .with_stream_chunks(chunk_count)
                                .with_first_chunk_at(first_chunk_at),
                        );
                        return;
                    }
                }
                Err(_err) => {
                    request_telemetry.finish(
                        LlmGatewayOutcome::error(502, "stream_chunk").with_stream_chunks(chunk_count).with_first_chunk_at(first_chunk_at),
                    );
                    let _ = LlmResponseSender::send(sender, HttpResponseBuilder::chunked_end(), request_received_at, ctx);
                    log_debug!(
                        ctx,
                        "LLM OpenAI pass-through stream chunk failed",
                        audience = eden_logger_internal::LogAudience::Internal
                    );
                    return;
                }
            }
        }

        if LlmResponseSender::send(sender, HttpResponseBuilder::chunked_end(), request_received_at, ctx) {
            request_telemetry.finish(Self::passthrough_outcome(status, chunk_count, first_chunk_at));
        } else {
            request_telemetry.finish(
                LlmGatewayOutcome::error(499, "client_disconnect").with_stream_chunks(chunk_count).with_first_chunk_at(first_chunk_at),
            );
        }
    }

    fn passthrough_outcome(status: u16, stream_chunks: u64, first_chunk_at: Option<Instant>) -> LlmGatewayOutcome<'static> {
        if status >= 400 {
            LlmGatewayOutcome::error(status, "upstream_status")
        } else {
            LlmGatewayOutcome::status(status)
        }
        .with_stream_chunks(stream_chunks)
        .with_first_chunk_at(first_chunk_at)
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_completion(
        ep: &LlmEp,
        mut invocation: LlmInvocation,
        sender: &BytesQueueSender,
        request_received_at: Instant,
        interlay_cache_uuid: &InterlayCacheUuid,
        interlay_endpoints: &DashMap<InterlayCacheUuid, InterlayState>,
        close: bool,
        request_telemetry: &mut LlmGatewayRequestTelemetry,
        feature_engine: &LlmGatewayFeatureEngine,
        route_class: String,
        prompt_token_estimate: u32,
        completion_token_estimate: u32,
        ctx: &LogContext,
    ) {
        let endpoint_cache_uuid = match LlmEndpointResolver::resolve(interlay_cache_uuid, interlay_endpoints) {
            Ok(endpoint_cache_uuid) => endpoint_cache_uuid,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(err.status, "endpoint_resolve"));
                let _ = LlmResponseSender::send(
                    sender,
                    HttpResponseBuilder::json_error(err.status, &err.message, true),
                    request_received_at,
                    ctx,
                );
                return;
            }
        };
        request_telemetry.set_endpoint_uuid(&endpoint_cache_uuid);

        let pool = match ep.pool().read_conn_async(&endpoint_cache_uuid).await {
            Ok(pool) => pool,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(502, "pool_read"));
                let _ =
                    LlmResponseSender::send(sender, HttpResponseBuilder::json_error(502, &err.to_string(), true), request_received_at, ctx);
                return;
            }
        };

        let client = match pool.get().await {
            Ok(client) => client,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(502, "pool_checkout"));
                let _ =
                    LlmResponseSender::send(sender, HttpResponseBuilder::json_error(502, &err.to_string(), true), request_received_at, ctx);
                return;
            }
        };

        let provider_metadata = client.provider_metadata(invocation.overrides.model.clone()).ok();
        request_telemetry.set_provider_metadata(provider_metadata.as_ref());
        Self::apply_route_decision(
            &client,
            &mut invocation,
            provider_metadata.as_ref(),
            request_telemetry,
            feature_engine,
            &route_class,
            prompt_token_estimate,
            completion_token_estimate,
        );

        match client.chat(&invocation).await {
            Ok(mut response) => {
                request_telemetry.set_provider_metadata(Some(&response.provider));
                let response_used_tools = matches!(&response.message.kind, LlmMessageKind::ToolUse { calls } if !calls.is_empty());
                request_telemetry.set_tool_used(request_telemetry.tool_used() || response_used_tools);
                let usage = response.usage.clone();
                if response_used_tools {
                    let mut body = OpenAiResponseMapper::chat_completion_response(response);
                    let response_inspection = feature_engine.inspect_response_value(&mut body);
                    request_telemetry.set_response_inspection(&response_inspection);
                    if let Some(block) = response_inspection.block_reason() {
                        request_telemetry.finish(LlmGatewayOutcome::error(block.status, block.error_type).with_usage(usage.as_ref()));
                        let _ = LlmResponseSender::send(
                            sender,
                            HttpResponseBuilder::json_error(block.status, &block.message, close),
                            request_received_at,
                            ctx,
                        );
                        return;
                    }
                    request_telemetry.finish(LlmGatewayOutcome::success_with_usage(usage.as_ref()));
                    let _ = LlmResponseSender::send(sender, HttpResponseBuilder::json(200, &body, close), request_received_at, ctx);
                } else {
                    let response_inspection = feature_engine.inspect_response_text_delta(&mut response.message.content);
                    request_telemetry.set_response_inspection(&response_inspection);
                    if let Some(block) = response_inspection.block_reason() {
                        request_telemetry.finish(LlmGatewayOutcome::error(block.status, block.error_type).with_usage(usage.as_ref()));
                        let _ = LlmResponseSender::send(
                            sender,
                            HttpResponseBuilder::json_error(block.status, &block.message, close),
                            request_received_at,
                            ctx,
                        );
                        return;
                    }
                    request_telemetry.finish(LlmGatewayOutcome::success_with_usage(usage.as_ref()));
                    let _ = LlmResponseSender::send(
                        sender,
                        HttpResponseBuilder::chat_completion_text(&response, close),
                        request_received_at,
                        ctx,
                    );
                }
            }
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(502, "upstream_request"));
                let _ =
                    LlmResponseSender::send(sender, HttpResponseBuilder::json_error(502, &err.to_string(), true), request_received_at, ctx);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_streaming_completion(
        ep: &LlmEp,
        mut invocation: LlmInvocation,
        sender: &BytesQueueSender,
        request_received_at: Instant,
        interlay_cache_uuid: &InterlayCacheUuid,
        interlay_endpoints: &DashMap<InterlayCacheUuid, InterlayState>,
        request_telemetry: &mut LlmGatewayRequestTelemetry,
        feature_engine: &LlmGatewayFeatureEngine,
        route_class: String,
        prompt_token_estimate: u32,
        completion_token_estimate: u32,
        ctx: &LogContext,
    ) {
        let endpoint_cache_uuid = match LlmEndpointResolver::resolve(interlay_cache_uuid, interlay_endpoints) {
            Ok(endpoint_cache_uuid) => endpoint_cache_uuid,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(err.status, "endpoint_resolve"));
                let _ = LlmResponseSender::send(
                    sender,
                    HttpResponseBuilder::json_error(err.status, &err.message, true),
                    request_received_at,
                    ctx,
                );
                return;
            }
        };
        request_telemetry.set_endpoint_uuid(&endpoint_cache_uuid);

        let pool = match ep.pool().read_conn_async(&endpoint_cache_uuid).await {
            Ok(pool) => pool,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(502, "pool_read"));
                let _ =
                    LlmResponseSender::send(sender, HttpResponseBuilder::json_error(502, &err.to_string(), true), request_received_at, ctx);
                return;
            }
        };

        let client = match pool.get().await {
            Ok(client) => client,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(502, "pool_checkout"));
                let _ =
                    LlmResponseSender::send(sender, HttpResponseBuilder::json_error(502, &err.to_string(), true), request_received_at, ctx);
                return;
            }
        };

        let provider_metadata = client.provider_metadata(invocation.overrides.model.clone()).ok();
        request_telemetry.set_provider_metadata(provider_metadata.as_ref());
        Self::apply_route_decision(
            &client,
            &mut invocation,
            provider_metadata.as_ref(),
            request_telemetry,
            feature_engine,
            &route_class,
            prompt_token_estimate,
            completion_token_estimate,
        );

        let mut stream = match client.chat_stream(&invocation).await {
            Ok(stream) => stream,
            Err(err) => {
                request_telemetry.finish(LlmGatewayOutcome::error(502, "stream_request"));
                let _ =
                    LlmResponseSender::send(sender, HttpResponseBuilder::json_error(502, &err.to_string(), true), request_received_at, ctx);
                return;
            }
        };

        if !LlmResponseSender::send(sender, HttpResponseBuilder::streaming_headers(), request_received_at, ctx) {
            request_telemetry.finish(LlmGatewayOutcome::error(499, "client_disconnect"));
            return;
        }
        let id = CompletionMetadata::completion_id();
        let created = CompletionMetadata::unix_timestamp_seconds();
        let model = request_telemetry.response_model().unwrap_or_default().to_string();
        let mut chunk_count = 0_u64;
        let mut usage = None;
        let mut first_chunk_at = None;
        let mut previous_chunk_at = None;

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => {
                    chunk_count = chunk_count.saturating_add(1);
                    let chunk_at = Instant::now();
                    if first_chunk_at.is_none() {
                        first_chunk_at = Some(chunk_at);
                    } else if let Some(previous) = previous_chunk_at {
                        request_telemetry.record_time_per_output_chunk(GatewayTelemetry::elapsed_between_us(previous, chunk_at));
                    }
                    previous_chunk_at = Some(chunk_at);
                    if !chunk.tool_calls.is_empty() {
                        request_telemetry.set_tool_used(true);
                    }
                    let delta = chunk.delta;
                    let tool_calls = chunk.tool_calls;
                    let chunk_usage = chunk.usage;
                    if let Some(chunk_usage) = chunk_usage.clone() {
                        LlmUsageAccumulator::accumulate(&mut usage, chunk_usage);
                    }
                    match (delta, tool_calls, chunk_usage) {
                        (Some(mut delta), tool_calls, None) if tool_calls.is_empty() => {
                            let response_inspection = feature_engine.inspect_response_text_delta(&mut delta);
                            request_telemetry.set_response_inspection(&response_inspection);
                            if let Some(block) = response_inspection.block_reason() {
                                request_telemetry.finish(
                                    LlmGatewayOutcome::error(block.status, block.error_type)
                                        .with_usage(usage.as_ref())
                                        .with_stream_chunks(chunk_count)
                                        .with_first_chunk_at(first_chunk_at),
                                );
                                let error = json!({
                                    "error": {
                                        "message": block.message,
                                        "type": "eden_gateway_error",
                                        "code": block.status
                                    }
                                });
                                let _ = LlmResponseSender::send(sender, HttpResponseBuilder::sse_json(&error), request_received_at, ctx);
                                let _ = LlmResponseSender::send(sender, HttpResponseBuilder::chunked_end(), request_received_at, ctx);
                                return;
                            }
                            if !LlmResponseSender::send(
                                sender,
                                HttpResponseBuilder::sse_chat_text_delta(&id, created, &model, &delta),
                                request_received_at,
                                ctx,
                            ) {
                                request_telemetry.finish(
                                    LlmGatewayOutcome::error(499, "client_disconnect")
                                        .with_usage(usage.as_ref())
                                        .with_stream_chunks(chunk_count)
                                        .with_first_chunk_at(first_chunk_at),
                                );
                                return;
                            }
                        }
                        (delta, tool_calls, chunk_usage) => {
                            let mut body = OpenAiResponseMapper::stream_chunk(&id, created, &model, delta, tool_calls, chunk_usage);
                            let response_inspection = feature_engine.inspect_response_value(&mut body);
                            request_telemetry.set_response_inspection(&response_inspection);
                            if let Some(block) = response_inspection.block_reason() {
                                request_telemetry.finish(
                                    LlmGatewayOutcome::error(block.status, block.error_type)
                                        .with_usage(usage.as_ref())
                                        .with_stream_chunks(chunk_count)
                                        .with_first_chunk_at(first_chunk_at),
                                );
                                let error = json!({
                                    "error": {
                                        "message": block.message,
                                        "type": "eden_gateway_error",
                                        "code": block.status
                                    }
                                });
                                let _ = LlmResponseSender::send(sender, HttpResponseBuilder::sse_json(&error), request_received_at, ctx);
                                let _ = LlmResponseSender::send(sender, HttpResponseBuilder::chunked_end(), request_received_at, ctx);
                                return;
                            }
                            if !LlmResponseSender::send(sender, HttpResponseBuilder::sse_json(&body), request_received_at, ctx) {
                                request_telemetry.finish(
                                    LlmGatewayOutcome::error(499, "client_disconnect")
                                        .with_usage(usage.as_ref())
                                        .with_stream_chunks(chunk_count)
                                        .with_first_chunk_at(first_chunk_at),
                                );
                                return;
                            }
                        }
                    }
                }
                Err(err) => {
                    request_telemetry.finish(
                        LlmGatewayOutcome::error(502, "stream_chunk")
                            .with_usage(usage.as_ref())
                            .with_stream_chunks(chunk_count)
                            .with_first_chunk_at(first_chunk_at),
                    );
                    let body = json!({
                        "error": {
                            "message": err.to_string(),
                            "type": "eden_gateway_error",
                            "code": 502
                        }
                    });
                    let _ = LlmResponseSender::send(sender, HttpResponseBuilder::sse_json(&body), request_received_at, ctx);
                    let _ = LlmResponseSender::send(sender, HttpResponseBuilder::chunked_end(), request_received_at, ctx);
                    return;
                }
            }
        }

        if LlmResponseSender::send(sender, HttpResponseBuilder::sse_done_chunk(), request_received_at, ctx) {
            request_telemetry.finish(
                LlmGatewayOutcome::success_with_usage(usage.as_ref())
                    .with_stream_chunks(chunk_count)
                    .with_first_chunk_at(first_chunk_at),
            );
        } else {
            request_telemetry.finish(
                LlmGatewayOutcome::error(499, "client_disconnect")
                    .with_usage(usage.as_ref())
                    .with_stream_chunks(chunk_count)
                    .with_first_chunk_at(first_chunk_at),
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_route_decision(
        client: &endpoint_core::llm_core::comm::LlmClient,
        invocation: &mut LlmInvocation,
        provider_metadata: Option<&endpoints::endpoint::llm::LlmProviderMetadata>,
        request_telemetry: &mut LlmGatewayRequestTelemetry,
        feature_engine: &LlmGatewayFeatureEngine,
        route_class: &str,
        prompt_token_estimate: u32,
        completion_token_estimate: u32,
    ) {
        let Some(provider_metadata) = provider_metadata else {
            return;
        };

        let requested_model = invocation.overrides.model.as_deref().filter(|model| !model.is_empty()).unwrap_or(&provider_metadata.model);
        let route_decision = feature_engine.select_route(
            &provider_metadata.provider,
            requested_model,
            route_class,
            prompt_token_estimate,
            completion_token_estimate,
        );
        request_telemetry.set_route_decision(&route_decision);

        if route_decision.selected_model_changed() {
            invocation.overrides.model = Some(route_decision.selected_model.clone());
            let selected_metadata = client.provider_metadata(invocation.overrides.model.clone()).ok();
            request_telemetry.set_provider_metadata(selected_metadata.as_ref());
        }
    }
}

struct LlmEndpointResolver;

impl LlmEndpointResolver {
    fn resolve(
        interlay_cache_uuid: &InterlayCacheUuid,
        interlay_endpoints: &DashMap<InterlayCacheUuid, InterlayState>,
    ) -> Result<EndpointCacheUuid, GatewayHttpError> {
        let Some(state) = interlay_endpoints.get(interlay_cache_uuid) else {
            return Err(GatewayHttpError::new(503, "interlay endpoint is not available"));
        };

        if state.endpoint_kind() != EpKind::Llm {
            return Err(GatewayHttpError::new(400, "interlay is not configured for an LLM endpoint"));
        }

        Ok(state.endpoint_uuid().clone())
    }
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    version: u8,
    headers: Vec<(String, String)>,
    body: Bytes,
}

impl HttpRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find_map(|(header_name, value)| header_name.eq_ignore_ascii_case(name).then_some(value.as_str()))
    }

    fn should_close(&self) -> bool {
        match self.header("connection") {
            Some(value) if value.eq_ignore_ascii_case("close") => true,
            Some(value) if value.eq_ignore_ascii_case("keep-alive") => false,
            _ => self.version == 0,
        }
    }

    fn path_without_query(&self) -> &str {
        self.path.split_once('?').map_or(self.path.as_str(), |(path, _)| path)
    }
}

#[derive(Debug)]
struct ParsedHttpRequest {
    method: String,
    path: String,
    version: u8,
    headers: Vec<(String, String)>,
    header_len: usize,
    total_len: usize,
}

impl ParsedHttpRequest {
    fn into_request(self, bytes: Bytes) -> HttpRequest {
        let body = bytes.slice(self.header_len..self.total_len);
        HttpRequest {
            method: self.method,
            path: self.path,
            version: self.version,
            headers: self.headers,
            body,
        }
    }
}

struct HttpRequestParser;

impl HttpRequestParser {
    fn try_parse(buffer: &BytesMut) -> Result<Option<ParsedHttpRequest>, GatewayHttpError> {
        if buffer.len() > MAX_LLM_HTTP_REQUEST_BYTES {
            return Err(GatewayHttpError::new(413, "request exceeds maximum LLM gateway payload size"));
        }

        let mut headers = [httparse::EMPTY_HEADER; MAX_HTTP_HEADERS];
        let mut request = httparse::Request::new(&mut headers);
        let header_len = match request.parse(buffer) {
            Ok(httparse::Status::Complete(header_len)) => header_len,
            Ok(httparse::Status::Partial) => return Ok(None),
            Err(httparse::Error::TooManyHeaders) => return Err(GatewayHttpError::new(431, "too many HTTP headers")),
            Err(err) => return Err(GatewayHttpError::new(400, format!("invalid HTTP request: {err}"))),
        };

        let method = request.method.ok_or_else(|| GatewayHttpError::new(400, "missing HTTP method"))?.to_string();
        let path = request.path.ok_or_else(|| GatewayHttpError::new(400, "missing HTTP path"))?.to_string();
        let version = request.version.ok_or_else(|| GatewayHttpError::new(400, "missing HTTP version"))?;
        let headers = request
            .headers
            .iter()
            .map(|header| {
                let value = String::from_utf8_lossy(header.value).trim().to_string();
                (header.name.to_string(), value)
            })
            .collect::<Vec<_>>();

        if Self::has_chunked_transfer_encoding(&headers) {
            return Err(GatewayHttpError::new(501, "chunked request bodies are not supported by the LLM gateway"));
        }

        let content_len = Self::content_length(&headers)?;
        let total_len = header_len
            .checked_add(content_len)
            .ok_or_else(|| GatewayHttpError::new(413, "request exceeds maximum LLM gateway payload size"))?;

        if total_len > MAX_LLM_HTTP_REQUEST_BYTES {
            return Err(GatewayHttpError::new(413, "request exceeds maximum LLM gateway payload size"));
        }

        if buffer.len() < total_len {
            return Ok(None);
        }

        Ok(Some(ParsedHttpRequest { method, path, version, headers, header_len, total_len }))
    }

    fn has_chunked_transfer_encoding(headers: &[(String, String)]) -> bool {
        headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("transfer-encoding") && value.split(',').any(|part| part.trim().eq_ignore_ascii_case("chunked"))
        })
    }

    fn content_length(headers: &[(String, String)]) -> Result<usize, GatewayHttpError> {
        let mut parsed: Option<usize> = None;
        for (name, value) in headers {
            if !name.eq_ignore_ascii_case("content-length") {
                continue;
            }

            let len = value.parse::<usize>().map_err(|_| GatewayHttpError::new(400, "invalid Content-Length header"))?;

            if parsed.is_some_and(|previous| previous != len) {
                return Err(GatewayHttpError::new(400, "conflicting Content-Length headers"));
            }

            parsed = Some(len);
        }

        Ok(parsed.unwrap_or(0))
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiFastRequestShape {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    stream: Option<bool>,
}

impl OpenAiFastRequestShape {
    fn passthrough_model(&self) -> Option<&str> {
        self.model.as_deref().map(str::trim).filter(|model| !model.is_empty())
    }
}

enum OpenAiPassthroughRequest<'a> {
    Borrowed(OpenAiFastRequestView<'a>),
    Owned(OpenAiFastRequestShape),
}

impl<'a> OpenAiPassthroughRequest<'a> {
    fn parse(body: &'a [u8]) -> Result<Self, serde_json::Error> {
        if let Some(view) = OpenAiFastRequestView::scan(body) {
            return Ok(Self::Borrowed(view));
        }

        serde_json::from_slice::<OpenAiFastRequestShape>(body).map(Self::Owned)
    }

    fn passthrough_model(&self) -> Option<&str> {
        match self {
            Self::Borrowed(view) => view.passthrough_model(),
            Self::Owned(shape) => shape.passthrough_model(),
        }
    }

    fn stream(&self) -> bool {
        match self {
            Self::Borrowed(view) => view.stream.unwrap_or(false),
            Self::Owned(shape) => shape.stream.unwrap_or(false),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OpenAiFastRequestView<'a> {
    model: Option<&'a str>,
    stream: Option<bool>,
}

impl<'a> OpenAiFastRequestView<'a> {
    fn scan(body: &'a [u8]) -> Option<Self> {
        FastJsonTopLevelScanner::new(body).scan_openai_request()
    }

    fn passthrough_model(&self) -> Option<&'a str> {
        self.model.map(str::trim).filter(|model| !model.is_empty())
    }
}

struct FastJsonTopLevelScanner<'a> {
    body: &'a [u8],
    pos: usize,
}

impl<'a> FastJsonTopLevelScanner<'a> {
    const MAX_DEPTH: usize = 128;

    fn new(body: &'a [u8]) -> Self {
        Self { body, pos: 0 }
    }

    fn scan_openai_request(mut self) -> Option<OpenAiFastRequestView<'a>> {
        self.skip_ws();
        self.expect_byte(b'{')?;
        self.skip_ws();

        let mut model = None;
        let mut stream = None;

        if self.consume_byte(b'}') {
            self.skip_ws();
            return (self.pos == self.body.len()).then_some(OpenAiFastRequestView { model, stream });
        }

        loop {
            let key = self.parse_borrowed_string()?;
            self.skip_ws();
            self.expect_byte(b':')?;
            self.skip_ws();

            match key {
                "model" => {
                    if model.is_some() {
                        return None;
                    }
                    model = Some(self.parse_borrowed_string()?);
                }
                "stream" => {
                    if stream.is_some() {
                        return None;
                    }
                    stream = Some(self.parse_bool()?);
                }
                _ => self.skip_value(0)?,
            }

            self.skip_ws();
            if self.consume_byte(b'}') {
                self.skip_ws();
                return (self.pos == self.body.len()).then_some(OpenAiFastRequestView { model, stream });
            }
            self.expect_byte(b',')?;
            self.skip_ws();
        }
    }

    fn skip_value(&mut self, depth: usize) -> Option<()> {
        if depth > Self::MAX_DEPTH {
            return None;
        }

        self.skip_ws();
        match self.peek()? {
            b'"' => self.parse_borrowed_string().map(|_| ()),
            b'{' => self.skip_object(depth.saturating_add(1)),
            b'[' => self.skip_array(depth.saturating_add(1)),
            b't' => self.expect_bytes(b"true"),
            b'f' => self.expect_bytes(b"false"),
            b'n' => self.expect_bytes(b"null"),
            b'-' | b'0'..=b'9' => self.skip_number(),
            _ => None,
        }
    }

    fn skip_object(&mut self, depth: usize) -> Option<()> {
        self.expect_byte(b'{')?;
        self.skip_ws();
        if self.consume_byte(b'}') {
            return Some(());
        }

        loop {
            self.parse_borrowed_string()?;
            self.skip_ws();
            self.expect_byte(b':')?;
            self.skip_value(depth)?;
            self.skip_ws();
            if self.consume_byte(b'}') {
                return Some(());
            }
            self.expect_byte(b',')?;
            self.skip_ws();
        }
    }

    fn skip_array(&mut self, depth: usize) -> Option<()> {
        self.expect_byte(b'[')?;
        self.skip_ws();
        if self.consume_byte(b']') {
            return Some(());
        }

        loop {
            self.skip_value(depth)?;
            self.skip_ws();
            if self.consume_byte(b']') {
                return Some(());
            }
            self.expect_byte(b',')?;
            self.skip_ws();
        }
    }

    fn parse_borrowed_string(&mut self) -> Option<&'a str> {
        self.expect_byte(b'"')?;
        let start = self.pos;
        while self.pos < self.body.len() {
            match self.body[self.pos] {
                b'"' => {
                    let value = std::str::from_utf8(&self.body[start..self.pos]).ok()?;
                    self.pos = self.pos.saturating_add(1);
                    return Some(value);
                }
                b'\\' | 0x00..=0x1f => return None,
                _ => self.pos = self.pos.saturating_add(1),
            }
        }
        None
    }

    fn parse_bool(&mut self) -> Option<bool> {
        if self.body.get(self.pos..self.pos.saturating_add(4)) == Some(b"true") {
            self.pos = self.pos.saturating_add(4);
            Some(true)
        } else if self.body.get(self.pos..self.pos.saturating_add(5)) == Some(b"false") {
            self.pos = self.pos.saturating_add(5);
            Some(false)
        } else {
            None
        }
    }

    fn skip_number(&mut self) -> Option<()> {
        if self.consume_byte(b'-') && !self.peek()?.is_ascii_digit() {
            return None;
        }

        let first_digit = self.peek()?;
        if first_digit == b'0' {
            self.pos = self.pos.saturating_add(1);
        } else if first_digit.is_ascii_digit() {
            self.pos = self.pos.saturating_add(1);
            while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                self.pos = self.pos.saturating_add(1);
            }
        } else {
            return None;
        }

        if self.consume_byte(b'.') {
            if !self.peek()?.is_ascii_digit() {
                return None;
            }
            while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                self.pos = self.pos.saturating_add(1);
            }
        }

        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos = self.pos.saturating_add(1);
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos = self.pos.saturating_add(1);
            }
            if !self.peek()?.is_ascii_digit() {
                return None;
            }
            while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                self.pos = self.pos.saturating_add(1);
            }
        }

        Some(())
    }

    fn expect_bytes(&mut self, expected: &[u8]) -> Option<()> {
        if self.body.get(self.pos..self.pos.saturating_add(expected.len())) == Some(expected) {
            self.pos = self.pos.saturating_add(expected.len());
            Some(())
        } else {
            None
        }
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.pos = self.pos.saturating_add(1);
        }
    }

    fn expect_byte(&mut self, byte: u8) -> Option<()> {
        self.consume_byte(byte).then_some(())
    }

    fn consume_byte(&mut self, byte: u8) -> bool {
        if self.peek() == Some(byte) {
            self.pos = self.pos.saturating_add(1);
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<u8> {
        self.body.get(self.pos).copied()
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiChatCompletionRequest {
    #[serde(default)]
    model: Option<String>,
    messages: Vec<OpenAiMessage>,
    #[serde(default)]
    stream: Option<bool>,
    #[serde(default)]
    temperature: Option<f32>,
    #[serde(default)]
    top_p: Option<f32>,
    #[serde(default)]
    top_k: Option<u32>,
    #[serde(default)]
    max_tokens: Option<u32>,
    #[serde(default)]
    max_completion_tokens: Option<u32>,
    #[serde(default)]
    tools: Vec<LlmToolDefinition>,
    #[serde(default)]
    tool_choice: Option<Value>,
    #[serde(default)]
    response_format: Option<Value>,
    #[serde(default)]
    parallel_tool_calls: Option<bool>,
}

impl OpenAiChatCompletionRequest {
    fn into_invocation(self) -> Result<LlmInvocation, GatewayHttpError> {
        if self.messages.is_empty() {
            return Err(GatewayHttpError::new(400, "messages must not be empty"));
        }

        let mut system_prompts = Vec::new();
        let mut conversation = Vec::new();

        for message in self.messages {
            match message.role.as_str() {
                "system" | "developer" => {
                    let text = message.content_text();
                    if !text.is_empty() {
                        system_prompts.push(text);
                    }
                }
                "user" => conversation.extend(message.into_llm_messages(LlmMessageRole::User)),
                "assistant" => conversation.extend(message.into_assistant_messages()),
                "tool" => conversation.push(message.into_tool_result_message()),
                role => return Err(GatewayHttpError::new(400, format!("unsupported message role: {role}"))),
            }
        }

        if conversation.is_empty() {
            return Err(GatewayHttpError::new(400, "at least one non-system message is required"));
        }

        Ok(LlmInvocation {
            conversation_id: None,
            conversation,
            tools: self.tools,
            tool_choice: OpenAiRequestMapper::parse_tool_choice(self.tool_choice)?,
            system_prompt: (!system_prompts.is_empty()).then(|| system_prompts.join("\n\n")),
            system_prompt_blocks: None,
            overrides: LlmRequestOverrides {
                model: self.model,
                max_tokens: self.max_completion_tokens.or(self.max_tokens),
                temperature: self.temperature,
                top_p: self.top_p,
                top_k: self.top_k,
                thinking_budget: None,
            },
            response_format: OpenAiRequestMapper::parse_response_format(self.response_format)?,
            parallel_tool_calls: self.parallel_tool_calls,
            tool_connections: Vec::new(),
            tool_endpoint_uuids: Vec::new(),
            turn_context: None,
        })
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiMessage {
    role: String,
    #[serde(default)]
    content: Option<OpenAiContent>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    tool_call_id: Option<String>,
    #[serde(default)]
    tool_calls: Vec<OpenAiToolCall>,
}

impl OpenAiMessage {
    fn content_text(&self) -> String {
        self.content.as_ref().map_or_else(String::new, OpenAiContent::text)
    }

    fn into_llm_messages(self, role: LlmMessageRole) -> Vec<LlmMessage> {
        match self.content {
            Some(OpenAiContent::Text(content)) => vec![OpenAiRequestMapper::text_message(role, content)],
            Some(OpenAiContent::Parts(parts)) => OpenAiRequestMapper::content_part_messages(role, parts),
            None => vec![OpenAiRequestMapper::text_message(role, String::new())],
        }
    }

    fn into_assistant_messages(self) -> Vec<LlmMessage> {
        if !self.tool_calls.is_empty() {
            return vec![LlmMessage {
                role: LlmMessageRole::Assistant,
                content: self.content_text(),
                kind: LlmMessageKind::ToolUse {
                    calls: self.tool_calls.into_iter().enumerate().map(|(index, call)| call.into_llm_tool_call(index)).collect(),
                },
            }];
        }

        self.into_llm_messages(LlmMessageRole::Assistant)
    }

    fn into_tool_result_message(self) -> LlmMessage {
        let content = self.content_text();
        let id = self.tool_call_id.unwrap_or_else(|| "call_unknown".to_string());
        let name = self.name.unwrap_or_else(|| "tool".to_string());
        LlmMessage {
            role: LlmMessageRole::User,
            content: content.clone(),
            kind: LlmMessageKind::ToolResult {
                calls: vec![LlmToolCall {
                    id,
                    call_type: "function".to_string(),
                    function: LlmFunctionCall { name, arguments: content },
                }],
            },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OpenAiContent {
    Text(String),
    Parts(Vec<OpenAiContentPart>),
}

impl OpenAiContent {
    fn text(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Parts(parts) => parts.iter().filter_map(OpenAiContentPart::text).collect::<Vec<_>>().join("\n"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiContentPart {
    #[serde(rename = "type")]
    part_type: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    image_url: Option<OpenAiImageUrl>,
}

impl OpenAiContentPart {
    fn text(&self) -> Option<String> {
        match self.part_type.as_str() {
            "text" | "input_text" => self.text.clone(),
            _ => None,
        }
    }

    fn image_url(&self) -> Option<String> {
        match self.part_type.as_str() {
            "image_url" | "input_image" => self.image_url.as_ref().map(OpenAiImageUrl::url),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OpenAiImageUrl {
    Url { url: String },
    String(String),
}

impl OpenAiImageUrl {
    fn url(&self) -> String {
        match self {
            Self::Url { url } | Self::String(url) => url.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiToolCall {
    #[serde(default)]
    id: Option<String>,
    #[serde(default = "OpenAiRequestMapper::function_tool_type", rename = "type")]
    call_type: String,
    function: OpenAiFunctionCall,
}

impl OpenAiToolCall {
    fn into_llm_tool_call(self, index: usize) -> LlmToolCall {
        LlmToolCall {
            id: self.id.unwrap_or_else(|| format!("call_{index}")),
            call_type: self.call_type,
            function: LlmFunctionCall {
                name: self.function.name,
                arguments: OpenAiRequestMapper::json_argument_to_string(self.function.arguments),
            },
        }
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiFunctionCall {
    name: String,
    #[serde(default)]
    arguments: Option<Value>,
}

struct OpenAiRequestMapper;

impl OpenAiRequestMapper {
    fn content_part_messages(role: LlmMessageRole, parts: Vec<OpenAiContentPart>) -> Vec<LlmMessage> {
        let text = parts.iter().filter_map(OpenAiContentPart::text).collect::<Vec<_>>().join("\n");
        let image_urls = parts.iter().filter_map(OpenAiContentPart::image_url).collect::<Vec<_>>();

        if image_urls.is_empty() {
            return vec![Self::text_message(role, text)];
        }

        let mut messages = Vec::with_capacity(image_urls.len());
        for (index, url) in image_urls.into_iter().enumerate() {
            messages.push(LlmMessage {
                role: role.clone(),
                content: if index == 0 { text.clone() } else { String::new() },
                kind: LlmMessageKind::ImageUrl { url },
            });
        }
        messages
    }

    fn text_message(role: LlmMessageRole, content: String) -> LlmMessage {
        LlmMessage { role, content, kind: LlmMessageKind::Text }
    }

    fn parse_tool_choice(value: Option<Value>) -> Result<Option<LlmToolChoice>, GatewayHttpError> {
        let Some(value) = value else {
            return Ok(None);
        };

        match value {
            Value::String(choice) => match choice.as_str() {
                "auto" => Ok(Some(LlmToolChoice::Auto)),
                "none" => Ok(Some(LlmToolChoice::None)),
                "required" => Ok(Some(LlmToolChoice::Any)),
                other => Err(GatewayHttpError::new(400, format!("unsupported tool_choice: {other}"))),
            },
            Value::Object(map) => {
                let choice_type = map.get("type").and_then(Value::as_str).unwrap_or_default();
                if choice_type != "function" {
                    return Err(GatewayHttpError::new(400, "only function tool_choice objects are supported"));
                }

                let Some(name) = map.get("function").and_then(|function| function.get("name")).and_then(Value::as_str) else {
                    return Err(GatewayHttpError::new(400, "function tool_choice requires function.name"));
                };

                Ok(Some(LlmToolChoice::Tool { name: name.to_string() }))
            }
            _ => Err(GatewayHttpError::new(400, "tool_choice must be a string or function object")),
        }
    }

    fn parse_response_format(value: Option<Value>) -> Result<Option<LlmStructuredOutputFormat>, GatewayHttpError> {
        let Some(value) = value else {
            return Ok(None);
        };

        if let Ok(format) = serde_json::from_value::<LlmStructuredOutputFormat>(value.clone()) {
            return Ok(Some(format));
        }

        let Some(format_type) = value.get("type").and_then(Value::as_str) else {
            return Err(GatewayHttpError::new(400, "response_format requires a type"));
        };

        match format_type {
            "text" => Ok(None),
            "json_object" => Ok(Some(LlmStructuredOutputFormat {
                name: "response".to_string(),
                description: None,
                schema: None,
                strict: None,
            })),
            "json_schema" => {
                let Some(schema_value) = value.get("json_schema") else {
                    return Err(GatewayHttpError::new(400, "json_schema response_format requires json_schema"));
                };
                let name = schema_value.get("name").and_then(Value::as_str).unwrap_or("response").to_string();
                let description = schema_value.get("description").and_then(Value::as_str).map(str::to_string);
                let schema = schema_value.get("schema").cloned();
                let strict = schema_value.get("strict").and_then(Value::as_bool);
                Ok(Some(LlmStructuredOutputFormat { name, description, schema, strict }))
            }
            other => Err(GatewayHttpError::new(400, format!("unsupported response_format type: {other}"))),
        }
    }

    fn json_argument_to_string(value: Option<Value>) -> String {
        match value {
            Some(Value::String(arguments)) => arguments,
            Some(value) => value.to_string(),
            None => "{}".to_string(),
        }
    }

    fn function_tool_type() -> String {
        "function".to_string()
    }
}

struct OpenAiResponseMapper;

impl OpenAiResponseMapper {
    fn chat_completion_response(response: LlmChatResponse) -> Value {
        let id = CompletionMetadata::completion_id();
        let created = CompletionMetadata::unix_timestamp_seconds();
        let model = response.provider.model.clone();
        let (message, finish_reason) = Self::response_message(response.message);
        let usage = response.usage.map(Self::usage_value).unwrap_or(Value::Null);

        json!({
            "id": id,
            "object": "chat.completion",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "message": message,
                "finish_reason": finish_reason
            }],
            "usage": usage
        })
    }

    fn response_message(message: LlmMessage) -> (Value, &'static str) {
        match message.kind {
            LlmMessageKind::ToolUse { calls } if !calls.is_empty() => {
                let content = if message.content.is_empty() {
                    Value::Null
                } else {
                    Value::String(message.content)
                };
                (
                    json!({
                        "role": "assistant",
                        "content": content,
                        "tool_calls": Self::tool_calls(&calls)
                    }),
                    "tool_calls",
                )
            }
            _ => (
                json!({
                    "role": "assistant",
                    "content": message.content
                }),
                "stop",
            ),
        }
    }

    fn tool_calls(calls: &[LlmToolCall]) -> Vec<Value> {
        calls
            .iter()
            .map(|call| {
                json!({
                    "id": call.id,
                    "type": call.call_type,
                    "function": {
                        "name": call.function.name,
                        "arguments": call.function.arguments
                    }
                })
            })
            .collect()
    }

    fn stream_chunk(
        id: &str,
        created: u64,
        model: &str,
        delta: Option<String>,
        tool_calls: Vec<LlmToolCall>,
        usage: Option<LlmUsage>,
    ) -> Value {
        let mut delta_value = json!({
            "role": "assistant"
        });

        if let Some(delta) = delta {
            delta_value["content"] = json!(delta);
        }

        if !tool_calls.is_empty() {
            delta_value["tool_calls"] = Value::Array(
                tool_calls
                    .iter()
                    .enumerate()
                    .map(|(index, call)| {
                        json!({
                            "index": index,
                            "id": call.id,
                            "type": call.call_type,
                            "function": {
                                "name": call.function.name,
                                "arguments": call.function.arguments
                            }
                        })
                    })
                    .collect(),
            );
        }

        json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "delta": delta_value,
                "finish_reason": Value::Null
            }],
            "usage": usage.map(Self::usage_value).unwrap_or(Value::Null)
        })
    }

    fn usage_value(usage: LlmUsage) -> Value {
        match serde_json::to_value(usage) {
            Ok(value) => value,
            Err(err) => json!({
                "serialization_error": err.to_string()
            }),
        }
    }
}

struct HttpResponseBuilder;

impl HttpResponseBuilder {
    fn json(status: u16, body: &Value, close: bool) -> Bytes {
        let body = Self::serialize_json(body);
        Self::http(status, "application/json", body, close)
    }

    fn json_error(status: u16, message: &str, close: bool) -> Bytes {
        Self::json(
            status,
            &json!({
                "error": {
                    "message": message,
                    "type": "eden_gateway_error",
                    "code": status
                }
            }),
            close,
        )
    }

    fn raw(status: u16, content_type: Option<&str>, body: Bytes, close: bool) -> Bytes {
        Self::http_bytes(status, content_type.unwrap_or("application/json"), body, close)
    }

    fn chat_completion_text(response: &LlmChatResponse, close: bool) -> Bytes {
        let body = Self::chat_completion_text_body(
            &CompletionMetadata::completion_id(),
            CompletionMetadata::unix_timestamp_seconds(),
            &response.provider.model,
            &response.message.content,
            response.usage.as_ref(),
        );
        Self::http(200, "application/json", body, close)
    }

    fn chat_completion_text_body(id: &str, created: u64, model: &str, content: &str, usage: Option<&LlmUsage>) -> Vec<u8> {
        let mut body = Vec::with_capacity(192 + id.len() + model.len() + content.len());
        body.extend_from_slice(b"{\"id\":");
        Self::push_json_string(&mut body, id);
        body.extend_from_slice(b",\"object\":\"chat.completion\",\"created\":");
        Self::push_u64(&mut body, created);
        body.extend_from_slice(b",\"model\":");
        Self::push_json_string(&mut body, model);
        body.extend_from_slice(b",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":");
        Self::push_json_string(&mut body, content);
        body.extend_from_slice(b"},\"finish_reason\":\"stop\"}],\"usage\":");
        Self::push_usage(&mut body, usage);
        body.push(b'}');
        body
    }

    fn empty(status: u16, close: bool) -> Bytes {
        Self::http(status, "text/plain", Vec::new(), close)
    }

    fn streaming_headers() -> Bytes {
        Bytes::from_static(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
        )
    }

    fn raw_streaming_headers(status: u16, content_type: Option<&str>) -> Bytes {
        let reason = Self::reason_phrase(status);
        Bytes::from(format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: {}\r\nCache-Control: no-cache\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            content_type.unwrap_or("text/event-stream")
        ))
    }

    fn sse_json(body: &Value) -> Bytes {
        let payload = match serde_json::to_vec(body) {
            Ok(payload) => payload,
            Err(err) => json!({
                "error": {
                    "message": err.to_string(),
                    "type": "eden_gateway_error",
                    "code": 500
                }
            })
            .to_string()
            .into_bytes(),
        };
        Self::chunked_sse_payload(&payload)
    }

    fn raw_chunk(payload: &[u8]) -> Bytes {
        let mut response = Vec::with_capacity(16 + payload.len() + 2);
        Self::push_hex_len(&mut response, payload.len());
        response.extend_from_slice(b"\r\n");
        response.extend_from_slice(payload);
        response.extend_from_slice(b"\r\n");
        Bytes::from(response)
    }

    fn sse_chat_text_delta(id: &str, created: u64, model: &str, delta: &str) -> Bytes {
        let mut payload = Vec::with_capacity(160 + id.len() + model.len() + delta.len());
        payload.extend_from_slice(b"{\"id\":");
        Self::push_json_string(&mut payload, id);
        payload.extend_from_slice(b",\"object\":\"chat.completion.chunk\",\"created\":");
        Self::push_u64(&mut payload, created);
        payload.extend_from_slice(b",\"model\":");
        Self::push_json_string(&mut payload, model);
        payload.extend_from_slice(b",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":");
        Self::push_json_string(&mut payload, delta);
        payload.extend_from_slice(b"},\"finish_reason\":null}],\"usage\":null}");
        Self::chunked_sse_payload(&payload)
    }

    fn sse_done_chunk() -> Bytes {
        Bytes::from_static(b"e\r\ndata: [DONE]\n\n\r\n0\r\n\r\n")
    }

    fn chunked_end() -> Bytes {
        Bytes::from_static(b"0\r\n\r\n")
    }

    fn chunked_sse_payload(payload: &[u8]) -> Bytes {
        let frame_len = b"data: ".len() + payload.len() + b"\n\n".len();
        let mut response = Vec::with_capacity(16 + 2 + frame_len + 2);
        Self::push_hex_len(&mut response, frame_len);
        response.extend_from_slice(b"\r\n");
        response.extend_from_slice(b"data: ");
        response.extend_from_slice(payload);
        response.extend_from_slice(b"\n\n\r\n");
        Bytes::from(response)
    }

    fn push_hex_len(buffer: &mut Vec<u8>, len: usize) {
        let mut digits = [0_u8; usize::BITS as usize / 4];
        let mut value = len;
        let mut cursor = digits.len();

        loop {
            cursor -= 1;
            let digit = (value & 0x0f) as u8;
            digits[cursor] = match digit {
                0..=9 => b'0' + digit,
                _ => b'a' + (digit - 10),
            };
            value >>= 4;
            if value == 0 {
                break;
            }
        }

        buffer.extend_from_slice(&digits[cursor..]);
    }

    fn push_u64(buffer: &mut Vec<u8>, value: u64) {
        let mut formatted = itoa::Buffer::new();
        buffer.extend_from_slice(formatted.format(value).as_bytes());
    }

    fn push_json_string(buffer: &mut Vec<u8>, value: &str) {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        buffer.push(b'"');
        let bytes = value.as_bytes();
        let mut start = 0;
        for (index, byte) in bytes.iter().copied().enumerate() {
            let escape = match byte {
                b'"' => Some(&b"\\\""[..]),
                b'\\' => Some(&b"\\\\"[..]),
                b'\n' => Some(&b"\\n"[..]),
                b'\r' => Some(&b"\\r"[..]),
                b'\t' => Some(&b"\\t"[..]),
                0x08 => Some(&b"\\b"[..]),
                0x0c => Some(&b"\\f"[..]),
                0x00..=0x1f => {
                    buffer.extend_from_slice(&bytes[start..index]);
                    buffer.extend_from_slice(b"\\u00");
                    buffer.push(HEX[(byte >> 4) as usize]);
                    buffer.push(HEX[(byte & 0x0f) as usize]);
                    start = index + 1;
                    continue;
                }
                _ => None,
            };

            if let Some(escape) = escape {
                buffer.extend_from_slice(&bytes[start..index]);
                buffer.extend_from_slice(escape);
                start = index + 1;
            }
        }
        buffer.extend_from_slice(&bytes[start..]);
        buffer.push(b'"');
    }

    fn push_usage(buffer: &mut Vec<u8>, usage: Option<&LlmUsage>) {
        let Some(usage) = usage else {
            buffer.extend_from_slice(b"null");
            return;
        };

        buffer.extend_from_slice(b"{\"prompt_tokens\":");
        Self::push_u64(buffer, u64::from(usage.prompt_tokens));
        buffer.extend_from_slice(b",\"completion_tokens\":");
        Self::push_u64(buffer, u64::from(usage.completion_tokens));
        buffer.extend_from_slice(b",\"total_tokens\":");
        Self::push_u64(buffer, u64::from(usage.total_tokens));

        if let Some(details) = usage.completion_tokens_details.as_ref() {
            buffer.extend_from_slice(b",\"completion_tokens_details\":{");
            let mut wrote_field = false;
            if let Some(reasoning_tokens) = details.reasoning_tokens {
                buffer.extend_from_slice(b"\"reasoning_tokens\":");
                Self::push_u64(buffer, u64::from(reasoning_tokens));
                wrote_field = true;
            }
            if let Some(audio_tokens) = details.audio_tokens {
                if wrote_field {
                    buffer.push(b',');
                }
                buffer.extend_from_slice(b"\"audio_tokens\":");
                Self::push_u64(buffer, u64::from(audio_tokens));
            }
            buffer.push(b'}');
        }

        if let Some(details) = usage.prompt_tokens_details.as_ref() {
            buffer.extend_from_slice(b",\"prompt_tokens_details\":{");
            let mut wrote_field = false;
            if let Some(cached_tokens) = details.cached_tokens {
                buffer.extend_from_slice(b"\"cached_tokens\":");
                Self::push_u64(buffer, u64::from(cached_tokens));
                wrote_field = true;
            }
            if let Some(audio_tokens) = details.audio_tokens {
                if wrote_field {
                    buffer.push(b',');
                }
                buffer.extend_from_slice(b"\"audio_tokens\":");
                Self::push_u64(buffer, u64::from(audio_tokens));
            }
            buffer.push(b'}');
        }

        buffer.push(b'}');
    }

    fn serialize_json(body: &Value) -> Vec<u8> {
        match serde_json::to_vec(body) {
            Ok(body) => body,
            Err(err) => json!({
                "error": {
                    "message": err.to_string(),
                    "type": "eden_gateway_error",
                    "code": 500
                }
            })
            .to_string()
            .into_bytes(),
        }
    }

    fn http(status: u16, content_type: &str, body: Vec<u8>, close: bool) -> Bytes {
        Self::http_bytes(status, content_type, Bytes::from(body), close)
    }

    fn http_bytes(status: u16, content_type: &str, body: Bytes, close: bool) -> Bytes {
        let reason = Self::reason_phrase(status);
        let connection = if close { "close" } else { "keep-alive" };
        let headers = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: {connection}\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: authorization, content-type\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\n\r\n",
            body.len()
        );

        let mut response = Vec::with_capacity(headers.len() + body.len());
        response.extend_from_slice(headers.as_bytes());
        response.extend_from_slice(&body);
        Bytes::from(response)
    }

    fn reason_phrase(status: u16) -> &'static str {
        match status {
            200 => "OK",
            204 => "No Content",
            400 => "Bad Request",
            404 => "Not Found",
            405 => "Method Not Allowed",
            413 => "Payload Too Large",
            431 => "Request Header Fields Too Large",
            501 => "Not Implemented",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            _ => "Internal Server Error",
        }
    }
}

#[derive(Debug)]
struct GatewayHttpError {
    status: u16,
    message: String,
}

impl GatewayHttpError {
    fn new(status: u16, message: impl Into<String>) -> Self {
        Self { status, message: message.into() }
    }
}

struct CompletionMetadata;

impl CompletionMetadata {
    fn completion_id() -> String {
        format!("chatcmpl-{}", Self::unix_timestamp_seconds())
    }

    fn unix_timestamp_seconds() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(_) => 0,
        }
    }
}

struct LlmGatewayTokenEstimator;

impl LlmGatewayTokenEstimator {
    fn prompt_tokens(analysis: &self::analysis::LlmPayloadAnalysis) -> u32 {
        analysis.prompt_characters.saturating_div(4).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_complete_http_request() {
        let mut bytes = BytesMut::from(&b"POST /v1/chat/completions HTTP/1.1\r\nHost: eden\r\nContent-Length: 2\r\n\r\n{}"[..]);
        let original_len = bytes.len();

        let parsed = match HttpRequestParser::try_parse(&bytes) {
            Ok(Some(parsed)) => parsed,
            other => panic!("expected complete request, got {other:?}"),
        };
        let total_len = parsed.total_len;
        let request_bytes = bytes.split_to(total_len).freeze();
        let request = parsed.into_request(request_bytes);

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/v1/chat/completions");
        assert_eq!(request.body, Bytes::from_static(b"{}"));
        assert_eq!(total_len, original_len);
        assert!(bytes.is_empty());
    }

    #[test]
    fn fast_request_shape_extracts_passthrough_model() {
        let request = match serde_json::from_value::<OpenAiFastRequestShape>(json!({
            "model": " synthetic-gpt ",
            "stream": true,
            "messages": [{"role": "user", "content": "hello"}]
        })) {
            Ok(request) => request,
            Err(err) => panic!("fast request shape should deserialize: {err}"),
        };

        assert_eq!(request.passthrough_model(), Some("synthetic-gpt"));
        assert_eq!(request.stream, Some(true));

        let missing = OpenAiFastRequestShape { model: Some("  ".to_string()), stream: None };
        assert_eq!(missing.passthrough_model(), None);
    }

    #[test]
    fn fast_request_view_borrows_top_level_model_and_stream() {
        let body =
            br#"{"messages":[{"role":"user","content":"hello"}],"metadata":{"model":"nested"},"model":" synthetic-gpt ","stream":true}"#;

        let request = OpenAiPassthroughRequest::parse(body).expect("fast request should parse");

        assert_eq!(request.passthrough_model(), Some("synthetic-gpt"));
        assert!(request.stream());
        assert!(matches!(request, OpenAiPassthroughRequest::Borrowed(_)));
    }

    #[test]
    fn fast_request_view_falls_back_for_escaped_model() {
        let body = br#"{"model":"synthetic\u002dgpt","stream":false}"#;

        let request = OpenAiPassthroughRequest::parse(body).expect("serde fallback should parse escaped model");

        assert_eq!(request.passthrough_model(), Some("synthetic-gpt"));
        assert!(!request.stream());
        assert!(matches!(request, OpenAiPassthroughRequest::Owned(_)));
    }

    #[test]
    fn fast_request_view_preserves_invalid_json_errors() {
        let body = br#"{"model":"synthetic-gpt","stream":true"#;

        assert!(OpenAiPassthroughRequest::parse(body).is_err());
    }

    #[test]
    fn fast_request_view_preserves_invalid_stream_type_errors() {
        let body = br#"{"model":"synthetic-gpt","stream":"true"}"#;

        assert!(OpenAiPassthroughRequest::parse(body).is_err());
    }

    #[test]
    fn maps_openai_request_to_llm_invocation() {
        let request = match serde_json::from_value::<OpenAiChatCompletionRequest>(json!({
            "model": "gpt-test",
            "messages": [
                {"role": "system", "content": "Be concise."},
                {"role": "user", "content": "hello"}
            ],
            "temperature": 0.2,
            "tool_choice": "auto"
        })) {
            Ok(request) => request,
            Err(err) => panic!("request should deserialize: {err}"),
        };

        let invocation = match request.into_invocation() {
            Ok(invocation) => invocation,
            Err(err) => panic!("request should map to invocation: {err:?}"),
        };

        assert_eq!(invocation.system_prompt.as_deref(), Some("Be concise."));
        assert_eq!(invocation.conversation.len(), 1);
        assert_eq!(invocation.conversation[0].content, "hello");
        assert_eq!(invocation.overrides.model.as_deref(), Some("gpt-test"));
        assert_eq!(invocation.overrides.temperature, Some(0.2));
        assert_eq!(invocation.tool_choice, Some(LlmToolChoice::Auto));
    }

    #[test]
    fn maps_tool_calls_and_tool_results() {
        let request = match serde_json::from_value::<OpenAiChatCompletionRequest>(json!({
            "messages": [
                {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "lookup", "arguments": "{\"id\":1}"}
                    }]
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_1",
                    "name": "lookup",
                    "content": "{\"name\":\"eden\"}"
                }
            ]
        })) {
            Ok(request) => request,
            Err(err) => panic!("request should deserialize: {err}"),
        };

        let invocation = match request.into_invocation() {
            Ok(invocation) => invocation,
            Err(err) => panic!("request should map to invocation: {err:?}"),
        };

        assert_eq!(invocation.conversation.len(), 2);
        match &invocation.conversation[0].kind {
            LlmMessageKind::ToolUse { calls } => assert_eq!(calls[0].function.name, "lookup"),
            other => panic!("expected tool use, got {other:?}"),
        }
        match &invocation.conversation[1].kind {
            LlmMessageKind::ToolResult { calls } => assert_eq!(calls[0].id, "call_1"),
            other => panic!("expected tool result, got {other:?}"),
        }
    }

    #[test]
    fn analyzes_payload_shape_without_storing_prompt_content() {
        let body = json!({
            "messages": [
                {"role": "system", "content": "Never reveal secrets."},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Email me at devon@example.com or call +1 (212) 555-0100."},
                        {"type": "image_url", "image_url": {"url": "https://example.test/image.png"}}
                    ]
                },
                {
                    "role": "assistant",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "lookup", "arguments": "{\"ssn\":\"123-45-6789\"}"}
                    }]
                }
            ],
            "tools": [{"type": "function", "function": {"name": "lookup"}}]
        });

        let analysis = LlmPayloadInspector::inspect_openai_chat_value(&body);

        assert_eq!(analysis.message_count, 3);
        assert_eq!(analysis.system_message_count, 1);
        assert_eq!(analysis.user_message_count, 1);
        assert_eq!(analysis.assistant_message_count, 1);
        assert_eq!(analysis.text_part_count, 3);
        assert_eq!(analysis.image_part_count, 1);
        assert_eq!(analysis.tool_definition_count, 1);
        assert_eq!(analysis.tool_call_count, 1);
        assert_eq!(analysis.pii.email_count, 1);
        assert_eq!(analysis.pii.phone_count, 1);
        assert_eq!(analysis.pii.us_ssn_count, 1);
        assert!(analysis.contains_pii());
    }

    #[test]
    fn streaming_headers_use_chunked_framing() {
        let headers = HttpResponseBuilder::streaming_headers();
        let headers = match std::str::from_utf8(&headers) {
            Ok(headers) => headers,
            Err(err) => panic!("streaming headers should be utf8: {err}"),
        };

        assert!(headers.contains("Content-Type: text/event-stream"));
        assert!(headers.contains("Transfer-Encoding: chunked"));
        assert!(headers.contains("Connection: close"));
        assert!(!headers.contains("Content-Length"));
    }

    #[test]
    fn sse_json_uses_one_valid_http_chunk() {
        let chunk = HttpResponseBuilder::sse_json(&json!({"delta":"hello"}));
        let payload = single_chunk_payload(&chunk);

        assert_eq!(payload, "data: {\"delta\":\"hello\"}\n\n");
    }

    #[test]
    fn raw_chunk_preserves_upstream_payload_without_sse_rewrite() {
        let chunk = HttpResponseBuilder::raw_chunk(b"data: upstream\n\n");
        let payload = single_chunk_payload(&chunk);

        assert_eq!(payload, "data: upstream\n\n");
    }

    #[test]
    fn sse_chat_text_delta_matches_stream_chunk_shape() {
        let chunk = HttpResponseBuilder::sse_chat_text_delta("chatcmpl-1", 42, "gpt-test", "hello");
        let payload = single_chunk_payload(&chunk);
        let Some(json_payload) = payload.strip_prefix("data: ").and_then(|payload| payload.strip_suffix("\n\n")) else {
            panic!("sse payload should be framed as data");
        };
        let parsed = match serde_json::from_str::<Value>(json_payload) {
            Ok(parsed) => parsed,
            Err(err) => panic!("fast text chunk should be valid json: {err}"),
        };
        let expected = OpenAiResponseMapper::stream_chunk("chatcmpl-1", 42, "gpt-test", Some("hello".to_string()), Vec::new(), None);

        assert_eq!(parsed, expected);
    }

    #[test]
    fn sse_chat_text_delta_escapes_json_strings_without_serde_value() {
        let delta = "quote: \" slash: \\ newline:\n control:\u{0001} cafe: café";
        let chunk = HttpResponseBuilder::sse_chat_text_delta("chat\"id", 42, "gpt\\test", delta);
        let payload = single_chunk_payload(&chunk);
        let Some(json_payload) = payload.strip_prefix("data: ").and_then(|payload| payload.strip_suffix("\n\n")) else {
            panic!("sse payload should be framed as data");
        };
        let parsed = match serde_json::from_str::<Value>(json_payload) {
            Ok(parsed) => parsed,
            Err(err) => panic!("escaped text chunk should be valid json: {err}"),
        };

        assert_eq!(parsed["id"], "chat\"id");
        assert_eq!(parsed["model"], "gpt\\test");
        assert_eq!(parsed["choices"][0]["delta"]["content"], delta);
    }

    #[test]
    fn chat_completion_text_body_matches_openai_mapper_shape() {
        let response = LlmChatResponse {
            message: LlmMessage {
                role: LlmMessageRole::Assistant,
                content: "hello".to_string(),
                kind: LlmMessageKind::Text,
            },
            conversation: Vec::new(),
            usage: None,
            thinking: None,
            provider: endpoint_core::llm_core::LlmProviderMetadata::new("synthetic", "gpt-test", None),
            conversation_id: None,
        };
        let body = HttpResponseBuilder::chat_completion_text_body(
            "chatcmpl-1",
            42,
            "gpt-test",
            &response.message.content,
            response.usage.as_ref(),
        );
        let parsed = match serde_json::from_slice::<Value>(&body) {
            Ok(parsed) => parsed,
            Err(err) => panic!("fast text completion should be valid json: {err}"),
        };
        let mut expected = OpenAiResponseMapper::chat_completion_response(response);
        expected["id"] = json!("chatcmpl-1");
        expected["created"] = json!(42);

        assert_eq!(parsed, expected);
    }

    #[test]
    fn chat_completion_text_body_escapes_json_and_serializes_usage_without_value() {
        let usage = LlmUsage {
            prompt_tokens: 11,
            completion_tokens: 7,
            total_tokens: 18,
            completion_tokens_details: Some(endpoint_core::llm_core::LlmCompletionTokensDetails {
                reasoning_tokens: Some(3),
                audio_tokens: Some(2),
            }),
            prompt_tokens_details: Some(endpoint_core::llm_core::types::LlmPromptTokensDetails {
                cached_tokens: Some(5),
                audio_tokens: Some(1),
            }),
        };
        let content = "quote: \" slash: \\ newline:\n control:\u{0001} cafe: café";
        let body = HttpResponseBuilder::chat_completion_text_body("chat\"id", 42, "gpt\\test", content, Some(&usage));
        let parsed = match serde_json::from_slice::<Value>(&body) {
            Ok(parsed) => parsed,
            Err(err) => panic!("escaped text completion should be valid json: {err}"),
        };

        assert_eq!(parsed["id"], "chat\"id");
        assert_eq!(parsed["model"], "gpt\\test");
        assert_eq!(parsed["choices"][0]["message"]["content"], content);
        assert_eq!(parsed["usage"]["prompt_tokens"], 11);
        assert_eq!(parsed["usage"]["completion_tokens"], 7);
        assert_eq!(parsed["usage"]["total_tokens"], 18);
        assert_eq!(parsed["usage"]["completion_tokens_details"]["reasoning_tokens"], 3);
        assert_eq!(parsed["usage"]["completion_tokens_details"]["audio_tokens"], 2);
        assert_eq!(parsed["usage"]["prompt_tokens_details"]["cached_tokens"], 5);
        assert_eq!(parsed["usage"]["prompt_tokens_details"]["audio_tokens"], 1);
    }

    #[test]
    fn sse_done_terminates_chunked_response() {
        assert_eq!(HttpResponseBuilder::sse_done_chunk(), Bytes::from_static(b"e\r\ndata: [DONE]\n\n\r\n0\r\n\r\n"));
        assert_eq!(HttpResponseBuilder::chunked_end(), Bytes::from_static(b"0\r\n\r\n"));
    }

    fn single_chunk_payload(chunk: &Bytes) -> &str {
        let chunk = match std::str::from_utf8(chunk) {
            Ok(chunk) => chunk,
            Err(err) => panic!("sse chunk should be utf8: {err}"),
        };
        let Some((hex_len, rest)) = chunk.split_once("\r\n") else {
            panic!("chunk should start with hex length");
        };
        let len = match usize::from_str_radix(hex_len, 16) {
            Ok(len) => len,
            Err(err) => panic!("chunk length should be hex: {err}"),
        };
        let Some((payload, trailer)) = rest.split_once("\r\n") else {
            panic!("chunk should end with CRLF");
        };

        assert_eq!(payload.len(), len);
        assert_eq!(trailer, "");
        payload
    }
}
