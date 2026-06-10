use crate::comm::llm::utils::{elapsed_millis, json_size_u32};
use actix_http::header::{CACHE_CONTROL, CONTENT_TYPE, HeaderValue};
use actix_web::http::StatusCode;
use actix_web::web::Bytes;
use actix_web::{HttpRequest, HttpResponse, HttpResponseBuilder, web};
use async_stream::stream as async_stream;
use chrono::{Duration, Utc};
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use database::db::methods::llm::{NewLlmGatewayResponseCacheEntry, NewLlmGatewayUsageRollup, StoredLlmGatewayRouteRollup};
use eden_core::format::{EndpointUuid, OrganizationUuid};
use eden_core::telemetry::{FastSpan, TelemetryWrapper};
use eden_gateway::agent::{AgentGatewayRoute, AgentGatewayState, AgentGatewayUsageEvent};
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::llm_core::comm::accumulate_usage;
use endpoint_core::llm_core::connection::LlmProvider;
use endpoint_core::llm_core::{
    LLM_GATEWAY_AGENT_FINGERPRINT_HEADER, LLM_GATEWAY_AGENT_ID_HEADER, LLM_GATEWAY_AGENT_PRINCIPAL_HEADER,
    LLM_GATEWAY_AGENT_SESSION_HEADER, LLM_GATEWAY_AGENT_TAGS_HEADER, LlmCacheStatus, LlmChatResponse, LlmGatewayModelCatalog,
    LlmKvCacheMode, LlmKvCacheStatus, LlmModelOperation, LlmPiiScanner, LlmProviderMetadata, LlmRouteOptimizationMode, LlmUsage,
    OpenAiAssistantMessage, OpenAiChatCompletionChoice, OpenAiChatCompletionChunk, OpenAiChatCompletionChunkChoice,
    OpenAiChatCompletionChunkDelta, OpenAiChatCompletionRequest, OpenAiChatCompletionResponse, OpenAiChunkToolCall, OpenAiFunctionCall,
    OpenAiResponsesRequest, OpenAiResponsesResponse, PolicyAction, PriceArbitrageMode, PriceRouteCandidate, PriceSource,
    ResolvedLlmConnection, build_final_llm_message, choose_openrouter_price_route, estimate_cost_micros, evaluate_pre_egress_policy,
    format_openai_stream_chunk, format_openai_stream_error, new_openai_completion_id, openai_request_to_invocation,
    openai_response_from_llm, openai_responses_response_from_chat, openrouter_price_route_candidates, request_mentions_tools,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use telemetry_extensions_macro::with_telemetry;

use super::analytics::{build_proxy_analytics_context, record_proxy_llm_usage_metrics};
use super::auth::bearer_api_key;
use super::keys::ApiKey;
use super::state::{ProxyAnalyticsContext, ProxyGatewayState, ProxyRouteStatsSnapshot, current_budget_month_bucket};
use super::{
    build_proxy_client, fetch_llm_endpoint_schema, governance_block_error_response, openai_error_from_ep_error, openai_error_response,
    provider_metadata_from_resolved, proxy_governance_policy,
};

#[with_telemetry]
pub async fn chat_completions(
    req: HttpRequest,
    body: Bytes,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    proxy_state: web::Data<ProxyGatewayState>,
    agent_gateway: Option<web::Data<AgentGatewayState>>,
) -> Result<HttpResponse, actix_web::Error> {
    proxy_openai_generation(
        req,
        body,
        database,
        proxy_state,
        agent_gateway,
        OpenAiProxySurface::ChatCompletions,
        telemetry_wrapper,
        &mut span,
    )
    .await
}

#[with_telemetry]
pub async fn responses(
    req: HttpRequest,
    body: Bytes,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    proxy_state: web::Data<ProxyGatewayState>,
    agent_gateway: Option<web::Data<AgentGatewayState>>,
) -> Result<HttpResponse, actix_web::Error> {
    proxy_openai_generation(
        req,
        body,
        database,
        proxy_state,
        agent_gateway,
        OpenAiProxySurface::Responses,
        telemetry_wrapper,
        &mut span,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn proxy_openai_generation(
    req: HttpRequest,
    body: Bytes,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    proxy_state: web::Data<ProxyGatewayState>,
    agent_gateway: Option<web::Data<AgentGatewayState>>,
    surface: OpenAiProxySurface,
    telemetry_wrapper: &mut TelemetryWrapper,
    span: &mut FastSpan,
) -> Result<HttpResponse, actix_web::Error> {
    let request_started = std::time::Instant::now();
    let now = Utc::now();

    let plaintext_key = match bearer_api_key(&req) {
        Some(key) => key,
        None => {
            return Ok(openai_error_response(
                StatusCode::UNAUTHORIZED,
                "Missing or invalid Authorization header",
                "authentication_error",
                "invalid_api_key",
            ));
        }
    };

    let api_key = match proxy_state.resolve_plaintext_key(&plaintext_key) {
        Some(api_key) => api_key,
        None => {
            return Ok(openai_error_response(
                StatusCode::UNAUTHORIZED,
                "Invalid proxy API key",
                "authentication_error",
                "invalid_api_key",
            ));
        }
    };

    if !api_key.enabled {
        return Ok(openai_error_response(
            StatusCode::FORBIDDEN,
            "API key is disabled",
            "invalid_request_error",
            "api_key_disabled",
        ));
    }

    let downstream_consumers = proxy_downstream_consumers(&req);

    if !proxy_state.check_rate_limit(api_key.id, api_key.rate_limit_rpm, now) {
        persist_proxy_rate_limited_rollups(&database, &api_key, &downstream_consumers, now, telemetry_wrapper).await;
        return Ok(openai_error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "Rate limit exceeded for API key",
            "rate_limit_error",
            "rate_limit_exceeded",
        ));
    }

    let (request, normalized_body) = match surface.chat_request_from_body(&body) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let operation = surface.operation(request.stream);

    let requested_model = request.model.trim().to_string();
    if requested_model.is_empty() {
        return Ok(openai_error_response(
            StatusCode::BAD_REQUEST,
            "model must not be empty",
            "invalid_request_error",
            "invalid_request",
        ));
    }

    let org_uuid = OrganizationUuid::from(api_key.org_uuid);
    let endpoint_uuid = EndpointUuid::from(api_key.endpoint_uuid);
    let endpoint_schema = match fetch_llm_endpoint_schema(&database, &org_uuid, endpoint_uuid.clone(), telemetry_wrapper).await {
        Ok(endpoint_schema) => endpoint_schema,
        Err(err) => return Ok(openai_error_from_ep_error(err, span)),
    };

    telemetry_wrapper.mut_labels(|labels| {
        labels.set_endpoint_uuid(endpoint_schema.uuid());
        labels.set_endpoint_id(endpoint_schema.id());
        labels.set_endpoint_kind(endpoint_schema.kind());
    });

    let (client, resolved_connection, credential_id) = match build_proxy_client(&endpoint_schema) {
        Ok(client) => client,
        Err(err) => return Ok(openai_error_from_ep_error(err, span)),
    };

    let mut invocation = match openai_request_to_invocation(&request) {
        Ok(invocation) => invocation,
        Err(err) => return Ok(openai_error_from_ep_error(err, span)),
    };

    let prompt_token_estimate = estimate_prompt_tokens(&request);
    let completion_token_estimate = request.max_tokens.or(resolved_connection.defaults.max_tokens).unwrap_or(1_024);
    let agent_context = match resolve_proxy_agent_context(
        &req,
        agent_gateway.as_ref().map(web::Data::get_ref),
        &api_key,
        prompt_token_estimate,
        completion_token_estimate,
    ) {
        Ok(agent_context) => agent_context,
        Err(response) => return Ok(response),
    };
    let route_decision = select_proxy_route(
        &req,
        &request,
        &proxy_state,
        &api_key,
        api_key.endpoint_uuid,
        resolved_connection.provider,
        &requested_model,
        prompt_token_estimate,
        completion_token_estimate,
        now,
    );
    if route_decision.selected_model != requested_model {
        invocation.overrides.model = Some(route_decision.selected_model.clone());
    }

    let provider_meta = match provider_metadata_from_resolved(&resolved_connection, &invocation) {
        Ok(provider_meta) => provider_meta,
        Err(err) => return Ok(openai_error_from_ep_error(err, span)),
    };

    let mut analytics_context =
        build_proxy_analytics_context(endpoint_uuid, &org_uuid, &api_key, credential_id.clone(), &request, &invocation, &normalized_body);
    analytics_context.requested_provider = Some(resolved_connection.provider.to_string());
    analytics_context.requested_model = Some(requested_model.clone());
    analytics_context.baseline_estimated_cost_micros = route_decision.baseline_estimated_cost_micros;
    analytics_context.selected_estimated_cost_micros = route_decision.selected_estimated_cost_micros;
    analytics_context.estimated_arbitrage_savings_micros = route_decision.estimated_arbitrage_savings_micros;
    analytics_context.arbitrage_reason = Some(route_decision.reason.clone());
    analytics_context.price_source = route_decision.price_source.map(|source| source.to_string());
    analytics_context.route_optimization_mode = route_decision.route_optimization_mode;
    analytics_context.kv_cache_mode = route_decision.kv_cache_mode;
    analytics_context.kv_cache_status = route_decision.kv_cache_status;
    analytics_context.estimated_kv_cache_savings_micros = route_decision.estimated_kv_cache_savings_micros;
    analytics_context.route_move_reason = route_decision.route_move_reason.clone();
    analytics_context.conversation_route_key = route_decision.conversation_route_key.clone();
    let request_mentions_tools = request_mentions_tools(&request);
    let metrics = telemetry_wrapper.metrics().clone();
    // Apply the org-wide dictionary plus the agent's own, both pre-compiled and
    // cached (built once at load / update) rather than recompiled per request.
    let org_pii_matcher = proxy_state.org_pii_matcher(api_key.org_uuid);
    let pii_scanner =
        LlmPiiScanner::with_compiled_dictionaries([org_pii_matcher, api_key.pii_matcher.clone()].into_iter().flatten().collect());
    let governance_policy = proxy_governance_policy(&api_key);
    let governance_decision = match evaluate_pre_egress_policy(&governance_policy, &mut invocation, &pii_scanner) {
        Ok(decision) => decision,
        Err(err) => return Ok(openai_error_from_ep_error(err, span)),
    };
    let prompt_pii_types = governance_decision.pii_result.type_names();

    proxy_state.mark_last_used(api_key.id, now);

    if governance_decision.blocked {
        let error_message =
            governance_decision.block_reason.clone().unwrap_or_else(|| "request blocked by LLM governance policy".to_string());
        record_proxy_llm_usage_metrics(
            metrics.as_ref(),
            None,
            &provider_meta,
            operation,
            &analytics_context,
            request_mentions_tools,
            0,
            request.stream,
            elapsed_millis(request_started),
            0,
            false,
            Some(error_message.clone()),
            PolicyAction::Block,
            governance_decision.pii_result.detected,
            &prompt_pii_types,
        );
        return Ok(governance_block_error_response(&error_message));
    }

    sync_proxy_budget_window(&database, &proxy_state, &api_key, now, telemetry_wrapper).await;
    if !proxy_state.check_budget_limit(api_key.id, api_key.budget_tokens_monthly, request.max_tokens.map(u64::from), now) {
        persist_proxy_rate_limited_rollups(&database, &api_key, &downstream_consumers, now, telemetry_wrapper).await;
        let error_message = "Monthly token budget exceeded for API key".to_string();
        record_proxy_llm_usage_metrics(
            metrics.as_ref(),
            None,
            &provider_meta,
            operation,
            &analytics_context,
            request_mentions_tools,
            0,
            request.stream,
            elapsed_millis(request_started),
            0,
            false,
            Some(error_message),
            PolicyAction::Block,
            governance_decision.pii_result.detected,
            &prompt_pii_types,
        );
        return Ok(openai_error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "Monthly token budget exceeded for API key",
            "insufficient_quota",
            "budget_exceeded",
        ));
    }

    let response_cache_ttl_secs = response_cache_ttl_secs(&api_key, &request, request_mentions_tools);
    let response_cache_key = response_cache_ttl_secs.map(|_| response_cache_key(&api_key, &provider_meta, &normalized_body));

    if !request.stream
        && let Some(cache_key) = response_cache_key.as_deref()
    {
        if let Some(cached_response) = lookup_cached_response(&database, &proxy_state, &api_key, cache_key, now, telemetry_wrapper).await {
            let openai_response = prepare_cached_openai_response(cached_response, &provider_meta);
            let cached_usage = usage_from_openai_response(&openai_response);
            let response_body = surface.response_from_chat(openai_response);
            let usage_recorded_at = Utc::now();
            proxy_state.record_budget_usage(
                api_key.id,
                api_key.budget_tokens_monthly,
                u64::from(cached_usage.total_tokens),
                usage_recorded_at,
            );
            let estimated_cache_savings_micros = estimate_cost_micros(
                &provider_meta.provider,
                &provider_meta.model,
                cached_usage.prompt_tokens,
                cached_usage.completion_tokens,
            );
            let cache_context = analytics_context.with_cache_status(LlmCacheStatus::Hit, estimated_cache_savings_micros);
            persist_proxy_usage_rollups(
                &database,
                &api_key,
                &downstream_consumers,
                &cache_context,
                Some(&cached_usage),
                &provider_meta,
                usage_recorded_at,
                telemetry_wrapper,
            )
            .await;

            record_proxy_llm_usage_metrics(
                metrics.as_ref(),
                Some(&cached_usage),
                &provider_meta,
                operation,
                &cache_context,
                false,
                0,
                false,
                elapsed_millis(request_started),
                json_size_u32(&response_body),
                true,
                None,
                governance_decision.action_taken,
                governance_decision.pii_result.detected,
                &prompt_pii_types,
            );

            let mut response = HttpResponse::Ok();
            insert_agent_response_headers(&mut response, agent_context.as_ref());
            return Ok(response
                .insert_header(("x-eden-cache", "hit"))
                .insert_header(("x-eden-kv-cache", analytics_context.kv_cache_status.to_string()))
                .json(response_body));
        }
    }

    if request.stream {
        let stream = match client.chat_stream(&invocation).await {
            Ok(stream) => stream,
            Err(err) => {
                record_proxy_llm_usage_metrics(
                    metrics.as_ref(),
                    None,
                    &provider_meta,
                    operation,
                    &analytics_context,
                    request_mentions_tools,
                    0,
                    true,
                    elapsed_millis(request_started),
                    0,
                    false,
                    Some(err.to_string()),
                    governance_decision.action_taken,
                    governance_decision.pii_result.detected,
                    &prompt_pii_types,
                );
                return Ok(openai_error_from_ep_error(err, span));
            }
        };

        let response_id = new_openai_completion_id();
        let created = Utc::now().timestamp();
        let model = provider_meta.model.clone();
        let provider_meta_for_stream = provider_meta.clone();
        let analytics_context_for_stream = analytics_context.clone();
        let downstream_consumers_for_stream = downstream_consumers.clone();
        let metrics_for_stream = metrics.clone();
        let database_for_stream = database.clone();
        let telemetry_labels_for_stream = telemetry_wrapper.labels();
        let telemetry_durations_for_stream = telemetry_wrapper.durations().clone();
        let request_started_for_stream = request_started;
        let prompt_governance_for_stream = governance_decision.clone();
        let pii_scanner_for_stream = pii_scanner.clone();
        let api_key_for_stream = api_key.clone();
        let proxy_state_for_stream = proxy_state.clone();
        let route_decision_for_stream = route_decision.clone();

        let output = async_stream! {
            let mut accumulated_usage: Option<LlmUsage> = None;
            let mut tool_call_count = 0_u32;
            let mut saw_tool_calls = false;
            let mut final_tool_calls = Vec::new();
            let mut final_content = String::new();
            let mut llm_stream = stream;

            let initial_chunk = OpenAiChatCompletionChunk {
                id: response_id.clone(),
                object: "chat.completion.chunk",
                created,
                model: model.clone(),
                choices: vec![OpenAiChatCompletionChunkChoice {
                    index: 0,
                    delta: OpenAiChatCompletionChunkDelta {
                        role: Some("assistant"),
                        ..Default::default()
                    },
                    finish_reason: None,
                }],
            };
            yield Ok::<Bytes, actix_web::Error>(Bytes::from(format_openai_stream_chunk(&initial_chunk)));

            while let Some(chunk_result) = llm_stream.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        accumulate_usage(&mut accumulated_usage, chunk.usage.clone());

                        if let Some(delta) = chunk.delta {
                            final_content.push_str(&delta);
                            let content_chunk = OpenAiChatCompletionChunk {
                                id: response_id.clone(),
                                object: "chat.completion.chunk",
                                created,
                                model: model.clone(),
                                choices: vec![OpenAiChatCompletionChunkChoice {
                                    index: 0,
                                    delta: OpenAiChatCompletionChunkDelta {
                                        content: Some(delta),
                                        ..Default::default()
                                    },
                                    finish_reason: None,
                                }],
                            };
                            yield Ok::<Bytes, actix_web::Error>(Bytes::from(format_openai_stream_chunk(&content_chunk)));
                        }

                        if !chunk.tool_calls.is_empty() {
                            saw_tool_calls = true;
                            tool_call_count = tool_call_count
                                .saturating_add(u32::try_from(chunk.tool_calls.len()).unwrap_or(u32::MAX));

                            for call in chunk.tool_calls {
                                let tool_index = u32::try_from(final_tool_calls.len()).unwrap_or(u32::MAX);
                                final_tool_calls.push(call.clone());

                                let tool_chunk = OpenAiChatCompletionChunk {
                                    id: response_id.clone(),
                                    object: "chat.completion.chunk",
                                    created,
                                    model: model.clone(),
                                    choices: vec![OpenAiChatCompletionChunkChoice {
                                        index: 0,
                                        delta: OpenAiChatCompletionChunkDelta {
                                            tool_calls: vec![OpenAiChunkToolCall {
                                                index: tool_index,
                                                id: Some(call.id.clone()),
                                                call_type: Some(call.call_type.clone()),
                                                function: Some(OpenAiFunctionCall {
                                                    name: call.function.name.clone(),
                                                    arguments: call.function.arguments.clone(),
                                                }),
                                            }],
                                            ..Default::default()
                                        },
                                        finish_reason: None,
                                    }],
                                };
                                yield Ok::<Bytes, actix_web::Error>(Bytes::from(format_openai_stream_chunk(&tool_chunk)));
                            }
                        }
                    }
                    Err(err) => {
                        let final_message = build_final_llm_message(final_content.clone(), &final_tool_calls);
                        let response = LlmChatResponse {
                            message: final_message,
                            conversation: Vec::new(),
                            usage: accumulated_usage.clone(),
                            thinking: None,
                            provider: provider_meta_for_stream.clone(),
                            conversation_id: None,
                        };
                        let completion_pii = pii_scanner_for_stream.scan_completion_message(&response.message);
                        let mut pii_result = prompt_governance_for_stream.pii_result.clone();
                        pii_result.merge(&completion_pii);
                        let pii_types = pii_result.type_names();
                        let policy_action = prompt_governance_for_stream.action_taken.merge(if completion_pii.detected {
                            PolicyAction::AuditOnly
                        } else {
                            PolicyAction::Allow
                        });

                        record_proxy_llm_usage_metrics(
                            metrics_for_stream.as_ref(),
                            response.usage.as_ref(),
                            &provider_meta_for_stream,
                            operation,
                            &analytics_context_for_stream,
                            request_mentions_tools || saw_tool_calls,
                            tool_call_count,
                            true,
                            elapsed_millis(request_started_for_stream),
                            json_size_u32(&response),
                            false,
                            Some(err.to_string()),
                            policy_action,
                            pii_result.detected,
                            &pii_types,
                        );

                        yield Ok::<Bytes, actix_web::Error>(Bytes::from(format_openai_stream_error(
                            "Upstream LLM request failed",
                            "server_error",
                            "upstream_error",
                        )));
                        yield Ok::<Bytes, actix_web::Error>(Bytes::from("data: [DONE]\n\n"));
                        return;
                    }
                }
            }

            let finish_reason = if saw_tool_calls { "tool_calls" } else { "stop" }.to_string();
            let final_message = build_final_llm_message(final_content.clone(), &final_tool_calls);
            let response = LlmChatResponse {
                message: final_message,
                conversation: Vec::new(),
                usage: accumulated_usage.clone(),
                thinking: None,
                provider: provider_meta_for_stream.clone(),
                conversation_id: None,
            };
            let completion_pii = pii_scanner_for_stream.scan_completion_message(&response.message);
            let mut pii_result = prompt_governance_for_stream.pii_result.clone();
            pii_result.merge(&completion_pii);
            let pii_types = pii_result.type_names();
            let policy_action = prompt_governance_for_stream.action_taken.merge(if completion_pii.detected {
                PolicyAction::AuditOnly
            } else {
                PolicyAction::Allow
            });
            if let Some(usage) = response.usage.as_ref() {
                proxy_state_for_stream.record_budget_usage(
                    api_key_for_stream.id,
                    api_key_for_stream.budget_tokens_monthly,
                    u64::from(usage.total_tokens),
                    Utc::now(),
                );
                let route_stats_snapshot = record_successful_route(
                    proxy_state_for_stream.as_ref(),
                    &api_key_for_stream,
                    &provider_meta_for_stream.provider,
                    &provider_meta_for_stream.model,
                    &route_decision_for_stream,
                    elapsed_millis(request_started_for_stream),
                    usage.total_tokens,
                    Utc::now(),
                );
                let mut stream_telemetry = eden_core::telemetry::TelemetryWrapper::new(
                    metrics_for_stream.clone(),
                    telemetry_labels_for_stream.clone(),
                    telemetry_durations_for_stream.clone(),
                );
                persist_proxy_usage_rollups(
                    &database_for_stream,
                    &api_key_for_stream,
                    &downstream_consumers_for_stream,
                    &analytics_context_for_stream,
                    response.usage.as_ref(),
                    &provider_meta_for_stream,
                    Utc::now(),
                    &mut stream_telemetry,
                )
                .await;
                persist_proxy_route_rollup(
                    &database_for_stream,
                    &api_key_for_stream,
                    &provider_meta_for_stream.provider,
                    &provider_meta_for_stream.model,
                    route_stats_snapshot,
                    &mut stream_telemetry,
                )
                .await;
            }

            record_proxy_llm_usage_metrics(
                metrics_for_stream.as_ref(),
                response.usage.as_ref(),
                &provider_meta_for_stream,
                operation,
                &analytics_context_for_stream,
                request_mentions_tools || saw_tool_calls,
                tool_call_count,
                true,
                elapsed_millis(request_started_for_stream),
                json_size_u32(&response),
                true,
                None,
                policy_action,
                pii_result.detected,
                &pii_types,
            );

            let final_chunk = OpenAiChatCompletionChunk {
                id: response_id,
                object: "chat.completion.chunk",
                created,
                model,
                choices: vec![OpenAiChatCompletionChunkChoice {
                    index: 0,
                    delta: OpenAiChatCompletionChunkDelta::default(),
                    finish_reason: Some(finish_reason),
                }],
            };
            yield Ok::<Bytes, actix_web::Error>(Bytes::from(format_openai_stream_chunk(&final_chunk)));
            yield Ok::<Bytes, actix_web::Error>(Bytes::from("data: [DONE]\n\n"));
        };

        let mut response = HttpResponse::Ok();
        insert_agent_response_headers(&mut response, agent_context.as_ref());
        return Ok(response
            .insert_header((CONTENT_TYPE, "text/event-stream"))
            .insert_header((CACHE_CONTROL, "no-cache"))
            .insert_header(("x-eden-kv-cache", analytics_context.kv_cache_status.to_string()))
            .streaming(output));
    }

    let response = match client.chat(&invocation).await {
        Ok(response) => response,
        Err(err) => {
            let error_context = analytics_context.with_cache_status(
                if response_cache_ttl_secs.is_some() {
                    LlmCacheStatus::Miss
                } else {
                    LlmCacheStatus::Bypass
                },
                0,
            );
            record_proxy_llm_usage_metrics(
                metrics.as_ref(),
                None,
                &provider_meta,
                operation,
                &error_context,
                request_mentions_tools,
                0,
                false,
                elapsed_millis(request_started),
                0,
                false,
                Some(err.to_string()),
                governance_decision.action_taken,
                governance_decision.pii_result.detected,
                &prompt_pii_types,
            );
            return Ok(openai_error_from_ep_error(err, span));
        }
    };

    let usage_for_metrics = response.usage.clone();
    let completion_pii = pii_scanner.scan_completion_message(&response.message);
    let mut pii_result = governance_decision.pii_result.clone();
    pii_result.merge(&completion_pii);
    let pii_types = pii_result.type_names();
    let policy_action = governance_decision.action_taken.merge(if completion_pii.detected {
        PolicyAction::AuditOnly
    } else {
        PolicyAction::Allow
    });
    let mut route_stats_snapshot = None;
    if let Some(usage) = usage_for_metrics.as_ref() {
        let usage_recorded_at = Utc::now();
        proxy_state.record_budget_usage(api_key.id, api_key.budget_tokens_monthly, u64::from(usage.total_tokens), usage_recorded_at);
        route_stats_snapshot = Some(record_successful_route(
            proxy_state.as_ref(),
            &api_key,
            &provider_meta.provider,
            &provider_meta.model,
            &route_decision,
            elapsed_millis(request_started),
            usage.total_tokens,
            usage_recorded_at,
        ));
    }
    let openai_response = openai_response_from_llm(new_openai_completion_id(), response, Utc::now().timestamp());
    let response_body = surface.response_from_chat(openai_response.clone());
    let response_bytes = json_size_u32(&response_body);
    let tool_call_count = openai_response
        .choices
        .first()
        .map(|choice| u32::try_from(choice.message.tool_calls.len()).unwrap_or(u32::MAX))
        .unwrap_or_default();
    let cache_context = if !pii_result.detected
        && let (Some(ttl_secs), Some(cache_key)) = (response_cache_ttl_secs, response_cache_key)
    {
        let cache_stored_at = Utc::now();
        proxy_state.store_response_cache(cache_key.clone(), api_key.org_uuid, openai_response.clone(), ttl_secs, cache_stored_at);
        persist_response_cache_entry(
            &database,
            &api_key,
            &provider_meta,
            &normalized_body,
            &cache_key,
            &openai_response,
            analytics_context.prompt_fingerprint.as_deref(),
            ttl_secs,
            cache_stored_at,
            telemetry_wrapper,
        )
        .await;
        analytics_context.with_cache_status(LlmCacheStatus::Store, 0)
    } else {
        analytics_context.with_cache_status(LlmCacheStatus::Bypass, 0)
    };

    if let Some(usage) = usage_for_metrics.as_ref() {
        persist_proxy_usage_rollups(
            &database,
            &api_key,
            &downstream_consumers,
            &cache_context,
            Some(usage),
            &provider_meta,
            Utc::now(),
            telemetry_wrapper,
        )
        .await;
    }
    if let Some(route_stats_snapshot) = route_stats_snapshot {
        persist_proxy_route_rollup(
            &database,
            &api_key,
            &provider_meta.provider,
            &provider_meta.model,
            route_stats_snapshot,
            telemetry_wrapper,
        )
        .await;
    }

    record_proxy_llm_usage_metrics(
        metrics.as_ref(),
        usage_for_metrics.as_ref(),
        &provider_meta,
        operation,
        &cache_context,
        request_mentions_tools || tool_call_count > 0,
        tool_call_count,
        false,
        elapsed_millis(request_started),
        response_bytes,
        true,
        None,
        policy_action,
        pii_result.detected,
        &pii_types,
    );

    let mut response = HttpResponse::Ok();
    insert_agent_response_headers(&mut response, agent_context.as_ref());
    Ok(response.insert_header(("x-eden-kv-cache", analytics_context.kv_cache_status.to_string())).json(response_body))
}

#[derive(Debug, Clone, Copy)]
enum OpenAiProxySurface {
    ChatCompletions,
    Responses,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum OpenAiProxyResponseBody {
    Chat(OpenAiChatCompletionResponse),
    Responses(OpenAiResponsesResponse),
}

impl OpenAiProxySurface {
    fn chat_request_from_body(self, body: &Bytes) -> Result<(OpenAiChatCompletionRequest, Bytes), HttpResponse> {
        match self {
            Self::ChatCompletions => {
                let request = serde_json::from_slice(body).map_err(|_| {
                    openai_error_response(
                        StatusCode::BAD_REQUEST,
                        "Request body must be valid JSON",
                        "invalid_request_error",
                        "invalid_request",
                    )
                })?;
                Ok((request, body.clone()))
            }
            Self::Responses => {
                let request: OpenAiResponsesRequest = serde_json::from_slice(body).map_err(|_| {
                    openai_error_response(
                        StatusCode::BAD_REQUEST,
                        "Request body must be valid Responses JSON",
                        "invalid_request_error",
                        "invalid_request",
                    )
                })?;
                let request = request.into_chat_completion_request().map_err(|err| {
                    openai_error_response(StatusCode::BAD_REQUEST, &err.to_string(), "invalid_request_error", "invalid_request")
                })?;
                let normalized = serde_json::to_vec(&request).map_err(|_| {
                    openai_error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to normalize Responses request",
                        "server_error",
                        "serialization_error",
                    )
                })?;
                Ok((request, Bytes::from(normalized)))
            }
        }
    }

    fn operation(self, stream: bool) -> &'static str {
        match (self, stream) {
            (Self::ChatCompletions, true) => "chat.completions.stream",
            (Self::ChatCompletions, false) => "chat.completions",
            (Self::Responses, _) => "responses",
        }
    }

    fn response_from_chat(self, response: OpenAiChatCompletionResponse) -> OpenAiProxyResponseBody {
        match self {
            Self::ChatCompletions => OpenAiProxyResponseBody::Chat(response),
            Self::Responses => OpenAiProxyResponseBody::Responses(openai_responses_response_from_chat(response)),
        }
    }
}

#[with_telemetry]
pub async fn list_models(
    req: HttpRequest,
    database: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    proxy_state: web::Data<ProxyGatewayState>,
) -> Result<HttpResponse, actix_web::Error> {
    let now = Utc::now();

    let plaintext_key = match bearer_api_key(&req) {
        Some(key) => key,
        None => {
            return Ok(openai_error_response(
                StatusCode::UNAUTHORIZED,
                "Missing or invalid Authorization header",
                "authentication_error",
                "invalid_api_key",
            ));
        }
    };

    let api_key = match proxy_state.resolve_plaintext_key(&plaintext_key) {
        Some(api_key) => api_key,
        None => {
            return Ok(openai_error_response(
                StatusCode::UNAUTHORIZED,
                "Invalid proxy API key",
                "authentication_error",
                "invalid_api_key",
            ));
        }
    };

    if !api_key.enabled {
        return Ok(openai_error_response(
            StatusCode::FORBIDDEN,
            "API key is disabled",
            "invalid_request_error",
            "api_key_disabled",
        ));
    }

    let org_uuid = OrganizationUuid::from(api_key.org_uuid);
    let endpoint_uuid = EndpointUuid::from(api_key.endpoint_uuid);
    let endpoint_schema = match fetch_llm_endpoint_schema(&database, &org_uuid, endpoint_uuid, telemetry_wrapper).await {
        Ok(endpoint_schema) => endpoint_schema,
        Err(err) => return Ok(openai_error_from_ep_error(err, &mut span)),
    };

    telemetry_wrapper.mut_labels(|labels| {
        labels.set_endpoint_uuid(endpoint_schema.uuid());
        labels.set_endpoint_id(endpoint_schema.id());
        labels.set_endpoint_kind(endpoint_schema.kind());
    });

    let (_, resolved_connection, _) = match build_proxy_client(&endpoint_schema) {
        Ok(client) => client,
        Err(err) => return Ok(openai_error_from_ep_error(err, &mut span)),
    };

    proxy_state.mark_last_used(api_key.id, now);
    Ok(HttpResponse::Ok().json(openai_models_response(&api_key, &resolved_connection)))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct OpenAiModelListResponse {
    object: &'static str,
    data: Vec<OpenAiModelResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct OpenAiModelResponse {
    id: String,
    object: &'static str,
    created: u64,
    owned_by: String,
}

fn openai_models_response(api_key: &ApiKey, resolved_connection: &ResolvedLlmConnection) -> OpenAiModelListResponse {
    let owned_by = resolved_connection.provider.to_string();
    let data = gateway_model_ids(api_key, resolved_connection)
        .into_iter()
        .map(|id| OpenAiModelResponse { id, object: "model", created: 0, owned_by: owned_by.clone() })
        .collect();

    OpenAiModelListResponse { object: "list", data }
}

fn gateway_model_ids(api_key: &ApiKey, resolved_connection: &ResolvedLlmConnection) -> Vec<String> {
    let mut models = Vec::new();

    if let Some(allowlist) = api_key.model_allowlist.as_ref() {
        for model in allowlist {
            push_unique_model_id(&mut models, model);
        }
        return models;
    }

    push_unique_model_id(&mut models, &resolved_connection.defaults.model);

    for entry in LlmGatewayModelCatalog::builtin().entries() {
        if entry.provider.eq_ignore_ascii_case(resolved_connection.provider.as_str())
            && entry.operations.contains(&LlmModelOperation::ChatCompletions)
        {
            push_unique_model_id(&mut models, &entry.id);
        }
    }

    models
}

fn push_unique_model_id(models: &mut Vec<String>, model: &str) {
    let model = model.trim();
    if model.is_empty() || models.iter().any(|existing| existing.eq_ignore_ascii_case(model)) {
        return;
    }
    models.push(model.to_string());
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProxyAgentContext {
    agent_id: String,
    session_id: String,
    fingerprint: String,
    principal: Option<String>,
    tags_json: Option<String>,
}

fn resolve_proxy_agent_context(
    req: &HttpRequest,
    agent_gateway: Option<&AgentGatewayState>,
    api_key: &super::keys::ApiKey,
    prompt_tokens: u32,
    completion_tokens: u32,
) -> Result<Option<ProxyAgentContext>, HttpResponse> {
    let Some(agent_id) = proxy_consumer_header(req, LLM_GATEWAY_AGENT_ID_HEADER) else {
        return Ok(None);
    };

    let Some(agent_gateway) = agent_gateway else {
        return Err(openai_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Agent gateway runtime is unavailable",
            "server_error",
            "agent_gateway_unavailable",
        ));
    };

    let org_id = OrganizationUuid::from(api_key.org_uuid).to_string();
    let Some(route) = agent_gateway.route_to_agent(&org_id, &agent_id) else {
        return Err(openai_error_response(
            StatusCode::FORBIDDEN,
            "Agent has no active gateway route",
            "invalid_request_error",
            "agent_route_missing",
        ));
    };

    let usage = AgentGatewayUsageEvent {
        request_count: 1,
        prompt_tokens: u64::from(prompt_tokens),
        completion_tokens: u64::from(completion_tokens),
        total_tokens: u64::from(prompt_tokens).saturating_add(u64::from(completion_tokens)),
        cost_microdollars: 0,
    };
    let decision = match agent_gateway.record_usage(&org_id, &route.session_id, usage) {
        Ok(decision) => decision,
        Err(_) => {
            return Err(openai_error_response(
                StatusCode::FORBIDDEN,
                "Agent gateway route is no longer active",
                "invalid_request_error",
                "agent_route_missing",
            ));
        }
    };

    if !decision.allowed {
        let reason = decision.reason.map(|reason| reason.to_string()).unwrap_or_else(|| "usage_window".to_string());
        return Err(openai_error_response(
            StatusCode::TOO_MANY_REQUESTS,
            &format!("Agent gateway rate limit exceeded: {reason}"),
            "rate_limit_error",
            "agent_rate_limited",
        ));
    }

    Ok(Some(ProxyAgentContext::from_route(route)))
}

impl ProxyAgentContext {
    fn from_route(route: AgentGatewayRoute) -> Self {
        let tags_json = if route.identity.tags.is_empty() {
            None
        } else {
            serde_json::to_string(&route.identity.tags).ok()
        };
        Self {
            agent_id: route.agent_id,
            session_id: route.session_id,
            fingerprint: route.identity.fingerprint,
            principal: route.identity.principal,
            tags_json,
        }
    }
}

fn insert_agent_response_headers(builder: &mut HttpResponseBuilder, context: Option<&ProxyAgentContext>) {
    let Some(context) = context else {
        return;
    };

    insert_header_if_valid(builder, LLM_GATEWAY_AGENT_ID_HEADER, &context.agent_id);
    insert_header_if_valid(builder, LLM_GATEWAY_AGENT_SESSION_HEADER, &context.session_id);
    insert_header_if_valid(builder, LLM_GATEWAY_AGENT_FINGERPRINT_HEADER, &context.fingerprint);
    if let Some(principal) = context.principal.as_deref() {
        insert_header_if_valid(builder, LLM_GATEWAY_AGENT_PRINCIPAL_HEADER, principal);
    }
    if let Some(tags_json) = context.tags_json.as_deref() {
        insert_header_if_valid(builder, LLM_GATEWAY_AGENT_TAGS_HEADER, tags_json);
    }
}

fn insert_header_if_valid(builder: &mut HttpResponseBuilder, name: &'static str, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        builder.insert_header((name, value));
    }
}

#[derive(Debug, Clone)]
struct ProxyRouteDecision {
    selected_model: String,
    price_source: Option<PriceSource>,
    baseline_estimated_cost_micros: u64,
    selected_estimated_cost_micros: u64,
    estimated_arbitrage_savings_micros: u64,
    reason: String,
    route_optimization_mode: LlmRouteOptimizationMode,
    kv_cache_mode: LlmKvCacheMode,
    kv_cache_status: LlmKvCacheStatus,
    estimated_kv_cache_savings_micros: u64,
    route_move_reason: Option<String>,
    conversation_route_key: Option<String>,
}

#[allow(clippy::too_many_arguments)]
fn select_proxy_route(
    req: &HttpRequest,
    request: &OpenAiChatCompletionRequest,
    proxy_state: &ProxyGatewayState,
    api_key: &super::keys::ApiKey,
    endpoint_uuid: uuid::Uuid,
    provider: LlmProvider,
    requested_model: &str,
    prompt_tokens: u32,
    completion_tokens: u32,
    now: chrono::DateTime<Utc>,
) -> ProxyRouteDecision {
    if !api_key_allows_requested_model(api_key, requested_model) {
        let estimated_cost_micros = estimate_cost_micros(&provider.to_string(), requested_model, prompt_tokens, completion_tokens);
        return ProxyRouteDecision {
            selected_model: requested_model.to_string(),
            price_source: None,
            baseline_estimated_cost_micros: estimated_cost_micros,
            selected_estimated_cost_micros: estimated_cost_micros,
            estimated_arbitrage_savings_micros: 0,
            reason: "requested_model_not_allowed".to_string(),
            route_optimization_mode: api_key.route_optimization_mode,
            kv_cache_mode: api_key.kv_cache_mode,
            kv_cache_status: LlmKvCacheStatus::Bypass,
            estimated_kv_cache_savings_micros: 0,
            route_move_reason: None,
            conversation_route_key: None,
        };
    }

    let price_mode = if provider == LlmProvider::OpenRouter {
        api_key.price_arbitrage_mode
    } else {
        PriceArbitrageMode::Disabled
    };
    let base =
        choose_openrouter_price_route(price_mode, requested_model, api_key.model_allowlist.as_deref(), prompt_tokens, completion_tokens);
    let mut decision = ProxyRouteDecision {
        selected_model: base.selected_model,
        price_source: base.price_source,
        baseline_estimated_cost_micros: base.baseline_estimated_cost_micros,
        selected_estimated_cost_micros: base.selected_estimated_cost_micros,
        estimated_arbitrage_savings_micros: base.estimated_savings_micros,
        reason: base.reason,
        route_optimization_mode: api_key.route_optimization_mode,
        kv_cache_mode: api_key.kv_cache_mode,
        kv_cache_status: LlmKvCacheStatus::Bypass,
        estimated_kv_cache_savings_micros: 0,
        route_move_reason: None,
        conversation_route_key: None,
    };

    if provider != LlmProvider::OpenRouter {
        return decision;
    }

    let route_optimization_active = api_key.route_optimization_mode != LlmRouteOptimizationMode::Cost;
    if api_key.price_arbitrage_mode == PriceArbitrageMode::Disabled
        && api_key.kv_cache_mode == LlmKvCacheMode::Disabled
        && !route_optimization_active
    {
        return decision;
    }

    let allowed_models = if api_key.price_arbitrage_mode == PriceArbitrageMode::AllowedModelsCheapest
        || api_key.kv_cache_mode == LlmKvCacheMode::Adaptive
        || route_optimization_active
    {
        api_key.model_allowlist.as_deref()
    } else {
        None
    };
    let candidates = openrouter_price_route_candidates(requested_model, allowed_models, prompt_tokens, completion_tokens);
    let Some(mut selected) = best_route_candidate(proxy_state, endpoint_uuid, api_key.route_optimization_mode, &candidates) else {
        return decision;
    };

    let baseline_cost =
        decision
            .baseline_estimated_cost_micros
            .max(estimate_cost_micros("openrouter", requested_model, prompt_tokens, completion_tokens));
    let mut kv_cache_status = LlmKvCacheStatus::Bypass;
    let mut route_move_reason = None;
    let mut estimated_kv_cache_savings_micros = 0;
    let conversation_route_key = kv_cache_ttl_secs(api_key).and_then(|_| conversation_route_key(req, request, api_key));

    if let (Some(route_key), LlmKvCacheMode::Affinity | LlmKvCacheMode::Adaptive) =
        (conversation_route_key.as_deref(), api_key.kv_cache_mode)
    {
        match proxy_state.lookup_conversation_route(route_key, now) {
            Some(existing_route) => {
                if let Some(affinity_candidate) = find_route_candidate(&candidates, &existing_route.provider, &existing_route.model) {
                    match api_key.kv_cache_mode {
                        LlmKvCacheMode::Affinity => {
                            selected = affinity_candidate;
                            kv_cache_status = LlmKvCacheStatus::Hit;
                            estimated_kv_cache_savings_micros = estimate_kv_cache_savings_micros(affinity_candidate, prompt_tokens);
                        }
                        LlmKvCacheMode::Adaptive => {
                            let best_score = route_candidate_score(proxy_state, endpoint_uuid, api_key.route_optimization_mode, selected);
                            let affinity_score =
                                route_candidate_score(proxy_state, endpoint_uuid, api_key.route_optimization_mode, affinity_candidate);
                            if route_models_match(&selected.model, &affinity_candidate.model) {
                                kv_cache_status = LlmKvCacheStatus::Hit;
                                estimated_kv_cache_savings_micros = estimate_kv_cache_savings_micros(affinity_candidate, prompt_tokens);
                            } else if score_improvement_percent(affinity_score, best_score) >= route_switch_threshold_percent(api_key) {
                                kv_cache_status = LlmKvCacheStatus::Move;
                                route_move_reason = Some(format!("{}_threshold_exceeded", api_key.route_optimization_mode));
                            } else {
                                selected = affinity_candidate;
                                kv_cache_status = LlmKvCacheStatus::Hit;
                                estimated_kv_cache_savings_micros = estimate_kv_cache_savings_micros(affinity_candidate, prompt_tokens);
                                route_move_reason = Some("affinity_within_threshold".to_string());
                            }
                        }
                        LlmKvCacheMode::Disabled => {}
                    }
                } else {
                    kv_cache_status = LlmKvCacheStatus::Miss;
                    route_move_reason = Some("cached_route_not_allowed".to_string());
                }
            }
            None => {
                kv_cache_status = LlmKvCacheStatus::Miss;
            }
        }
    }

    let selected_cost = selected.estimated_cost_micros;
    let estimated_savings = baseline_cost.saturating_sub(selected_cost);
    let reason = route_reason(
        api_key.route_optimization_mode,
        requested_model,
        &selected.model,
        estimated_savings,
        kv_cache_status,
    );

    decision.selected_model = selected.model.clone();
    decision.price_source = Some(selected.source);
    decision.baseline_estimated_cost_micros = baseline_cost;
    decision.selected_estimated_cost_micros = selected_cost;
    decision.estimated_arbitrage_savings_micros = estimated_savings;
    decision.reason = reason;
    decision.kv_cache_status = kv_cache_status;
    decision.estimated_kv_cache_savings_micros = estimated_kv_cache_savings_micros;
    decision.route_move_reason = route_move_reason;
    decision.conversation_route_key = conversation_route_key;
    decision
}

fn api_key_allows_requested_model(api_key: &super::keys::ApiKey, requested_model: &str) -> bool {
    api_key
        .model_allowlist
        .as_ref()
        .is_none_or(|allowlist| allowlist.iter().any(|allowed_model| route_models_match(allowed_model, requested_model)))
}

fn best_route_candidate<'a>(
    proxy_state: &ProxyGatewayState,
    endpoint_uuid: uuid::Uuid,
    mode: LlmRouteOptimizationMode,
    candidates: &'a [PriceRouteCandidate],
) -> Option<&'a PriceRouteCandidate> {
    candidates.iter().min_by(|left, right| {
        route_candidate_score(proxy_state, endpoint_uuid, mode, left)
            .total_cmp(&route_candidate_score(proxy_state, endpoint_uuid, mode, right))
            .then_with(|| left.estimated_cost_micros.cmp(&right.estimated_cost_micros))
            .then_with(|| left.model.cmp(&right.model))
    })
}

fn route_candidate_score(
    proxy_state: &ProxyGatewayState,
    endpoint_uuid: uuid::Uuid,
    mode: LlmRouteOptimizationMode,
    candidate: &PriceRouteCandidate,
) -> f64 {
    let stats = proxy_state.route_stats(endpoint_uuid, &candidate.provider, &candidate.model);
    match mode {
        LlmRouteOptimizationMode::Cost => candidate.estimated_cost_micros as f64,
        LlmRouteOptimizationMode::Latency => stats.map(|stats| stats.avg_latency_ms).unwrap_or_else(|| cold_route_score(candidate)),
        LlmRouteOptimizationMode::Throughput => stats.and_then(throughput_score).unwrap_or_else(|| cold_route_score(candidate)),
        LlmRouteOptimizationMode::Balanced => balanced_route_score(candidate, stats),
    }
}

fn throughput_score(stats: ProxyRouteStatsSnapshot) -> Option<f64> {
    if stats.avg_tokens_per_second <= 0.0 {
        return None;
    }
    Some(1_000_000.0 / stats.avg_tokens_per_second)
}

fn balanced_route_score(candidate: &PriceRouteCandidate, stats: Option<ProxyRouteStatsSnapshot>) -> f64 {
    let cost_score = candidate.estimated_cost_micros as f64 / 1_000.0;
    let Some(stats) = stats else {
        return cost_score + 1_000.0;
    };
    let throughput_penalty = throughput_score(stats).unwrap_or(1_000.0);
    let sample_penalty = if stats.request_count < 3 { 250.0 } else { 0.0 };
    cost_score + stats.avg_latency_ms + throughput_penalty + sample_penalty
}

fn cold_route_score(candidate: &PriceRouteCandidate) -> f64 {
    100_000.0 + (candidate.estimated_cost_micros as f64 / 1_000.0)
}

fn find_route_candidate<'a>(candidates: &'a [PriceRouteCandidate], provider: &str, model: &str) -> Option<&'a PriceRouteCandidate> {
    candidates
        .iter()
        .find(|candidate| candidate.provider.eq_ignore_ascii_case(provider) && route_models_match(&candidate.model, model))
}

fn route_models_match(left: &str, right: &str) -> bool {
    normalize_route_model(left) == normalize_route_model(right)
}

fn normalize_route_model(model: &str) -> String {
    let normalized = model.trim().to_ascii_lowercase();
    normalized.split_once('/').map(|(_, suffix)| suffix.to_string()).unwrap_or(normalized)
}

fn score_improvement_percent(current_score: f64, best_score: f64) -> u8 {
    if !current_score.is_finite() || !best_score.is_finite() || current_score <= 0.0 || best_score >= current_score {
        return 0;
    }
    (((current_score - best_score) / current_score) * 100.0).round().clamp(0.0, 100.0) as u8
}

fn route_switch_threshold_percent(api_key: &super::keys::ApiKey) -> u8 {
    api_key.route_switch_threshold_percent.unwrap_or(15).min(100)
}

fn route_reason(
    mode: LlmRouteOptimizationMode,
    requested_model: &str,
    selected_model: &str,
    estimated_savings_micros: u64,
    kv_cache_status: LlmKvCacheStatus,
) -> String {
    if kv_cache_status == LlmKvCacheStatus::Move {
        return format!("{mode}_route_moved");
    }
    if route_models_match(requested_model, selected_model) {
        if estimated_savings_micros > 0 {
            "same_model_cheaper_provider".to_string()
        } else {
            "no_route_change".to_string()
        }
    } else if mode == LlmRouteOptimizationMode::Cost && estimated_savings_micros > 0 {
        "allowed_model_cheaper".to_string()
    } else {
        format!("{mode}_route_selected")
    }
}

fn kv_cache_ttl_secs(api_key: &super::keys::ApiKey) -> Option<u64> {
    if api_key.kv_cache_mode == LlmKvCacheMode::Disabled {
        return None;
    }
    Some(api_key.kv_cache_ttl_secs.unwrap_or(1_800).clamp(1, 86_400))
}

fn conversation_route_key(req: &HttpRequest, request: &OpenAiChatCompletionRequest, api_key: &super::keys::ApiKey) -> Option<String> {
    let raw_key = conversation_header(req).or_else(|| prompt_prefix_fingerprint(request))?;
    let mut hasher = Sha256::new();
    hasher.update(api_key.org_uuid.as_bytes());
    hasher.update(api_key.endpoint_uuid.as_bytes());
    hasher.update(api_key.id.as_bytes());
    hasher.update(raw_key.as_bytes());
    Some(hex::encode(hasher.finalize()))
}

fn conversation_header(req: &HttpRequest) -> Option<String> {
    ["x-eden-conversation-id", "x-conversation-id", "openai-conversation-id"]
        .iter()
        .filter_map(|name| req.headers().get(*name))
        .filter_map(|value| value.to_str().ok())
        .map(str::trim)
        .find(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn prompt_prefix_fingerprint(request: &OpenAiChatCompletionRequest) -> Option<String> {
    if request.messages.len() < 2 {
        return None;
    }
    let mut hasher = Sha256::new();
    for message in request.messages.iter().take(request.messages.len().saturating_sub(1)) {
        hasher.update(message.role.as_bytes());
        if let Some(content) = message.content.as_deref() {
            hasher.update(content.as_bytes());
        }
        for tool_call in &message.tool_calls {
            hasher.update(tool_call.id.as_bytes());
            hasher.update(tool_call.call_type.as_bytes());
            hasher.update(tool_call.function.name.as_bytes());
            hasher.update(tool_call.function.arguments.as_bytes());
        }
        if let Some(tool_call_id) = message.tool_call_id.as_deref() {
            hasher.update(tool_call_id.as_bytes());
        }
    }
    Some(hex::encode(hasher.finalize()))
}

fn estimate_kv_cache_savings_micros(candidate: &PriceRouteCandidate, prompt_tokens: u32) -> u64 {
    let prompt_cost = (prompt_tokens as u128) * (candidate.input_micros_per_million as u128);
    (((prompt_cost + 500_000) / 1_000_000) / 2) as u64
}

fn record_successful_route(
    proxy_state: &ProxyGatewayState,
    api_key: &super::keys::ApiKey,
    provider: &str,
    model: &str,
    route_decision: &ProxyRouteDecision,
    latency_ms: u64,
    total_tokens: u32,
    now: chrono::DateTime<Utc>,
) -> ProxyRouteStatsSnapshot {
    let snapshot = proxy_state.record_route_observation(api_key.endpoint_uuid, provider, model, latency_ms, total_tokens, now);
    if let (Some(route_key), Some(ttl_secs)) = (route_decision.conversation_route_key.as_ref(), kv_cache_ttl_secs(api_key)) {
        proxy_state.store_conversation_route(route_key.clone(), api_key.org_uuid, provider.to_string(), model.to_string(), ttl_secs, now);
    }
    snapshot
}

fn estimate_prompt_tokens(request: &OpenAiChatCompletionRequest) -> u32 {
    let message_chars = request
        .messages
        .iter()
        .map(|message| {
            message.role.len()
                + message.content.as_ref().map(String::len).unwrap_or_default()
                + message
                    .tool_calls
                    .iter()
                    .map(|call| call.id.len() + call.call_type.len() + call.function.name.len() + call.function.arguments.len())
                    .sum::<usize>()
                + message.tool_call_id.as_ref().map(String::len).unwrap_or_default()
        })
        .sum::<usize>();
    let tool_chars = request.tools.iter().filter_map(|tool| serde_json::to_string(tool).ok()).map(|tool| tool.len()).sum::<usize>();
    let estimated = (message_chars + tool_chars).saturating_div(4).saturating_add(request.messages.len().saturating_mul(4));
    u32::try_from(estimated.max(1)).unwrap_or(u32::MAX)
}

fn response_cache_ttl_secs(
    api_key: &super::keys::ApiKey,
    request: &OpenAiChatCompletionRequest,
    request_mentions_tools: bool,
) -> Option<u64> {
    if request.stream || request_mentions_tools {
        return None;
    }
    api_key.response_cache_ttl_secs.filter(|ttl| *ttl > 0)
}

fn response_cache_key(api_key: &super::keys::ApiKey, provider: &endpoint_core::llm_core::LlmProviderMetadata, body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(api_key.org_uuid.as_bytes());
    hasher.update(api_key.endpoint_uuid.as_bytes());
    hasher.update(api_key.id.as_bytes());
    hasher.update(provider.provider.as_bytes());
    hasher.update(provider.model.as_bytes());
    hasher.update(body);
    hex::encode(hasher.finalize())
}

fn prepare_cached_openai_response(
    mut response: OpenAiChatCompletionResponse,
    provider: &endpoint_core::llm_core::LlmProviderMetadata,
) -> OpenAiChatCompletionResponse {
    response.id = new_openai_completion_id();
    response.created = Utc::now().timestamp();
    response.model = provider.model.clone();
    response
}

fn usage_from_openai_response(response: &OpenAiChatCompletionResponse) -> LlmUsage {
    LlmUsage {
        prompt_tokens: response.usage.prompt_tokens,
        completion_tokens: response.usage.completion_tokens,
        total_tokens: response.usage.total_tokens,
        completion_tokens_details: None,
        prompt_tokens_details: None,
    }
}

fn openai_response_from_cached_value(value: serde_json::Value) -> Result<OpenAiChatCompletionResponse, serde_json::Error> {
    let cached = serde_json::from_value::<CachedOpenAiChatCompletionResponse>(value)?;
    Ok(OpenAiChatCompletionResponse {
        id: cached.id,
        object: "chat.completion",
        created: cached.created,
        model: cached.model,
        choices: cached
            .choices
            .into_iter()
            .map(|choice| OpenAiChatCompletionChoice {
                index: choice.index,
                message: OpenAiAssistantMessage {
                    role: "assistant",
                    content: choice.message.content,
                    tool_calls: choice.message.tool_calls,
                },
                finish_reason: choice.finish_reason,
            })
            .collect(),
        usage: endpoint_core::llm_core::OpenAiUsage {
            prompt_tokens: cached.usage.prompt_tokens,
            completion_tokens: cached.usage.completion_tokens,
            total_tokens: cached.usage.total_tokens,
        },
    })
}

#[derive(Debug, Deserialize)]
struct CachedOpenAiChatCompletionResponse {
    id: String,
    created: i64,
    model: String,
    choices: Vec<CachedOpenAiChatCompletionChoice>,
    usage: CachedOpenAiUsage,
}

#[derive(Debug, Deserialize)]
struct CachedOpenAiChatCompletionChoice {
    index: u32,
    message: CachedOpenAiAssistantMessage,
    finish_reason: String,
}

#[derive(Debug, Deserialize)]
struct CachedOpenAiAssistantMessage {
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<endpoint_core::llm_core::OpenAiToolCall>,
}

#[derive(Debug, Deserialize)]
struct CachedOpenAiUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

async fn sync_proxy_budget_window(
    database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
    proxy_state: &ProxyGatewayState,
    api_key: &super::keys::ApiKey,
    now: chrono::DateTime<Utc>,
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) {
    if api_key.budget_tokens_monthly.filter(|limit| *limit > 0).is_none() {
        return;
    }

    let org_uuid = OrganizationUuid::from(api_key.org_uuid);
    let month_bucket = current_budget_month_bucket(now);
    match database
        .get_llm_gateway_usage_rollup(&org_uuid, "api_key", &api_key.id.to_string(), month_bucket, telemetry_wrapper)
        .await
    {
        Ok(Some(rollup)) => proxy_state.set_budget_usage(api_key.id, month_bucket, rollup.total_tokens),
        Ok(None) => {}
        Err(error) => tracing::warn!(%error, key_id = %api_key.id, "Failed to hydrate LLM gateway durable budget window"),
    }
}

async fn lookup_cached_response(
    database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
    proxy_state: &ProxyGatewayState,
    api_key: &super::keys::ApiKey,
    cache_key: &str,
    now: chrono::DateTime<Utc>,
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) -> Option<OpenAiChatCompletionResponse> {
    if let Some(cached_response) = proxy_state.lookup_response_cache(api_key.org_uuid, cache_key, now) {
        return Some(cached_response);
    }

    let org_uuid = OrganizationUuid::from(api_key.org_uuid);
    let cache_entry = match database.get_llm_gateway_response_cache_entry(&org_uuid, cache_key, now, telemetry_wrapper).await {
        Ok(cache_entry) => cache_entry,
        Err(error) => {
            tracing::warn!(%error, key_id = %api_key.id, "Failed to load durable LLM gateway response cache entry");
            return None;
        }
    }?;

    let response = match openai_response_from_cached_value(cache_entry.response_json) {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, cache_key, "Failed to decode durable LLM gateway response cache entry");
            return None;
        }
    };

    let ttl_secs = cache_entry.expires_at.signed_duration_since(now).num_seconds().max(1) as u64;
    proxy_state.store_response_cache(cache_key.to_string(), api_key.org_uuid, response.clone(), ttl_secs, now);
    if let Err(error) = database.touch_llm_gateway_response_cache_entry(&org_uuid, cache_key, now, telemetry_wrapper).await {
        tracing::warn!(%error, cache_key, "Failed to touch durable LLM gateway response cache entry");
    }
    Some(response)
}

#[allow(clippy::too_many_arguments)]
async fn persist_response_cache_entry(
    database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
    api_key: &super::keys::ApiKey,
    provider: &LlmProviderMetadata,
    request_body: &[u8],
    cache_key: &str,
    response: &OpenAiChatCompletionResponse,
    prompt_fingerprint: Option<&str>,
    ttl_secs: u64,
    now: chrono::DateTime<Utc>,
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) {
    let response_json = match serde_json::to_value(response) {
        Ok(response_json) => response_json,
        Err(error) => {
            tracing::warn!(%error, cache_key, "Failed to encode LLM gateway response cache entry");
            return;
        }
    };
    let usage = usage_from_openai_response(response);
    let estimated_cost_micros = estimate_cost_micros(&provider.provider, &provider.model, usage.prompt_tokens, usage.completion_tokens);
    let ttl_secs = i64::try_from(ttl_secs.min(86_400)).unwrap_or(86_400);
    let expires_at = now + Duration::seconds(ttl_secs.max(1));
    let org_uuid = OrganizationUuid::from(api_key.org_uuid);
    let request_hash = hash_bytes(request_body);

    let entry = NewLlmGatewayResponseCacheEntry {
        cache_key,
        organization_uuid: &org_uuid,
        endpoint_uuid: api_key.endpoint_uuid,
        key_id: api_key.id,
        provider: &provider.provider,
        model: &provider.model,
        request_hash: &request_hash,
        prompt_fingerprint,
        response_json: &response_json,
        prompt_tokens: u64::from(usage.prompt_tokens),
        completion_tokens: u64::from(usage.completion_tokens),
        total_tokens: u64::from(usage.total_tokens),
        estimated_cost_micros,
        created_at: now,
        updated_at: now,
        expires_at,
    };

    if let Err(error) = database.upsert_llm_gateway_response_cache_entry(entry, telemetry_wrapper).await {
        tracing::warn!(%error, cache_key, "Failed to persist LLM gateway response cache entry");
    }
}

#[allow(clippy::too_many_arguments)]
async fn persist_proxy_usage_rollups(
    database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
    api_key: &super::keys::ApiKey,
    downstream_consumers: &[ProxyDownstreamConsumer],
    context: &ProxyAnalyticsContext,
    usage: Option<&LlmUsage>,
    provider: &LlmProviderMetadata,
    now: chrono::DateTime<Utc>,
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) {
    let Some(usage) = usage else {
        return;
    };

    let org_uuid = OrganizationUuid::from(api_key.org_uuid);
    let month_bucket = current_budget_month_bucket(now);
    let cache_hit_count = if context.cache_status == LlmCacheStatus::Hit { 1 } else { 0 };
    let kv_cache_hit_count = if matches!(context.kv_cache_status, LlmKvCacheStatus::Hit | LlmKvCacheStatus::Move) {
        1
    } else {
        0
    };
    let estimated_cost_micros = if context.cache_status == LlmCacheStatus::Hit {
        0
    } else {
        estimate_cost_micros(&provider.provider, &provider.model, usage.prompt_tokens, usage.completion_tokens)
    };

    let mut consumers = vec![
        ProxyDownstreamConsumer {
            kind: "organization".to_string(),
            id: api_key.org_uuid.to_string(),
        },
        ProxyDownstreamConsumer { kind: "api_key".to_string(), id: api_key.id.to_string() },
    ];
    consumers.extend_from_slice(downstream_consumers);

    for consumer in consumers {
        let rollup = NewLlmGatewayUsageRollup {
            organization_uuid: &org_uuid,
            consumer_kind: &consumer.kind,
            consumer_id: &consumer.id,
            month_bucket,
            endpoint_uuid: Some(api_key.endpoint_uuid),
            request_count: 1,
            prompt_tokens: u64::from(usage.prompt_tokens),
            completion_tokens: u64::from(usage.completion_tokens),
            total_tokens: u64::from(usage.total_tokens),
            estimated_cost_micros,
            cache_hit_count,
            kv_cache_hit_count,
            rate_limited_count: 0,
            updated_at: now,
        };
        if let Err(error) = database.record_llm_gateway_usage_rollup(rollup, telemetry_wrapper).await {
            tracing::warn!(
                %error,
                consumer_kind = consumer.kind,
                consumer_id = consumer.id,
                "Failed to persist LLM gateway usage rollup"
            );
        }
    }
}

async fn persist_proxy_rate_limited_rollups(
    database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
    api_key: &super::keys::ApiKey,
    downstream_consumers: &[ProxyDownstreamConsumer],
    now: chrono::DateTime<Utc>,
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) {
    let org_uuid = OrganizationUuid::from(api_key.org_uuid);
    let month_bucket = current_budget_month_bucket(now);
    let mut consumers = vec![
        ProxyDownstreamConsumer {
            kind: "organization".to_string(),
            id: api_key.org_uuid.to_string(),
        },
        ProxyDownstreamConsumer { kind: "api_key".to_string(), id: api_key.id.to_string() },
    ];
    consumers.extend_from_slice(downstream_consumers);

    for consumer in consumers {
        let rollup = NewLlmGatewayUsageRollup {
            organization_uuid: &org_uuid,
            consumer_kind: &consumer.kind,
            consumer_id: &consumer.id,
            month_bucket,
            endpoint_uuid: Some(api_key.endpoint_uuid),
            request_count: 0,
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
            estimated_cost_micros: 0,
            cache_hit_count: 0,
            kv_cache_hit_count: 0,
            rate_limited_count: 1,
            updated_at: now,
        };
        if let Err(error) = database.record_llm_gateway_usage_rollup(rollup, telemetry_wrapper).await {
            tracing::warn!(
                %error,
                consumer_kind = consumer.kind,
                consumer_id = consumer.id,
                "Failed to persist LLM gateway rate-limited rollup"
            );
        }
    }
}

async fn persist_proxy_route_rollup(
    database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
    api_key: &super::keys::ApiKey,
    provider: &str,
    model: &str,
    snapshot: ProxyRouteStatsSnapshot,
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) {
    let rollup = StoredLlmGatewayRouteRollup {
        organization_uuid: OrganizationUuid::from(api_key.org_uuid),
        endpoint_uuid: api_key.endpoint_uuid,
        provider: provider.to_ascii_lowercase(),
        model: model.to_ascii_lowercase(),
        route_class: "default".to_string(),
        success_count: snapshot.request_count,
        error_count: 0,
        total_latency_ms: snapshot.total_latency_ms,
        min_latency_ms: snapshot.min_latency_ms,
        max_latency_ms: snapshot.max_latency_ms,
        total_output_tokens: snapshot.total_output_tokens,
        total_duration_ms: snapshot.total_duration_ms,
        first_observed_at: snapshot.first_seen_at,
        last_observed_at: snapshot.last_seen_at,
        updated_at: Utc::now(),
    };

    if let Err(error) = database.upsert_llm_gateway_route_rollup(&rollup, telemetry_wrapper).await {
        tracing::warn!(%error, key_id = %api_key.id, "Failed to persist LLM gateway route rollup");
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProxyDownstreamConsumer {
    kind: String,
    id: String,
}

fn proxy_downstream_consumers(req: &HttpRequest) -> Vec<ProxyDownstreamConsumer> {
    let candidates = [
        ("user", "x-eden-user-id"),
        ("agent", "x-eden-agent-id"),
        ("consumer", "x-eden-consumer-id"),
    ];
    let mut consumers = Vec::new();
    for (kind, header_name) in candidates {
        if let Some(id) = proxy_consumer_header(req, header_name)
            && !consumers.iter().any(|consumer: &ProxyDownstreamConsumer| consumer.kind == kind && consumer.id == id)
        {
            consumers.push(ProxyDownstreamConsumer { kind: kind.to_string(), id });
        }
    }
    consumers
}

fn proxy_consumer_header(req: &HttpRequest, header_name: &str) -> Option<String> {
    req.headers()
        .get(header_name)?
        .to_str()
        .ok()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(256).collect())
}

fn hash_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use eden_gateway::agent::{
        AgentConnectionMetrics, AgentConnectionRegistration, AgentGatewayIdentity, AgentGatewayNetworkEndpoint, AgentGatewayRateLimit,
        AgentGatewayTransport,
    };
    use endpoint_core::llm_core::connection::LlmConnectionDefaults;
    use endpoint_core::llm_core::credential::ResolvedProviderConfig;
    use endpoint_core::llm_core::{LlmKvCacheMode, LlmRouteOptimizationMode, PolicyAction, PriceArbitrageMode};
    use uuid::Uuid;

    fn api_key(org_uuid: Uuid) -> super::super::keys::ApiKey {
        let now = Utc::now();
        super::super::keys::ApiKey {
            id: Uuid::new_v4(),
            org_uuid,
            name: "test-key".to_string(),
            key_hash: "hash".to_string(),
            key_prefix: "eden-gateway-test".to_string(),
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
        }
    }

    fn agent_request(agent_id: Uuid) -> HttpRequest {
        actix_web::test::TestRequest::default()
            .insert_header((LLM_GATEWAY_AGENT_ID_HEADER, agent_id.to_string()))
            .to_http_request()
    }

    fn resolved_connection(provider: LlmProvider, model: &str) -> ResolvedLlmConnection {
        ResolvedLlmConnection {
            provider,
            credential_id: None,
            api_key: Some("test-key".to_string()),
            credential_base_url: None,
            defaults: LlmConnectionDefaults { model: model.to_string(), ..Default::default() },
            provider_config: ResolvedProviderConfig::None,
        }
    }

    #[test]
    fn models_response_uses_api_key_allowlist_when_present() {
        let org_uuid = Uuid::new_v4();
        let mut key = api_key(org_uuid);
        key.model_allowlist = Some(vec![
            "openai/gpt-4.1-mini".to_string(),
            " openai/gpt-4.1-mini ".to_string(),
            "anthropic/claude-sonnet-4.5".to_string(),
            String::new(),
        ]);
        let resolved = resolved_connection(LlmProvider::OpenRouter, "openai/gpt-4.1");

        let response = openai_models_response(&key, &resolved);
        let ids = response.data.iter().map(|model| model.id.as_str()).collect::<Vec<_>>();

        assert_eq!(ids, vec!["openai/gpt-4.1-mini", "anthropic/claude-sonnet-4.5"]);
        assert!(response.data.iter().all(|model| model.object == "model"));
        assert!(response.data.iter().all(|model| model.owned_by == "openrouter"));
    }

    #[test]
    fn models_response_includes_endpoint_default_without_allowlist() {
        let org_uuid = Uuid::new_v4();
        let key = api_key(org_uuid);
        let resolved = resolved_connection(LlmProvider::OpenAI, "gpt-5.4-mini");

        let response = openai_models_response(&key, &resolved);
        let ids = response.data.iter().map(|model| model.id.as_str()).collect::<Vec<_>>();

        assert_eq!(response.object, "list");
        assert!(ids.contains(&"gpt-5.4-mini"));
        assert!(ids.contains(&"gpt-5"));
    }

    #[test]
    fn responses_surface_normalizes_to_chat_request() {
        let body = Bytes::from_static(br#"{"model":"openrouter/test","input":"hello","instructions":"be direct","max_output_tokens":32}"#);

        let (request, normalized_body) =
            OpenAiProxySurface::Responses.chat_request_from_body(&body).expect("responses body should normalize");

        assert_eq!(OpenAiProxySurface::Responses.operation(false), "responses");
        assert_eq!(request.model, "openrouter/test");
        assert_eq!(request.max_tokens, Some(32));
        assert_eq!(request.messages.len(), 2);
        assert_ne!(normalized_body, body);
    }

    #[test]
    fn responses_surface_rejects_streaming_until_supported() {
        let body = Bytes::from_static(br#"{"model":"openrouter/test","input":"hello","stream":true}"#);

        let response = OpenAiProxySurface::Responses
            .chat_request_from_body(&body)
            .expect_err("streaming Responses should return a client error");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn route_selection_preserves_disallowed_requested_model_for_governance() {
        let org_uuid = Uuid::new_v4();
        let mut key = api_key(org_uuid);
        key.model_allowlist = Some(vec!["openai/gpt-4o-mini".to_string()]);
        key.route_optimization_mode = LlmRouteOptimizationMode::Balanced;
        key.kv_cache_mode = LlmKvCacheMode::Adaptive;

        let req = actix_web::test::TestRequest::default().to_http_request();
        let request = OpenAiChatCompletionRequest {
            model: "unapproved-model".to_string(),
            messages: Vec::new(),
            stream: false,
            temperature: None,
            max_tokens: Some(32),
            top_p: None,
            tools: Vec::new(),
            tool_choice: None,
        };

        let decision = select_proxy_route(
            &req,
            &request,
            &ProxyGatewayState::default(),
            &key,
            key.endpoint_uuid,
            LlmProvider::OpenRouter,
            "unapproved-model",
            10,
            32,
            Utc::now(),
        );

        assert_eq!(decision.selected_model, "unapproved-model");
        assert_eq!(decision.reason, "requested_model_not_allowed");
    }

    #[test]
    fn proxy_model_allowlist_accepts_provider_prefixed_aliases() {
        let mut key = api_key(Uuid::new_v4());
        key.model_allowlist = Some(vec!["openai/gpt-4o-mini".to_string()]);

        assert!(api_key_allows_requested_model(&key, "GPT-4O-MINI"));
    }

    #[test]
    fn agent_handler_rejects_unregistered_agent_route() {
        let org_uuid = Uuid::new_v4();
        let agent_id = Uuid::new_v4();
        let gateway = AgentGatewayState::default();
        let key = api_key(org_uuid);

        let response = resolve_proxy_agent_context(&agent_request(agent_id), Some(&gateway), &key, 12, 24)
            .expect_err("agent without an active route should be rejected");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn agent_handler_selects_route_and_records_usage_window() {
        let org_uuid = Uuid::new_v4();
        let agent_id = Uuid::new_v4();
        let gateway = AgentGatewayState::default();
        let key = api_key(org_uuid);
        let org_id = OrganizationUuid::from(org_uuid).to_string();
        let session = gateway
            .register_connection(AgentConnectionRegistration {
                org_id: org_id.clone(),
                agent_id: agent_id.to_string(),
                transport: AgentGatewayTransport::A2aHttp,
                identity: AgentGatewayIdentity::default(),
                endpoint: AgentGatewayNetworkEndpoint::default(),
                metrics: AgentConnectionMetrics::default(),
                rate_limit: AgentGatewayRateLimit::default(),
            })
            .expect("agent registration should succeed");

        let context = resolve_proxy_agent_context(&agent_request(agent_id), Some(&gateway), &key, 12, 24)
            .expect("handler should resolve")
            .expect("agent header should produce context");

        assert_eq!(context.agent_id, agent_id.to_string());
        assert_eq!(context.session_id, session.session_id);
        let usage = gateway.list_usage(&org_id);
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].requests, 1);
        assert_eq!(usage[0].prompt_tokens, 12);
        assert_eq!(usage[0].completion_tokens, 24);
    }
}
