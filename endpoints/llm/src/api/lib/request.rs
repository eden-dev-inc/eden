use crate::api::lib::LlmApi;
use crate::output::LlmOutput;
use crate::request::LlmRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_warn};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use llm_core::comm::accumulate_usage;
use llm_core::{
    LlmAsync, LlmChatResponse, LlmInvocation, LlmMessage, LlmMessageKind, LlmMessageRole, LlmRequestOverrides, LlmStructuredOutputFormat,
    LlmToolChoice, LlmToolConnection, LlmToolDefinition, LlmTx, LlmUsage, SystemPromptBlock, ToolRuntime,
};
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use tokio::time::timeout;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<LlmApi, LlmOutput> = ApiInfo::new(EpKind::Llm, LlmApi::Request, "Execute an LLM chat turn", ReqType::Read, true);

const TOOL_DISCOVERY_TIMEOUT_SECS: u64 = 10;

crate::llm_endpoint! {
    Request,
    API_INFO,
    struct {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conversation_id: Option<String>,
        #[serde(default)]
        conversation: Vec<LlmMessage>,
        #[serde(default)]
        tools: Vec<LlmToolDefinition>,
        #[serde(default)]
        tool_choice: Option<LlmToolChoice>,
        #[serde(default)]
        system_prompt: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        system_prompt_blocks: Option<Vec<SystemPromptBlock>>,
        #[serde(default)]
        overrides: LlmRequestOverrides,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        response_format: Option<LlmStructuredOutputFormat>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parallel_tool_calls: Option<bool>,
        #[serde(default)]
        tool_connections: Vec<LlmToolConnection>,
        #[serde(default)]
        tool_endpoint_uuids: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn_context: Option<String>,
    }
}

type OutputWrapper = LlmOutput;

impl_simple_operation!(SimpleInput, LlmAsync, LlmTx, LlmApi, LlmRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: LlmAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("llm.{}.{}", API_INFO.api, function_name!()));
        let log_ctx = ctx_with_trace!().with_feature("llm.endpoint.request");

        let client = context.get().await.map_err(EpError::connect)?;
        let max_tool_passes = client.max_tool_passes();
        let start = std::time::SystemTime::now();

        let mut invocation = self.to_invocation();

        let mut tool_runtime: Option<ToolRuntime> = None;
        if !invocation.tool_connections.is_empty() {
            let runtime = match timeout(
                std::time::Duration::from_secs(TOOL_DISCOVERY_TIMEOUT_SECS),
                ToolRuntime::new(invocation.tool_connections()),
            )
            .await
            {
                Ok(result) => result.map_err(EpError::request)?,
                Err(_) => {
                    log_warn!(
                        log_ctx.clone(),
                        "Timed out initializing tools",
                        audience = LogAudience::Internal,
                        timeout_secs = TOOL_DISCOVERY_TIMEOUT_SECS
                    );
                    return Err(EpError::request("timed out initializing tools"));
                }
            };
            if !runtime.tool_definitions().is_empty() {
                invocation.tools = runtime.tool_definitions().to_vec();
            }
            tool_runtime = Some(runtime);
        }

        let mut conversation = invocation.conversation.clone();
        let mut conversation_delta: Vec<LlmMessage> = Vec::new();
        let mut accumulated_usage: Option<LlmUsage> = None;
        let mut final_response: Option<LlmChatResponse> = None;

        'outer: for iteration in 0..max_tool_passes {
            invocation.conversation = conversation.clone();

            let mut response = client.chat(&invocation).await.map_err(EpError::request)?;

            let usage_fragment = response.usage.take();
            accumulate_usage(&mut accumulated_usage, usage_fragment);

            conversation.extend(response.conversation.clone());
            conversation_delta.extend(response.conversation.clone());

            match response.message.kind.clone() {
                LlmMessageKind::ToolUse { calls } if !calls.is_empty() && tool_runtime.is_some() => {
                    log_debug!(
                        log_ctx.clone(),
                        "Executing tool calls",
                        audience = LogAudience::Internal,
                        iteration = iteration,
                        call_count = calls.len()
                    );

                    let mut tool_result_calls = Vec::with_capacity(calls.len());
                    let mut tool_result_snippets = Vec::with_capacity(calls.len());

                    for call in &calls {
                        let outputs = tool_runtime.as_ref().expect("tool runtime available").call_tool(call).await?;
                        let snippet = if outputs.is_empty() { String::new() } else { outputs.join("\n") };

                        tool_result_snippets.push(snippet.clone());

                        let mut result_call = call.clone();
                        result_call.function.arguments = snippet;
                        tool_result_calls.push(result_call);
                    }

                    let combined_text = if tool_result_snippets.is_empty() {
                        String::new()
                    } else {
                        tool_result_snippets.join("\n\n")
                    };

                    let tool_result_message = LlmMessage {
                        role: LlmMessageRole::User,
                        content: combined_text,
                        kind: LlmMessageKind::ToolResult { calls: tool_result_calls.clone() },
                    };

                    conversation.push(tool_result_message.clone());
                    conversation_delta.push(tool_result_message);

                    if iteration + 1 == max_tool_passes {
                        log_warn!(
                            log_ctx.clone(),
                            "Reached maximum tool iterations; returning intermediate response",
                            audience = LogAudience::Internal,
                            max_passes = max_tool_passes
                        );
                        response.usage = accumulated_usage.clone();
                        response.conversation = conversation_delta.clone();
                        final_response = Some(response);
                        break 'outer;
                    }

                    continue 'outer;
                }
                // If client provided tools, they want to handle execution themselves
                // Only generate "Tool runtime unavailable" if no tools were provided
                LlmMessageKind::ToolUse { calls } if !calls.is_empty() && invocation.tools.is_empty() => {
                    log_warn!(
                        log_ctx.clone(),
                        "LLM requested tool calls but no tools or tool connections were provided",
                        audience = LogAudience::Internal,
                        call_count = calls.len()
                    );
                    let error_text = "Tool runtime unavailable";
                    let mut tool_result_calls = Vec::with_capacity(calls.len());
                    for call in &calls {
                        let mut result_call = call.clone();
                        result_call.function.arguments = error_text.to_string();
                        tool_result_calls.push(result_call);
                    }
                    if !tool_result_calls.is_empty() {
                        let tool_result_message = LlmMessage {
                            role: LlmMessageRole::User,
                            content: error_text.to_string(),
                            kind: LlmMessageKind::ToolResult { calls: tool_result_calls.clone() },
                        };
                        conversation.push(tool_result_message.clone());
                        conversation_delta.push(tool_result_message);
                    }
                    response.usage = accumulated_usage.clone();
                    response.conversation = conversation_delta.clone();
                    final_response = Some(response);
                    break 'outer;
                }
                _ => {
                    response.usage = accumulated_usage.clone();
                    response.conversation = conversation_delta.clone();
                    final_response = Some(response);
                    break 'outer;
                }
            }
        }

        let response = final_response.ok_or_else(|| EpError::request("no response generated"))?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from llm",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
                FastSpanAttribute::new("provider", response.provider.provider.clone()),
                FastSpanAttribute::new("model", response.provider.model.clone()),
            ],
        );

        Ok(Box::new(LlmOutput::chat(response).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, context: &mut LlmTx, telemetry_wrapper: &mut TelemetryWrapper) {
        let _ = context;
        let _ = telemetry_wrapper;
        let log_ctx = ctx_with_trace!().with_feature("llm.endpoint.request");
        log_warn!(log_ctx, "LLM transactions are not supported; operation ignored", audience = LogAudience::Internal);
    }
}

impl SimpleInput {
    fn to_invocation(&self) -> LlmInvocation {
        LlmInvocation {
            conversation_id: self.conversation_id().clone(),
            conversation: self.conversation().clone(),
            tools: self.tools().clone(),
            tool_choice: self.tool_choice().clone(),
            system_prompt: self.system_prompt().clone(),
            system_prompt_blocks: self.system_prompt_blocks().clone(),
            overrides: self.overrides().clone(),
            response_format: self.response_format().clone(),
            parallel_tool_calls: *self.parallel_tool_calls(),
            tool_connections: self.tool_connections().clone(),
            tool_endpoint_uuids: self.tool_endpoint_uuids().clone(),
            turn_context: self.turn_context().clone(),
        }
    }
}
