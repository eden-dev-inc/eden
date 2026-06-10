//! Azure OpenAI Chat Completions over the *classic* per-deployment URL.
//!
//! The upstream `llm::backends::azure_openai` only targets Azure's newer
//! unified `/openai/v1/` router. Tenants and api-versions that require the
//! classic shape — `{endpoint}/openai/deployments/{deployment}/chat/completions`
//! with `?api-version=…` — are handled here.
//!
//! Wire format mirrors OpenAI Chat Completions (so the request body shape is
//! shared with OpenAI), with these Azure-specific behaviours:
//!
//! * Auth uses an `api-key` header instead of `Authorization: Bearer …`.
//! * The deployment selects the model, so the request body **omits `model`**.
//! * Token-limit field name is configurable on the target: newer api-versions
//!   require `max_completion_tokens`; older ones accept `max_tokens`.

use crate::comm::{
    LlmStream, LlmStreamChunk, compacted_provider_messages, inject_turn_context_into_provider_messages, normalize_tool_schema_for_provider,
};
use crate::connection::{AzureMaxTokensField, AzureOpenAiClassicConfig};
use crate::credential::{ResolvedLlmConnection, ResolvedProviderConfig};
use crate::types::{
    LlmChatResponse, LlmCompletionTokensDetails, LlmFunctionCall, LlmInvocation, LlmMessage, LlmMessageKind, LlmMessageRole,
    LlmProviderMetadata, LlmToolCall, LlmUsage,
};
use error::EpError;
use futures::StreamExt;
use once_cell::sync::Lazy;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::warn;

static AZURE_HTTP_CLIENT: Lazy<Client> = Lazy::new(Client::new);

const AZURE_AUTH_HEADER: &str = "api-key";

/// Issue a non-streaming Azure OpenAI classic chat request.
pub async fn chat(
    resolved: &ResolvedLlmConnection,
    invocation: &LlmInvocation,
    effective_model: &str,
    base_url: &str,
) -> Result<LlmChatResponse, EpError> {
    let classic = azure_classic_config(resolved)?;
    let api_key = azure_api_key(resolved)?;
    let url = build_azure_url(base_url, &classic.deployment_id, &classic.api_version)?;
    let body = build_request_body(resolved, classic, invocation, false);

    let response = AZURE_HTTP_CLIENT
        .post(url.clone())
        .header(AZURE_AUTH_HEADER, api_key)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(EpError::request)?;

    let status = response.status();
    let raw_body = response.text().await.map_err(EpError::request)?;
    if !status.is_success() {
        return Err(EpError::request(format!(
            "Azure OpenAI API returned {}: {}",
            status,
            truncate_provider_error(&raw_body)
        )));
    }

    let parsed: AzureChatResponse =
        serde_json::from_str(&raw_body).map_err(|err| EpError::request(format!("failed to parse Azure OpenAI response: {err}")))?;

    let usage = parsed.usage.clone().map(Into::<LlmUsage>::into);
    let (text, tool_calls) = parsed.first_message();
    let kind = if tool_calls.is_empty() {
        LlmMessageKind::Text
    } else {
        LlmMessageKind::ToolUse { calls: tool_calls }
    };
    let message = LlmMessage { role: LlmMessageRole::Assistant, content: text, kind };
    let conversation = vec![message.clone()];

    Ok(LlmChatResponse {
        message,
        conversation,
        usage,
        thinking: None,
        provider: LlmProviderMetadata::new(resolved.provider.to_string(), effective_model.to_string(), Some(base_url.to_string())),
        conversation_id: invocation.conversation_id().cloned(),
    })
}

/// Issue a streaming Azure OpenAI classic chat request and return a chunk stream.
pub async fn chat_stream(
    resolved: &ResolvedLlmConnection,
    invocation: &LlmInvocation,
    effective_model: &str,
    base_url: &str,
) -> Result<LlmStream, EpError> {
    let _ = effective_model; // metadata only; deployment URL selects the model
    let classic = azure_classic_config(resolved)?;
    let api_key = azure_api_key(resolved)?;
    let url = build_azure_url(base_url, &classic.deployment_id, &classic.api_version)?;
    let body = build_request_body(resolved, classic, invocation, true);

    let response = AZURE_HTTP_CLIENT
        .post(url)
        .header(AZURE_AUTH_HEADER, api_key)
        .header("content-type", "application/json")
        .header("accept", "text/event-stream")
        .json(&body)
        .send()
        .await
        .map_err(EpError::request)?;

    let status = response.status();
    if !status.is_success() {
        let raw_body = response.text().await.map_err(EpError::request)?;
        return Err(EpError::request(format!(
            "Azure OpenAI API returned {}: {}",
            status,
            truncate_provider_error(&raw_body)
        )));
    }

    Ok(create_sse_stream(response))
}

fn azure_classic_config(resolved: &ResolvedLlmConnection) -> Result<&AzureOpenAiClassicConfig, EpError> {
    match &resolved.provider_config {
        ResolvedProviderConfig::AzureClassic(cfg) => Ok(cfg),
        ResolvedProviderConfig::None => {
            Err(EpError::connect("Azure OpenAI route entered without an AzureOpenAiClassicConfig — this is a bug"))
        }
    }
}

fn azure_api_key(resolved: &ResolvedLlmConnection) -> Result<&str, EpError> {
    resolved
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .ok_or_else(|| EpError::connect("missing Azure OpenAI API key"))
}

fn build_azure_url(base_url: &str, deployment_id: &str, api_version: &str) -> Result<Url, EpError> {
    let trimmed = base_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(EpError::connect("Azure OpenAI base URL is empty"));
    }
    let mut url =
        Url::parse(trimmed).map_err(|err| EpError::connect(format!("Azure OpenAI base URL `{trimmed}` is not a valid URL: {err}")))?;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| EpError::connect(format!("Azure OpenAI base URL `{trimmed}` cannot have a path appended")))?;
        // Preserve any existing trailing segment but strip an accidental empty
        // tail so push() doesn't insert a leading `//`.
        segments.pop_if_empty();
        segments.extend(["openai", "deployments", deployment_id, "chat", "completions"]);
    }
    url.query_pairs_mut().append_pair("api-version", api_version);
    Ok(url)
}

fn build_request_body(
    resolved: &ResolvedLlmConnection,
    classic: &AzureOpenAiClassicConfig,
    invocation: &LlmInvocation,
    stream: bool,
) -> Value {
    let mut body = json!({
        "messages": serialize_messages(invocation),
        "stream": stream,
    });

    let map = body.as_object_mut().expect("body is an object");

    if let Some(temp) = invocation.overrides.temperature.or(resolved.defaults.temperature) {
        map.insert("temperature".to_string(), json!(temp));
    }

    if let Some(max_tokens) = invocation.overrides.max_tokens.or(resolved.defaults.max_tokens) {
        let clamped = max_tokens.max(16);
        let field = match classic.max_tokens_field {
            AzureMaxTokensField::MaxTokens => "max_tokens",
            AzureMaxTokensField::MaxCompletionTokens | AzureMaxTokensField::Auto => "max_completion_tokens",
        };
        map.insert(field.to_string(), json!(clamped));
    }

    if let Some(top_p) = invocation.overrides.top_p.or(resolved.defaults.top_p) {
        map.insert("top_p".to_string(), json!(top_p));
    }

    if let Some(system) = invocation.effective_system_prompt() {
        // Insert system message at the top of the messages array.
        let messages = map.get_mut("messages").expect("messages set above").as_array_mut().expect("array");
        messages.insert(0, json!({ "role": "system", "content": system }));
    }

    let tools = invocation.tools.clone();
    if !tools.is_empty() {
        let tools_json: Vec<Value> = tools
            .into_iter()
            .map(|tool| {
                json!({
                    "type": tool.r#type,
                    "function": {
                        "name": tool.function.name,
                        "description": tool.function.description.unwrap_or_default(),
                        "parameters": normalize_tool_schema_for_provider(resolved.provider, &tool.function.parameters),
                    },
                })
            })
            .collect();
        map.insert("tools".to_string(), Value::Array(tools_json));
    }

    if let Some(choice) = &invocation.tool_choice {
        map.insert("tool_choice".to_string(), serialize_tool_choice(choice));
    }

    if let Some(enable) = invocation.parallel_tool_calls {
        map.insert("parallel_tool_calls".to_string(), json!(enable));
    }

    if let Some(schema) = &invocation.response_format {
        map.insert(
            "response_format".to_string(),
            json!({
                "type": "json_schema",
                "json_schema": {
                    "name": schema.name,
                    "description": schema.description,
                    "schema": schema.schema,
                    "strict": schema.strict,
                },
            }),
        );
    }

    if stream {
        // Ask Azure to emit usage in the final SSE chunk.
        map.insert("stream_options".to_string(), json!({"include_usage": true}));
    }

    body
}

fn serialize_tool_choice(choice: &crate::types::LlmToolChoice) -> Value {
    // Match OpenAI's wire format. The shapes are:
    //   "auto" | "none" | "required" | {"type": "function", "function": {"name": "..."}}
    use crate::types::LlmToolChoice;
    match choice {
        LlmToolChoice::Auto => json!("auto"),
        LlmToolChoice::None => json!("none"),
        // Eden's `Any` maps to OpenAI's `"required"` — the model must call a tool.
        LlmToolChoice::Any => json!("required"),
        LlmToolChoice::Tool { name } => json!({ "type": "function", "function": { "name": name } }),
    }
}

fn serialize_messages(invocation: &LlmInvocation) -> Vec<Value> {
    let mut messages = compacted_provider_messages(invocation);
    inject_turn_context_into_provider_messages(&mut messages, invocation.turn_context.as_deref());
    messages.iter().flat_map(serialize_message).collect()
}

fn serialize_message(message: &LlmMessage) -> Vec<Value> {
    let role = match message.role {
        LlmMessageRole::User => "user",
        LlmMessageRole::Assistant => "assistant",
    };

    match &message.kind {
        LlmMessageKind::Text => vec![json!({ "role": role, "content": message.content })],
        LlmMessageKind::ImageUrl { url } => vec![json!({
            "role": role,
            "content": [
                {"type": "text", "text": message.content},
                {"type": "image_url", "image_url": {"url": url}},
            ],
        })],
        LlmMessageKind::ToolUse { calls } => {
            let tool_calls: Vec<Value> = calls
                .iter()
                .map(|call| {
                    json!({
                        "id": call.id,
                        "type": call.call_type,
                        "function": {
                            "name": call.function.name,
                            "arguments": call.function.arguments,
                        },
                    })
                })
                .collect();
            let mut obj = serde_json::Map::new();
            obj.insert("role".to_string(), json!(role));
            if !message.content.is_empty() {
                obj.insert("content".to_string(), json!(message.content));
            }
            obj.insert("tool_calls".to_string(), Value::Array(tool_calls));
            vec![Value::Object(obj)]
        }
        LlmMessageKind::ToolResult { calls } => {
            // OpenAI/Azure expect one message per tool result with role="tool"
            // and tool_call_id set, regardless of the source role.
            calls
                .iter()
                .map(|call| {
                    let content = if call.function.arguments.is_empty() {
                        message.content.clone()
                    } else {
                        call.function.arguments.clone()
                    };
                    json!({
                        "role": "tool",
                        "tool_call_id": call.id,
                        "content": content,
                    })
                })
                .collect()
        }
    }
}

fn truncate_provider_error(body: &str) -> String {
    let mut truncated = body.chars().take(512).collect::<String>();
    if body.chars().count() > 512 {
        truncated.push_str("...");
    }
    truncated
}

// ---------------------------------------------------------------------------
// Response shapes
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Clone)]
struct AzureChatResponse {
    #[serde(default)]
    choices: Vec<AzureChatChoice>,
    #[serde(default)]
    usage: Option<AzureUsage>,
}

impl AzureChatResponse {
    fn first_message(self) -> (String, Vec<LlmToolCall>) {
        let Some(choice) = self.choices.into_iter().next() else {
            return (String::new(), Vec::new());
        };
        let text = choice.message.content.unwrap_or_default();
        let tool_calls = choice.message.tool_calls.unwrap_or_default().into_iter().map(Into::into).collect();
        (text, tool_calls)
    }
}

#[derive(Debug, Deserialize, Clone)]
struct AzureChatChoice {
    message: AzureChatMessage,
}

#[derive(Debug, Deserialize, Clone)]
struct AzureChatMessage {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<AzureToolCall>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct AzureToolCall {
    id: String,
    #[serde(rename = "type", default = "default_tool_type")]
    call_type: String,
    function: AzureToolFunction,
}

fn default_tool_type() -> String {
    "function".to_string()
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct AzureToolFunction {
    #[serde(default)]
    name: String,
    #[serde(default)]
    arguments: String,
}

impl From<AzureToolCall> for LlmToolCall {
    fn from(call: AzureToolCall) -> Self {
        LlmToolCall {
            id: call.id,
            call_type: call.call_type,
            function: LlmFunctionCall { name: call.function.name, arguments: call.function.arguments },
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
struct AzureUsage {
    #[serde(default)]
    prompt_tokens: u32,
    #[serde(default)]
    completion_tokens: u32,
    #[serde(default)]
    total_tokens: u32,
    #[serde(default)]
    completion_tokens_details: Option<AzureCompletionTokensDetails>,
}

#[derive(Debug, Deserialize, Clone)]
struct AzureCompletionTokensDetails {
    #[serde(default)]
    reasoning_tokens: Option<u32>,
}

impl From<AzureUsage> for LlmUsage {
    fn from(usage: AzureUsage) -> Self {
        LlmUsage {
            prompt_tokens: usage.prompt_tokens,
            completion_tokens: usage.completion_tokens,
            total_tokens: usage.total_tokens,
            completion_tokens_details: usage
                .completion_tokens_details
                .map(|d| LlmCompletionTokensDetails { reasoning_tokens: d.reasoning_tokens, audio_tokens: None }),
            prompt_tokens_details: None,
        }
    }
}

// ---------------------------------------------------------------------------
// SSE streaming
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct AzureStreamChunk {
    #[serde(default)]
    choices: Vec<AzureStreamChoice>,
    #[serde(default)]
    usage: Option<AzureUsage>,
    #[serde(default)]
    error: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct AzureStreamChoice {
    #[serde(default)]
    delta: AzureStreamDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct AzureStreamDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<AzureStreamToolCallDelta>>,
}

#[derive(Debug, Deserialize)]
struct AzureStreamToolCallDelta {
    #[serde(default)]
    index: u32,
    #[serde(default)]
    id: Option<String>,
    #[serde(default, rename = "type")]
    call_type: Option<String>,
    #[serde(default)]
    function: Option<AzureStreamToolFunctionDelta>,
}

#[derive(Debug, Deserialize)]
struct AzureStreamToolFunctionDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

fn create_sse_stream(response: reqwest::Response) -> LlmStream {
    // SSE events arrive as `data: <json>\n\n`. Buffer raw bytes from the body,
    // pull out one event per double-newline boundary, and parse each `data:`
    // payload into a chunk. `[DONE]` terminates the stream cleanly.
    let byte_stream = response.bytes_stream();
    let mut buffer: Vec<u8> = Vec::with_capacity(4096);
    let mut done = false;
    let mut tool_accumulator = AzureToolCallAccumulator::default();

    let mapped = byte_stream.flat_map(move |chunk_result| {
        let mut emitted: Vec<Result<LlmStreamChunk, EpError>> = Vec::new();

        match chunk_result {
            Ok(bytes) => {
                if done {
                    return futures::stream::iter(emitted);
                }
                buffer.extend_from_slice(&bytes);

                while let Some(event_end) = find_event_boundary(&buffer) {
                    let event_bytes = buffer.drain(..event_end).collect::<Vec<u8>>();
                    let event_str = match std::str::from_utf8(&event_bytes) {
                        Ok(s) => s,
                        Err(err) => {
                            emitted.push(Err(EpError::request(format!("Azure OpenAI SSE chunk is not valid UTF-8: {err}"))));
                            done = true;
                            return futures::stream::iter(emitted);
                        }
                    };

                    for line in event_str.lines() {
                        let Some(payload) = line.strip_prefix("data:").map(str::trim) else {
                            continue;
                        };
                        if payload.is_empty() {
                            continue;
                        }
                        if payload == "[DONE]" {
                            done = true;
                            break;
                        }
                        match parse_azure_sse_payload(payload, &mut tool_accumulator) {
                            Ok(Some(chunk)) => {
                                emitted.push(Ok(chunk));
                            }
                            Ok(None) => {}
                            Err(err) => {
                                emitted.push(Err(err));
                                done = true;
                                break;
                            }
                        }
                    }

                    if done {
                        break;
                    }
                }
            }
            Err(err) => {
                emitted.push(Err(EpError::request(err)));
                done = true;
            }
        }

        futures::stream::iter(emitted)
    });

    Box::pin(mapped)
}

fn parse_azure_sse_payload(payload: &str, tool_accumulator: &mut AzureToolCallAccumulator) -> Result<Option<LlmStreamChunk>, EpError> {
    let parsed = match serde_json::from_str::<AzureStreamChunk>(payload) {
        Ok(parsed) => parsed,
        Err(err) => {
            let preview = truncate_provider_error(payload);
            warn!(
                target = "llm.azure_openai.stream",
                error = %err,
                payload = %preview,
                "failed to parse Azure OpenAI stream chunk",
            );
            return Err(EpError::request(format!("failed to parse Azure OpenAI stream chunk: {err}: {preview}")));
        }
    };

    stream_chunk_from(parsed, tool_accumulator)
}

fn find_event_boundary(buffer: &[u8]) -> Option<usize> {
    // SSE events terminate with a blank line — `\n\n` or `\r\n\r\n`. Return
    // the index *after* the terminator so the drain consumes the trailing
    // newlines too.
    buffer
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .or_else(|| buffer.windows(2).position(|w| w == b"\n\n").map(|i| i + 2))
}

fn stream_chunk_from(parsed: AzureStreamChunk, tool_accumulator: &mut AzureToolCallAccumulator) -> Result<Option<LlmStreamChunk>, EpError> {
    if let Some(error) = parsed.error {
        let preview = truncate_provider_error(&error.to_string());
        warn!(
            target = "llm.azure_openai.stream",
            provider_error = %preview,
            "Azure OpenAI stream returned an error frame",
        );
        return Err(EpError::request(format!("Azure OpenAI stream error: {preview}")));
    }

    let mut delta_text: Option<String> = None;
    let mut emit_tool_calls = false;

    for choice in parsed.choices {
        if let Some(content) = choice.delta.content {
            match delta_text.as_mut() {
                Some(existing) => existing.push_str(&content),
                None => delta_text = Some(content),
            }
        }
        if let Some(calls) = choice.delta.tool_calls {
            for call in calls {
                tool_accumulator.push_delta(call)?;
            }
        }
        if choice.finish_reason.as_deref() == Some("tool_calls") {
            emit_tool_calls = true;
        }
    }

    let tool_calls = if emit_tool_calls {
        tool_accumulator.take_completed()
    } else {
        Vec::new()
    };
    let usage = parsed.usage.map(Into::<LlmUsage>::into);

    if delta_text.is_none() && tool_calls.is_empty() && usage.is_none() {
        return Ok(None);
    }

    Ok(Some(LlmStreamChunk { delta: delta_text, tool_calls, usage, thinking: None }))
}

#[derive(Debug, Default)]
struct AzureToolCallAccumulator {
    calls: Vec<Option<AzureToolCallState>>,
}

impl AzureToolCallAccumulator {
    fn push_delta(&mut self, delta: AzureStreamToolCallDelta) -> Result<(), EpError> {
        let index = usize::try_from(delta.index).map_err(|_| EpError::request("Azure OpenAI stream tool call index is invalid"))?;
        if self.calls.len() <= index {
            self.calls.resize_with(index + 1, || None);
        }

        let state = self.calls[index].get_or_insert_with(AzureToolCallState::default);
        if let Some(id) = delta.id.filter(|id| !id.is_empty())
            && state.id.is_empty()
        {
            state.id = id;
        }
        if let Some(call_type) = delta.call_type.filter(|call_type| !call_type.is_empty())
            && state.call_type.is_empty()
        {
            state.call_type = call_type;
        }
        if let Some(function) = delta.function {
            if let Some(name) = function.name.filter(|name| !name.is_empty())
                && state.name.is_empty()
            {
                state.name = name;
            }
            if let Some(arguments) = function.arguments {
                state.arguments.push_str(&arguments);
            }
        }

        Ok(())
    }

    fn take_completed(&mut self) -> Vec<LlmToolCall> {
        self.calls.drain(..).filter_map(|state| state.and_then(AzureToolCallState::into_tool_call)).collect()
    }
}

#[derive(Debug, Default)]
struct AzureToolCallState {
    id: String,
    call_type: String,
    name: String,
    arguments: String,
}

impl AzureToolCallState {
    fn into_tool_call(self) -> Option<LlmToolCall> {
        if self.id.is_empty() && self.name.is_empty() && self.arguments.is_empty() {
            return None;
        }

        let arguments = if self.arguments.is_empty() {
            "{}".to_string()
        } else {
            self.arguments
        };
        Some(LlmToolCall {
            id: self.id,
            call_type: if self.call_type.is_empty() {
                default_tool_type()
            } else {
                self.call_type
            },
            function: LlmFunctionCall { name: self.name, arguments },
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{AzureMaxTokensField, AzureOpenAiClassicConfig, LlmConnectionDefaults, LlmProvider};
    use crate::credential::ResolvedProviderConfig;
    use crate::types::{LlmFunctionToolDefinition, LlmRequestOverrides, LlmToolDefinition};

    fn sample_resolved(max_tokens_field: AzureMaxTokensField) -> ResolvedLlmConnection {
        ResolvedLlmConnection {
            provider: LlmProvider::AzureOpenAI,
            credential_id: None,
            api_key: Some("test-key".into()),
            credential_base_url: Some("https://my-resource.openai.azure.com".into()),
            defaults: LlmConnectionDefaults {
                model: "gpt-4o".into(),
                max_tokens: Some(1024),
                temperature: Some(0.3),
                ..Default::default()
            },
            provider_config: ResolvedProviderConfig::AzureClassic(AzureOpenAiClassicConfig {
                deployment_id: "my deploy".into(),
                api_version: "2024-08-01-preview".into(),
                max_tokens_field,
            }),
        }
    }

    fn classic(max_tokens_field: AzureMaxTokensField) -> AzureOpenAiClassicConfig {
        AzureOpenAiClassicConfig {
            deployment_id: "my deploy".into(),
            api_version: "2024-08-01-preview".into(),
            max_tokens_field,
        }
    }

    #[test]
    fn url_encodes_deployment_segment_and_api_version_query() {
        let url = build_azure_url("https://my-resource.openai.azure.com/", "my deploy", "2024-08-01-preview").expect("url");

        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("my-resource.openai.azure.com"));
        // Path segments are percent-encoded — spaces become `%20`.
        assert_eq!(url.path(), "/openai/deployments/my%20deploy/chat/completions");
        assert_eq!(url.query(), Some("api-version=2024-08-01-preview"));
    }

    #[test]
    fn body_omits_model_field() {
        let resolved = sample_resolved(AzureMaxTokensField::Auto);
        let cfg = classic(AzureMaxTokensField::Auto);
        let invocation = LlmInvocation {
            conversation: vec![LlmMessage {
                role: LlmMessageRole::User,
                content: "hi".into(),
                kind: LlmMessageKind::Text,
            }],
            ..Default::default()
        };
        let body = build_request_body(&resolved, &cfg, &invocation, false);
        let obj = body.as_object().expect("object");
        assert!(
            !obj.contains_key("model"),
            "Azure classic request body must omit `model` — deployment in URL selects it"
        );
        assert_eq!(obj["messages"][0]["role"], "user");
        assert_eq!(obj["messages"][0]["content"], "hi");
        assert_eq!(obj["stream"], false);
    }

    #[test]
    fn auto_max_tokens_field_serializes_as_max_completion_tokens() {
        let resolved = sample_resolved(AzureMaxTokensField::Auto);
        let cfg = classic(AzureMaxTokensField::Auto);
        let invocation = LlmInvocation {
            overrides: LlmRequestOverrides { max_tokens: Some(500), ..Default::default() },
            ..Default::default()
        };
        let body = build_request_body(&resolved, &cfg, &invocation, false);
        let obj = body.as_object().expect("object");
        assert!(obj.contains_key("max_completion_tokens"));
        assert!(!obj.contains_key("max_tokens"));
        assert_eq!(obj["max_completion_tokens"], 500);
    }

    #[test]
    fn explicit_max_tokens_field_uses_legacy_name() {
        let resolved = sample_resolved(AzureMaxTokensField::MaxTokens);
        let cfg = classic(AzureMaxTokensField::MaxTokens);
        let invocation = LlmInvocation {
            overrides: LlmRequestOverrides { max_tokens: Some(800), ..Default::default() },
            ..Default::default()
        };
        let body = build_request_body(&resolved, &cfg, &invocation, false);
        let obj = body.as_object().expect("object");
        assert!(obj.contains_key("max_tokens"));
        assert!(!obj.contains_key("max_completion_tokens"));
        assert_eq!(obj["max_tokens"], 800);
    }

    #[test]
    fn min_max_tokens_clamps_to_sixteen() {
        let resolved = sample_resolved(AzureMaxTokensField::MaxTokens);
        let cfg = classic(AzureMaxTokensField::MaxTokens);
        let invocation = LlmInvocation {
            overrides: LlmRequestOverrides { max_tokens: Some(1), ..Default::default() },
            ..Default::default()
        };
        let body = build_request_body(&resolved, &cfg, &invocation, false);
        assert_eq!(body["max_tokens"], 16);
    }

    #[test]
    fn system_prompt_inserts_as_first_message() {
        let resolved = sample_resolved(AzureMaxTokensField::Auto);
        let cfg = classic(AzureMaxTokensField::Auto);
        let invocation = LlmInvocation {
            conversation: vec![LlmMessage {
                role: LlmMessageRole::User,
                content: "hi".into(),
                kind: LlmMessageKind::Text,
            }],
            system_prompt: Some("you are a helpful assistant".into()),
            ..Default::default()
        };
        let body = build_request_body(&resolved, &cfg, &invocation, false);
        let messages = body["messages"].as_array().expect("messages array");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["role"], "system");
        assert_eq!(messages[0]["content"], "you are a helpful assistant");
    }

    #[test]
    fn messages_drop_orphan_tool_results_and_inject_turn_context() {
        let resolved = sample_resolved(AzureMaxTokensField::Auto);
        let cfg = classic(AzureMaxTokensField::Auto);
        let invocation = LlmInvocation {
            conversation: vec![
                LlmMessage {
                    role: LlmMessageRole::User,
                    content: "{\"rows\": 0}".into(),
                    kind: LlmMessageKind::ToolResult {
                        calls: vec![LlmToolCall {
                            id: "orphan".into(),
                            call_type: "function".into(),
                            function: LlmFunctionCall { name: "query".into(), arguments: "{\"rows\": 0}".into() },
                        }],
                    },
                },
                LlmMessage {
                    role: LlmMessageRole::User,
                    content: "What changed?".into(),
                    kind: LlmMessageKind::Text,
                },
            ],
            turn_context: Some("Route hints go here".into()),
            ..Default::default()
        };
        let body = build_request_body(&resolved, &cfg, &invocation, false);
        let messages = body["messages"].as_array().expect("messages array");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["role"], "user");
        assert!(
            messages[0]["content"]
                .as_str()
                .expect("content string")
                .starts_with("<eden-context>\nRoute hints go here\n</eden-context>\n\nWhat changed?")
        );
    }

    #[test]
    fn tools_serialize_as_openai_function_definitions() {
        let resolved = sample_resolved(AzureMaxTokensField::Auto);
        let cfg = classic(AzureMaxTokensField::Auto);
        let invocation = LlmInvocation {
            tools: vec![LlmToolDefinition {
                r#type: "function".into(),
                function: LlmFunctionToolDefinition {
                    name: "query".into(),
                    description: Some("Run a query".into()),
                    parameters: json!({
                        "type": "object",
                        "properties": {
                            "sql": {"type": "string"},
                            "values": {"type": "array", "items": true}
                        }
                    }),
                    example_usage: None,
                },
            }],
            ..Default::default()
        };
        let body = build_request_body(&resolved, &cfg, &invocation, false);
        let tools = body["tools"].as_array().expect("tools array");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "function");
        assert_eq!(tools[0]["function"]["name"], "query");
        assert_eq!(tools[0]["function"]["description"], "Run a query");
        assert!(tools[0]["function"]["parameters"]["properties"]["sql"].is_object());
        assert_eq!(tools[0]["function"]["parameters"]["properties"]["values"]["items"], json!({}));
    }

    #[test]
    fn streaming_body_requests_usage_in_final_chunk() {
        let resolved = sample_resolved(AzureMaxTokensField::Auto);
        let cfg = classic(AzureMaxTokensField::Auto);
        let invocation = LlmInvocation::default();
        let body = build_request_body(&resolved, &cfg, &invocation, true);
        assert_eq!(body["stream"], true);
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    #[test]
    fn tool_result_messages_become_role_tool_with_call_id() {
        let cfg = classic(AzureMaxTokensField::Auto);
        let resolved = sample_resolved(AzureMaxTokensField::Auto);
        let invocation = LlmInvocation {
            conversation: vec![
                LlmMessage {
                    role: LlmMessageRole::Assistant,
                    content: String::new(),
                    kind: LlmMessageKind::ToolUse {
                        calls: vec![LlmToolCall {
                            id: "call_1".into(),
                            call_type: "function".into(),
                            function: LlmFunctionCall { name: "query".into(), arguments: "{}".into() },
                        }],
                    },
                },
                LlmMessage {
                    role: LlmMessageRole::User,
                    content: "{\"rows\": 0}".into(),
                    kind: LlmMessageKind::ToolResult {
                        calls: vec![LlmToolCall {
                            id: "call_1".into(),
                            call_type: "function".into(),
                            function: LlmFunctionCall { name: "query".into(), arguments: "{\"rows\": 0}".into() },
                        }],
                    },
                },
            ],
            ..Default::default()
        };
        let body = build_request_body(&resolved, &cfg, &invocation, false);
        let messages = body["messages"].as_array().expect("messages array");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["role"], "assistant");
        assert_eq!(messages[0]["tool_calls"][0]["id"], "call_1");
        assert_eq!(messages[1]["role"], "tool");
        assert_eq!(messages[1]["tool_call_id"], "call_1");
        let content = messages[1]["content"].as_str().expect("tool content should be a string");
        let content_json: Value = serde_json::from_str(content).expect("tool content should be json");
        assert_eq!(content_json, json!({"type":"json","value":{"rows":0},"truncated":false}));
    }

    #[test]
    fn streaming_tool_calls_emit_once_after_finish_reason() {
        let mut accumulator = AzureToolCallAccumulator::default();
        let payloads = [
            json!({
                "choices": [{
                    "delta": {
                        "tool_calls": [{
                            "index": 0,
                            "id": "call_query",
                            "type": "function",
                            "function": {
                                "name": "query",
                                "arguments": "{\"sql\":"
                            }
                        }]
                    }
                }]
            })
            .to_string(),
            json!({
                "choices": [{
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "function": {
                                    "arguments": "\"select 1\""
                                }
                            },
                            {
                                "index": 1,
                                "id": "call_read",
                                "type": "function",
                                "function": {
                                    "name": "read_file",
                                    "arguments": "{\"path\":"
                                }
                            }
                        ]
                    }
                }]
            })
            .to_string(),
            json!({
                "choices": [{
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "function": {
                                    "arguments": "}"
                                }
                            },
                            {
                                "index": 1,
                                "function": {
                                    "arguments": "\"/tmp/a\"}"
                                }
                            }
                        ]
                    }
                }]
            })
            .to_string(),
        ];

        for payload in payloads {
            let chunk = parse_azure_sse_payload(&payload, &mut accumulator).expect("parse chunk");
            assert!(chunk.is_none(), "partial tool-call deltas must not emit tool calls");
        }

        let finish_payload = json!({
            "choices": [{
                "delta": {},
                "finish_reason": "tool_calls"
            }]
        })
        .to_string();
        let chunk = parse_azure_sse_payload(&finish_payload, &mut accumulator)
            .expect("parse finish chunk")
            .expect("finish chunk should emit tool calls");

        assert_eq!(chunk.tool_calls.len(), 2);
        assert_eq!(chunk.tool_calls[0].id, "call_query");
        assert_eq!(chunk.tool_calls[0].function.name, "query");
        assert_eq!(chunk.tool_calls[0].function.arguments, "{\"sql\":\"select 1\"}");
        assert_eq!(chunk.tool_calls[1].id, "call_read");
        assert_eq!(chunk.tool_calls[1].function.name, "read_file");
        assert_eq!(chunk.tool_calls[1].function.arguments, "{\"path\":\"/tmp/a\"}");
    }

    #[test]
    fn stream_error_frame_surfaces_truncated_provider_error() {
        let mut accumulator = AzureToolCallAccumulator::default();
        let long_message = "x".repeat(700);
        let payload = json!({
            "error": {
                "message": long_message,
                "type": "server_error"
            }
        })
        .to_string();

        let err = parse_azure_sse_payload(&payload, &mut accumulator).expect_err("error frame should fail");
        let message = err.to_string();

        assert!(message.contains("Azure OpenAI stream error"));
        assert!(message.contains("..."));
        assert!(!message.contains(&"x".repeat(600)));
    }

    #[test]
    fn malformed_stream_payload_error_is_truncated() {
        let mut accumulator = AzureToolCallAccumulator::default();
        let tail = "x".repeat(700);
        let payload = format!("{{\"choices\": [{tail}");

        let err = parse_azure_sse_payload(&payload, &mut accumulator).expect_err("malformed chunk should fail");
        let message = err.to_string();

        assert!(message.contains("failed to parse Azure OpenAI stream chunk"));
        assert!(message.contains("..."));
        assert!(!message.contains(&"x".repeat(600)));
    }

    #[test]
    fn error_when_base_url_empty() {
        let err = build_azure_url("", "deploy", "v").expect_err("empty base URL should fail");
        assert!(format!("{err}").contains("empty"));
    }

    #[test]
    fn missing_api_key_errors() {
        let mut resolved = sample_resolved(AzureMaxTokensField::Auto);
        resolved.api_key = None;
        let err = azure_api_key(&resolved).expect_err("missing api key");
        assert!(format!("{err}").contains("missing Azure OpenAI API key"));
    }
}
