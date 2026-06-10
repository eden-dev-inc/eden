use crate::LlmParam;
use crate::azure_openai_classic;
use crate::connection::LlmProvider;
use crate::credential::ResolvedLlmConnection;
use crate::tool_result_projection::{
    DEFAULT_TOOL_RESULT_MAX_BYTES, DEFAULT_TOOL_RESULT_MAX_CELLS, DEFAULT_TOOL_RESULT_MAX_ROWS, project_tool_result,
};
use crate::types::{
    CacheHint, LlmChatResponse, LlmFunctionCall, LlmInvocation, LlmMessage, LlmMessageKind, LlmMessageRole, LlmProviderMetadata,
    LlmRequestOverrides, LlmToolCall, LlmUsage,
};
use bytes::Bytes;
use error::EpError;
use futures::{Stream, StreamExt, stream};
use llm::builder::{FunctionBuilder, LLMBackend, LLMBuilder};
use llm::chat::StreamChunk;
use llm::chat::{ChatMessage, Tool, ToolChoice as LlmToolChoiceInner};
use once_cell::sync::Lazy;
use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use tiktoken_rs::CoreBPE;
use tracing::{info, warn};

// Lazily initialised cl100k_base tokenizer (GPT-3.5/4 BPE encoding).
//
// Used as a fallback when a provider returns no token usage in its response.
// cl100k_base is a reasonable approximation for all supported providers
// (OpenAI, Anthropic, and Ollama all use similar BPE vocabularies).
//
// Performance notes:
// - Initialisation: deserialises ~100K BPE vocabulary entries (~tens of ms),
//   paid once on the first fallback call via `Lazy`.
// - `encode_ordinary()`: O(n) in text length, typically microseconds for
//   normal chat messages. It allocates a Vec<u32> of token IDs; tiktoken-rs
//   has no count-only API so the allocation is unavoidable.
// - This path is not on the hot path: it only fires when a provider omits
//   usage data (Ollama on cache hits, unexpected gaps on other providers).
//   Even at high context lengths the cost is negligible relative to LLM
//   round-trip latency.
//
// Initialisation failure is logged once and the char-based estimate (~4
// chars/token) is used as a last resort.
static CL100K: Lazy<Option<CoreBPE>> = Lazy::new(|| match tiktoken_rs::cl100k_base() {
    Ok(bpe) => Some(bpe),
    Err(e) => {
        tracing::warn!(error = %e, "failed to initialise cl100k_base tokenizer; token counts will fall back to char estimate");
        None
    }
});
static OLLAMA_HTTP_CLIENT: Lazy<Client> = Lazy::new(Client::new);

fn count_tokens(text: &str) -> u32 {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return 0;
    }
    match CL100K.as_ref() {
        Some(bpe) => bpe.encode_ordinary(trimmed).len() as u32,
        // Last-resort fallback: ~4 chars/token, rounded up.
        None => (trimmed.chars().count() as u32).div_ceil(4),
    }
}

fn count_prompt_tokens(messages: &[ChatMessage]) -> u32 {
    messages.iter().map(|m| count_tokens(&m.content)).sum()
}

fn usage_or_estimate(usage: Option<LlmUsage>, provider: LlmProvider, messages: &[ChatMessage], response_text: &str) -> Option<LlmUsage> {
    if usage.is_some() {
        return usage;
    }

    // Ollama's native backend never returns token usage — estimation is the expected path.
    // All other providers should return real usage; log a warning so unexpected gaps are visible.
    if provider == LlmProvider::Ollama {
        tracing::debug!("Ollama native backend returned no token usage; using tiktoken estimate");
    } else {
        tracing::warn!(
            provider = ?provider,
            "LLM provider returned no token usage; falling back to tiktoken estimate for rate limiting"
        );
    }

    let prompt_tokens = count_prompt_tokens(messages);
    let completion_tokens = count_tokens(response_text);

    if prompt_tokens == 0 && completion_tokens == 0 {
        return None;
    }

    Some(LlmUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens: prompt_tokens.saturating_add(completion_tokens),
        completion_tokens_details: None,
        prompt_tokens_details: None,
    })
}

/// Streaming delta chunk returned during an in-flight LLM response.
#[derive(Debug, Clone)]
pub struct LlmStreamChunk {
    /// Incremental text, if present.
    pub delta: Option<String>,
    /// Usage metadata when the provider surfaces it mid-stream (often final chunk only).
    pub usage: Option<LlmUsage>,
    /// Tool calls requested by the model (streaming signal).
    pub tool_calls: Vec<LlmToolCall>,
    /// Thinking/reasoning delta from extended thinking models.
    /// Emitted as the model reasons before producing its response.
    pub thinking: Option<String>,
}

/// Type alias for a pinned streaming iterator of LLM chunks.
pub type LlmStream = Pin<Box<dyn Stream<Item = Result<LlmStreamChunk, EpError>> + Send + 'static>>;

/// Raw response returned by the OpenAI-compatible pass-through fast path.
pub struct LlmRawChatResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: Bytes,
    pub provider: LlmProviderMetadata,
}

/// Raw byte stream returned by the OpenAI-compatible pass-through fast path.
pub type LlmRawByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, EpError>> + Send + 'static>>;

/// Raw streaming response returned by the OpenAI-compatible pass-through fast path.
pub struct LlmRawStreamResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub stream: LlmRawByteStream,
    pub provider: LlmProviderMetadata,
}

#[derive(Serialize)]
struct OpenAiCompatibleChatStreamRequest<'a> {
    model: &'a str,
    messages: Vec<OpenAiCompatibleMessage>,
    stream: bool,
    stream_options: OpenAiCompatibleStreamOptions,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_k: Option<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<Tool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<LlmToolChoiceInner>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parallel_tool_calls: Option<bool>,
}

#[derive(Serialize)]
struct OpenAiCompatibleStreamOptions {
    include_usage: bool,
}

#[derive(Serialize)]
struct OpenAiCompatibleMessage {
    role: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tool_calls: Vec<OpenAiCompatibleToolCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

#[derive(Serialize)]
struct OpenAiCompatibleToolCall {
    id: String,
    #[serde(rename = "type")]
    call_type: String,
    function: OpenAiCompatibleFunctionCall,
}

#[derive(Serialize)]
struct OpenAiCompatibleFunctionCall {
    name: String,
    arguments: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiCompatibleStreamFrame {
    #[serde(default)]
    choices: Vec<OpenAiCompatibleStreamChoice>,
    #[serde(default)]
    usage: Option<LlmUsage>,
}

#[derive(Debug, Deserialize)]
struct OpenAiCompatibleStreamChoice {
    #[serde(default)]
    delta: OpenAiCompatibleStreamDelta,
}

#[derive(Debug, Default, Deserialize)]
struct OpenAiCompatibleStreamDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<OpenAiCompatibleStreamToolCall>,
}

#[derive(Debug, Deserialize)]
struct OpenAiCompatibleStreamToolCall {
    #[serde(default)]
    id: Option<String>,
    #[serde(rename = "type", default)]
    call_type: Option<String>,
    #[serde(default)]
    function: Option<OpenAiCompatibleStreamFunctionCall>,
}

#[derive(Debug, Deserialize)]
struct OpenAiCompatibleStreamFunctionCall {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

static ANTHROPIC_HTTP_CLIENT: Lazy<Client> = Lazy::new(Client::new);
static RAW_OPENAI_HTTP_CLIENT: Lazy<Client> = Lazy::new(Client::new);

struct RawOpenAiPassthroughPlan {
    url: String,
    api_key: Option<String>,
    provider: LlmProviderMetadata,
}

fn openai_chat_completions_url(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    if trimmed.ends_with("/chat/completions") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/chat/completions")
    }
}

fn response_content_type(response: &reqwest::Response) -> Option<String> {
    response.headers().get(CONTENT_TYPE).and_then(|value| value.to_str().ok()).map(str::to_string)
}

async fn openai_compatible_chat_stream(
    resolved: &ResolvedLlmConnection,
    invocation: &LlmInvocation,
    effective_model: &str,
    base_url: &str,
) -> Result<LlmStream, EpError> {
    let api_key = resolved
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|api_key| !api_key.is_empty())
        .ok_or_else(|| EpError::connect(format!("Missing {} API key", resolved.provider)))?;

    let body = OpenAiCompatibleChatStreamRequest {
        model: effective_model,
        messages: openai_compatible_messages(invocation),
        stream: true,
        stream_options: OpenAiCompatibleStreamOptions { include_usage: true },
        temperature: invocation.overrides.temperature.or(resolved.defaults.temperature),
        max_tokens: invocation.overrides.max_tokens.or(resolved.defaults.max_tokens).map(|tokens| tokens.max(16)),
        top_p: invocation.overrides.top_p.or(resolved.defaults.top_p),
        top_k: invocation.overrides.top_k.or(resolved.defaults.top_k),
        tools: normalized_tools_for_provider(resolved.provider, invocation.tools()),
        tool_choice: invocation.tool_choice(),
        parallel_tool_calls: invocation.parallel_tool_calls,
    };

    let response = RAW_OPENAI_HTTP_CLIENT
        .post(openai_chat_completions_url(base_url))
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "text/event-stream")
        .header(AUTHORIZATION, format!("Bearer {api_key}"))
        .json(&body)
        .send()
        .await
        .map_err(EpError::request)?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.map_err(EpError::request)?;
        return Err(EpError::request(format!("{} API returned error status: {status}: {body}", resolved.provider)));
    }

    Ok(create_openai_compatible_stream(response))
}

fn openai_compatible_messages(invocation: &LlmInvocation) -> Vec<OpenAiCompatibleMessage> {
    let mut messages = Vec::new();
    if let Some(system) = invocation.effective_system_prompt().filter(|system| !system.trim().is_empty()) {
        messages.push(OpenAiCompatibleMessage {
            role: "system",
            content: Some(system),
            tool_calls: Vec::new(),
            tool_call_id: None,
        });
    }

    for message in provider_messages(invocation) {
        match message.kind {
            LlmMessageKind::Text | LlmMessageKind::ImageUrl { .. } => {
                messages.push(OpenAiCompatibleMessage {
                    role: match message.role {
                        LlmMessageRole::User => "user",
                        LlmMessageRole::Assistant => "assistant",
                    },
                    content: (!message.content.is_empty()).then_some(message.content),
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                });
            }
            LlmMessageKind::ToolUse { calls } => {
                messages.push(OpenAiCompatibleMessage {
                    role: "assistant",
                    content: (!message.content.is_empty()).then_some(message.content),
                    tool_calls: calls.into_iter().map(openai_compatible_tool_call).collect(),
                    tool_call_id: None,
                });
            }
            LlmMessageKind::ToolResult { calls } => {
                for call in calls {
                    let content = if message.content.is_empty() {
                        call.function.arguments.clone()
                    } else {
                        message.content.clone()
                    };
                    messages.push(OpenAiCompatibleMessage {
                        role: "tool",
                        content: Some(content),
                        tool_calls: Vec::new(),
                        tool_call_id: Some(call.id),
                    });
                }
            }
        }
    }

    messages
}

fn openai_compatible_tool_call(call: LlmToolCall) -> OpenAiCompatibleToolCall {
    OpenAiCompatibleToolCall {
        id: call.id,
        call_type: call.call_type,
        function: OpenAiCompatibleFunctionCall { name: call.function.name, arguments: call.function.arguments },
    }
}

fn create_openai_compatible_stream(response: reqwest::Response) -> LlmStream {
    let stream = response
        .bytes_stream()
        .scan(Vec::<u8>::new(), |buffer, chunk| {
            let events = match chunk {
                Ok(bytes) => {
                    buffer.extend_from_slice(&bytes);
                    drain_openai_sse_events(buffer)
                }
                Err(err) => vec![Err(EpError::request(err))],
            };
            futures::future::ready(Some(events))
        })
        .flat_map(stream::iter);
    Box::pin(stream)
}

fn drain_openai_sse_events(buffer: &mut Vec<u8>) -> Vec<Result<LlmStreamChunk, EpError>> {
    let mut events = Vec::new();
    while let Some((end, delimiter_len)) = find_sse_event_end(buffer) {
        let mut event = buffer.drain(..end + delimiter_len).collect::<Vec<_>>();
        while matches!(event.last(), Some(b'\n' | b'\r')) {
            event.pop();
        }
        if let Some(parsed) = parse_openai_sse_event(&event) {
            events.push(parsed);
        }
    }
    events
}

fn find_sse_event_end(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n").map(|index| (index, 2));
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n").map(|index| (index, 4));
    match (lf, crlf) {
        (Some(left), Some(right)) => Some(if left.0 <= right.0 { left } else { right }),
        (Some(found), None) | (None, Some(found)) => Some(found),
        (None, None) => None,
    }
}

fn parse_openai_sse_event(event: &[u8]) -> Option<Result<LlmStreamChunk, EpError>> {
    let event = match std::str::from_utf8(event) {
        Ok(event) => event,
        Err(err) => return Some(Err(EpError::serde(format!("invalid OpenAI SSE frame: {err}")))),
    };

    let mut payload = String::new();
    for line in event.lines() {
        let line = line.trim_end_matches('\r');
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        if !payload.is_empty() {
            payload.push('\n');
        }
        payload.push_str(data.trim_start());
    }

    let payload = payload.trim();
    if payload.is_empty() || payload == "[DONE]" {
        return None;
    }

    let frame = match serde_json::from_str::<OpenAiCompatibleStreamFrame>(payload) {
        Ok(frame) => frame,
        Err(err) => return Some(Err(EpError::serde(format!("invalid OpenAI SSE JSON frame: {err}")))),
    };

    let mut delta = None;
    let mut tool_calls = Vec::new();
    for choice in frame.choices {
        if delta.is_none() {
            delta = choice.delta.content;
        }
        tool_calls.extend(choice.delta.tool_calls.into_iter().map(openai_compatible_stream_tool_call));
    }

    if delta.is_none() && tool_calls.is_empty() && frame.usage.is_none() {
        return None;
    }

    Some(Ok(LlmStreamChunk { delta, usage: frame.usage, tool_calls, thinking: None }))
}

fn openai_compatible_stream_tool_call(call: OpenAiCompatibleStreamToolCall) -> LlmToolCall {
    let function = call.function.unwrap_or(OpenAiCompatibleStreamFunctionCall { name: None, arguments: None });
    LlmToolCall {
        id: call.id.unwrap_or_default(),
        call_type: call.call_type.unwrap_or_else(|| "function".to_string()),
        function: LlmFunctionCall {
            name: function.name.unwrap_or_default(),
            arguments: function.arguments.unwrap_or_default(),
        },
    }
}

fn supports_openai_compatible_stream(invocation: &LlmInvocation) -> bool {
    invocation.structured_output().is_none()
        && invocation.overrides.thinking_budget.is_none()
        && invocation.conversation.iter().all(|message| !matches!(message.kind, LlmMessageKind::ImageUrl { .. }))
}

#[derive(Debug, Clone)]
pub struct LlmClient {
    connection: Arc<RwLock<ResolvedLlmConnection>>,
    max_tool_passes: usize,
}

impl LlmClient {
    pub fn new(connection: Arc<RwLock<ResolvedLlmConnection>>, max_tool_passes: usize) -> Result<Self, EpError> {
        Ok(Self { connection, max_tool_passes: max_tool_passes.max(1) })
    }

    pub fn max_tool_passes(&self) -> usize {
        self.max_tool_passes
    }

    /// Clone the fully-resolved connection (provider + api_key + base_url).
    ///
    /// Used by the LLM metadata collector to authenticate provider `/models`
    /// calls. Returns an error only if the internal lock is poisoned.
    pub fn resolved_connection(&self) -> Result<ResolvedLlmConnection, EpError> {
        self.connection
            .read()
            .map(|guard| guard.clone())
            .map_err(|_| EpError::request("failed to acquire LLM connection state"))
    }

    pub fn provider_metadata(&self, model_override: Option<String>) -> Result<LlmProviderMetadata, EpError> {
        let resolved = self
            .connection
            .read()
            .map(|guard| guard.clone())
            .map_err(|_| EpError::request("failed to acquire LLM connection state"))?;

        let effective_model = resolved.effective_model(model_override);
        let base_url = resolved.base_url()?;

        Ok(LlmProviderMetadata::new(resolved.provider.to_string(), effective_model, Some(base_url)))
    }

    pub async fn chat_openai_passthrough(&self, request_body: Bytes, request_model: &str) -> Result<Option<LlmRawChatResponse>, EpError> {
        let Some(plan) = self.openai_passthrough_plan(request_model)? else {
            return Ok(None);
        };

        let mut request = RAW_OPENAI_HTTP_CLIENT.post(plan.url).header(CONTENT_TYPE, "application/json").body(request_body);
        if let Some(api_key) = plan.api_key.as_deref() {
            request = request.header(AUTHORIZATION, format!("Bearer {api_key}"));
        }

        let response = request.send().await.map_err(EpError::request)?;
        let status = response.status().as_u16();
        let content_type = response_content_type(&response);
        let body = response.bytes().await.map_err(EpError::request)?;

        Ok(Some(LlmRawChatResponse { status, content_type, body, provider: plan.provider }))
    }

    pub async fn chat_stream_openai_passthrough(
        &self,
        request_body: Bytes,
        request_model: &str,
    ) -> Result<Option<LlmRawStreamResponse>, EpError> {
        let Some(plan) = self.openai_passthrough_plan(request_model)? else {
            return Ok(None);
        };

        let mut request = RAW_OPENAI_HTTP_CLIENT
            .post(plan.url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "text/event-stream")
            .body(request_body);
        if let Some(api_key) = plan.api_key.as_deref() {
            request = request.header(AUTHORIZATION, format!("Bearer {api_key}"));
        }

        let response = request.send().await.map_err(EpError::request)?;
        let status = response.status().as_u16();
        let content_type = response_content_type(&response);
        let stream = response.bytes_stream().map(|chunk| chunk.map_err(EpError::request));

        Ok(Some(LlmRawStreamResponse {
            status,
            content_type,
            stream: Box::pin(stream),
            provider: plan.provider,
        }))
    }

    fn openai_passthrough_plan(&self, request_model: &str) -> Result<Option<RawOpenAiPassthroughPlan>, EpError> {
        let request_model = request_model.trim();
        if request_model.is_empty() {
            return Ok(None);
        }

        let resolved = self
            .connection
            .read()
            .map(|guard| guard.clone())
            .map_err(|_| EpError::request("failed to acquire LLM connection state"))?;

        if !matches!(resolved.provider, LlmProvider::OpenAI | LlmProvider::OpenRouter) {
            return Ok(None);
        }

        if resolved.defaults.temperature.is_some()
            || resolved.defaults.max_tokens.is_some()
            || resolved.defaults.top_p.is_some()
            || resolved.defaults.top_k.is_some()
        {
            return Ok(None);
        }

        let base_url = resolved.base_url()?;
        let url = openai_chat_completions_url(&base_url);
        let provider = LlmProviderMetadata::new(resolved.provider.to_string(), request_model.to_string(), Some(base_url));
        let api_key = resolved.api_key.as_deref().map(str::trim).filter(|key| !key.is_empty()).map(str::to_string);

        Ok(Some(RawOpenAiPassthroughPlan { url, api_key, provider }))
    }

    pub async fn chat(&self, invocation: &LlmInvocation) -> Result<LlmChatResponse, EpError> {
        let resolved = self
            .connection
            .read()
            .map(|guard| guard.clone())
            .map_err(|_| EpError::request("failed to acquire LLM connection state"))?;

        let effective_model = resolved.effective_model(invocation.overrides.model.clone());
        let base_url = resolved.base_url()?;

        let backend = match chat_route(&resolved, &effective_model)? {
            ChatRoute::OllamaNative => return ollama_chat(&resolved, invocation, &effective_model, &base_url).await,
            ChatRoute::AnthropicMessages => return anthropic_chat(&resolved, invocation, &effective_model, &base_url).await,
            ChatRoute::AzureOpenAiClassic => return azure_openai_classic::chat(&resolved, invocation, &effective_model, &base_url).await,
            ChatRoute::Upstream(backend) => backend,
        };

        let messages = provider_chat_messages(invocation);
        let tools = normalized_tools_for_provider(resolved.provider, invocation.tools());
        let tools_ref = if tools.is_empty() { None } else { Some(tools.as_slice()) };

        let builder = build_upstream_chat_builder(backend, &resolved, invocation, &effective_model, &base_url, &tools);

        let provider = builder.build().map_err(EpError::connect)?;

        let response = provider.chat_with_tools(&messages, tools_ref).await.map_err(EpError::request)?;

        let text = response.text().unwrap_or_default();
        let tool_calls: Vec<LlmToolCall> = response.tool_calls().unwrap_or_default().into_iter().map(Into::into).collect();
        let usage = usage_or_estimate(response.usage().map(Into::<LlmUsage>::into), resolved.provider, &messages, &text);
        let thinking = response.thinking();

        let message_kind = if tool_calls.is_empty() {
            LlmMessageKind::Text
        } else {
            LlmMessageKind::ToolUse { calls: tool_calls }
        };

        let message = LlmMessage {
            role: LlmMessageRole::Assistant,
            content: text,
            kind: message_kind,
        };
        let conversation_delta = vec![message.clone()];

        Ok(LlmChatResponse {
            message,
            conversation: conversation_delta,
            usage,
            thinking,
            provider: LlmProviderMetadata::new(resolved.provider.to_string(), effective_model, Some(base_url)),
            conversation_id: invocation.conversation_id().cloned(),
        })
    }

    pub async fn chat_stream(&self, invocation: &LlmInvocation) -> Result<LlmStream, EpError> {
        let resolved = self
            .connection
            .read()
            .map(|guard| guard.clone())
            .map_err(|_| EpError::request("failed to acquire LLM connection state"))?;

        let effective_model = resolved.effective_model(invocation.overrides.model.clone());
        let base_url = resolved.base_url()?;

        let backend = match chat_route(&resolved, &effective_model)? {
            ChatRoute::AzureOpenAiClassic => {
                return azure_openai_classic::chat_stream(&resolved, invocation, &effective_model, &base_url).await;
            }
            ChatRoute::OllamaNative => return ollama_chat_stream(&resolved, invocation, &effective_model, &base_url).await,
            ChatRoute::AnthropicMessages => return anthropic_chat_stream(&resolved, invocation, &effective_model, &base_url).await,
            ChatRoute::Upstream(backend) => backend,
        };

        if matches!(resolved.provider, LlmProvider::OpenAI | LlmProvider::OpenRouter) && supports_openai_compatible_stream(invocation) {
            return openai_compatible_chat_stream(&resolved, invocation, &effective_model, &base_url).await;
        }

        let messages = provider_chat_messages(invocation);
        let tools = invocation.tools();

        let builder = build_upstream_chat_builder(backend, &resolved, invocation, &effective_model, &base_url, &tools);

        let provider = builder.build().map_err(EpError::connect)?;

        if let Ok(stream) = provider.chat_stream_struct(&messages).await {
            let mapped = stream.map(|result| match result {
                Ok(chunk) => {
                    let tool_calls: Vec<LlmToolCall> = chunk
                        .choices
                        .first()
                        .and_then(|choice| choice.delta.tool_calls.clone())
                        .unwrap_or_default()
                        .into_iter()
                        .map(Into::into)
                        .collect();
                    let delta = chunk.choices.first().and_then(|choice| choice.delta.content.clone());
                    let usage = chunk.usage.map(Into::<LlmUsage>::into);
                    Ok(LlmStreamChunk { delta, usage, tool_calls, thinking: None })
                }
                Err(err) => Err(EpError::request(err)),
            });
            return Ok(Box::pin(mapped));
        }

        if !tools.is_empty()
            && let Ok(stream) = provider.chat_stream_with_tools(&messages, Some(tools.as_slice())).await
        {
            let mapped = stream.map(|result| match result {
                Ok(chunk) => match chunk {
                    StreamChunk::Text(text) => Ok(LlmStreamChunk {
                        delta: Some(text),
                        usage: None,
                        tool_calls: Vec::new(),
                        thinking: None,
                    }),
                    StreamChunk::ToolUseComplete { tool_call, .. } => Ok(LlmStreamChunk {
                        delta: None,
                        usage: None,
                        tool_calls: vec![LlmToolCall {
                            id: tool_call.id,
                            call_type: tool_call.call_type,
                            function: LlmFunctionCall {
                                name: tool_call.function.name,
                                arguments: tool_call.function.arguments,
                            },
                        }],
                        thinking: None,
                    }),
                    StreamChunk::Done { .. } | StreamChunk::ToolUseStart { .. } | StreamChunk::ToolUseInputDelta { .. } => {
                        Ok(LlmStreamChunk {
                            delta: None,
                            usage: None,
                            tool_calls: Vec::new(),
                            thinking: None,
                        })
                    }
                },
                Err(err) => Err(EpError::request(err)),
            });
            return Ok(Box::pin(mapped));
        }

        let stream = provider.chat_stream(&messages).await.map_err(EpError::request)?;

        let mapped = stream.map(|result| match result {
            Ok(text) => Ok(LlmStreamChunk {
                delta: Some(text),
                usage: None,
                tool_calls: Vec::new(),
                thinking: None,
            }),
            Err(err) => Err(EpError::request(err)),
        });

        Ok(Box::pin(mapped))
    }

    pub fn set_param(&self, param: LlmParam, value: &Value) -> Result<(), EpError> {
        let mut state = self.connection.write().map_err(|_| EpError::request("failed to acquire LLM connection state for update"))?;

        match param {
            LlmParam::Model => {
                let model = value.as_str().ok_or_else(|| EpError::serde("passed model is not a string"))?;
                state.defaults.model = model.to_string();
            }
            LlmParam::Temperature => {
                if value.is_null() {
                    state.defaults.temperature = None;
                } else {
                    let temperature = value.as_f64().ok_or_else(|| EpError::serde("passed temperature is not a number"))?;
                    state.defaults.temperature = Some(temperature as f32);
                }
            }
            LlmParam::MaxTokens => {
                if value.is_null() {
                    state.defaults.max_tokens = None;
                } else {
                    let max_tokens = value.as_u64().ok_or_else(|| EpError::serde("passed max tokens is not a number"))?;
                    state.defaults.max_tokens = Some(max_tokens as u32);
                }
            }
            LlmParam::TopP => {
                if value.is_null() {
                    state.defaults.top_p = None;
                } else {
                    let top_p = value.as_f64().ok_or_else(|| EpError::serde("passed top_p is not a number"))?;
                    state.defaults.top_p = Some(top_p as f32);
                }
            }
            LlmParam::TopK => {
                if value.is_null() {
                    state.defaults.top_k = None;
                } else {
                    let top_k = value.as_u64().ok_or_else(|| EpError::serde("passed top_k is not a number"))?;
                    state.defaults.top_k = Some(top_k as u32);
                }
            }
            LlmParam::BaseUrlOverride => {
                if value.is_null() {
                    state.defaults.base_url_override = None;
                } else {
                    let url = value.as_str().ok_or_else(|| EpError::serde("passed base_url_override is not a string"))?;
                    let trimmed = url.trim();
                    if trimmed.is_empty() {
                        state.defaults.base_url_override = None;
                    } else {
                        state.defaults.base_url_override = Some(trimmed.to_string());
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn health_ping(&self) -> Result<(), EpError> {
        let invocation = LlmInvocation {
            conversation_id: None,
            conversation: vec![LlmMessage {
                role: LlmMessageRole::User,
                content: "ping".to_string(),
                kind: LlmMessageKind::Text,
            }],
            tools: Vec::new(),
            tool_choice: None,
            system_prompt: None,
            system_prompt_blocks: None,
            overrides: LlmRequestOverrides {
                model: None,
                max_tokens: Some(1),
                temperature: None,
                top_p: None,
                top_k: None,
                thinking_budget: None,
            },
            response_format: None,
            parallel_tool_calls: None,
            tool_connections: Vec::new(),
            tool_endpoint_uuids: Vec::new(),
            turn_context: None,
        };

        self.chat(&invocation).await.map(|_| ())
    }
}

#[derive(Debug, Clone, Serialize)]
struct OllamaChatRequest {
    model: String,
    messages: Vec<OllamaChatMessage>,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    options: Option<OllamaOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<OllamaResponseFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<OllamaTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    think: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
struct OllamaOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_k: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "num_predict")]
    max_tokens: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
struct OllamaChatMessage {
    role: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<OllamaToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaToolCall {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    call_type: Option<String>,
    function: OllamaFunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaFunctionCall {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    index: Option<usize>,
    name: String,
    arguments: Value,
}

#[derive(Debug, Clone, Serialize)]
struct OllamaTool {
    #[serde(rename = "type")]
    tool_type: String,
    function: OllamaFunctionTool,
}

#[derive(Debug, Clone, Serialize)]
struct OllamaFunctionTool {
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum OllamaResponseType {
    StructuredOutput(Value),
}

#[derive(Debug, Clone, Serialize)]
struct OllamaResponseFormat {
    #[serde(flatten)]
    format: OllamaResponseType,
}

#[derive(Debug, Clone, Deserialize)]
struct OllamaChatResponse {
    #[serde(default)]
    message: Option<OllamaResponseMessage>,
    #[serde(default)]
    prompt_eval_count: Option<u32>,
    #[serde(default)]
    eval_count: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
struct OllamaStreamResponse {
    #[serde(default)]
    message: Option<OllamaResponseMessage>,
    #[serde(default)]
    done: bool,
    #[serde(default)]
    prompt_eval_count: Option<u32>,
    #[serde(default)]
    eval_count: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
struct OllamaResponseMessage {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    thinking: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<OllamaToolCall>>,
}

#[derive(Debug, Default)]
struct OllamaInlineThinkingState {
    enabled: bool,
    buffering: bool,
    finished: bool,
    pending: String,
}

async fn ollama_chat(
    resolved: &ResolvedLlmConnection,
    invocation: &LlmInvocation,
    effective_model: &str,
    base_url: &str,
) -> Result<LlmChatResponse, EpError> {
    let messages = provider_chat_messages(invocation);
    let request = build_ollama_request(resolved, invocation, effective_model, false);
    let response: OllamaChatResponse = OLLAMA_HTTP_CLIENT
        .post(ollama_chat_url(base_url))
        .json(&request)
        .send()
        .await
        .map_err(EpError::request)?
        .error_for_status()
        .map_err(EpError::request)?
        .json()
        .await
        .map_err(EpError::request)?;

    let response_message = response.message.unwrap_or(OllamaResponseMessage { content: None, thinking: None, tool_calls: None });
    let (text, inline_thinking) =
        normalize_ollama_chat_content(response_message.content.unwrap_or_default(), response_message.thinking.clone());
    let tool_calls = ollama_tool_calls(response_message.tool_calls.unwrap_or_default());
    let usage = usage_or_estimate(ollama_usage(response.prompt_eval_count, response.eval_count), LlmProvider::Ollama, &messages, &text);

    let message_kind = if tool_calls.is_empty() {
        LlmMessageKind::Text
    } else {
        LlmMessageKind::ToolUse { calls: tool_calls }
    };

    let message = LlmMessage {
        role: LlmMessageRole::Assistant,
        content: text,
        kind: message_kind,
    };

    Ok(LlmChatResponse {
        message: message.clone(),
        conversation: vec![message],
        usage,
        thinking: inline_thinking,
        provider: LlmProviderMetadata::new(resolved.provider.to_string(), effective_model.to_string(), Some(base_url.to_string())),
        conversation_id: invocation.conversation_id().cloned(),
    })
}

async fn ollama_chat_stream(
    resolved: &ResolvedLlmConnection,
    invocation: &LlmInvocation,
    effective_model: &str,
    base_url: &str,
) -> Result<LlmStream, EpError> {
    let messages = provider_chat_messages(invocation);
    let request = build_ollama_request(resolved, invocation, effective_model, true);
    let response = OLLAMA_HTTP_CLIENT.post(ollama_chat_url(base_url)).json(&request).send().await.map_err(EpError::request)?;
    let response = response.error_for_status().map_err(EpError::request)?;
    Ok(create_ollama_tool_stream(response, messages, ollama_may_inline_think(effective_model)))
}

fn build_ollama_request(
    resolved: &ResolvedLlmConnection,
    invocation: &LlmInvocation,
    effective_model: &str,
    stream: bool,
) -> OllamaChatRequest {
    let mut messages = ollama_messages(invocation);
    if let Some(system) = invocation.effective_system_prompt() {
        messages.insert(
            0,
            OllamaChatMessage {
                role: "system",
                content: Some(system),
                thinking: None,
                tool_calls: None,
                tool_name: None,
            },
        );
    }

    let tools = normalized_tools_for_provider(resolved.provider, invocation.tools());
    let format = invocation
        .structured_output()
        .and_then(|schema| schema.schema)
        .map(|schema| OllamaResponseFormat { format: OllamaResponseType::StructuredOutput(schema) });
    let options = OllamaOptions {
        temperature: invocation.overrides.temperature.or(resolved.defaults.temperature),
        top_p: invocation.overrides.top_p.or(resolved.defaults.top_p),
        top_k: invocation.overrides.top_k.or(resolved.defaults.top_k),
        max_tokens: invocation.overrides.max_tokens.or(resolved.defaults.max_tokens),
    };
    let options = (options.temperature.is_some() || options.top_p.is_some() || options.top_k.is_some() || options.max_tokens.is_some())
        .then_some(options);

    OllamaChatRequest {
        model: effective_model.to_string(),
        messages,
        stream,
        options,
        format,
        tools: (!tools.is_empty()).then(|| tools.iter().map(OllamaTool::from).collect()),
        think: Some(invocation.overrides.thinking_budget.is_some()),
    }
}

fn ollama_chat_url(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    if trimmed.ends_with("/api") {
        format!("{trimmed}/chat")
    } else {
        format!("{trimmed}/api/chat")
    }
}

fn ollama_messages(invocation: &LlmInvocation) -> Vec<OllamaChatMessage> {
    let mut messages = Vec::new();
    for message in provider_messages(invocation) {
        match &message.kind {
            LlmMessageKind::ToolResult { calls } if !calls.is_empty() => {
                for call in calls {
                    let content = if call.function.arguments.is_empty() {
                        message.content.clone()
                    } else {
                        call.function.arguments.clone()
                    };
                    messages.push(OllamaChatMessage {
                        role: "tool",
                        content: Some(content),
                        thinking: None,
                        tool_calls: None,
                        tool_name: Some(call.function.name.clone()),
                    });
                }
            }
            LlmMessageKind::ToolUse { calls } => {
                messages.push(OllamaChatMessage {
                    role: "assistant",
                    content: (!message.content.is_empty()).then(|| message.content.clone()),
                    thinking: None,
                    tool_calls: Some(calls.iter().enumerate().map(|(idx, call)| OllamaToolCall::from_llm(call, Some(idx))).collect()),
                    tool_name: None,
                });
            }
            _ => {
                messages.push(OllamaChatMessage {
                    role: match message.role {
                        LlmMessageRole::User => "user",
                        LlmMessageRole::Assistant => "assistant",
                    },
                    content: Some(message.content.clone()),
                    thinking: None,
                    tool_calls: None,
                    tool_name: None,
                });
            }
        }
    }
    messages
}

fn ollama_tool_calls(tool_calls: Vec<OllamaToolCall>) -> Vec<LlmToolCall> {
    tool_calls
        .into_iter()
        .enumerate()
        .map(|(idx, call)| LlmToolCall {
            id: call.id.unwrap_or_else(|| format!("call_{}_{}", call.function.name, call.function.index.unwrap_or(idx))),
            call_type: call.call_type.unwrap_or_else(|| "function".to_string()),
            function: LlmFunctionCall {
                name: call.function.name,
                arguments: serde_json::to_string(&call.function.arguments).unwrap_or_else(|_| "{}".to_string()),
            },
        })
        .collect()
}

fn ollama_usage(prompt_eval_count: Option<u32>, eval_count: Option<u32>) -> Option<LlmUsage> {
    let prompt_tokens = prompt_eval_count?;
    let completion_tokens = eval_count.unwrap_or(0);
    Some(LlmUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens: prompt_tokens.saturating_add(completion_tokens),
        completion_tokens_details: None,
        prompt_tokens_details: None,
    })
}

fn create_ollama_tool_stream(response: reqwest::Response, messages: Vec<ChatMessage>, inline_thinking_enabled: bool) -> LlmStream {
    let stream = response
        .bytes_stream()
        .scan(
            (
                String::new(),
                Vec::new(),
                String::new(),
                OllamaInlineThinkingState { enabled: inline_thinking_enabled, ..Default::default() },
            ),
            move |(buffer, utf8_buffer, response_text, inline_thinking), chunk| {
                let result = match chunk {
                    Ok(bytes) => {
                        utf8_buffer.extend_from_slice(&bytes);
                        match String::from_utf8(utf8_buffer.clone()) {
                            Ok(text) => {
                                buffer.push_str(&text);
                                utf8_buffer.clear();
                            }
                            Err(err) => {
                                let valid_up_to = err.utf8_error().valid_up_to();
                                if valid_up_to > 0 {
                                    let valid = String::from_utf8_lossy(&utf8_buffer[..valid_up_to]);
                                    buffer.push_str(&valid);
                                    utf8_buffer.drain(..valid_up_to);
                                }
                            }
                        }

                        let mut results = Vec::new();
                        while let Some(position) = buffer.find('\n') {
                            let line = buffer[..position].trim().to_string();
                            buffer.drain(..=position);
                            match parse_ollama_line(&line, &messages, response_text, inline_thinking) {
                                Ok(Some(chunk)) => results.push(Ok(chunk)),
                                Ok(None) => {}
                                Err(err) => results.push(Err(err)),
                            }
                        }
                        Some(results)
                    }
                    Err(err) => Some(vec![Err(EpError::request(err))]),
                };
                async move { result }
            },
        )
        .flat_map(stream::iter);
    Box::pin(stream)
}

fn parse_ollama_line(
    line: &str,
    messages: &[ChatMessage],
    response_text: &mut String,
    inline_thinking: &mut OllamaInlineThinkingState,
) -> Result<Option<LlmStreamChunk>, EpError> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let response = serde_json::from_str::<OllamaStreamResponse>(trimmed)
        .map_err(|err| EpError::request(format!("failed to parse Ollama stream chunk: {err}")))?;
    let mut delta = None;
    let mut thinking = None;
    let mut tool_calls = Vec::new();

    if let Some(message) = response.message {
        if let Some(content) = message.content.filter(|content| !content.is_empty()) {
            let (delta_content, inline_thinking_text) = split_ollama_stream_content(content, inline_thinking, response.done);
            if let Some(thinking_text) = inline_thinking_text {
                thinking = Some(match thinking {
                    Some(existing) => format!("{existing}{thinking_text}"),
                    None => thinking_text,
                });
            }
            if let Some(visible_content) = delta_content {
                response_text.push_str(&visible_content);
                delta = Some(visible_content);
            }
        }
        if let Some(native_thinking) = message.thinking.filter(|thinking| !thinking.is_empty()) {
            thinking = Some(match thinking {
                Some(existing) => format!("{existing}{native_thinking}"),
                None => native_thinking,
            });
        }
        tool_calls = ollama_tool_calls(message.tool_calls.unwrap_or_default());
    }

    let usage = if response.done {
        usage_or_estimate(
            ollama_usage(response.prompt_eval_count, response.eval_count),
            LlmProvider::Ollama,
            messages,
            response_text,
        )
    } else {
        None
    };

    if delta.is_none() && thinking.is_none() && tool_calls.is_empty() && usage.is_none() {
        return Ok(None);
    }

    Ok(Some(LlmStreamChunk { delta, usage, tool_calls, thinking }))
}

fn ollama_may_inline_think(model: &str) -> bool {
    let normalized = model.to_ascii_lowercase();
    normalized.starts_with("qwen3") || normalized.contains(":qwen3") || normalized.contains("/qwen3")
}

fn normalize_ollama_chat_content(content: String, thinking: Option<String>) -> (String, Option<String>) {
    let native_thinking = thinking.filter(|thinking| !thinking.is_empty());
    if let Some(existing_thinking) = native_thinking {
        return (content, Some(existing_thinking));
    }

    if let Some((thinking_text, answer_text)) = extract_inline_ollama_thinking(&content) {
        return (answer_text, Some(thinking_text));
    }

    (content, None)
}

fn split_ollama_stream_content(content: String, state: &mut OllamaInlineThinkingState, done: bool) -> (Option<String>, Option<String>) {
    if !state.enabled || state.finished {
        return (!content.is_empty()).then_some(content).map_or((None, None), |delta| (Some(delta), None));
    }

    if state.buffering {
        state.pending.push_str(&content);
        if let Some((thinking_text, answer_text)) = extract_inline_ollama_thinking(&state.pending) {
            state.pending.clear();
            state.buffering = false;
            state.finished = true;
            let delta = (!answer_text.is_empty()).then_some(answer_text);
            let thinking = (!thinking_text.is_empty()).then_some(thinking_text);
            return (delta, thinking);
        }
        if done {
            let flushed = std::mem::take(&mut state.pending);
            state.buffering = false;
            state.finished = true;
            return (!flushed.is_empty()).then_some(flushed).map_or((None, None), |delta| (Some(delta), None));
        }
        return (None, None);
    }

    if let Some((thinking_text, answer_text)) = extract_inline_ollama_thinking(&content) {
        state.finished = true;
        let delta = (!answer_text.is_empty()).then_some(answer_text);
        let thinking = (!thinking_text.is_empty()).then_some(thinking_text);
        return (delta, thinking);
    }

    if done {
        state.finished = true;
        return (!content.is_empty()).then_some(content).map_or((None, None), |delta| (Some(delta), None));
    }

    state.buffering = true;
    state.pending.push_str(&content);
    (None, None)
}

fn extract_inline_ollama_thinking(content: &str) -> Option<(String, String)> {
    let (thinking_prefix, answer_suffix) = content.split_once("</think>")?;
    let thinking = thinking_prefix.trim_start_matches("<think>").trim().to_string();
    let answer = answer_suffix.trim_start_matches(['\r', '\n']).to_string();
    Some((thinking, answer))
}

impl OllamaToolCall {
    fn from_llm(call: &LlmToolCall, index: Option<usize>) -> Self {
        Self {
            id: Some(call.id.clone()),
            call_type: Some(call.call_type.clone()),
            function: OllamaFunctionCall {
                index,
                name: call.function.name.clone(),
                arguments: serde_json::from_str(&call.function.arguments)
                    .unwrap_or_else(|_| Value::String(call.function.arguments.clone())),
            },
        }
    }
}

impl From<&Tool> for OllamaTool {
    fn from(tool: &Tool) -> Self {
        Self {
            tool_type: "function".to_string(),
            function: OllamaFunctionTool {
                name: tool.function.name.clone(),
                description: tool.function.description.clone(),
                parameters: tool.function.parameters.clone(),
            },
        }
    }
}

const ANTHROPIC_API_VERSION: &str = "2023-06-01";
const ANTHROPIC_DEFAULT_MAX_TOKENS: u32 = 300;
const ANTHROPIC_DEFAULT_TEMPERATURE: f32 = 0.7;
const ANTHROPIC_MAX_EXPLICIT_CACHE_BREAKPOINTS: usize = 4;

#[derive(Debug, Clone, Serialize)]
struct AnthropicCacheControl {
    #[serde(rename = "type")]
    cache_type: &'static str,
}

impl AnthropicCacheControl {
    fn ephemeral() -> Self {
        Self { cache_type: "ephemeral" }
    }
}

#[derive(Debug, Clone, Serialize)]
struct AnthropicThinkingConfig {
    #[serde(rename = "type")]
    thinking_type: &'static str,
    budget_tokens: u32,
}

#[derive(Debug, Clone, Serialize)]
struct AnthropicTool {
    name: String,
    description: String,
    #[serde(rename = "input_schema")]
    schema: Value,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum AnthropicSystem {
    Text(String),
    Blocks(Vec<AnthropicTextBlock>),
}

#[derive(Debug, Clone, Serialize)]
struct AnthropicTextBlock {
    #[serde(rename = "type")]
    block_type: &'static str,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<AnthropicCacheControl>,
}

#[derive(Debug, Clone, Serialize)]
struct AnthropicMessage {
    role: &'static str,
    content: Vec<AnthropicMessageContent>,
}

#[derive(Debug, Clone, Serialize)]
struct AnthropicMessageContent {
    #[serde(rename = "type")]
    block_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    image_url: Option<AnthropicImageUrl>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "id")]
    tool_use_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "name")]
    tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "input")]
    tool_input: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "tool_use_id")]
    tool_result_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "content")]
    tool_output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<AnthropicCacheControl>,
}

#[derive(Debug, Clone, Serialize)]
struct AnthropicImageUrl {
    url: String,
}

#[derive(Debug, Clone, Serialize)]
struct AnthropicMessagesRequest {
    messages: Vec<AnthropicMessage>,
    model: String,
    max_tokens: u32,
    temperature: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<AnthropicSystem>,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_k: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<AnthropicTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking: Option<AnthropicThinkingConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<AnthropicCacheControl>,
}

#[derive(Debug, Clone, Deserialize)]
struct AnthropicCompleteResponse {
    content: Vec<AnthropicContent>,
    usage: Option<AnthropicUsage>,
}

#[derive(Debug, Clone, Deserialize)]
struct AnthropicUsage {
    input_tokens: u32,
    output_tokens: u32,
    #[serde(default)]
    cache_creation_input_tokens: Option<u32>,
    #[serde(default)]
    cache_read_input_tokens: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
struct AnthropicContent {
    #[serde(default)]
    text: Option<String>,
    #[serde(default, rename = "type")]
    content_type: Option<String>,
    #[serde(default)]
    thinking: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: Option<Value>,
    #[serde(default)]
    id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct AnthropicStreamResponse {
    #[serde(rename = "type")]
    response_type: String,
    #[serde(default)]
    index: Option<usize>,
    #[serde(default)]
    content_block: Option<AnthropicStreamContentBlock>,
    #[serde(default)]
    delta: Option<AnthropicStreamDelta>,
    #[serde(default)]
    usage: Option<AnthropicUsage>,
    #[serde(default)]
    message: Option<AnthropicStreamMessage>,
}

#[derive(Debug, Clone, Deserialize)]
struct AnthropicStreamMessage {
    #[serde(default)]
    usage: Option<AnthropicUsage>,
}

#[derive(Debug, Clone, Deserialize)]
struct AnthropicStreamContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct AnthropicStreamDelta {
    #[serde(default, rename = "type")]
    delta_type: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    partial_json: Option<String>,
    #[serde(default)]
    stop_reason: Option<String>,
    #[serde(default)]
    thinking: Option<String>,
}

#[derive(Debug, Default)]
struct AnthropicToolUseState {
    id: String,
    name: String,
    json_buffer: String,
}

async fn anthropic_chat(
    resolved: &ResolvedLlmConnection,
    invocation: &LlmInvocation,
    effective_model: &str,
    base_url: &str,
) -> Result<LlmChatResponse, EpError> {
    let request = build_anthropic_request(resolved, invocation, effective_model, false);
    let url = anthropic_messages_url(base_url);
    let api_key = anthropic_api_key(resolved)?;

    let mut builder = ANTHROPIC_HTTP_CLIENT
        .post(url)
        .header("content-type", "application/json")
        .header("anthropic-version", ANTHROPIC_API_VERSION);

    builder = apply_anthropic_auth_headers(builder, resolved.provider, api_key);

    if anthropic_prompt_cache_enabled(effective_model) {
        builder = builder.header("anthropic-beta", "prompt-caching-2024-07-31");
    }

    let response = builder.json(&request).send().await.map_err(EpError::request)?;

    let status = response.status();
    let body = response.text().await.map_err(EpError::request)?;
    if !status.is_success() {
        return Err(EpError::request(format!("Anthropic API returned {}: {}", status, truncate_provider_error(&body))));
    }

    let response: AnthropicCompleteResponse =
        serde_json::from_str(&body).map_err(|err| EpError::request(format!("failed to parse Anthropic response: {err}")))?;

    let tool_calls = anthropic_tool_calls(&response);
    let thinking = anthropic_thinking(&response);
    let text = anthropic_text(&response);
    let usage = anthropic_usage(response.usage.clone());

    let message_kind = if tool_calls.is_empty() {
        LlmMessageKind::Text
    } else {
        LlmMessageKind::ToolUse { calls: tool_calls }
    };

    let message = LlmMessage {
        role: LlmMessageRole::Assistant,
        content: text,
        kind: message_kind,
    };
    let conversation = vec![message.clone()];

    Ok(LlmChatResponse {
        message,
        conversation,
        usage,
        thinking,
        provider: LlmProviderMetadata::new(resolved.provider.to_string(), effective_model.to_string(), Some(base_url.to_string())),
        conversation_id: invocation.conversation_id().cloned(),
    })
}

async fn anthropic_chat_stream(
    resolved: &ResolvedLlmConnection,
    invocation: &LlmInvocation,
    effective_model: &str,
    base_url: &str,
) -> Result<LlmStream, EpError> {
    let request = build_anthropic_request(resolved, invocation, effective_model, true);
    let url = anthropic_messages_url(base_url);
    let api_key = anthropic_api_key(resolved)?;

    let mut builder = ANTHROPIC_HTTP_CLIENT
        .post(url)
        .header("content-type", "application/json")
        .header("anthropic-version", ANTHROPIC_API_VERSION);

    builder = apply_anthropic_auth_headers(builder, resolved.provider, api_key);

    if anthropic_prompt_cache_enabled(effective_model) {
        builder = builder.header("anthropic-beta", "prompt-caching-2024-07-31");
    }

    let response = builder.json(&request).send().await.map_err(EpError::request)?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.map_err(EpError::request)?;
        return Err(EpError::request(format!("Anthropic API returned {}: {}", status, truncate_provider_error(&body))));
    }

    Ok(create_anthropic_tool_stream(response))
}

fn build_anthropic_request(
    resolved: &ResolvedLlmConnection,
    invocation: &LlmInvocation,
    effective_model: &str,
    stream: bool,
) -> AnthropicMessagesRequest {
    let tools = invocation.tools();
    let tool_choice = invocation.tool_choice();
    let cache_enabled = anthropic_prompt_cache_enabled(effective_model);

    let mut budget = ANTHROPIC_MAX_EXPLICIT_CACHE_BREAKPOINTS;

    let (system, system_breakpoints_used) = anthropic_system_with_budget(invocation, cache_enabled, budget);
    budget -= system_breakpoints_used;

    let messages = anthropic_messages_with_cache(invocation, cache_enabled, budget);

    AnthropicMessagesRequest {
        messages,
        model: effective_model.to_string(),
        max_tokens: invocation.overrides.max_tokens.or(resolved.defaults.max_tokens).unwrap_or(ANTHROPIC_DEFAULT_MAX_TOKENS).max(16),
        temperature: invocation.overrides.temperature.or(resolved.defaults.temperature).unwrap_or(ANTHROPIC_DEFAULT_TEMPERATURE),
        system,
        stream,
        top_p: invocation.overrides.top_p.or(resolved.defaults.top_p),
        top_k: invocation.overrides.top_k.or(resolved.defaults.top_k),
        tools: anthropic_tools(&tools),
        tool_choice: anthropic_tool_choice(tool_choice, !tools.is_empty()),
        thinking: invocation
            .overrides
            .thinking_budget
            .map(|budget_tokens| AnthropicThinkingConfig { thinking_type: "enabled", budget_tokens }),
        cache_control: None,
    }
}

fn anthropic_api_key(resolved: &ResolvedLlmConnection) -> Result<&str, EpError> {
    resolved
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|api_key| !api_key.is_empty())
        .ok_or_else(|| EpError::connect("missing Anthropic API key"))
}

fn apply_anthropic_auth_headers(builder: reqwest::RequestBuilder, provider: LlmProvider, api_key: &str) -> reqwest::RequestBuilder {
    match provider {
        LlmProvider::OpenRouter => builder.header("authorization", format!("Bearer {api_key}")),
        _ => builder.header("x-api-key", api_key),
    }
}

fn anthropic_messages_url(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    if trimmed.ends_with("/v1") {
        format!("{trimmed}/messages")
    } else {
        format!("{trimmed}/v1/messages")
    }
}

fn truncate_provider_error(body: &str) -> String {
    let mut truncated = body.chars().take(512).collect::<String>();
    if body.chars().count() > 512 {
        truncated.push_str("...");
    }
    truncated
}

fn anthropic_prompt_cache_enabled(model: &str) -> bool {
    let model = model.trim().to_ascii_lowercase();
    let bare = strip_anthropic_model_prefix(&model);
    bare.starts_with("claude") && (bare.contains("-3") || bare.contains("-4"))
}

fn should_use_anthropic_path(resolved: &ResolvedLlmConnection, model: &str) -> bool {
    match resolved.provider {
        LlmProvider::Anthropic => true,
        LlmProvider::OpenRouter => is_anthropic_model(model),
        _ => false,
    }
}

/// Where a chat invocation should be dispatched. Selected once per call so the
/// downstream code stops scattering ad-hoc provider matches across `chat` and
/// `chat_stream`.
enum ChatRoute {
    /// Route via the upstream `llm` crate using the given backend. Used for
    /// OpenAI and non-Anthropic OpenRouter traffic.
    Upstream(LLMBackend),
    /// Hand-rolled Ollama path — the upstream backend never reports token
    /// usage, so we issue HTTP directly and estimate.
    OllamaNative,
    /// Hand-rolled Anthropic Messages API path — used for first-party
    /// Anthropic and for Anthropic-family models routed through OpenRouter,
    /// because the upstream builder cannot opt into prompt caching.
    AnthropicMessages,
    /// Hand-rolled Azure OpenAI classic deployment-path. The upstream
    /// `llm::backends::azure_openai` only targets Azure's newer unified
    /// `/openai/v1/` router, so the classic
    /// `/openai/deployments/{deployment}/chat/completions` shape is
    /// implemented separately.
    AzureOpenAiClassic,
}

fn chat_route(resolved: &ResolvedLlmConnection, model: &str) -> Result<ChatRoute, EpError> {
    if resolved.provider == LlmProvider::AzureOpenAI {
        return Ok(ChatRoute::AzureOpenAiClassic);
    }
    if resolved.provider == LlmProvider::Ollama {
        return Ok(ChatRoute::OllamaNative);
    }
    if should_use_anthropic_path(resolved, model) {
        return Ok(ChatRoute::AnthropicMessages);
    }
    let backend = LLMBackend::try_from(&resolved.provider)?;
    Ok(ChatRoute::Upstream(backend))
}

/// Common `LLMBuilder` setup shared by `chat` and `chat_stream`. Extracted so
/// the option-by-option wiring lives in one place — and so new upstream-routed
/// providers don't grow a duplicated 50-line block in both call sites.
fn build_upstream_chat_builder(
    backend: LLMBackend,
    resolved: &ResolvedLlmConnection,
    invocation: &LlmInvocation,
    effective_model: &str,
    base_url: &str,
    tools: &[Tool],
) -> LLMBuilder {
    let mut builder = LLMBuilder::new().backend(backend).model(effective_model.to_string()).base_url(base_url.to_string());

    if let Some(api_key) = resolved.api_key.as_deref().map(str::trim).filter(|key| !key.is_empty()) {
        builder = builder.api_key(api_key.to_string());
    }

    if let Some(temp) = invocation.overrides.temperature.or(resolved.defaults.temperature) {
        builder = builder.temperature(temp);
    }

    if let Some(max_tokens) = invocation.overrides.max_tokens.or(resolved.defaults.max_tokens) {
        builder = builder.max_tokens(max_tokens.max(16));
    }

    if let Some(top_p) = invocation.overrides.top_p.or(resolved.defaults.top_p) {
        builder = builder.top_p(top_p);
    }

    if let Some(top_k) = invocation.overrides.top_k.or(resolved.defaults.top_k) {
        builder = builder.top_k(top_k);
    }

    if let Some(system) = invocation.effective_system_prompt() {
        builder = builder.system(system);
    }

    for tool in tools {
        builder = builder.function(
            FunctionBuilder::new(&tool.function.name)
                .description(&tool.function.description)
                .json_schema(normalize_tool_schema_for_provider(resolved.provider, &tool.function.parameters)),
        );
    }

    if let Some(choice) = invocation.tool_choice() {
        builder = builder.tool_choice(choice);
    }

    if let Some(enable) = invocation.parallel_tool_calls {
        builder = builder.enable_parallel_tool_use(enable);
    }

    if let Some(budget) = invocation.overrides.thinking_budget {
        builder = builder.reasoning(true).reasoning_budget_tokens(budget);
    }

    if let Some(schema) = invocation.structured_output() {
        builder = builder.schema(schema);
    }

    builder
}

fn is_anthropic_model(model: &str) -> bool {
    let lower = model.trim().to_ascii_lowercase();
    lower.starts_with("anthropic/") || lower.starts_with("claude") || lower.contains("/claude")
}

fn strip_anthropic_model_prefix(model: &str) -> &str {
    model.strip_prefix("anthropic/").unwrap_or(model)
}

fn anthropic_tools(tools: &[Tool]) -> Option<Vec<AnthropicTool>> {
    (!tools.is_empty()).then(|| {
        tools
            .iter()
            .map(|tool| AnthropicTool {
                name: tool.function.name.clone(),
                description: tool.function.description.clone(),
                schema: tool.function.parameters.clone(),
            })
            .collect()
    })
}

fn anthropic_tool_choice(choice: Option<LlmToolChoiceInner>, has_tools: bool) -> Option<HashMap<String, String>> {
    if !has_tools {
        return None;
    }

    match choice {
        Some(LlmToolChoiceInner::Auto) => Some(HashMap::from([("type".to_string(), "auto".to_string())])),
        Some(LlmToolChoiceInner::Any) => Some(HashMap::from([("type".to_string(), "any".to_string())])),
        Some(LlmToolChoiceInner::Tool(name)) => Some(HashMap::from([("type".to_string(), "tool".to_string()), ("name".to_string(), name)])),
        Some(LlmToolChoiceInner::None) => Some(HashMap::from([("type".to_string(), "none".to_string())])),
        None => None,
    }
}

/// Builds Anthropic system prompt blocks with cache breakpoint allocation.
///
/// Returns the system prompt and the number of breakpoints consumed.
/// `Stable` blocks get breakpoints before `SessionStable` blocks; within the
/// same hint tier, larger blocks get priority.
fn anthropic_system_with_budget(invocation: &LlmInvocation, cache_enabled: bool, budget: usize) -> (Option<AnthropicSystem>, usize) {
    if !cache_enabled {
        if let Some(blocks) = invocation.system_prompt_blocks() {
            let rendered: Vec<AnthropicTextBlock> = blocks
                .iter()
                .filter(|b| !b.content.trim().is_empty())
                .map(|b| AnthropicTextBlock {
                    block_type: "text",
                    text: b.content.trim().to_string(),
                    cache_control: None,
                })
                .collect();
            if !rendered.is_empty() {
                return (Some(AnthropicSystem::Blocks(rendered)), 0);
            }
        }
        return (invocation.effective_system_prompt().map(AnthropicSystem::Text), 0);
    }

    let Some(blocks) = invocation.system_prompt_blocks() else {
        return (invocation.effective_system_prompt().map(AnthropicSystem::Text), 0);
    };

    let mut rendered: Vec<(usize, AnthropicTextBlock)> = blocks
        .iter()
        .enumerate()
        .filter(|(_, b)| !b.content.trim().is_empty())
        .map(|(idx, b)| {
            (
                idx,
                AnthropicTextBlock {
                    block_type: "text",
                    text: b.content.trim().to_string(),
                    cache_control: None,
                },
            )
        })
        .collect();

    if rendered.is_empty() {
        return (invocation.effective_system_prompt().map(AnthropicSystem::Text), 0);
    }

    let mut candidates: Vec<(usize, &CacheHint, usize)> = blocks
        .iter()
        .enumerate()
        .filter(|(_, b)| b.cache_hint.is_cacheable() && !b.content.trim().is_empty())
        .map(|(idx, b)| (idx, &b.cache_hint, b.content.trim().len()))
        .collect();

    candidates.sort_by(|a, b| {
        let hint_order = |h: &CacheHint| -> u8 {
            match h {
                CacheHint::Stable => 0,
                CacheHint::SessionStable => 1,
                CacheHint::Volatile => 2,
            }
        };
        hint_order(a.1).cmp(&hint_order(b.1)).then_with(|| b.2.cmp(&a.2))
    });

    let system_budget = budget.min(candidates.len());
    let mut breakpoint_indices: Vec<usize> = candidates.iter().take(system_budget).map(|(idx, _, _)| *idx).collect();
    breakpoint_indices.sort_unstable();

    let mut log_parts: Vec<String> = Vec::new();
    for &bp_idx in &breakpoint_indices {
        if let Some((_, block)) = rendered.iter_mut().find(|(idx, _)| *idx == bp_idx) {
            block.cache_control = Some(AnthropicCacheControl::ephemeral());
            let kind = &blocks[bp_idx].kind;
            let chars = block.text.len();
            log_parts.push(format!("{kind:?}: ~{chars} chars"));
        }
    }

    let used = breakpoint_indices.len();
    let final_blocks: Vec<AnthropicTextBlock> = rendered.into_iter().map(|(_, b)| b).collect();

    if !log_parts.is_empty() {
        info!(
            "cache breakpoints: {} system ({}), {} remaining for history",
            used,
            log_parts.join(", "),
            budget.saturating_sub(used),
        );
    }

    (Some(AnthropicSystem::Blocks(final_blocks)), used)
}

/// Builds Anthropic conversation messages, optionally placing cache breakpoints
/// on trailing conversation history.
///
/// Breakpoints go on the last content block of the last N user messages before
/// the final turn, so each tool-loop iteration caches the prefix through the
/// previous tool result.
fn anthropic_messages_with_cache(invocation: &LlmInvocation, cache_enabled: bool, remaining_budget: usize) -> Vec<AnthropicMessage> {
    let messages: Vec<LlmMessage> = compacted_provider_messages(invocation);
    let total_messages = messages.len();
    let mut result: Vec<AnthropicMessage> = messages
        .into_iter()
        .map(|message| AnthropicMessage {
            role: match message.role {
                LlmMessageRole::User => "user",
                LlmMessageRole::Assistant => "assistant",
            },
            content: anthropic_message_content(message),
        })
        .collect();

    inject_turn_context_into_anthropic_messages(&mut result, invocation.turn_context.as_deref());

    if !cache_enabled || remaining_budget == 0 || result.is_empty() {
        return result;
    }

    let mut placed = 0usize;
    for msg_index in (0..result.len()).rev() {
        if placed >= remaining_budget {
            break;
        }
        if result[msg_index].role != "user" {
            continue;
        }
        if let Some(last_block) = result[msg_index].content.last_mut() {
            last_block.cache_control = Some(AnthropicCacheControl::ephemeral());
            placed += 1;
            info!(
                "cache breakpoints: history (position: msg {} of {}, type: {})",
                msg_index + 1,
                total_messages,
                last_block.block_type,
            );
        }
    }

    result
}

fn anthropic_message_content(message: LlmMessage) -> Vec<AnthropicMessageContent> {
    match message.kind {
        LlmMessageKind::Text => vec![AnthropicMessageContent {
            block_type: "text",
            text: Some(message.content),
            image_url: None,
            tool_use_id: None,
            tool_name: None,
            tool_input: None,
            tool_result_id: None,
            tool_output: None,
            cache_control: None,
        }],
        LlmMessageKind::ImageUrl { url } => vec![AnthropicMessageContent {
            block_type: "image_url",
            text: None,
            image_url: Some(AnthropicImageUrl { url }),
            tool_use_id: None,
            tool_name: None,
            tool_input: None,
            tool_result_id: None,
            tool_output: None,
            cache_control: None,
        }],
        LlmMessageKind::ToolUse { calls } => calls
            .into_iter()
            .map(|call| AnthropicMessageContent {
                block_type: "tool_use",
                text: None,
                image_url: None,
                tool_use_id: Some(call.id),
                tool_name: Some(call.function.name),
                tool_input: Some(serde_json::from_str::<Value>(&call.function.arguments).unwrap_or(Value::String(call.function.arguments))),
                tool_result_id: None,
                tool_output: None,
                cache_control: None,
            })
            .collect(),
        LlmMessageKind::ToolResult { calls } => calls
            .into_iter()
            .map(|call| AnthropicMessageContent {
                block_type: "tool_result",
                text: None,
                image_url: None,
                tool_use_id: None,
                tool_name: None,
                tool_input: None,
                tool_result_id: Some(call.id),
                tool_output: Some(call.function.arguments),
                cache_control: None,
            })
            .collect(),
    }
}

fn anthropic_text(response: &AnthropicCompleteResponse) -> String {
    response
        .content
        .iter()
        .filter_map(|content| {
            if content.content_type.as_deref() == Some("text") || content.content_type.is_none() {
                content.text.clone()
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn anthropic_thinking(response: &AnthropicCompleteResponse) -> Option<String> {
    response
        .content
        .iter()
        .find(|content| content.content_type.as_deref() == Some("thinking"))
        .and_then(|content| content.thinking.clone())
}

fn anthropic_tool_calls(response: &AnthropicCompleteResponse) -> Vec<LlmToolCall> {
    response
        .content
        .iter()
        .filter(|content| content.content_type.as_deref() == Some("tool_use"))
        .map(|content| {
            let tool_name = content.name.clone().unwrap_or_default();
            // `serde_json::to_string(Value)` is effectively infallible, but
            // still warn on the off-chance it ever fires (e.g. custom
            // Serialize impls leaking) so ops is not blind.
            let arguments = match content.input.as_ref() {
                Some(input) => serde_json::to_string(input).unwrap_or_else(|err| {
                    warn!(
                        target = "llm.anthropic.response",
                        tool_name = %tool_name,
                        error = %err,
                        "failed to serialize Anthropic tool_use.input; defaulting to empty object",
                    );
                    "{}".to_string()
                }),
                None => "{}".to_string(),
            };
            LlmToolCall {
                id: content.id.clone().unwrap_or_default(),
                call_type: "function".to_string(),
                function: LlmFunctionCall { name: tool_name, arguments },
            }
        })
        .collect()
}

fn anthropic_usage(usage: Option<AnthropicUsage>) -> Option<LlmUsage> {
    usage.map(|usage| {
        let cached_tokens = usage.cache_creation_input_tokens.unwrap_or(0) + usage.cache_read_input_tokens.unwrap_or(0);
        LlmUsage {
            prompt_tokens: usage.input_tokens,
            completion_tokens: usage.output_tokens,
            total_tokens: usage.input_tokens + usage.output_tokens,
            completion_tokens_details: None,
            prompt_tokens_details: (cached_tokens > 0)
                .then_some(crate::types::LlmPromptTokensDetails { cached_tokens: Some(cached_tokens), audio_tokens: None }),
        }
    })
}

/// Feed a new byte chunk into the SSE text buffer, updating the
/// UTF-8 continuation buffer and the decoded text buffer.
///
/// Split out as a pure function so the byte-split fuzz in
/// [`tests::anthropic_stream_buffer_handles_arbitrary_byte_splits`]
/// can exercise the state machine without mocking a
/// `reqwest::Response`. Callers feed any byte slice (including
/// splits mid-UTF-8 or mid-SSE-event) and can extract complete SSE
/// events via [`extract_sse_events`] after each call.
pub(crate) fn buffer_anthropic_sse_bytes(buffer: &mut String, utf8_buffer: &mut Vec<u8>, chunk: &[u8]) {
    utf8_buffer.extend_from_slice(chunk);
    match String::from_utf8(utf8_buffer.clone()) {
        Ok(text) => {
            buffer.push_str(&text);
            utf8_buffer.clear();
        }
        Err(err) => {
            let valid_up_to = err.utf8_error().valid_up_to();
            if valid_up_to > 0 {
                let valid = String::from_utf8_lossy(&utf8_buffer[..valid_up_to]);
                buffer.push_str(&valid);
                utf8_buffer.drain(..valid_up_to);
            }
        }
    }
}

/// Drain every complete SSE event (terminated by `\n\n`) from
/// `buffer` and return them in order. The trailing incomplete event
/// stays in the buffer for the next call.
pub(crate) fn extract_sse_events(buffer: &mut String) -> Vec<String> {
    let mut events = Vec::new();
    while let Some(position) = buffer.find("\n\n") {
        let event = buffer[..position + 2].to_string();
        buffer.drain(..position + 2);
        events.push(event);
    }
    events
}

fn create_anthropic_tool_stream(response: reqwest::Response) -> LlmStream {
    let stream = response
        .bytes_stream()
        .scan((String::new(), Vec::new(), HashMap::new()), move |(buffer, utf8_buffer, tool_states), chunk| {
            let result = match chunk {
                Ok(bytes) => {
                    buffer_anthropic_sse_bytes(buffer, utf8_buffer, &bytes);
                    let mut results = Vec::new();
                    for event in extract_sse_events(buffer) {
                        match parse_anthropic_sse_chunk_with_tools(&event, tool_states) {
                            Ok(Some(chunk)) => results.push(Ok(chunk)),
                            Ok(None) => {}
                            Err(err) => results.push(Err(err)),
                        }
                    }
                    Some(results)
                }
                Err(err) => Some(vec![Err(EpError::request(err))]),
            };
            async move { result }
        })
        .flat_map(stream::iter);
    Box::pin(stream)
}

fn parse_anthropic_sse_chunk_with_tools(
    chunk: &str,
    tool_states: &mut HashMap<usize, AnthropicToolUseState>,
) -> Result<Option<LlmStreamChunk>, EpError> {
    for line in chunk.lines() {
        let line = line.trim();
        let Some(data) = line.strip_prefix("data: ") else {
            continue;
        };
        let response = match serde_json::from_str::<AnthropicStreamResponse>(data) {
            Ok(response) => response,
            Err(err) => {
                // Surface malformed SSE frames instead of silently dropping
                // them. A single dropped frame can desync tool-call JSON
                // assembly; ops needs to see this. Truncate the payload so
                // we don't paste user content into logs.
                let preview: String = data.chars().take(160).collect();
                warn!(
                    target = "llm.anthropic.stream",
                    error = %err,
                    data_preview = %preview,
                    "discarding malformed Anthropic SSE frame",
                );
                continue;
            }
        };
        match response.response_type.as_str() {
            "message_start" => {
                let usage = response.message.and_then(|msg| msg.usage).and_then(|u| anthropic_usage(Some(u)));
                if usage.is_some() {
                    return Ok(Some(LlmStreamChunk { delta: None, usage, tool_calls: Vec::new(), thinking: None }));
                }
            }
            "content_block_start" => {
                if let (Some(index), Some(content_block)) = (response.index, response.content_block)
                    && content_block.block_type == "tool_use"
                {
                    tool_states.insert(
                        index,
                        AnthropicToolUseState {
                            id: content_block.id.unwrap_or_default(),
                            name: content_block.name.unwrap_or_default(),
                            json_buffer: String::new(),
                        },
                    );
                }
            }
            "content_block_delta" => {
                if let (Some(index), Some(delta)) = (response.index, response.delta) {
                    match delta.delta_type.as_deref() {
                        Some("text_delta") => {
                            if let Some(text) = delta.text {
                                return Ok(Some(LlmStreamChunk {
                                    delta: Some(text),
                                    usage: None,
                                    tool_calls: Vec::new(),
                                    thinking: None,
                                }));
                            }
                        }
                        Some("thinking_delta") => {
                            if let Some(thinking_text) = delta.thinking {
                                return Ok(Some(LlmStreamChunk {
                                    delta: None,
                                    usage: None,
                                    tool_calls: Vec::new(),
                                    thinking: Some(thinking_text),
                                }));
                            }
                        }
                        Some("input_json_delta") => {
                            if let Some(partial_json) = delta.partial_json
                                && let Some(state) = tool_states.get_mut(&index)
                            {
                                state.json_buffer.push_str(&partial_json);
                            }
                        }
                        _ => {}
                    }
                }
            }
            "content_block_stop" => {
                if let Some(index) = response.index
                    && let Some(state) = tool_states.remove(&index)
                {
                    let arguments = if state.json_buffer.is_empty() {
                        "{}".to_string()
                    } else {
                        state.json_buffer
                    };
                    // Validate the assembled JSON and warn if it's malformed
                    // so ops sees the streaming-assembly desync before the
                    // tool dispatcher rejects it. The tool dispatcher keeps
                    // ownership of the user-visible error path: it reports a
                    // structured "Invalid <tool> arguments" back to the LLM
                    // which can then retry with corrected args.
                    if arguments != "{}"
                        && let Err(err) = serde_json::from_str::<serde_json::Value>(&arguments)
                    {
                        let preview: String = arguments.chars().take(160).collect();
                        warn!(
                            target = "llm.anthropic.stream",
                            tool_name = %state.name,
                            tool_call_id = %state.id,
                            error = %err,
                            arguments_preview = %preview,
                            "Anthropic streaming tool-call arguments did not parse as JSON; \
                             forwarding to dispatcher which will return a tool error to the LLM",
                        );
                    }
                    return Ok(Some(LlmStreamChunk {
                        delta: None,
                        usage: None,
                        tool_calls: vec![LlmToolCall {
                            id: state.id,
                            call_type: "function".to_string(),
                            function: LlmFunctionCall { name: state.name, arguments },
                        }],
                        thinking: None,
                    }));
                }
            }
            "message_delta" => {
                let usage = response.usage.and_then(|u| anthropic_usage(Some(u)));
                let is_end = response.delta.as_ref().and_then(|d| d.stop_reason.as_deref()).is_some();
                if usage.is_some() || is_end {
                    return Ok(Some(LlmStreamChunk { delta: None, usage, tool_calls: Vec::new(), thinking: None }));
                }
            }
            _ => {}
        }
    }
    Ok(None)
}

/// Merge usage fragments into an accumulator, summing token counts and merging details when present.
pub fn accumulate_usage(into: &mut Option<LlmUsage>, fragment: Option<LlmUsage>) {
    if let Some(add) = fragment {
        match into {
            Some(existing) => {
                existing.prompt_tokens += add.prompt_tokens;
                existing.completion_tokens += add.completion_tokens;
                existing.total_tokens = existing.prompt_tokens + existing.completion_tokens;
                match (&mut existing.completion_tokens_details, add.completion_tokens_details) {
                    (Some(dst), Some(src)) => {
                        if let Some(tokens) = src.reasoning_tokens {
                            dst.reasoning_tokens = Some(dst.reasoning_tokens.unwrap_or(0) + tokens);
                        }
                        if let Some(tokens) = src.audio_tokens {
                            dst.audio_tokens = Some(dst.audio_tokens.unwrap_or(0) + tokens);
                        }
                    }
                    (None, Some(src)) => existing.completion_tokens_details = Some(src),
                    _ => {}
                }
                match (&mut existing.prompt_tokens_details, add.prompt_tokens_details) {
                    (Some(dst), Some(src)) => {
                        if let Some(tokens) = src.cached_tokens {
                            dst.cached_tokens = Some(dst.cached_tokens.unwrap_or(0) + tokens);
                        }
                        if let Some(tokens) = src.audio_tokens {
                            dst.audio_tokens = Some(dst.audio_tokens.unwrap_or(0) + tokens);
                        }
                    }
                    (None, Some(src)) => existing.prompt_tokens_details = Some(src),
                    _ => {}
                }
            }
            None => {
                *into = Some(add);
            }
        }
    }
}

/// Build a ToolResult message for a single tool call, writing the result into the function arguments.
pub fn tool_result_message(call: &LlmToolCall, content: String) -> LlmMessage {
    let mut call_with_result = call.clone();
    call_with_result.function.arguments = content.clone();
    LlmMessage {
        role: LlmMessageRole::User,
        content,
        kind: LlmMessageKind::ToolResult { calls: vec![call_with_result] },
    }
}

fn provider_chat_messages(invocation: &LlmInvocation) -> Vec<ChatMessage> {
    provider_messages(invocation).into_iter().map(|message| message.to_chat_message()).collect()
}

fn provider_messages(invocation: &LlmInvocation) -> Vec<LlmMessage> {
    let mut messages = compacted_provider_messages(invocation);
    inject_turn_context_into_provider_messages(&mut messages, invocation.turn_context.as_deref());
    messages
}

pub(crate) fn compacted_provider_messages(invocation: &LlmInvocation) -> Vec<LlmMessage> {
    let last_tool_use_index = invocation
        .conversation
        .iter()
        .enumerate()
        .rev()
        .find_map(|(idx, message)| matches!(message.kind, LlmMessageKind::ToolUse { .. }).then_some(idx));

    // Track whether we've seen a ToolUse (assistant with tool_calls) so we can
    // drop orphaned ToolResult messages that appear before any ToolUse.
    // OpenAI requires every tool-role message to follow an assistant message
    // with tool_calls; orphaned results cause a 400 error.
    let mut seen_tool_use = false;

    invocation
        .conversation
        .iter()
        .enumerate()
        .filter_map(|(idx, message)| {
            if matches!(message.kind, LlmMessageKind::ToolUse { .. }) {
                seen_tool_use = true;
            }
            // Drop ToolResult messages that appear before any ToolUse
            if matches!(message.kind, LlmMessageKind::ToolResult { .. }) && !seen_tool_use {
                return None;
            }
            let message = compact_tool_result_message(message);
            if Some(idx) == last_tool_use_index {
                Some(message)
            } else {
                Some(compact_tool_use_message(&message))
            }
        })
        .collect()
}

fn format_turn_context_prefix(turn_context: Option<&str>) -> Option<String> {
    turn_context
        .map(str::trim)
        .filter(|turn_context| !turn_context.is_empty())
        .map(|turn_context| format!("<eden-context>\n{turn_context}\n</eden-context>\n\n"))
}

pub(crate) fn inject_turn_context_into_provider_messages(messages: &mut [LlmMessage], turn_context: Option<&str>) {
    let Some(prefix) = format_turn_context_prefix(turn_context) else {
        return;
    };

    if let Some(last_user) = messages
        .iter_mut()
        .rev()
        .find(|message| message.role == LlmMessageRole::User && matches!(message.kind, LlmMessageKind::Text))
    {
        last_user.content = format!("{prefix}{}", last_user.content);
    }
}

fn inject_turn_context_into_anthropic_messages(messages: &mut [AnthropicMessage], turn_context: Option<&str>) {
    let Some(prefix) = format_turn_context_prefix(turn_context) else {
        return;
    };

    if let Some(last_user) = messages.iter_mut().rev().find(|message| {
        message.role == "user" && message.content.iter().any(|block| block.block_type == "text" && block.text.as_deref().is_some())
    }) && let Some(text_block) = last_user.content.iter_mut().find(|block| block.block_type == "text" && block.text.as_deref().is_some())
    {
        let existing = text_block.text.take().unwrap_or_default();
        text_block.text = Some(format!("{prefix}{existing}"));
    }
}

fn compact_tool_result_message(message: &LlmMessage) -> LlmMessage {
    let LlmMessageKind::ToolResult { calls } = &message.kind else {
        return message.clone();
    };

    let mut projected_calls = Vec::with_capacity(calls.len());
    for call in calls {
        let mut projected_call = call.clone();
        let raw_result = if projected_call.function.arguments.is_empty() {
            &message.content
        } else {
            &projected_call.function.arguments
        };
        projected_call.function.arguments = compact_tool_result(raw_result);
        projected_calls.push(projected_call);
    }

    let projected_content = match projected_calls.as_slice() {
        [] => compact_tool_result(&message.content),
        [call] => call.function.arguments.clone(),
        _ => format!(
            "[{}]",
            projected_calls.iter().map(|call| call.function.arguments.as_str()).collect::<Vec<_>>().join(",")
        ),
    };

    LlmMessage {
        role: message.role.clone(),
        content: projected_content,
        kind: LlmMessageKind::ToolResult { calls: projected_calls },
    }
}

fn compact_tool_result(raw: &str) -> String {
    project_tool_result(raw, DEFAULT_TOOL_RESULT_MAX_ROWS, DEFAULT_TOOL_RESULT_MAX_CELLS, DEFAULT_TOOL_RESULT_MAX_BYTES).to_compact_json()
}

fn compact_tool_use_message(message: &LlmMessage) -> LlmMessage {
    let LlmMessageKind::ToolUse { calls } = &message.kind else {
        return message.clone();
    };

    let projected_calls = calls
        .iter()
        .map(|call| {
            let mut projected_call = call.clone();
            projected_call.function.arguments = compact_tool_use_arguments(&call.function.arguments);
            projected_call
        })
        .collect::<Vec<_>>();

    LlmMessage {
        role: message.role.clone(),
        content: compact_text_preview(&message.content, 240),
        kind: LlmMessageKind::ToolUse { calls: projected_calls },
    }
}

fn compact_tool_use_arguments(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
        return serde_json::json!({
            "summary": compact_text_preview(trimmed, 180),
        })
        .to_string();
    };

    match value {
        Value::Object(map) => {
            let projected =
                map.into_iter().take(8).map(|(key, value)| (key, compact_tool_argument_value(value))).collect::<Map<String, Value>>();
            Value::Object(projected).to_string()
        }
        Value::Array(values) => Value::Array(values.into_iter().take(5).map(compact_tool_argument_value).collect()).to_string(),
        other => compact_tool_argument_value(other).to_string(),
    }
}

fn compact_tool_argument_value(value: Value) -> Value {
    match value {
        Value::String(text) => Value::String(compact_text_preview(&text, 120)),
        Value::Array(values) => Value::Array(values.into_iter().take(4).map(compact_tool_argument_value).collect()),
        Value::Object(map) => {
            Value::Object(map.into_iter().take(6).map(|(key, value)| (key, compact_tool_argument_value(value))).collect())
        }
        other => other,
    }
}

fn compact_text_preview(text: &str, max_chars: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        return normalized;
    }

    let truncated = normalized.char_indices().nth(max_chars).map(|(idx, _)| normalized[..idx].trim_end().to_string()).unwrap_or(normalized);
    format!("{truncated}...")
}

#[doc(hidden)]
pub fn normalize_tool_schema_for_provider(provider: LlmProvider, schema: &Value) -> Value {
    if !provider.is_openai_family() {
        return schema.clone();
    }

    let mut normalized = schema.clone();
    sanitize_openai_schema(&mut normalized);
    normalized
}

fn normalized_tools_for_provider(provider: LlmProvider, tools: Vec<llm::chat::Tool>) -> Vec<llm::chat::Tool> {
    tools
        .into_iter()
        .map(|mut tool| {
            tool.function.parameters = normalize_tool_schema_for_provider(provider, &tool.function.parameters);
            tool
        })
        .collect()
}

fn sanitize_openai_schema(schema: &mut Value) {
    let Value::Object(map) = schema else {
        return;
    };

    sanitize_openai_schema_object(map);
}

fn sanitize_openai_schema_object(map: &mut Map<String, Value>) {
    sanitize_schema_map_keyword(map, "properties");
    sanitize_schema_map_keyword(map, "patternProperties");
    sanitize_schema_map_keyword(map, "$defs");
    sanitize_schema_map_keyword(map, "definitions");
    sanitize_schema_map_keyword(map, "dependentSchemas");

    sanitize_single_schema_keyword(map, "additionalProperties");
    sanitize_single_schema_keyword(map, "propertyNames");
    sanitize_single_schema_keyword(map, "contains");
    sanitize_single_schema_keyword(map, "unevaluatedItems");
    sanitize_single_schema_keyword(map, "unevaluatedProperties");
    sanitize_single_schema_keyword(map, "not");
    sanitize_single_schema_keyword(map, "if");
    sanitize_single_schema_keyword(map, "then");
    sanitize_single_schema_keyword(map, "else");

    sanitize_schema_array_keyword(map, "allOf");
    sanitize_schema_array_keyword(map, "anyOf");
    sanitize_schema_array_keyword(map, "oneOf");
    sanitize_schema_array_keyword(map, "prefixItems");

    let additional_items = map.remove("additionalItems");
    sanitize_items_keyword(map, additional_items);
}

fn sanitize_schema_map_keyword(map: &mut Map<String, Value>, keyword: &str) {
    let Some(Value::Object(children)) = map.get_mut(keyword) else {
        return;
    };

    for schema in children.values_mut() {
        sanitize_openai_schema(schema);
    }
}

fn sanitize_single_schema_keyword(map: &mut Map<String, Value>, keyword: &str) {
    let Some(schema) = map.get_mut(keyword) else {
        return;
    };

    sanitize_openai_schema(schema);
}

fn sanitize_schema_array_keyword(map: &mut Map<String, Value>, keyword: &str) {
    let Some(Value::Array(schemas)) = map.get_mut(keyword) else {
        return;
    };

    for schema in schemas {
        sanitize_openai_schema(schema);
    }
}

fn sanitize_items_keyword(map: &mut Map<String, Value>, additional_items: Option<Value>) {
    let Some(items) = map.remove("items") else {
        if map.contains_key("prefixItems")
            && let Some(additional_items) = additional_items
        {
            map.insert("items".to_string(), openai_schema_from_schema_like(additional_items));
        }
        return;
    };

    match items {
        Value::Object(mut schema) => {
            sanitize_openai_schema_object(&mut schema);
            map.insert("items".to_string(), Value::Object(schema));
        }
        Value::Bool(allowed) => {
            map.insert("items".to_string(), openai_schema_from_bool(allowed));
        }
        Value::Array(mut tuple_schemas) => {
            for schema in &mut tuple_schemas {
                sanitize_openai_schema(schema);
            }

            if !map.contains_key("prefixItems") {
                map.insert("prefixItems".to_string(), Value::Array(tuple_schemas));
            }

            if let Some(additional_items) = additional_items {
                map.insert("items".to_string(), openai_schema_from_schema_like(additional_items));
            }
        }
        other => {
            map.insert("items".to_string(), other);
        }
    }
}

fn openai_schema_from_schema_like(schema: Value) -> Value {
    match schema {
        Value::Object(mut map) => {
            sanitize_openai_schema_object(&mut map);
            Value::Object(map)
        }
        Value::Bool(allowed) => openai_schema_from_bool(allowed),
        other => other,
    }
}

fn openai_schema_from_bool(allowed: bool) -> Value {
    if allowed {
        Value::Object(Map::new())
    } else {
        let mut not_map = Map::new();
        not_map.insert("not".to_string(), Value::Object(Map::new()));
        Value::Object(not_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sanitizes_array_items_keyword_for_openai_family_only() {
        let schema = json!({
            "type": "object",
            "properties": {
                "parameters": {
                    "type": "array",
                    "items": true
                }
            }
        });

        let expected = json!({
            "type": "object",
            "properties": {
                "parameters": {
                    "type": "array",
                    "items": {}
                }
            }
        });

        assert_eq!(normalize_tool_schema_for_provider(LlmProvider::OpenAI, &schema), expected);
        assert_eq!(normalize_tool_schema_for_provider(LlmProvider::AzureOpenAI, &schema), expected);
        assert_eq!(normalize_tool_schema_for_provider(LlmProvider::Anthropic, &schema), schema);
        assert_eq!(normalize_tool_schema_for_provider(LlmProvider::Ollama, &schema), schema);
        assert_eq!(normalize_tool_schema_for_provider(LlmProvider::OpenRouter, &schema), schema);
    }

    #[test]
    fn openai_preserves_false_items_semantics() {
        let schema = json!({
            "type": "array",
            "items": false
        });

        assert_eq!(
            normalize_tool_schema_for_provider(LlmProvider::OpenAI, &schema),
            json!({
                "type": "array",
                "items": {
                    "not": {}
                }
            })
        );
    }

    #[test]
    fn openai_converts_legacy_tuple_items_without_losing_constraints() {
        let schema = json!({
            "type": "array",
            "items": [
                { "type": "string" },
                { "type": "integer" }
            ],
            "additionalItems": false
        });

        assert_eq!(
            normalize_tool_schema_for_provider(LlmProvider::OpenAI, &schema),
            json!({
                "type": "array",
                "prefixItems": [
                    { "type": "string" },
                    { "type": "integer" }
                ],
                "items": {
                    "not": {}
                }
            })
        );
    }

    #[test]
    fn openai_preserves_existing_prefix_items_when_legacy_tuple_items_are_present() {
        let schema = json!({
            "type": "array",
            "items": [
                { "type": "string" }
            ],
            "prefixItems": [
                { "type": "integer" }
            ],
            "additionalItems": false
        });

        assert_eq!(
            normalize_tool_schema_for_provider(LlmProvider::OpenAI, &schema),
            json!({
                "type": "array",
                "prefixItems": [
                    { "type": "integer" }
                ],
                "items": {
                    "not": {}
                }
            })
        );
    }

    #[test]
    fn openai_drops_additional_items_for_non_tuple_items() {
        let schema = json!({
            "type": "array",
            "items": {
                "type": "string"
            },
            "additionalItems": false
        });

        assert_eq!(
            normalize_tool_schema_for_provider(LlmProvider::OpenAI, &schema),
            json!({
                "type": "array",
                "items": {
                    "type": "string"
                }
            })
        );
    }

    #[test]
    fn openai_converts_additional_items_when_prefix_items_are_already_present() {
        let schema = json!({
            "type": "array",
            "prefixItems": [
                { "type": "string" }
            ],
            "additionalItems": false
        });

        assert_eq!(
            normalize_tool_schema_for_provider(LlmProvider::OpenAI, &schema),
            json!({
                "type": "array",
                "prefixItems": [
                    { "type": "string" }
                ],
                "items": {
                    "not": {}
                }
            })
        );
    }

    #[test]
    fn openai_leaves_non_schema_items_payloads_untouched() {
        let schema = json!({
            "type": "object",
            "properties": {
                "payload": {
                    "type": "object",
                    "default": {
                        "items": false
                    },
                    "examples": [
                        {
                            "items": false
                        }
                    ]
                }
            }
        });

        assert_eq!(normalize_tool_schema_for_provider(LlmProvider::OpenAI, &schema), schema);
    }

    #[test]
    fn provider_messages_compact_historical_tool_use_arguments() {
        // Use a long string (>120 chars) so compact_tool_argument_value actually truncates it,
        // making the compacted args differ from the original.
        let long_sql = "a]".repeat(100); // 200 chars — exceeds the 120-char compaction threshold
        let old_args = json!({
            "sql": long_sql,
            "filters": {
                "customer": "Acme Corporation International Holdings",
                "region": "north-america"
            },
            "limit": 500
        })
        .to_string();
        let fresh_args = json!({ "table": "orders" }).to_string();

        let old_tool_use = LlmMessage {
            role: LlmMessageRole::Assistant,
            content: "Running a large query against the warehouse to inspect historical orders.".to_string(),
            kind: LlmMessageKind::ToolUse {
                calls: vec![LlmToolCall {
                    id: "call-old".to_string(),
                    call_type: "function".to_string(),
                    function: LlmFunctionCall {
                        name: "orders__execute_query".to_string(),
                        arguments: old_args.clone(),
                    },
                }],
            },
        };
        let fresh_tool_use = LlmMessage {
            role: LlmMessageRole::Assistant,
            content: "Checking the latest table metadata.".to_string(),
            kind: LlmMessageKind::ToolUse {
                calls: vec![LlmToolCall {
                    id: "call-fresh".to_string(),
                    call_type: "function".to_string(),
                    function: LlmFunctionCall {
                        name: "orders__describe_table".to_string(),
                        arguments: fresh_args.clone(),
                    },
                }],
            },
        };

        let invocation = LlmInvocation {
            conversation: vec![
                old_tool_use,
                LlmMessage {
                    role: LlmMessageRole::User,
                    content: "{\"rows\": 500}".to_string(),
                    kind: LlmMessageKind::ToolResult {
                        calls: vec![LlmToolCall {
                            id: "call-old".to_string(),
                            call_type: "function".to_string(),
                            function: LlmFunctionCall {
                                name: "orders__execute_query".to_string(),
                                arguments: "{\"rows\": 500}".to_string(),
                            },
                        }],
                    },
                },
                fresh_tool_use,
            ],
            turn_context: None,
            ..Default::default()
        };

        let provider_messages = provider_messages(&invocation);

        let LlmMessageKind::ToolUse { calls: old_calls } = &provider_messages[0].kind else {
            panic!("expected historical tool use");
        };
        assert_ne!(old_calls[0].function.arguments, old_args);
        assert!(serde_json::from_str::<Value>(&old_calls[0].function.arguments).is_ok());

        let LlmMessageKind::ToolUse { calls: fresh_calls } = &provider_messages[2].kind else {
            panic!("expected fresh tool use");
        };
        assert_eq!(fresh_calls[0].function.arguments, fresh_args);
    }

    #[test]
    fn provider_messages_prepend_turn_context_to_last_user_text_message() {
        let invocation = LlmInvocation {
            conversation: vec![
                LlmMessage {
                    role: LlmMessageRole::User,
                    content: "Earlier question".to_string(),
                    kind: LlmMessageKind::Text,
                },
                LlmMessage {
                    role: LlmMessageRole::Assistant,
                    content: "Previous answer".to_string(),
                    kind: LlmMessageKind::Text,
                },
                LlmMessage {
                    role: LlmMessageRole::User,
                    content: "Latest question".to_string(),
                    kind: LlmMessageKind::Text,
                },
            ],
            turn_context: Some("Route hints go here".to_string()),
            ..Default::default()
        };

        let messages = provider_messages(&invocation);

        assert_eq!(messages[0].content, "Earlier question");
        assert!(messages[2].content.starts_with("<eden-context>\nRoute hints go here\n</eden-context>\n\nLatest question"));
    }

    #[test]
    fn anthropic_messages_prepend_turn_context_to_last_user_text_block() {
        let invocation = LlmInvocation {
            conversation: vec![
                LlmMessage {
                    role: LlmMessageRole::User,
                    content: "{\"rows\": 12}".to_string(),
                    kind: LlmMessageKind::ToolResult {
                        calls: vec![LlmToolCall {
                            id: "call-1".to_string(),
                            call_type: "function".to_string(),
                            function: LlmFunctionCall {
                                name: "orders__execute_query".to_string(),
                                arguments: "{\"rows\": 12}".to_string(),
                            },
                        }],
                    },
                },
                LlmMessage {
                    role: LlmMessageRole::User,
                    content: "Inspect the latest orders".to_string(),
                    kind: LlmMessageKind::Text,
                },
            ],
            turn_context: Some("Semantic brief".to_string()),
            ..Default::default()
        };

        let messages = anthropic_messages_with_cache(&invocation, false, 0);

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, "user");
        assert_eq!(
            messages[0].content[0].text.as_deref(),
            Some("<eden-context>\nSemantic brief\n</eden-context>\n\nInspect the latest orders")
        );
    }

    // -------------------------------------------------------------
    // Anthropic SSE byte-split fuzz
    //
    // A server's TCP chunk boundaries are not aligned with SSE
    // event boundaries, nor with UTF-8 character boundaries. The
    // buffering layer in `create_anthropic_tool_stream` has to
    // handle a chunk that lands mid-UTF-8, mid-event, or both, and
    // still assemble the same tool-call arguments as the "delivered
    // in one chunk" case. These tests exercise the buffering /
    // event-extraction helpers over every possible split of a
    // representative transcript to catch off-by-one assembly bugs.
    // -------------------------------------------------------------

    /// Drive the buffer state machine with a pre-chunked byte
    /// stream and return the assembled tool-call arguments in call
    /// order. Returns one entry per successfully assembled tool
    /// call (empty if the transcript contains none).
    fn collect_tool_call_arguments(chunks: &[&[u8]]) -> Vec<(String, String)> {
        let mut buffer = String::new();
        let mut utf8_buffer: Vec<u8> = Vec::new();
        let mut tool_states: HashMap<usize, AnthropicToolUseState> = HashMap::new();
        let mut assembled = Vec::new();
        for chunk in chunks {
            buffer_anthropic_sse_bytes(&mut buffer, &mut utf8_buffer, chunk);
            for event in extract_sse_events(&mut buffer) {
                if let Ok(Some(stream_chunk)) = parse_anthropic_sse_chunk_with_tools(&event, &mut tool_states) {
                    for tool_call in stream_chunk.tool_calls {
                        assembled.push((tool_call.function.name, tool_call.function.arguments));
                    }
                }
            }
        }
        assembled
    }

    fn synthetic_anthropic_tool_stream() -> String {
        // One complete Anthropic SSE transcript with a tool call
        // assembled from three `input_json_delta` fragments. Empty
        // lines are terminators — the outer buffer splits on
        // `\n\n`. Text content is intentionally tiny; the point is
        // the assembly shape, not realistic payload size.
        [
            "event: message_start",
            r#"data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":1}}}"#,
            "",
            "event: content_block_start",
            r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"execute_query"}}"#,
            "",
            "event: content_block_delta",
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"rows\":"}}"#,
            "",
            "event: content_block_delta",
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":" 12, \"table"}}"#,
            "",
            "event: content_block_delta",
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\": \"orders\"}"}}"#,
            "",
            "event: content_block_stop",
            r#"data: {"type":"content_block_stop","index":0}"#,
            "",
            "event: message_delta",
            r#"data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"input_tokens":10,"output_tokens":12}}"#,
            "",
        ]
        .join("\n")
    }

    #[test]
    fn anthropic_stream_delivers_full_tool_call_when_chunked_as_one_blob() {
        let transcript = synthetic_anthropic_tool_stream();
        let calls = collect_tool_call_arguments(&[transcript.as_bytes()]);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "execute_query");
        assert_eq!(calls[0].1, "{\"rows\": 12, \"table\": \"orders\"}");
    }

    #[test]
    fn anthropic_stream_buffer_handles_arbitrary_byte_splits() {
        let transcript = synthetic_anthropic_tool_stream();
        let bytes = transcript.as_bytes();

        // Exhaustively split the transcript at every possible byte
        // boundary into exactly two chunks. Each split must
        // reassemble the same tool-call arguments as the single
        // blob case.
        let baseline = collect_tool_call_arguments(&[bytes]);
        assert_eq!(baseline.len(), 1, "baseline must produce exactly one tool call");

        for split in 0..=bytes.len() {
            let first = &bytes[..split];
            let second = &bytes[split..];
            let calls = collect_tool_call_arguments(&[first, second]);
            assert_eq!(
                calls,
                baseline,
                "split={split} ({:02x} / {:02x}): assembled tool calls do not match baseline",
                first.last().copied().unwrap_or(0),
                second.first().copied().unwrap_or(0)
            );
        }
    }

    #[test]
    fn anthropic_stream_buffer_handles_three_way_byte_splits_near_event_boundaries() {
        // Three-chunk split stress test. Rather than O(n^2) which
        // would be prohibitive for a 900-byte transcript, probe
        // every third split relative to each SSE event terminator.
        // Catches bugs where a chunk boundary lands inside a JSON
        // string, inside a `data: ` prefix, or between `\n` and
        // the second `\n` of an event terminator.
        let transcript = synthetic_anthropic_tool_stream();
        let bytes = transcript.as_bytes();
        let baseline = collect_tool_call_arguments(&[bytes]);

        // Probe split points near every `\n\n`, on either side and
        // exactly on the boundary.
        let mut interesting: Vec<usize> = Vec::new();
        let mut cursor = 0;
        while let Some(position) = transcript[cursor..].find("\n\n") {
            let absolute = cursor + position;
            // before, exactly at the first \n, at the second \n, after.
            for offset in [0_usize, 1, 2] {
                if absolute + offset <= bytes.len() {
                    interesting.push(absolute + offset);
                }
            }
            cursor = absolute + 2;
        }
        // Throw in a handful of mid-JSON boundary offsets too.
        for i in (0..bytes.len()).step_by(37) {
            interesting.push(i);
        }
        interesting.sort();
        interesting.dedup();

        for first_split in &interesting {
            for second_split in &interesting {
                if second_split <= first_split || *second_split > bytes.len() {
                    continue;
                }
                let a = &bytes[..*first_split];
                let b = &bytes[*first_split..*second_split];
                let c = &bytes[*second_split..];
                let calls = collect_tool_call_arguments(&[a, b, c]);
                assert_eq!(
                    calls, baseline,
                    "3-way split at ({first_split}, {second_split}) did not reassemble the expected tool call"
                );
            }
        }
    }

    #[test]
    fn anthropic_stream_buffer_preserves_multibyte_across_chunk_split() {
        // Build a tool call whose partial JSON includes a
        // multi-byte UTF-8 codepoint, then split the byte stream
        // mid-codepoint. The buffering layer must hold back the
        // incomplete bytes until the continuation arrives, and the
        // assembled arguments must still be the complete string.
        // `é` as raw UTF-8 bytes in the transcript (not JSON `\u00e9`
        // escapes) so the buffer state machine has to carry bytes
        // across the split.
        let delta_line = "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"note\\\": \\\"café été\\\"}\"}}";
        let transcript = [
            "event: content_block_start",
            r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_u","name":"note"}}"#,
            "",
            "event: content_block_delta",
            delta_line,
            "",
            "event: content_block_stop",
            r#"data: {"type":"content_block_stop","index":0}"#,
            "",
            // Trailing empty line so the `content_block_stop`
            // event is followed by a `\n\n` terminator and the
            // outer buffer flushes it.
            "",
        ]
        .join("\n");
        let bytes = transcript.as_bytes();

        // Find a multi-byte character position to split inside.
        // `é` is 0xc3 0xa9 in UTF-8.
        let split_at =
            bytes.windows(2).position(|w| w == [0xc3, 0xa9]).expect("transcript should contain at least one `é` (0xc3 0xa9)") + 1; // land between the two bytes of `é`.

        let calls = collect_tool_call_arguments(&[&bytes[..split_at], &bytes[split_at..]]);
        assert_eq!(calls.len(), 1, "tool call must be assembled across the mid-codepoint split");
        assert_eq!(calls[0].0, "note");
        // The assembled string arrives as JSON with the original
        // unicode characters intact.
        assert!(
            calls[0].1.contains("café été"),
            "assembled arguments should contain the full multi-byte string, got {:?}",
            calls[0].1
        );
    }
}
