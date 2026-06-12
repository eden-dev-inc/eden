use crate::types::{
    LlmChatResponse, LlmFunctionCall, LlmInvocation, LlmMessage, LlmMessageKind, LlmMessageRole, LlmRequestOverrides, LlmToolCall,
    LlmToolChoice, LlmToolDefinition, LlmUsage,
};
use error::EpError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAiChatCompletionRequest {
    pub model: String,
    #[serde(default)]
    pub messages: Vec<OpenAiChatMessage>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    #[serde(default)]
    pub tools: Vec<LlmToolDefinition>,
    #[serde(default)]
    pub tool_choice: Option<OpenAiToolChoice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAiChatMessage {
    pub role: String,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub tool_calls: Vec<OpenAiToolCall>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAiToolCall {
    pub id: String,
    #[serde(rename = "type", default = "default_function_tool_type")]
    pub call_type: String,
    pub function: OpenAiFunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAiFunctionCall {
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OpenAiToolChoice {
    Named(OpenAiNamedToolChoice),
    Mode(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAiNamedToolChoice {
    #[serde(rename = "type")]
    pub call_type: String,
    pub function: OpenAiToolChoiceFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAiToolChoiceFunction {
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiChatCompletionResponse {
    pub id: String,
    pub object: &'static str,
    pub created: i64,
    pub model: String,
    pub choices: Vec<OpenAiChatCompletionChoice>,
    pub usage: OpenAiUsage,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiChatCompletionChoice {
    pub index: u32,
    pub message: OpenAiAssistantMessage,
    pub finish_reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiAssistantMessage {
    pub role: &'static str,
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<OpenAiToolCall>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiChatCompletionChunk {
    pub id: String,
    pub object: &'static str,
    pub created: i64,
    pub model: String,
    pub choices: Vec<OpenAiChatCompletionChunkChoice>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiChatCompletionChunkChoice {
    pub index: u32,
    pub delta: OpenAiChatCompletionChunkDelta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct OpenAiChatCompletionChunkDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<OpenAiChunkToolCall>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiChunkToolCall {
    pub index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub call_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<OpenAiFunctionCall>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiErrorEnvelope {
    pub error: OpenAiErrorBody,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiErrorBody {
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: String,
    pub code: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenAiResponsesRequest {
    pub model: String,
    #[serde(default)]
    pub input: Option<OpenAiResponsesInput>,
    #[serde(default)]
    pub instructions: Option<String>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    #[serde(default)]
    pub tools: Vec<Value>,
    #[serde(default)]
    pub tool_choice: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OpenAiResponsesInput {
    Text(String),
    Items(Vec<OpenAiResponsesInputItem>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenAiResponsesInputItem {
    #[serde(rename = "type", default)]
    pub item_type: Option<String>,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub call_id: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub content: Option<OpenAiResponsesContent>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub arguments: Option<String>,
    #[serde(default)]
    pub output: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OpenAiResponsesContent {
    Text(String),
    Parts(Vec<OpenAiResponsesContentPart>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenAiResponsesContentPart {
    #[serde(rename = "type", default)]
    pub part_type: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiResponsesResponse {
    pub id: String,
    pub object: &'static str,
    pub created_at: i64,
    pub status: &'static str,
    pub model: String,
    pub output: Vec<OpenAiResponsesOutputItem>,
    pub output_text: String,
    pub usage: OpenAiResponsesUsage,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum OpenAiResponsesOutputItem {
    Message(OpenAiResponsesMessageOutput),
    FunctionCall(OpenAiResponsesFunctionCallOutput),
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiResponsesMessageOutput {
    pub id: String,
    #[serde(rename = "type")]
    pub item_type: &'static str,
    pub status: &'static str,
    pub role: &'static str,
    pub content: Vec<OpenAiResponsesOutputContent>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiResponsesOutputContent {
    #[serde(rename = "type")]
    pub content_type: &'static str,
    pub text: String,
    pub annotations: Vec<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiResponsesFunctionCallOutput {
    pub id: String,
    #[serde(rename = "type")]
    pub item_type: &'static str,
    pub status: &'static str,
    pub call_id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAiResponsesUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub total_tokens: u32,
}

fn default_function_tool_type() -> String {
    "function".to_string()
}

pub fn openai_request_to_invocation(request: &OpenAiChatCompletionRequest) -> Result<LlmInvocation, EpError> {
    let mut system_prompt_parts = Vec::new();
    let mut conversation = Vec::new();
    let mut prior_tool_calls: HashMap<String, LlmToolCall> = HashMap::new();

    for message in &request.messages {
        match message.role.as_str() {
            "system" => {
                if let Some(content) = message.content.as_deref() {
                    let trimmed = content.trim();
                    if !trimmed.is_empty() {
                        system_prompt_parts.push(trimmed.to_string());
                    }
                }
            }
            "user" => {
                conversation.push(LlmMessage {
                    role: LlmMessageRole::User,
                    content: message.content.clone().unwrap_or_default(),
                    kind: LlmMessageKind::Text,
                });
            }
            "assistant" => {
                let tool_calls = message.tool_calls.iter().cloned().map(openai_tool_call_to_llm).collect::<Vec<_>>();
                for call in &tool_calls {
                    prior_tool_calls.insert(call.id.clone(), call.clone());
                }

                let kind = if tool_calls.is_empty() {
                    LlmMessageKind::Text
                } else {
                    LlmMessageKind::ToolUse { calls: tool_calls }
                };

                conversation.push(LlmMessage {
                    role: LlmMessageRole::Assistant,
                    content: message.content.clone().unwrap_or_default(),
                    kind,
                });
            }
            "tool" => {
                let Some(tool_call_id) = message.tool_call_id.as_deref() else {
                    return Err(EpError::request("tool messages must include tool_call_id"));
                };

                let Some(mut tool_call) = prior_tool_calls.get(tool_call_id).cloned() else {
                    return Err(EpError::request("tool message references an unknown tool_call_id"));
                };
                let content = message.content.clone().unwrap_or_default();
                tool_call.function.arguments = content.clone();

                conversation.push(LlmMessage {
                    role: LlmMessageRole::User,
                    content,
                    kind: LlmMessageKind::ToolResult { calls: vec![tool_call] },
                });
            }
            other => {
                return Err(EpError::request(format!("unsupported OpenAI message role `{other}`")));
            }
        }
    }

    Ok(LlmInvocation {
        conversation_id: None,
        conversation,
        tools: request.tools.clone(),
        tool_choice: request.tool_choice.clone().map(openai_tool_choice_to_llm).transpose()?,
        system_prompt: if system_prompt_parts.is_empty() {
            None
        } else {
            Some(system_prompt_parts.join("\n\n"))
        },
        system_prompt_blocks: None,
        turn_context: None,
        overrides: LlmRequestOverrides {
            model: Some(request.model.trim().to_string()),
            max_tokens: request.max_tokens,
            temperature: request.temperature,
            top_p: request.top_p,
            top_k: None,
            thinking_budget: None,
        },
        response_format: None,
        parallel_tool_calls: None,
        tool_connections: Vec::new(),
        tool_endpoint_uuids: Vec::new(),
    })
}

pub fn openai_tool_call_to_llm(value: OpenAiToolCall) -> LlmToolCall {
    LlmToolCall {
        id: value.id,
        call_type: value.call_type,
        function: LlmFunctionCall {
            name: value.function.name,
            arguments: value.function.arguments,
        },
    }
}

pub fn llm_tool_call_to_openai(value: LlmToolCall) -> OpenAiToolCall {
    OpenAiToolCall {
        id: value.id,
        call_type: value.call_type,
        function: OpenAiFunctionCall {
            name: value.function.name,
            arguments: value.function.arguments,
        },
    }
}

pub fn openai_tool_choice_to_llm(value: OpenAiToolChoice) -> Result<LlmToolChoice, EpError> {
    match value {
        OpenAiToolChoice::Mode(mode) => match mode.as_str() {
            "auto" => Ok(LlmToolChoice::Auto),
            "none" => Ok(LlmToolChoice::None),
            "required" => Ok(LlmToolChoice::Any),
            other => Err(EpError::request(format!("unsupported tool_choice `{other}`"))),
        },
        OpenAiToolChoice::Named(choice) => {
            if choice.call_type != "function" {
                return Err(EpError::request("only function tool_choice is supported"));
            }
            Ok(LlmToolChoice::Tool { name: choice.function.name })
        }
    }
}

impl OpenAiResponsesRequest {
    pub fn into_chat_completion_request(self) -> Result<OpenAiChatCompletionRequest, EpError> {
        if self.stream {
            return Err(EpError::request("Responses streaming is not supported by the Eden LLM gateway yet"));
        }

        let mut messages = Vec::new();
        if let Some(instructions) = self.instructions.as_deref().map(str::trim).filter(|value| !value.is_empty()) {
            messages.push(OpenAiChatMessage {
                role: "system".to_string(),
                content: Some(instructions.to_string()),
                tool_calls: Vec::new(),
                tool_call_id: None,
            });
        }

        let input = self.input.ok_or_else(|| EpError::request("Responses requests must include input"))?;
        match input {
            OpenAiResponsesInput::Text(text) => {
                messages.push(OpenAiChatMessage {
                    role: "user".to_string(),
                    content: Some(text),
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                });
            }
            OpenAiResponsesInput::Items(items) => {
                if items.is_empty() {
                    return Err(EpError::request("Responses input array must not be empty"));
                }
                for item in items {
                    messages.push(responses_input_item_to_chat_message(item)?);
                }
            }
        }

        Ok(OpenAiChatCompletionRequest {
            model: self.model,
            messages,
            stream: false,
            temperature: self.temperature,
            max_tokens: self.max_output_tokens.or(self.max_tokens),
            top_p: self.top_p,
            tools: responses_tools_to_chat_tools(self.tools)?,
            tool_choice: self.tool_choice.map(responses_tool_choice_to_chat).transpose()?,
        })
    }
}

fn responses_input_item_to_chat_message(item: OpenAiResponsesInputItem) -> Result<OpenAiChatMessage, EpError> {
    match item.item_type.as_deref().unwrap_or("message") {
        "message" => {
            let role = item
                .role
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| EpError::request("Responses message input items must include role"))?
                .to_string();
            let content = item.content.map(responses_content_to_text).transpose()?.unwrap_or_default();
            Ok(OpenAiChatMessage {
                role,
                content: Some(content),
                tool_calls: Vec::new(),
                tool_call_id: None,
            })
        }
        "function_call" => {
            let call_id = item.call_id.or(item.id).ok_or_else(|| EpError::request("Responses function_call items must include call_id"))?;
            let name = item.name.ok_or_else(|| EpError::request("Responses function_call items must include name"))?;
            let arguments = item.arguments.unwrap_or_default();
            Ok(OpenAiChatMessage {
                role: "assistant".to_string(),
                content: None,
                tool_calls: vec![OpenAiToolCall {
                    id: call_id,
                    call_type: "function".to_string(),
                    function: OpenAiFunctionCall { name, arguments },
                }],
                tool_call_id: None,
            })
        }
        "function_call_output" => {
            let call_id =
                item.call_id.or(item.id).ok_or_else(|| EpError::request("Responses function_call_output items must include call_id"))?;
            Ok(OpenAiChatMessage {
                role: "tool".to_string(),
                content: Some(item.output.unwrap_or_default()),
                tool_calls: Vec::new(),
                tool_call_id: Some(call_id),
            })
        }
        other => Err(EpError::request(format!(
            "Responses input item type `{other}` is not supported by the Eden LLM gateway yet"
        ))),
    }
}

fn responses_content_to_text(content: OpenAiResponsesContent) -> Result<String, EpError> {
    match content {
        OpenAiResponsesContent::Text(text) => Ok(text),
        OpenAiResponsesContent::Parts(parts) => {
            let mut text_parts = Vec::new();
            for part in parts {
                match part.part_type.as_deref().unwrap_or("text") {
                    "input_text" | "output_text" | "text" => {
                        text_parts.push(part.text.unwrap_or_default());
                    }
                    other => {
                        return Err(EpError::request(format!(
                            "Responses content part type `{other}` is not supported by the Eden LLM gateway yet"
                        )));
                    }
                }
            }
            Ok(text_parts.join("\n"))
        }
    }
}

fn responses_tools_to_chat_tools(tools: Vec<Value>) -> Result<Vec<LlmToolDefinition>, EpError> {
    tools
        .into_iter()
        .map(|tool| {
            let tool_type = tool.get("type").and_then(Value::as_str).unwrap_or("function");
            if tool_type != "function" {
                return Err(EpError::request(format!(
                    "Responses tool type `{tool_type}` is not supported by the Eden LLM gateway yet"
                )));
            }
            serde_json::from_value(tool).map_err(|err| EpError::request(format!("invalid Responses function tool: {err}")))
        })
        .collect()
}

fn responses_tool_choice_to_chat(value: Value) -> Result<OpenAiToolChoice, EpError> {
    serde_json::from_value(value).map_err(|err| EpError::request(format!("unsupported Responses tool_choice: {err}")))
}

pub fn request_mentions_tools(request: &OpenAiChatCompletionRequest) -> bool {
    !request.tools.is_empty() || request.messages.iter().any(|message| message.role == "tool" || !message.tool_calls.is_empty())
}

pub fn openai_response_from_llm(id: String, response: LlmChatResponse, created: i64) -> OpenAiChatCompletionResponse {
    let finish_reason = match &response.message.kind {
        LlmMessageKind::ToolUse { calls } if !calls.is_empty() => "tool_calls",
        _ => "stop",
    }
    .to_string();

    let tool_calls = match response.message.kind {
        LlmMessageKind::ToolUse { calls } => calls.into_iter().map(llm_tool_call_to_openai).collect(),
        _ => Vec::new(),
    };

    OpenAiChatCompletionResponse {
        id,
        object: "chat.completion",
        created,
        model: response.provider.model.clone(),
        choices: vec![OpenAiChatCompletionChoice {
            index: 0,
            message: OpenAiAssistantMessage {
                role: "assistant",
                content: if response.message.content.is_empty() && !tool_calls.is_empty() {
                    None
                } else {
                    Some(response.message.content)
                },
                tool_calls,
            },
            finish_reason,
        }],
        usage: openai_usage_from_option(response.usage.as_ref()),
    }
}

pub fn openai_responses_response_from_chat(response: OpenAiChatCompletionResponse) -> OpenAiResponsesResponse {
    let response_id = new_openai_response_id();
    let mut output_text = String::new();
    let mut output = Vec::new();

    if let Some(choice) = response.choices.into_iter().next() {
        output_text = choice.message.content.unwrap_or_default();
        output.push(OpenAiResponsesOutputItem::Message(OpenAiResponsesMessageOutput {
            id: format!("msg_{}", Uuid::new_v4().simple()),
            item_type: "message",
            status: "completed",
            role: "assistant",
            content: vec![OpenAiResponsesOutputContent {
                content_type: "output_text",
                text: output_text.clone(),
                annotations: Vec::new(),
            }],
        }));
        for call in choice.message.tool_calls {
            output.push(OpenAiResponsesOutputItem::FunctionCall(OpenAiResponsesFunctionCallOutput {
                id: format!("fc_{}", Uuid::new_v4().simple()),
                item_type: "function_call",
                status: "completed",
                call_id: call.id,
                name: call.function.name,
                arguments: call.function.arguments,
            }));
        }
    }

    OpenAiResponsesResponse {
        id: response_id,
        object: "response",
        created_at: response.created,
        status: "completed",
        model: response.model,
        output,
        output_text,
        usage: OpenAiResponsesUsage {
            input_tokens: response.usage.prompt_tokens,
            output_tokens: response.usage.completion_tokens,
            total_tokens: response.usage.total_tokens,
        },
    }
}

pub fn build_final_llm_message(content: String, tool_calls: &[LlmToolCall]) -> LlmMessage {
    if tool_calls.is_empty() {
        LlmMessage {
            role: LlmMessageRole::Assistant,
            content,
            kind: LlmMessageKind::Text,
        }
    } else {
        LlmMessage {
            role: LlmMessageRole::Assistant,
            content,
            kind: LlmMessageKind::ToolUse { calls: tool_calls.to_vec() },
        }
    }
}

pub fn openai_usage_from_option(usage: Option<&LlmUsage>) -> OpenAiUsage {
    OpenAiUsage {
        prompt_tokens: usage.map(|usage| usage.prompt_tokens).unwrap_or_default(),
        completion_tokens: usage.map(|usage| usage.completion_tokens).unwrap_or_default(),
        total_tokens: usage.map(|usage| usage.total_tokens).unwrap_or_default(),
    }
}

pub fn new_openai_completion_id() -> String {
    format!("chatcmpl-{}", Uuid::new_v4().simple())
}

pub fn new_openai_response_id() -> String {
    format!("resp_{}", Uuid::new_v4().simple())
}

pub fn openai_error_envelope(message: &str, error_type: &str, code: &str) -> OpenAiErrorEnvelope {
    OpenAiErrorEnvelope {
        error: OpenAiErrorBody {
            message: message.to_string(),
            error_type: error_type.to_string(),
            code: code.to_string(),
        },
    }
}

pub fn format_openai_stream_chunk<T: Serialize>(value: &T) -> String {
    match serde_json::to_string(value) {
        Ok(body) => format!("data: {body}\n\n"),
        Err(_) => format_openai_stream_error("failed to serialize stream chunk", "server_error", "serialization_error"),
    }
}

pub fn format_openai_stream_error(message: &str, error_type: &str, code: &str) -> String {
    match serde_json::to_string(&openai_error_envelope(message, error_type, code)) {
        Ok(body) => format!("data: {body}\n\n"),
        Err(_) => String::from(
            "data: {\"error\":{\"message\":\"request failed\",\"type\":\"server_error\",\"code\":\"serialization_error\"}}\n\n",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> OpenAiChatCompletionRequest {
        OpenAiChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![
                OpenAiChatMessage {
                    role: "system".to_string(),
                    content: Some("You are helpful.".to_string()),
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                },
                OpenAiChatMessage {
                    role: "user".to_string(),
                    content: Some("Hello".to_string()),
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                },
            ],
            stream: false,
            temperature: Some(0.7),
            max_tokens: Some(32),
            top_p: Some(0.95),
            tools: Vec::new(),
            tool_choice: None,
        }
    }

    #[test]
    fn translates_openai_request_to_invocation() {
        let invocation = openai_request_to_invocation(&sample_request()).expect("request should translate");
        assert_eq!(invocation.overrides.model.as_deref(), Some("gpt-4o"));
        assert_eq!(invocation.system_prompt.as_deref(), Some("You are helpful."));
        assert_eq!(invocation.conversation.len(), 1);
        assert_eq!(invocation.conversation[0].content, "Hello");
    }

    #[test]
    fn translates_tool_messages_using_prior_tool_call_metadata() {
        let request = OpenAiChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![
                OpenAiChatMessage {
                    role: "assistant".to_string(),
                    content: None,
                    tool_calls: vec![OpenAiToolCall {
                        id: "call_1".to_string(),
                        call_type: "function".to_string(),
                        function: OpenAiFunctionCall {
                            name: "lookup".to_string(),
                            arguments: "{\"id\":1}".to_string(),
                        },
                    }],
                    tool_call_id: None,
                },
                OpenAiChatMessage {
                    role: "tool".to_string(),
                    content: Some("{\"name\":\"eden\"}".to_string()),
                    tool_calls: Vec::new(),
                    tool_call_id: Some("call_1".to_string()),
                },
            ],
            stream: false,
            temperature: None,
            max_tokens: None,
            top_p: None,
            tools: Vec::new(),
            tool_choice: None,
        };

        let invocation = openai_request_to_invocation(&request).expect("tool messages should translate");
        assert_eq!(invocation.conversation.len(), 2);
        match &invocation.conversation[1].kind {
            LlmMessageKind::ToolResult { calls } => {
                assert_eq!(calls[0].id, "call_1");
                assert_eq!(calls[0].function.name, "lookup");
                assert_eq!(calls[0].function.arguments, "{\"name\":\"eden\"}");
            }
            other => panic!("expected ToolResult, got {other:?}"),
        }
    }

    #[test]
    fn translates_responses_text_input_to_chat_request() {
        let request = OpenAiResponsesRequest {
            model: "openrouter/test-model".to_string(),
            input: Some(OpenAiResponsesInput::Text("Analyze this portfolio signal.".to_string())),
            instructions: Some("Answer tersely.".to_string()),
            stream: false,
            temperature: Some(0.2),
            max_output_tokens: Some(128),
            max_tokens: None,
            top_p: None,
            tools: Vec::new(),
            tool_choice: None,
        }
        .into_chat_completion_request()
        .expect("Responses request should translate");

        assert_eq!(request.model, "openrouter/test-model");
        assert_eq!(request.max_tokens, Some(128));
        assert_eq!(request.messages.len(), 2);
        assert_eq!(request.messages[0].role, "system");
        assert_eq!(request.messages[0].content.as_deref(), Some("Answer tersely."));
        assert_eq!(request.messages[1].role, "user");
        assert_eq!(request.messages[1].content.as_deref(), Some("Analyze this portfolio signal."));
    }

    #[test]
    fn translates_responses_message_parts_to_chat_request() {
        let request = OpenAiResponsesRequest {
            model: "gpt-4o".to_string(),
            input: Some(OpenAiResponsesInput::Items(vec![OpenAiResponsesInputItem {
                item_type: Some("message".to_string()),
                id: None,
                call_id: None,
                role: Some("user".to_string()),
                content: Some(OpenAiResponsesContent::Parts(vec![
                    OpenAiResponsesContentPart {
                        part_type: Some("input_text".to_string()),
                        text: Some("First".to_string()),
                    },
                    OpenAiResponsesContentPart {
                        part_type: Some("text".to_string()),
                        text: Some("Second".to_string()),
                    },
                ])),
                name: None,
                arguments: None,
                output: None,
            }])),
            instructions: None,
            stream: false,
            temperature: None,
            max_output_tokens: None,
            max_tokens: Some(64),
            top_p: None,
            tools: Vec::new(),
            tool_choice: None,
        }
        .into_chat_completion_request()
        .expect("Responses message parts should translate");

        assert_eq!(request.max_tokens, Some(64));
        assert_eq!(request.messages.len(), 1);
        assert_eq!(request.messages[0].content.as_deref(), Some("First\nSecond"));
    }

    #[test]
    fn rejects_unsupported_responses_content_parts() {
        let error = OpenAiResponsesRequest {
            model: "gpt-4o".to_string(),
            input: Some(OpenAiResponsesInput::Items(vec![OpenAiResponsesInputItem {
                item_type: Some("message".to_string()),
                id: None,
                call_id: None,
                role: Some("user".to_string()),
                content: Some(OpenAiResponsesContent::Parts(vec![OpenAiResponsesContentPart {
                    part_type: Some("input_image".to_string()),
                    text: None,
                }])),
                name: None,
                arguments: None,
                output: None,
            }])),
            instructions: None,
            stream: false,
            temperature: None,
            max_output_tokens: None,
            max_tokens: None,
            top_p: None,
            tools: Vec::new(),
            tool_choice: None,
        }
        .into_chat_completion_request()
        .expect_err("unsupported Responses content should fail");

        assert!(error.to_string().contains("input_image"));
    }

    #[test]
    fn maps_chat_response_to_responses_shape() {
        let response = openai_responses_response_from_chat(OpenAiChatCompletionResponse {
            id: "chatcmpl-test".to_string(),
            object: "chat.completion",
            created: 123,
            model: "openrouter/test-model".to_string(),
            choices: vec![OpenAiChatCompletionChoice {
                index: 0,
                message: OpenAiAssistantMessage {
                    role: "assistant",
                    content: Some("Ready".to_string()),
                    tool_calls: Vec::new(),
                },
                finish_reason: "stop".to_string(),
            }],
            usage: OpenAiUsage { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
        });

        assert!(response.id.starts_with("resp_"));
        assert_eq!(response.object, "response");
        assert_eq!(response.output_text, "Ready");
        assert_eq!(response.usage.input_tokens, 10);
        assert_eq!(response.usage.output_tokens, 5);
        assert_eq!(response.usage.total_tokens, 15);
    }
}
