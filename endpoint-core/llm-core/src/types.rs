use chrono::{DateTime, Utc};
use format::{EndpointUuid, OrganizationUuid};
use llm::FunctionCall as LlmFunctionCallInner;
use llm::ToolCall as LlmToolCallInner;
use llm::chat::{ChatMessage, ChatMessageBuilder, ChatRole, StructuredOutputFormat, Tool, ToolChoice as LlmToolChoiceInner, Usage};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use utoipa::ToSchema;
use uuid::Uuid;

/// Safety level of a tool call, used to drive auto-approval logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolSafety {
    /// Read-only operation: safe to auto-approve under `auto_allow_reads` policy.
    Safe,
    /// Mutating operation: requires explicit approval unless `auto_allow_all` is set.
    Moderate,
    /// Destructive or privileged operation: always requires explicit approval.
    Dangerous,
}

/// Per-conversation policy for when the backend auto-approves tool calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolApprovalMode {
    /// Every tool call requires explicit user approval.
    #[default]
    Ask,
    /// Read-only tools (GET / list / describe / …) are auto-approved; mutations require approval.
    AutoAllowReads,
    /// All tool calls are auto-approved without user interaction.
    AutoAllowAll,
}

impl fmt::Display for ToolApprovalMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ask => write!(f, "ask"),
            Self::AutoAllowReads => write!(f, "auto_allow_reads"),
            Self::AutoAllowAll => write!(f, "auto_allow_all"),
        }
    }
}

impl From<&str> for ToolApprovalMode {
    fn from(s: &str) -> Self {
        match s {
            "auto_allow_reads" => Self::AutoAllowReads,
            "auto_allow_all" => Self::AutoAllowAll,
            _ => Self::Ask,
        }
    }
}

/// Source that initiated an LLM request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TrafficSource {
    LlmGateway,
    AgentGateway,
    ProxyApp,
    InternalJob,
}

impl TrafficSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LlmGateway => "llm_gateway",
            Self::AgentGateway => "agent_gateway",
            Self::ProxyApp => "proxy_app",
            Self::InternalJob => "internal_job",
        }
    }
}

impl fmt::Display for TrafficSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Action taken by LLM governance or safety policy checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum PolicyAction {
    #[default]
    Allow,
    Redact,
    Block,
    AuditOnly,
}

impl PolicyAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Redact => "redact",
            Self::Block => "block",
            Self::AuditOnly => "audit_only",
        }
    }

    /// Returns the more restrictive of two policy actions.
    pub fn merge(self, other: Self) -> Self {
        if other.rank() > self.rank() { other } else { self }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Allow => 0,
            Self::AuditOnly => 1,
            Self::Redact => 2,
            Self::Block => 3,
        }
    }
}

impl fmt::Display for PolicyAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Gateway response-cache handling for an LLM operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum LlmCacheStatus {
    #[default]
    Bypass,
    Miss,
    Hit,
    Store,
}

impl LlmCacheStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bypass => "bypass",
            Self::Miss => "miss",
            Self::Hit => "hit",
            Self::Store => "store",
        }
    }
}

impl fmt::Display for LlmCacheStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Objective used when the proxy can choose between multiple model routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum LlmRouteOptimizationMode {
    #[default]
    Cost,
    Latency,
    Throughput,
    Balanced,
}

impl LlmRouteOptimizationMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cost => "cost",
            Self::Latency => "latency",
            Self::Throughput => "throughput",
            Self::Balanced => "balanced",
        }
    }
}

impl fmt::Display for LlmRouteOptimizationMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How strongly the proxy should keep conversation affinity for KV/prefix cache reuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum LlmKvCacheMode {
    #[default]
    Disabled,
    Affinity,
    Adaptive,
}

impl LlmKvCacheMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Affinity => "affinity",
            Self::Adaptive => "adaptive",
        }
    }
}

impl fmt::Display for LlmKvCacheMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Conversation route-cache result for KV/prefix cache affinity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum LlmKvCacheStatus {
    #[default]
    Bypass,
    Miss,
    Hit,
    Move,
}

impl LlmKvCacheStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bypass => "bypass",
            Self::Miss => "miss",
            Self::Hit => "hit",
            Self::Move => "move",
        }
    }
}

impl fmt::Display for LlmKvCacheStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Canonical analytics event emitted after a completed LLM operation.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct LlmOperationEvent {
    pub timestamp: DateTime<Utc>,
    pub organization_uuid: OrganizationUuid,
    pub endpoint_uuid: EndpointUuid,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_model: Option<String>,
    pub operation: String,
    pub traffic_source: TrafficSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<String>,
    /// Immutable registry agent (`llm_agents.id`) this request is attributed to.
    /// Canonical attribution key, distinct from the mutable `x-eden-agent-id`
    /// header. `None` for historical rows and non-agent traffic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_uuid: Option<Uuid>,
    pub streaming: bool,
    pub tool_used: bool,
    pub tool_call_count: u32,
    pub message_count: u32,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
    /// Exact bytes of the request/response bodies on the wire. `0` for
    /// historical rows recorded before byte accounting.
    #[serde(default)]
    pub request_bytes: u32,
    #[serde(default)]
    pub response_bytes: u32,
    pub estimated_provider_cost_micros: u64,
    #[serde(default)]
    pub baseline_estimated_cost_micros: u64,
    #[serde(default)]
    pub selected_estimated_cost_micros: u64,
    #[serde(default)]
    pub estimated_arbitrage_savings_micros: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arbitrage_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_source: Option<String>,
    #[serde(default)]
    pub cache_status: LlmCacheStatus,
    #[serde(default)]
    pub estimated_cache_savings_micros: u64,
    #[serde(default)]
    pub route_optimization_mode: LlmRouteOptimizationMode,
    #[serde(default)]
    pub kv_cache_mode: LlmKvCacheMode,
    #[serde(default)]
    pub kv_cache_status: LlmKvCacheStatus,
    #[serde(default)]
    pub estimated_kv_cache_savings_micros: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_move_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_route_key: Option<String>,
    pub latency_ms: u64,
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub policy_action: PolicyAction,
    pub pii_detected: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pii_types: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_fingerprint: Option<String>,
}

/// Role of a message within a conversation.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LlmMessageRole {
    User,
    Assistant,
}

impl From<LlmMessageRole> for ChatRole {
    fn from(value: LlmMessageRole) -> Self {
        match value {
            LlmMessageRole::User => ChatRole::User,
            LlmMessageRole::Assistant => ChatRole::Assistant,
        }
    }
}

impl From<ChatRole> for LlmMessageRole {
    fn from(value: ChatRole) -> Self {
        match value {
            ChatRole::User => LlmMessageRole::User,
            ChatRole::Assistant => LlmMessageRole::Assistant,
        }
    }
}

/// Canonical tool call representation used across providers.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct LlmToolCall {
    pub id: String,
    #[serde(default = "default_tool_call_type")]
    pub call_type: String,
    pub function: LlmFunctionCall,
}

fn default_tool_call_type() -> String {
    "function".to_string()
}

impl From<LlmToolCall> for LlmToolCallInner {
    fn from(value: LlmToolCall) -> Self {
        Self {
            id: value.id,
            call_type: value.call_type,
            function: LlmFunctionCallInner {
                name: value.function.name,
                arguments: value.function.arguments,
            },
        }
    }
}

impl From<LlmToolCallInner> for LlmToolCall {
    fn from(value: LlmToolCallInner) -> Self {
        Self {
            id: value.id,
            call_type: value.call_type,
            function: LlmFunctionCall {
                name: value.function.name,
                arguments: value.function.arguments,
            },
        }
    }
}

/// Canonical function call information for a tool invocation.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct LlmFunctionCall {
    pub name: String,
    pub arguments: String,
}

/// Type of content carried by an LLM message.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[derive(Default)]
pub enum LlmMessageKind {
    #[default]
    Text,
    ImageUrl {
        url: String,
    },
    ToolUse {
        calls: Vec<LlmToolCall>,
    },
    ToolResult {
        calls: Vec<LlmToolCall>,
    },
}

/// Canonical message shape stored and exchanged with LLM endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct LlmMessage {
    pub role: LlmMessageRole,
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub kind: LlmMessageKind,
}

impl LlmMessage {
    pub fn to_chat_message(&self) -> ChatMessage {
        let mut builder: ChatMessageBuilder = match self.role {
            LlmMessageRole::User => ChatMessage::user(),
            LlmMessageRole::Assistant => ChatMessage::assistant(),
        };

        if !self.content.is_empty() {
            builder = builder.content(self.content.clone());
        }

        match &self.kind {
            LlmMessageKind::Text => {}
            LlmMessageKind::ImageUrl { url } => {
                builder = builder.image_url(url.clone());
            }
            LlmMessageKind::ToolUse { calls } => {
                let converted: Vec<LlmToolCallInner> = calls.clone().into_iter().map(Into::into).collect();
                builder = builder.tool_use(converted);
            }
            LlmMessageKind::ToolResult { calls } => {
                let converted: Vec<LlmToolCallInner> = calls.clone().into_iter().map(Into::into).collect();
                builder = builder.tool_result(converted);
            }
        }

        builder.build()
    }
}

/// Canonical function definition for tools exposed to the model.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default, PartialEq)]
pub struct LlmFunctionToolDefinition {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub parameters: Value,
    /// Optional usage example shown in the endpoint catalog to guide the LLM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub example_usage: Option<String>,
}

/// Tool definition exposed to LLMs.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct LlmToolDefinition {
    #[serde(default = "default_tool_type")]
    pub r#type: String,
    pub function: LlmFunctionToolDefinition,
}

fn default_tool_type() -> String {
    "function".to_string()
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default, PartialEq, Eq)]
pub struct ToolAnnotations {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only_hint: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destructive_hint: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotent_hint: Option<bool>,
}

/// Runtime binding metadata for an exposed tool.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct LlmToolBinding {
    pub binding_id: Uuid,
    /// Name presented to the LLM (e.g. `usr-<id>__execute_postgres_query`)
    pub name: String,
    /// Internal client key associated with the tool connection.
    pub client_key: String,
    /// Original tool name reported by the tool server.
    pub remote_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<ToolAnnotations>,
    /// Definition advertised to the LLM.
    pub definition: LlmToolDefinition,
    /// Names of tools commonly used alongside this tool.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub related_tools: Vec<String>,
    /// Describes a common workflow sequence, e.g. "typically called after list_tables".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_hint: Option<String>,
}

impl From<LlmToolDefinition> for Tool {
    fn from(value: LlmToolDefinition) -> Self {
        Tool {
            tool_type: value.r#type,
            function: llm::chat::FunctionTool {
                name: value.function.name,
                description: value.function.description.unwrap_or_default(),
                parameters: value.function.parameters,
            },
            cache_control: None,
        }
    }
}

/// Connection configuration for executing tools.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct LlmToolConnection {
    /// Prefix used in tool names to identify which tool client should handle the call.
    pub client_key: String,
    /// Fully qualified tool endpoint used to communicate with the tool server.
    pub tools_url: String,
    /// Bearer token forwarded to the tool server for authenticated access.
    pub bearer_token: String,
    /// Stable endpoint UUID used for planning and routing when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_uuid: Option<String>,
    /// Human-friendly endpoint identifier (e.g. registered endpoint name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_name: Option<String>,
    /// Optional endpoint description configured by the user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_description: Option<String>,
    /// Eden endpoint kind (e.g. "postgres", "mongo", "redis").
    /// Set for managed Eden endpoints; `None` for externally registered tool servers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_kind: Option<String>,
    #[serde(default)]
    pub trust_annotations: bool,
    /// Only set for server-generated internal routes that are trusted by
    /// construction and must not be blocked by loopback/private-IP SSRF checks.
    #[serde(default, skip_serializing_if = "is_false", skip_deserializing)]
    pub skip_ssrf_validation: bool,
}

impl LlmToolConnection {
    /// Synthesizes a display label combining endpoint name and description.
    pub fn endpoint_label(&self) -> Option<String> {
        let name = self.endpoint_name.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty());
        let description = self.endpoint_description.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty());

        match (name, description) {
            (Some(name), Some(description)) => Some(format!("{name} - {description}")),
            (Some(name), None) => Some(name.to_string()),
            (None, Some(description)) => Some(description.to_string()),
            (None, None) => None,
        }
    }
}

/// Tool choice preferences for the model.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LlmToolChoice {
    Any,
    Auto,
    Tool { name: String },
    None,
}

impl From<LlmToolChoice> for LlmToolChoiceInner {
    fn from(value: LlmToolChoice) -> Self {
        match value {
            LlmToolChoice::Any => LlmToolChoiceInner::Any,
            LlmToolChoice::Auto => LlmToolChoiceInner::Auto,
            LlmToolChoice::Tool { name } => LlmToolChoiceInner::Tool(name),
            LlmToolChoice::None => LlmToolChoiceInner::None,
        }
    }
}

/// Overrides for a single invocation (model parameters etc.).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default, PartialEq)]
pub struct LlmRequestOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<u32>,
    /// Budget for extended thinking (chain-of-thought reasoning).
    /// When set, the provider should allocate this many tokens for
    /// the model's internal reasoning before producing the response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking_budget: Option<u32>,
}

/// Structured output format request.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct LlmStructuredOutputFormat {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

impl From<LlmStructuredOutputFormat> for StructuredOutputFormat {
    fn from(value: LlmStructuredOutputFormat) -> Self {
        StructuredOutputFormat {
            name: value.name,
            description: value.description,
            schema: value.schema,
            strict: value.strict,
        }
    }
}

impl From<StructuredOutputFormat> for LlmStructuredOutputFormat {
    fn from(value: StructuredOutputFormat) -> Self {
        Self {
            name: value.name,
            description: value.description,
            schema: value.schema,
            strict: value.strict,
        }
    }
}

/// Cache hint for prompt or message content.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CacheHint {
    /// Content is constant across requests. Cache as aggressively as possible.
    Stable,
    /// Content stays stable within a conversation/session.
    SessionStable,
    /// Content changes frequently. Do not cache.
    #[default]
    Volatile,
}

impl CacheHint {
    pub fn is_cacheable(self) -> bool {
        matches!(self, Self::Stable | Self::SessionStable)
    }
}

/// System-prompt block category.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SystemPromptBlockKind {
    Identity,
    EndpointCatalog,
    ConversationSummary,
    ConversationPrompt,
    CoreToolPlaybook,
    SkillGroupCatalog,
    ActiveSkillCards,
    SkillChangeNotifications,
    SemanticContext,
}

/// System-prompt block with provider cache hint.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SystemPromptBlock {
    pub kind: SystemPromptBlockKind,
    pub content: String,
    #[serde(default)]
    pub cache_hint: CacheHint,
}

/// Request payload sent to the LLM client for a single invocation.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default, PartialEq)]
pub struct LlmInvocation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub conversation: Vec<LlmMessage>,
    #[serde(default)]
    pub tools: Vec<LlmToolDefinition>,
    #[serde(default)]
    pub tool_choice: Option<LlmToolChoice>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt_blocks: Option<Vec<SystemPromptBlock>>,
    #[serde(default)]
    pub overrides: LlmRequestOverrides,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_format: Option<LlmStructuredOutputFormat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    #[serde(default)]
    pub tool_connections: Vec<LlmToolConnection>,
    #[serde(default)]
    pub tool_endpoint_uuids: Vec<String>,
    /// Per-turn context injected into the user message at the adapter edge.
    /// Not persisted in conversation history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_context: Option<String>,
}

impl LlmInvocation {
    pub fn chat_messages(&self) -> Vec<ChatMessage> {
        self.conversation.iter().map(LlmMessage::to_chat_message).collect()
    }

    pub fn tools(&self) -> Vec<Tool> {
        self.tools.clone().into_iter().map(Into::into).collect()
    }

    pub fn tool_choice(&self) -> Option<LlmToolChoiceInner> {
        self.tool_choice.clone().map(Into::into)
    }

    pub fn structured_output(&self) -> Option<StructuredOutputFormat> {
        self.response_format.clone().map(Into::into)
    }

    pub fn system_prompt_blocks(&self) -> Option<&[SystemPromptBlock]> {
        self.system_prompt_blocks.as_deref()
    }

    pub fn effective_system_prompt(&self) -> Option<String> {
        self.system_prompt_blocks()
            .and_then(Self::flatten_system_prompt_blocks)
            .or_else(|| self.system_prompt.as_ref().map(|prompt| prompt.trim()).filter(|prompt| !prompt.is_empty()).map(str::to_string))
    }

    pub fn flatten_system_prompt_blocks(blocks: &[SystemPromptBlock]) -> Option<String> {
        let prompt = blocks.iter().map(|block| block.content.trim()).filter(|content| !content.is_empty()).collect::<Vec<_>>().join("\n\n");

        if prompt.trim().is_empty() { None } else { Some(prompt) }
    }

    pub fn tool_connections(&self) -> &[LlmToolConnection] {
        &self.tool_connections
    }

    pub fn conversation_id(&self) -> Option<&String> {
        self.conversation_id.as_ref()
    }

    pub fn tool_endpoint_uuids(&self) -> &[String] {
        &self.tool_endpoint_uuids
    }
}

/// Token usage metadata for a completion.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct LlmCompletionTokensDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_tokens: Option<u32>,
}

impl From<llm::chat::CompletionTokensDetails> for LlmCompletionTokensDetails {
    fn from(value: llm::chat::CompletionTokensDetails) -> Self {
        Self {
            reasoning_tokens: value.reasoning_tokens,
            audio_tokens: value.audio_tokens,
        }
    }
}

/// Prompt token metadata.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct LlmPromptTokensDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_tokens: Option<u32>,
}

impl From<llm::chat::PromptTokensDetails> for LlmPromptTokensDetails {
    fn from(value: llm::chat::PromptTokensDetails) -> Self {
        Self {
            cached_tokens: value.cached_tokens,
            audio_tokens: value.audio_tokens,
        }
    }
}

/// Usage information returned by providers.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct LlmUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens_details: Option<LlmCompletionTokensDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens_details: Option<LlmPromptTokensDetails>,
}

impl From<Usage> for LlmUsage {
    fn from(value: Usage) -> Self {
        Self {
            prompt_tokens: value.prompt_tokens,
            completion_tokens: value.completion_tokens,
            total_tokens: value.total_tokens,
            completion_tokens_details: value.completion_tokens_details.map(Into::into),
            prompt_tokens_details: value.prompt_tokens_details.map(Into::into),
        }
    }
}

/// Information about which provider served a request.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct LlmProviderMetadata {
    pub provider: String,
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
}

impl LlmProviderMetadata {
    pub fn new(provider: impl Into<String>, model: impl Into<String>, base_url: Option<String>) -> Self {
        Self { provider: provider.into(), model: model.into(), base_url }
    }
}

/// High-level response returned from the LLM endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct LlmChatResponse {
    pub message: LlmMessage,
    #[serde(default)]
    pub conversation: Vec<LlmMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<LlmUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking: Option<String>,
    pub provider: LlmProviderMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use format::{EndpointUuid, OrganizationUuid};
    use serde_json::json;
    use uuid::Uuid;

    fn sample_operation_event() -> LlmOperationEvent {
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
            tool_used: false,
            tool_call_count: 0,
            message_count: 1,
            prompt_tokens: 10,
            completion_tokens: 5,
            total_tokens: 15,
            request_bytes: 0,
            response_bytes: 0,
            estimated_provider_cost_micros: 0,
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
            latency_ms: 1,
            success: true,
            error_message: None,
            policy_action: PolicyAction::Allow,
            pii_detected: false,
            pii_types: Vec::new(),
            prompt_fingerprint: None,
        }
    }

    #[test]
    fn llm_operation_event_round_trips_agent_uuid() {
        let agent = Uuid::parse_str("11111111-2222-3333-4444-555555555555").expect("valid uuid");
        let mut event = sample_operation_event();
        event.agent_uuid = Some(agent);

        let serialized = serde_json::to_string(&event).expect("event serializes");
        assert!(serialized.contains("agent_uuid"), "agent_uuid is on the wire when set");

        let parsed: LlmOperationEvent = serde_json::from_str(&serialized).expect("event deserializes");
        assert_eq!(parsed.agent_uuid, Some(agent), "agent_uuid round-trips by immutable uuid");
    }

    #[test]
    fn llm_operation_event_agent_uuid_defaults_to_none_for_historical_rows() {
        let event = sample_operation_event();
        assert_eq!(event.agent_uuid, None, "default attribution is None");

        let serialized = serde_json::to_string(&event).expect("event serializes");
        assert!(!serialized.contains("agent_uuid"), "absent agent_uuid is omitted from the wire");

        let parsed: LlmOperationEvent = serde_json::from_str(&serialized).expect("rows without agent_uuid still deserialize");
        assert_eq!(parsed.agent_uuid, None, "missing agent_uuid defaults to None");
    }

    #[test]
    fn tool_connection_ignores_skip_ssrf_validation_from_json() {
        let connection: LlmToolConnection = serde_json::from_value(json!({
            "client_key": "conn",
            "tools_url": "https://example.com/tools",
            "bearer_token": "token",
            "skip_ssrf_validation": true
        }))
        .expect("tool connection should deserialize");

        assert!(!connection.skip_ssrf_validation, "external payloads must not be able to enable SSRF bypass");
    }
}
