#[cfg(embedded_db)]
use std::sync::RwLock;
#[cfg(embedded_db)]
use std::time::Duration;
use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::Arc,
    time::Instant,
};

use once_cell::sync::Lazy;
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
};
use reqwest::Client as HttpClient;
use rmcp::{
    RoleClient,
    model::{
        CallToolRequestParam, CallToolResult, ClientCapabilities, ClientInfo, Content, Implementation, ListToolsResult,
        PaginatedRequestParam, ProtocolVersion, RawContent, Tool as RmcpTool, ToolAnnotations as RmcpToolAnnotations,
    },
    service::{ClientInitializeError, RunningService, ServiceError, ServiceExt},
    transport::StreamableHttpClientTransport,
    transport::streamable_http_client::StreamableHttpClientTransportConfig,
};
use serde_json::Value;

use crate::types::{
    LlmFunctionToolDefinition, LlmToolBinding, LlmToolCall, LlmToolConnection, LlmToolDefinition, ToolAnnotations, ToolSafety,
};
use eden_config::AgentsSecurityConfig;
use error::EpError;
use futures::future::try_join_all;
use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr, ToSocketAddrs};
use uuid::Uuid;

const CLIENT_NAME: &str = "eden-llm-endpoint";
const CLIENT_VERSION: &str = "1.0.0";
const TOOL_NAME_DELIMITER: &str = "__";
const TOOL_LATENCY_UNIT: &str = "ms";
/// Default TTL for production tool discovery cache entries.
pub const TOOL_DISCOVERY_CACHE_TTL_SECS: u64 = 120;
#[cfg(embedded_db)]
const TOOL_DISCOVERY_CACHE_TTL: Duration = Duration::from_secs(300);
#[cfg(embedded_db)]
const TOOL_DISCOVERY_CACHE_MAX_ENTRIES: usize = 256;

/// External cache for tool discovery results in production builds.
///
/// Implemented by the service layer (e.g., using Redis) to avoid adding
/// storage dependencies to `llm-core`. Cache keys should be scoped to
/// `org_uuid` to enforce tenant isolation.
#[async_trait::async_trait]
pub trait ToolDiscoveryCache: Send + Sync {
    /// Look up cached tool bindings for a connection.
    async fn get_bindings(&self, connection: &LlmToolConnection) -> Option<Vec<LlmToolBinding>>;
    /// Store tool bindings for a connection with a TTL.
    async fn set_bindings(&self, connection: &LlmToolConnection, bindings: &[LlmToolBinding]);
    /// Invalidate all cached bindings for an endpoint.
    async fn invalidate_endpoint(&self, endpoint_uuid: &str);
}

#[cfg(embedded_db)]
#[derive(Debug, Clone)]
struct CachedToolDiscovery {
    bindings: Vec<LlmToolBinding>,
    discovered_at: Instant,
}

struct ToolCallMetrics {
    attempts: Counter<u64>,
    failures: Counter<u64>,
    latency: Histogram<f64>,
}

impl ToolCallMetrics {
    fn new() -> Self {
        let meter = global::meter("eden.llm.tool");
        let prefix = "eden.llm.tool.";
        Self {
            attempts: meter.u64_counter(prefix.to_owned() + "attempts").with_description("Total tool invocation attempts").build(),
            failures: meter.u64_counter(prefix.to_owned() + "failures").with_description("Number of tool invocations that failed").build(),
            latency: meter
                .f64_histogram(prefix.to_owned() + "latency")
                .with_description("Tool invocation latency")
                .with_unit(TOOL_LATENCY_UNIT)
                .build(),
        }
    }

    fn record_success(&self, attributes: &[KeyValue], duration: std::time::Duration) {
        self.attempts.add(1, attributes);
        self.latency.record(duration.as_secs_f64() * 1_000.0, attributes);
    }

    fn record_failure(&self, attributes: &[KeyValue], duration: std::time::Duration, reason: &str) {
        self.attempts.add(1, attributes);
        self.latency.record(duration.as_secs_f64() * 1_000.0, attributes);
        let mut failure_attrs = attributes.to_vec();
        failure_attrs.push(KeyValue::new("reason", reason.to_string()));
        self.failures.add(1, &failure_attrs);
    }
}

static TOOL_CALL_METRICS: Lazy<ToolCallMetrics> = Lazy::new(ToolCallMetrics::new);

/// Shared HTTP client for tool endpoint connections. Reusing a single client
/// preserves HTTP/2 connection multiplexing across endpoints and avoids the
/// overhead of TLS handshake + connection setup on every request.
static SHARED_TOOL_HTTP_CLIENT: Lazy<HttpClient> = Lazy::new(|| {
    HttpClient::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("failed to build shared tool HTTP client")
});
#[cfg(embedded_db)]
// Keep discovery reuse local-only. Server builds should always rediscover so
// capability changes and tenant boundaries are reflected immediately.
static TOOL_DISCOVERY_CACHE: Lazy<RwLock<HashMap<String, CachedToolDiscovery>>> = Lazy::new(|| RwLock::new(HashMap::new()));

#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)] // Error variants from upstream crates
pub enum ToolClientError {
    #[error("failed to initialize tool client: {0}")]
    Initialize(#[from] ClientInitializeError),
    #[error("tools service error: {0}")]
    Service(#[from] ServiceError),
    #[error("invalid tool arguments: {0}")]
    InvalidArguments(String),
    #[error("failed to build HTTP client: {0}")]
    Http(#[from] reqwest::Error),
    #[error("SSRF protection: {0}")]
    SsrfBlocked(String),
}

impl From<ToolClientError> for EpError {
    fn from(value: ToolClientError) -> Self {
        EpError::request(value)
    }
}

#[derive(Clone)]
pub struct ToolClient {
    running: Arc<RunningService<RoleClient, ClientInfo>>,
}

impl ToolClient {
    pub async fn connect_over_streamable_http(connection: &LlmToolConnection) -> Result<Self, ToolClientError> {
        // Re-validate resolved IPs at connection time to catch DNS rebinding.
        validate_connection_url(connection)?;

        let client = SHARED_TOOL_HTTP_CLIENT.clone();
        let config =
            StreamableHttpClientTransportConfig::with_uri(connection.tools_url.clone()).auth_header(connection.bearer_token.clone());
        let transport = StreamableHttpClientTransport::with_client(client, config);
        Self::connect_with_transport(transport).await
    }

    async fn connect_with_transport(transport: StreamableHttpClientTransport<HttpClient>) -> Result<Self, ToolClientError> {
        let service = ClientInfo {
            protocol_version: ProtocolVersion::LATEST,
            capabilities: ClientCapabilities::default(),
            client_info: default_implementation(),
        };
        let running = service.serve(transport).await?;
        Ok(Self { running: Arc::new(running) })
    }

    fn peer(&self) -> &rmcp::service::Peer<RoleClient> {
        self.running.peer()
    }

    pub async fn call_tool(&self, name: &str, arguments: Value) -> Result<CallToolResult, ToolClientError> {
        let arguments = value_to_json_object(arguments)?;
        Ok(self.peer().call_tool(CallToolRequestParam { name: name.to_owned().into(), arguments }).await?)
    }

    pub async fn list_tools(&self, next_cursor: Option<String>) -> Result<ListToolsResult, ToolClientError> {
        let cursor = next_cursor.map(|cursor| PaginatedRequestParam { cursor: Some(cursor) });
        Ok(self.peer().list_tools(cursor).await?)
    }
}

fn default_implementation() -> Implementation {
    Implementation {
        name: CLIENT_NAME.to_string(),
        title: None,
        version: CLIENT_VERSION.to_string(),
        icons: None,
        website_url: None,
    }
}

#[allow(clippy::result_large_err)] // Error type size from upstream crates
fn value_to_json_object(value: Value) -> Result<Option<rmcp::model::JsonObject>, ToolClientError> {
    match value {
        Value::Null => Ok(None),
        Value::Object(map) => Ok(Some(map)),
        other => Err(ToolClientError::InvalidArguments(format!("expected JSON object arguments, received {other}"))),
    }
}

/// Maps `(remote_name, call_args) -> ToolSafety`. Keyed by `client_key` in `ToolRuntime`.
pub type SafetyFn = Arc<dyn Fn(&str, &Value) -> ToolSafety + Send + Sync>;

pub struct ToolRuntime {
    clients: HashMap<String, ToolClient>,
    bindings: Vec<LlmToolBinding>,
    binding_index: HashMap<String, usize>,
    definitions: Vec<LlmToolDefinition>,
    classifiers: HashMap<String, SafetyFn>,
    trusted_annotation_clients: HashSet<String>,
}

impl ToolRuntime {
    pub async fn new(connections: &[LlmToolConnection]) -> Result<Self, EpError> {
        let tasks = connections.iter().cloned().map(|connection| async move {
            let client = ToolClient::connect_over_streamable_http(&connection).await?;

            let bindings = discover_tool_bindings_for_runtime(&client, &connection).await?;

            Ok::<_, EpError>((connection.client_key.clone(), client, bindings))
        });

        let results = try_join_all(tasks).await?;

        let mut clients = HashMap::with_capacity(results.len());
        let mut bindings = Vec::new();

        for (client_key, client, mut discovered) in results {
            bindings.append(&mut discovered);
            clients.insert(client_key, client);
        }

        bindings.sort_by(|a, b| a.name.cmp(&b.name));

        let mut binding_index = HashMap::with_capacity(bindings.len());
        for (idx, binding) in bindings.iter().enumerate() {
            binding_index.insert(binding.name.clone(), idx);
        }

        let definitions = bindings.iter().map(|binding| binding.definition.clone()).collect();

        Ok(Self {
            clients,
            bindings,
            binding_index,
            definitions,
            classifiers: HashMap::new(),
            trusted_annotation_clients: connections
                .iter()
                .filter(|connection| connection.trust_annotations)
                .map(|connection| connection.client_key.clone())
                .collect(),
        })
    }

    /// Constructs a `ToolRuntime` with an external discovery cache for production.
    ///
    /// When provided, the cache is checked before making HTTP discovery calls.
    /// On cache miss, bindings are discovered via HTTP and written to the cache.
    /// Clients are always connected so `call_tool` works regardless of cache hit.
    pub async fn new_with_cache(connections: &[LlmToolConnection], cache: Arc<dyn ToolDiscoveryCache>) -> Result<Self, EpError> {
        let tasks = connections.iter().cloned().map(|connection| {
            let cache = Arc::clone(&cache);
            async move {
                let client = ToolClient::connect_over_streamable_http(&connection).await?;
                let bindings = if let Some(cached) = cache.get_bindings(&connection).await {
                    cached
                } else {
                    let discovered = discover_tool_bindings(&client, &connection).await?;
                    cache.set_bindings(&connection, &discovered).await;
                    discovered
                };
                Ok::<_, EpError>((connection.client_key.clone(), client, bindings))
            }
        });

        let results = try_join_all(tasks).await?;

        let mut clients = HashMap::with_capacity(results.len());
        let mut bindings = Vec::new();

        for (client_key, client, mut discovered) in results {
            bindings.append(&mut discovered);
            clients.insert(client_key, client);
        }

        bindings.sort_by(|a, b| a.name.cmp(&b.name));

        let mut binding_index = HashMap::with_capacity(bindings.len());
        for (idx, binding) in bindings.iter().enumerate() {
            binding_index.insert(binding.name.clone(), idx);
        }

        let definitions = bindings.iter().map(|binding| binding.definition.clone()).collect();

        Ok(Self {
            clients,
            bindings,
            binding_index,
            definitions,
            classifiers: HashMap::new(),
            trusted_annotation_clients: connections
                .iter()
                .filter(|connection| connection.trust_annotations)
                .map(|connection| connection.client_key.clone())
                .collect(),
        })
    }

    /// Constructs a `ToolRuntime` with endpoint-specific safety classifiers.
    ///
    /// `classifiers` maps `client_key -> SafetyFn`. Connections without an entry
    /// default to [`ToolSafety::Moderate`].
    pub async fn new_with_classifiers(connections: &[LlmToolConnection], classifiers: HashMap<String, SafetyFn>) -> Result<Self, EpError> {
        let mut runtime = Self::new(connections).await?;
        runtime.classifiers = classifiers;
        Ok(runtime)
    }

    /// Constructs with both a discovery cache and safety classifiers.
    pub async fn new_with_cache_and_classifiers(
        connections: &[LlmToolConnection],
        cache: Arc<dyn ToolDiscoveryCache>,
        classifiers: HashMap<String, SafetyFn>,
    ) -> Result<Self, EpError> {
        let mut runtime = Self::new_with_cache(connections, cache).await?;
        runtime.classifiers = classifiers;
        Ok(runtime)
    }

    /// Returns the safety level for a tool call.
    ///
    /// Looks up the binding by `qualified_name` (`client_key__remote_name`),
    /// then calls the classifier registered for that `client_key`.
    /// Falls back to trusted MCP annotations, then [`ToolSafety::Moderate`].
    pub fn safety_class(&self, qualified_name: &str, args: &Value) -> ToolSafety {
        let Some(&idx) = self.binding_index.get(qualified_name) else {
            return ToolSafety::Moderate;
        };
        let binding = &self.bindings[idx];
        if let Some(classifier) = self.classifiers.get(&binding.client_key) {
            return classifier(&binding.remote_name, args);
        }
        if !self.trusted_annotation_clients.contains(&binding.client_key) {
            return ToolSafety::Moderate;
        }
        annotation_safety(binding.annotations.as_ref())
    }

    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    pub fn tool_definitions(&self) -> &[LlmToolDefinition] {
        &self.definitions
    }

    pub fn tool_bindings(&self) -> &[LlmToolBinding] {
        &self.bindings
    }

    pub async fn call_tool(&self, call: &LlmToolCall) -> Result<Vec<String>, EpError> {
        let index = self
            .binding_index
            .get(call.function.name.as_str())
            .ok_or_else(|| EpError::request(format!("no tool binding registered for '{}'", call.function.name)))?;
        let binding = &self.bindings[*index];
        let client = self
            .clients
            .get(&binding.client_key)
            .ok_or_else(|| EpError::request(format!("no tool client registered for {}", binding.client_key)))?;
        let attributes = vec![
            KeyValue::new("tool", call.function.name.clone()),
            KeyValue::new("client_key", binding.client_key.clone()),
        ];
        let start = Instant::now();
        let args_str = call.function.arguments.trim();
        let arguments_json: Value = if args_str.is_empty() {
            Value::Object(serde_json::Map::new())
        } else {
            match serde_json::from_str(args_str) {
                Ok(value) => value,
                Err(err) => {
                    TOOL_CALL_METRICS.record_failure(&attributes, start.elapsed(), "invalid_arguments");
                    return Err(EpError::request(format!("invalid tool arguments for '{}': {err}", call.function.name)));
                }
            }
        };
        match client.call_tool(binding.remote_name.as_str(), arguments_json).await {
            Ok(result) => {
                TOOL_CALL_METRICS.record_success(&attributes, start.elapsed());
                Ok(extract_tool_content(&result))
            }
            Err(err) => {
                let reason = classify_tool_error(&err);
                TOOL_CALL_METRICS.record_failure(&attributes, start.elapsed(), reason);
                Err(err.into())
            }
        }
    }
}

#[cfg(embedded_db)]
async fn discover_tool_bindings_for_runtime(client: &ToolClient, connection: &LlmToolConnection) -> Result<Vec<LlmToolBinding>, EpError> {
    match get_cached_tool_bindings(connection) {
        Some(bindings) => Ok(bindings),
        None => discover_and_cache_tool_bindings(client, connection).await,
    }
}

#[cfg(not(embedded_db))]
async fn discover_tool_bindings_for_runtime(client: &ToolClient, connection: &LlmToolConnection) -> Result<Vec<LlmToolBinding>, EpError> {
    discover_tool_bindings(client, connection).await
}

async fn discover_tool_bindings(client: &ToolClient, connection: &LlmToolConnection) -> Result<Vec<LlmToolBinding>, EpError> {
    let mut bindings = Vec::new();
    let mut next_cursor: Option<String> = None;

    loop {
        let page = client.list_tools(next_cursor.clone()).await?;
        bindings.extend(convert_tools(connection, &page.tools));
        next_cursor = page.next_cursor;
        if next_cursor.is_none() {
            break;
        }
    }

    Ok(bindings)
}

#[cfg(embedded_db)]
async fn discover_and_cache_tool_bindings(client: &ToolClient, connection: &LlmToolConnection) -> Result<Vec<LlmToolBinding>, EpError> {
    let bindings = discover_tool_bindings(client, connection).await?;
    cache_tool_bindings(connection, bindings.clone());
    Ok(bindings)
}

#[cfg(embedded_db)]
fn get_cached_tool_bindings(connection: &LlmToolConnection) -> Option<Vec<LlmToolBinding>> {
    let cache_key = tool_discovery_cache_key(connection);
    read_cached_tool_bindings(cache_key.as_str(), Instant::now(), TOOL_DISCOVERY_CACHE_TTL)
}

#[cfg(embedded_db)]
fn cache_tool_bindings(connection: &LlmToolConnection, bindings: Vec<LlmToolBinding>) {
    let cache_key = tool_discovery_cache_key(connection);
    write_cached_tool_bindings(cache_key, bindings, Instant::now(), TOOL_DISCOVERY_CACHE_TTL, TOOL_DISCOVERY_CACHE_MAX_ENTRIES);
}

#[cfg(embedded_db)]
fn tool_discovery_cache_key(connection: &LlmToolConnection) -> String {
    const CACHE_KEY_NAMESPACE: Uuid = Uuid::from_u128(0x746f_6f6c_6469_7363_6f76_6572_795f_6b65);
    let token_fingerprint = Uuid::new_v5(&CACHE_KEY_NAMESPACE, connection.bearer_token.as_bytes());
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        connection.client_key,
        connection.tools_url,
        token_fingerprint,
        connection.endpoint_uuid.as_deref().unwrap_or_default(),
        connection.endpoint_name.as_deref().unwrap_or_default(),
        connection.endpoint_description.as_deref().unwrap_or_default(),
        connection.endpoint_kind.as_deref().unwrap_or_default(),
    )
}

#[cfg(embedded_db)]
fn read_cached_tool_bindings(cache_key: &str, now: Instant, ttl: Duration) -> Option<Vec<LlmToolBinding>> {
    let Ok(mut cache) = TOOL_DISCOVERY_CACHE.write() else {
        return None;
    };

    prune_tool_discovery_cache(&mut cache, now, ttl);

    let cached = cache.get(cache_key)?;
    Some(cached.bindings.clone())
}

#[cfg(embedded_db)]
fn write_cached_tool_bindings(cache_key: String, bindings: Vec<LlmToolBinding>, now: Instant, ttl: Duration, max_entries: usize) {
    let Ok(mut cache) = TOOL_DISCOVERY_CACHE.write() else {
        return;
    };

    prune_tool_discovery_cache(&mut cache, now, ttl);
    cache.insert(cache_key, CachedToolDiscovery { bindings, discovered_at: now });
    trim_tool_discovery_cache(&mut cache, max_entries);
}

#[cfg(embedded_db)]
fn prune_tool_discovery_cache(cache: &mut HashMap<String, CachedToolDiscovery>, now: Instant, ttl: Duration) {
    cache.retain(|_, entry| now.saturating_duration_since(entry.discovered_at) <= ttl);
}

#[cfg(embedded_db)]
fn trim_tool_discovery_cache(cache: &mut HashMap<String, CachedToolDiscovery>, max_entries: usize) {
    if cache.len() <= max_entries {
        return;
    }

    let mut entries: Vec<(String, Instant)> = cache.iter().map(|(key, entry)| (key.clone(), entry.discovered_at)).collect();
    entries.sort_by_key(|(_, discovered_at)| *discovered_at);

    let overflow = cache.len().saturating_sub(max_entries);
    for (key, _) in entries.into_iter().take(overflow) {
        cache.remove(&key);
    }
}

#[cfg(embedded_db)]
pub fn clear_tool_discovery_cache() {
    if let Ok(mut cache) = TOOL_DISCOVERY_CACHE.write() {
        cache.clear();
    }
}

#[cfg(not(embedded_db))]
pub fn clear_tool_discovery_cache() {}

fn classify_tool_error(err: &ToolClientError) -> &'static str {
    match err {
        ToolClientError::Initialize(_) => "initialize",
        ToolClientError::Service(_) => "service",
        ToolClientError::InvalidArguments(_) => "invalid_arguments",
        ToolClientError::Http(_) => "http",
        ToolClientError::SsrfBlocked(_) => "ssrf_blocked",
    }
}

fn extract_tool_content(result: &CallToolResult) -> Vec<String> {
    result.content.iter().map(format_tool_content).collect()
}

fn format_tool_content(content: &Content) -> String {
    match &content.raw {
        RawContent::Text(text) => text.text.clone(),
        other => format!("{other:?}"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolName {
    client_key: String,
    tool_name: String,
}

impl ToolName {
    pub fn new(client_key: impl Into<String>, tool_name: impl Into<String>) -> Self {
        Self { client_key: client_key.into(), tool_name: tool_name.into() }
    }

    pub fn to_qualified_name(&self) -> String {
        format!("{}{}{}", self.client_key, TOOL_NAME_DELIMITER, self.tool_name)
    }

    pub fn client_key(&self) -> &str {
        &self.client_key
    }

    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }
}

impl fmt::Display for ToolName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_qualified_name())
    }
}

impl TryFrom<&str> for ToolName {
    type Error = EpError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if let Some(idx) = value.find(TOOL_NAME_DELIMITER) {
            let client_key = &value[..idx];
            let tool_name = &value[idx + TOOL_NAME_DELIMITER.len()..];
            if client_key.is_empty() || tool_name.is_empty() {
                return Err(EpError::request(format!(
                    "invalid tool name '{value}': both client key and tool name must be non-empty"
                )));
            }
            Ok(Self {
                client_key: client_key.to_string(),
                tool_name: tool_name.to_string(),
            })
        } else {
            Err(EpError::request(format!("invalid tool name '{value}', expected delimiter '{TOOL_NAME_DELIMITER}'")))
        }
    }
}

impl TryFrom<&String> for ToolName {
    type Error = EpError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

fn annotation_safety(annotations: Option<&ToolAnnotations>) -> ToolSafety {
    let Some(annotations) = annotations else {
        return ToolSafety::Moderate;
    };
    if annotations.read_only_hint == Some(true) {
        return ToolSafety::Safe;
    }
    if annotations.destructive_hint == Some(true) {
        return ToolSafety::Dangerous;
    }
    ToolSafety::Moderate
}

fn convert_tool_annotations(annotations: Option<&RmcpToolAnnotations>) -> Option<ToolAnnotations> {
    annotations.map(|annotations| ToolAnnotations {
        read_only_hint: annotations.read_only_hint,
        destructive_hint: annotations.destructive_hint,
        idempotent_hint: annotations.idempotent_hint,
    })
}

fn convert_tools(connection: &LlmToolConnection, tools: &[RmcpTool]) -> Vec<LlmToolBinding> {
    let prefix = connection.client_key.as_str();
    let endpoint_label = connection.endpoint_label();
    let endpoint_kind = connection.endpoint_kind.as_deref().map(str::trim).filter(|kind| !kind.is_empty());
    let endpoint_uuid = connection.endpoint_uuid.as_deref().map(str::trim).filter(|uuid| !uuid.is_empty());
    tools
        .iter()
        .map(|tool| {
            let name = format!("{prefix}{TOOL_NAME_DELIMITER}{}", tool.name);
            let base_description = tool.description.as_ref().map(|d| d.trim()).filter(|d| !d.is_empty());
            let mut context_parts = Vec::new();
            if let Some(label) = endpoint_label.as_ref() {
                context_parts.push(format!("Endpoint: {label}"));
            }
            if let Some(kind) = endpoint_kind {
                context_parts.push(format!("Kind: {kind}"));
            }
            if let Some(uuid) = endpoint_uuid {
                context_parts.push(format!("UUID: {uuid}"));
            }
            let context_suffix = (!context_parts.is_empty()).then(|| format!(" ({})", context_parts.join(" | ")));
            let description = match (base_description, context_suffix) {
                (Some(base), Some(context)) => Some(format!("{base}{context}")),
                (Some(base), None) => Some(base.to_string()),
                (None, Some(context)) => Some(context.trim().trim_start_matches('(').trim_end_matches(')').to_string()),
                (None, None) => None,
            };
            let parameters = Value::Object((*tool.input_schema).clone());
            let definition = LlmToolDefinition {
                r#type: "function".to_string(),
                function: LlmFunctionToolDefinition {
                    name: name.clone(),
                    description,
                    parameters,
                    example_usage: None,
                },
            };
            let binding_id = tool_binding_id(prefix, &tool.name);
            LlmToolBinding {
                binding_id,
                name,
                client_key: connection.client_key.clone(),
                remote_name: tool.name.to_string(),
                annotations: convert_tool_annotations(tool.annotations.as_ref()),
                definition,
                related_tools: Vec::new(),
                workflow_hint: None,
            }
        })
        .collect()
}

fn tool_binding_id(client_key: &str, tool_name: &str) -> Uuid {
    const BINDING_NAMESPACE: Uuid = Uuid::from_u128(0xed4e_6c6d_746f_6f6c_6269_6e64_6e61_6d65);
    let identifier = format!("{client_key}:{tool_name}");
    Uuid::new_v5(&BINDING_NAMESPACE, identifier.as_bytes())
}

// SSRF protection: defense-in-depth IP validation at connection time

#[allow(clippy::result_large_err)] // Error type size from upstream `ClientInitializeError`
fn validate_connection_url(connection: &LlmToolConnection) -> Result<(), ToolClientError> {
    if connection.skip_ssrf_validation {
        return Ok(());
    }

    validate_url_ips_with_security(connection.tools_url.as_str(), &eden_config::agents().security)
}

/// Resolves the hostname in `tools_url` and rejects any private/internal IPs.
///
/// This is the defense-in-depth layer against DNS rebinding: even if a URL
/// passed registration validation, the hostname may now resolve to an internal
/// address.
#[allow(clippy::result_large_err)] // Error type size from upstream `ClientInitializeError`
#[cfg(test)]
fn validate_url_ips(tools_url: &str) -> Result<(), ToolClientError> {
    validate_url_ips_with_security(tools_url, &eden_config::agents().security)
}

#[allow(clippy::result_large_err)] // Error type size from upstream `ClientInitializeError`
fn validate_url_ips_with_security(tools_url: &str, security: &AgentsSecurityConfig) -> Result<(), ToolClientError> {
    let parsed = reqwest::Url::parse(tools_url).map_err(|e| ToolClientError::SsrfBlocked(format!("invalid tool endpoint URL: {e}")))?;

    let host = match parsed.host_str() {
        Some(h) => h,
        None => return Err(ToolClientError::SsrfBlocked("tool endpoint URL has no host".to_string())),
    };

    // If the host is an IP literal, check it directly.
    if let Ok(ip) = host.parse::<IpAddr>() {
        if should_skip_private_ip_checks(host, security) {
            return Ok(());
        }
        return if is_ssrf_private_ip(&ip) {
            Err(ToolClientError::SsrfBlocked(format!("tool endpoint must not target a private/internal IP ({ip})")))
        } else {
            Ok(())
        };
    }

    if should_skip_private_ip_checks(host, security) {
        return Ok(());
    }

    // Resolve the hostname and check every returned address.
    // NOTE: There is a TOCTOU window between this check and the actual HTTP connection
    // (reqwest resolves DNS again internally). A DNS rebinding attack could exploit this
    // by returning a public IP here but a private IP at connection time. To mitigate,
    // we resolve twice with a brief pause to catch naive rebinding attempts.
    let port = parsed.port_or_known_default().unwrap_or(443);
    let socket_addr = format!("{host}:{port}");

    let resolve_and_check = |label: &str| -> Result<(), ToolClientError> {
        let addrs: Vec<_> = socket_addr
            .to_socket_addrs()
            .map_err(|e| ToolClientError::SsrfBlocked(format!("cannot resolve tool endpoint hostname '{host}': {e}")))?
            .collect();

        if addrs.is_empty() {
            return Err(ToolClientError::SsrfBlocked(format!(
                "tool endpoint hostname '{host}' does not resolve to any addresses"
            )));
        }

        for addr in &addrs {
            if is_ssrf_private_ip(&addr.ip()) {
                return Err(ToolClientError::SsrfBlocked(format!(
                    "tool endpoint hostname '{host}' resolves to private/internal IP ({}) during {label} check",
                    addr.ip()
                )));
            }
        }

        Ok(())
    };

    // First resolution check
    resolve_and_check("initial")?;

    // Second resolution to catch naive DNS rebinding (short TTL flips)
    resolve_and_check("secondary")?;

    Ok(())
}

fn should_skip_private_ip_checks(host: &str, security: &AgentsSecurityConfig) -> bool {
    security.tool_endpoint_allow_private_ips
        || security
            .tool_endpoint_allowed_hosts
            .iter()
            .any(|allowed| normalize_host_for_match(allowed) == normalize_host_for_match(host))
}

fn normalize_host_for_match(host: &str) -> String {
    host.trim().trim_start_matches('[').trim_end_matches(']').to_ascii_lowercase()
}

/// Returns `true` if `ip` belongs to a private/loopback/link-local or
/// otherwise non-routable network range.
fn is_ssrf_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_ssrf_private_ipv4(v4),
        IpAddr::V6(v6) => is_ssrf_private_ipv6(v6),
    }
}

fn is_ssrf_private_ipv4(ip: &Ipv4Addr) -> bool {
    if ip.is_loopback() {
        return true;
    }
    let o = ip.octets();
    // 10.0.0.0/8
    if o[0] == 10 {
        return true;
    }
    // 172.16.0.0/12
    if o[0] == 172 && (o[1] & 0xF0) == 16 {
        return true;
    }
    // 192.168.0.0/16
    if o[0] == 192 && o[1] == 168 {
        return true;
    }
    // 169.254.0.0/16: link-local
    if ip.is_link_local() {
        return true;
    }
    // 0.0.0.0/8: "this" network
    if o[0] == 0 {
        return true;
    }
    // 100.64.0.0/10: shared address space (CGN / cloud-internal)
    if o[0] == 100 && (o[1] & 0xC0) == 64 {
        return true;
    }
    // 198.18.0.0/15: benchmarking
    if o[0] == 198 && (o[1] & 0xFE) == 18 {
        return true;
    }
    false
}

fn is_ssrf_private_ipv6(ip: &Ipv6Addr) -> bool {
    if ip.is_loopback() || ip.is_unspecified() {
        return true;
    }
    // fc00::/7: unique local
    if (ip.segments()[0] & 0xFE00) == 0xFC00 {
        return true;
    }
    // fe80::/10: link-local
    if (ip.segments()[0] & 0xFFC0) == 0xFE80 {
        return true;
    }
    // IPv4-mapped IPv6 (::ffff:x.x.x.x)
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_ssrf_private_ipv4(&v4);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_name_formats_and_round_trips() {
        let tool_name = ToolName::new("abc123", "do_thing");

        assert_eq!(tool_name.to_string(), "abc123__do_thing");
        assert_eq!(tool_name.to_qualified_name(), "abc123__do_thing");
        assert_eq!(tool_name.client_key(), "abc123");
        assert_eq!(tool_name.tool_name(), "do_thing");

        let qualified = tool_name.to_string();
        let parsed = ToolName::try_from(qualified.as_str()).expect("string should parse");
        assert_eq!(parsed, tool_name);
    }

    #[test]
    fn tool_name_rejects_missing_delimiter() {
        let err = ToolName::try_from("missing_delimiter").unwrap_err();

        assert!(format!("{err}").contains("expected delimiter '__'"), "unexpected error message: {err}");
    }

    #[test]
    fn safety_class_uses_injected_classifier() {
        use crate::types::{LlmFunctionToolDefinition, LlmToolBinding, LlmToolDefinition, ToolAnnotations, ToolSafety};
        use serde_json::json;
        use std::sync::Arc;

        let client_key = "pg-abc12345-postgres".to_string();
        let remote_name = "execute_postgres_query".to_string();
        let qualified_name = format!("{client_key}__{remote_name}");

        let binding = LlmToolBinding {
            binding_id: uuid::Uuid::new_v4(),
            name: qualified_name.clone(),
            client_key: client_key.clone(),
            remote_name: remote_name.clone(),
            annotations: Some(ToolAnnotations { destructive_hint: Some(true), ..ToolAnnotations::default() }),
            definition: LlmToolDefinition {
                r#type: "function".to_string(),
                function: LlmFunctionToolDefinition {
                    name: qualified_name.clone(),
                    description: None,
                    parameters: serde_json::Value::Null,
                    ..Default::default()
                },
            },
            related_tools: Vec::new(),
            workflow_hint: None,
        };

        let classifier: SafetyFn = Arc::new(|_rn, args| {
            if let Some(q) = args.get("query").and_then(|v| v.as_str())
                && q.trim_start().to_uppercase().starts_with("SELECT")
            {
                return ToolSafety::Safe;
            }
            ToolSafety::Moderate
        });

        let mut classifiers = HashMap::new();
        classifiers.insert(client_key.clone(), classifier);

        let mut runtime = ToolRuntime {
            clients: HashMap::new(),
            bindings: vec![binding],
            binding_index: {
                let mut idx = HashMap::new();
                idx.insert(qualified_name.clone(), 0);
                idx
            },
            definitions: vec![],
            classifiers,
            trusted_annotation_clients: HashSet::from([client_key.clone()]),
        };

        let select_args = json!({ "query": "SELECT * FROM users" });
        assert_eq!(runtime.safety_class(&qualified_name, &select_args), ToolSafety::Safe);

        let insert_args = json!({ "query": "INSERT INTO users (name) VALUES ('bob')" });
        assert_eq!(runtime.safety_class(&qualified_name, &insert_args), ToolSafety::Moderate);

        assert_eq!(runtime.safety_class("unknown__tool", &select_args), ToolSafety::Moderate);

        let other_binding = LlmToolBinding {
            binding_id: uuid::Uuid::new_v4(),
            name: "other__tool".to_string(),
            client_key: "other".to_string(),
            remote_name: "tool".to_string(),
            annotations: None,
            definition: LlmToolDefinition {
                r#type: "function".to_string(),
                function: LlmFunctionToolDefinition {
                    name: "other__tool".to_string(),
                    description: None,
                    parameters: serde_json::Value::Null,
                    ..Default::default()
                },
            },
            related_tools: Vec::new(),
            workflow_hint: None,
        };
        runtime.bindings.push(other_binding);
        runtime.binding_index.insert("other__tool".to_string(), 1);
        assert_eq!(runtime.safety_class("other__tool", &select_args), ToolSafety::Moderate);
    }

    #[test]
    fn safety_class_uses_trusted_annotations_when_classifier_missing() {
        use crate::types::{LlmFunctionToolDefinition, LlmToolBinding, LlmToolDefinition, ToolAnnotations, ToolSafety};
        use serde_json::json;

        let client_key = "trusted".to_string();
        let qualified_name = "trusted__tool".to_string();
        let mut runtime = ToolRuntime {
            clients: HashMap::new(),
            bindings: vec![LlmToolBinding {
                binding_id: uuid::Uuid::new_v4(),
                name: qualified_name.clone(),
                client_key: client_key.clone(),
                remote_name: "tool".to_string(),
                annotations: Some(ToolAnnotations { read_only_hint: Some(true), ..ToolAnnotations::default() }),
                definition: LlmToolDefinition {
                    r#type: "function".to_string(),
                    function: LlmFunctionToolDefinition {
                        name: qualified_name.clone(),
                        description: None,
                        parameters: serde_json::Value::Null,
                        ..Default::default()
                    },
                },
                related_tools: Vec::new(),
                workflow_hint: None,
            }],
            binding_index: HashMap::from([(qualified_name.clone(), 0)]),
            definitions: vec![],
            classifiers: HashMap::new(),
            trusted_annotation_clients: HashSet::from([client_key]),
        };

        assert_eq!(runtime.safety_class(&qualified_name, &json!({})), ToolSafety::Safe);

        runtime.bindings[0].annotations = Some(ToolAnnotations { destructive_hint: Some(true), ..ToolAnnotations::default() });
        assert_eq!(runtime.safety_class(&qualified_name, &json!({})), ToolSafety::Dangerous);

        runtime.bindings[0].annotations = Some(ToolAnnotations::default());
        assert_eq!(runtime.safety_class(&qualified_name, &json!({})), ToolSafety::Moderate);
    }

    #[test]
    fn safety_class_ignores_untrusted_annotations_without_classifier() {
        use crate::types::{LlmFunctionToolDefinition, LlmToolBinding, LlmToolDefinition, ToolAnnotations, ToolSafety};
        use serde_json::json;

        let qualified_name = "untrusted__tool".to_string();
        let runtime = ToolRuntime {
            clients: HashMap::new(),
            bindings: vec![LlmToolBinding {
                binding_id: uuid::Uuid::new_v4(),
                name: qualified_name.clone(),
                client_key: "untrusted".to_string(),
                remote_name: "tool".to_string(),
                annotations: Some(ToolAnnotations { read_only_hint: Some(true), ..ToolAnnotations::default() }),
                definition: LlmToolDefinition {
                    r#type: "function".to_string(),
                    function: LlmFunctionToolDefinition {
                        name: qualified_name.clone(),
                        description: None,
                        parameters: serde_json::Value::Null,
                        ..Default::default()
                    },
                },
                related_tools: Vec::new(),
                workflow_hint: None,
            }],
            binding_index: HashMap::from([(qualified_name.clone(), 0)]),
            definitions: vec![],
            classifiers: HashMap::new(),
            trusted_annotation_clients: HashSet::new(),
        };

        assert_eq!(runtime.safety_class(&qualified_name, &json!({})), ToolSafety::Moderate);
    }

    #[test]
    fn convert_tools_preserves_rmcp_annotations() {
        use rmcp::model::ToolAnnotations as RmcpToolAnnotations;
        use serde_json::Map;
        use std::sync::Arc;

        let connection = crate::types::LlmToolConnection {
            client_key: "conn".to_string(),
            tools_url: "https://example.com/tools".to_string(),
            bearer_token: "token".to_string(),
            endpoint_uuid: None,
            endpoint_name: None,
            endpoint_description: None,
            endpoint_kind: None,
            trust_annotations: false,
            skip_ssrf_validation: false,
        };
        let tool = RmcpTool {
            name: "lookup".into(),
            title: None,
            description: Some("Lookup".into()),
            input_schema: Arc::new(Map::new()),
            output_schema: None,
            annotations: Some(RmcpToolAnnotations {
                title: None,
                read_only_hint: Some(true),
                destructive_hint: Some(false),
                idempotent_hint: Some(true),
                open_world_hint: None,
            }),
            icons: None,
            meta: None,
        };

        let bindings = convert_tools(&connection, &[tool]);

        assert_eq!(
            bindings[0].annotations,
            Some(ToolAnnotations {
                read_only_hint: Some(true),
                destructive_hint: Some(false),
                idempotent_hint: Some(true),
            })
        );
    }

    #[test]
    fn convert_tools_enriches_description_with_endpoint_context() {
        use serde_json::Map;
        use std::sync::Arc;

        let connection = crate::types::LlmToolConnection {
            client_key: "orders-12345678-sql".to_string(),
            tools_url: "https://example.com/tools".to_string(),
            bearer_token: "token".to_string(),
            endpoint_uuid: Some("12345678-1234-1234-1234-123456789abc".to_string()),
            endpoint_name: Some("prod-orders".to_string()),
            endpoint_description: Some("Production order database".to_string()),
            endpoint_kind: Some("postgres".to_string()),
            trust_annotations: false,
            skip_ssrf_validation: false,
        };
        let tool = RmcpTool {
            name: "execute_query".into(),
            title: None,
            description: Some("Execute a query".into()),
            input_schema: Arc::new(Map::new()),
            output_schema: None,
            annotations: None,
            icons: None,
            meta: None,
        };

        let bindings = convert_tools(&connection, &[tool]);
        let description = bindings[0].definition.function.description.as_deref().expect("description");

        assert!(description.contains("Endpoint: prod-orders - Production order database"));
        assert!(description.contains("Kind: postgres"));
        assert!(description.contains("UUID: 12345678-1234-1234-1234-123456789abc"));
    }

    #[cfg(embedded_db)]
    #[test]
    fn tool_discovery_cache_key_hashes_token_and_endpoint_context() {
        let mut connection = crate::types::LlmToolConnection {
            client_key: "orders-12345678-sql".to_string(),
            tools_url: "https://example.com/tools".to_string(),
            bearer_token: "super-secret-token".to_string(),
            endpoint_uuid: Some("12345678-1234-1234-1234-123456789abc".to_string()),
            endpoint_name: Some("prod-orders".to_string()),
            endpoint_description: Some("Production order database".to_string()),
            endpoint_kind: Some("postgres".to_string()),
            trust_annotations: false,
            skip_ssrf_validation: false,
        };

        let cache_key = tool_discovery_cache_key(&connection);
        assert!(!cache_key.contains("super-secret-token"));

        let mut different_token = connection.clone();
        different_token.bearer_token = "different-token".to_string();
        assert_ne!(cache_key, tool_discovery_cache_key(&different_token));

        connection.endpoint_description = Some("Replica order database".to_string());
        assert_ne!(cache_key, tool_discovery_cache_key(&connection));
    }

    #[cfg(embedded_db)]
    #[test]
    fn cached_tool_bindings_respect_ttl() {
        let cache_key = format!("cache-test-{}", Uuid::new_v4());
        let binding = LlmToolBinding {
            binding_id: Uuid::new_v4(),
            name: "orders-123__execute_query".to_string(),
            client_key: "orders-123".to_string(),
            remote_name: "execute_query".to_string(),
            annotations: None,
            definition: LlmToolDefinition {
                r#type: "function".to_string(),
                function: LlmFunctionToolDefinition {
                    name: "orders-123__execute_query".to_string(),
                    description: Some("Execute query".to_string()),
                    parameters: serde_json::json!({}),
                    ..Default::default()
                },
            },
            related_tools: Vec::new(),
            workflow_hint: None,
        };

        let discovered_at = Instant::now();
        write_cached_tool_bindings(
            cache_key.clone(),
            vec![binding.clone()],
            discovered_at,
            Duration::from_secs(60),
            TOOL_DISCOVERY_CACHE_MAX_ENTRIES,
        );

        let cached = read_cached_tool_bindings(cache_key.as_str(), discovered_at + Duration::from_secs(30), Duration::from_secs(60));
        assert_eq!(cached, Some(vec![binding.clone()]));

        let expired = read_cached_tool_bindings(cache_key.as_str(), discovered_at + Duration::from_secs(61), Duration::from_secs(60));
        assert!(expired.is_none());
    }

    #[cfg(embedded_db)]
    #[test]
    fn clear_tool_discovery_cache_removes_cached_entries() {
        let cache_key = format!("cache-clear-test-{}", Uuid::new_v4());
        let binding = LlmToolBinding {
            binding_id: Uuid::new_v4(),
            name: "orders-123__execute_query".to_string(),
            client_key: "orders-123".to_string(),
            remote_name: "execute_query".to_string(),
            annotations: None,
            definition: LlmToolDefinition {
                r#type: "function".to_string(),
                function: LlmFunctionToolDefinition {
                    name: "orders-123__execute_query".to_string(),
                    description: Some("Execute query".to_string()),
                    parameters: serde_json::json!({}),
                    ..Default::default()
                },
            },
            related_tools: Vec::new(),
            workflow_hint: None,
        };

        write_cached_tool_bindings(
            cache_key.clone(),
            vec![binding],
            Instant::now(),
            Duration::from_secs(60),
            TOOL_DISCOVERY_CACHE_MAX_ENTRIES,
        );
        assert!(read_cached_tool_bindings(cache_key.as_str(), Instant::now(), Duration::from_secs(60)).is_some());

        clear_tool_discovery_cache();

        assert!(read_cached_tool_bindings(cache_key.as_str(), Instant::now(), Duration::from_secs(60)).is_none());
    }

    #[test]
    fn ssrf_rejects_private_ipv4_literals() {
        for url in [
            "https://10.0.0.1/tools",
            "https://172.16.0.1/tools",
            "https://192.168.1.1/tools",
            "https://127.0.0.1/tools",
            "https://169.254.1.1/tools",
            "https://0.0.0.1/tools",
            "https://100.64.0.1/tools",
            "https://198.18.0.1/tools",
        ] {
            let err = validate_url_ips(url);
            assert!(err.is_err(), "expected {url} to be rejected, but it was accepted");
        }
    }

    #[test]
    fn ssrf_rejects_private_ipv6_literals() {
        for url in ["https://[::1]/tools", "https://[fd00::1]/tools", "https://[fe80::1]/tools"] {
            let err = validate_url_ips(url);
            assert!(err.is_err(), "expected {url} to be rejected, but it was accepted");
        }
    }

    #[test]
    fn ssrf_allows_public_ip_literal() {
        let result = validate_url_ips("https://8.8.8.8/tools");
        assert!(result.is_ok(), "expected public IP to be accepted");
    }

    #[test]
    fn ssrf_rejects_malformed_url() {
        let result = validate_url_ips("not a url");
        assert!(result.is_err(), "expected malformed URL to be rejected");
    }

    #[test]
    fn ssrf_allows_private_ip_literal_when_private_ips_are_enabled() {
        let security = AgentsSecurityConfig {
            tool_endpoint_allow_private_ips: true,
            ..AgentsSecurityConfig::default()
        };

        let result = validate_url_ips_with_security("https://10.0.0.1/tools", &security);
        assert!(result.is_ok(), "expected allow-private-ips config to bypass SSRF rejection");
    }

    #[test]
    fn ssrf_allows_allowlisted_localhost() {
        let security = AgentsSecurityConfig {
            tool_endpoint_allowed_hosts: vec!["localhost".to_string()],
            ..AgentsSecurityConfig::default()
        };

        let result = validate_url_ips_with_security("http://localhost:8000/tools", &security);
        assert!(result.is_ok(), "expected allowlisted localhost to bypass SSRF rejection");
    }

    #[test]
    fn connection_validation_skips_ssrf_check_for_internal_routes() {
        let connection = crate::types::LlmToolConnection {
            client_key: "eden".to_string(),
            tools_url: "http://localhost:8000/api/v1/tools/eden".to_string(),
            bearer_token: "token".to_string(),
            endpoint_uuid: None,
            endpoint_name: Some("Eden".to_string()),
            endpoint_description: None,
            endpoint_kind: None,
            trust_annotations: true,
            skip_ssrf_validation: true,
        };

        let result = validate_connection_url(&connection);
        assert!(result.is_ok(), "expected trusted internal routes to bypass SSRF validation");
    }

    #[test]
    fn ssrf_private_ipv4_detection() {
        assert!(is_ssrf_private_ip(&IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(is_ssrf_private_ip(&IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1))));
        assert!(is_ssrf_private_ip(&IpAddr::V4(Ipv4Addr::new(172, 31, 255, 255))));
        assert!(is_ssrf_private_ip(&IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1))));
        assert!(is_ssrf_private_ip(&IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
        assert!(is_ssrf_private_ip(&IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1))));
        assert!(is_ssrf_private_ip(&IpAddr::V4(Ipv4Addr::new(0, 0, 0, 1))));
        assert!(is_ssrf_private_ip(&IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1))));
        assert!(is_ssrf_private_ip(&IpAddr::V4(Ipv4Addr::new(198, 18, 0, 1))));
        // Public IPs should not be flagged
        assert!(!is_ssrf_private_ip(&IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
        assert!(!is_ssrf_private_ip(&IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
        assert!(!is_ssrf_private_ip(&IpAddr::V4(Ipv4Addr::new(172, 32, 0, 1))));
        assert!(!is_ssrf_private_ip(&IpAddr::V4(Ipv4Addr::new(100, 128, 0, 1))));
    }

    #[test]
    fn ssrf_private_ipv6_detection() {
        assert!(is_ssrf_private_ip(&IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(is_ssrf_private_ip(&IpAddr::V6(Ipv6Addr::UNSPECIFIED)));
        assert!(is_ssrf_private_ip(&IpAddr::V6(Ipv6Addr::new(0xFD00, 0, 0, 0, 0, 0, 0, 1))));
        assert!(is_ssrf_private_ip(&IpAddr::V6(Ipv6Addr::new(0xFE80, 0, 0, 0, 0, 0, 0, 1))));
        // IPv4-mapped private
        let mapped = Ipv6Addr::new(0, 0, 0, 0, 0, 0xFFFF, 0x0A00, 0x0001);
        assert!(is_ssrf_private_ip(&IpAddr::V6(mapped)));
        // Public IPv6
        assert!(!is_ssrf_private_ip(&IpAddr::V6(Ipv6Addr::new(0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888))));
    }
}
