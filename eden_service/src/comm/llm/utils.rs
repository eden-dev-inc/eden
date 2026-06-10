use bytes::{BufMut, Bytes, BytesMut};
use eden_core::error::{EpError, TimeoutError};
use eden_core::request::ServerData;
use endpoint_core::llm_core::{LlmToolConnection, ToolRuntime};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::time::Instant;
use tokio::time;
use uuid::Uuid;

pub const EDEN_TOOLS_SERVER_KEY: &str = "eden-tools";
pub const EDEN_TOOLS_SERVER_NAME: &str = "Eden Tools";
pub const EDEN_TOOLS_SERVER_DESCRIPTION: &str = "Built-in Eden tool server";

pub fn canonicalize_bearer_token(token: &str) -> String {
    let trimmed = token.trim();
    let stripped = trimmed.strip_prefix("Bearer ").or_else(|| trimmed.strip_prefix("bearer ")).unwrap_or(trimmed);
    stripped.trim().to_string()
}

pub fn eden_tool_connection(base_url: &str, bearer_token: &str) -> Result<LlmToolConnection, EpError> {
    let base_url = base_url.trim().trim_end_matches('/');
    if base_url.is_empty() {
        return Err(EpError::request("Eden tools base URL is empty"));
    }

    let bearer_token = canonicalize_bearer_token(bearer_token);
    if bearer_token.is_empty() {
        return Err(EpError::request("bearer token missing for Eden tool server"));
    }

    Ok(LlmToolConnection {
        client_key: EDEN_TOOLS_SERVER_KEY.to_string(),
        tools_url: format!("{base_url}/api/v1/tools/eden"),
        bearer_token,
        endpoint_uuid: None,
        endpoint_name: Some(EDEN_TOOLS_SERVER_NAME.to_string()),
        endpoint_description: Some(EDEN_TOOLS_SERVER_DESCRIPTION.to_string()),
        endpoint_kind: None,
        trust_annotations: true,
        skip_ssrf_validation: true,
    })
}

pub fn normalize_endpoint_list(values: Vec<String>) -> Vec<String> {
    let mut normalized: Vec<String> = values.into_iter().map(|value| value.trim().to_string()).filter(|value| !value.is_empty()).collect();
    normalized.sort();
    normalized.dedup();
    normalized
}

pub fn prefer_specific_tool_endpoints(values: Vec<String>) -> Vec<String> {
    let mut eden = Vec::new();
    let mut specific = Vec::new();

    for value in values {
        if value == EDEN_TOOLS_SERVER_KEY {
            eden.push(value);
        } else {
            specific.push(value);
        }
    }

    eden.extend(specific);
    eden
}

pub fn normalize_and_filter_endpoint_list(values: Vec<String>, allowed: &HashSet<Uuid>) -> Vec<String> {
    let filtered = normalize_endpoint_list(values)
        .into_iter()
        .filter(|value| value == EDEN_TOOLS_SERVER_KEY || Uuid::parse_str(value).ok().map(|uuid| allowed.contains(&uuid)).unwrap_or(false))
        .collect();
    prefer_specific_tool_endpoints(filtered)
}

pub fn normalize_and_validate_endpoint_list(values: Vec<String>, allowed: &HashSet<Uuid>) -> Result<Vec<String>, EpError> {
    let normalized = normalize_endpoint_list(values);
    for value in &normalized {
        if value == EDEN_TOOLS_SERVER_KEY {
            continue;
        }
        let uuid = Uuid::parse_str(value).map_err(|e| EpError::parse(format!("invalid tool endpoint UUID '{value}': {e}")))?;
        if !allowed.contains(&uuid) {
            return Err(EpError::rbac(format!("tool endpoint is not permitted for this user: {value}")));
        }
    }
    Ok(prefer_specific_tool_endpoints(normalized))
}

/// Build an [`LlmClient`](endpoint_core::llm_core::comm::LlmClient) from
/// the internal LLM tier configuration.
///
/// Resolves `tier_name` via `eden_config::services().llm.resolve_tier()`.
/// Returns `Ok(None)` when the tier is not configured or the model is empty,
/// allowing callers to fall back to the customer's client.
pub fn build_internal_tier_client(tier_name: &str) -> Result<Option<endpoint_core::llm_core::comm::LlmClient>, EpError> {
    use endpoint_core::llm_core::ResolvedLlmConnection;
    use endpoint_core::llm_core::connection::{LlmConnectionDefaults, LlmProvider};

    let llm_cfg = eden_config::services().llm.clone();
    let Some(resolved) = llm_cfg.resolve_tier(tier_name) else {
        return Ok(None);
    };

    let provider = resolved.provider.as_deref().unwrap_or("anthropic").parse::<LlmProvider>().map_err(|_| {
        EpError::request(format!(
            "internal LLM tier `{}` has unknown provider `{}`",
            tier_name,
            resolved.provider.as_deref().unwrap_or("")
        ))
    })?;

    let resolved_connection = ResolvedLlmConnection {
        provider,
        credential_id: None,
        api_key: resolved.api_key,
        credential_base_url: resolved.base_url,
        defaults: LlmConnectionDefaults {
            model: resolved.model,
            temperature: None,
            max_tokens: None,
            top_p: None,
            top_k: None,
            base_url_override: None,
        },
        provider_config: endpoint_core::llm_core::credential::ResolvedProviderConfig::None,
    };

    let connection = std::sync::Arc::new(std::sync::RwLock::new(resolved_connection));
    let client = endpoint_core::llm_core::comm::LlmClient::new(connection, 1)?;
    Ok(Some(client))
}

pub async fn tool_runtime_with_timeout(server_data: &ServerData, connections: &[LlmToolConnection]) -> Result<ToolRuntime, EpError> {
    let fut = ToolRuntime::new(connections);
    if let Some(duration) = server_data.tools_service_timeout() {
        let result = time::timeout(duration, fut)
            .await
            .map_err(|_| EpError::Timeout(TimeoutError::Custom(format!("tool discovery timed out after {:?}", duration))))?;
        return result;
    }

    fut.await
}

/// Tool runtime construction with an external discovery cache.
///
/// When a cache is provided, tool binding discovery results are cached to
/// avoid redundant HTTP calls to tool servers on every request.
pub async fn tool_runtime_with_cache_and_timeout(
    server_data: &ServerData,
    connections: &[LlmToolConnection],
    cache: std::sync::Arc<dyn endpoint_core::llm_core::ToolDiscoveryCache>,
) -> Result<ToolRuntime, EpError> {
    let fut = ToolRuntime::new_with_cache(connections, cache);
    if let Some(duration) = server_data.tools_service_timeout() {
        let result = time::timeout(duration, fut)
            .await
            .map_err(|_| EpError::Timeout(TimeoutError::Custom(format!("tool discovery timed out after {:?}", duration))))?;
        return result;
    }

    fut.await
}

/// Internal-cache implementation of [`ToolDiscoveryCache`] for production builds.
///
/// Cache keys are scoped to `org_uuid` and `endpoint_uuid` for tenant
/// isolation and targeted endpoint invalidation. Entries expire after
/// `ttl_secs` and can be explicitly invalidated on DDL events.
pub struct ShardToolDiscoveryCache {
    cache: database::internal_cache::InternalCache,
    org_uuid: String,
    ttl_secs: u64,
}

const TOOL_DISCOVERY_NAMESPACE: &[u8] = b"eden-tool-discovery";

impl ShardToolDiscoveryCache {
    pub fn new(cache: database::internal_cache::InternalCache, org_uuid: String, ttl_secs: u64) -> Self {
        Self { cache, org_uuid, ttl_secs }
    }

    fn cache_key(&self, connection: &LlmToolConnection) -> Bytes {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::hash::DefaultHasher::new();
        connection.client_key.hash(&mut hasher);
        connection.tools_url.hash(&mut hasher);
        connection.bearer_token.len().hash(&mut hasher);
        connection.bearer_token.as_bytes().first().hash(&mut hasher);
        let hash = hasher.finish();
        let endpoint_uuid = connection.endpoint_uuid.as_deref().unwrap_or_default();
        let mut key = BytesMut::with_capacity(self.org_uuid.len() + 1 + endpoint_uuid.len() + 1 + 8);
        key.extend_from_slice(self.org_uuid.as_bytes());
        key.put_u8(0);
        key.extend_from_slice(endpoint_uuid.as_bytes());
        key.put_u8(0);
        key.put_u64(hash);
        key.freeze()
    }

    fn cache_prefix(&self, endpoint_uuid: &str) -> Bytes {
        let mut prefix = BytesMut::with_capacity(self.org_uuid.len() + 1 + endpoint_uuid.len() + 1);
        prefix.extend_from_slice(self.org_uuid.as_bytes());
        prefix.put_u8(0);
        prefix.extend_from_slice(endpoint_uuid.as_bytes());
        prefix.put_u8(0);
        prefix.freeze()
    }
}

#[async_trait::async_trait]
impl endpoint_core::llm_core::ToolDiscoveryCache for ShardToolDiscoveryCache {
    async fn get_bindings(&self, connection: &LlmToolConnection) -> Option<Vec<endpoint_core::llm_core::LlmToolBinding>> {
        let key = self.cache_key(connection);
        self.cache.json_kv_get(TOOL_DISCOVERY_NAMESPACE, key).await.ok().flatten()
    }

    async fn set_bindings(&self, connection: &LlmToolConnection, bindings: &[endpoint_core::llm_core::LlmToolBinding]) {
        let key = self.cache_key(connection);
        let _ = self.cache.json_kv_set_ex(TOOL_DISCOVERY_NAMESPACE, key, bindings, self.ttl_secs).await;
    }

    async fn invalidate_endpoint(&self, endpoint_uuid: &str) {
        let _ = self.cache.json_kv_del_with_prefix(TOOL_DISCOVERY_NAMESPACE, self.cache_prefix(endpoint_uuid)).await;
    }
}

pub fn prompt_fingerprint<T: Serialize>(value: &T) -> Option<String> {
    let serialized = serde_json::to_vec(value).ok()?;
    let digest = Sha256::digest(serialized);
    Some(hex::encode(digest))
}

pub fn elapsed_millis(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

pub fn json_size_u32<T: Serialize>(value: &T) -> u32 {
    serde_json::to_vec(value).map(|encoded| u32::try_from(encoded.len()).unwrap_or(u32::MAX)).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::canonicalize_bearer_token;

    #[test]
    fn canonicalize_bearer_token_strips_prefix_and_whitespace() {
        assert_eq!(canonicalize_bearer_token("  Bearer   token-value  "), "token-value");
        assert_eq!(canonicalize_bearer_token("bearer another-token"), "another-token");
    }

    #[test]
    fn canonicalize_bearer_token_returns_trimmed_value_when_no_prefix() {
        assert_eq!(canonicalize_bearer_token("  raw-token  "), "raw-token");
    }
}
