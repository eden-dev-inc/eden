use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::sql_file;
use base64::Engine;
use chrono::{DateTime, Utc};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::{EdenUuid, OrganizationUuid, UserUuid};
use eden_core::telemetry::FastSpanStatus;
use eden_core::telemetry::TelemetryWrapper;
#[cfg(embedded_db)]
use ep_core::database::schema::Row;
use function_name::named;
use llm_core::connection::LlmProvider;
use llm_core::{CustomPiiTerm, LlmKvCacheMode, LlmRouteOptimizationMode, LlmToolBinding, PolicyAction, PriceArbitrageMode};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
#[cfg(not(embedded_db))]
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct StoredExecutionRun {
    pub id: Uuid,
    pub organization_uuid: Uuid,
    pub principal_type: String,
    pub principal_id: Uuid,
    pub endpoint_uuid: Uuid,
    pub trigger_kind: String,
    pub trigger_metadata: Value,
    pub conversation_id: Option<Uuid>,
    pub agent_id: Option<Uuid>,
    pub request_payload: Value,
    pub state: String,
    pub plan: Option<Value>,
    pub checkpoint: Option<Value>,
    pub response_text: Option<String>,
    pub error: Option<String>,
    pub duration_ms: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct StoredEvidenceRecord {
    pub id: Uuid,
    pub run_id: Uuid,
    pub step_index: i32,
    pub kind: String,
    pub payload: Value,
    pub source: Option<String>,
    pub timestamp_ms: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StoredRunEvent {
    pub id: Uuid,
    pub run_id: Uuid,
    pub event_type: String,
    pub payload: Value,
    pub tokens_used: Option<i32>,
    pub trace_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StoredTriggerSource {
    pub id: Uuid,
    pub organization_uuid: Uuid,
    pub name: String,
    pub source_type: String,
    pub config: Value,
    pub hmac_secret: Option<String>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StoredTriggerEvent {
    pub id: Uuid,
    pub source_id: Uuid,
    pub event_type: String,
    pub payload: Value,
    pub idempotency_key: Option<String>,
    pub correlation_id: Option<Uuid>,
    pub matched_agent_id: Option<Uuid>,
    pub matched_run_id: Option<Uuid>,
    pub state: String,
    pub received_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct StoredAgentTriggerRule {
    pub id: Uuid,
    pub agent_id: Uuid,
    pub source_id: Uuid,
    pub event_type_filter: Option<String>,
    pub payload_filter: Option<Value>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StoredSystemPrompt {
    pub id: Uuid,
    pub prompt_key: String,
    pub display_name: String,
    pub description: Option<String>,
    pub prompt: String,
    pub is_active: bool,
    pub is_default: bool,
}

#[derive(Debug, Clone)]
pub struct StoredUserDbCredential {
    pub id: Uuid,
    pub user_uuid: Uuid,
    pub organization_uuid: Uuid,
    pub endpoint_uuid: Uuid,
    pub db_username: String,
    pub db_password_encrypted: Vec<u8>,
    pub auth_method: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StoredSkill {
    pub id: Uuid,
    pub name: String,
    pub display_name: String,
    pub description: String,
    pub body_markdown: String,
    pub tags: Vec<String>,
    pub estimated_tokens: i32,
    pub source_format: String,
    pub is_active: bool,
    pub source_provider: String,
    pub source_repo_url: Option<String>,
    pub source_path: Option<String>,
    pub source_ref: Option<String>,
    pub source_url: Option<String>,
    pub skill_tier: String,
    pub endpoint_kind: Option<String>,
    /// Owning organisation. `None` means the row is a global skill visible
    /// to every tenant; `Some(org)` is a tenant-private skill.
    pub organization_uuid: Option<Uuid>,
}

pub struct NewSkill<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
    pub description: &'a str,
    pub body_markdown: &'a str,
    pub tags: Vec<String>,
    pub estimated_tokens: i32,
    pub source_format: &'a str,
    pub is_active: bool,
    pub source_provider: &'a str,
    pub source_repo_url: Option<&'a str>,
    pub source_path: Option<&'a str>,
    pub source_ref: Option<&'a str>,
    pub source_url: Option<&'a str>,
    pub skill_tier: &'a str,
    pub endpoint_kind: Option<&'a str>,
    /// Owning organisation. `None` creates a global skill (operator-only
    /// writes); `Some(org)` creates/updates a tenant-private row.
    pub organization_uuid: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct StoredUserToolsEndpoint {
    pub id: Uuid,
    pub organization_uuid: OrganizationUuid,
    pub created_by: UserUuid,
    pub name: String,
    pub description: Option<String>,
    pub client_key: String,
    pub tools_url: String,
    pub bearer_token: String,
    pub tool_snapshot: Vec<LlmToolBinding>,
    pub validated_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewUserToolsEndpoint<'a> {
    pub id: Uuid,
    pub organization_uuid: &'a OrganizationUuid,
    pub created_by: &'a UserUuid,
    pub name: &'a str,
    pub description: Option<&'a str>,
    pub client_key: &'a str,
    pub tools_url: &'a str,
    pub bearer_token: &'a str,
    pub tool_snapshot: &'a [LlmToolBinding],
    pub validated_at: Option<DateTime<Utc>>,
    pub last_error: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct StoredLlmCredential {
    pub id: Uuid,
    pub organization_uuid: OrganizationUuid,
    pub provider: LlmProvider,
    pub label: Option<String>,
    pub description: Option<String>,
    pub base_url: Option<String>,
    pub api_key: String,
    pub deleted_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewLlmCredential<'a> {
    pub id: Uuid,
    pub organization_uuid: &'a OrganizationUuid,
    pub provider: LlmProvider,
    pub label: Option<&'a str>,
    pub description: Option<&'a str>,
    pub base_url: Option<&'a str>,
    pub api_key: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredLlmGatewayApiKey {
    pub id: Uuid,
    pub organization_uuid: OrganizationUuid,
    pub name: String,
    pub key_hash: String,
    pub key_prefix: String,
    pub endpoint_uuid: Uuid,
    pub agent_uuid: Uuid,
    pub model_allowlist: Option<Vec<String>>,
    pub rate_limit_rpm: Option<u32>,
    pub budget_tokens_monthly: Option<u64>,
    pub pii_policy: PolicyAction,
    /// User-defined PII dictionary terms with per-term enforcement actions.
    pub custom_pii_terms: Vec<CustomPiiTerm>,
    pub price_arbitrage_mode: PriceArbitrageMode,
    pub response_cache_ttl_secs: Option<u64>,
    pub route_optimization_mode: LlmRouteOptimizationMode,
    pub kv_cache_mode: LlmKvCacheMode,
    pub kv_cache_ttl_secs: Option<u64>,
    pub route_switch_threshold_percent: Option<u8>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

/// Organization-wide custom PII dictionary, applied to every agent in the org
/// (per-agent `custom_pii_terms` add to it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredOrgPiiDictionary {
    pub organization_uuid: OrganizationUuid,
    pub terms: Vec<CustomPiiTerm>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredLlmGatewayResponseCacheEntry {
    pub cache_key: String,
    pub organization_uuid: OrganizationUuid,
    pub endpoint_uuid: Uuid,
    pub key_id: Uuid,
    pub provider: String,
    pub model: String,
    pub request_hash: String,
    pub prompt_fingerprint: Option<String>,
    pub response_json: Value,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub estimated_cost_micros: u64,
    pub hit_count: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub last_hit_at: Option<DateTime<Utc>>,
}

pub struct NewLlmGatewayResponseCacheEntry<'a> {
    pub cache_key: &'a str,
    pub organization_uuid: &'a OrganizationUuid,
    pub endpoint_uuid: Uuid,
    pub key_id: Uuid,
    pub provider: &'a str,
    pub model: &'a str,
    pub request_hash: &'a str,
    pub prompt_fingerprint: Option<&'a str>,
    pub response_json: &'a Value,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub estimated_cost_micros: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredLlmGatewayRouteRollup {
    pub organization_uuid: OrganizationUuid,
    pub endpoint_uuid: Uuid,
    pub provider: String,
    pub model: String,
    pub route_class: String,
    pub success_count: u64,
    pub error_count: u64,
    pub total_latency_ms: u64,
    pub min_latency_ms: u64,
    pub max_latency_ms: u64,
    pub total_output_tokens: u64,
    pub total_duration_ms: u64,
    pub first_observed_at: DateTime<Utc>,
    pub last_observed_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredLlmGatewayUsageRollup {
    pub organization_uuid: OrganizationUuid,
    pub consumer_kind: String,
    pub consumer_id: String,
    pub month_bucket: i32,
    pub endpoint_uuid: Option<Uuid>,
    pub request_count: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub estimated_cost_micros: u64,
    pub cache_hit_count: u64,
    pub kv_cache_hit_count: u64,
    pub rate_limited_count: u64,
    pub updated_at: DateTime<Utc>,
}

pub struct NewLlmGatewayUsageRollup<'a> {
    pub organization_uuid: &'a OrganizationUuid,
    pub consumer_kind: &'a str,
    pub consumer_id: &'a str,
    pub month_bucket: i32,
    pub endpoint_uuid: Option<Uuid>,
    pub request_count: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub estimated_cost_micros: u64,
    pub cache_hit_count: u64,
    pub kv_cache_hit_count: u64,
    pub rate_limited_count: u64,
    pub updated_at: DateTime<Utc>,
}

fn parse_provider(value: &str) -> Result<LlmProvider, EpError> {
    value
        .parse::<LlmProvider>()
        .map_err(|_| EpError::database(format!("Unsupported LLM provider `{}` stored in credential", value)))
}

fn parse_stored_llm_enum<T>(field: &str, value: String) -> Result<T, EpError>
where
    T: DeserializeOwned,
{
    serde_json::from_value(Value::String(value.clone()))
        .map_err(|e| EpError::database(format!("Unsupported LLM gateway {field} `{value}` stored in database: {e}")))
}

fn optional_i64_to_u32(field: &str, value: Option<i64>) -> Result<Option<u32>, EpError> {
    value
        .map(|value| {
            u32::try_from(value).map_err(|_| EpError::database(format!("LLM gateway {field} stored value `{value}` is outside u32 range")))
        })
        .transpose()
}

fn optional_i64_to_u64(field: &str, value: Option<i64>) -> Result<Option<u64>, EpError> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| EpError::database(format!("LLM gateway {field} stored value `{value}` is outside u64 range")))
        })
        .transpose()
}

fn optional_i64_to_u8(field: &str, value: Option<i64>) -> Result<Option<u8>, EpError> {
    value
        .map(|value| {
            u8::try_from(value).map_err(|_| EpError::database(format!("LLM gateway {field} stored value `{value}` is outside u8 range")))
        })
        .transpose()
}

fn i64_to_u64(field: &str, value: i64) -> Result<u64, EpError> {
    u64::try_from(value).map_err(|_| EpError::database(format!("LLM gateway {field} stored value `{value}` is outside u64 range")))
}

fn optional_u64_to_i64(field: &str, value: Option<u64>) -> Result<Option<i64>, EpError> {
    value
        .map(|value| {
            i64::try_from(value).map_err(|_| EpError::request(format!("LLM gateway {field} value `{value}` exceeds durable storage range")))
        })
        .transpose()
}

fn u64_to_i64(field: &str, value: u64) -> Result<i64, EpError> {
    i64::try_from(value).map_err(|_| EpError::request(format!("LLM gateway {field} value `{value}` exceeds durable storage range")))
}

const SECRET_SENTINEL: &str = "enc:v1:";
const ORG_SECRET_SENTINEL: &str = "enc:org:v1:";

fn preferred_org_secret_key_ref(org_uuid: Uuid) -> String {
    let base_key_ref = eden_config::encryption().org_key_env_var.clone();
    format!("{base_key_ref}__{}", org_uuid.simple().to_string().to_uppercase())
}

fn configured_secret_material() -> Result<Vec<u8>, EpError> {
    if let Ok(value) = std::env::var("EDEN_DB_ENCRYPTION_KEY") {
        if let Ok(bytes) = hex::decode(&value)
            && bytes.len() == crate::db::encryption::KEY_SIZE
        {
            return Ok(bytes);
        }
        if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(value.as_bytes())
            && bytes.len() == crate::db::encryption::KEY_SIZE
        {
            return Ok(bytes);
        }
        return Ok(value.into_bytes());
    }

    Err(EpError::auth(
        "EDEN_DB_ENCRYPTION_KEY must be set to decrypt legacy shared-key ciphertext".to_string(),
    ))
}

fn credential_encryption_key() -> Result<[u8; crate::db::encryption::KEY_SIZE], EpError> {
    let digest = Sha256::digest(configured_secret_material()?);
    let mut key = [0u8; crate::db::encryption::KEY_SIZE];
    key.copy_from_slice(&digest);
    Ok(key)
}

fn decrypt_secret_string(secret: &str) -> Result<String, EpError> {
    let Some(encoded) = secret.strip_prefix(SECRET_SENTINEL) else {
        return Ok(secret.to_string());
    };

    let key = credential_encryption_key()?;
    let ciphertext = base64::engine::general_purpose::STANDARD
        .decode(encoded.as_bytes())
        .map_err(|e| EpError::parse(format!("invalid encrypted secret encoding: {e}")))?;
    let plaintext = crate::db::encryption::decrypt_with_key(&key, &ciphertext)?;
    String::from_utf8(plaintext).map_err(|e| EpError::parse(format!("decrypted secret is not valid UTF-8: {e}")))
}

fn decrypt_secret_bytes(secret: &[u8]) -> Result<Vec<u8>, EpError> {
    let Some(encoded) = secret.strip_prefix(SECRET_SENTINEL.as_bytes()) else {
        return Ok(secret.to_vec());
    };

    let key = credential_encryption_key()?;
    let ciphertext = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|e| EpError::parse(format!("invalid encrypted secret encoding: {e}")))?;
    crate::db::encryption::decrypt_with_key(&key, &ciphertext)
}

fn row_to_credential(row: &Row) -> Result<StoredLlmCredential, EpError> {
    let provider = parse_provider(row.get::<_, String>("provider").as_str())?;
    Ok(StoredLlmCredential {
        id: row.get("id"),
        organization_uuid: OrganizationUuid::from(row.get::<_, Uuid>("organization_uuid")),
        provider,
        label: row.get("label"),
        description: row.get("description"),
        base_url: row.get("base_url"),
        api_key: row.get("api_key"),
        deleted_at: row.get("deleted_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn row_to_llm_gateway_api_key(row: &Row) -> Result<StoredLlmGatewayApiKey, EpError> {
    Ok(StoredLlmGatewayApiKey {
        id: row.get("id"),
        organization_uuid: OrganizationUuid::from(row.get::<_, Uuid>("organization_uuid")),
        name: row.get("name"),
        key_hash: row.get("key_hash"),
        key_prefix: row.get("key_prefix"),
        endpoint_uuid: row.get("endpoint_uuid"),
        agent_uuid: row.get("agent_uuid"),
        model_allowlist: row.get("model_allowlist"),
        rate_limit_rpm: optional_i64_to_u32("rate_limit_rpm", row.get("rate_limit_rpm"))?,
        budget_tokens_monthly: optional_i64_to_u64("budget_tokens_monthly", row.get("budget_tokens_monthly"))?,
        pii_policy: parse_stored_llm_enum("pii_policy", row.get("pii_policy"))?,
        custom_pii_terms: serde_json::from_value(row.get("custom_pii_terms")).unwrap_or_default(),
        price_arbitrage_mode: parse_stored_llm_enum("price_arbitrage_mode", row.get("price_arbitrage_mode"))?,
        response_cache_ttl_secs: optional_i64_to_u64("response_cache_ttl_secs", row.get("response_cache_ttl_secs"))?,
        route_optimization_mode: parse_stored_llm_enum("route_optimization_mode", row.get("route_optimization_mode"))?,
        kv_cache_mode: parse_stored_llm_enum("kv_cache_mode", row.get("kv_cache_mode"))?,
        kv_cache_ttl_secs: optional_i64_to_u64("kv_cache_ttl_secs", row.get("kv_cache_ttl_secs"))?,
        route_switch_threshold_percent: optional_i64_to_u8("route_switch_threshold_percent", row.get("route_switch_threshold_percent"))?,
        enabled: row.get("enabled"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        last_used_at: row.get("last_used_at"),
    })
}

fn row_to_org_pii_dictionary(row: &Row) -> StoredOrgPiiDictionary {
    StoredOrgPiiDictionary {
        organization_uuid: OrganizationUuid::from(row.get::<_, Uuid>("organization_uuid")),
        terms: serde_json::from_value(row.get("terms")).unwrap_or_default(),
        updated_at: row.get("updated_at"),
    }
}

fn row_to_llm_gateway_response_cache(row: &Row) -> Result<StoredLlmGatewayResponseCacheEntry, EpError> {
    Ok(StoredLlmGatewayResponseCacheEntry {
        cache_key: row.get("cache_key"),
        organization_uuid: OrganizationUuid::from(row.get::<_, Uuid>("organization_uuid")),
        endpoint_uuid: row.get("endpoint_uuid"),
        key_id: row.get("key_id"),
        provider: row.get("provider"),
        model: row.get("model"),
        request_hash: row.get("request_hash"),
        prompt_fingerprint: row.get("prompt_fingerprint"),
        response_json: row.get("response_json"),
        prompt_tokens: i64_to_u64("prompt_tokens", row.get("prompt_tokens"))?,
        completion_tokens: i64_to_u64("completion_tokens", row.get("completion_tokens"))?,
        total_tokens: i64_to_u64("total_tokens", row.get("total_tokens"))?,
        estimated_cost_micros: i64_to_u64("estimated_cost_micros", row.get("estimated_cost_micros"))?,
        hit_count: i64_to_u64("hit_count", row.get("hit_count"))?,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        expires_at: row.get("expires_at"),
        last_hit_at: row.get("last_hit_at"),
    })
}

fn row_to_llm_gateway_route_rollup(row: &Row) -> Result<StoredLlmGatewayRouteRollup, EpError> {
    Ok(StoredLlmGatewayRouteRollup {
        organization_uuid: OrganizationUuid::from(row.get::<_, Uuid>("organization_uuid")),
        endpoint_uuid: row.get("endpoint_uuid"),
        provider: row.get("provider"),
        model: row.get("model"),
        route_class: row.get("route_class"),
        success_count: i64_to_u64("success_count", row.get("success_count"))?,
        error_count: i64_to_u64("error_count", row.get("error_count"))?,
        total_latency_ms: i64_to_u64("total_latency_ms", row.get("total_latency_ms"))?,
        min_latency_ms: i64_to_u64("min_latency_ms", row.get("min_latency_ms"))?,
        max_latency_ms: i64_to_u64("max_latency_ms", row.get("max_latency_ms"))?,
        total_output_tokens: i64_to_u64("total_output_tokens", row.get("total_output_tokens"))?,
        total_duration_ms: i64_to_u64("total_duration_ms", row.get("total_duration_ms"))?,
        first_observed_at: row.get("first_observed_at"),
        last_observed_at: row.get("last_observed_at"),
        updated_at: row.get("updated_at"),
    })
}

fn row_to_llm_gateway_usage_rollup(row: &Row) -> Result<StoredLlmGatewayUsageRollup, EpError> {
    Ok(StoredLlmGatewayUsageRollup {
        organization_uuid: OrganizationUuid::from(row.get::<_, Uuid>("organization_uuid")),
        consumer_kind: row.get("consumer_kind"),
        consumer_id: row.get("consumer_id"),
        month_bucket: row.get("month_bucket"),
        endpoint_uuid: row.get("endpoint_uuid"),
        request_count: i64_to_u64("request_count", row.get("request_count"))?,
        prompt_tokens: i64_to_u64("prompt_tokens", row.get("prompt_tokens"))?,
        completion_tokens: i64_to_u64("completion_tokens", row.get("completion_tokens"))?,
        total_tokens: i64_to_u64("total_tokens", row.get("total_tokens"))?,
        estimated_cost_micros: i64_to_u64("estimated_cost_micros", row.get("estimated_cost_micros"))?,
        cache_hit_count: i64_to_u64("cache_hit_count", row.get("cache_hit_count"))?,
        kv_cache_hit_count: i64_to_u64("kv_cache_hit_count", row.get("kv_cache_hit_count"))?,
        rate_limited_count: i64_to_u64("rate_limited_count", row.get("rate_limited_count"))?,
        updated_at: row.get("updated_at"),
    })
}

fn row_to_trigger_source(row: &Row) -> StoredTriggerSource {
    StoredTriggerSource {
        id: row.get("id"),
        organization_uuid: row.get("organization_uuid"),
        name: row.get("name"),
        source_type: row.get("source_type"),
        config: row.get("config"),
        hmac_secret: row.get("hmac_secret"),
        is_active: row.get("is_active"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn row_to_trigger_event(row: &Row) -> StoredTriggerEvent {
    StoredTriggerEvent {
        id: row.get("id"),
        source_id: row.get("source_id"),
        event_type: row.get("event_type"),
        payload: row.get("payload"),
        idempotency_key: row.get("idempotency_key"),
        correlation_id: row.get("correlation_id"),
        matched_agent_id: row.get("matched_agent_id"),
        matched_run_id: row.get("matched_run_id"),
        state: row.get("state"),
        received_at: row.get("received_at"),
        processed_at: row.get("processed_at"),
    }
}

fn row_to_agent_trigger_rule(row: &Row) -> StoredAgentTriggerRule {
    StoredAgentTriggerRule {
        id: row.get("id"),
        agent_id: row.get("agent_id"),
        source_id: row.get("source_id"),
        event_type_filter: row.get("event_type_filter"),
        payload_filter: row.get("payload_filter"),
        is_active: row.get("is_active"),
        created_at: row.get("created_at"),
    }
}

fn row_to_user_db_credential(row: &Row) -> StoredUserDbCredential {
    StoredUserDbCredential {
        id: row.get("id"),
        user_uuid: row.get("user_uuid"),
        organization_uuid: row.get("organization_uuid"),
        endpoint_uuid: row.get("endpoint_uuid"),
        db_username: row.get("db_username"),
        db_password_encrypted: row.get("db_password_encrypted"),
        auth_method: row.get("auth_method"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

#[derive(Debug, Clone)]
pub struct StoredApprovalRequest {
    pub id: Uuid,
    pub run_id: Uuid,
    pub organization_uuid: Uuid,
    pub requested_by: Uuid,
    pub plan: Value,
    pub state: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub delegated_to: Option<Uuid>,
    pub required_approvals: i32,
    pub approval_count: i32,
    pub change_window_start: Option<DateTime<Utc>>,
    pub change_window_end: Option<DateTime<Utc>>,
    pub justification: Option<String>,
    pub decided_by: Option<Uuid>,
    pub decided_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

fn row_to_approval_request(row: &Row) -> StoredApprovalRequest {
    StoredApprovalRequest {
        id: row.get("id"),
        run_id: row.get("run_id"),
        organization_uuid: row.get("organization_uuid"),
        requested_by: row.get("requested_by"),
        plan: row.get("plan"),
        state: row.get("state"),
        expires_at: row.get("expires_at"),
        delegated_to: row.get("delegated_to"),
        required_approvals: row.get("required_approvals"),
        approval_count: row.get("approval_count"),
        change_window_start: row.get("change_window_start"),
        change_window_end: row.get("change_window_end"),
        justification: row.get("justification"),
        decided_by: row.get("decided_by"),
        decided_at: row.get("decided_at"),
        created_at: row.get("created_at"),
    }
}

fn row_to_execution_run(row: &Row) -> StoredExecutionRun {
    StoredExecutionRun {
        id: row.get("id"),
        organization_uuid: row.get("organization_uuid"),
        principal_type: row.get("principal_type"),
        principal_id: row.get("principal_id"),
        endpoint_uuid: row.get("endpoint_uuid"),
        trigger_kind: row.get("trigger_kind"),
        trigger_metadata: row.get("trigger_metadata"),
        conversation_id: row.get("conversation_id"),
        agent_id: row.get("agent_id"),
        request_payload: row.get("request_payload"),
        state: row.get("state"),
        plan: row.get("plan"),
        checkpoint: row.get("checkpoint"),
        response_text: row.get("response_text"),
        error: row.get("error"),
        duration_ms: row.get("duration_ms"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        completed_at: row.get("completed_at"),
    }
}

#[derive(Debug, Clone)]
pub struct UpdateLlmCredential<'a> {
    pub organization_uuid: &'a OrganizationUuid,
    pub credential_id: Uuid,
    pub label: Option<&'a str>,
    pub description: Option<&'a str>,
    pub base_url: Option<&'a str>,
    pub api_key: Option<&'a str>,
}

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    fn row_to_system_prompt(row: &Row) -> StoredSystemPrompt {
        StoredSystemPrompt {
            id: row.get("id"),
            prompt_key: row.get("prompt_key"),
            display_name: row.get("display_name"),
            description: row.get("description"),
            prompt: row.get("prompt"),
            is_active: row.get("is_active"),
            is_default: row.get("is_default"),
        }
    }

    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    #[named]
    pub async fn upsert_llm_system_prompt(
        &self,
        id: Uuid,
        prompt_key: &str,
        display_name: &str,
        description: Option<&str>,
        prompt: &str,
        is_active: bool,
        is_default: bool,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredSystemPrompt> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_one(
                sql_file!("insert", "llm/system_prompt"),
                &[&id, &prompt_key, &display_name, &description, &prompt, &is_active, &is_default],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(Self::row_to_system_prompt(&row))
    }

    #[named]
    pub async fn list_llm_system_prompts(&self, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Vec<StoredSystemPrompt>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/system_prompts"), &[]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.into_iter().map(|row| Self::row_to_system_prompt(&row)).collect())
    }

    #[named]
    pub async fn get_llm_system_prompt_by_key(
        &self,
        prompt_key: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredSystemPrompt>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("select", "llm/system_prompt_by_key"), &[&prompt_key]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.map(|row| Self::row_to_system_prompt(&row)))
    }

    pub async fn list_llm_system_prompts_for_org(
        &self,
        _organization_uuid: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredSystemPrompt>> {
        // System prompts are currently stored globally. Until the storage schema
        // grows organization scoping, reuse the global listing path instead of
        // querying against a non-existent organization_uuid column.
        self.list_llm_system_prompts(telemetry_wrapper).await
    }

    pub async fn get_llm_system_prompt_by_key_for_org(
        &self,
        _organization_uuid: Uuid,
        prompt_key: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredSystemPrompt>> {
        // System prompts are currently stored globally. Until the storage schema
        // grows organization scoping, reuse the global lookup path instead of
        // querying against a non-existent organization_uuid column.
        self.get_llm_system_prompt_by_key(prompt_key, telemetry_wrapper).await
    }

    // ── Skills ──

    fn row_to_skill(row: &Row) -> StoredSkill {
        StoredSkill {
            id: row.get("id"),
            name: row.get("name"),
            display_name: row.get("display_name"),
            description: row.get("description"),
            body_markdown: row.get("body_markdown"),
            tags: row.get("tags"),
            estimated_tokens: row.get("estimated_tokens"),
            source_format: row.get("source_format"),
            is_active: row.get("is_active"),
            source_provider: row.get("source_provider"),
            source_repo_url: row.get("source_repo_url"),
            source_path: row.get("source_path"),
            source_ref: row.get("source_ref"),
            source_url: row.get("source_url"),
            skill_tier: row.get("skill_tier"),
            endpoint_kind: row.get("endpoint_kind"),
            organization_uuid: row.get("organization_uuid"),
        }
    }

    #[named]
    pub async fn upsert_skill(&self, id: Uuid, skill: NewSkill<'_>, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<StoredSkill> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        // Upsert SQL branches on whether the row is global or tenant-scoped
        // so the ON CONFLICT clause can reference the matching partial-unique
        // index directly rather than fighting a COALESCE expression.
        let sql_key: &str = if skill.organization_uuid.is_some() {
            "insert_tenant"
        } else {
            "insert_global"
        };
        let sql = match sql_key {
            "insert_tenant" => sql_file!("insert", "llm/skill_tenant"),
            _ => sql_file!("insert", "llm/skill"),
        };
        // The global-skill insert (`llm/skill`) references only $1..$16 and forces
        // organization_uuid to a NULL literal; the tenant variant (`llm/skill_tenant`)
        // binds all 17. Postgres tolerates a trailing unused bind, but libsql/SQLite
        // rejects it ("bind index 17 is out of bounds"), so pass exactly the params
        // the chosen statement references.
        let params: [&(dyn tokio_postgres::types::ToSql + Sync); 17] = [
            &id,
            &skill.name,
            &skill.display_name,
            &skill.description,
            &skill.body_markdown,
            &skill.tags,
            &skill.estimated_tokens,
            &skill.source_format,
            &skill.is_active,
            &skill.source_provider,
            &skill.source_repo_url,
            &skill.source_path,
            &skill.source_ref,
            &skill.source_url,
            &skill.skill_tier,
            &skill.endpoint_kind,
            &skill.organization_uuid,
        ];
        let bind_params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = if sql_key == "insert_tenant" { &params } else { &params[..16] };
        let row = conn.query_one(sql, bind_params).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(Self::row_to_skill(&row))
    }

    #[named]
    pub async fn list_active_skills(&self, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Vec<StoredSkill>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/skills"), &[]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.into_iter().map(|row| Self::row_to_skill(&row)).collect())
    }

    #[named]
    pub async fn list_active_skills_for_org(
        &self,
        organization_uuid: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredSkill>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/skills_for_org"), &[&organization_uuid]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.into_iter().map(|row| Self::row_to_skill(&row)).collect())
    }

    #[named]
    pub async fn list_all_skills(&self, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Vec<StoredSkill>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/all_skills"), &[]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.into_iter().map(|row| Self::row_to_skill(&row)).collect())
    }

    #[named]
    pub async fn list_all_skills_for_org(
        &self,
        organization_uuid: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredSkill>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/all_skills_for_org"), &[&organization_uuid]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.into_iter().map(|row| Self::row_to_skill(&row)).collect())
    }

    #[named]
    pub async fn get_skill_by_name(&self, name: &str, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Option<StoredSkill>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("select", "llm/skill_by_name"), &[&name]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.map(|row| Self::row_to_skill(&row)))
    }

    #[named]
    pub async fn get_skill_by_name_for_org(
        &self,
        organization_uuid: Uuid,
        name: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredSkill>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("select", "llm/skill_by_name_for_org"), &[&organization_uuid, &name]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.map(|row| Self::row_to_skill(&row)))
    }

    #[named]
    pub async fn get_skill_by_uuid(&self, id: Uuid, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Option<StoredSkill>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("select", "llm/skill_by_uuid"), &[&id]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.map(|row| Self::row_to_skill(&row)))
    }

    #[named]
    pub async fn get_skill_by_uuid_for_org(
        &self,
        organization_uuid: Uuid,
        id: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredSkill>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("select", "llm/skill_by_uuid_for_org"), &[&organization_uuid, &id]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.map(|row| Self::row_to_skill(&row)))
    }

    /// Update a skill, scoping the WHERE clause to the caller's tenant.
    ///
    /// `scope_organization_uuid` behaves like `skill_by_uuid_for_org`:
    /// - `Some(org)` matches only rows owned by `org` (global rows are
    ///   invisible to this call; that is the point of the guard).
    /// - `None` matches only global rows (`organization_uuid IS NULL`).
    ///   Reserved for operator tooling; customer-facing handlers MUST
    ///   always pass `Some(auth.org_uuid().uuid())`.
    ///
    /// Returns `Ok(None)` when the row is missing *or* belongs to a
    /// different tenant; the caller surfaces both as `skill not found`.
    #[named]
    pub async fn update_skill(
        &self,
        id: Uuid,
        scope_organization_uuid: Option<Uuid>,
        skill: NewSkill<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredSkill>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_opt(
                sql_file!("update", "llm_skill"),
                &[
                    &id,
                    &skill.display_name,
                    &skill.description,
                    &skill.body_markdown,
                    &skill.tags,
                    &skill.estimated_tokens,
                    &skill.source_format,
                    &skill.is_active,
                    &skill.source_provider,
                    &skill.source_repo_url,
                    &skill.source_path,
                    &skill.source_ref,
                    &skill.source_url,
                    &skill.skill_tier,
                    &skill.endpoint_kind,
                    &scope_organization_uuid,
                ],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(row.map(|row| Self::row_to_skill(&row)))
    }

    #[named]
    pub async fn list_skills_by_source_provider(
        &self,
        source_provider: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredSkill>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/skills_by_source_provider"), &[&source_provider]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.into_iter().map(|row| Self::row_to_skill(&row)).collect())
    }

    #[named]
    pub async fn list_skills_by_tier(&self, tier: &str, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Vec<StoredSkill>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/skills_by_tier"), &[&tier]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.into_iter().map(|row| Self::row_to_skill(&row)).collect())
    }

    /// Delete a skill, scoping the WHERE clause to the caller's tenant.
    ///
    /// Semantics of `scope_organization_uuid` match [`Self::update_skill`]:
    /// `Some(org)` only deletes rows owned by `org`; `None` only deletes
    /// globals. Returns `false` when the row is missing or belongs to a
    /// different tenant, so a cross-tenant `DELETE` is indistinguishable
    /// from a 404 to the caller.
    #[named]
    pub async fn delete_skill(
        &self,
        id: Uuid,
        scope_organization_uuid: Option<Uuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<bool> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("delete", "llm_skill"), &[&id, &scope_organization_uuid]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.is_some())
    }

    #[named]
    pub async fn insert_llm_user_tools_endpoint(
        &self,
        new_endpoint: NewUserToolsEndpoint<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredUserToolsEndpoint> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let tool_snapshot_json = serde_json::to_value(new_endpoint.tool_snapshot).map_err(EpError::serde)?;
        let encrypted_bearer_token = self
            .encrypt_secret_string_for_org(new_endpoint.organization_uuid.uuid(), new_endpoint.bearer_token, telemetry_wrapper)
            .await?;

        let row = conn
            .query_one(
                sql_file!("insert", "llm/user_tools_endpoint"),
                &[
                    &new_endpoint.id,
                    &new_endpoint.organization_uuid.uuid(),
                    &new_endpoint.created_by.uuid(),
                    &new_endpoint.name,
                    &new_endpoint.description,
                    &new_endpoint.client_key,
                    &new_endpoint.tools_url,
                    &encrypted_bearer_token,
                    &tool_snapshot_json,
                    &new_endpoint.validated_at,
                    &new_endpoint.last_error,
                ],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        self.hydrate_user_tools_endpoint_from_row(row, telemetry_wrapper).await
    }

    #[named]
    pub async fn list_user_tools_endpoints(
        &self,
        organization_uuid: &OrganizationUuid,
        user_uuid: &UserUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredUserToolsEndpoint>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn
            .query(sql_file!("select", "llm/user_tools_endpoints"), &[&organization_uuid.uuid(), &user_uuid.uuid()])
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        let mut endpoints = Vec::with_capacity(rows.len());
        for row in rows {
            endpoints.push(self.hydrate_user_tools_endpoint_from_row(row, telemetry_wrapper).await?);
        }
        Ok(endpoints)
    }

    #[named]
    pub async fn get_user_tools_endpoint(
        &self,
        endpoint_id: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredUserToolsEndpoint>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("select", "llm/user_tools_endpoint"), &[&endpoint_id]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        match row {
            Some(row) => Ok(Some(self.hydrate_user_tools_endpoint_from_row(row, telemetry_wrapper).await?)),
            None => Ok(None),
        }
    }

    #[named]
    pub async fn delete_user_tools_endpoint(
        &self,
        endpoint_id: Uuid,
        organization_uuid: &OrganizationUuid,
        user_uuid: &UserUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<bool> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let affected = conn
            .execute(
                sql_file!("delete", "llm/user_tools_endpoint"),
                &[&endpoint_id, &organization_uuid.uuid(), &user_uuid.uuid()],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(affected > 0)
    }
}

fn read_user_tools_endpoint(row: Row) -> ResultEP<StoredUserToolsEndpoint> {
    let tool_snapshot_value: Value = row.get("tool_snapshot");
    let tool_snapshot: Vec<LlmToolBinding> = serde_json::from_value(tool_snapshot_value).map_err(EpError::serde)?;
    Ok(StoredUserToolsEndpoint {
        id: row.get("id"),
        organization_uuid: OrganizationUuid::from(row.get::<_, Uuid>("organization_uuid")),
        created_by: UserUuid::from(row.get::<_, Uuid>("created_by")),
        name: row.get("name"),
        description: row.get("description"),
        client_key: row.get("client_key"),
        tools_url: row.get("tools_url"),
        bearer_token: row.get("bearer_token"),
        tool_snapshot,
        validated_at: row.get("validated_at"),
        last_error: row.get("last_error"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    async fn resolve_org_secret_key_ref(
        &self,
        organization_uuid: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<String>> {
        let Some(provider_name) = self.org_key_provider().map(|provider| provider.provider_name().to_string()) else {
            return Err(EpError::auth("organization secret provider not configured".to_string()));
        };

        let mut span = telemetry_wrapper.client_tracer("resolve_org_secret_key_ref");
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let existing = conn.query_opt(sql_file!("select", "org_key_ref"), &[&organization_uuid]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        if let Some(row) = existing {
            let key_ref: String = row.get("key_ref");
            let expected = preferred_org_secret_key_ref(organization_uuid);
            if key_ref != expected {
                return Err(EpError::auth(format!(
                    "organization secret key ref must be org-specific; expected `{expected}` but found `{key_ref}`"
                )));
            }
            return Ok(Some(key_ref));
        }

        let key_ref = preferred_org_secret_key_ref(organization_uuid);
        conn.execute(sql_file!("insert", "org_key_ref"), &[&organization_uuid, &provider_name, &key_ref]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(Some(key_ref))
    }

    async fn encrypt_secret_string_for_org(
        &self,
        organization_uuid: Uuid,
        secret: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<String> {
        let Some(key_ref) = self.resolve_org_secret_key_ref(organization_uuid, telemetry_wrapper).await? else {
            return Err(EpError::auth("organization secret key ref resolution failed".to_string()));
        };
        let provider = self.org_key_provider().ok_or_else(|| EpError::auth("organization secret provider not configured".to_string()))?;
        let ciphertext = provider.wrap(&key_ref, secret.as_bytes()).await?;
        Ok(format!("{ORG_SECRET_SENTINEL}{}", base64::engine::general_purpose::STANDARD.encode(ciphertext)))
    }

    async fn decrypt_secret_string_for_org(
        &self,
        organization_uuid: Uuid,
        secret: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<String> {
        let Some(encoded) = secret.strip_prefix(ORG_SECRET_SENTINEL) else {
            return decrypt_secret_string(secret);
        };
        let key_ref = self
            .resolve_org_secret_key_ref(organization_uuid, telemetry_wrapper)
            .await?
            .ok_or_else(|| EpError::auth("organization secret provider not configured".to_string()))?;
        let provider = self.org_key_provider().ok_or_else(|| EpError::auth("organization secret provider not configured".to_string()))?;
        let ciphertext = base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .map_err(|e| EpError::parse(format!("invalid encrypted secret encoding: {e}")))?;
        let plaintext = provider.unwrap(&key_ref, &ciphertext).await?;
        String::from_utf8(plaintext).map_err(|e| EpError::parse(format!("decrypted secret is not valid UTF-8: {e}")))
    }

    async fn encrypt_secret_bytes_for_org(
        &self,
        organization_uuid: Uuid,
        secret: &[u8],
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<u8>> {
        let Some(key_ref) = self.resolve_org_secret_key_ref(organization_uuid, telemetry_wrapper).await? else {
            return Err(EpError::auth("organization secret key ref resolution failed".to_string()));
        };
        let provider = self.org_key_provider().ok_or_else(|| EpError::auth("organization secret provider not configured".to_string()))?;
        let ciphertext = provider.wrap(&key_ref, secret).await?;
        let mut encoded = ORG_SECRET_SENTINEL.as_bytes().to_vec();
        encoded.extend_from_slice(base64::engine::general_purpose::STANDARD.encode(ciphertext).as_bytes());
        Ok(encoded)
    }

    async fn decrypt_secret_bytes_for_org(
        &self,
        organization_uuid: Uuid,
        secret: &[u8],
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<u8>> {
        let Some(encoded) = secret.strip_prefix(ORG_SECRET_SENTINEL.as_bytes()) else {
            return decrypt_secret_bytes(secret);
        };
        let key_ref = self
            .resolve_org_secret_key_ref(organization_uuid, telemetry_wrapper)
            .await?
            .ok_or_else(|| EpError::auth("organization secret provider not configured".to_string()))?;
        let provider = self.org_key_provider().ok_or_else(|| EpError::auth("organization secret provider not configured".to_string()))?;
        let ciphertext = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|e| EpError::parse(format!("invalid encrypted secret encoding: {e}")))?;
        provider.unwrap(&key_ref, &ciphertext).await
    }

    async fn hydrate_llm_credential_from_row(&self, row: Row, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<StoredLlmCredential> {
        let mut credential = row_to_credential(&row)?;
        credential.api_key =
            self.decrypt_secret_string_for_org(credential.organization_uuid.uuid(), &credential.api_key, telemetry_wrapper).await?;
        Ok(credential)
    }

    async fn hydrate_user_tools_endpoint_from_row(
        &self,
        row: Row,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredUserToolsEndpoint> {
        let mut endpoint = read_user_tools_endpoint(row)?;
        endpoint.bearer_token =
            self.decrypt_secret_string_for_org(endpoint.organization_uuid.uuid(), &endpoint.bearer_token, telemetry_wrapper).await?;
        Ok(endpoint)
    }

    async fn hydrate_user_db_credential_from_row(
        &self,
        row: Row,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredUserDbCredential> {
        let mut credential = row_to_user_db_credential(&row);
        credential.db_password_encrypted = self
            .decrypt_secret_bytes_for_org(credential.organization_uuid, &credential.db_password_encrypted, telemetry_wrapper)
            .await?;
        Ok(credential)
    }

    #[named]
    pub async fn insert_llm_credential(
        &self,
        new_credential: NewLlmCredential<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredLlmCredential> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let label = new_credential.label.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
        });
        let description = new_credential.description.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
        });
        let base_url = new_credential.base_url.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
        });
        let api_key = new_credential.api_key.trim().to_string();
        if api_key.is_empty() {
            return Err(EpError::request("api_key must not be empty"));
        }

        let encrypted_api_key =
            self.encrypt_secret_string_for_org(new_credential.organization_uuid.uuid(), &api_key, telemetry_wrapper).await?;

        let row = conn
            .query_one(
                sql_file!("insert", "llm/credential"),
                &[
                    &new_credential.id,
                    &new_credential.organization_uuid.uuid(),
                    &new_credential.provider.to_string(),
                    &label,
                    &description,
                    &base_url,
                    &encrypted_api_key,
                ],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        self.hydrate_llm_credential_from_row(row, telemetry_wrapper).await
    }

    #[named]
    pub async fn list_llm_credentials(
        &self,
        organization_uuid: &OrganizationUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredLlmCredential>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/credentials"), &[&organization_uuid.uuid()]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        let mut credentials = Vec::with_capacity(rows.len());
        for row in rows {
            credentials.push(self.hydrate_llm_credential_from_row(row, telemetry_wrapper).await?);
        }
        Ok(credentials)
    }

    #[named]
    pub async fn get_llm_credential(
        &self,
        organization_uuid: &OrganizationUuid,
        credential_id: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredLlmCredential>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row =
            conn.query_opt(sql_file!("select", "llm/credential"), &[&organization_uuid.uuid(), &credential_id]).await.map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        match row {
            Some(row) => Ok(Some(self.hydrate_llm_credential_from_row(row, telemetry_wrapper).await?)),
            None => Ok(None),
        }
    }

    #[named]
    pub async fn update_llm_credential(
        &self,
        update: UpdateLlmCredential<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredLlmCredential> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let existing = self
            .get_llm_credential(update.organization_uuid, update.credential_id, telemetry_wrapper)
            .await?
            .ok_or_else(|| EpError::request("requested credential does not exist"))?;

        let label = update
            .label
            .map(|value| value.trim().to_string())
            .and_then(|value| if value.is_empty() { None } else { Some(value) })
            .or(existing.label.clone());
        let description = update
            .description
            .map(|value| value.trim().to_string())
            .and_then(|value| if value.is_empty() { None } else { Some(value) })
            .or(existing.description.clone());
        let base_url = update
            .base_url
            .map(|value| value.trim().to_string())
            .and_then(|value| if value.is_empty() { None } else { Some(value) })
            .or(existing.base_url.clone());
        let api_key = update.api_key.map(|value| value.trim().to_string()).unwrap_or(existing.api_key.clone());
        if api_key.is_empty() {
            return Err(EpError::request("api_key must not be empty"));
        }
        let encrypted_api_key = self.encrypt_secret_string_for_org(update.organization_uuid.uuid(), &api_key, telemetry_wrapper).await?;

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_one(
                sql_file!("update", "llm/credential"),
                &[
                    &update.organization_uuid.uuid(),
                    &update.credential_id,
                    &label,
                    &description,
                    &base_url,
                    &encrypted_api_key,
                ],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        self.hydrate_llm_credential_from_row(row, telemetry_wrapper).await
    }

    #[named]
    pub async fn delete_llm_credential(
        &self,
        organization_uuid: &OrganizationUuid,
        credential_id: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<bool> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let affected =
            conn.execute(sql_file!("delete", "llm/credential"), &[&organization_uuid.uuid(), &credential_id]).await.map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(affected > 0)
    }

    #[named]
    pub async fn upsert_llm_gateway_api_key(
        &self,
        key: &StoredLlmGatewayApiKey,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredLlmGatewayApiKey> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let model_allowlist = key.model_allowlist.as_ref().map(|allowlist| allowlist.iter().map(String::as_str).collect::<Vec<_>>());
        let rate_limit_rpm = key.rate_limit_rpm.map(i64::from);
        let budget_tokens_monthly = optional_u64_to_i64("budget_tokens_monthly", key.budget_tokens_monthly)?;
        let pii_policy = key.pii_policy.to_string();
        let custom_pii_terms = serde_json::to_value(&key.custom_pii_terms).map_err(EpError::serde)?;
        let price_arbitrage_mode = key.price_arbitrage_mode.to_string();
        let response_cache_ttl_secs = optional_u64_to_i64("response_cache_ttl_secs", key.response_cache_ttl_secs)?;
        let route_optimization_mode = key.route_optimization_mode.to_string();
        let kv_cache_mode = key.kv_cache_mode.to_string();
        let kv_cache_ttl_secs = optional_u64_to_i64("kv_cache_ttl_secs", key.kv_cache_ttl_secs)?;
        let route_switch_threshold_percent = key.route_switch_threshold_percent.map(i64::from);

        let row = conn
            .query_one(
                sql_file!("insert", "llm/gateway_api_key"),
                &[
                    &key.id,
                    &key.organization_uuid.uuid(),
                    &key.name,
                    &key.key_hash,
                    &key.key_prefix,
                    &key.endpoint_uuid,
                    &model_allowlist,
                    &rate_limit_rpm,
                    &budget_tokens_monthly,
                    &pii_policy,
                    &price_arbitrage_mode,
                    &response_cache_ttl_secs,
                    &route_optimization_mode,
                    &kv_cache_mode,
                    &kv_cache_ttl_secs,
                    &route_switch_threshold_percent,
                    &key.enabled,
                    &key.created_at,
                    &key.updated_at,
                    &key.last_used_at,
                    &key.agent_uuid,
                    &custom_pii_terms,
                ],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        row_to_llm_gateway_api_key(&row)
    }

    #[named]
    pub async fn list_llm_gateway_api_keys(&self, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Vec<StoredLlmGatewayApiKey>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/gateway_api_keys"), &[]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        rows.iter().map(row_to_llm_gateway_api_key).collect()
    }

    #[named]
    pub async fn delete_llm_gateway_api_key(
        &self,
        organization_uuid: &OrganizationUuid,
        key_id: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<bool> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let deleted =
            conn.query_opt(sql_file!("delete", "llm/gateway_api_key"), &[&organization_uuid.uuid(), &key_id]).await.map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(deleted.is_some())
    }

    /// Load one organization's PII dictionary, if it has been configured.
    #[named]
    pub async fn load_org_pii_dictionary(
        &self,
        organization_uuid: &OrganizationUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredOrgPiiDictionary>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("select", "llm/org_pii_dictionary"), &[&organization_uuid.uuid()]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.map(|row| row_to_org_pii_dictionary(&row)))
    }

    /// Load every organization's PII dictionary (for startup hydration).
    #[named]
    pub async fn list_org_pii_dictionaries(&self, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Vec<StoredOrgPiiDictionary>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/org_pii_dictionaries"), &[]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.iter().map(row_to_org_pii_dictionary).collect())
    }

    /// Replace one organization's PII dictionary.
    #[named]
    pub async fn upsert_org_pii_dictionary(
        &self,
        organization_uuid: &OrganizationUuid,
        terms: &[CustomPiiTerm],
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredOrgPiiDictionary> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let terms_json = serde_json::to_value(terms).map_err(EpError::serde)?;
        let row = conn.query_one(sql_file!("insert", "llm/org_pii_dictionary"), &[&organization_uuid.uuid(), &terms_json]).await.map_err(
            |e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            },
        )?;

        Ok(row_to_org_pii_dictionary(&row))
    }

    #[named]
    pub async fn upsert_llm_gateway_response_cache_entry(
        &self,
        entry: NewLlmGatewayResponseCacheEntry<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredLlmGatewayResponseCacheEntry> {
        let prompt_tokens = u64_to_i64("prompt_tokens", entry.prompt_tokens)?;
        let completion_tokens = u64_to_i64("completion_tokens", entry.completion_tokens)?;
        let total_tokens = u64_to_i64("total_tokens", entry.total_tokens)?;
        let estimated_cost_micros = u64_to_i64("estimated_cost_micros", entry.estimated_cost_micros)?;
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_one(
                sql_file!("insert", "llm/gateway_response_cache"),
                &[
                    &entry.cache_key,
                    &entry.organization_uuid.uuid(),
                    &entry.endpoint_uuid,
                    &entry.key_id,
                    &entry.provider,
                    &entry.model,
                    &entry.request_hash,
                    &entry.prompt_fingerprint,
                    entry.response_json,
                    &prompt_tokens,
                    &completion_tokens,
                    &total_tokens,
                    &estimated_cost_micros,
                    &entry.created_at,
                    &entry.updated_at,
                    &entry.expires_at,
                ],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        row_to_llm_gateway_response_cache(&row)
    }

    #[named]
    pub async fn get_llm_gateway_response_cache_entry(
        &self,
        organization_uuid: &OrganizationUuid,
        cache_key: &str,
        now: DateTime<Utc>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredLlmGatewayResponseCacheEntry>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_opt(sql_file!("select", "llm/gateway_response_cache"), &[&organization_uuid.uuid(), &cache_key, &now])
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        row.as_ref().map(row_to_llm_gateway_response_cache).transpose()
    }

    #[named]
    pub async fn touch_llm_gateway_response_cache_entry(
        &self,
        organization_uuid: &OrganizationUuid,
        cache_key: &str,
        now: DateTime<Utc>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<bool> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let updated = conn
            .execute(
                sql_file!("update", "llm/gateway_response_cache_touch"),
                &[&organization_uuid.uuid(), &cache_key, &now],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(updated > 0)
    }

    #[named]
    pub async fn upsert_llm_gateway_route_rollup(
        &self,
        rollup: &StoredLlmGatewayRouteRollup,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredLlmGatewayRouteRollup> {
        let success_count = u64_to_i64("success_count", rollup.success_count)?;
        let error_count = u64_to_i64("error_count", rollup.error_count)?;
        let total_latency_ms = u64_to_i64("total_latency_ms", rollup.total_latency_ms)?;
        let min_latency_ms = u64_to_i64("min_latency_ms", rollup.min_latency_ms)?;
        let max_latency_ms = u64_to_i64("max_latency_ms", rollup.max_latency_ms)?;
        let total_output_tokens = u64_to_i64("total_output_tokens", rollup.total_output_tokens)?;
        let total_duration_ms = u64_to_i64("total_duration_ms", rollup.total_duration_ms)?;
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_one(
                sql_file!("insert", "llm/gateway_route_rollup"),
                &[
                    &rollup.organization_uuid.uuid(),
                    &rollup.endpoint_uuid,
                    &rollup.provider,
                    &rollup.model,
                    &rollup.route_class,
                    &success_count,
                    &error_count,
                    &total_latency_ms,
                    &min_latency_ms,
                    &max_latency_ms,
                    &total_output_tokens,
                    &total_duration_ms,
                    &rollup.first_observed_at,
                    &rollup.last_observed_at,
                    &rollup.updated_at,
                ],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        row_to_llm_gateway_route_rollup(&row)
    }

    #[named]
    pub async fn list_llm_gateway_route_rollups(
        &self,
        organization_uuid: &OrganizationUuid,
        limit: i64,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredLlmGatewayRouteRollup>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn
            .query(sql_file!("select", "llm/gateway_route_rollups"), &[&organization_uuid.uuid(), &limit.max(1)])
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        rows.iter().map(row_to_llm_gateway_route_rollup).collect()
    }

    #[named]
    pub async fn list_all_llm_gateway_route_rollups(
        &self,
        limit: i64,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredLlmGatewayRouteRollup>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/gateway_route_rollups_all"), &[&limit.max(1)]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        rows.iter().map(row_to_llm_gateway_route_rollup).collect()
    }

    #[named]
    pub async fn record_llm_gateway_usage_rollup(
        &self,
        rollup: NewLlmGatewayUsageRollup<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredLlmGatewayUsageRollup> {
        let request_count = u64_to_i64("request_count", rollup.request_count)?;
        let prompt_tokens = u64_to_i64("prompt_tokens", rollup.prompt_tokens)?;
        let completion_tokens = u64_to_i64("completion_tokens", rollup.completion_tokens)?;
        let total_tokens = u64_to_i64("total_tokens", rollup.total_tokens)?;
        let estimated_cost_micros = u64_to_i64("estimated_cost_micros", rollup.estimated_cost_micros)?;
        let cache_hit_count = u64_to_i64("cache_hit_count", rollup.cache_hit_count)?;
        let kv_cache_hit_count = u64_to_i64("kv_cache_hit_count", rollup.kv_cache_hit_count)?;
        let rate_limited_count = u64_to_i64("rate_limited_count", rollup.rate_limited_count)?;
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_one(
                sql_file!("insert", "llm/gateway_usage_rollup"),
                &[
                    &rollup.organization_uuid.uuid(),
                    &rollup.consumer_kind,
                    &rollup.consumer_id,
                    &rollup.month_bucket,
                    &rollup.endpoint_uuid,
                    &request_count,
                    &prompt_tokens,
                    &completion_tokens,
                    &total_tokens,
                    &estimated_cost_micros,
                    &cache_hit_count,
                    &kv_cache_hit_count,
                    &rate_limited_count,
                    &rollup.updated_at,
                ],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        row_to_llm_gateway_usage_rollup(&row)
    }

    #[named]
    pub async fn get_llm_gateway_usage_rollup(
        &self,
        organization_uuid: &OrganizationUuid,
        consumer_kind: &str,
        consumer_id: &str,
        month_bucket: i32,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredLlmGatewayUsageRollup>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_opt(
                sql_file!("select", "llm/gateway_usage_rollup"),
                &[&organization_uuid.uuid(), &consumer_kind, &consumer_id, &month_bucket],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        row.as_ref().map(row_to_llm_gateway_usage_rollup).transpose()
    }

    #[named]
    pub async fn list_llm_gateway_usage_rollups(
        &self,
        organization_uuid: &OrganizationUuid,
        month_bucket: i32,
        limit: i64,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredLlmGatewayUsageRollup>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn
            .query(
                sql_file!("select", "llm/gateway_usage_rollups"),
                &[&organization_uuid.uuid(), &month_bucket, &limit.max(1)],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        rows.iter().map(row_to_llm_gateway_usage_rollup).collect()
    }

    #[named]
    pub async fn fetch_llm_credentials_by_ids(
        &self,
        organization_uuid: &OrganizationUuid,
        credential_ids: &[Uuid],
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredLlmCredential>> {
        if credential_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        #[cfg(not(embedded_db))]
        let rows = conn.query(sql_file!("select", "llm/credentials_by_ids"), &[&organization_uuid.uuid(), &credential_ids]).await.map_err(
            |e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            },
        )?;

        #[cfg(embedded_db)]
        let rows = {
            // SQLite has no ANY(); build a dynamic IN clause instead
            let placeholders: Vec<String> = (0..credential_ids.len()).map(|i| format!("?{}", i + 2)).collect();
            let sql = format!(
                "SELECT id, organization_uuid, provider, label, description, base_url, api_key, deleted_at, created_at, updated_at FROM llm_credentials WHERE organization_uuid = ?1 AND id IN ({}) AND deleted_at IS NULL",
                placeholders.join(", ")
            );
            let org_uuid = organization_uuid.uuid();
            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::with_capacity(1 + credential_ids.len());
            params.push(&org_uuid);
            for id in credential_ids {
                params.push(id);
            }
            conn.query(&sql, &params).await.map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?
        };

        let mut credentials = Vec::with_capacity(rows.len());
        let mut found: std::collections::HashSet<Uuid> = std::collections::HashSet::new();
        for row in rows {
            let credential = self.hydrate_llm_credential_from_row(row, telemetry_wrapper).await?;
            found.insert(credential.id);
            credentials.push(credential);
        }

        let requested: std::collections::HashSet<Uuid> = credential_ids.iter().copied().collect();
        if found.len() != requested.len() {
            let missing: Vec<String> = requested.difference(&found).map(|id| id.to_string()).collect();
            return Err(EpError::request(format!("Missing credentials for ids: {}", missing.join(", "))));
        }

        Ok(credentials)
    }

    /// Insert a run event into the `run_events` table for audit logging.
    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_run_event(
        &self,
        run_id: Uuid,
        event_type: &str,
        payload: &Value,
        tokens_used: Option<i32>,
        cost_usd: Option<f64>,
        trace_id: Option<&str>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(
            sql_file!("insert", "run_event"),
            &[&run_id, &event_type, payload, &tokens_used, &cost_usd, &trace_id],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    /// Insert a single evidence record.
    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_evidence_record(
        &self,
        id: Uuid,
        run_id: Uuid,
        step_index: i32,
        kind: &str,
        payload: &Value,
        source: Option<&str>,
        timestamp_ms: i64,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(
            sql_file!("insert", "evidence_record"),
            &[&id, &run_id, &step_index, &kind, payload, &source, &timestamp_ms],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    /// Retrieve all evidence records for a given run, ordered by step index.
    #[named]
    pub async fn get_evidence_by_run(&self, run_id: Uuid, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Vec<StoredEvidenceRecord>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "evidence_by_run"), &[&run_id]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows
            .iter()
            .map(|row| StoredEvidenceRecord {
                id: row.get("id"),
                run_id: row.get("run_id"),
                step_index: row.get("step_index"),
                kind: row.get("kind"),
                payload: row.get("payload"),
                source: row.get("source"),
                timestamp_ms: row.get("timestamp_ms"),
                created_at: row.get("created_at"),
            })
            .collect())
    }

    /// Retrieve recent run events for a given run, newest-first.
    #[named]
    pub async fn load_run_events_by_run(
        &self,
        run_id: Uuid,
        limit: i64,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredRunEvent>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "run_events_by_run"), &[&run_id, &limit]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows
            .iter()
            .map(|row| StoredRunEvent {
                id: row.get("id"),
                run_id: row.get("run_id"),
                event_type: row.get("event_type"),
                payload: row.get("payload"),
                tokens_used: row.get("tokens_used"),
                trace_id: row.get("trace_id"),
                created_at: row.get("created_at"),
            })
            .collect())
    }

    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_approval_request(
        &self,
        id: Uuid,
        run_id: Uuid,
        org_uuid: Uuid,
        requested_by: Uuid,
        plan: &Value,
        state: &str,
        expires_at: Option<DateTime<Utc>>,
        delegated_to: Option<Uuid>,
        required_approvals: i32,
        approval_count: i32,
        change_window_start: Option<DateTime<Utc>>,
        change_window_end: Option<DateTime<Utc>>,
        justification: Option<&str>,
        decided_by: Option<Uuid>,
        decided_at: Option<DateTime<Utc>>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(
            sql_file!("insert", "llm/approval_request"),
            &[
                &id,
                &run_id,
                &org_uuid,
                &requested_by,
                plan,
                &state,
                &expires_at,
                &delegated_to,
                &required_approvals,
                &approval_count,
                &change_window_start,
                &change_window_end,
                &justification,
                &decided_by,
                &decided_at,
            ],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    /// Check whether `user_uuid` belongs to `organization_uuid` via the
    /// `organization_users` junction table.
    #[named]
    pub async fn is_user_in_org(
        &self,
        organization_uuid: Uuid,
        user_uuid: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<bool> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row =
            conn.query_opt(sql_file!("select", "organization/user_membership"), &[&organization_uuid, &user_uuid])
                .await
                .map_err(|e| {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                    EpError::database(e)
                })?;

        Ok(row.is_some())
    }

    #[named]
    pub async fn load_approval_request(
        &self,
        id: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredApprovalRequest>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("select", "llm/approval_request"), &[&id]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.as_ref().map(row_to_approval_request))
    }

    #[named]
    pub async fn list_pending_approvals(
        &self,
        org_uuid: Uuid,
        limit: i64,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredApprovalRequest>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/approval_requests_pending"), &[&org_uuid, &limit]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.iter().map(row_to_approval_request).collect())
    }

    #[named]
    pub async fn approve_approval_request(
        &self,
        id: Uuid,
        decided_by: Uuid,
        justification: Option<&str>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredApprovalRequest>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("update", "llm/approval_request_approve"), &[&id, &decided_by, &justification]).await.map_err(
            |e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            },
        )?;

        Ok(row.as_ref().map(row_to_approval_request))
    }

    #[named]
    pub async fn reject_approval_request(
        &self,
        id: Uuid,
        decided_by: Uuid,
        justification: Option<&str>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredApprovalRequest>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row =
            conn.query_opt(sql_file!("update", "llm/approval_request_reject"), &[&id, &decided_by, &justification])
                .await
                .map_err(|e| {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                    EpError::database(e)
                })?;

        Ok(row.as_ref().map(row_to_approval_request))
    }

    #[named]
    pub async fn delegate_approval_request(
        &self,
        id: Uuid,
        delegated_to: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredApprovalRequest>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("update", "llm/approval_request_delegate"), &[&id, &delegated_to]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.as_ref().map(row_to_approval_request))
    }

    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_execution_run(
        &self,
        id: Uuid,
        organization_uuid: Uuid,
        principal_type: &str,
        principal_id: Uuid,
        endpoint_uuid: Uuid,
        trigger_kind: &str,
        trigger_metadata: &Value,
        conversation_id: Option<Uuid>,
        agent_id: Option<Uuid>,
        request_payload: &Value,
        state: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(
            sql_file!("insert", "execution_run"),
            &[
                &id,
                &organization_uuid,
                &principal_type,
                &principal_id,
                &endpoint_uuid,
                &trigger_kind,
                trigger_metadata,
                &conversation_id,
                &agent_id,
                request_payload,
                &state,
            ],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    pub async fn load_execution_run(&self, id: Uuid, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Option<StoredExecutionRun>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("select", "execution_run"), &[&id]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.as_ref().map(row_to_execution_run))
    }

    #[named]
    pub async fn load_resumable_execution_run(
        &self,
        conversation_id: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredExecutionRun>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("select", "execution_run_resumable"), &[&conversation_id]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.as_ref().map(row_to_execution_run))
    }

    #[named]
    pub async fn compare_and_set_execution_run_state(
        &self,
        id: Uuid,
        to_state: &str,
        from_states: &[&str],
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<bool> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let from_states_owned: Vec<String> = from_states.iter().map(|s| s.to_string()).collect();
        let row = conn.query_opt(sql_file!("update", "execution_run_state"), &[&id, &to_state, &from_states_owned]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.is_some())
    }

    #[named]
    pub async fn store_execution_run_plan(&self, id: Uuid, plan: &Value, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(sql_file!("update", "execution_run_plan"), &[&id, plan]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    pub async fn store_execution_run_checkpoint(
        &self,
        id: Uuid,
        checkpoint: &Value,
        state: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(sql_file!("update", "execution_run_checkpoint"), &[&id, checkpoint, &state]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    pub async fn finish_execution_run(
        &self,
        id: Uuid,
        state: &str,
        response_text: Option<&str>,
        error: Option<&str>,
        duration_ms: i64,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(sql_file!("update", "execution_run_finish"), &[&id, &state, &response_text, &error, &duration_ms])
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(())
    }

    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_trigger_source(
        &self,
        id: Uuid,
        organization_uuid: Uuid,
        name: &str,
        source_type: &str,
        config: &Value,
        hmac_secret: Option<&str>,
        is_active: bool,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(
            "INSERT INTO trigger_sources (id, organization_uuid, name, source_type, config, hmac_secret, is_active, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            &[&id, &organization_uuid, &name, &source_type, config, &hmac_secret, &is_active],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    pub async fn list_trigger_sources_by_org(
        &self,
        organization_uuid: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredTriggerSource>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn
            .query(
                "SELECT id, organization_uuid, name, source_type, config, hmac_secret, is_active, created_at, updated_at
                 FROM trigger_sources
                 WHERE organization_uuid = $1
                 ORDER BY created_at DESC",
                &[&organization_uuid],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(rows.iter().map(row_to_trigger_source).collect())
    }

    #[named]
    pub async fn load_trigger_source(
        &self,
        source_id: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredTriggerSource>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_opt(
                "SELECT id, organization_uuid, name, source_type, config, hmac_secret, is_active, created_at, updated_at
                 FROM trigger_sources
                 WHERE id = $1
                 LIMIT 1",
                &[&source_id],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(row.as_ref().map(row_to_trigger_source))
    }

    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_trigger_event(
        &self,
        id: Uuid,
        source_id: Uuid,
        event_type: &str,
        payload: &Value,
        idempotency_key: Option<&str>,
        correlation_id: Option<Uuid>,
        matched_agent_id: Option<Uuid>,
        matched_run_id: Option<Uuid>,
        state: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(
            "INSERT INTO trigger_events (
                id, source_id, event_type, payload, idempotency_key, correlation_id,
                matched_agent_id, matched_run_id, state, received_at, processed_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, NULL)",
            &[
                &id,
                &source_id,
                &event_type,
                payload,
                &idempotency_key,
                &correlation_id,
                &matched_agent_id,
                &matched_run_id,
                &state,
            ],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    pub async fn load_trigger_event_by_source_and_idempotency(
        &self,
        source_id: Uuid,
        idempotency_key: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredTriggerEvent>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_opt(
                "SELECT id, source_id, event_type, payload, idempotency_key, correlation_id, matched_agent_id,
                        matched_run_id, state, received_at, processed_at
                 FROM trigger_events
                 WHERE source_id = $1
                   AND idempotency_key = $2
                 LIMIT 1",
                &[&source_id, &idempotency_key],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(row.as_ref().map(row_to_trigger_event))
    }

    #[named]
    pub async fn has_recent_anomaly_trigger_event(
        &self,
        source_id: Uuid,
        endpoint_uuid: &str,
        detector: &str,
        received_after: DateTime<Utc>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<bool> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_opt(
                "SELECT 1
                 FROM trigger_events
                 WHERE source_id = $1
                   AND event_type = 'anomaly_transition'
                   AND payload ->> 'endpoint_uuid' = $2
                   AND payload ->> 'detector' = $3
                   AND received_at > $4
                 LIMIT 1",
                &[&source_id, &endpoint_uuid, &detector, &received_after],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(row.is_some())
    }

    #[named]
    pub async fn update_trigger_event_state(
        &self,
        event_id: Uuid,
        state: &str,
        matched_agent_id: Option<Uuid>,
        matched_run_id: Option<Uuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(
            "UPDATE trigger_events
             SET state = $2,
                 matched_agent_id = COALESCE($3, matched_agent_id),
                 matched_run_id = COALESCE($4, matched_run_id),
                 processed_at = CURRENT_TIMESTAMP
             WHERE id = $1",
            &[&event_id, &state, &matched_agent_id, &matched_run_id],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_agent_trigger_rule(
        &self,
        id: Uuid,
        agent_id: Uuid,
        source_id: Uuid,
        event_type_filter: Option<&str>,
        payload_filter: Option<&Value>,
        is_active: bool,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(
            "INSERT INTO agent_trigger_rules (
                id, agent_id, source_id, event_type_filter, payload_filter, is_active, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)",
            &[&id, &agent_id, &source_id, &event_type_filter, &payload_filter, &is_active],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    pub async fn list_trigger_rules_by_source(
        &self,
        source_id: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredAgentTriggerRule>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn
            .query(
                "SELECT id, agent_id, source_id, event_type_filter, payload_filter, is_active, created_at
                 FROM agent_trigger_rules
                 WHERE source_id = $1
                 ORDER BY created_at DESC",
                &[&source_id],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(rows.iter().map(row_to_agent_trigger_rule).collect())
    }

    #[named]
    pub async fn list_active_trigger_rules_by_source(
        &self,
        source_id: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredAgentTriggerRule>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn
            .query(
                "SELECT id, agent_id, source_id, event_type_filter, payload_filter, is_active, created_at
                 FROM agent_trigger_rules
                 WHERE source_id = $1
                   AND is_active = TRUE
                 ORDER BY created_at DESC",
                &[&source_id],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(rows.iter().map(row_to_agent_trigger_rule).collect())
    }

    #[named]
    pub async fn delete_agent_trigger_rule(
        &self,
        source_id: Uuid,
        rule_id: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<bool> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let affected = conn
            .execute(
                "DELETE FROM agent_trigger_rules
                 WHERE id = $1
                   AND source_id = $2",
                &[&rule_id, &source_id],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(affected > 0)
    }

    #[allow(clippy::too_many_arguments)]
    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_user_db_credential(
        &self,
        user_uuid: Uuid,
        org_uuid: Uuid,
        endpoint_uuid: Uuid,
        db_username: &str,
        encrypted_password: &[u8],
        auth_method: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredUserDbCredential> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;
        let stored_password = self.encrypt_secret_bytes_for_org(org_uuid, encrypted_password, telemetry_wrapper).await?;

        let id = Uuid::new_v4();
        let row = conn
            .query_one(
                sql_file!("insert", "llm/user_db_credential"),
                &[
                    &id,
                    &user_uuid,
                    &org_uuid,
                    &endpoint_uuid,
                    &db_username,
                    &stored_password,
                    &auth_method,
                ],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        self.hydrate_user_db_credential_from_row(row, telemetry_wrapper).await
    }

    #[named]
    pub async fn get_user_db_credential(
        &self,
        user_uuid: Uuid,
        endpoint_uuid: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredUserDbCredential>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("select", "llm/user_db_credential"), &[&user_uuid, &endpoint_uuid]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        match row {
            Some(row) => Ok(Some(self.hydrate_user_db_credential_from_row(row, telemetry_wrapper).await?)),
            None => Ok(None),
        }
    }

    #[named]
    pub async fn delete_user_db_credential(
        &self,
        user_uuid: Uuid,
        endpoint_uuid: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<bool> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let affected = conn.execute(sql_file!("delete", "llm/user_db_credential"), &[&user_uuid, &endpoint_uuid]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(affected > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::{decrypt_secret_bytes, decrypt_secret_string, preferred_org_secret_key_ref};
    use serial_test::serial;
    use uuid::Uuid;

    #[test]
    #[serial]
    fn secret_string_decrypt_preserves_legacy_plaintext() {
        assert_eq!(
            decrypt_secret_string("legacy-secret").expect("legacy plaintext should pass through"),
            "legacy-secret"
        );
    }

    #[test]
    #[serial]
    fn secret_bytes_decrypt_preserves_legacy_plaintext() {
        assert_eq!(
            decrypt_secret_bytes(b"legacy-password").expect("legacy plaintext should pass through"),
            b"legacy-password"
        );
    }

    #[test]
    #[serial]
    fn preferred_org_secret_key_ref_is_always_org_specific() {
        let org_uuid = Uuid::parse_str("7a7df1fd-6f2d-4cf5-84f6-7e2731c8a7d5").expect("static uuid should parse");
        let override_name = "EDEN_ORG_ENCRYPTION_KEY__7A7DF1FD6F2D4CF584F67E2731C8A7D5";
        let preferred = preferred_org_secret_key_ref(org_uuid);

        assert_eq!(preferred, override_name);
    }
}
