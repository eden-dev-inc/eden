//! # Endpoint-Level Security (ELS) Policy Management
//!
//! Named policies per endpoint, assigned to users in sync or copy mode.
//! Postgres is the authoritative store; ShardMap typed namespaces serve as the fast-read cache.
//!
//! ## Tables
//!
//! - `els_policies(uuid PK, endpoint_uuid, name, strategy, config JSONB, ...)`
//! - `els_policy_assignments(endpoint_uuid, user_uuid PK, policy_uuid FK, mode, ...)`
//!
//! ## Internal Cache
//!
//! - **Namespace**: endpoint cache UUID
//! - **Item**: user UUID
//! - **Value**: Resolved effective policy JSON `{ "strategy": "...", "config": {...} }`
//!
//! ## Assignment Modes
//!
//! - **Sync**: config resolved live from the referenced policy. Updating the policy
//!   re-caches all sync'd users.
//! - **Copy**: config snapshot taken at assignment time. Independent of the policy.
//!
//! ## Encryption-at-rest
//!
//! Config JSONB fields are encrypted via AES-256-GCM envelope encryption
//! (`db::encryption`) using per-endpoint DEKs wrapped by an org-level key.
//! Encrypted values are stored as `{"__encrypted": "<base64>"}` in Postgres
//! and prefixed with `ENC:` in the internal cache. Legacy plaintext rows are
//! transparently readable (decrypt detects the sentinel and passes through).
//!
//! Encryption is active when an `OrgKeyProvider` is configured on
//! `DatabaseManager` (production default: `EnvKeyProvider`). In embedded-db
//! mode and tests without a provider, encryption is skipped.
//!
//! The wrapped DEK and org key-ref are also stored in the ELS cache namespace
//! (`__dek` / `__key_ref` items) so the PG proxy can decrypt cache entries without
//! hitting Postgres.

use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection, ShardCache};
use crate::sql_file;
use chrono::{DateTime, Utc};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid};
use eden_core::format::endpoint::EpKind;
use eden_core::format::{EdenUuid, EndpointUuid, IdKind, OrganizationUuid, PolicyUuid, UserUuid};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use utoipa::ToSchema;
use uuid::Uuid;

cfg_if::cfg_if! {
    if #[cfg(embedded_db)] {
        /// Row type returned by `pg_connection().query()` in embedded-db mode.
        type DbRow = ep_core::database::schema::Row;
    } else {
        use base64::Engine;
        use eden_logger_internal::{LogAudience, log_warn, trace_context};

        /// Row type returned by `pg_connection().query()` outside embedded-db mode.
        type DbRow = tokio_postgres::Row;
    }
}

const ELS_PREFIX: &str = "els::";
pub const ELS_DEFAULT_PAGE_LIMIT: i64 = 50;
pub const ELS_MAX_PAGE_LIMIT: i64 = 1_000;
const ELS_WARM_BATCH_SIZE: i64 = 1_000;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// ELS enforcement strategy — one variant per endpoint type.
///
/// Each variant defines the expected shape of the `config` JSONB payload
/// and how credentials are applied at request time.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ElsStrategy {
    /// PostgreSQL session variables injected via `SET` commands.
    /// Config: `{"variables": {"app.tenant_id": "t-123", ...}}`
    Postgres,
    /// MySQL connection credentials.
    /// Config: `{"username": "...", "password": "..."}`
    Mysql,
    /// MSSQL connection credentials.
    /// Config: `{"username": "...", "password": "..."}`
    Mssql,
    /// Oracle connection credentials.
    /// Config: `{"username": "...", "password": "...", "privilege": "..."}`
    Oracle,
    /// MongoDB connection credentials.
    /// Config: `{"username": "...", "password": "...", "mechanism": "..."}`
    Mongo,
    /// Redis connection credentials.
    /// Config: `{"username": "...", "password": "..."}`
    Redis,
    /// Cassandra connection credentials.
    /// Config: `{"username": "...", "password": "..."}`
    Cassandra,
    /// ClickHouse connection credentials.
    /// Config: `{"username": "...", "password": "..."}`
    Clickhouse,
    /// Snowflake key-pair or OAuth auth.
    /// Config: `{"user": "...", "private_key": "..."}` or `{"oauth_token": "..."}`
    Snowflake,
    /// AWS IAM credentials.
    /// Config: `{"access_key_id": "...", "secret_access_key": "...", "session_token": "...", "role_arn": "..."}`
    Aws,
    /// RDS IAM credentials (same shape as AWS).
    /// Config: `{"access_key_id": "...", "secret_access_key": "..."}`
    Rds,
    /// ElastiCache connection credentials.
    /// Config: `{"username": "...", "password": "..."}`
    Elasticache,
    /// HTTP headers / auth.
    /// Config: `{"headers": {"Authorization": "Bearer ...", ...}}`
    Http,
    /// Salesforce OAuth credentials.
    /// Config: `{"access_token": "...", "instance_url": "..."}`
    Salesforce,
    /// Databricks token auth.
    /// Config: `{"token": "..."}`
    Databricks,
    /// Datadog API key auth.
    /// Config: `{"api_key": "...", "app_key": "..."}`
    Datadog,
    /// Pinecone API key auth.
    /// Config: `{"api_key": "..."}`
    Pinecone,
    /// Weaviate API key auth.
    /// Config: `{"api_key": "..."}`
    Weaviate,
    /// PostHog personal API key auth.
    /// Config: `{"api_key": "..."}`
    Posthog,
    /// Tavily API key auth.
    /// Config: `{"api_key": "..."}`
    Tavily,
    /// LLM provider auth.
    /// Config: `{"api_key": "...", "provider": "..."}`
    Llm,
    /// Custom function auth (free-form JSONB).
    Function,
    /// Azure Microsoft Entra ID (service principal or pre-acquired Bearer token).
    /// Config: `{"tenant_id": "...", "client_id": "...", "client_secret": "..."}` or `{"access_token": "..."}`
    Azure,
    /// GitLab personal/project/OAuth token.
    /// Config: `{"token": "..."}`
    Gitlab,
    /// Google Workspace OAuth2 credentials.
    /// Config: `{"client_id": "...", "client_secret": "...", "refresh_token": "..."}`
    GoogleWorkspace,
    /// S3-compatible IAM credentials.
    /// Config: `{"access_key_id": "...", "secret_access_key": "...", "session_token": "..."}`
    S3,
    /// Eraser API key auth.
    /// Config: `{"api_key": "..."}`
    Eraser,
}

impl ElsStrategy {
    /// Return the ELS strategy for a given endpoint type, or `None` if the
    /// endpoint type does not support ELS.
    pub fn from_ep_kind(kind: EpKind) -> Option<Self> {
        match kind {
            EpKind::Postgres => Some(Self::Postgres),
            EpKind::Mysql => Some(Self::Mysql),
            EpKind::Mssql => Some(Self::Mssql),
            EpKind::Oracle => Some(Self::Oracle),
            EpKind::Mongo => Some(Self::Mongo),
            EpKind::Redis => Some(Self::Redis),
            EpKind::Cassandra => Some(Self::Cassandra),
            EpKind::Clickhouse => Some(Self::Clickhouse),
            EpKind::Snowflake => Some(Self::Snowflake),
            EpKind::Aws => Some(Self::Aws),
            EpKind::Rds => Some(Self::Rds),
            EpKind::Elasticache => Some(Self::Elasticache),
            EpKind::Http => Some(Self::Http),
            EpKind::Salesforce => Some(Self::Salesforce),
            EpKind::Databricks => Some(Self::Databricks),
            EpKind::Datadog => Some(Self::Datadog),
            EpKind::Pinecone => Some(Self::Pinecone),
            EpKind::Weaviate => Some(Self::Weaviate),
            EpKind::Tavily => Some(Self::Tavily),
            EpKind::Llm => Some(Self::Llm),
            EpKind::Function => Some(Self::Function),
            EpKind::Posthog => Some(Self::Posthog),
            EpKind::Azure => Some(Self::Azure),
            EpKind::Gitlab => Some(Self::Gitlab),
            EpKind::GoogleWorkspace => Some(Self::GoogleWorkspace),
            EpKind::S3 => Some(Self::S3),
            EpKind::Eraser => Some(Self::Eraser),
        }
    }

    /// Database column value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
            Self::Mssql => "mssql",
            Self::Oracle => "oracle",
            Self::Mongo => "mongo",
            Self::Redis => "redis",
            Self::Cassandra => "cassandra",
            Self::Clickhouse => "clickhouse",
            Self::Snowflake => "snowflake",
            Self::Aws => "aws",
            Self::Rds => "rds",
            Self::Elasticache => "elasticache",
            Self::Http => "http",
            Self::Salesforce => "salesforce",
            Self::Databricks => "databricks",
            Self::Datadog => "datadog",
            Self::Pinecone => "pinecone",
            Self::Posthog => "posthog",
            Self::Weaviate => "weaviate",
            Self::Tavily => "tavily",
            Self::Llm => "llm",
            Self::Function => "function",
            Self::Azure => "azure",
            Self::Gitlab => "gitlab",
            Self::GoogleWorkspace => "google_workspace",
            Self::S3 => "s3",
            Self::Eraser => "eraser",
        }
    }

    /// Parse from database column value.
    pub fn from_str_value(s: &str) -> Option<Self> {
        match s {
            // Current format
            "postgres" => Some(Self::Postgres),
            "mysql" => Some(Self::Mysql),
            "mssql" => Some(Self::Mssql),
            "oracle" => Some(Self::Oracle),
            "mongo" => Some(Self::Mongo),
            "redis" => Some(Self::Redis),
            "cassandra" => Some(Self::Cassandra),
            "clickhouse" => Some(Self::Clickhouse),
            "snowflake" => Some(Self::Snowflake),
            "aws" => Some(Self::Aws),
            "rds" => Some(Self::Rds),
            "elasticache" => Some(Self::Elasticache),
            "http" => Some(Self::Http),
            "salesforce" => Some(Self::Salesforce),
            "databricks" => Some(Self::Databricks),
            "datadog" => Some(Self::Datadog),
            "pinecone" => Some(Self::Pinecone),
            "posthog" => Some(Self::Posthog),
            "weaviate" => Some(Self::Weaviate),
            "tavily" => Some(Self::Tavily),
            "llm" => Some(Self::Llm),
            "function" => Some(Self::Function),
            "azure" => Some(Self::Azure),
            "gitlab" => Some(Self::Gitlab),
            "google_workspace" => Some(Self::GoogleWorkspace),
            "s3" => Some(Self::S3),
            "eraser" => Some(Self::Eraser),
            // Legacy compat: accept old format on read
            "postgres_session_variables" => Some(Self::Postgres),
            _ => None,
        }
    }

    /// Validate that the config JSONB has the required shape for this strategy.
    pub fn validate_config(&self, config: &serde_json::Value) -> ResultEP<()> {
        let obj = config.as_object().ok_or_else(|| EpError::parse("ELS config must be a JSON object".to_string()))?;

        match self {
            Self::Postgres => {
                require_object_field(obj, "variables")?;
            }
            Self::Redis => {
                let has_username = obj.get("username").is_some_and(|v| v.is_string());
                let has_endpoint_uuid = obj.get("endpoint_uuid").is_some_and(|v| v.is_string());
                match (has_username, has_endpoint_uuid) {
                    (true, false) | (false, true) => {}
                    (true, true) => {
                        return Err(EpError::parse(
                            "Redis ELS config must set either 'username' or 'endpoint_uuid', not both".to_string(),
                        ));
                    }
                    (false, false) => {
                        return Err(EpError::parse(
                            "Redis ELS config requires either 'username' (string) or 'endpoint_uuid' (string)".to_string(),
                        ));
                    }
                }
            }
            Self::Cassandra | Self::Elasticache => {
                require_string_field(obj, "username")?;
            }
            // Session-variable capable databases: accept credentials, variables, or both.
            Self::Mysql | Self::Mssql | Self::Clickhouse => {
                let has_username = obj.get("username").is_some_and(|v| v.is_string());
                let has_variables = obj.get("variables").is_some_and(|v| v.is_object());
                if !has_username && !has_variables {
                    return Err(EpError::parse("ELS config requires 'username' (string) or 'variables' (object)".to_string()));
                }
            }
            Self::Oracle => {
                require_string_field(obj, "username")?;
            }
            Self::Mongo => {
                require_string_field(obj, "username")?;
            }
            Self::Snowflake => {
                // Key-pair auth, OAuth token, or session variables
                let has_private_key = obj.contains_key("private_key");
                let has_oauth = obj.contains_key("oauth_token");
                let has_variables = obj.get("variables").is_some_and(|v| v.is_object());
                if !has_private_key && !has_oauth && !has_variables {
                    return Err(EpError::parse(
                        "Snowflake ELS config requires 'private_key', 'oauth_token', or 'variables'".to_string(),
                    ));
                }
                if has_private_key {
                    require_string_field(obj, "user")?;
                }
            }
            Self::Aws | Self::Rds => {
                require_string_field(obj, "access_key_id")?;
                require_string_field(obj, "secret_access_key")?;
            }
            Self::Http => {
                require_object_field(obj, "headers")?;
            }
            Self::Salesforce => {
                require_string_field(obj, "access_token")?;
            }
            Self::Databricks => {
                require_string_field(obj, "token")?;
            }
            Self::Datadog => {
                require_string_field(obj, "api_key")?;
            }
            Self::Pinecone | Self::Posthog | Self::Weaviate | Self::Tavily => {
                require_string_field(obj, "api_key")?;
            }
            Self::Llm => {
                require_string_field(obj, "api_key")?;
            }
            Self::Azure => {
                // Service principal (tenant_id + client_id + client_secret) or access token
                let has_access_token = obj.get("access_token").is_some_and(|v| v.is_string());
                let has_tenant_id = obj.get("tenant_id").is_some_and(|v| v.is_string());
                if !has_access_token && !has_tenant_id {
                    return Err(EpError::parse(
                        "Azure ELS config requires 'tenant_id' (service principal) or 'access_token'".to_string(),
                    ));
                }
            }
            Self::Gitlab => {
                require_string_field(obj, "token")?;
            }
            Self::GoogleWorkspace => {
                require_string_field(obj, "client_id")?;
                require_string_field(obj, "client_secret")?;
                require_string_field(obj, "refresh_token")?;
            }
            Self::S3 => {
                require_string_field(obj, "access_key_id")?;
                require_string_field(obj, "secret_access_key")?;
            }
            Self::Eraser => {
                require_string_field(obj, "api_key")?;
            }
            // Free-form: any valid JSON object is accepted.
            Self::Function => {}
        }

        Ok(())
    }
}

/// Require that `obj[field]` exists and is a string.
fn require_string_field(obj: &serde_json::Map<String, serde_json::Value>, field: &str) -> ResultEP<()> {
    match obj.get(field) {
        Some(v) if v.is_string() => Ok(()),
        Some(_) => Err(EpError::parse(format!("ELS config field '{field}' must be a string"))),
        None => Err(EpError::parse(format!("ELS config requires '{field}' field"))),
    }
}

/// Require that `obj[field]` exists and is a JSON object.
fn require_object_field(obj: &serde_json::Map<String, serde_json::Value>, field: &str) -> ResultEP<()> {
    match obj.get(field) {
        Some(v) if v.is_object() => Ok(()),
        Some(_) => Err(EpError::parse(format!("ELS config field '{field}' must be an object"))),
        None => Err(EpError::parse(format!("ELS config requires '{field}' field"))),
    }
}

impl std::fmt::Display for ElsStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Assignment mode: sync (live reference) or copy (snapshot).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentMode {
    Sync,
    Copy,
}

impl AssignmentMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::Copy => "copy",
        }
    }

    pub fn from_str_value(s: &str) -> Option<Self> {
        match s {
            "sync" => Some(Self::Sync),
            "copy" => Some(Self::Copy),
            _ => None,
        }
    }
}

/// Request body for creating a named policy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct CreatePolicyRequest {
    pub name: String,
    pub strategy: ElsStrategy,
    pub config: serde_json::Value,
}

/// Request body for updating an existing policy's effective config.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct UpdatePolicyRequest {
    pub strategy: ElsStrategy,
    pub config: serde_json::Value,
}

/// Request body for validating a policy config against an endpoint type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ValidatePolicyRequest {
    pub strategy: ElsStrategy,
    pub config: serde_json::Value,
}

/// Request body for assigning a policy to a user.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct AssignPolicyRequest {
    pub policy_uuid: PolicyUuid,
    pub mode: AssignmentMode,
}

/// Request body for assigning a policy to many users at once.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct BulkAssignUsersRequest {
    pub policy_uuid: PolicyUuid,
    pub mode: AssignmentMode,
    pub user_uuids: Vec<UserUuid>,
}

/// Request body for bulk-unassigning selected users.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct BulkUnassignUsersRequest {
    pub user_uuids: Vec<UserUuid>,
}

/// Bulk assignment summary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct BulkAssignUsersResult {
    pub assigned: usize,
    pub already_assigned: usize,
}

/// Shared limit/offset pagination for ELS list endpoints.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaginationParams {
    pub limit: i64,
    pub offset: i64,
}

impl Default for PaginationParams {
    fn default() -> Self {
        Self { limit: ELS_DEFAULT_PAGE_LIMIT, offset: 0 }
    }
}

/// Generic paginated response wrapper.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaginatedItems<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}

impl<T> PaginatedItems<T> {
    pub fn map_items<U, F>(self, mut f: F) -> PaginatedItems<U>
    where
        F: FnMut(T) -> U,
    {
        PaginatedItems {
            items: self.items.into_iter().map(&mut f).collect(),
            total: self.total,
            limit: self.limit,
            offset: self.offset,
        }
    }
}

/// A named ELS policy definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ElsPolicy {
    pub policy_uuid: PolicyUuid,
    pub name: String,
    pub strategy: ElsStrategy,
    pub config: serde_json::Value,
}

/// A user's effective (resolved) policy assignment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct UserPolicyAssignment {
    pub user_uuid: UserUuid,
    pub policy_uuid: PolicyUuid,
    pub policy_name: String,
    pub mode: AssignmentMode,
    pub strategy: ElsStrategy,
    /// Resolved config — from policy (sync) or snapshot (copy).
    pub config: serde_json::Value,
}

/// Lightweight resolved policy for the internal cache.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResolvedPolicy {
    pub strategy: ElsStrategy,
    pub config: serde_json::Value,
}

impl ResolvedPolicy {
    /// Deserialize the config JSONB into a typed `EpAuth` object based on
    /// the strategy. This is the bridge between the DB/cache layer and the
    /// typed auth system in `endpoint_core`.
    pub fn resolve(&self) -> ResultEP<Box<dyn ep_core::ep_auth::EpAuth>> {
        ep_core::ep_auth::resolve_ep_auth(self.strategy.as_str(), &self.config)
    }
}

// ---------------------------------------------------------------------------
// Redacted variants for API responses (no credential data)
// ---------------------------------------------------------------------------

/// Redacted ELS policy — config excluded to prevent credential leakage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ElsPolicyRedacted {
    pub policy_uuid: PolicyUuid,
    pub name: String,
    pub strategy: ElsStrategy,
}

impl From<ElsPolicy> for ElsPolicyRedacted {
    fn from(p: ElsPolicy) -> Self {
        Self {
            policy_uuid: p.policy_uuid,
            name: p.name,
            strategy: p.strategy,
        }
    }
}

/// Redacted user assignment — config excluded to prevent credential leakage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct UserPolicyAssignmentRedacted {
    pub user_uuid: UserUuid,
    pub policy_uuid: PolicyUuid,
    pub policy_name: String,
    pub mode: AssignmentMode,
    pub strategy: ElsStrategy,
}

impl From<UserPolicyAssignment> for UserPolicyAssignmentRedacted {
    fn from(a: UserPolicyAssignment) -> Self {
        Self {
            user_uuid: a.user_uuid,
            policy_uuid: a.policy_uuid,
            policy_name: a.policy_name,
            mode: a.mode,
            strategy: a.strategy,
        }
    }
}

// ---------------------------------------------------------------------------
// Version lifecycle types
// ---------------------------------------------------------------------------

/// Status of an ELS policy version in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ElsVersionStatus {
    Draft,
    Active,
    Superseded,
    Rejected,
}

impl ElsVersionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Active => "active",
            Self::Superseded => "superseded",
            Self::Rejected => "rejected",
        }
    }

    pub fn from_str_value(s: &str) -> Option<Self> {
        match s {
            "draft" => Some(Self::Draft),
            "active" => Some(Self::Active),
            "superseded" => Some(Self::Superseded),
            "rejected" => Some(Self::Rejected),
            _ => None,
        }
    }
}

/// An immutable version of an ELS policy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ElsPolicyVersion {
    pub policy_uuid: PolicyUuid,
    pub version: i32,
    pub strategy: ElsStrategy,
    pub config: serde_json::Value,
    pub status: ElsVersionStatus,
    pub created_by: UserUuid,
    pub created_at: DateTime<Utc>,
}

/// Pointer to the currently active version of an ELS policy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ElsPolicyPointer {
    pub policy_uuid: PolicyUuid,
    pub active_version: Option<i32>,
    pub activated_by: Option<UserUuid>,
    pub activated_at: Option<DateTime<Utc>>,
}

/// Request body for creating a new policy version (draft).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct CreateVersionRequest {
    pub strategy: ElsStrategy,
    pub config: serde_json::Value,
}

/// Request body for promoting a version, with optimistic lock.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct PromoteVersionRequest {
    /// The current active version (for optimistic locking). `None` for first promotion.
    pub expected_current: Option<i32>,
}

/// Request body for rolling back to a previous version, with optimistic lock.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct RollbackVersionRequest {
    /// The current active version (for optimistic locking).
    pub expected_current: i32,
}

/// API-safe version without config (credentials redacted).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ElsPolicyVersionRedacted {
    pub policy_uuid: PolicyUuid,
    pub version: i32,
    pub strategy: ElsStrategy,
    pub status: ElsVersionStatus,
    pub created_by: UserUuid,
    pub created_at: DateTime<Utc>,
}

impl From<ElsPolicyVersion> for ElsPolicyVersionRedacted {
    fn from(v: ElsPolicyVersion) -> Self {
        Self {
            policy_uuid: v.policy_uuid,
            version: v.version,
            strategy: v.strategy,
            status: v.status,
            created_by: v.created_by,
            created_at: v.created_at,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn els_key(endpoint: &EndpointCacheUuid) -> String {
    format!("{ELS_PREFIX}{endpoint}")
}

/// Extract the organization UUID from an `EndpointCacheUuid`.
///
/// Every ELS operation requires the org UUID for defense-in-depth SQL filtering.
/// Returns an error if the endpoint cache key was constructed without an org context.
fn els_org_uuid(endpoint: &EndpointCacheUuid) -> ResultEP<Uuid> {
    endpoint
        .org()
        .map(|o| o.uuid())
        .ok_or_else(|| EpError::auth("ELS operations require an organization-scoped endpoint".to_string()))
}

// ---------------------------------------------------------------------------
// DEK management — per-endpoint envelope encryption keys
// ---------------------------------------------------------------------------

use crate::db::encryption::{self, KEY_SIZE};

/// Read the configured org-key env var name from `EdenConfig`.
/// Every organization gets its own derived key-ref name so tenant keys cannot
/// silently collapse onto one shared env var.
fn org_key_env_var(org_uuid: Uuid) -> String {
    format!("{}__{}", eden_config::encryption().org_key_env_var, org_uuid.simple().to_string().to_uppercase())
}

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Resolve the current effective policy config for assignment/copy flows.
    ///
    /// If a policy has an active promoted version, that version is the source of
    /// truth. Otherwise, the base policy row is used.
    async fn els_current_policy_config(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
    ) -> ResultEP<(String, ElsStrategy, serde_json::Value)> {
        let policy = self
            .els_get_policy(endpoint, policy_uuid)
            .await?
            .ok_or_else(|| EpError::auth("ELS policy not found or does not belong to this endpoint".to_string()))?;

        if let Some(pointer) = self.els_get_pointer(policy_uuid).await?
            && let Some(active_version) = pointer.active_version
            && let Some(version) = self.els_get_version(endpoint, policy_uuid, active_version).await?
        {
            return Ok((policy.name, version.strategy, version.config));
        }

        Ok((policy.name, policy.strategy, policy.config))
    }

    /// Retrieve (or lazily create) the active DEK for an endpoint.
    ///
    /// Returns `None` if no `OrgKeyProvider` is configured (encryption disabled).
    /// When a provider is present, ensures `org_key_refs` and `encryption_keys`
    /// rows exist, unwraps the DEK, and returns the raw key bytes.
    async fn els_dek(&self, endpoint: &EndpointCacheUuid) -> ResultEP<Option<[u8; KEY_SIZE]>> {
        if !eden_config::encryption().enabled {
            return Ok(None);
        }
        let provider = match self.org_key_provider() {
            Some(p) => p,
            None => return Ok(None),
        };

        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();
        let conn = self.pg_connection().await?;

        // 1. Ensure org_key_refs row exists (idempotent upsert).
        let key_env_var = org_key_env_var(org_uuid);
        conn.execute(sql_file!("insert", "org_key_ref"), &[&org_uuid, &provider.provider_name(), &key_env_var])
            .await
            .map_err(|e| EpError::database(format!("Failed to ensure org key ref: {e}")))?;

        // 2. Lookup key_ref for this org.
        let org_rows = conn
            .query(sql_file!("select", "org_key_ref"), &[&org_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to read org key ref: {e}")))?;
        let key_ref: String =
            org_rows.first().ok_or_else(|| EpError::database("org_key_refs row missing after upsert".to_string()))?.get("key_ref");
        if key_ref != key_env_var {
            return Err(EpError::auth(format!(
                "organization key ref must be org-specific; expected `{key_env_var}` but found `{key_ref}`"
            )));
        }

        // 3. Lookup active DEK for this endpoint.
        let dek_rows = conn
            .query(sql_file!("select", "encryption_key_active"), &[&org_uuid, &endpoint_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to read encryption key: {e}")))?;

        let wrapped_key: Vec<u8> = if let Some(row) = dek_rows.first() {
            row.get("wrapped_key")
        } else {
            // 4. No DEK yet — generate, wrap, and store.
            // TODO(key-rotation): version is hardcoded to 1. When rotation is
            // implemented, SELECT MAX(version) for this (org, endpoint) pair,
            // deactivate the previous row, and insert version + 1.
            let dek = encryption::generate_dek();
            let wrapped = provider.wrap(&key_ref, &dek).await?;
            let key_uuid = Uuid::new_v4();
            let version: i32 = 1;
            conn.execute(
                sql_file!("insert", "encryption_key"),
                &[&key_uuid, &org_uuid, &endpoint_uuid, &wrapped, &org_uuid, &version],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to store new DEK: {e}")))?;
            return Ok(Some(dek));
        };

        // 5. Unwrap the DEK.
        let dek_bytes = provider.unwrap(&key_ref, &wrapped_key).await?;
        if dek_bytes.len() != KEY_SIZE {
            return Err(EpError::parse(format!("Unwrapped DEK is {} bytes, expected {KEY_SIZE}", dek_bytes.len())));
        }
        let mut dek = [0u8; KEY_SIZE];
        dek.copy_from_slice(&dek_bytes);
        Ok(Some(dek))
    }

    cfg_if::cfg_if! {
        if #[cfg(embedded_db)] {
            /// No-op in embedded-db mode.
            async fn els_store_cache_dek(&self, _endpoint: &EndpointCacheUuid) -> ResultEP<()> {
                Ok(())
            }
        } else {
            /// Store wrapped DEK + key_ref in the ELS cache namespace so that the proxy
            /// can decrypt cache entries without hitting Postgres.
            ///
            /// Idempotent and safe to call on every cache write.
            async fn els_store_cache_dek(&self, endpoint: &EndpointCacheUuid) -> ResultEP<()> {
                if self.org_key_provider().is_none() {
                    return Ok(());
                }

                let org_uuid = els_org_uuid(endpoint)?;
                let endpoint_uuid = endpoint.uuid();
                let conn = self.pg_connection().await?;

                let org_rows = conn
                    .query(sql_file!("select", "org_key_ref"), &[&org_uuid])
                    .await
                    .map_err(|e| EpError::database(format!("Failed to read org key ref for cache DEK: {e}")))?;
                let Some(org_row) = org_rows.first() else { return Ok(()) };
                let key_ref: String = org_row.get("key_ref");
                let expected_key_ref = org_key_env_var(org_uuid);
                if key_ref != expected_key_ref {
                    return Err(EpError::auth(format!(
                        "organization key ref must be org-specific; expected `{expected_key_ref}` but found `{key_ref}`"
                    )));
                }

                let dek_rows = conn
                    .query(sql_file!("select", "encryption_key_active"), &[&org_uuid, &endpoint_uuid])
                    .await
                    .map_err(|e| EpError::database(format!("Failed to read DEK for cache: {e}")))?;
                let Some(dek_row) = dek_rows.first() else { return Ok(()) };
                let wrapped_key: Vec<u8> = dek_row.get("wrapped_key");

                let cache_key = els_key(endpoint);
                let b64_dek = base64::engine::general_purpose::STANDARD.encode(&wrapped_key);
                self.internal_cache().els_policy_set_raw(&cache_key, CACHE_DEK_FIELD, &b64_dek).await?;
                self.internal_cache().els_policy_set_raw(&cache_key, CACHE_KEY_REF_FIELD, &key_ref).await?;

                Ok(())
            }
        }
    }

    /// Encrypt a config value if encryption is enabled; otherwise return as-is.
    async fn els_encrypt_config(&self, endpoint: &EndpointCacheUuid, config: &serde_json::Value) -> ResultEP<serde_json::Value> {
        match self.els_dek(endpoint).await? {
            Some(dek) => encryption::encrypt_config(&dek, config),
            None => Ok(config.clone()),
        }
    }

    /// Decrypt a config value if it carries the encryption sentinel; otherwise
    /// return as-is (legacy plaintext).
    async fn els_decrypt_config(&self, endpoint: &EndpointCacheUuid, config: &serde_json::Value) -> ResultEP<serde_json::Value> {
        let deks = self.els_deks(endpoint).await?;
        decrypt_config_with_deks(&deks, config)
    }

    /// Retrieve all DEK versions for an endpoint, ordered newest-first.
    ///
    /// The active DEK is first when present, followed by older versions. This
    /// allows decrypt paths to remain backward-compatible during key rotation.
    async fn els_deks(&self, endpoint: &EndpointCacheUuid) -> ResultEP<Vec<[u8; KEY_SIZE]>> {
        if !eden_config::encryption().enabled {
            return Ok(Vec::new());
        }
        let provider = match self.org_key_provider() {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };

        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();
        let conn = self.pg_connection().await?;

        let org_rows = conn
            .query(sql_file!("select", "org_key_ref"), &[&org_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to read org key ref: {e}")))?;
        let Some(org_row) = org_rows.first() else { return Ok(Vec::new()) };
        let key_ref: String = org_row.get("key_ref");
        let expected_key_ref = org_key_env_var(org_uuid);
        if key_ref != expected_key_ref {
            return Err(EpError::auth(format!(
                "organization key ref must be org-specific; expected `{expected_key_ref}` but found `{key_ref}`"
            )));
        }

        let dek_rows = conn
            .query(sql_file!("select", "encryption_keys_by_endpoint"), &[&org_uuid, &endpoint_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to read encryption keys: {e}")))?;

        let mut deks = Vec::with_capacity(dek_rows.len());
        for row in dek_rows {
            let wrapped_key: Vec<u8> = row.get("wrapped_key");
            let dek_bytes = provider.unwrap(&key_ref, &wrapped_key).await?;
            if dek_bytes.len() != KEY_SIZE {
                return Err(EpError::parse(format!("Unwrapped DEK is {} bytes, expected {KEY_SIZE}", dek_bytes.len())));
            }
            let mut dek = [0u8; KEY_SIZE];
            dek.copy_from_slice(&dek_bytes);
            deks.push(dek);
        }
        Ok(deks)
    }
}

/// Cache a single user's resolved policy in the internal shared cache.
///
/// When `dek` is `Some`, the value is encrypted before storage.
/// Reserved cache entries carry cache-level encryption metadata.
/// These are stored alongside user entries in the `els::{endpoint}` cache group
/// so that consumers (e.g. the PG proxy) can decrypt without hitting Postgres.
#[cfg(not(embedded_db))]
const CACHE_DEK_FIELD: &str = "__dek";
#[cfg(not(embedded_db))]
const CACHE_KEY_REF_FIELD: &str = "__key_ref";

fn serialize_cached_policy(resolved: &ResolvedPolicy, dek: Option<&[u8; KEY_SIZE]>) -> ResultEP<String> {
    let json = serde_json::to_string(resolved).map_err(EpError::serde)?;
    match dek {
        Some(k) => encryption::encrypt_cache_value(k, &json),
        None => Ok(json),
    }
}

fn decrypt_config_with_deks(deks: &[[u8; KEY_SIZE]], config: &serde_json::Value) -> ResultEP<serde_json::Value> {
    if deks.is_empty() {
        return Ok(config.clone());
    }

    let mut last_err = None;
    for dek in deks {
        match encryption::decrypt_config(dek, config) {
            Ok(value) => return Ok(value),
            Err(err) => last_err = Some(err),
        }
    }

    Err(last_err.unwrap_or_else(|| EpError::parse("Failed to decrypt ELS config with available DEKs".to_string())))
}

cfg_if::cfg_if! {
    if #[cfg(embedded_db)] {
        async fn cache_user_policy<T: ShardCache>(
            db: &T,
            endpoint: &EndpointCacheUuid,
            user_uuid: &UserUuid,
            resolved: &ResolvedPolicy,
            _dek: Option<&[u8; KEY_SIZE]>,
        ) -> ResultEP<()> {
            let key = els_key(endpoint);
            let field = user_uuid.to_string();
            let value = serialize_cached_policy(resolved, None)?;
            db.internal_cache().els_policy_set_raw(&key, &field, &value).await?;
            Ok(())
        }

        async fn cache_user_policies_batch<T: ShardCache>(
            db: &T,
            endpoint: &EndpointCacheUuid,
            entries: &[(UserUuid, ResolvedPolicy)],
            _dek: Option<&[u8; KEY_SIZE]>,
        ) -> ResultEP<()> {
            for (user_uuid, resolved) in entries {
                cache_user_policy(db, endpoint, user_uuid, resolved, None).await?;
            }
            Ok(())
        }

        async fn uncache_user_policy<T: ShardCache>(db: &T, endpoint: &EndpointCacheUuid, user_uuid: &UserUuid) -> ResultEP<()> {
            let key = els_key(endpoint);
            let field = user_uuid.to_string();
            db.internal_cache().els_policy_del(&key, &field).await?;
            Ok(())
        }

        async fn uncache_user_policies_batch<T: ShardCache>(
            db: &T,
            endpoint: &EndpointCacheUuid,
            user_uuids: &[UserUuid],
        ) -> ResultEP<()> {
            for user_uuid in user_uuids {
                uncache_user_policy(db, endpoint, user_uuid).await?;
            }
            Ok(())
        }
    } else {
        fn decrypt_cached_policy_with_deks(deks: &[[u8; KEY_SIZE]], raw: &str) -> ResultEP<String> {
            if !raw.starts_with("ENC:") {
                return Ok(raw.to_string());
            }

            let mut last_err = None;
            for dek in deks {
                match encryption::decrypt_cache_value(dek, raw) {
                    Ok(json) => return Ok(json),
                    Err(err) => last_err = Some(err),
                }
            }

            Err(last_err.unwrap_or_else(|| EpError::parse("Failed to decrypt cached ELS policy with available DEKs".to_string())))
        }

        async fn cache_user_policy<T: ShardCache>(
            db: &T,
            endpoint: &EndpointCacheUuid,
            user_uuid: &UserUuid,
            resolved: &ResolvedPolicy,
            dek: Option<&[u8; KEY_SIZE]>,
        ) -> ResultEP<()> {
            let key = els_key(endpoint);
            let field = user_uuid.to_string();
            let value = serialize_cached_policy(resolved, dek)?;
            db.internal_cache().els_policy_set_raw(&key, &field, &value).await?;
            Ok(())
        }

        async fn cache_user_policies_batch<T: ShardCache>(
            db: &T,
            endpoint: &EndpointCacheUuid,
            entries: &[(UserUuid, ResolvedPolicy)],
            dek: Option<&[u8; KEY_SIZE]>,
        ) -> ResultEP<()> {
            if entries.is_empty() {
                return Ok(());
            }

            let key = els_key(endpoint);
            for (user_uuid, resolved) in entries {
                let value = serialize_cached_policy(resolved, dek)?;
                db.internal_cache().els_policy_set_raw(&key, &user_uuid.to_string(), &value).await?;
            }
            Ok(())
        }

        async fn uncache_user_policy<T: ShardCache>(db: &T, endpoint: &EndpointCacheUuid, user_uuid: &UserUuid) -> ResultEP<()> {
            let key = els_key(endpoint);
            let field = user_uuid.to_string();
            db.internal_cache().els_policy_del(&key, &field).await?;
            Ok(())
        }

        async fn uncache_user_policies_batch<T: ShardCache>(
            db: &T,
            endpoint: &EndpointCacheUuid,
            user_uuids: &[UserUuid],
        ) -> ResultEP<()> {
            if user_uuids.is_empty() {
                return Ok(());
            }

            let key = els_key(endpoint);
            for user_uuid in user_uuids {
                db.internal_cache().els_policy_del(&key, &user_uuid.to_string()).await?;
            }
            Ok(())
        }
    }
}

cfg_if::cfg_if! {
    if #[cfg(embedded_db)] {
        async fn clear_endpoint_cache<T: ShardCache>(db: &T, endpoint: &EndpointCacheUuid) -> ResultEP<()> {
            db.internal_cache().els_clear_endpoint(&els_key(endpoint)).await?;
            Ok(())
        }

        async fn clear_all_els_caches<T: ShardCache>(db: &T) -> ResultEP<()> {
            db.internal_cache().els_clear_all().await
        }
    } else {
        async fn clear_endpoint_cache<T: ShardCache>(db: &T, endpoint: &EndpointCacheUuid) -> ResultEP<()> {
            let key = els_key(endpoint);
            db.internal_cache().els_clear_endpoint(&key).await?;
            Ok(())
        }

        async fn clear_all_els_caches<T: ShardCache>(db: &T) -> ResultEP<()> {
            db.internal_cache().els_clear_all().await
        }
    }
}

cfg_if::cfg_if! {
    if #[cfg(embedded_db)] {
        async fn read_cache_policy<T: ShardCache>(
            db: &T,
            endpoint: &EndpointCacheUuid,
            user_uuid: &UserUuid,
            _deks: &[[u8; KEY_SIZE]],
        ) -> Option<ResolvedPolicy> {
            let key = els_key(endpoint);
            let field = user_uuid.to_string();
            let cached = db.internal_cache().els_policy_get_raw(&key, &field).await.ok()??;
            serde_json::from_str::<ResolvedPolicy>(&cached).ok()
        }

        async fn cache_assignment_exists<T: ShardCache>(db: &T, endpoint: &EndpointCacheUuid, user_uuid: &UserUuid) -> Option<bool> {
            db.internal_cache().els_policy_exists(&els_key(endpoint), &user_uuid.to_string()).await.ok()
        }
    } else {
        async fn read_cache_policy<T: ShardCache>(
            db: &T,
            endpoint: &EndpointCacheUuid,
            user_uuid: &UserUuid,
            deks: &[[u8; KEY_SIZE]],
        ) -> Option<ResolvedPolicy> {
            let key = els_key(endpoint);
            let field = user_uuid.to_string();
            let cached: Option<String> = match db.internal_cache().els_policy_get_raw(&key, &field).await {
                Ok(cached) => cached,
                Err(error) => {
                    let ctx = trace_context()
                        .with_feature("els.cache")
                        .with_additional("endpoint_uuid", endpoint.to_string())
                        .with_additional("user_uuid", user_uuid.to_string());
                    log_warn!(
                        ctx,
                        "ELS cache read skipped: internal cache read failed",
                        audience = LogAudience::Internal,
                        error = error.to_string()
                    );
                    return None;
                }
            };
            match cached {
                Some(raw) => {
                    let json_str = match decrypt_cached_policy_with_deks(deks, &raw) {
                        Ok(json) => json,
                        Err(error) => {
                            let ctx = trace_context()
                                .with_feature("els.cache")
                                .with_additional("endpoint_uuid", endpoint.to_string())
                                .with_additional("user_uuid", user_uuid.to_string());
                            log_warn!(
                                ctx,
                                "ELS cache read skipped: failed to decrypt cached policy",
                                audience = LogAudience::Internal,
                                error = error.to_string()
                            );
                            return None;
                        }
                    };
                    match serde_json::from_str::<ResolvedPolicy>(&json_str) {
                        Ok(resolved) => Some(resolved),
                        Err(error) => {
                            let ctx = trace_context()
                                .with_feature("els.cache")
                                .with_additional("endpoint_uuid", endpoint.to_string())
                                .with_additional("user_uuid", user_uuid.to_string());
                            log_warn!(
                                ctx,
                                "ELS cache read skipped: failed to deserialize cached policy",
                                audience = LogAudience::Internal,
                                error = error.to_string()
                            );
                            let _ = db.internal_cache().els_policy_del(&key, &field).await;
                            None
                        }
                    }
                }
                None => None,
            }
        }

        async fn cache_assignment_exists<T: ShardCache>(db: &T, endpoint: &EndpointCacheUuid, user_uuid: &UserUuid) -> Option<bool> {
            let key = els_key(endpoint);
            let field = user_uuid.to_string();
            db.internal_cache().els_policy_exists(&key, &field).await.ok()
        }
    }
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// ELS policy and assignment CRUD (Postgres authoritative, ShardMap cache).
pub trait ElsCommands {
    // -- Policy CRUD --

    fn els_create_policy(
        &self,
        endpoint: &EndpointCacheUuid,
        req: &CreatePolicyRequest,
    ) -> impl std::future::Future<Output = ResultEP<Uuid>> + Send;

    fn els_get_policy(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
    ) -> impl std::future::Future<Output = ResultEP<Option<ElsPolicy>>> + Send;

    fn els_list_policies(
        &self,
        endpoint: &EndpointCacheUuid,
        pagination: PaginationParams,
    ) -> impl std::future::Future<Output = ResultEP<PaginatedItems<ElsPolicy>>> + Send;

    fn els_update_policy(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
        strategy: &ElsStrategy,
        config: &serde_json::Value,
    ) -> impl std::future::Future<Output = ResultEP<()>> + Send;

    fn els_delete_policy(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
    ) -> impl std::future::Future<Output = ResultEP<bool>> + Send;

    fn els_delete_all_policies(&self, endpoint: &EndpointCacheUuid) -> impl std::future::Future<Output = ResultEP<()>> + Send;

    // -- Assignment CRUD --

    fn els_assign_user(
        &self,
        endpoint: &EndpointCacheUuid,
        user_uuid: &UserUuid,
        req: &AssignPolicyRequest,
    ) -> impl std::future::Future<Output = ResultEP<()>> + Send;

    fn els_assign_users(
        &self,
        endpoint: &EndpointCacheUuid,
        req: &BulkAssignUsersRequest,
    ) -> impl std::future::Future<Output = ResultEP<BulkAssignUsersResult>> + Send;

    fn els_get_user_policy(
        &self,
        endpoint: &EndpointCacheUuid,
        user_uuid: &UserUuid,
    ) -> impl std::future::Future<Output = ResultEP<Option<UserPolicyAssignment>>> + Send;

    fn els_list_user_assignments(
        &self,
        endpoint: &EndpointCacheUuid,
        pagination: PaginationParams,
    ) -> impl std::future::Future<Output = ResultEP<PaginatedItems<UserPolicyAssignment>>> + Send;

    fn els_unassign_user(
        &self,
        endpoint: &EndpointCacheUuid,
        user_uuid: &UserUuid,
    ) -> impl std::future::Future<Output = ResultEP<bool>> + Send;

    fn els_unassign_all(&self, endpoint: &EndpointCacheUuid) -> impl std::future::Future<Output = ResultEP<()>> + Send;

    fn els_unassign_users(
        &self,
        endpoint: &EndpointCacheUuid,
        user_uuids: &[UserUuid],
    ) -> impl std::future::Future<Output = ResultEP<usize>> + Send;

    fn els_refresh_user_policy(
        &self,
        endpoint: &EndpointCacheUuid,
        user_uuid: &UserUuid,
    ) -> impl std::future::Future<Output = ResultEP<()>> + Send;

    fn els_uncache_users(
        &self,
        endpoint: &EndpointCacheUuid,
        user_uuids: &[UserUuid],
    ) -> impl std::future::Future<Output = ResultEP<()>> + Send;

    fn els_has_assignment(
        &self,
        endpoint: &EndpointCacheUuid,
        user_uuid: &UserUuid,
    ) -> impl std::future::Future<Output = ResultEP<bool>> + Send;

    /// Fast-path ELS lookup: internal cache first, Postgres fallback.
    /// Returns the resolved typed `EpAuth` for this user+endpoint, or `None`
    /// if no ELS policy is assigned.
    fn els_resolve_auth(
        &self,
        endpoint: &EndpointCacheUuid,
        user_uuid: &UserUuid,
    ) -> impl std::future::Future<Output = ResultEP<Option<Box<dyn ep_core::ep_auth::EpAuth>>>> + Send;

    // -- Version lifecycle --

    /// Create a new draft version for a policy. Returns the new version number.
    fn els_create_version(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
        strategy: &ElsStrategy,
        config: &serde_json::Value,
        created_by: &UserUuid,
    ) -> impl std::future::Future<Output = ResultEP<i32>> + Send;

    /// Get a specific version of a policy.
    fn els_get_version(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
        version: i32,
    ) -> impl std::future::Future<Output = ResultEP<Option<ElsPolicyVersion>>> + Send;

    /// List all versions for a policy (newest first).
    fn els_list_versions(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
        pagination: PaginationParams,
    ) -> impl std::future::Future<Output = ResultEP<PaginatedItems<ElsPolicyVersion>>> + Send;

    /// Get the active version pointer for a policy.
    fn els_get_pointer(&self, policy_uuid: &PolicyUuid) -> impl std::future::Future<Output = ResultEP<Option<ElsPolicyPointer>>> + Send;

    /// Promote a Draft version to Active. Uses optimistic locking.
    /// Marks the previous active version (if any) as Superseded.
    fn els_promote_version(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
        version: i32,
        expected_current: Option<i32>,
        activated_by: &UserUuid,
    ) -> impl std::future::Future<Output = ResultEP<()>> + Send;

    /// Rollback to a Superseded version. Uses optimistic locking.
    /// Marks the current active version as Superseded.
    fn els_rollback(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
        target_version: i32,
        expected_current: i32,
        activated_by: &UserUuid,
    ) -> impl std::future::Future<Output = ResultEP<()>> + Send;

    fn els_reject_version(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
        version: i32,
    ) -> impl std::future::Future<Output = ResultEP<()>> + Send;

    fn els_warm_all_caches(&self) -> impl std::future::Future<Output = ResultEP<usize>> + Send;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl<R, P, C> ElsCommands for DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    // -- Policy CRUD --

    async fn els_create_policy(&self, endpoint: &EndpointCacheUuid, req: &CreatePolicyRequest) -> ResultEP<Uuid> {
        let policy_uuid = Uuid::new_v4();
        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();
        let strategy_str = req.strategy.as_str().to_owned();
        let encrypted_config = self.els_encrypt_config(endpoint, &req.config).await?;

        let rows = self
            .pg_connection()
            .await?
            .query(
                sql_file!("insert", "els_policy"),
                &[&policy_uuid, &org_uuid, &endpoint_uuid, &req.name, &strategy_str, &encrypted_config],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to create ELS policy: {e}")))?;

        let returned_uuid: Uuid =
            rows.first().ok_or_else(|| EpError::database("ELS policy insert returned no rows".to_string()))?.get("uuid");

        Ok(returned_uuid)
    }

    async fn els_get_policy(&self, endpoint: &EndpointCacheUuid, policy_uuid: &PolicyUuid) -> ResultEP<Option<ElsPolicy>> {
        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();
        let rows = self
            .pg_connection()
            .await?
            .query(sql_file!("select", "els_policy"), &[&policy_uuid.uuid(), &endpoint_uuid, &org_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to get ELS policy: {e}")))?;

        match rows.first() {
            Some(row) => {
                let strategy_str: String = row.get("strategy");
                let strategy = ElsStrategy::from_str_value(&strategy_str)
                    .ok_or_else(|| EpError::parse(format!("Unknown ELS strategy: {strategy_str}")))?;
                let raw_config: serde_json::Value = row.get("config");
                let config = self.els_decrypt_config(endpoint, &raw_config).await?;
                Ok(Some(ElsPolicy {
                    policy_uuid: row.get("uuid"),
                    name: row.get("name"),
                    strategy,
                    config,
                }))
            }
            None => Ok(None),
        }
    }

    async fn els_list_policies(&self, endpoint: &EndpointCacheUuid, pagination: PaginationParams) -> ResultEP<PaginatedItems<ElsPolicy>> {
        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();
        let conn = self.pg_connection().await?;
        let deks = self.els_deks(endpoint).await?;
        let rows = conn
            .query(
                sql_file!("select", "els_policies_by_endpoint"),
                &[&endpoint_uuid, &org_uuid, &pagination.limit, &pagination.offset],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to list ELS policies: {e}")))?;
        let total_row = conn
            .query_one(sql_file!("select", "els_policies_by_endpoint_count"), &[&endpoint_uuid, &org_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to count ELS policies: {e}")))?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let strategy_str: String = row.get("strategy");
            let strategy = ElsStrategy::from_str_value(&strategy_str)
                .ok_or_else(|| EpError::parse(format!("Unknown ELS strategy: {strategy_str}")))?;
            let raw_config: serde_json::Value = row.get("config");
            let config = decrypt_config_with_deks(&deks, &raw_config)?;
            result.push(ElsPolicy {
                policy_uuid: row.get("uuid"),
                name: row.get("name"),
                strategy,
                config,
            });
        }
        Ok(PaginatedItems {
            items: result,
            total: total_row.get::<_, i64>("total"),
            limit: pagination.limit,
            offset: pagination.offset,
        })
    }

    async fn els_update_policy(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
        strategy: &ElsStrategy,
        config: &serde_json::Value,
    ) -> ResultEP<()> {
        let strategy_str = strategy.as_str().to_owned();
        let raw_uuid = policy_uuid.uuid();
        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();
        let encrypted_config = self.els_encrypt_config(endpoint, config).await?;

        // Update Postgres (scoped to endpoint_uuid + org_uuid to prevent cross-org access)
        let rows_updated = self
            .pg_connection()
            .await?
            .execute(
                sql_file!("update", "els_policy"),
                &[&raw_uuid, &endpoint_uuid, &strategy_str, &encrypted_config, &org_uuid],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to update ELS policy: {e}")))?;

        if rows_updated == 0 {
            return Err(EpError::auth("ELS policy not found or does not belong to this endpoint".to_string()));
        }

        // Re-cache sync'd users
        let sync_rows = self
            .pg_connection()
            .await?
            .query(sql_file!("select", "els_policy_assignments_by_policy"), &[&raw_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to query sync'd assignments: {e}")))?;

        let resolved = ResolvedPolicy { strategy: *strategy, config: config.clone() };
        let deks = self.els_deks(endpoint).await?;
        let active_dek = deks.first();
        self.els_store_cache_dek(endpoint).await?;
        let affected_users = sync_rows.iter().map(|row| row.get("user_uuid")).collect::<Vec<UserUuid>>();
        uncache_user_policies_batch(self, endpoint, &affected_users).await?;

        let cache_entries =
            sync_rows.into_iter().map(|row| (row.get("user_uuid"), resolved.clone())).collect::<Vec<(UserUuid, ResolvedPolicy)>>();
        cache_user_policies_batch(self, endpoint, &cache_entries, active_dek).await?;

        Ok(())
    }

    async fn els_delete_policy(&self, endpoint: &EndpointCacheUuid, policy_uuid: &PolicyUuid) -> ResultEP<bool> {
        let raw_uuid = policy_uuid.uuid();
        let org_uuid = els_org_uuid(endpoint)?;
        #[cfg(embedded_db)]
        let conn = self.pg_connection().await?;
        #[cfg(not(embedded_db))]
        let mut conn = self.pg_connection().await?;
        let tx = conn
            .transaction()
            .await
            .map_err(|e| EpError::database(format!("Failed to start transaction for ELS policy delete: {e}")))?;

        // Find ALL affected users BEFORE cascade delete removes them
        let all_assignment_rows = tx
            .query(sql_file!("select", "els_policy_assignment_users_by_policy"), &[&raw_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to query policy assignments: {e}")))?;
        let affected_users = all_assignment_rows.iter().map(|row| row.get("user_uuid")).collect::<Vec<UserUuid>>();

        // Delete from Postgres (CASCADE removes assignments, scoped to endpoint + org)
        let endpoint_uuid = endpoint.uuid();
        let rows = tx
            .execute(sql_file!("delete", "els_policy"), &[&raw_uuid, &endpoint_uuid, &org_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to delete ELS policy: {e}")))?;

        tx.commit().await.map_err(|e| EpError::database(format!("Failed to commit ELS policy delete: {e}")))?;

        clear_endpoint_cache(self, endpoint).await?;
        if !affected_users.is_empty() {
            uncache_user_policies_batch(self, endpoint, &affected_users).await?;
        }

        Ok(rows > 0)
    }

    async fn els_delete_all_policies(&self, endpoint: &EndpointCacheUuid) -> ResultEP<()> {
        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();

        // Delete from Postgres (CASCADE removes assignments, scoped to org)
        self.pg_connection()
            .await?
            .execute(sql_file!("delete", "els_policies_by_endpoint"), &[&endpoint_uuid, &org_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to delete all ELS policies: {e}")))?;

        clear_endpoint_cache(self, endpoint).await?;

        Ok(())
    }

    // -- Assignment CRUD --

    async fn els_assign_user(&self, endpoint: &EndpointCacheUuid, user_uuid: &UserUuid, req: &AssignPolicyRequest) -> ResultEP<()> {
        let (_policy_name, policy_strategy, policy_config) = self.els_current_policy_config(endpoint, &req.policy_uuid).await?;

        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();
        let mode_str = req.mode.as_str().to_owned();

        // For copy mode, snapshot the config (encrypted). For sync, leave NULL.
        let (strategy_snapshot, config_snapshot): (Option<String>, Option<serde_json::Value>) = match req.mode {
            AssignmentMode::Copy => {
                let encrypted = self.els_encrypt_config(endpoint, &policy_config).await?;
                (Some(policy_strategy.as_str().to_owned()), Some(encrypted))
            }
            AssignmentMode::Sync => (None, None),
        };

        let version_ms = Utc::now().timestamp_millis();
        #[cfg(embedded_db)]
        let conn = self.pg_connection().await?;
        #[cfg(not(embedded_db))]
        let mut conn = self.pg_connection().await?;
        let tx = conn
            .transaction()
            .await
            .map_err(|e| EpError::database(format!("Failed to start transaction for ELS assignment: {e}")))?;

        tx.execute(
            sql_file!("insert", "els_policy_assignment"),
            &[
                &org_uuid,
                &endpoint_uuid,
                &user_uuid,
                &req.policy_uuid,
                &mode_str,
                &strategy_snapshot,
                &config_snapshot,
            ],
        )
        .await
        .map_err(|e| EpError::database(format!("Failed to assign ELS policy: {e}")))?;

        tx.execute(
            sql_file!("delete", "rbac_row_delete"),
            &[
                &org_uuid,
                &IdKind::Endpoint.as_str(),
                &endpoint_uuid,
                &IdKind::User.as_str(),
                &user_uuid.uuid(),
                &version_ms,
                &0i64,
            ],
        )
        .await
        .map_err(|e| EpError::database(format!("Failed to revoke endpoint RBAC during ELS assignment: {e}")))?;

        let remaining_control = tx
            .query(
                sql_file!("select", "rbac_control_verify"),
                &[
                    &org_uuid,
                    &IdKind::Endpoint.as_str(),
                    &endpoint_uuid,
                    &IdKind::User.as_str(),
                    &user_uuid.uuid(),
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to verify endpoint RBAC revocation during ELS assignment: {e}")))?;
        if !remaining_control.is_empty() {
            return Err(EpError::auth(
                "Cannot assign ELS policy while endpoint RBAC remains active for this user".to_string(),
            ));
        }

        let remaining_data = tx
            .query(
                sql_file!("select", "rbac_data_verify"),
                &[&org_uuid, &endpoint_uuid, &IdKind::User.as_str(), &user_uuid.uuid()],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to verify endpoint data RBAC revocation during ELS assignment: {e}")))?;
        if !remaining_data.is_empty() {
            return Err(EpError::auth(
                "Cannot assign ELS policy while endpoint data-plane RBAC remains active for this user".to_string(),
            ));
        }

        tx.commit().await.map_err(|e| EpError::database(format!("Failed to commit ELS assignment: {e}")))?;

        // Internal cache — always cache the resolved config (cache encryption is separate).
        let resolved = ResolvedPolicy { strategy: policy_strategy, config: policy_config };
        let deks = self.els_deks(endpoint).await?;
        let active_dek = deks.first();
        self.els_store_cache_dek(endpoint).await?;
        cache_user_policy(self, endpoint, user_uuid, &resolved, active_dek).await?;

        Ok(())
    }

    async fn els_assign_users(&self, endpoint: &EndpointCacheUuid, req: &BulkAssignUsersRequest) -> ResultEP<BulkAssignUsersResult> {
        let (_policy_name, policy_strategy, policy_config) = self.els_current_policy_config(endpoint, &req.policy_uuid).await?;

        let deduped = req.user_uuids.iter().cloned().collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>();
        if deduped.is_empty() {
            return Ok(BulkAssignUsersResult { assigned: 0, already_assigned: 0 });
        }

        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();
        let mode_str = req.mode.as_str().to_owned();
        let user_uuid_values = deduped.iter().map(EdenUuid::uuid).collect::<Vec<_>>();

        let (strategy_snapshot, config_snapshot): (Option<String>, Option<serde_json::Value>) = match req.mode {
            AssignmentMode::Copy => {
                let encrypted = self.els_encrypt_config(endpoint, &policy_config).await?;
                (Some(policy_strategy.as_str().to_owned()), Some(encrypted))
            }
            AssignmentMode::Sync => (None, None),
        };

        #[cfg(embedded_db)]
        let conn = self.pg_connection().await?;
        #[cfg(not(embedded_db))]
        let mut conn = self.pg_connection().await?;
        let tx = conn
            .transaction()
            .await
            .map_err(|e| EpError::database(format!("Failed to start transaction for bulk ELS assignment: {e}")))?;

        let existing_row = tx
            .query_one(
                sql_file!("select", "els_policy_assignment_count_for_users"),
                &[&endpoint_uuid, &org_uuid, &user_uuid_values],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to count existing ELS assignments: {e}")))?;
        let already_assigned = usize::try_from(existing_row.get::<_, i64>("total"))
            .map_err(|e| EpError::database(format!("ELS assignment count overflow: {e}")))?;

        tx.execute(
            sql_file!("insert", "els_policy_assignments_bulk"),
            &[
                &org_uuid,
                &endpoint_uuid,
                &user_uuid_values,
                &req.policy_uuid,
                &mode_str,
                &strategy_snapshot,
                &config_snapshot,
            ],
        )
        .await
        .map_err(|e| EpError::database(format!("Failed to bulk assign ELS policy: {e}")))?;

        let version_ms = Utc::now().timestamp_millis();
        for (index, user_uuid) in deduped.iter().enumerate() {
            let version_seq = i64::try_from(index).map_err(|e| EpError::database(format!("ELS assignment version overflow: {e}")))?;
            tx.execute(
                sql_file!("delete", "rbac_row_delete"),
                &[
                    &org_uuid,
                    &IdKind::Endpoint.as_str(),
                    &endpoint_uuid,
                    &IdKind::User.as_str(),
                    &user_uuid.uuid(),
                    &version_ms,
                    &version_seq,
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to revoke endpoint RBAC during bulk ELS assignment: {e}")))?;
        }

        let remaining_control = tx
            .query(
                sql_file!("select", "rbac_control_verify_subjects"),
                &[
                    &org_uuid,
                    &IdKind::Endpoint.as_str(),
                    &endpoint_uuid,
                    &IdKind::User.as_str(),
                    &user_uuid_values,
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to verify endpoint RBAC revocation during bulk ELS assignment: {e}")))?;
        if let Some(row) = remaining_control.first() {
            let user_uuid: Uuid = row.get("subject_uuid");
            return Err(EpError::auth(format!(
                "Cannot assign ELS policy while endpoint RBAC remains active for user {user_uuid}"
            )));
        }

        let remaining_data = tx
            .query(
                sql_file!("select", "rbac_data_verify_subjects"),
                &[&org_uuid, &endpoint_uuid, &IdKind::User.as_str(), &user_uuid_values],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to verify endpoint data RBAC revocation during bulk ELS assignment: {e}")))?;
        if let Some(row) = remaining_data.first() {
            let user_uuid: Uuid = row.get("subject_uuid");
            return Err(EpError::auth(format!(
                "Cannot assign ELS policy while endpoint data-plane RBAC remains active for user {user_uuid}"
            )));
        }

        tx.commit().await.map_err(|e| EpError::database(format!("Failed to commit bulk ELS assignment: {e}")))?;

        let resolved = ResolvedPolicy { strategy: policy_strategy, config: policy_config };
        let deks = self.els_deks(endpoint).await?;
        let active_dek = deks.first();
        self.els_store_cache_dek(endpoint).await?;
        uncache_user_policies_batch(self, endpoint, &deduped).await?;
        let cache_entries = deduped.into_iter().map(|user_uuid| (user_uuid, resolved.clone())).collect::<Vec<(UserUuid, ResolvedPolicy)>>();
        cache_user_policies_batch(self, endpoint, &cache_entries, active_dek).await?;

        Ok(BulkAssignUsersResult {
            assigned: cache_entries.len().saturating_sub(already_assigned),
            already_assigned,
        })
    }

    async fn els_get_user_policy(&self, endpoint: &EndpointCacheUuid, user_uuid: &UserUuid) -> ResultEP<Option<UserPolicyAssignment>> {
        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();

        let rows = self
            .pg_connection()
            .await?
            .query(sql_file!("select", "els_policy_assignment"), &[&endpoint_uuid, &user_uuid, &org_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to get user ELS assignment: {e}")))?;

        match rows.first() {
            Some(row) => {
                let mut a = parse_assignment_row(row)?;
                a.config = self.els_decrypt_config(endpoint, &a.config).await?;
                Ok(Some(a))
            }
            None => Ok(None),
        }
    }

    async fn els_list_user_assignments(
        &self,
        endpoint: &EndpointCacheUuid,
        pagination: PaginationParams,
    ) -> ResultEP<PaginatedItems<UserPolicyAssignment>> {
        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();
        let conn = self.pg_connection().await?;
        let deks = self.els_deks(endpoint).await?;

        let rows = conn
            .query(
                sql_file!("select", "els_policy_assignments_by_endpoint"),
                &[&endpoint_uuid, &org_uuid, &pagination.limit, &pagination.offset],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to list ELS assignments: {e}")))?;
        let total_row = conn
            .query_one(sql_file!("select", "els_policy_assignments_by_endpoint_count"), &[&endpoint_uuid, &org_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to count ELS assignments: {e}")))?;

        let mut result = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut a = parse_assignment_row(row)?;
            a.config = decrypt_config_with_deks(&deks, &a.config)?;
            result.push(a);
        }
        Ok(PaginatedItems {
            items: result,
            total: total_row.get::<_, i64>("total"),
            limit: pagination.limit,
            offset: pagination.offset,
        })
    }

    async fn els_unassign_user(&self, endpoint: &EndpointCacheUuid, user_uuid: &UserUuid) -> ResultEP<bool> {
        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();

        let rows = self
            .pg_connection()
            .await?
            .execute(sql_file!("delete", "els_policy_assignment"), &[&endpoint_uuid, &user_uuid, &org_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to unassign ELS policy: {e}")))?;

        uncache_user_policy(self, endpoint, user_uuid).await?;
        Ok(rows > 0)
    }

    async fn els_unassign_all(&self, endpoint: &EndpointCacheUuid) -> ResultEP<()> {
        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();

        self.pg_connection()
            .await?
            .execute(sql_file!("delete", "els_policy_assignments_by_endpoint"), &[&endpoint_uuid, &org_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to unassign all ELS policies: {e}")))?;

        // Clear cache for this endpoint
        clear_endpoint_cache(self, endpoint).await?;

        Ok(())
    }

    async fn els_unassign_users(&self, endpoint: &EndpointCacheUuid, user_uuids: &[UserUuid]) -> ResultEP<usize> {
        if user_uuids.is_empty() {
            return Ok(0);
        }

        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();
        let user_uuid_values = user_uuids.iter().map(EdenUuid::uuid).collect::<Vec<_>>();
        let rows = self
            .pg_connection()
            .await?
            .query(
                sql_file!("delete", "els_policy_assignments_by_endpoint_users"),
                &[&endpoint_uuid, &org_uuid, &user_uuid_values],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to bulk unassign ELS policies: {e}")))?;

        let removed = rows.into_iter().map(|row| row.get("user_uuid")).collect::<Vec<UserUuid>>();
        uncache_user_policies_batch(self, endpoint, &removed).await?;
        Ok(removed.len())
    }

    async fn els_refresh_user_policy(&self, endpoint: &EndpointCacheUuid, user_uuid: &UserUuid) -> ResultEP<()> {
        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();
        let assignment = self
            .els_get_user_policy(endpoint, user_uuid)
            .await?
            .ok_or_else(|| EpError::auth("ELS assignment not found".to_string()))?;

        if assignment.mode != AssignmentMode::Copy {
            return Err(EpError::parse("Only copy-mode ELS assignments can be refreshed".to_string()));
        }

        let (_policy_name, policy_strategy, policy_config) = self.els_current_policy_config(endpoint, &assignment.policy_uuid).await?;
        let encrypted_config = self.els_encrypt_config(endpoint, &policy_config).await?;

        let rows = self
            .pg_connection()
            .await?
            .execute(
                sql_file!("update", "els_policy_assignment_refresh"),
                &[&endpoint_uuid, &user_uuid, &org_uuid, &policy_strategy.as_str(), &encrypted_config],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to refresh copy-mode ELS assignment: {e}")))?;

        if rows == 0 {
            return Err(EpError::auth("ELS copy-mode assignment not found".to_string()));
        }

        let resolved = ResolvedPolicy { strategy: policy_strategy, config: policy_config };
        let deks = self.els_deks(endpoint).await?;
        let active_dek = deks.first();
        self.els_store_cache_dek(endpoint).await?;
        cache_user_policy(self, endpoint, user_uuid, &resolved, active_dek).await?;

        Ok(())
    }

    async fn els_uncache_users(&self, endpoint: &EndpointCacheUuid, user_uuids: &[UserUuid]) -> ResultEP<()> {
        uncache_user_policies_batch(self, endpoint, user_uuids).await
    }

    async fn els_has_assignment(&self, endpoint: &EndpointCacheUuid, user_uuid: &UserUuid) -> ResultEP<bool> {
        if cache_assignment_exists(self, endpoint, user_uuid).await == Some(true) {
            return Ok(true);
        }

        let org_uuid = els_org_uuid(endpoint)?;
        let endpoint_uuid = endpoint.uuid();
        let row = self
            .pg_connection()
            .await?
            .query_one(sql_file!("select", "els_policy_assignment_exists"), &[&endpoint_uuid, &user_uuid, &org_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to check ELS assignment: {e}")))?;

        Ok(row.get::<_, bool>("assignment_exists"))
    }

    async fn els_resolve_auth(
        &self,
        endpoint: &EndpointCacheUuid,
        user_uuid: &UserUuid,
    ) -> ResultEP<Option<Box<dyn ep_core::ep_auth::EpAuth>>> {
        let deks = self.els_deks(endpoint).await?;
        let active_dek = deks.first();

        // 1. Try cache first (fast path).
        //    Cache errors are intentionally non-fatal — Postgres is authoritative.
        if let Some(resolved) = read_cache_policy(self, endpoint, user_uuid, &deks).await {
            return resolved.resolve().map(Some);
        }

        // 2. Postgres fallback — look up assignment and resolve
        let assignment = self.els_get_user_policy(endpoint, user_uuid).await?;
        match assignment {
            Some(a) => {
                let resolved = ResolvedPolicy { strategy: a.strategy, config: a.config };
                // Re-populate cache for next time (best-effort)
                let _ = self.els_store_cache_dek(endpoint).await;
                let _ = cache_user_policy(self, endpoint, user_uuid, &resolved, active_dek).await;
                resolved.resolve().map(Some)
            }
            None => Ok(None),
        }
    }

    // -- Version lifecycle --

    async fn els_create_version(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
        strategy: &ElsStrategy,
        config: &serde_json::Value,
        created_by: &UserUuid,
    ) -> ResultEP<i32> {
        let raw_uuid = policy_uuid.uuid();
        let strategy_str = strategy.as_str().to_owned();
        let encrypted_config = self.els_encrypt_config(endpoint, config).await?;

        // Compute next version and insert. The PK (policy_uuid, version) prevents
        // duplicates. On the rare concurrent insert collision, retry once.
        let rows = match self
            .pg_connection()
            .await?
            .query(
                sql_file!("insert", "els_policy_version"),
                &[&raw_uuid, &strategy_str, &encrypted_config, created_by],
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("unique") || msg.contains("duplicate") || msg.contains("23505") {
                    // Concurrent insert raced — retry once with fresh connection
                    self.pg_connection()
                        .await?
                        .query(
                            sql_file!("insert", "els_policy_version"),
                            &[&raw_uuid, &strategy_str, &encrypted_config, created_by],
                        )
                        .await
                        .map_err(|e| EpError::database(format!("Failed to create ELS version after retry: {e}")))?
                } else {
                    return Err(EpError::database(format!("Failed to create ELS version: {e}")));
                }
            }
        };

        let version: i32 = rows.first().ok_or_else(|| EpError::database("ELS version insert returned no rows".to_string()))?.get("version");

        // Ensure pointer row exists (no-op if already present)
        self.pg_connection()
            .await?
            .execute(sql_file!("insert", "els_policy_pointer"), &[&raw_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to ensure ELS pointer: {e}")))?;

        Ok(version)
    }

    async fn els_get_version(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
        version: i32,
    ) -> ResultEP<Option<ElsPolicyVersion>> {
        let raw_uuid = policy_uuid.uuid();
        let rows = self
            .pg_connection()
            .await?
            .query(sql_file!("select", "els_policy_version"), &[&raw_uuid, &version])
            .await
            .map_err(|e| EpError::database(format!("Failed to get ELS version: {e}")))?;

        match rows.first() {
            Some(row) => {
                let mut v = parse_version_row(row)?;
                v.config = self.els_decrypt_config(endpoint, &v.config).await?;
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }

    async fn els_list_versions(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
        pagination: PaginationParams,
    ) -> ResultEP<PaginatedItems<ElsPolicyVersion>> {
        let raw_uuid = policy_uuid.uuid();
        let conn = self.pg_connection().await?;
        let deks = self.els_deks(endpoint).await?;
        let rows = conn
            .query(
                sql_file!("select", "els_policy_versions_by_policy"),
                &[&raw_uuid, &pagination.limit, &pagination.offset],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to list ELS versions: {e}")))?;
        let total_row = conn
            .query_one(sql_file!("select", "els_policy_versions_by_policy_count"), &[&raw_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to count ELS versions: {e}")))?;

        let mut result = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut v = parse_version_row(row)?;
            v.config = decrypt_config_with_deks(&deks, &v.config)?;
            result.push(v);
        }
        Ok(PaginatedItems {
            items: result,
            total: total_row.get::<_, i64>("total"),
            limit: pagination.limit,
            offset: pagination.offset,
        })
    }

    async fn els_get_pointer(&self, policy_uuid: &PolicyUuid) -> ResultEP<Option<ElsPolicyPointer>> {
        let raw_uuid = policy_uuid.uuid();
        let rows = self
            .pg_connection()
            .await?
            .query(sql_file!("select", "els_policy_pointer"), &[&raw_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to get ELS pointer: {e}")))?;

        match rows.first() {
            Some(row) => Ok(Some(ElsPolicyPointer {
                policy_uuid: row.get("policy_uuid"),
                active_version: row.get("active_version"),
                activated_by: row.get("activated_by"),
                activated_at: row.get("activated_at"),
            })),
            None => Ok(None),
        }
    }

    async fn els_promote_version(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
        version: i32,
        expected_current: Option<i32>,
        activated_by: &UserUuid,
    ) -> ResultEP<()> {
        let raw_uuid = policy_uuid.uuid();

        self.els_get_policy(endpoint, policy_uuid)
            .await?
            .ok_or_else(|| EpError::auth("ELS policy not found or does not belong to this endpoint".to_string()))?;

        let target = self
            .els_get_version(endpoint, policy_uuid, version)
            .await?
            .ok_or_else(|| EpError::parse(format!("ELS version {version} not found")))?;

        if target.status != ElsVersionStatus::Draft {
            return Err(EpError::parse(format!(
                "Cannot promote version {version}: status is '{}', expected 'draft'",
                target.status.as_str()
            )));
        }

        target.strategy.validate_config(&target.config)?;

        // All pointer + status mutations run inside a single transaction so a
        // crash between steps cannot leave the pointer referencing a non-active version.
        #[cfg(embedded_db)]
        let conn = self.pg_connection().await?;
        #[cfg(not(embedded_db))]
        let mut conn = self.pg_connection().await?;
        let tx = conn.transaction().await.map_err(|e| EpError::database(format!("Failed to start transaction for ELS promote: {e}")))?;

        let promoted_rows = tx
            .query(
                sql_file!("update", "els_policy_pointer_promote"),
                &[&raw_uuid, &version, &activated_by, &expected_current],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to promote ELS version: {e}")))?;

        if promoted_rows.is_empty() {
            return Err(EpError::parse(
                "Promotion conflict: active_version has changed since you last read it. Retry with the current active_version.".to_string(),
            ));
        }

        if let Some(old_version) = expected_current {
            let rows = tx
                .query(
                    sql_file!("update", "els_policy_version_status"),
                    &[
                        &raw_uuid,
                        &old_version,
                        &ElsVersionStatus::Superseded.as_str(),
                        &ElsVersionStatus::Active.as_str(),
                    ],
                )
                .await
                .map_err(|e| EpError::database(format!("Failed to supersede old version: {e}")))?;
            if rows.is_empty() {
                return Err(EpError::parse(format!(
                    "Cannot supersede version {old_version}: unexpected status (expected 'active')"
                )));
            }
        }

        let rows = tx
            .query(
                sql_file!("update", "els_policy_version_status"),
                &[
                    &raw_uuid,
                    &version,
                    &ElsVersionStatus::Active.as_str(),
                    &ElsVersionStatus::Draft.as_str(),
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to activate version: {e}")))?;
        if rows.is_empty() {
            return Err(EpError::parse(format!("Cannot activate version {version}: unexpected status (expected 'draft')")));
        }

        let sync_rows = tx
            .query(sql_file!("select", "els_policy_assignments_by_policy"), &[&raw_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to query sync'd assignments: {e}")))?;

        tx.commit().await.map_err(|e| EpError::database(format!("Failed to commit ELS promote: {e}")))?;

        let resolved = ResolvedPolicy { strategy: target.strategy, config: target.config };
        let dek = self.els_dek(endpoint).await?;
        self.els_store_cache_dek(endpoint).await?;
        let affected_users = sync_rows.iter().map(|row| row.get("user_uuid")).collect::<Vec<UserUuid>>();
        uncache_user_policies_batch(self, endpoint, &affected_users).await?;
        let cache_entries =
            sync_rows.into_iter().map(|row| (row.get("user_uuid"), resolved.clone())).collect::<Vec<(UserUuid, ResolvedPolicy)>>();
        cache_user_policies_batch(self, endpoint, &cache_entries, dek.as_ref()).await?;

        Ok(())
    }

    async fn els_rollback(
        &self,
        endpoint: &EndpointCacheUuid,
        policy_uuid: &PolicyUuid,
        target_version: i32,
        expected_current: i32,
        activated_by: &UserUuid,
    ) -> ResultEP<()> {
        let raw_uuid = policy_uuid.uuid();

        self.els_get_policy(endpoint, policy_uuid)
            .await?
            .ok_or_else(|| EpError::auth("ELS policy not found or does not belong to this endpoint".to_string()))?;

        let target = self
            .els_get_version(endpoint, policy_uuid, target_version)
            .await?
            .ok_or_else(|| EpError::parse(format!("ELS version {target_version} not found")))?;

        if target.status != ElsVersionStatus::Superseded {
            return Err(EpError::parse(format!(
                "Cannot rollback to version {target_version}: status is '{}', expected 'superseded'",
                target.status.as_str()
            )));
        }

        target.strategy.validate_config(&target.config)?;

        // All pointer + status mutations run inside a single transaction so a
        // crash between steps cannot leave the pointer referencing a non-active version.
        #[cfg(embedded_db)]
        let conn = self.pg_connection().await?;
        #[cfg(not(embedded_db))]
        let mut conn = self.pg_connection().await?;
        let tx = conn.transaction().await.map_err(|e| EpError::database(format!("Failed to start transaction for ELS rollback: {e}")))?;

        let expected: Option<i32> = Some(expected_current);
        let promoted_rows = tx
            .query(
                sql_file!("update", "els_policy_pointer_promote"),
                &[&raw_uuid, &target_version, &activated_by, &expected],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to rollback ELS version: {e}")))?;

        if promoted_rows.is_empty() {
            return Err(EpError::parse(
                "Rollback conflict: active_version has changed. Retry with the current active_version.".to_string(),
            ));
        }

        let rows = tx
            .query(
                sql_file!("update", "els_policy_version_status"),
                &[
                    &raw_uuid,
                    &expected_current,
                    &ElsVersionStatus::Superseded.as_str(),
                    &ElsVersionStatus::Active.as_str(),
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to supersede current version: {e}")))?;
        if rows.is_empty() {
            return Err(EpError::parse(format!(
                "Cannot supersede version {expected_current}: unexpected status (expected 'active')"
            )));
        }

        let rows = tx
            .query(
                sql_file!("update", "els_policy_version_status"),
                &[
                    &raw_uuid,
                    &target_version,
                    &ElsVersionStatus::Active.as_str(),
                    &ElsVersionStatus::Superseded.as_str(),
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to activate rollback version: {e}")))?;
        if rows.is_empty() {
            return Err(EpError::parse(format!(
                "Cannot activate version {target_version}: unexpected status (expected 'superseded')"
            )));
        }

        let sync_rows = tx
            .query(sql_file!("select", "els_policy_assignments_by_policy"), &[&raw_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to query sync'd assignments: {e}")))?;

        tx.commit().await.map_err(|e| EpError::database(format!("Failed to commit ELS rollback: {e}")))?;

        let resolved = ResolvedPolicy { strategy: target.strategy, config: target.config };
        let dek = self.els_dek(endpoint).await?;
        self.els_store_cache_dek(endpoint).await?;
        let affected_users = sync_rows.iter().map(|row| row.get("user_uuid")).collect::<Vec<UserUuid>>();
        uncache_user_policies_batch(self, endpoint, &affected_users).await?;
        let cache_entries =
            sync_rows.into_iter().map(|row| (row.get("user_uuid"), resolved.clone())).collect::<Vec<(UserUuid, ResolvedPolicy)>>();
        cache_user_policies_batch(self, endpoint, &cache_entries, dek.as_ref()).await?;

        Ok(())
    }

    async fn els_reject_version(&self, endpoint: &EndpointCacheUuid, policy_uuid: &PolicyUuid, version: i32) -> ResultEP<()> {
        let raw_uuid = policy_uuid.uuid();

        self.els_get_policy(endpoint, policy_uuid)
            .await?
            .ok_or_else(|| EpError::auth("ELS policy not found or does not belong to this endpoint".to_string()))?;

        let target = self
            .els_get_version(endpoint, policy_uuid, version)
            .await?
            .ok_or_else(|| EpError::parse(format!("ELS version {version} not found")))?;

        if target.status != ElsVersionStatus::Draft {
            return Err(EpError::parse(format!(
                "Cannot reject version {version}: status is '{}', expected 'draft'",
                target.status.as_str()
            )));
        }

        let rows = self
            .pg_connection()
            .await?
            .query(
                sql_file!("update", "els_policy_version_status"),
                &[
                    &raw_uuid,
                    &version,
                    &ElsVersionStatus::Rejected.as_str(),
                    &ElsVersionStatus::Draft.as_str(),
                ],
            )
            .await
            .map_err(|e| EpError::database(format!("Failed to reject ELS version: {e}")))?;

        if rows.is_empty() {
            return Err(EpError::parse(format!("Cannot reject version {version}: unexpected status (expected 'draft')")));
        }

        Ok(())
    }

    async fn els_warm_all_caches(&self) -> ResultEP<usize> {
        let mut offset = 0i64;
        let mut grouped = BTreeMap::<EndpointCacheUuid, Vec<(UserUuid, String, serde_json::Value)>>::new();
        let mut warmed = 0usize;

        clear_all_els_caches(self).await?;

        loop {
            let rows = self
                .pg_connection()
                .await?
                .query(sql_file!("select", "els_assignments_warm_all"), &[&ELS_WARM_BATCH_SIZE, &offset])
                .await
                .map_err(|e| EpError::database(format!("Failed to query ELS assignments for cache warmup: {e}")))?;

            if rows.is_empty() {
                break;
            }

            let batch_len = rows.len();
            grouped.clear();

            for row in rows {
                let org_uuid: OrganizationUuid = row.get("org_uuid");
                let endpoint_uuid: EndpointUuid = row.get("endpoint_uuid");
                let endpoint_cache_uuid =
                    EndpointCacheUuid::new(Some(eden_core::format::cache_uuid::OrganizationCacheUuid::new(None, org_uuid)), endpoint_uuid);

                grouped.entry(endpoint_cache_uuid).or_default().push((row.get("user_uuid"), row.get("strategy"), row.get("config")));
            }

            for (endpoint_cache_uuid, entries) in &grouped {
                let deks = self.els_deks(endpoint_cache_uuid).await?;
                self.els_store_cache_dek(endpoint_cache_uuid).await?;
                let mut resolved_entries = Vec::with_capacity(entries.len());
                for (user_uuid, strategy_str, encrypted_config) in entries {
                    let strategy = ElsStrategy::from_str_value(strategy_str)
                        .ok_or_else(|| EpError::parse(format!("Unknown ELS strategy during warmup: {strategy_str}")))?;
                    let config = decrypt_config_with_deks(&deks, encrypted_config)?;
                    resolved_entries.push((user_uuid.clone(), ResolvedPolicy { strategy, config }));
                }
                let active_dek = deks.first();
                cache_user_policies_batch(self, endpoint_cache_uuid, &resolved_entries, active_dek).await?;
                warmed = warmed.saturating_add(resolved_entries.len());
            }

            if batch_len < ELS_WARM_BATCH_SIZE as usize {
                break;
            }

            offset = offset.saturating_add(ELS_WARM_BATCH_SIZE);
        }

        Ok(warmed)
    }
}

// ---------------------------------------------------------------------------
// Row parsing
// ---------------------------------------------------------------------------

fn parse_assignment_row(row: &DbRow) -> ResultEP<UserPolicyAssignment> {
    let strategy_str: String = row.get("strategy");
    let strategy =
        ElsStrategy::from_str_value(&strategy_str).ok_or_else(|| EpError::parse(format!("Unknown ELS strategy: {strategy_str}")))?;

    let mode_str: String = row.get("mode");
    let mode = AssignmentMode::from_str_value(&mode_str).ok_or_else(|| EpError::parse(format!("Unknown assignment mode: {mode_str}")))?;

    let user_uuid: UserUuid = row.get("user_uuid");
    let policy_uuid: PolicyUuid = row.get("policy_uuid");

    Ok(UserPolicyAssignment {
        user_uuid,
        policy_uuid,
        policy_name: row.get("policy_name"),
        mode,
        strategy,
        config: row.get("config"),
    })
}

fn parse_version_row(row: &DbRow) -> ResultEP<ElsPolicyVersion> {
    let strategy_str: String = row.get("strategy");
    let strategy =
        ElsStrategy::from_str_value(&strategy_str).ok_or_else(|| EpError::parse(format!("Unknown ELS strategy: {strategy_str}")))?;

    let status_str: String = row.get("status");
    let status =
        ElsVersionStatus::from_str_value(&status_str).ok_or_else(|| EpError::parse(format!("Unknown ELS version status: {status_str}")))?;

    Ok(ElsPolicyVersion {
        policy_uuid: row.get("policy_uuid"),
        version: row.get("version"),
        strategy,
        config: row.get("config"),
        status,
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_els_key_format() {
        use eden_core::format::EdenUuid;
        use eden_core::format::{EndpointUuid, OrganizationCacheUuid, OrganizationUuid};

        let org = OrganizationCacheUuid::new(None, OrganizationUuid::new(Uuid::nil()));
        let ep = EndpointCacheUuid::new(Some(org), EndpointUuid::new(Uuid::nil()));
        let key = els_key(&ep);
        assert!(key.starts_with("els::"));
    }

    #[test]
    fn test_els_strategy_from_ep_kind() {
        assert_eq!(ElsStrategy::from_ep_kind(EpKind::Postgres), Some(ElsStrategy::Postgres));
        assert_eq!(ElsStrategy::from_ep_kind(EpKind::Mongo), Some(ElsStrategy::Mongo));
        assert_eq!(ElsStrategy::from_ep_kind(EpKind::Redis), Some(ElsStrategy::Redis));
        assert_eq!(ElsStrategy::from_ep_kind(EpKind::Mysql), Some(ElsStrategy::Mysql));
        assert_eq!(ElsStrategy::from_ep_kind(EpKind::Aws), Some(ElsStrategy::Aws));
        assert_eq!(ElsStrategy::from_ep_kind(EpKind::Http), Some(ElsStrategy::Http));
        assert_eq!(ElsStrategy::from_ep_kind(EpKind::Eraser), Some(ElsStrategy::Eraser));
    }

    #[test]
    fn test_els_strategy_roundtrip() {
        let strategies = [
            ElsStrategy::Postgres,
            ElsStrategy::Mysql,
            ElsStrategy::Mssql,
            ElsStrategy::Oracle,
            ElsStrategy::Mongo,
            ElsStrategy::Redis,
            ElsStrategy::Cassandra,
            ElsStrategy::Clickhouse,
            ElsStrategy::Snowflake,
            ElsStrategy::Aws,
            ElsStrategy::Rds,
            ElsStrategy::Elasticache,
            ElsStrategy::Http,
            ElsStrategy::Salesforce,
            ElsStrategy::Databricks,
            ElsStrategy::Datadog,
            ElsStrategy::Pinecone,
            ElsStrategy::Weaviate,
            ElsStrategy::Tavily,
            ElsStrategy::Llm,
            ElsStrategy::Function,
        ];
        for s in strategies {
            assert_eq!(ElsStrategy::from_str_value(s.as_str()), Some(s), "roundtrip failed for {:?}", s);
        }
    }

    #[test]
    fn test_els_strategy_legacy_compat() {
        assert_eq!(ElsStrategy::from_str_value("postgres_session_variables"), Some(ElsStrategy::Postgres));
    }

    #[test]
    fn test_assignment_mode_roundtrip() {
        assert_eq!(AssignmentMode::from_str_value("sync"), Some(AssignmentMode::Sync));
        assert_eq!(AssignmentMode::from_str_value("copy"), Some(AssignmentMode::Copy));
        assert_eq!(AssignmentMode::from_str_value("other"), None);
    }

    #[test]
    fn test_create_policy_request_serde() {
        let req = CreatePolicyRequest {
            name: "tenant-analyst".to_string(),
            strategy: ElsStrategy::Postgres,
            config: serde_json::json!({
                "db_user": "analyst",
                "variables": { "app.tenant_id": "t-123" }
            }),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let parsed: CreatePolicyRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, parsed);
    }

    #[test]
    fn test_update_policy_request_serde() {
        let req = UpdatePolicyRequest {
            strategy: ElsStrategy::Postgres,
            config: serde_json::json!({
                "variables": { "app.tenant_id": "t-123" }
            }),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let parsed: UpdatePolicyRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, parsed);
        assert!(!json.contains("\"name\""));
    }

    #[test]
    fn test_validate_policy_request_serde() {
        let req = ValidatePolicyRequest {
            strategy: ElsStrategy::Http,
            config: serde_json::json!({
                "headers": { "Authorization": "Bearer abc" }
            }),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let parsed: ValidatePolicyRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, parsed);
    }

    #[test]
    fn decrypt_config_with_deks_uses_first_matching_key() {
        let first = encryption::generate_dek();
        let second = encryption::generate_dek();
        let config = serde_json::json!({ "variables": { "app.role": "reader" } });
        let encrypted = encryption::encrypt_config(&second, &config).expect("encrypt");

        let decrypted = decrypt_config_with_deks(&[first, second], &encrypted).expect("decrypt");
        assert_eq!(decrypted, config);
    }

    #[test]
    fn decrypt_config_with_deks_errors_when_all_keys_fail() {
        let encrypting = encryption::generate_dek();
        let wrong = encryption::generate_dek();
        let config = serde_json::json!({ "headers": { "Authorization": "Bearer test" } });
        let encrypted = encryption::encrypt_config(&encrypting, &config).expect("encrypt");

        let err = decrypt_config_with_deks(&[wrong], &encrypted).expect_err("wrong key should fail");
        assert!(err.to_string().contains("decrypt") || err.to_string().contains("Failed"));
    }

    #[test]
    fn decrypt_config_with_deks_passthrough_when_no_keys_are_available() {
        let config = serde_json::json!({ "username": "reader" });
        let decrypted = decrypt_config_with_deks(&[], &config).expect("passthrough");
        assert_eq!(decrypted, config);
    }

    #[test]
    fn test_assign_policy_request_serde() {
        let req = AssignPolicyRequest {
            policy_uuid: PolicyUuid::from(Uuid::nil()),
            mode: AssignmentMode::Sync,
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let parsed: AssignPolicyRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, parsed);

        // Verify mode serialization
        assert!(json.contains("\"sync\""));
    }

    #[test]
    fn test_resolved_policy_serde() {
        let resolved = ResolvedPolicy {
            strategy: ElsStrategy::Postgres,
            config: serde_json::json!({"db_user": "analyst", "variables": {}}),
        };
        let json = serde_json::to_string(&resolved).expect("serialize");
        let parsed: ResolvedPolicy = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resolved, parsed);
    }

    // -- validate_config tests --

    #[test]
    fn test_validate_config_postgres_valid() {
        let config = serde_json::json!({"variables": {"app.tenant_id": "t-123"}});
        assert!(ElsStrategy::Postgres.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_postgres_missing_variables() {
        let config = serde_json::json!({"db_user": "analyst"});
        assert!(ElsStrategy::Postgres.validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_connection_creds_valid() {
        let config = serde_json::json!({"username": "user1", "password": "pass"});
        for strategy in [
            ElsStrategy::Mysql,
            ElsStrategy::Mssql,
            ElsStrategy::Cassandra,
            ElsStrategy::Clickhouse,
            ElsStrategy::Elasticache,
            ElsStrategy::Oracle,
            ElsStrategy::Mongo,
        ] {
            assert!(strategy.validate_config(&config).is_ok(), "failed for {:?}", strategy);
        }
        assert!(ElsStrategy::Redis.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_redis_endpoint_switch_valid() {
        let config = serde_json::json!({"endpoint_uuid": "endpoint-123"});
        assert!(ElsStrategy::Redis.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_redis_rejects_mixed_acl_and_endpoint_switch() {
        let config = serde_json::json!({"username": "user1", "endpoint_uuid": "endpoint-123"});
        assert!(ElsStrategy::Redis.validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_redis_requires_acl_or_endpoint_switch() {
        let config = serde_json::json!({"password": "pass"});
        assert!(ElsStrategy::Redis.validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_connection_creds_missing_username() {
        let config = serde_json::json!({"password": "pass"});
        assert!(ElsStrategy::Mysql.validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_aws_valid() {
        let config = serde_json::json!({"access_key_id": "AK", "secret_access_key": "SK"});
        assert!(ElsStrategy::Aws.validate_config(&config).is_ok());
        assert!(ElsStrategy::Rds.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_aws_missing_secret() {
        let config = serde_json::json!({"access_key_id": "AK"});
        assert!(ElsStrategy::Aws.validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_http_valid() {
        let config = serde_json::json!({"headers": {"Authorization": "Bearer tok"}});
        assert!(ElsStrategy::Http.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_http_headers_not_object() {
        let config = serde_json::json!({"headers": "not-an-object"});
        assert!(ElsStrategy::Http.validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_snowflake_keypair() {
        let config = serde_json::json!({"user": "u", "private_key": "PEM..."});
        assert!(ElsStrategy::Snowflake.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_snowflake_oauth() {
        let config = serde_json::json!({"oauth_token": "tok"});
        assert!(ElsStrategy::Snowflake.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_snowflake_neither() {
        let config = serde_json::json!({"user": "u"});
        assert!(ElsStrategy::Snowflake.validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_api_key_strategies() {
        let config = serde_json::json!({"api_key": "key123"});
        for strategy in [ElsStrategy::Pinecone, ElsStrategy::Weaviate, ElsStrategy::Tavily, ElsStrategy::Llm] {
            assert!(strategy.validate_config(&config).is_ok(), "failed for {:?}", strategy);
        }
    }

    #[test]
    fn test_validate_config_salesforce_valid() {
        let config = serde_json::json!({"access_token": "tok", "instance_url": "https://sf.com"});
        assert!(ElsStrategy::Salesforce.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_databricks_valid() {
        let config = serde_json::json!({"token": "dapi..."});
        assert!(ElsStrategy::Databricks.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_datadog_valid() {
        let config = serde_json::json!({"api_key": "key", "app_key": "app"});
        assert!(ElsStrategy::Datadog.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_function_accepts_anything() {
        let config = serde_json::json!({"any": "thing", "nested": {"ok": true}});
        assert!(ElsStrategy::Function.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_rejects_non_object() {
        let config = serde_json::json!("just a string");
        assert!(ElsStrategy::Postgres.validate_config(&config).is_err());
    }

    // -- Version lifecycle type tests --

    #[test]
    fn test_version_status_roundtrip() {
        let statuses = [
            ElsVersionStatus::Draft,
            ElsVersionStatus::Active,
            ElsVersionStatus::Superseded,
            ElsVersionStatus::Rejected,
        ];
        for s in statuses {
            assert_eq!(ElsVersionStatus::from_str_value(s.as_str()), Some(s), "roundtrip failed for {:?}", s);
        }
    }

    #[test]
    fn test_version_status_unknown() {
        assert_eq!(ElsVersionStatus::from_str_value("unknown"), None);
    }

    #[test]
    fn test_create_version_request_serde() {
        let req = CreateVersionRequest {
            strategy: ElsStrategy::Postgres,
            config: serde_json::json!({"variables": {"app.tenant_id": "t-123"}}),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let parsed: CreateVersionRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, parsed);
    }

    #[test]
    fn test_promote_version_request_serde() {
        let req = PromoteVersionRequest { expected_current: Some(3) };
        let json = serde_json::to_string(&req).expect("serialize");
        let parsed: PromoteVersionRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, parsed);

        let req_none = PromoteVersionRequest { expected_current: None };
        let json_none = serde_json::to_string(&req_none).expect("serialize");
        let parsed_none: PromoteVersionRequest = serde_json::from_str(&json_none).expect("deserialize");
        assert_eq!(req_none, parsed_none);
    }

    #[test]
    fn test_els_policy_version_serde() {
        let ver = ElsPolicyVersion {
            policy_uuid: PolicyUuid::from(Uuid::nil()),
            version: 1,
            strategy: ElsStrategy::Aws,
            config: serde_json::json!({"access_key_id": "AK", "secret_access_key": "SK"}),
            status: ElsVersionStatus::Draft,
            created_by: UserUuid::from(Uuid::nil()),
            created_at: DateTime::<Utc>::default(),
        };
        let json = serde_json::to_string(&ver).expect("serialize");
        let parsed: ElsPolicyVersion = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(ver, parsed);
    }

    #[test]
    fn test_validate_config_mysql_variables_valid() {
        let config = serde_json::json!({"variables": {"tenant_id": "t-123"}});
        assert!(ElsStrategy::Mysql.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_mysql_credentials_and_variables_valid() {
        let config = serde_json::json!({"username": "user1", "variables": {"role": "reader"}});
        assert!(ElsStrategy::Mysql.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_clickhouse_variables_valid() {
        let config = serde_json::json!({"variables": {"max_threads": "4"}});
        assert!(ElsStrategy::Clickhouse.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_snowflake_variables_valid() {
        let config = serde_json::json!({"variables": {"QUERY_TAG": "my-app"}});
        assert!(ElsStrategy::Snowflake.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_snowflake_variables_with_keypair_valid() {
        let config = serde_json::json!({"user": "U", "private_key": "K", "variables": {"QUERY_TAG": "test"}});
        assert!(ElsStrategy::Snowflake.validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_mysql_neither_username_nor_variables_fails() {
        let config = serde_json::json!({"password": "pass"});
        assert!(ElsStrategy::Mysql.validate_config(&config).is_err());
    }
}

// ---------------------------------------------------------------------------
// Integration tests (require PG + Redis via testcontainers)
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "infra-tests"))]
mod infra_tests {
    use super::*;
    use crate::db::rbac::{ControlPlaneRbac, DataPlaneRbac};
    use crate::test_utils::database_test_utils::create_database_manager;
    use eden_core::format::rbac::{ControlPerms, ControlPlaneRbacData, DataPerms, DataPlaneRbacData};

    /// Helper: create a test endpoint cache UUID.
    fn test_endpoint(org_uuid: Uuid, endpoint_uuid: Uuid) -> EndpointCacheUuid {
        use eden_core::format::{EndpointUuid, OrganizationCacheUuid, OrganizationUuid};
        let org = OrganizationCacheUuid::new(None, OrganizationUuid::new(org_uuid));
        EndpointCacheUuid::new(Some(org), EndpointUuid::new(endpoint_uuid))
    }

    /// Helper: insert an org row so FK constraints are satisfied.
    async fn insert_org<R: EdenRedisConnection + Sync, P: EdenPostgresConnection + Sync, C: EdenClickhouseConnection + Sync>(
        db: &DatabaseManager<R, P, C>,
        org_uuid: Uuid,
    ) {
        let conn = db.pg_connection().await.expect("pg connection");
        let org_id = format!("org-{org_uuid}");
        conn.execute(
            "INSERT INTO organizations (id, uuid, created_at, updated_at) VALUES ($1, $2, NOW(), NOW()) ON CONFLICT (uuid) DO NOTHING",
            &[&org_id, &org_uuid],
        )
        .await
        .expect("insert organization");
    }

    fn postgres_config() -> serde_json::Value {
        serde_json::json!({"variables": {"app.tenant_id": "tenant-1"}})
    }

    fn postgres_config_v2() -> serde_json::Value {
        serde_json::json!({"variables": {"app.tenant_id": "tenant-2", "app.role": "reader"}})
    }

    fn default_pagination() -> PaginationParams {
        PaginationParams::default()
    }

    // -----------------------------------------------------------------------
    // Policy CRUD
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn els_create_and_get_policy() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "tenant-isolation".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };

        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create policy");
        let policy_id = PolicyUuid::from(policy_uuid);

        let fetched = db.els_get_policy(&ep, &policy_id).await.expect("get policy");
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.name, "tenant-isolation");
        assert_eq!(fetched.strategy, ElsStrategy::Postgres);
        assert_eq!(fetched.config, postgres_config());
    }

    #[tokio::test]
    async fn els_get_nonexistent_policy_returns_none() {
        let db = create_database_manager().await;
        let dummy_ep = test_endpoint(Uuid::new_v4(), Uuid::new_v4());
        let result = db.els_get_policy(&dummy_ep, &PolicyUuid::from(Uuid::new_v4())).await.expect("get");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn els_list_policies_by_endpoint() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        insert_org(&db, org).await;

        // Create two policies on same endpoint
        for name in ["policy-a", "policy-b"] {
            let req = CreatePolicyRequest {
                name: name.to_string(),
                strategy: ElsStrategy::Postgres,
                config: postgres_config(),
            };
            db.els_create_policy(&ep, &req).await.expect("create");
        }

        let policies = db.els_list_policies(&ep, default_pagination()).await.expect("list");
        assert_eq!(policies.items.len(), 2);
    }

    #[tokio::test]
    async fn els_update_policy_changes_config() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "policy".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        let new_config = postgres_config_v2();
        db.els_update_policy(&ep, &policy_id, &ElsStrategy::Postgres, &new_config).await.expect("update");

        let fetched = db.els_get_policy(&ep, &policy_id).await.expect("get").unwrap();
        assert_eq!(fetched.config, new_config);
    }

    #[tokio::test]
    async fn els_delete_policy_removes_it() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "to-delete".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        let deleted = db.els_delete_policy(&ep, &policy_id).await.expect("delete");
        assert!(deleted);

        let fetched = db.els_get_policy(&ep, &policy_id).await.expect("get");
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn els_delete_nonexistent_policy_returns_false() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep = test_endpoint(org, Uuid::new_v4());
        insert_org(&db, org).await;

        let deleted = db.els_delete_policy(&ep, &PolicyUuid::from(Uuid::new_v4())).await.expect("delete");
        assert!(!deleted);
    }

    #[tokio::test]
    async fn els_duplicate_policy_name_per_endpoint_upserts() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep = test_endpoint(org, Uuid::new_v4());
        insert_org(&db, org).await;

        let req1 = CreatePolicyRequest {
            name: "same-name".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };

        let uuid1 = db.els_create_policy(&ep, &req1).await.expect("first create");

        // Second create with same name but different config — should upsert
        let req2 = CreatePolicyRequest {
            name: "same-name".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config_v2(),
        };
        let uuid2 = db.els_create_policy(&ep, &req2).await.expect("upsert create");

        // Upsert should return the EXISTING row's UUID (Bug 1 fix)
        assert_eq!(uuid1, uuid2, "upsert should return the existing policy uuid");

        // Config should be updated on the original policy
        let policy = db.els_get_policy(&ep, &PolicyUuid::from(uuid1)).await.expect("get").unwrap();
        assert_eq!(policy.config, postgres_config_v2(), "upsert should update the config");

        // Only one policy should exist on the endpoint
        let policies = db.els_list_policies(&ep, default_pagination()).await.expect("list");
        assert_eq!(policies.items.len(), 1);
    }

    // -----------------------------------------------------------------------
    // Assignment CRUD (sync vs copy)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn els_assign_user_sync_mode_resolves_from_policy() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "sync-policy".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create policy");
        let policy_id = PolicyUuid::from(policy_uuid);

        let assign_req = AssignPolicyRequest { policy_uuid: policy_id.clone(), mode: AssignmentMode::Sync };
        db.els_assign_user(&ep, &user, &assign_req).await.expect("assign");

        let assignment = db.els_get_user_policy(&ep, &user).await.expect("get assignment").unwrap();
        assert_eq!(assignment.mode, AssignmentMode::Sync);
        assert_eq!(assignment.strategy, ElsStrategy::Postgres);
        assert_eq!(assignment.config, postgres_config(), "sync mode should resolve config from policy");
    }

    #[tokio::test]
    async fn els_assign_user_copy_mode_snapshots_config() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "copy-policy".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create policy");
        let policy_id = PolicyUuid::from(policy_uuid);

        let assign_req = AssignPolicyRequest { policy_uuid: policy_id.clone(), mode: AssignmentMode::Copy };
        db.els_assign_user(&ep, &user, &assign_req).await.expect("assign copy");

        // Update the policy config AFTER assignment
        db.els_update_policy(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config_v2()).await.expect("update policy");

        // Copy-mode user should still see the ORIGINAL config (snapshot at assignment time)
        let assignment = db.els_get_user_policy(&ep, &user).await.expect("get assignment").unwrap();
        assert_eq!(assignment.mode, AssignmentMode::Copy);
        assert_eq!(assignment.config, postgres_config(), "copy mode should retain snapshot, not updated config");
    }

    #[tokio::test]
    async fn els_sync_mode_reflects_policy_updates() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "sync-update".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        let assign_req = AssignPolicyRequest { policy_uuid: policy_id.clone(), mode: AssignmentMode::Sync };
        db.els_assign_user(&ep, &user, &assign_req).await.expect("assign sync");

        // Update the policy
        db.els_update_policy(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config_v2()).await.expect("update");

        // Sync-mode user should see the UPDATED config
        let assignment = db.els_get_user_policy(&ep, &user).await.expect("get").unwrap();
        assert_eq!(assignment.config, postgres_config_v2(), "sync mode should reflect updated config");
    }

    #[tokio::test]
    async fn els_list_user_assignments() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "list-test".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        // Assign two users
        for _ in 0..2 {
            let user = UserUuid::from(Uuid::new_v4());
            let assign_req = AssignPolicyRequest { policy_uuid: policy_id.clone(), mode: AssignmentMode::Sync };
            db.els_assign_user(&ep, &user, &assign_req).await.expect("assign");
        }

        let assignments = db.els_list_user_assignments(&ep, default_pagination()).await.expect("list");
        assert_eq!(assignments.items.len(), 2);
    }

    #[tokio::test]
    async fn els_has_assignment_is_scoped_per_user_and_endpoint() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_a = test_endpoint(org, Uuid::new_v4());
        let ep_b = test_endpoint(org, Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "exists-scope".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep_a, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        let user_a = UserUuid::from(Uuid::new_v4());
        let user_b = UserUuid::from(Uuid::new_v4());
        db.els_assign_user(&ep_a, &user_a, &AssignPolicyRequest { policy_uuid: policy_id, mode: AssignmentMode::Sync })
            .await
            .expect("assign");

        assert!(db.els_has_assignment(&ep_a, &user_a).await.expect("user a on ep a"));
        assert!(!db.els_has_assignment(&ep_a, &user_b).await.expect("user b on ep a"));
        assert!(!db.els_has_assignment(&ep_b, &user_a).await.expect("user a on ep b"));
    }

    #[tokio::test]
    async fn els_list_policies_paginates_and_reports_total() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep = test_endpoint(org, Uuid::new_v4());
        insert_org(&db, org).await;

        for name in ["policy-a", "policy-b", "policy-c"] {
            let req = CreatePolicyRequest {
                name: name.to_string(),
                strategy: ElsStrategy::Postgres,
                config: postgres_config(),
            };
            db.els_create_policy(&ep, &req).await.expect("create");
        }

        let page = db.els_list_policies(&ep, PaginationParams { limit: 1, offset: 1 }).await.expect("list paged");

        assert_eq!(page.total, 3);
        assert_eq!(page.limit, 1);
        assert_eq!(page.offset, 1);
        assert_eq!(page.items.len(), 1);
    }

    #[tokio::test]
    async fn els_assign_users_bulk_returns_summary() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep = test_endpoint(org, Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "bulk-assign".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        let existing_user = UserUuid::from(Uuid::new_v4());
        db.els_assign_user(
            &ep,
            &existing_user,
            &AssignPolicyRequest { policy_uuid: policy_id.clone(), mode: AssignmentMode::Sync },
        )
        .await
        .expect("assign existing");

        let new_user = UserUuid::from(Uuid::new_v4());
        let summary = db
            .els_assign_users(
                &ep,
                &BulkAssignUsersRequest {
                    policy_uuid: policy_id.clone(),
                    mode: AssignmentMode::Sync,
                    user_uuids: vec![existing_user.clone(), new_user.clone()],
                },
            )
            .await
            .expect("bulk assign");

        assert_eq!(summary, BulkAssignUsersResult { assigned: 1, already_assigned: 1 });

        let assignments = db.els_list_user_assignments(&ep, default_pagination()).await.expect("list");
        assert_eq!(assignments.total, 2);
    }

    #[tokio::test]
    async fn els_assign_user_revokes_endpoint_rbac_before_commit() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let endpoint_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, endpoint_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "rbac-exclusive".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        db.control_plane_grant(
            &ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: "endpoint".to_string(),
                entity_uuid: endpoint_uuid,
                subject_kind: "user".to_string(),
                subject_uuid: user.uuid(),
                perms: ControlPerms::READ,
            },
            100,
            0,
        )
        .await
        .expect("grant control-plane rbac");

        db.data_plane_grant(
            &DataPlaneRbacData {
                org_uuid: org,
                endpoint_uuid,
                subject_kind: "user".to_string(),
                subject_uuid: user.uuid(),
                perms: DataPerms::READ,
            },
            100,
            0,
        )
        .await
        .expect("grant data-plane rbac");

        db.els_assign_user(&ep, &user, &AssignPolicyRequest { policy_uuid: policy_id, mode: AssignmentMode::Sync })
            .await
            .expect("assign els policy");

        let control =
            db.control_plane_get(org, IdKind::Endpoint, endpoint_uuid, IdKind::User, user.uuid()).await.expect("get control perms");
        assert!(control.is_empty(), "control-plane RBAC should be revoked when ELS is assigned");

        let data = db.data_plane_get(org, endpoint_uuid, IdKind::User, user.uuid()).await.expect("get data perms");
        assert!(data.is_empty(), "data-plane RBAC should be revoked when ELS is assigned");

        let assignment = db.els_get_user_policy(&ep, &user).await.expect("get assignment");
        assert!(assignment.is_some(), "ELS assignment should persist after RBAC revocation");
    }

    #[tokio::test]
    async fn els_assign_users_bulk_revokes_endpoint_rbac_for_each_user() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let endpoint_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, endpoint_uuid);
        let users = vec![UserUuid::from(Uuid::new_v4()), UserUuid::from(Uuid::new_v4())];
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "bulk-rbac-exclusive".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        for (index, user) in users.iter().enumerate() {
            let version_seq = i64::try_from(index).expect("version seq");
            db.control_plane_grant(
                &ControlPlaneRbacData {
                    org_uuid: org,
                    entity_kind: "endpoint".to_string(),
                    entity_uuid: endpoint_uuid,
                    subject_kind: "user".to_string(),
                    subject_uuid: user.uuid(),
                    perms: ControlPerms::READ,
                },
                100,
                version_seq,
            )
            .await
            .expect("grant control-plane rbac");

            db.data_plane_grant(
                &DataPlaneRbacData {
                    org_uuid: org,
                    endpoint_uuid,
                    subject_kind: "user".to_string(),
                    subject_uuid: user.uuid(),
                    perms: DataPerms::READ,
                },
                100,
                version_seq,
            )
            .await
            .expect("grant data-plane rbac");
        }

        db.els_assign_users(
            &ep,
            &BulkAssignUsersRequest {
                policy_uuid: policy_id,
                mode: AssignmentMode::Sync,
                user_uuids: users.clone(),
            },
        )
        .await
        .expect("bulk assign els policy");

        for user in &users {
            let control =
                db.control_plane_get(org, IdKind::Endpoint, endpoint_uuid, IdKind::User, user.uuid()).await.expect("get control perms");
            assert!(control.is_empty(), "control-plane RBAC should be revoked for every bulk-assigned user");

            let data = db.data_plane_get(org, endpoint_uuid, IdKind::User, user.uuid()).await.expect("get data perms");
            assert!(data.is_empty(), "data-plane RBAC should be revoked for every bulk-assigned user");
        }
    }

    #[tokio::test]
    async fn els_unassign_user_removes_assignment() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "unassign-test".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        let assign_req = AssignPolicyRequest { policy_uuid: policy_id, mode: AssignmentMode::Sync };
        db.els_assign_user(&ep, &user, &assign_req).await.expect("assign");

        let removed = db.els_unassign_user(&ep, &user).await.expect("unassign");
        assert!(removed);

        let assignment = db.els_get_user_policy(&ep, &user).await.expect("get after unassign");
        assert!(assignment.is_none());
    }

    #[tokio::test]
    async fn els_unassign_nonexistent_user_returns_false() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep = test_endpoint(org, Uuid::new_v4());
        insert_org(&db, org).await;

        let removed = db.els_unassign_user(&ep, &UserUuid::from(Uuid::new_v4())).await.expect("unassign");
        assert!(!removed);
    }

    #[tokio::test]
    async fn els_unassign_users_bulk_removes_only_requested_users() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep = test_endpoint(org, Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "bulk-unassign".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        let users = vec![UserUuid::from(Uuid::new_v4()), UserUuid::from(Uuid::new_v4())];
        for user in &users {
            db.els_assign_user(&ep, user, &AssignPolicyRequest { policy_uuid: policy_id.clone(), mode: AssignmentMode::Sync })
                .await
                .expect("assign");
        }

        let removed = db.els_unassign_users(&ep, &[users[0].clone()]).await.expect("bulk unassign");
        assert_eq!(removed, 1);
        assert!(db.els_get_user_policy(&ep, &users[0]).await.expect("first user").is_none());
        assert!(db.els_get_user_policy(&ep, &users[1]).await.expect("second user").is_some());
    }

    #[tokio::test]
    async fn els_unassign_users_empty_slice_is_a_noop() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep = test_endpoint(org, Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "empty-unassign".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        let users = vec![UserUuid::from(Uuid::new_v4()), UserUuid::from(Uuid::new_v4())];
        for user in &users {
            db.els_assign_user(&ep, user, &AssignPolicyRequest { policy_uuid: policy_id.clone(), mode: AssignmentMode::Sync })
                .await
                .expect("assign");
        }

        let removed = db.els_unassign_users(&ep, &[]).await.expect("empty bulk unassign");
        assert_eq!(removed, 0, "empty input should not remove any assignments");

        let assignments = db.els_list_user_assignments(&ep, default_pagination()).await.expect("list");
        assert_eq!(assignments.total, 2, "existing assignments must remain untouched");
    }

    #[tokio::test]
    async fn els_refresh_copy_mode_assignment_updates_snapshot() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep = test_endpoint(org, Uuid::new_v4());
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "copy-refresh".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        db.els_assign_user(&ep, &user, &AssignPolicyRequest { policy_uuid: policy_id.clone(), mode: AssignmentMode::Copy })
            .await
            .expect("assign copy");

        db.els_update_policy(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config_v2()).await.expect("update policy");
        db.els_refresh_user_policy(&ep, &user).await.expect("refresh copy assignment");

        let assignment = db.els_get_user_policy(&ep, &user).await.expect("get assignment").expect("assignment exists");
        assert_eq!(assignment.mode, AssignmentMode::Copy);
        assert_eq!(
            assignment.config,
            postgres_config_v2(),
            "copy-mode refresh should resnapshot the latest policy config"
        );
    }

    #[tokio::test]
    async fn els_warm_all_caches_clears_stale_endpoint_cache_without_assignments() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep = test_endpoint(org, Uuid::new_v4());
        let user = UserUuid::from(Uuid::new_v4());
        cache_user_policy(
            &db,
            &ep,
            &user,
            &ResolvedPolicy { strategy: ElsStrategy::Postgres, config: postgres_config() },
            None,
        )
        .await
        .expect("seed stale cache");
        let exists_before = cache_assignment_exists(&db, &ep, &user).await.expect("check stale key");
        assert!(exists_before, "stale key should exist before warmup");

        let warmed = db.els_warm_all_caches().await.expect("warm caches");
        assert_eq!(warmed, 0, "warmup without assignments should not report cached users");

        let exists_after = cache_assignment_exists(&db, &ep, &user).await.expect("check key after warmup");
        assert!(!exists_after, "warmup should clear stale endpoint cache entries even when no assignments remain");
    }

    #[tokio::test]
    async fn els_warm_all_caches_repopulates_assignments_after_full_clear() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep = test_endpoint(org, Uuid::new_v4());
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "warmup-repopulate".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        db.els_assign_user(&ep, &user, &AssignPolicyRequest { policy_uuid: policy_id, mode: AssignmentMode::Sync })
            .await
            .expect("assign");

        clear_all_els_caches(&db).await.expect("clear all ELS caches");
        let warmed = db.els_warm_all_caches().await.expect("warm caches");
        assert_eq!(warmed, 1, "warmup should repopulate the assigned user");

        let cached = cache_assignment_exists(&db, &ep, &user).await.expect("read warmed cache");
        assert!(cached, "warmup should restore the cached assignment entry");
    }

    #[tokio::test]
    async fn els_delete_policy_cascades_to_assignments() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "cascade-test".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        let assign_req = AssignPolicyRequest { policy_uuid: policy_id.clone(), mode: AssignmentMode::Sync };
        db.els_assign_user(&ep, &user, &assign_req).await.expect("assign");

        // Delete the policy — assignments should cascade
        db.els_delete_policy(&ep, &policy_id).await.expect("delete policy");

        let assignment = db.els_get_user_policy(&ep, &user).await.expect("get after cascade");
        assert!(assignment.is_none(), "assignment should be cascade-deleted with policy");
    }

    // -----------------------------------------------------------------------
    // Version lifecycle
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn els_version_lifecycle_create_promote_supersede() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        // Create a policy
        let req = CreatePolicyRequest {
            name: "versioned".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create policy");
        let policy_id = PolicyUuid::from(policy_uuid);

        // Create version 1 (draft)
        let v1 = db
            .els_create_version(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config(), &user)
            .await
            .expect("create version 1");
        assert_eq!(v1, 1);

        // Verify it's draft
        let version = db.els_get_version(&ep, &policy_id, 1).await.expect("get v1").unwrap();
        assert_eq!(version.status, ElsVersionStatus::Draft);

        // Promote version 1 (first promotion, expected_current = None)
        db.els_promote_version(&ep, &policy_id, 1, None, &user).await.expect("promote v1");

        // Verify v1 is now active
        let version = db.els_get_version(&ep, &policy_id, 1).await.expect("get v1 after promote").unwrap();
        assert_eq!(version.status, ElsVersionStatus::Active);

        // Verify pointer
        let pointer = db.els_get_pointer(&policy_id).await.expect("get pointer").unwrap();
        assert_eq!(pointer.active_version, Some(1));

        // Create version 2
        let v2 = db
            .els_create_version(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config_v2(), &user)
            .await
            .expect("create version 2");
        assert_eq!(v2, 2);

        // Promote version 2 (expected_current = 1)
        db.els_promote_version(&ep, &policy_id, 2, Some(1), &user).await.expect("promote v2");

        // v1 should be superseded, v2 should be active
        let v1_after = db.els_get_version(&ep, &policy_id, 1).await.expect("get v1").unwrap();
        assert_eq!(v1_after.status, ElsVersionStatus::Superseded);

        let v2_after = db.els_get_version(&ep, &policy_id, 2).await.expect("get v2").unwrap();
        assert_eq!(v2_after.status, ElsVersionStatus::Active);

        let pointer = db.els_get_pointer(&policy_id).await.expect("pointer").unwrap();
        assert_eq!(pointer.active_version, Some(2));
    }

    #[tokio::test]
    async fn els_promote_non_draft_version_fails() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "no-double-promote".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        db.els_create_version(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config(), &user).await.expect("create v1");

        // Promote v1
        db.els_promote_version(&ep, &policy_id, 1, None, &user).await.expect("promote v1");

        // Try to promote v1 again (it's now Active, not Draft)
        let result = db.els_promote_version(&ep, &policy_id, 1, Some(1), &user).await;
        assert!(result.is_err(), "promoting an already-active version should fail");
    }

    #[tokio::test]
    async fn els_reject_version_transitions_draft_to_rejected() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "rejectable".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        db.els_create_version(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config(), &user).await.expect("create v1");
        db.els_reject_version(&ep, &policy_id, 1).await.expect("reject v1");

        let version = db.els_get_version(&ep, &policy_id, 1).await.expect("get v1").expect("version exists");
        assert_eq!(version.status, ElsVersionStatus::Rejected);
    }

    #[tokio::test]
    async fn els_promote_with_wrong_expected_current_fails() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "conflict-test".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        db.els_create_version(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config(), &user).await.expect("create v1");
        db.els_promote_version(&ep, &policy_id, 1, None, &user).await.expect("promote v1");

        db.els_create_version(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config_v2(), &user).await.expect("create v2");

        // Try to promote v2 with wrong expected_current (999 instead of 1)
        let result = db.els_promote_version(&ep, &policy_id, 2, Some(999), &user).await;
        assert!(result.is_err(), "wrong expected_current should trigger optimistic lock conflict");
    }

    #[tokio::test]
    async fn els_rollback_to_superseded_version() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "rollback-test".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        // v1: promote
        db.els_create_version(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config(), &user).await.expect("create v1");
        db.els_promote_version(&ep, &policy_id, 1, None, &user).await.expect("promote v1");

        // v2: promote (supersedes v1)
        db.els_create_version(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config_v2(), &user).await.expect("create v2");
        db.els_promote_version(&ep, &policy_id, 2, Some(1), &user).await.expect("promote v2");

        // Rollback to v1 (expected_current = 2)
        db.els_rollback(&ep, &policy_id, 1, 2, &user).await.expect("rollback to v1");

        // v1 should be active again, v2 should be superseded
        let v1 = db.els_get_version(&ep, &policy_id, 1).await.expect("get v1").unwrap();
        assert_eq!(v1.status, ElsVersionStatus::Active);

        let v2 = db.els_get_version(&ep, &policy_id, 2).await.expect("get v2").unwrap();
        assert_eq!(v2.status, ElsVersionStatus::Superseded);

        let pointer = db.els_get_pointer(&policy_id).await.expect("pointer").unwrap();
        assert_eq!(pointer.active_version, Some(1));
    }

    #[tokio::test]
    async fn els_rollback_to_non_superseded_version_fails() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "bad-rollback".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        db.els_create_version(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config(), &user).await.expect("create v1");
        db.els_promote_version(&ep, &policy_id, 1, None, &user).await.expect("promote v1");

        db.els_create_version(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config_v2(), &user).await.expect("create v2");

        // Try to rollback to v2 which is still Draft (not Superseded)
        let result = db.els_rollback(&ep, &policy_id, 2, 1, &user).await;
        assert!(result.is_err(), "cannot rollback to a draft version");
    }

    #[tokio::test]
    async fn els_list_versions() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "versions-list".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        // Create 3 versions
        for _ in 0..3 {
            db.els_create_version(&ep, &policy_id, &ElsStrategy::Postgres, &postgres_config(), &user).await.expect("create version");
        }

        let versions = db.els_list_versions(&ep, &policy_id, default_pagination()).await.expect("list versions");
        assert_eq!(versions.items.len(), 3);
        // Sorted by version DESC (newest first)
        assert_eq!(versions.items[0].version, 3);
        assert_eq!(versions.items[1].version, 2);
        assert_eq!(versions.items[2].version, 1);
    }

    // -----------------------------------------------------------------------
    // Endpoint isolation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn els_policies_are_scoped_to_endpoint() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep1 = test_endpoint(org, Uuid::new_v4());
        let ep2 = test_endpoint(org, Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "same-name".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };

        // Same policy name on different endpoints should work
        db.els_create_policy(&ep1, &req).await.expect("create on ep1");
        db.els_create_policy(&ep2, &req).await.expect("create on ep2");

        let ep1_policies = db.els_list_policies(&ep1, default_pagination()).await.expect("list ep1");
        let ep2_policies = db.els_list_policies(&ep2, default_pagination()).await.expect("list ep2");
        assert_eq!(ep1_policies.items.len(), 1);
        assert_eq!(ep2_policies.items.len(), 1);
        assert_ne!(ep1_policies.items[0].policy_uuid, ep2_policies.items[0].policy_uuid);
    }

    #[tokio::test]
    async fn els_delete_all_policies_clears_endpoint() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, ep_uuid);
        insert_org(&db, org).await;

        for name in ["a", "b", "c"] {
            let req = CreatePolicyRequest {
                name: name.to_string(),
                strategy: ElsStrategy::Postgres,
                config: postgres_config(),
            };
            db.els_create_policy(&ep, &req).await.expect("create");
        }

        db.els_delete_all_policies(&ep).await.expect("delete all");

        let policies = db.els_list_policies(&ep, default_pagination()).await.expect("list");
        assert!(policies.items.is_empty());
    }

    // -----------------------------------------------------------------------
    // Assign to nonexistent policy
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn els_assign_user_to_nonexistent_policy_fails() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let ep = test_endpoint(org, Uuid::new_v4());
        insert_org(&db, org).await;

        let assign_req = AssignPolicyRequest {
            policy_uuid: PolicyUuid::from(Uuid::new_v4()),
            mode: AssignmentMode::Sync,
        };

        let result = db.els_assign_user(&ep, &UserUuid::from(Uuid::new_v4()), &assign_req).await;
        assert!(result.is_err(), "assigning nonexistent policy should fail");
    }

    // -----------------------------------------------------------------------
    // Security: IDOR — cross-endpoint policy access prevention
    // -----------------------------------------------------------------------

    /// Verify that getting a policy with the wrong endpoint returns None (not the policy).
    #[tokio::test]
    async fn els_get_policy_wrong_endpoint_returns_none() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let ep_a = test_endpoint(org, Uuid::new_v4());
        let ep_b = test_endpoint(org, Uuid::new_v4());

        let req = CreatePolicyRequest {
            name: "secret-policy".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep_a, &req).await.expect("create on ep_a");
        let policy_id = PolicyUuid::from(policy_uuid);

        // Should succeed with correct endpoint
        let found = db.els_get_policy(&ep_a, &policy_id).await.expect("get with correct ep");
        assert!(found.is_some(), "policy should be found on its own endpoint");

        // Should return None with wrong endpoint — prevents IDOR
        let not_found = db.els_get_policy(&ep_b, &policy_id).await.expect("get with wrong ep");
        assert!(not_found.is_none(), "policy must NOT be accessible from a different endpoint");
    }

    /// Verify that updating a policy on the wrong endpoint fails (0 rows updated).
    #[tokio::test]
    async fn els_update_policy_wrong_endpoint_fails() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let ep_a = test_endpoint(org, Uuid::new_v4());
        let ep_b = test_endpoint(org, Uuid::new_v4());

        let req = CreatePolicyRequest {
            name: "update-test".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep_a, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        // Update with wrong endpoint should fail
        let result = db.els_update_policy(&ep_b, &policy_id, &ElsStrategy::Postgres, &postgres_config_v2()).await;
        assert!(result.is_err(), "update with wrong endpoint must fail");

        // Original config should be unchanged
        let fetched = db.els_get_policy(&ep_a, &policy_id).await.expect("get").expect("exists");
        assert_eq!(fetched.config, postgres_config(), "config must remain unchanged after failed cross-endpoint update");
    }

    /// Verify that deleting a policy on the wrong endpoint does nothing.
    #[tokio::test]
    async fn els_delete_policy_wrong_endpoint_returns_false() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let ep_a = test_endpoint(org, Uuid::new_v4());
        let ep_b = test_endpoint(org, Uuid::new_v4());

        let req = CreatePolicyRequest {
            name: "delete-test".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep_a, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        // Delete with wrong endpoint should not remove anything
        let deleted = db.els_delete_policy(&ep_b, &policy_id).await.expect("delete attempt");
        assert!(!deleted, "delete with wrong endpoint must not succeed");

        // Policy should still exist on the correct endpoint
        let still_exists = db.els_get_policy(&ep_a, &policy_id).await.expect("get");
        assert!(still_exists.is_some(), "policy must still exist after failed cross-endpoint delete");
    }

    /// Verify that assigning a policy from a different endpoint fails.
    #[tokio::test]
    async fn els_assign_cross_endpoint_policy_fails() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let ep_a = test_endpoint(org, Uuid::new_v4());
        let ep_b = test_endpoint(org, Uuid::new_v4());

        // Create policy on endpoint A
        let req = CreatePolicyRequest {
            name: "ep-a-policy".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep_a, &req).await.expect("create on ep_a");
        let policy_id = PolicyUuid::from(policy_uuid);

        // Try to assign ep_a's policy to a user via ep_b — should fail
        let assign_req = AssignPolicyRequest { policy_uuid: policy_id, mode: AssignmentMode::Sync };
        let result = db.els_assign_user(&ep_b, &UserUuid::from(Uuid::new_v4()), &assign_req).await;
        assert!(result.is_err(), "cross-endpoint policy assignment must fail");
    }

    #[tokio::test]
    async fn els_assign_user_fails_when_newer_rbac_rows_remain_active() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        let endpoint_uuid = Uuid::new_v4();
        let ep = test_endpoint(org, endpoint_uuid);
        let user = UserUuid::from(Uuid::new_v4());
        insert_org(&db, org).await;

        let req = CreatePolicyRequest {
            name: "stale-revoke".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        let future_version_ms = Utc::now().timestamp_millis() + 60_000;
        db.control_plane_grant(
            &ControlPlaneRbacData {
                org_uuid: org,
                entity_kind: "endpoint".to_string(),
                entity_uuid: endpoint_uuid,
                subject_kind: "user".to_string(),
                subject_uuid: user.uuid(),
                perms: ControlPerms::READ,
            },
            future_version_ms,
            0,
        )
        .await
        .expect("grant newer control-plane rbac");

        db.data_plane_grant(
            &DataPlaneRbacData {
                org_uuid: org,
                endpoint_uuid,
                subject_kind: "user".to_string(),
                subject_uuid: user.uuid(),
                perms: DataPerms::READ,
            },
            future_version_ms,
            1,
        )
        .await
        .expect("grant newer data-plane rbac");

        let result = db.els_assign_user(&ep, &user, &AssignPolicyRequest { policy_uuid: policy_id, mode: AssignmentMode::Sync }).await;
        assert!(result.is_err(), "ELS assignment must fail when newer RBAC rows remain active");

        let control =
            db.control_plane_get(org, IdKind::Endpoint, endpoint_uuid, IdKind::User, user.uuid()).await.expect("get control perms");
        assert_eq!(control, ControlPerms::READ, "newer RBAC control row should remain untouched");

        let data = db.data_plane_get(org, endpoint_uuid, IdKind::User, user.uuid()).await.expect("get data perms");
        assert_eq!(data, DataPerms::READ, "newer RBAC data row should remain untouched");

        let assignment = db.els_get_user_policy(&ep, &user).await.expect("get assignment");
        assert!(assignment.is_none(), "failed ELS assignment should roll back the new assignment row");
    }

    /// Verify that promoting a policy version from the wrong endpoint fails.
    #[tokio::test]
    async fn els_promote_version_wrong_endpoint_fails() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let ep_a = test_endpoint(org, Uuid::new_v4());
        let ep_b = test_endpoint(org, Uuid::new_v4());

        let req = CreatePolicyRequest {
            name: "version-test".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep_a, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        // Create a draft version
        let version = db
            .els_create_version(&ep_a, &policy_id, &ElsStrategy::Postgres, &postgres_config(), &UserUuid::from(Uuid::new_v4()))
            .await
            .expect("create version");

        // Try to promote from wrong endpoint — should fail
        let result = db.els_promote_version(&ep_b, &policy_id, version, None, &UserUuid::from(Uuid::new_v4())).await;
        assert!(result.is_err(), "promoting from wrong endpoint must fail");
    }

    /// Verify that rollback from the wrong endpoint fails.
    #[tokio::test]
    async fn els_rollback_wrong_endpoint_fails() {
        let db = create_database_manager().await;
        let org = Uuid::new_v4();
        insert_org(&db, org).await;

        let ep_a = test_endpoint(org, Uuid::new_v4());
        let ep_b = test_endpoint(org, Uuid::new_v4());

        let user = UserUuid::from(Uuid::new_v4());

        let req = CreatePolicyRequest {
            name: "rollback-test".to_string(),
            strategy: ElsStrategy::Postgres,
            config: postgres_config(),
        };
        let policy_uuid = db.els_create_policy(&ep_a, &req).await.expect("create");
        let policy_id = PolicyUuid::from(policy_uuid);

        // Create and promote version 1
        let v1 = db.els_create_version(&ep_a, &policy_id, &ElsStrategy::Postgres, &postgres_config(), &user).await.expect("v1");
        db.els_promote_version(&ep_a, &policy_id, v1, None, &user).await.expect("promote v1");

        // Create and promote version 2
        let v2 = db.els_create_version(&ep_a, &policy_id, &ElsStrategy::Postgres, &postgres_config_v2(), &user).await.expect("v2");
        db.els_promote_version(&ep_a, &policy_id, v2, Some(v1), &user).await.expect("promote v2");

        // Try to rollback from wrong endpoint — should fail
        let result = db.els_rollback(&ep_b, &policy_id, v1, v2, &user).await;
        assert!(result.is_err(), "rollback from wrong endpoint must fail");
    }

    // -----------------------------------------------------------------------
    // Security: Redacted response types exclude credentials
    // -----------------------------------------------------------------------

    #[test]
    fn els_policy_redacted_excludes_config() {
        let policy = ElsPolicy {
            policy_uuid: PolicyUuid::from(Uuid::new_v4()),
            name: "test".to_string(),
            strategy: ElsStrategy::Postgres,
            config: serde_json::json!({"variables": {"secret": "top-secret-password"}}),
        };

        let redacted = ElsPolicyRedacted::from(policy);
        let json = serde_json::to_string(&redacted).expect("serialize");

        assert!(!json.contains("top-secret-password"), "redacted policy must not contain secret config");
        assert!(!json.contains("config"), "redacted policy must not have config field");
        assert!(json.contains("test"), "redacted policy should contain the name");
    }

    #[test]
    fn els_user_assignment_redacted_excludes_config() {
        let assignment = UserPolicyAssignment {
            user_uuid: UserUuid::from(Uuid::new_v4()),
            policy_uuid: PolicyUuid::from(Uuid::new_v4()),
            policy_name: "test-policy".to_string(),
            mode: AssignmentMode::Sync,
            strategy: ElsStrategy::Postgres,
            config: serde_json::json!({"variables": {"password": "secret123"}}),
        };

        let redacted = UserPolicyAssignmentRedacted::from(assignment);
        let json = serde_json::to_string(&redacted).expect("serialize");

        assert!(!json.contains("secret123"), "redacted assignment must not contain secret config");
        assert!(!json.contains("config"), "redacted assignment must not have config field");
        assert!(json.contains("test-policy"), "redacted assignment should contain policy name");
    }
}
