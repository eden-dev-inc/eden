//! # Endpoint-Level Auth (EpAuth)
//!
//! Typed per-endpoint auth structures for ELS policies. Each endpoint type
//! defines its own auth shape; the trait provides dynamic downcasting via
//! `as_any()`, following the same pattern as [`EpConfig`](crate::ep::EpConfig).
//!
//! ## Usage
//!
//! ```ignore
//! use ep_core::ep_auth::{EpAuth, resolve_ep_auth};
//!
//! // Given a resolved ELS policy (strategy + config JSON):
//! let auth: Box<dyn EpAuth> = resolve_ep_auth(strategy, &config)?;
//!
//! // Downcast to the concrete type:
//! if let Some(pg) = auth.as_any().downcast_ref::<PostgresAuth>() {
//!     for (k, v) in &pg.variables {
//!         // SET session variable k = v
//!     }
//! }
//! ```

use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Typed auth credentials for a specific endpoint type.
///
/// Implementations carry the strongly-typed fields that the proxy layer needs
/// to apply per-user credentials at request time.
pub trait EpAuth: Send + Sync + Debug {
    /// The endpoint kind this auth applies to.
    fn kind(&self) -> EpKind;

    /// Downcast to the concrete auth type.
    fn as_any(&self) -> &dyn Any;

    /// Clone into a boxed trait object.
    fn clone_box(&self) -> Box<dyn EpAuth>;

    /// Serialize back to JSON (for caching or API responses).
    fn to_json(&self) -> ResultEP<serde_json::Value>;
}

impl Clone for Box<dyn EpAuth> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

// ---------------------------------------------------------------------------
// Per-endpoint auth structs
// ---------------------------------------------------------------------------

/// Implement a redacted `Debug` that never prints secret fields.
macro_rules! redacted_debug {
    ($ty:ident { $($field:ident),* $(,)? }) => {
        impl std::fmt::Debug for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($ty))
                    $(.field(stringify!($field), &"[REDACTED]"))*
                    .finish()
            }
        }
    };
}

/// PostgreSQL: session variables injected via `SET` commands.
#[derive(Clone, Serialize, Deserialize)]
pub struct PostgresAuth {
    pub variables: HashMap<String, String>,
}
redacted_debug!(PostgresAuth { variables });

/// MySQL connection credentials with optional session variables.
#[derive(Clone, Serialize, Deserialize)]
pub struct MysqlAuth {
    pub username: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub variables: HashMap<String, String>,
}
redacted_debug!(MysqlAuth { password });

/// MSSQL connection credentials.
#[derive(Clone, Serialize, Deserialize)]
pub struct MssqlAuth {
    pub username: String,
    #[serde(default)]
    pub password: Option<String>,
}
redacted_debug!(MssqlAuth { password });

/// Oracle connection credentials.
#[derive(Clone, Serialize, Deserialize)]
pub struct OracleAuth {
    pub username: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub privilege: Option<String>,
}
redacted_debug!(OracleAuth { password });

/// MongoDB connection credentials.
#[derive(Clone, Serialize, Deserialize)]
pub struct MongoAuth {
    pub username: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub mechanism: Option<String>,
}
redacted_debug!(MongoAuth { password });

/// Redis connection credentials.
#[derive(Clone, Serialize, Deserialize)]
pub struct RedisAuth {
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub endpoint_uuid: Option<String>,
}
redacted_debug!(RedisAuth { password });

/// Cassandra connection credentials.
#[derive(Clone, Serialize, Deserialize)]
pub struct CassandraAuth {
    pub username: String,
    #[serde(default)]
    pub password: Option<String>,
}
redacted_debug!(CassandraAuth { password });

/// ClickHouse connection credentials with optional session variables.
#[derive(Clone, Serialize, Deserialize)]
pub struct ClickhouseAuth {
    pub username: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub variables: HashMap<String, String>,
}
redacted_debug!(ClickhouseAuth { password });

/// Snowflake auth — key-pair or OAuth, with optional session variables.
#[derive(Clone, Serialize, Deserialize)]
pub struct SnowflakeAuth {
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub private_key: Option<String>,
    #[serde(default)]
    pub oauth_token: Option<String>,
    #[serde(default)]
    pub variables: HashMap<String, String>,
}
redacted_debug!(SnowflakeAuth { private_key, oauth_token });

/// AWS IAM credentials.
#[derive(Clone, Serialize, Deserialize)]
pub struct AwsAuth {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
    #[serde(default)]
    pub role_arn: Option<String>,
}
redacted_debug!(AwsAuth { secret_access_key, session_token });

/// RDS IAM credentials (same shape as AWS).
#[derive(Clone, Serialize, Deserialize)]
pub struct RdsAuth {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
    #[serde(default)]
    pub role_arn: Option<String>,
}
redacted_debug!(RdsAuth { secret_access_key, session_token });

/// ElastiCache connection credentials.
#[derive(Clone, Serialize, Deserialize)]
pub struct ElasticacheAuth {
    pub username: String,
    #[serde(default)]
    pub password: Option<String>,
}
redacted_debug!(ElasticacheAuth { password });

/// HTTP headers / auth.
#[derive(Clone, Serialize, Deserialize)]
pub struct HttpAuth {
    pub headers: HashMap<String, String>,
}
redacted_debug!(HttpAuth { headers });

/// Salesforce OAuth credentials.
#[derive(Clone, Serialize, Deserialize)]
pub struct SalesforceAuth {
    pub access_token: String,
    #[serde(default)]
    pub instance_url: Option<String>,
}
redacted_debug!(SalesforceAuth { access_token });

/// Databricks token auth.
#[derive(Clone, Serialize, Deserialize)]
pub struct DatabricksAuth {
    pub token: String,
    #[serde(default)]
    pub host: Option<String>,
}
redacted_debug!(DatabricksAuth { token });

/// Datadog API key auth.
#[derive(Clone, Serialize, Deserialize)]
pub struct DatadogAuth {
    pub api_key: String,
    #[serde(default)]
    pub app_key: Option<String>,
}
redacted_debug!(DatadogAuth { api_key, app_key });

/// Pinecone API key auth.
#[derive(Clone, Serialize, Deserialize)]
pub struct PineconeAuth {
    pub api_key: String,
}
redacted_debug!(PineconeAuth { api_key });

/// Weaviate API key auth.
#[derive(Clone, Serialize, Deserialize)]
pub struct WeaviateAuth {
    pub api_key: String,
}
redacted_debug!(WeaviateAuth { api_key });

/// PostHog personal API key auth.
#[derive(Clone, Serialize, Deserialize)]
pub struct PosthogAuth {
    pub api_key: String,
}
redacted_debug!(PosthogAuth { api_key });

/// Tavily API key auth.
#[derive(Clone, Serialize, Deserialize)]
pub struct TavilyAuth {
    pub api_key: String,
}
redacted_debug!(TavilyAuth { api_key });

/// LLM provider auth.
#[derive(Clone, Serialize, Deserialize)]
pub struct LlmAuth {
    pub api_key: String,
    #[serde(default)]
    pub provider: Option<String>,
}
redacted_debug!(LlmAuth { api_key });

/// Custom function auth (free-form).
#[derive(Clone, Serialize, Deserialize)]
pub struct FunctionAuth {
    #[serde(flatten)]
    pub data: serde_json::Value,
}
redacted_debug!(FunctionAuth { data });

/// Azure Microsoft Entra ID (EntraID) auth.
///
/// Supports two modes:
/// - **Service principal**: `tenant_id` + `client_id` + `client_secret` —
///   the token is acquired from `https://login.microsoftonline.com/{tenant_id}`
///   via the OAuth2 client-credentials flow.
/// - **Pre-acquired token**: `access_token` — a Bearer token already obtained
///   from Microsoft Entra ID, sent as `Authorization: Bearer {token}`.
#[derive(Clone, Serialize, Deserialize)]
pub struct AzureAuth {
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub client_secret: Option<String>,
    #[serde(default)]
    pub access_token: Option<String>,
}
redacted_debug!(AzureAuth { client_secret, access_token });

/// GitLab personal/project/OAuth token.
#[derive(Clone, Serialize, Deserialize)]
pub struct GitlabAuth {
    pub token: String,
}
redacted_debug!(GitlabAuth { token });

/// Google Workspace OAuth2 credentials.
#[derive(Clone, Serialize, Deserialize)]
pub struct GoogleWorkspaceAuth {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
    #[serde(default)]
    pub subject: Option<String>,
}
redacted_debug!(GoogleWorkspaceAuth { client_secret, refresh_token });

/// S3-compatible IAM credentials.
#[derive(Clone, Serialize, Deserialize)]
pub struct S3Auth {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
}
redacted_debug!(S3Auth { secret_access_key, session_token });

/// Eraser API key auth.
#[derive(Clone, Serialize, Deserialize)]
pub struct EraserAuth {
    pub api_key: String,
}
redacted_debug!(EraserAuth { api_key });

// ---------------------------------------------------------------------------
// Auth application methods
// ---------------------------------------------------------------------------

// Shared helpers for SQL identifier/literal escaping (safe for injection into
// SET commands). These mirror the PG proxy's existing escape routines.

/// Escape a SQL identifier using PostgreSQL double-quote escaping (internal quotes are doubled).
fn escape_sql_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('\0', "").replace('"', "\"\""))
}

/// Escape a string for interpolation into a SQL single-quoted literal.
fn escape_sql_literal(s: &str) -> String {
    s.replace('\0', "").replace('\'', "''")
}

impl PostgresAuth {
    /// Build a SQL prefix of `SET key = 'value'; ...` statements to prepend
    /// to every query. Returns `(prefix_sql, set_count)`.
    pub fn sql_prefix(&self) -> (String, usize) {
        let mut prefix = String::new();
        let mut count = 0;
        for (name, value) in &self.variables {
            if name.is_empty() {
                continue;
            }
            let safe_name = escape_sql_identifier(name);
            prefix.push_str(&format!("SET {} = '{}'; ", safe_name, escape_sql_literal(value)));
            count += 1;
        }
        (prefix, count)
    }

    /// Return variables as a reference for direct iteration.
    pub fn variables(&self) -> &HashMap<String, String> {
        &self.variables
    }
}

/// Escape a SQL identifier using MySQL backtick escaping (internal backticks are doubled).
fn escape_mysql_identifier(name: &str) -> String {
    format!("`{}`", name.replace('\0', "").replace('`', "``"))
}

/// Escape a string for interpolation into a MySQL single-quoted literal.
fn escape_mysql_literal(s: &str) -> String {
    s.replace('\0', "").replace('\\', "\\\\").replace('\'', "''")
}

impl MysqlAuth {
    /// Build a SQL prefix of `SET @name = 'value'; ...` for MySQL user-defined
    /// variables. Returns `(prefix_sql, set_count)`.
    pub fn sql_prefix(&self) -> (String, usize) {
        let mut prefix = String::new();
        let mut count = 0;
        for (name, value) in &self.variables {
            if name.is_empty() {
                continue;
            }
            // MySQL user variables use @name syntax; backtick-escape the name
            // portion to prevent injection.
            prefix.push_str(&format!("SET @{} = '{}'; ", escape_mysql_identifier(name), escape_mysql_literal(value)));
            count += 1;
        }
        (prefix, count)
    }
}

impl ClickhouseAuth {
    /// Build a SQL prefix of `SET name = 'value'; ...` for ClickHouse session
    /// settings. Returns `(prefix_sql, set_count)`.
    pub fn sql_prefix(&self) -> (String, usize) {
        let mut prefix = String::new();
        let mut count = 0;
        for (name, value) in &self.variables {
            if name.is_empty() {
                continue;
            }
            let safe_name = escape_sql_identifier(name);
            prefix.push_str(&format!("SET {} = '{}'; ", safe_name, escape_sql_literal(value)));
            count += 1;
        }
        (prefix, count)
    }
}

/// Shared connection credential accessors for endpoint types that authenticate
/// with username + optional password.
pub trait ConnectionCredentials {
    fn username(&self) -> &str;
    fn password(&self) -> Option<&str>;
}

macro_rules! impl_connection_credentials {
    ($ty:ident) => {
        impl ConnectionCredentials for $ty {
            fn username(&self) -> &str {
                &self.username
            }
            fn password(&self) -> Option<&str> {
                self.password.as_deref()
            }
        }
    };
}

impl_connection_credentials!(MysqlAuth);
impl_connection_credentials!(MssqlAuth);
impl_connection_credentials!(CassandraAuth);
impl_connection_credentials!(ClickhouseAuth);
impl_connection_credentials!(ElasticacheAuth);

impl RedisAuth {
    pub fn endpoint_uuid(&self) -> Option<&str> {
        self.endpoint_uuid.as_deref()
    }
}

impl ConnectionCredentials for RedisAuth {
    fn username(&self) -> &str {
        self.username.as_deref().unwrap_or("")
    }

    fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }
}

impl OracleAuth {
    /// Oracle-specific: optional privilege (SYSDBA, SYSOPER, etc.).
    pub fn privilege(&self) -> Option<&str> {
        self.privilege.as_deref()
    }
}

impl ConnectionCredentials for OracleAuth {
    fn username(&self) -> &str {
        &self.username
    }
    fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }
}

impl MongoAuth {
    /// MongoDB auth mechanism (SCRAM-SHA-256, PLAIN, etc.).
    pub fn mechanism(&self) -> Option<&str> {
        self.mechanism.as_deref()
    }
}

impl ConnectionCredentials for MongoAuth {
    fn username(&self) -> &str {
        &self.username
    }
    fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }
}

impl SnowflakeAuth {
    /// Whether this is key-pair auth (vs OAuth).
    pub fn is_keypair(&self) -> bool {
        self.private_key.is_some()
    }

    /// Whether this is OAuth token auth.
    pub fn is_oauth(&self) -> bool {
        self.oauth_token.is_some()
    }

    /// Build a SQL prefix of `ALTER SESSION SET name = 'value'; ...` for
    /// Snowflake session parameters. Returns `(prefix_sql, set_count)`.
    pub fn session_prefix(&self) -> (String, usize) {
        let mut prefix = String::new();
        let mut count = 0;
        for (name, value) in &self.variables {
            if name.is_empty() {
                continue;
            }
            let safe_name = escape_sql_identifier(name);
            prefix.push_str(&format!("ALTER SESSION SET {} = '{}'; ", safe_name, escape_sql_literal(value)));
            count += 1;
        }
        (prefix, count)
    }
}

/// Shared HTTP header accessors for endpoint types that authenticate via
/// request headers (API keys, bearer tokens, etc.).
pub trait HeaderCredentials {
    /// Return HTTP headers to inject into outgoing requests.
    fn auth_headers(&self) -> HashMap<String, String>;
}

impl HeaderCredentials for HttpAuth {
    fn auth_headers(&self) -> HashMap<String, String> {
        self.headers.clone()
    }
}

impl HeaderCredentials for SalesforceAuth {
    fn auth_headers(&self) -> HashMap<String, String> {
        let mut h = HashMap::with_capacity(1);
        h.insert("Authorization".to_string(), format!("Bearer {}", self.access_token));
        h
    }
}

impl SalesforceAuth {
    /// The Salesforce instance URL for API calls.
    pub fn instance_url(&self) -> Option<&str> {
        self.instance_url.as_deref()
    }
}

impl HeaderCredentials for DatabricksAuth {
    fn auth_headers(&self) -> HashMap<String, String> {
        let mut h = HashMap::with_capacity(1);
        h.insert("Authorization".to_string(), format!("Bearer {}", self.token));
        h
    }
}

impl DatabricksAuth {
    /// The Databricks workspace host.
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }
}

impl HeaderCredentials for DatadogAuth {
    fn auth_headers(&self) -> HashMap<String, String> {
        let mut h = HashMap::with_capacity(2);
        h.insert("DD-API-KEY".to_string(), self.api_key.clone());
        if let Some(ref app_key) = self.app_key {
            h.insert("DD-APPLICATION-KEY".to_string(), app_key.clone());
        }
        h
    }
}

impl HeaderCredentials for PineconeAuth {
    fn auth_headers(&self) -> HashMap<String, String> {
        let mut h = HashMap::with_capacity(1);
        h.insert("Api-Key".to_string(), self.api_key.clone());
        h
    }
}

impl HeaderCredentials for PosthogAuth {
    fn auth_headers(&self) -> HashMap<String, String> {
        let mut h = HashMap::with_capacity(1);
        h.insert("Authorization".to_string(), format!("Bearer {}", self.api_key));
        h
    }
}

impl HeaderCredentials for WeaviateAuth {
    fn auth_headers(&self) -> HashMap<String, String> {
        let mut h = HashMap::with_capacity(1);
        h.insert("Authorization".to_string(), format!("Bearer {}", self.api_key));
        h
    }
}

impl HeaderCredentials for TavilyAuth {
    fn auth_headers(&self) -> HashMap<String, String> {
        let mut h = HashMap::with_capacity(1);
        h.insert("Authorization".to_string(), format!("Bearer {}", self.api_key));
        h
    }
}

impl HeaderCredentials for LlmAuth {
    fn auth_headers(&self) -> HashMap<String, String> {
        let mut h = HashMap::with_capacity(1);
        h.insert("Authorization".to_string(), format!("Bearer {}", self.api_key));
        h
    }
}

impl LlmAuth {
    /// The LLM provider name (openai, anthropic, etc.).
    pub fn provider(&self) -> Option<&str> {
        self.provider.as_deref()
    }
}

impl HeaderCredentials for AzureAuth {
    fn auth_headers(&self) -> HashMap<String, String> {
        let mut h = HashMap::with_capacity(1);
        if let Some(ref token) = self.access_token {
            h.insert("Authorization".to_string(), format!("Bearer {token}"));
        }
        h
    }
}

impl HeaderCredentials for GitlabAuth {
    fn auth_headers(&self) -> HashMap<String, String> {
        let mut h = HashMap::with_capacity(1);
        h.insert("PRIVATE-TOKEN".to_string(), self.token.clone());
        h
    }
}

impl HeaderCredentials for GoogleWorkspaceAuth {
    fn auth_headers(&self) -> HashMap<String, String> {
        // Note: actual OAuth token exchange happens at connection time; the
        // refresh_token itself is the stored credential.
        HashMap::new()
    }
}

impl HeaderCredentials for EraserAuth {
    fn auth_headers(&self) -> HashMap<String, String> {
        let mut h = HashMap::with_capacity(1);
        h.insert("Authorization".to_string(), format!("Bearer {}", self.api_key));
        h
    }
}

/// Shared IAM credential accessors for AWS-style endpoints.
pub trait IamCredentials {
    fn access_key_id(&self) -> &str;
    fn secret_access_key(&self) -> &str;
    fn session_token(&self) -> Option<&str>;
    fn role_arn(&self) -> Option<&str>;
}

impl IamCredentials for AwsAuth {
    fn access_key_id(&self) -> &str {
        &self.access_key_id
    }
    fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }
    fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }
    fn role_arn(&self) -> Option<&str> {
        self.role_arn.as_deref()
    }
}

impl IamCredentials for RdsAuth {
    fn access_key_id(&self) -> &str {
        &self.access_key_id
    }
    fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }
    fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }
    fn role_arn(&self) -> Option<&str> {
        self.role_arn.as_deref()
    }
}

impl IamCredentials for S3Auth {
    fn access_key_id(&self) -> &str {
        &self.access_key_id
    }
    fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }
    fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }
    fn role_arn(&self) -> Option<&str> {
        None
    }
}

impl FunctionAuth {
    /// Access the free-form config data.
    pub fn data(&self) -> &serde_json::Value {
        &self.data
    }
}

// ---------------------------------------------------------------------------
// EpAuth impls (macro to reduce boilerplate)
// ---------------------------------------------------------------------------

macro_rules! impl_ep_auth {
    ($ty:ident, $kind:expr) => {
        impl EpAuth for $ty {
            fn kind(&self) -> EpKind {
                $kind
            }
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn clone_box(&self) -> Box<dyn EpAuth> {
                Box::new(self.clone())
            }
            fn to_json(&self) -> ResultEP<serde_json::Value> {
                serde_json::to_value(self).map_err(EpError::serde)
            }
        }
    };
}

impl_ep_auth!(PostgresAuth, EpKind::Postgres);
impl_ep_auth!(MysqlAuth, EpKind::Mysql);
impl_ep_auth!(MssqlAuth, EpKind::Mssql);
impl_ep_auth!(OracleAuth, EpKind::Oracle);
impl_ep_auth!(MongoAuth, EpKind::Mongo);
impl_ep_auth!(RedisAuth, EpKind::Redis);
impl_ep_auth!(CassandraAuth, EpKind::Cassandra);
impl_ep_auth!(ClickhouseAuth, EpKind::Clickhouse);
impl_ep_auth!(SnowflakeAuth, EpKind::Snowflake);
impl_ep_auth!(AwsAuth, EpKind::Aws);
impl_ep_auth!(RdsAuth, EpKind::Rds);
impl_ep_auth!(ElasticacheAuth, EpKind::Elasticache);
impl_ep_auth!(HttpAuth, EpKind::Http);
impl_ep_auth!(SalesforceAuth, EpKind::Salesforce);
impl_ep_auth!(DatabricksAuth, EpKind::Databricks);
impl_ep_auth!(DatadogAuth, EpKind::Datadog);
impl_ep_auth!(PineconeAuth, EpKind::Pinecone);
impl_ep_auth!(PosthogAuth, EpKind::Posthog);
impl_ep_auth!(WeaviateAuth, EpKind::Weaviate);
impl_ep_auth!(TavilyAuth, EpKind::Tavily);
impl_ep_auth!(LlmAuth, EpKind::Llm);
impl_ep_auth!(FunctionAuth, EpKind::Function);
impl_ep_auth!(AzureAuth, EpKind::Azure);
impl_ep_auth!(GitlabAuth, EpKind::Gitlab);
impl_ep_auth!(GoogleWorkspaceAuth, EpKind::GoogleWorkspace);
impl_ep_auth!(S3Auth, EpKind::S3);
impl_ep_auth!(EraserAuth, EpKind::Eraser);

// ---------------------------------------------------------------------------
// Resolver: strategy + JSON config → Box<dyn EpAuth>
// ---------------------------------------------------------------------------

/// Resolve a strategy name and JSONB config into a typed `EpAuth` object.
///
/// The `strategy` string matches the values stored in `els_policies.strategy`
/// (e.g. `"postgres"`, `"mysql"`, `"aws"`).
///
/// Returns an error if the strategy is unknown or the config JSON does not
/// match the expected shape for that strategy.
pub fn resolve_ep_auth(strategy: &str, config: &serde_json::Value) -> ResultEP<Box<dyn EpAuth>> {
    match strategy {
        "postgres" | "postgres_session_variables" => {
            let auth: PostgresAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "mysql" => {
            let auth: MysqlAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "mssql" => {
            let auth: MssqlAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "oracle" => {
            let auth: OracleAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "mongo" => {
            let auth: MongoAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "redis" => {
            let auth: RedisAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "cassandra" => {
            let auth: CassandraAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "clickhouse" => {
            let auth: ClickhouseAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "snowflake" => {
            let auth: SnowflakeAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "aws" => {
            let auth: AwsAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "rds" => {
            let auth: RdsAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "elasticache" => {
            let auth: ElasticacheAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "http" => {
            let auth: HttpAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "salesforce" => {
            let auth: SalesforceAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "databricks" => {
            let auth: DatabricksAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "datadog" => {
            let auth: DatadogAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "pinecone" => {
            let auth: PineconeAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "posthog" => {
            let auth: PosthogAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "weaviate" => {
            let auth: WeaviateAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "tavily" => {
            let auth: TavilyAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "llm" => {
            let auth: LlmAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "function" => {
            let auth = FunctionAuth { data: config.clone() };
            Ok(Box::new(auth))
        }
        "azure" => {
            let auth: AzureAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "gitlab" => {
            let auth: GitlabAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "google_workspace" => {
            let auth: GoogleWorkspaceAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "s3" => {
            let auth: S3Auth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        "eraser" => {
            let auth: EraserAuth = serde_json::from_value(config.clone()).map_err(EpError::serde)?;
            Ok(Box::new(auth))
        }
        _ => Err(EpError::parse(format!("unknown ELS strategy: '{strategy}'"))),
    }
}

/// Resolve from an `EpKind` and config JSON. Convenience wrapper that maps
/// the kind to its strategy string.
pub fn resolve_ep_auth_from_kind(kind: EpKind, config: &serde_json::Value) -> ResultEP<Box<dyn EpAuth>> {
    let strategy = ep_kind_to_strategy(kind)?;
    resolve_ep_auth(strategy, config)
}

/// Map an `EpKind` to its ELS strategy string. Returns an error for endpoint
/// types that do not support ELS (e.g. `Eraser`).
fn ep_kind_to_strategy(kind: EpKind) -> ResultEP<&'static str> {
    match kind {
        EpKind::Postgres => Ok("postgres"),
        EpKind::Mysql => Ok("mysql"),
        EpKind::Mssql => Ok("mssql"),
        EpKind::Oracle => Ok("oracle"),
        EpKind::Mongo => Ok("mongo"),
        EpKind::Redis => Ok("redis"),
        EpKind::Cassandra => Ok("cassandra"),
        EpKind::Clickhouse => Ok("clickhouse"),
        EpKind::Snowflake => Ok("snowflake"),
        EpKind::Aws => Ok("aws"),
        EpKind::Rds => Ok("rds"),
        EpKind::Elasticache => Ok("elasticache"),
        EpKind::Http => Ok("http"),
        EpKind::Salesforce => Ok("salesforce"),
        EpKind::Databricks => Ok("databricks"),
        EpKind::Datadog => Ok("datadog"),
        EpKind::Pinecone => Ok("pinecone"),
        EpKind::Weaviate => Ok("weaviate"),
        EpKind::Tavily => Ok("tavily"),
        EpKind::Llm => Ok("llm"),
        EpKind::Function => Ok("function"),
        EpKind::Posthog => Ok("posthog"),
        EpKind::Azure => Ok("azure"),
        EpKind::Gitlab => Ok("gitlab"),
        EpKind::GoogleWorkspace => Ok("google_workspace"),
        EpKind::S3 => Ok("s3"),
        EpKind::Eraser => Ok("eraser"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resolve_postgres_auth() {
        let config = json!({"variables": {"app.tenant_id": "t-123", "app.role": "reader"}});
        let auth = resolve_ep_auth("postgres", &config).unwrap();
        assert_eq!(auth.kind(), EpKind::Postgres);

        let pg = auth.as_any().downcast_ref::<PostgresAuth>().unwrap();
        assert_eq!(pg.variables.get("app.tenant_id").unwrap(), "t-123");
        assert_eq!(pg.variables.get("app.role").unwrap(), "reader");
    }

    #[test]
    fn resolve_postgres_legacy_compat() {
        let config = json!({"variables": {"key": "val"}});
        let auth = resolve_ep_auth("postgres_session_variables", &config).unwrap();
        assert_eq!(auth.kind(), EpKind::Postgres);
        auth.as_any().downcast_ref::<PostgresAuth>().unwrap();
    }

    #[test]
    fn resolve_mysql_auth() {
        let config = json!({"username": "admin", "password": "secret"});
        let auth = resolve_ep_auth("mysql", &config).unwrap();
        let mysql = auth.as_any().downcast_ref::<MysqlAuth>().unwrap();
        assert_eq!(mysql.username, "admin");
        assert_eq!(mysql.password.as_deref(), Some("secret"));
    }

    #[test]
    fn resolve_aws_auth() {
        let config = json!({
            "access_key_id": "AKIA...",
            "secret_access_key": "secret",
            "session_token": "tok"
        });
        let auth = resolve_ep_auth("aws", &config).unwrap();
        let aws = auth.as_any().downcast_ref::<AwsAuth>().unwrap();
        assert_eq!(aws.access_key_id, "AKIA...");
        assert_eq!(aws.session_token.as_deref(), Some("tok"));
    }

    #[test]
    fn resolve_http_auth() {
        let config = json!({"headers": {"Authorization": "Bearer xyz"}});
        let auth = resolve_ep_auth("http", &config).unwrap();
        let http = auth.as_any().downcast_ref::<HttpAuth>().unwrap();
        assert_eq!(http.headers.get("Authorization").unwrap(), "Bearer xyz");
    }

    #[test]
    fn resolve_snowflake_keypair() {
        let config = json!({"user": "MYUSER", "private_key": "-----BEGIN..."});
        let auth = resolve_ep_auth("snowflake", &config).unwrap();
        let sf = auth.as_any().downcast_ref::<SnowflakeAuth>().unwrap();
        assert_eq!(sf.user.as_deref(), Some("MYUSER"));
        assert!(sf.private_key.is_some());
        assert!(sf.oauth_token.is_none());
    }

    #[test]
    fn resolve_snowflake_oauth() {
        let config = json!({"oauth_token": "eyJ..."});
        let auth = resolve_ep_auth("snowflake", &config).unwrap();
        let sf = auth.as_any().downcast_ref::<SnowflakeAuth>().unwrap();
        assert!(sf.oauth_token.is_some());
        assert!(sf.user.is_none());
    }

    #[test]
    fn resolve_function_freeform() {
        let config = json!({"custom_key": 42, "nested": {"a": true}});
        let auth = resolve_ep_auth("function", &config).unwrap();
        let func = auth.as_any().downcast_ref::<FunctionAuth>().unwrap();
        assert_eq!(func.data, config);
    }

    #[test]
    fn resolve_unknown_strategy_errors() {
        let config = json!({});
        let result = resolve_ep_auth("unknown_db", &config);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_bad_config_errors() {
        // MySQL requires "username" string, passing a number should fail
        let config = json!({"username": 123});
        let result = resolve_ep_auth("mysql", &config);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_from_kind_all_supported() {
        let kinds = [
            (EpKind::Postgres, json!({"variables": {}})),
            (EpKind::Mysql, json!({"username": "u"})),
            (EpKind::Mssql, json!({"username": "u"})),
            (EpKind::Oracle, json!({"username": "u"})),
            (EpKind::Mongo, json!({"username": "u"})),
            (EpKind::Redis, json!({"username": "u"})),
            (EpKind::Cassandra, json!({"username": "u"})),
            (EpKind::Clickhouse, json!({"username": "u"})),
            (EpKind::Snowflake, json!({"oauth_token": "t"})),
            (EpKind::Aws, json!({"access_key_id": "a", "secret_access_key": "s"})),
            (EpKind::Rds, json!({"access_key_id": "a", "secret_access_key": "s"})),
            (EpKind::Elasticache, json!({"username": "u"})),
            (EpKind::Http, json!({"headers": {}})),
            (EpKind::Salesforce, json!({"access_token": "t"})),
            (EpKind::Databricks, json!({"token": "t"})),
            (EpKind::Datadog, json!({"api_key": "k"})),
            (EpKind::Pinecone, json!({"api_key": "k"})),
            (EpKind::Weaviate, json!({"api_key": "k"})),
            (EpKind::Tavily, json!({"api_key": "k"})),
            (EpKind::Llm, json!({"api_key": "k"})),
            (EpKind::Function, json!({"anything": true})),
            (EpKind::Posthog, json!({"api_key": "k"})),
            (EpKind::Azure, json!({"tenant_id": "t", "client_id": "c", "client_secret": "s"})),
            (EpKind::Gitlab, json!({"token": "t"})),
            (EpKind::GoogleWorkspace, json!({"client_id": "c", "client_secret": "s", "refresh_token": "r"})),
            (EpKind::S3, json!({"access_key_id": "a", "secret_access_key": "s"})),
            (EpKind::Eraser, json!({"api_key": "k"})),
        ];

        for (kind, config) in &kinds {
            let auth = resolve_ep_auth_from_kind(*kind, config).unwrap_or_else(|e| panic!("failed for {kind:?}: {e}"));
            assert_eq!(auth.kind(), *kind);
        }
    }

    #[test]
    fn resolve_from_kind_eraser() {
        let result = resolve_ep_auth_from_kind(EpKind::Eraser, &json!({"api_key": "ek-123"}));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().kind(), EpKind::Eraser);
    }

    #[test]
    fn clone_box_roundtrip() {
        let config = json!({"variables": {"k": "v"}});
        let auth = resolve_ep_auth("postgres", &config).unwrap();
        let cloned = auth.clone_box();
        let pg = cloned.as_any().downcast_ref::<PostgresAuth>().unwrap();
        assert_eq!(pg.variables.get("k").unwrap(), "v");
    }

    #[test]
    fn to_json_roundtrip() {
        let config = json!({"username": "admin", "password": "pw"});
        let auth = resolve_ep_auth("mysql", &config).unwrap();
        let json = auth.to_json().unwrap();
        let auth2 = resolve_ep_auth("mysql", &json).unwrap();
        let mysql = auth2.as_any().downcast_ref::<MysqlAuth>().unwrap();
        assert_eq!(mysql.username, "admin");
    }

    #[test]
    fn all_api_key_strategies() {
        for strategy in &["pinecone", "weaviate", "tavily"] {
            let config = json!({"api_key": "test-key"});
            let auth = resolve_ep_auth(strategy, &config).unwrap();
            let json = auth.to_json().unwrap();
            assert_eq!(json["api_key"], "test-key");
        }
    }

    // --- Auth application tests ---

    #[test]
    fn postgres_sql_prefix_basic() {
        let auth = PostgresAuth {
            variables: HashMap::from([("app.tenant_id".to_string(), "t-123".to_string())]),
        };
        let (prefix, count) = auth.sql_prefix();
        assert_eq!(count, 1);
        // Identifier is double-quoted per PostgreSQL escaping convention
        assert!(prefix.contains("SET \"app.tenant_id\" = 't-123'"));
        assert!(prefix.ends_with("; "));
    }

    #[test]
    fn postgres_sql_prefix_escapes_quotes() {
        let auth = PostgresAuth {
            variables: HashMap::from([("app.role".to_string(), "it's a test".to_string())]),
        };
        let (prefix, count) = auth.sql_prefix();
        assert_eq!(count, 1);
        assert!(prefix.contains("it''s a test"));
    }

    #[test]
    fn postgres_sql_prefix_escapes_unsafe_identifiers() {
        let auth = PostgresAuth {
            variables: HashMap::from([("app.foo; DROP TABLE--".to_string(), "val".to_string())]),
        };
        let (prefix, count) = auth.sql_prefix();
        assert_eq!(count, 1);
        // The identifier is safely double-quoted, neutralizing injection attempts.
        // Internal double quotes would be doubled; semicolons/dashes are harmless inside quotes.
        assert!(prefix.contains("\"app.foo; DROP TABLE--\""));
        // The injection payload is contained inside the quoted identifier, not executable
        assert!(!prefix.contains("SET app.foo; DROP TABLE"));
    }

    #[test]
    fn postgres_sql_prefix_strips_null_bytes() {
        let auth = PostgresAuth {
            variables: HashMap::from([("app.role\0shadow".to_string(), "va\0l".to_string())]),
        };
        let (prefix, count) = auth.sql_prefix();
        assert_eq!(count, 1);
        assert!(prefix.contains("\"app.roleshadow\""));
        assert!(prefix.contains("'val'"));
        assert!(!prefix.contains('\0'));
    }

    #[test]
    fn postgres_sql_prefix_empty_variables() {
        let auth = PostgresAuth { variables: HashMap::new() };
        let (prefix, count) = auth.sql_prefix();
        assert_eq!(count, 0);
        assert!(prefix.is_empty());
    }

    #[test]
    fn connection_credentials_trait() {
        let mysql = MysqlAuth {
            username: "admin".into(),
            password: Some("secret".into()),
            variables: HashMap::new(),
        };
        assert_eq!(ConnectionCredentials::username(&mysql), "admin");
        assert_eq!(ConnectionCredentials::password(&mysql), Some("secret"));

        let redis = RedisAuth {
            username: Some("default".into()),
            password: None,
            endpoint_uuid: None,
        };
        assert_eq!(ConnectionCredentials::username(&redis), "default");
        assert_eq!(ConnectionCredentials::password(&redis), None);
    }

    #[test]
    fn redis_auth_supports_endpoint_switching_payloads() {
        let auth = resolve_ep_auth("redis", &json!({"endpoint_uuid": "endpoint-123"})).unwrap();
        let redis = auth.as_any().downcast_ref::<RedisAuth>().unwrap();
        assert_eq!(redis.endpoint_uuid(), Some("endpoint-123"));
        assert_eq!(redis.username, None);
        assert_eq!(redis.password, None);
    }

    #[test]
    fn oracle_privilege() {
        let auth = OracleAuth {
            username: "sys".into(),
            password: Some("pw".into()),
            privilege: Some("SYSDBA".into()),
        };
        assert_eq!(auth.privilege(), Some("SYSDBA"));
        assert_eq!(ConnectionCredentials::username(&auth), "sys");
    }

    #[test]
    fn mongo_mechanism() {
        let auth = MongoAuth {
            username: "admin".into(),
            password: Some("pw".into()),
            mechanism: Some("SCRAM-SHA-256".into()),
        };
        assert_eq!(auth.mechanism(), Some("SCRAM-SHA-256"));
        assert_eq!(ConnectionCredentials::username(&auth), "admin");
    }

    #[test]
    fn snowflake_auth_mode_detection() {
        let keypair = SnowflakeAuth {
            user: Some("USER".into()),
            private_key: Some("key".into()),
            oauth_token: None,
            variables: HashMap::new(),
        };
        assert!(keypair.is_keypair());
        assert!(!keypair.is_oauth());

        let oauth = SnowflakeAuth {
            user: None,
            private_key: None,
            oauth_token: Some("tok".into()),
            variables: HashMap::new(),
        };
        assert!(!oauth.is_keypair());
        assert!(oauth.is_oauth());
    }

    #[test]
    fn header_credentials_http() {
        let auth = HttpAuth {
            headers: HashMap::from([("Authorization".into(), "Bearer xyz".into())]),
        };
        let headers = auth.auth_headers();
        assert_eq!(headers.get("Authorization").unwrap(), "Bearer xyz");
    }

    #[test]
    fn header_credentials_salesforce() {
        let auth = SalesforceAuth {
            access_token: "sf-token".into(),
            instance_url: Some("https://na1.salesforce.com".into()),
        };
        let headers = auth.auth_headers();
        assert_eq!(headers.get("Authorization").unwrap(), "Bearer sf-token");
        assert_eq!(auth.instance_url(), Some("https://na1.salesforce.com"));
    }

    #[test]
    fn header_credentials_databricks() {
        let auth = DatabricksAuth {
            token: "dbx-tok".into(),
            host: Some("workspace.cloud.databricks.com".into()),
        };
        let headers = auth.auth_headers();
        assert_eq!(headers.get("Authorization").unwrap(), "Bearer dbx-tok");
        assert_eq!(auth.host(), Some("workspace.cloud.databricks.com"));
    }

    #[test]
    fn header_credentials_datadog() {
        let auth = DatadogAuth { api_key: "dd-key".into(), app_key: Some("dd-app".into()) };
        let headers = auth.auth_headers();
        assert_eq!(headers.get("DD-API-KEY").unwrap(), "dd-key");
        assert_eq!(headers.get("DD-APPLICATION-KEY").unwrap(), "dd-app");
    }

    #[test]
    fn header_credentials_pinecone() {
        let auth = PineconeAuth { api_key: "pc-key".into() };
        let headers = auth.auth_headers();
        assert_eq!(headers.get("Api-Key").unwrap(), "pc-key");
    }

    #[test]
    fn header_credentials_weaviate() {
        let auth = WeaviateAuth { api_key: "wv-key".into() };
        let headers = auth.auth_headers();
        assert_eq!(headers.get("Authorization").unwrap(), "Bearer wv-key");
    }

    #[test]
    fn header_credentials_llm() {
        let auth = LlmAuth {
            api_key: "llm-key".into(),
            provider: Some("anthropic".into()),
        };
        let headers = auth.auth_headers();
        assert_eq!(headers.get("Authorization").unwrap(), "Bearer llm-key");
        assert_eq!(auth.provider(), Some("anthropic"));
    }

    #[test]
    fn iam_credentials_aws() {
        let auth = AwsAuth {
            access_key_id: "AKIA".into(),
            secret_access_key: "secret".into(),
            session_token: Some("tok".into()),
            role_arn: Some("arn:aws:iam::role/test".into()),
        };
        assert_eq!(IamCredentials::access_key_id(&auth), "AKIA");
        assert_eq!(IamCredentials::secret_access_key(&auth), "secret");
        assert_eq!(IamCredentials::session_token(&auth), Some("tok"));
        assert_eq!(IamCredentials::role_arn(&auth), Some("arn:aws:iam::role/test"));
    }

    #[test]
    fn iam_credentials_rds() {
        let auth = RdsAuth {
            access_key_id: "AKIA-RDS".into(),
            secret_access_key: "secret-rds".into(),
            session_token: None,
            role_arn: None,
        };
        assert_eq!(IamCredentials::access_key_id(&auth), "AKIA-RDS");
        assert_eq!(IamCredentials::session_token(&auth), None);
    }

    #[test]
    fn function_auth_data_access() {
        let data = json!({"custom": true, "nested": {"x": 1}});
        let auth = FunctionAuth { data: data.clone() };
        assert_eq!(auth.data(), &data);
    }

    // -----------------------------------------------------------------------
    // Security: Debug output must never contain secrets
    // -----------------------------------------------------------------------

    #[test]
    fn debug_redacts_postgres_auth_variables() {
        let auth = PostgresAuth {
            variables: [("app.secret".into(), "super-secret-value".into())].into(),
        };
        let debug_output = format!("{:?}", auth);
        assert!(!debug_output.contains("super-secret-value"), "Debug must not contain secret variable values");
        assert!(debug_output.contains("[REDACTED]"), "Debug must show [REDACTED]");
    }

    #[test]
    fn debug_redacts_password_fields() {
        let auth = MysqlAuth {
            username: "admin".into(),
            password: Some("hunter2".into()),
            variables: HashMap::new(),
        };
        let debug_output = format!("{:?}", auth);
        assert!(!debug_output.contains("hunter2"), "Debug must not contain password");
        assert!(debug_output.contains("[REDACTED]"), "Debug must show [REDACTED]");
    }

    #[test]
    fn debug_redacts_aws_secret_key() {
        let auth = AwsAuth {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: Some("FwoGZX...".into()),
            role_arn: None,
        };
        let debug_output = format!("{:?}", auth);
        assert!(!debug_output.contains("wJalrXUtnFEMI"), "Debug must not contain secret access key");
        assert!(!debug_output.contains("FwoGZX"), "Debug must not contain session token");
        assert!(debug_output.contains("[REDACTED]"), "Debug must show [REDACTED]");
    }

    #[test]
    fn debug_redacts_api_keys() {
        let auth = PineconeAuth { api_key: "pc-secret-key-12345".into() };
        let debug_output = format!("{:?}", auth);
        assert!(!debug_output.contains("pc-secret-key-12345"), "Debug must not contain API key");
        assert!(debug_output.contains("[REDACTED]"), "Debug must show [REDACTED]");
    }

    #[test]
    fn debug_redacts_snowflake_private_key() {
        let auth = SnowflakeAuth {
            user: Some("admin".into()),
            private_key: Some("-----BEGIN PRIVATE KEY-----\nMIIE...".into()),
            oauth_token: None,
            variables: HashMap::new(),
        };
        let debug_output = format!("{:?}", auth);
        assert!(!debug_output.contains("BEGIN PRIVATE KEY"), "Debug must not contain private key");
        assert!(debug_output.contains("[REDACTED]"), "Debug must show [REDACTED]");
    }

    // --- Session variable prefix tests ---

    #[test]
    fn mysql_sql_prefix_basic() {
        let auth = MysqlAuth {
            username: "admin".into(),
            password: None,
            variables: HashMap::from([("tenant_id".to_string(), "t-123".to_string())]),
        };
        let (prefix, count) = auth.sql_prefix();
        assert_eq!(count, 1);
        assert!(prefix.contains("SET @`tenant_id` = 't-123'"), "got: {prefix}");
        assert!(prefix.ends_with("; "));
    }

    #[test]
    fn mysql_sql_prefix_escapes_backticks() {
        let auth = MysqlAuth {
            username: "admin".into(),
            password: None,
            variables: HashMap::from([("bad`name".to_string(), "val".to_string())]),
        };
        let (prefix, count) = auth.sql_prefix();
        assert_eq!(count, 1);
        assert!(prefix.contains("@`bad``name`"), "backtick should be doubled, got: {prefix}");
    }

    #[test]
    fn mysql_sql_prefix_escapes_backslashes_and_strips_null_bytes() {
        let auth = MysqlAuth {
            username: "admin".into(),
            password: None,
            variables: HashMap::from([("tenant\0_id".to_string(), "it\\'s\0fine".to_string())]),
        };
        let (prefix, count) = auth.sql_prefix();
        assert_eq!(count, 1);
        assert!(prefix.contains("@`tenant_id`"), "null bytes should be stripped from identifiers, got: {prefix}");
        assert!(prefix.contains(r"'it\\''sfine'"), "backslashes should be escaped in MySQL literals, got: {prefix}");
        assert!(!prefix.contains('\0'));
    }

    #[test]
    fn mysql_sql_prefix_empty_variables() {
        let auth = MysqlAuth {
            username: "admin".into(),
            password: None,
            variables: HashMap::new(),
        };
        let (prefix, count) = auth.sql_prefix();
        assert_eq!(count, 0);
        assert!(prefix.is_empty());
    }

    #[test]
    fn clickhouse_sql_prefix_basic() {
        let auth = ClickhouseAuth {
            username: "admin".into(),
            password: None,
            variables: HashMap::from([("max_threads".to_string(), "4".to_string())]),
        };
        let (prefix, count) = auth.sql_prefix();
        assert_eq!(count, 1);
        assert!(prefix.contains("SET \"max_threads\" = '4'"), "got: {prefix}");
    }

    #[test]
    fn clickhouse_sql_prefix_empty_variables() {
        let auth = ClickhouseAuth {
            username: "admin".into(),
            password: None,
            variables: HashMap::new(),
        };
        let (prefix, count) = auth.sql_prefix();
        assert_eq!(count, 0);
        assert!(prefix.is_empty());
    }

    #[test]
    fn snowflake_session_prefix_basic() {
        let auth = SnowflakeAuth {
            user: None,
            private_key: None,
            oauth_token: None,
            variables: HashMap::from([("QUERY_TAG".to_string(), "my-app".to_string())]),
        };
        let (prefix, count) = auth.session_prefix();
        assert_eq!(count, 1);
        assert!(prefix.contains("ALTER SESSION SET \"QUERY_TAG\" = 'my-app'"), "got: {prefix}");
    }

    #[test]
    fn snowflake_session_prefix_escapes_quotes() {
        let auth = SnowflakeAuth {
            user: None,
            private_key: None,
            oauth_token: None,
            variables: HashMap::from([("tag".to_string(), "it's a test".to_string())]),
        };
        let (prefix, count) = auth.session_prefix();
        assert_eq!(count, 1);
        assert!(prefix.contains("it''s a test"), "single quote should be doubled, got: {prefix}");
    }

    #[test]
    fn snowflake_session_prefix_empty_variables() {
        let auth = SnowflakeAuth {
            user: Some("USER".into()),
            private_key: Some("key".into()),
            oauth_token: None,
            variables: HashMap::new(),
        };
        let (prefix, count) = auth.session_prefix();
        assert_eq!(count, 0);
        assert!(prefix.is_empty());
    }
}
