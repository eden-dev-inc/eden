//! # ELS (Endpoint-Level Security) — credential resolution and application.
//!
//! Called after RBAC verification to resolve a user's per-endpoint auth
//! credentials. Returns `Option<Box<dyn EpAuth>>` — `None` means no ELS
//! policy is assigned (the request proceeds with default endpoint credentials).
//!
//! ## Application strategies
//!
//! - **Postgres**: SQL `SET` prefix for session variables (existing behavior).
//! - **HTTP-family**: Header injection (Authorization, API keys).
//! - **Database endpoints**: Connection credential override — compose shared
//!   target with ELS credentials via `EpConfig::connection_with_auth`.
//!
//! Session-variable strategies continue to reuse the shared endpoint pool. A
//! one-off `connection_with_auth` bypass only happens for credential-override
//! strategies that must open a distinct authenticated connection.
//!
//! ## SQL injection
//!
//! PostgreSQL SET statements use the same `escape_sql_identifier` /
//! `escape_sql_literal` helpers as the proxy layer (defined on
//! [`PostgresAuth::sql_prefix`]).

use crate::EdenDb;
use database::db::cache::CacheFunctions;
use database::db::els::ElsCommands;
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use eden_core::auth::ParsedJwt;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::endpoint::EpKind;
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid, parse_kind_uuid};
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::ep::{ConnectionTier, EpConfig, EpConnection};
use endpoint_core::ep_core::ep_auth::{
    ClickhouseAuth, DatabricksAuth, DatadogAuth, EpAuth, HeaderCredentials, HttpAuth, LlmAuth, MysqlAuth, PineconeAuth, PostgresAuth,
    PosthogAuth, RedisAuth, SalesforceAuth, SnowflakeAuth, TavilyAuth, WeaviateAuth,
};
use endpoint_schema::endpoint::EndpointSchema;
use std::collections::HashMap;

/// Resolve the authenticated user's ELS credentials for an endpoint.
///
/// Uses the process-local ELS cache as the fast path, falling back to Postgres.
/// Returns `Ok(None)` if no ELS policy is assigned to this user for
/// this endpoint — the caller should proceed with default credentials.
pub async fn resolve_els(
    database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
    auth: &ParsedJwt,
    endpoint_cache_uuid: &EndpointCacheUuid,
) -> ResultEP<Option<Box<dyn EpAuth>>> {
    database.els_resolve_auth(endpoint_cache_uuid, auth.user_uuid()).await
}

pub async fn resolve_els_required(
    database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
    auth: &ParsedJwt,
    endpoint_cache_uuid: &EndpointCacheUuid,
) -> ResultEP<Box<dyn EpAuth>> {
    resolve_els(database, auth, endpoint_cache_uuid)
        .await?
        .ok_or_else(|| EpError::auth("ELS assignment exists but no credentials resolved".to_string()))
}

pub fn redis_els_endpoint_switch(auth: &dyn EpAuth) -> Option<&str> {
    auth.as_any().downcast_ref::<RedisAuth>().and_then(RedisAuth::endpoint_uuid)
}

pub async fn resolve_els_endpoint_switch_schema(
    db_manager: &EdenDb,
    org_cache: &OrganizationCacheUuid,
    endpoint_kind: EpKind,
    auth: &dyn EpAuth,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<Option<EndpointSchema>> {
    let Some(target_endpoint_uuid) = redis_els_endpoint_switch(auth) else {
        return Ok(None);
    };

    if endpoint_kind != EpKind::Redis {
        return Err(EpError::auth(format!("ELS endpoint switching is not supported for {:?} endpoints", endpoint_kind)));
    }

    let target_endpoint_uuid = parse_kind_uuid::<EndpointUuid>(target_endpoint_uuid)
        .map_err(|e| EpError::auth(format!("Invalid Redis ELS endpoint_uuid: {e}")))?;
    let target_cache_uuid = EndpointCacheUuid::new(Some(org_cache.clone()), target_endpoint_uuid);
    let target_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            db_manager,
            &CacheObjectType::new(Some(target_cache_uuid), None),
            telemetry_wrapper,
        )
        .await?;

    if target_schema.kind() != endpoint_kind {
        return Err(EpError::auth(format!(
            "Redis ELS endpoint switch target must also be {:?}, got {:?}",
            endpoint_kind,
            target_schema.kind()
        )));
    }

    Ok(Some(target_schema))
}

// ---------------------------------------------------------------------------
// ELS application result
// ---------------------------------------------------------------------------

/// How ELS credentials should be applied for this request.
#[derive(Debug)]
pub enum ElsApplication {
    /// Postgres session variables — prepend `SET` statements to the query.
    SessionPrefix(String),
    /// Connection credential override — use this connection instead of the
    /// default pool for this request.
    ConnectionOverride(Box<dyn EpConnection>),
    /// HTTP header injection — add/override these headers on the outgoing
    /// request.
    HeaderInjection(HashMap<String, String>),
    /// No ELS modification needed (auth type not applicable for this endpoint).
    None,
}

// ---------------------------------------------------------------------------
// Request-level ELS application
// ---------------------------------------------------------------------------

/// PostgreSQL operation types that use the simple query protocol and therefore
/// support multi-statement SQL (`SET key = 'value'; <query>`).
const PG_SIMPLE_QUERY_TYPES: &[&str] = &["SimpleQuery", "SimpleQueryReadOnly", "BatchExecute"];

/// Apply resolved ELS credentials to an endpoint request.
///
/// Dispatches to the correct injection strategy based on endpoint type:
/// - **Postgres/MySQL/ClickHouse/Snowflake**: SQL `SET` prefix for session variables.
/// - **HTTP-family**: Header injection into the request payload.
/// - **Database endpoints**: Returns `Some(EpConnection)` for the engine to
///   create a one-off connection with ELS credentials.
///
/// Returns `Ok(Some(conn))` when the engine should use a connection override,
/// `Ok(None)` when the request JSON was modified in-place (or no ELS needed).
pub fn apply_els_for_request(
    kind: EpKind,
    auth: &dyn EpAuth,
    config: &dyn EpConfig,
    tier: ConnectionTier,
    request_json: &mut serde_json::Value,
) -> ResultEP<Option<Box<dyn EpConnection>>> {
    match resolve_els_application(kind, auth, config, tier)? {
        ElsApplication::SessionPrefix(prefix) => {
            if apply_session_prefix(kind, request_json, &prefix) {
                Ok(None)
            } else {
                Err(EpError::auth(format!(
                    "Resolved ELS policy for {:?} endpoint requires session state that cannot be applied to this request type",
                    kind
                )))
            }
        }
        ElsApplication::HeaderInjection(headers) => {
            inject_els_headers(request_json, &headers);
            Ok(None)
        }
        ElsApplication::ConnectionOverride(conn) => Ok(Some(conn)),
        ElsApplication::None => Err(EpError::auth(format!(
            "Resolved ELS policy for {:?} endpoint, but the credentials could not be applied",
            kind
        ))),
    }
}

/// Apply resolved ELS credentials to a transaction payload.
///
/// Transaction requests wrap the actual per-operation payloads, so session
/// prefixes and header injection must be applied to each nested request entry
/// instead of the top-level transaction envelope.
pub fn apply_els_for_transaction(
    kind: EpKind,
    auth: &dyn EpAuth,
    config: &dyn EpConfig,
    tier: ConnectionTier,
    transaction_json: &mut serde_json::Value,
) -> ResultEP<Option<Box<dyn EpConnection>>> {
    match resolve_els_application(kind, auth, config, tier)? {
        ElsApplication::SessionPrefix(prefix) => {
            if apply_transaction_payloads(transaction_json, |request_json| apply_session_prefix(kind, request_json, &prefix)) {
                Ok(None)
            } else {
                Err(EpError::auth(format!(
                    "Resolved ELS policy for {:?} endpoint requires session state that cannot be applied to this transaction type",
                    kind
                )))
            }
        }
        ElsApplication::HeaderInjection(headers) => {
            if apply_transaction_payloads(transaction_json, |request_json| {
                inject_els_headers(request_json, &headers);
                true
            }) {
                Ok(None)
            } else {
                Err(EpError::auth(format!(
                    "Resolved ELS policy for {:?} endpoint requires header injection that cannot be applied to this transaction type",
                    kind
                )))
            }
        }
        ElsApplication::ConnectionOverride(conn) => Ok(Some(conn)),
        ElsApplication::None => Err(EpError::auth(format!(
            "Resolved ELS policy for {:?} endpoint, but the credentials could not be applied",
            kind
        ))),
    }
}

/// Prepend session variable `SET` statements to a request's SQL field.
///
/// PostgreSQL restricts prefix injection to simple-query types only (the
/// extended protocol doesn't support multi-statement). All other databases
/// apply the prefix to every query type.
fn apply_session_prefix(kind: EpKind, request_json: &mut serde_json::Value, prefix: &str) -> bool {
    // PostgreSQL: only simple-query types support multi-statement SET prefix.
    if kind == EpKind::Postgres {
        let is_simple = request_json.get("type").and_then(|v| v.as_str()).is_some_and(|t| PG_SIMPLE_QUERY_TYPES.contains(&t));
        if !is_simple {
            return false;
        }
    }

    // Each database stores the SQL in a different JSON field.
    let field = match kind {
        EpKind::Mysql => "sql",
        _ => "query", // PG, ClickHouse, Snowflake
    };

    if let Some(serde_json::Value::String(query)) = request_json.get_mut(field) {
        *query = format!("{prefix}{query}");
        return true;
    }

    false
}

/// Inject ELS headers into an HTTP-family request's JSON payload.
///
/// Merges ELS-provided headers into the request's `headers` object.
/// ELS headers take precedence over existing headers with the same key.
fn inject_els_headers(request_json: &mut serde_json::Value, headers: &HashMap<String, String>) {
    if let Some(obj) = request_json.as_object_mut() {
        let headers_val = obj.entry("headers").or_insert_with(|| serde_json::json!({}));
        if let Some(headers_obj) = headers_val.as_object_mut() {
            for (key, value) in headers {
                headers_obj.insert(key.clone(), serde_json::Value::String(value.clone()));
            }
        }
    }
}

fn apply_transaction_payloads<F>(transaction_json: &mut serde_json::Value, mut apply: F) -> bool
where
    F: FnMut(&mut serde_json::Value) -> bool,
{
    let Some(payloads) = transaction_payloads_mut(transaction_json) else {
        return false;
    };

    payloads.iter_mut().all(&mut apply)
}

fn transaction_payloads_mut(transaction_json: &mut serde_json::Value) -> Option<&mut Vec<serde_json::Value>> {
    match transaction_json {
        serde_json::Value::Object(map) => map.get_mut("data").and_then(serde_json::Value::as_array_mut),
        serde_json::Value::Array(items) => Some(items),
        _ => None,
    }
}

/// Determine how ELS credentials should be applied for a given endpoint type.
///
/// This is the expanded version of `apply_els_to_request` that handles all
/// endpoint types, not just Postgres.
pub fn resolve_els_application(kind: EpKind, auth: &dyn EpAuth, config: &dyn EpConfig, tier: ConnectionTier) -> ResultEP<ElsApplication> {
    if auth.kind() != kind {
        return Err(EpError::auth(format!(
            "Resolved ELS auth kind mismatch: endpoint is {:?}, auth is {:?}",
            kind,
            auth.kind()
        )));
    }

    match kind {
        // Postgres: session variables via SET prefix (existing behavior)
        EpKind::Postgres => {
            if let Some(pg_auth) = auth.as_any().downcast_ref::<PostgresAuth>() {
                let (prefix, count) = pg_auth.sql_prefix();
                if count > 0 {
                    return Ok(ElsApplication::SessionPrefix(prefix));
                }
            }
            // If the auth is not a session-variable payload, try connection credential override.
            if let Some(conn) = config.connection_with_auth(tier, auth) {
                return Ok(ElsApplication::ConnectionOverride(conn));
            }
            Ok(ElsApplication::None)
        }

        // HTTP-family: header injection.
        // Each endpoint kind has its own auth type (HttpAuth, SalesforceAuth, etc.)
        // that implements HeaderCredentials. Try each via downcast.
        EpKind::Http
        | EpKind::Salesforce
        | EpKind::Databricks
        | EpKind::Datadog
        | EpKind::Pinecone
        | EpKind::Posthog
        | EpKind::Weaviate
        | EpKind::Tavily
        | EpKind::Llm => {
            if let Some(headers) = extract_header_credentials(auth) {
                return Ok(ElsApplication::HeaderInjection(headers));
            }
            // Fall back to connection override via JSON round-trip
            if let Some(conn) = config.connection_with_auth(tier, auth) {
                return Ok(ElsApplication::ConnectionOverride(conn));
            }
            Ok(ElsApplication::None)
        }

        // Session-variable capable databases: prefer SET prefix, fall back to
        // connection override for credential-only policies.
        EpKind::Mysql | EpKind::Mssql | EpKind::Clickhouse | EpKind::Snowflake => {
            if let Some(prefix) = extract_session_prefix(auth) {
                return Ok(ElsApplication::SessionPrefix(prefix));
            }
            if let Some(conn) = config.connection_with_auth(tier, auth) {
                return Ok(ElsApplication::ConnectionOverride(conn));
            }
            Ok(ElsApplication::None)
        }

        // Connection-credential-only database endpoints
        EpKind::Oracle
        | EpKind::Mongo
        | EpKind::Redis
        | EpKind::Cassandra
        | EpKind::Aws
        | EpKind::Rds
        | EpKind::Elasticache
        | EpKind::Function => {
            if let Some(conn) = config.connection_with_auth(tier, auth) {
                return Ok(ElsApplication::ConnectionOverride(conn));
            }
            Ok(ElsApplication::None)
        }

        // Unsupported kinds: not part of the current ELS policy/auth matrix.
        EpKind::Eraser | EpKind::Azure | EpKind::Gitlab | EpKind::GoogleWorkspace | EpKind::S3 => {
            Err(EpError::auth(format!("ELS is not supported for {:?} endpoints", kind)))
        }
    }
}

/// Try to extract HTTP headers from an EpAuth by downcasting to each
/// HTTP-family auth type that implements `HeaderCredentials`.
fn extract_header_credentials(auth: &dyn EpAuth) -> Option<HashMap<String, String>> {
    let any = auth.as_any();

    if let Some(a) = any.downcast_ref::<HttpAuth>() {
        return Some(a.auth_headers());
    }
    if let Some(a) = any.downcast_ref::<SalesforceAuth>() {
        return Some(a.auth_headers());
    }
    if let Some(a) = any.downcast_ref::<DatabricksAuth>() {
        return Some(a.auth_headers());
    }
    if let Some(a) = any.downcast_ref::<DatadogAuth>() {
        return Some(a.auth_headers());
    }
    if let Some(a) = any.downcast_ref::<PineconeAuth>() {
        return Some(a.auth_headers());
    }
    if let Some(a) = any.downcast_ref::<PosthogAuth>() {
        return Some(a.auth_headers());
    }
    if let Some(a) = any.downcast_ref::<WeaviateAuth>() {
        return Some(a.auth_headers());
    }
    if let Some(a) = any.downcast_ref::<TavilyAuth>() {
        return Some(a.auth_headers());
    }
    if let Some(a) = any.downcast_ref::<LlmAuth>() {
        return Some(a.auth_headers());
    }
    None
}

/// Try to extract a session variable prefix from an EpAuth by downcasting
/// to each database auth type that supports session variables.
/// Returns `None` if the auth type has no variables or an empty variables map.
fn extract_session_prefix(auth: &dyn EpAuth) -> Option<String> {
    let any = auth.as_any();

    if let Some(a) = any.downcast_ref::<MysqlAuth>() {
        let (prefix, count) = a.sql_prefix();
        if count > 0 {
            return Some(prefix);
        }
    }
    if let Some(a) = any.downcast_ref::<ClickhouseAuth>() {
        let (prefix, count) = a.sql_prefix();
        if count > 0 {
            return Some(prefix);
        }
    }
    if let Some(a) = any.downcast_ref::<SnowflakeAuth>() {
        let (prefix, count) = a.session_prefix();
        if count > 0 {
            return Some(prefix);
        }
    }
    // MssqlAuth: TODO — add variables field and SET prefix when MSSQL support is implemented
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use std::any::Any;
    use std::error::Error;
    use tokio_postgres::types::{IsNull, ToSql, Type};

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestConnection {
        kind: EpKind,
        label: &'static str,
    }

    #[derive(Clone, Debug)]
    struct UnsupportedAuth {
        kind: EpKind,
    }

    impl EpAuth for UnsupportedAuth {
        fn kind(&self) -> EpKind {
            self.kind
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn clone_box(&self) -> Box<dyn EpAuth> {
            Box::new(self.clone())
        }

        fn to_json(&self) -> ResultEP<serde_json::Value> {
            Ok(serde_json::json!({ "kind": format!("{:?}", self.kind) }))
        }
    }

    impl EpConnection for TestConnection {
        fn as_connection(self: Box<Self>) -> Box<dyn EpConnection> {
            self
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn kind(&self) -> EpKind {
            self.kind
        }

        fn clone_box(&self) -> Box<dyn EpConnection> {
            Box::new(self.clone())
        }
    }

    #[derive(Clone, Debug)]
    struct TestConfig {
        kind: EpKind,
        allow_override: bool,
        label: &'static str,
    }

    impl TestConfig {
        fn new(kind: EpKind, allow_override: bool) -> Self {
            Self { kind, allow_override, label: "els-test-conn" }
        }
    }

    impl EpConfig for TestConfig {
        fn as_config(&self) -> Box<dyn EpConfig> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_mut_any(&mut self) -> &mut dyn Any {
            self
        }

        fn kind(&self) -> EpKind {
            self.kind
        }

        fn clone_box(&self) -> Box<dyn EpConfig> {
            Box::new(self.clone())
        }

        fn read_conn(&self) -> Option<Box<dyn EpConnection>> {
            None
        }

        fn write_conn(&self) -> Option<Box<dyn EpConnection>> {
            None
        }

        fn admin_conn(&self) -> Option<Box<dyn EpConnection>> {
            None
        }

        fn system_conn(&self) -> Option<Box<dyn EpConnection>> {
            None
        }

        fn update_read_conn(&mut self, _conn: Box<dyn EpConnection>) -> ResultEP<()> {
            Ok(())
        }

        fn update_write_conn(&mut self, _conn: Box<dyn EpConnection>) -> ResultEP<()> {
            Ok(())
        }

        fn update_admin_conn(&mut self, _conn: Box<dyn EpConnection>) -> ResultEP<()> {
            Ok(())
        }

        fn update_system_conn(&mut self, _conn: Box<dyn EpConnection>) -> ResultEP<()> {
            Ok(())
        }

        fn serialize(&self) -> ResultEP<serde_json::Value> {
            Ok(serde_json::json!({
                "kind": format!("{:?}", self.kind),
                "allow_override": self.allow_override,
            }))
        }

        fn connection_with_auth(&self, _tier: ConnectionTier, auth: &dyn EpAuth) -> Option<Box<dyn EpConnection>> {
            if self.allow_override && auth.kind() == self.kind {
                Some(Box::new(TestConnection { kind: self.kind, label: self.label }))
            } else {
                None
            }
        }
    }

    impl ToSql for TestConfig {
        fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            match *ty {
                Type::JSONB | Type::JSON => {
                    let json = self.serialize().map_err(|e| Box::new(e) as Box<dyn Error + Sync + Send>)?;
                    let encoded = serde_json::to_vec(&json).map_err(|e| Box::new(e) as Box<dyn Error + Sync + Send>)?;
                    out.extend_from_slice(&encoded);
                    Ok(IsNull::No)
                }
                _ => Err(format!("unsupported sql type for TestConfig: {ty}").into()),
            }
        }

        fn accepts(ty: &Type) -> bool {
            matches!(*ty, Type::JSONB | Type::JSON)
        }

        fn to_sql_checked(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            if !Self::accepts(ty) {
                return Err(format!("unsupported sql type for TestConfig: {ty}").into());
            }
            self.to_sql(ty, out)
        }
    }

    #[test]
    fn resolve_els_application_rejects_kind_mismatch() {
        let auth = MysqlAuth {
            username: "reader".to_string(),
            password: None,
            variables: HashMap::new(),
        };
        let config = TestConfig::new(EpKind::Postgres, false);

        let err = match resolve_els_application(EpKind::Postgres, &auth, &config, ConnectionTier::Read) {
            Ok(_) => panic!("kind mismatch should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("auth kind mismatch"));
    }

    #[test]
    fn apply_els_for_request_rejects_postgres_extended_protocol_session_prefix() {
        let auth = PostgresAuth {
            variables: HashMap::from([("app.role".to_string(), "reader".to_string())]),
        };
        let config = TestConfig::new(EpKind::Postgres, false);
        let mut request_json = serde_json::json!({
            "type": "Parse",
            "query": "SELECT 1",
        });

        let err = apply_els_for_request(EpKind::Postgres, &auth, &config, ConnectionTier::Read, &mut request_json)
            .expect_err("extended protocol should fail");

        assert!(err.to_string().contains("cannot be applied"));
        assert_eq!(request_json["query"], "SELECT 1");
    }

    #[test]
    fn apply_els_for_request_injects_http_headers_and_overrides_existing_values() {
        let auth = HttpAuth {
            headers: HashMap::from([
                ("Authorization".to_string(), "Bearer fresh-token".to_string()),
                ("X-Els".to_string(), "active".to_string()),
            ]),
        };
        let config = TestConfig::new(EpKind::Http, false);
        let mut request_json = serde_json::json!({
            "method": "GET",
            "headers": {
                "Authorization": "Bearer stale-token",
                "X-Existing": "keep-me",
            }
        });

        let conn = apply_els_for_request(EpKind::Http, &auth, &config, ConnectionTier::Read, &mut request_json)
            .expect("header injection should succeed");

        assert!(conn.is_none());
        assert_eq!(request_json["headers"]["Authorization"], "Bearer fresh-token");
        assert_eq!(request_json["headers"]["X-Els"], "active");
        assert_eq!(request_json["headers"]["X-Existing"], "keep-me");
    }

    #[test]
    fn apply_els_for_request_returns_connection_override_for_credential_only_auth() {
        let auth = MysqlAuth {
            username: "reader".to_string(),
            password: Some("secret".to_string()),
            variables: HashMap::new(),
        };
        let config = TestConfig::new(EpKind::Mysql, true);
        let mut request_json = serde_json::json!({
            "sql": "SELECT 1",
        });

        let conn = apply_els_for_request(EpKind::Mysql, &auth, &config, ConnectionTier::Read, &mut request_json)
            .expect("connection override should succeed")
            .expect("connection override should be returned");
        let test_conn = conn.as_any().downcast_ref::<TestConnection>().expect("expected test connection override");

        assert_eq!(test_conn.kind, EpKind::Mysql);
        assert_eq!(test_conn.label, "els-test-conn");
        assert_eq!(request_json["sql"], "SELECT 1");
    }

    #[test]
    fn apply_els_for_request_errors_when_auth_cannot_be_applied() {
        let auth = MysqlAuth {
            username: "reader".to_string(),
            password: Some("secret".to_string()),
            variables: HashMap::new(),
        };
        let config = TestConfig::new(EpKind::Mysql, false);
        let mut request_json = serde_json::json!({
            "sql": "SELECT 1",
        });

        let err = apply_els_for_request(EpKind::Mysql, &auth, &config, ConnectionTier::Read, &mut request_json)
            .expect_err("non-applicable auth should fail closed");

        assert!(err.to_string().contains("could not be applied"));
    }

    #[test]
    fn apply_els_for_request_prefixes_mysql_queries_when_variables_are_present() {
        let auth = MysqlAuth {
            username: "reader".to_string(),
            password: None,
            variables: HashMap::from([("tenant_id".to_string(), "acme".to_string())]),
        };
        let config = TestConfig::new(EpKind::Mysql, true);
        let mut request_json = serde_json::json!({
            "sql": "SELECT 1",
        });

        let conn = apply_els_for_request(EpKind::Mysql, &auth, &config, ConnectionTier::Read, &mut request_json)
            .expect("session prefix should succeed");

        assert!(conn.is_none());
        let sql = request_json["sql"].as_str().expect("sql string");
        assert!(sql.starts_with("SET @`tenant_id` = 'acme'; "));
        assert!(sql.ends_with("SELECT 1"));
    }

    #[test]
    fn apply_els_for_transaction_prefixes_nested_mysql_queries() {
        let auth = MysqlAuth {
            username: "reader".to_string(),
            password: None,
            variables: HashMap::from([("tenant_id".to_string(), "acme".to_string())]),
        };
        let config = TestConfig::new(EpKind::Mysql, true);
        let mut transaction_json = serde_json::json!({
            "kind": "Mysql",
            "data": [
                { "type": "Query", "sql": "SELECT 1" },
                { "type": "Query", "sql": "SELECT 2" }
            ]
        });

        let conn = apply_els_for_transaction(EpKind::Mysql, &auth, &config, ConnectionTier::Write, &mut transaction_json)
            .expect("transaction session prefix should succeed");

        assert!(conn.is_none());
        assert!(transaction_json["data"][0]["sql"].as_str().unwrap().starts_with("SET @`tenant_id` = 'acme'; "));
        assert!(transaction_json["data"][1]["sql"].as_str().unwrap().starts_with("SET @`tenant_id` = 'acme'; "));
    }

    #[test]
    fn apply_els_for_transaction_injects_nested_http_headers() {
        let auth = HttpAuth {
            headers: HashMap::from([("Authorization".to_string(), "Bearer fresh-token".to_string())]),
        };
        let config = TestConfig::new(EpKind::Http, false);
        let mut transaction_json = serde_json::json!({
            "kind": "Http",
            "data": [
                { "method": "GET", "headers": { "X-Test": "1" } },
                { "method": "POST" }
            ]
        });

        let conn = apply_els_for_transaction(EpKind::Http, &auth, &config, ConnectionTier::Write, &mut transaction_json)
            .expect("transaction header injection should succeed");

        assert!(conn.is_none());
        assert_eq!(transaction_json["data"][0]["headers"]["Authorization"], "Bearer fresh-token");
        assert_eq!(transaction_json["data"][1]["headers"]["Authorization"], "Bearer fresh-token");
    }

    #[test]
    fn apply_els_for_transaction_rejects_unrecognized_transaction_shape() {
        let auth = MysqlAuth {
            username: "reader".to_string(),
            password: None,
            variables: HashMap::from([("tenant_id".to_string(), "acme".to_string())]),
        };
        let config = TestConfig::new(EpKind::Mysql, true);
        let mut transaction_json = serde_json::json!({
            "kind": "Mysql",
            "request": []
        });

        let err = apply_els_for_transaction(EpKind::Mysql, &auth, &config, ConnectionTier::Write, &mut transaction_json)
            .expect_err("invalid transaction shape should fail");

        assert!(err.to_string().contains("transaction type"));
    }

    #[test]
    fn resolve_els_application_rejects_eraser_endpoints_directly() {
        let auth = UnsupportedAuth { kind: EpKind::Eraser };
        let config = TestConfig::new(EpKind::Eraser, false);

        let err = resolve_els_application(EpKind::Eraser, &auth, &config, ConnectionTier::Read).expect_err("eraser should fail");
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn resolve_els_application_rejects_unsupported_azure_endpoints() {
        let auth = UnsupportedAuth { kind: EpKind::Azure };
        let config = TestConfig::new(EpKind::Azure, true);

        let err = resolve_els_application(EpKind::Azure, &auth, &config, ConnectionTier::Read).expect_err("azure should fail");
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn redis_els_endpoint_switch_extracts_endpoint_uuid() {
        let auth = RedisAuth {
            username: None,
            password: None,
            endpoint_uuid: Some("endpoint-123".to_string()),
        };

        assert_eq!(redis_els_endpoint_switch(&auth), Some("endpoint-123"));
    }
}
