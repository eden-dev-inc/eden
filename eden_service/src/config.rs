use base64::prelude::*;
use borsh::BorshDeserialize;
use eden_core::db::DBKind;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::EdenNodeUuid;
use eden_core::request::InternalLlmSettings;
use uuid::Uuid;

/// API communication model (REST or gRPC).
pub enum ApiModel {
    Rest,
    Grpc,
}

pub trait ContainerConfig {
    fn new() -> ResultEP<Self>
    where
        Self: Sized;
    fn host(&self) -> String;
    fn port(&self) -> u16;
    fn url(&self) -> String;
}

pub trait DbConfig {
    fn new() -> ResultEP<Self>
    where
        Self: Sized;
    fn host(&self) -> String;
    fn port(&self) -> u16;
    fn username(&self) -> String;
    fn password(&self) -> String;
    fn kind(&self) -> DBKind;
    fn url(&self) -> String;
}

/// Eden service configuration loaded from environment variables.
#[derive(Debug)]
pub struct EdenAppConfig {
    host: String,
    port: u16,
    jwt_secret: Vec<u8>,
    jwt_expiry_s: u64,
    otlp_collector: String,
    otlp_db_collector: String,
    rate_limit: u64, // add a new request to quota every "rate_limit" milliseconds, e.g. 100 = allown 10 requests per second, but not more than 1 in 100 ms
    eden_node_uuid: EdenNodeUuid,
    relay_new_org_token: Option<String>,
    tools_service_timeout_secs: Option<u64>,
    internal_llm: Option<InternalLlmSettings>,
}

impl EdenAppConfig {
    pub fn jwt_secret(&self) -> &[u8] {
        &self.jwt_secret
    }

    pub fn jwt_expiry_s(&self) -> u64 {
        self.jwt_expiry_s
    }

    pub fn otlp_collector(&self) -> &str {
        &self.otlp_collector
    }

    pub fn otlp_db_collector(&self) -> &str {
        &self.otlp_db_collector
    }

    pub fn rate_limit(&self) -> u64 {
        self.rate_limit
    }

    pub fn eden_node_uuid(&self) -> &EdenNodeUuid {
        &self.eden_node_uuid
    }

    pub fn relay_new_org_token(&self) -> Option<&str> {
        self.relay_new_org_token.as_deref()
    }

    pub fn tools_service_timeout_secs(&self) -> Option<u64> {
        self.tools_service_timeout_secs
    }

    pub fn internal_llm(&self) -> Option<&InternalLlmSettings> {
        self.internal_llm.as_ref()
    }
}

impl ContainerConfig for EdenAppConfig {
    fn new() -> ResultEP<Self> {
        let svc = eden_config::services();
        let lim = eden_config::limits();
        let tel = eden_config::telemetry();

        let host = svc.eden.host.clone();
        let port = svc.eden.port;
        if port == 0 {
            return Err(EpError::database("invalid EDEN_PORT=0".to_string()));
        }

        let jwt_secret = match &svc.eden.jwt_secret {
            Some(s) => BASE64_STANDARD
                .decode(s.as_bytes())
                .map_err(|e| EpError::init(format!("JWT secret base64 decoding error: {}", e)))?
                .to_vec(),
            None => return Err(EpError::init("JWT secret not provided".to_string())),
        };

        let jwt_expiry_s = lim.jwt_expiry_secs;
        let otlp_collector = tel.otlp_collector.clone();
        let otlp_db_collector = tel.otlp_db_collector.clone();
        let rate_limit = lim.rate_limit_ms;

        let eden_node_uuid = match &svc.eden.node_uuid {
            Some(node_uuid_str) => EdenNodeUuid::from(Uuid::parse_str(node_uuid_str).map_err(EpError::database)?),
            None => EdenNodeUuid::new_uuid(),
        };

        let relay_new_org_token = svc.eden.new_org_token.clone();
        let tools_service_timeout_secs = Some(lim.tools_service_timeout_secs);

        let internal_llm = match &svc.llm.provider {
            Some(provider) if !provider.trim().is_empty() => {
                let provider = provider.trim().to_string();
                let model = svc.llm.model.as_ref().map(|m| m.trim().to_string()).filter(|m| !m.is_empty()).ok_or_else(|| {
                    EpError::init("EDEN_INTERNAL_LLM_MODEL must be provided when EDEN_INTERNAL_LLM_PROVIDER is set".to_string())
                })?;

                let api_key = svc.llm.api_key.as_ref().map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
                let base_url = svc.llm.base_url.as_ref().map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
                let system_prompt = svc.llm.system_prompt.as_ref().filter(|v| !v.trim().is_empty()).cloned();
                let temperature = svc.llm.temperature;
                let max_tokens = svc.llm.max_tokens;

                Some(InternalLlmSettings {
                    provider,
                    model,
                    api_key,
                    base_url,
                    system_prompt,
                    temperature,
                    max_tokens,
                })
            }
            _ => None,
        };

        Ok(Self {
            host,
            port,
            jwt_secret,
            jwt_expiry_s,
            otlp_collector,
            otlp_db_collector,
            rate_limit,
            eden_node_uuid,
            relay_new_org_token,
            tools_service_timeout_secs,
            internal_llm,
        })
    }
    fn host(&self) -> String {
        self.host.clone()
    }
    fn port(&self) -> u16 {
        self.port
    }
    fn url(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Engine service configuration for gRPC communication.
pub struct EngineConfig {
    host: String,
    port: u16,
    otlp_collector: String,
}

impl EngineConfig {
    pub fn otlp_collector(&self) -> String {
        self.otlp_collector.clone()
    }
}

impl ContainerConfig for EngineConfig {
    fn new() -> ResultEP<Self> {
        let svc = eden_config::services();
        let tel = eden_config::telemetry();

        let host = svc.engine.host.clone();
        let port = svc.engine.port;
        if port == 0 {
            return Err(EpError::database("invalid ENGINE_PORT=0".to_string()));
        }

        let otlp_collector = tel.engine_otlp_collector.clone();

        Ok(Self { host, port, otlp_collector })
    }
    fn host(&self) -> String {
        self.host.clone()
    }
    fn port(&self) -> u16 {
        self.port
    }
    fn url(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Redis database configuration with connection pooling and cache TTL.
#[derive(Clone, Debug)]
pub struct RedisConfig {
    host: String,
    port: u16,
    username: String,
    password: String,
    db_number: u8,
    cache_ttl: u64,
    /// `true` will offer `PSYNC` functionality at the proxy layer
    psync: bool,
    kind: DBKind,
}

impl RedisConfig {
    pub fn cache_ttl(&self) -> u64 {
        self.cache_ttl
    }
    pub fn psync(&self) -> bool {
        self.psync
    }
}

impl DbConfig for RedisConfig {
    fn new() -> ResultEP<Self> {
        let db = eden_config::databases();
        let lim = eden_config::limits();
        let feat = eden_config::features();

        let host = db.redis.host.clone();
        let port = db.redis.port;
        if port == 0 {
            return Err(EpError::database("invalid REDIS_PORT=0".to_string()));
        }

        let username = db.redis.username.clone();
        let password = db.redis.password.clone();
        let db_number = db.redis.db_number;
        let cache_ttl = lim.redis_cache_ttl_secs;
        let psync = feat.redis_psync;

        Ok(Self {
            host,
            port,
            username,
            password,
            db_number,
            cache_ttl,
            psync,
            kind: DBKind::Redis,
        })
    }
    fn host(&self) -> String {
        self.host.to_string()
    }
    fn port(&self) -> u16 {
        self.port
    }
    fn username(&self) -> String {
        self.username.to_string()
    }
    fn password(&self) -> String {
        self.password.to_string()
    }
    fn kind(&self) -> DBKind {
        self.kind.clone()
    }
    fn url(&self) -> String {
        if !self.username.is_empty() && !self.password.is_empty() {
            format!("redis://{}:{}@{}:{}/{}", self.username, self.password, self.host, self.port, self.db_number)
        } else {
            format!("redis://{}:{}/{}", self.host, self.port, self.db_number)
        }
    }
}

/// PostgreSQL database configuration with connection parameters.
#[derive(Clone, Debug, BorshDeserialize)]
pub struct PostgresConfig {
    host: String,
    port: u16,
    username: String,
    password: String,
    database_name: String,
    kind: DBKind,
}

impl DbConfig for PostgresConfig {
    fn new() -> Result<Self, EpError> {
        let db = eden_config::databases();

        let host = db.postgres.host.clone();
        let port = db.postgres.port;
        if port == 0 {
            return Err(EpError::database("invalid POSTGRES_PORT=0".to_string()));
        }

        let username = db.postgres.username.clone();
        let password = db.postgres.password.clone();
        let database_name = db.postgres.database.clone();

        Ok(Self {
            host,
            port,
            username,
            password,
            database_name,
            kind: DBKind::Postgres,
        })
    }
    fn host(&self) -> String {
        self.host.to_string()
    }
    fn port(&self) -> u16 {
        self.port
    }
    fn username(&self) -> String {
        self.username.to_string()
    }
    fn password(&self) -> String {
        self.password.to_string()
    }
    fn kind(&self) -> DBKind {
        self.kind.clone()
    }
    fn url(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.port, self.database_name
        )
    }
}

const DEFAULT_CLICKHOUSE_PORT: u16 = 8123;

/// ClickHouse database configuration for internal Eden usage.
#[derive(Clone, Debug)]
pub struct ClickhouseConfig {
    url: String,
    host: String,
    port: u16,
    username: String,
    password: String,
    database: Option<String>,
    pool_size: usize,
    kind: DBKind,
}

impl ClickhouseConfig {
    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    pub fn username_opt(&self) -> Option<&str> {
        let trimmed = self.username.trim();
        if trimmed.is_empty() { None } else { Some(trimmed) }
    }

    pub fn password_opt(&self) -> Option<&str> {
        let trimmed = self.password.trim();
        if trimmed.is_empty() { None } else { Some(trimmed) }
    }

    fn parse_host_port(url: &str) -> Result<(String, u16), EpError> {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            return Err(EpError::database("CLICKHOUSE_URL must be set".to_string()));
        }

        let without_scheme = trimmed.split("://").nth(1).unwrap_or(trimmed);
        let host_port_path = without_scheme.split('/').next().unwrap_or(without_scheme);
        let host_port = host_port_path.split('@').next_back().unwrap_or(host_port_path);

        if host_port.is_empty() {
            return Err(EpError::database(format!("invalid CLICKHOUSE_URL={}", trimmed)));
        }

        if host_port.starts_with('[') {
            let end = host_port.find(']').ok_or_else(|| EpError::database(format!("invalid CLICKHOUSE_URL={}", trimmed)))?;
            let host = host_port[1..end].to_string();
            if host.is_empty() {
                return Err(EpError::database(format!("invalid CLICKHOUSE_URL={}", trimmed)));
            }
            let rest = &host_port[end + 1..];
            let port = if let Some(port_str) = rest.strip_prefix(':') {
                if port_str.is_empty() {
                    DEFAULT_CLICKHOUSE_PORT
                } else {
                    str::parse::<u16>(port_str)
                        .map_err(|e| EpError::database(format!("invalid CLICKHOUSE_URL port {}: {}", port_str, e)))?
                }
            } else {
                DEFAULT_CLICKHOUSE_PORT
            };
            return Ok((host, port));
        }

        let mut parts = host_port.splitn(2, ':');
        let host = parts.next().unwrap_or("").trim().to_string();
        if host.is_empty() {
            return Err(EpError::database(format!("invalid CLICKHOUSE_URL={}", trimmed)));
        }
        let port = match parts.next() {
            Some(port_str) if !port_str.is_empty() => {
                str::parse::<u16>(port_str).map_err(|e| EpError::database(format!("invalid CLICKHOUSE_URL port {}: {}", port_str, e)))?
            }
            _ => DEFAULT_CLICKHOUSE_PORT,
        };

        Ok((host, port))
    }
}

impl DbConfig for ClickhouseConfig {
    fn new() -> Result<Self, EpError> {
        let db = eden_config::databases();
        let lim = eden_config::limits();

        let url = db.clickhouse.url.clone();
        let (host, port) = Self::parse_host_port(&url)?;

        let username = db.clickhouse.username.clone();
        let password = db.clickhouse.password.clone();
        let database = db.clickhouse.database.clone();
        let pool_size = lim.clickhouse_pool_size;

        Ok(Self {
            url,
            host,
            port,
            username,
            password,
            database,
            pool_size,
            kind: DBKind::Clickhouse,
        })
    }

    fn host(&self) -> String {
        self.host.clone()
    }
    fn port(&self) -> u16 {
        self.port
    }
    fn username(&self) -> String {
        self.username.clone()
    }
    fn password(&self) -> String {
        self.password.clone()
    }
    fn kind(&self) -> DBKind {
        self.kind.clone()
    }
    fn url(&self) -> String {
        self.url.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn eden_config_requires_jwt_secret() {
        eden_config::install_default_config();
        // Default config has no JWT secret, so EdenAppConfig::new() should fail.
        let result = EdenAppConfig::new();
        assert!(result.is_err(), "EdenAppConfig::new() should fail without a JWT secret");
    }

    #[test]
    #[serial]
    fn eden_config_succeeds_with_jwt_secret() {
        eden_config::install_default_config();
        let secret = BASE64_STANDARD.encode("test-secret-key");
        eden_config::update_config(|c| {
            c.services.eden.jwt_secret = Some(secret.clone());
        })
        .expect("update config");

        let cfg = EdenAppConfig::new().expect("construct eden config with JWT secret");
        assert_eq!(cfg.port(), 8000);
        assert!(!cfg.jwt_secret().is_empty());
    }

    #[test]
    #[serial]
    fn redis_psync_defaults_false() {
        eden_config::install_default_config();

        let cfg = RedisConfig::new().expect("construct redis config");
        assert!(!cfg.psync());
    }

    #[test]
    #[serial]
    fn redis_psync_parses_from_config() {
        eden_config::install_default_config();
        eden_config::update_config(|c| {
            c.features.redis_psync = true;
        })
        .expect("update config");

        let cfg = RedisConfig::new().expect("construct redis config");
        assert!(cfg.psync());
    }
}
