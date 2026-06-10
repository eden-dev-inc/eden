use crate::MongoAsync;
use crate::auth::MongoAuth;
use crate::connection::{MongoConnection, MongoCredentials, MongoTarget};
use borsh::{BorshDeserialize, BorshSerialize};
use core::fmt;
use deadpool::unmanaged::Pool;
use ep_core::ep::{EpConfig, EpConnection, RWPool};
use ep_core::impl_ep_config_target_auth;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongodb::Client;
use mongodb::bson::{Document, doc};
use mongodb::options::ClientOptions;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::env;
use std::fmt::Debug;
use telemetry::{FastSpanStatus, TelemetryWrapper};
use tracing::warn;
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(title = "MongoConfig")]
pub struct MongoConfig {
    pub auth: Option<MongoAuth>, // authentication method
    pub target: MongoTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<MongoCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<MongoCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<MongoCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<MongoCredentials>,
    pub content: ContentType, // content type (json, ejson)
    pub accept: AcceptType,   // accept type (json, ejson)
    pub api_key: String,      // api-key (if applicable)
}

impl_ep_config_target_auth!(MongoConfig, MongoConnection, MongoTarget, MongoCredentials, EpKind::Mongo);

impl MongoConfig {
    pub fn accept_type(&self) -> Option<String> {
        Some(self.accept.to_string())
    }
    pub fn content_type(&self) -> Option<String> {
        Some(self.content.to_string())
    }
    pub fn headers(&self) -> reqwest::header::HeaderMap {
        use reqwest::header::HeaderValue;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Access-Control-Request-Headers", HeaderValue::from_static("*"));
        if let Ok(content_type) = format!("application/{}", self.content.as_str()).parse() {
            headers.insert("Content-Type", content_type);
        }
        if let Ok(accept) = format!("application/{}", self.accept.as_str()).parse() {
            headers.insert("Accept", accept);
        }
        if let Ok(api_key) = self.api_key.parse() {
            headers.insert("api-key", api_key);
        }
        headers
    }
}

impl fmt::Display for MongoConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "target: {:?},\nread: {:?},\nwrite: {:?},\nadmin: {:?},\nsystem: {:?},\ncontent-type: {},\naccept: {},\napi-key: {}",
            self.target,
            self.read_credentials,
            self.write_credentials,
            self.admin_credentials,
            self.system_credentials,
            self.content,
            self.accept,
            self.api_key
        )
    }
}

// ---------------------------------------------------------------------------
// Backward-compatible deserialization
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct MongoConfigRaw {
    #[serde(default)]
    target: Option<MongoTarget>,
    #[serde(default)]
    read_credentials: Option<MongoCredentials>,
    #[serde(default)]
    write_credentials: Option<MongoCredentials>,
    #[serde(default)]
    admin_credentials: Option<MongoCredentials>,
    #[serde(default)]
    system_credentials: Option<MongoCredentials>,

    #[serde(default)]
    read_conn: Option<MongoConnection>,
    #[serde(default)]
    write_conn: Option<MongoConnection>,
    #[serde(default)]
    admin_conn: Option<MongoConnection>,
    #[serde(default)]
    system_conn: Option<MongoConnection>,

    // Extra fields
    #[serde(default)]
    auth: Option<MongoAuth>,
    #[serde(default)]
    content: ContentType,
    #[serde(default)]
    accept: AcceptType,
    #[serde(default)]
    api_key: String,
}

impl<'de> Deserialize<'de> for MongoConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = MongoConfigRaw::deserialize(deserializer)?;

        let has_target = raw.target.is_some();
        let has_legacy = raw.read_conn.is_some() || raw.write_conn.is_some() || raw.admin_conn.is_some() || raw.system_conn.is_some();

        if has_target && has_legacy {
            return Err(serde::de::Error::custom(
                "Ambiguous config: provide either 'target' or legacy 'read_conn'/'write_conn' fields, not both",
            ));
        }

        if let Some(target) = raw.target {
            Ok(MongoConfig {
                auth: raw.auth,
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
                content: raw.content,
                accept: raw.accept,
                api_key: raw.api_key,
            })
        } else if has_legacy {
            let first = raw.read_conn.as_ref().or(raw.write_conn.as_ref()).or(raw.admin_conn.as_ref()).or(raw.system_conn.as_ref());
            let (target, _) = first.map(|c| c.split()).transpose().map_err(serde::de::Error::custom)?.unwrap_or_default();

            let extract = |c: &Option<MongoConnection>| c.as_ref().and_then(|c| c.split().ok().map(|(_, creds)| creds));

            Ok(MongoConfig {
                auth: raw.auth,
                target,
                read_credentials: extract(&raw.read_conn),
                write_credentials: extract(&raw.write_conn),
                admin_credentials: extract(&raw.admin_conn),
                system_credentials: extract(&raw.system_conn),
                content: raw.content,
                accept: raw.accept,
                api_key: raw.api_key,
            })
        } else {
            Ok(MongoConfig {
                auth: raw.auth,
                content: raw.content,
                accept: raw.accept,
                api_key: raw.api_key,
                ..Default::default()
            })
        }
    }
}

impl RWPool<MongoAsync> for MongoConfig {
    #[named]
    async fn conn_async(&self, connection: Box<dyn EpConnection>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<MongoAsync, EpError> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));

        let connection = match connection.as_any().downcast_ref::<MongoConnection>() {
            Some(mongo_config) => mongo_config.to_owned(),
            None => {
                let error = "failed to downcast config";
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(error.to_string()) });
                return Err(EpError::connect(error));
            }
        };

        let client_options = ClientOptions::parse_async(&connection.url).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::connect(e)
        })?;

        let options = client_options.clone();

        let mut connections = vec![];
        let profiler_level = desired_profiler_level();
        let profiler_slow_ms = desired_profiler_slow_ms();

        for _ in 0..4 {
            let client = Client::with_options(options.clone()).map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::connect(e)
            })?;

            if connections.is_empty() {
                match configure_profiler(&client, profiler_level, profiler_slow_ms).await {
                    Ok(outcome) => {
                        let message = if outcome.changed {
                            format!(
                                "enabled MongoDB profiler at level {} (previous level: {})",
                                outcome.new_level,
                                outcome.previous_level.map(|lvl| lvl.to_string()).unwrap_or_else(|| "unknown".to_string())
                            )
                        } else {
                            format!("MongoDB profiler already at level {} – keeping existing configuration", outcome.new_level)
                        };
                        span.add_simple_event(message);
                    }
                    Err(err) => {
                        warn!(target: "eden.mongo", %err, "failed to configure MongoDB profiler");
                        span.add_simple_event(format!("failed to configure MongoDB profiler: {err}"));
                    }
                }
            }

            connections.push(client)
        }

        Ok(Pool::from(connections))
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub enum AcceptType {
    #[default]
    JSON,
    EJSON,
}

impl AcceptType {
    pub fn as_str(&self) -> &str {
        match self {
            AcceptType::JSON => "json",
            AcceptType::EJSON => "ejson",
        }
    }
}

impl fmt::Display for AcceptType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                AcceptType::JSON => "json",
                AcceptType::EJSON => "ejson",
            }
        )
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub enum ContentType {
    #[default]
    JSON,
    EJSON,
}

impl ContentType {
    pub fn as_str(&self) -> &str {
        match self {
            ContentType::JSON => "json",
            ContentType::EJSON => "ejson",
        }
    }
}

const PROFILER_LEVEL_ENV: &str = "EDEN_MONGO_PROFILER_LEVEL";
const PROFILER_SLOW_MS_ENV: &str = "EDEN_MONGO_PROFILER_SLOW_MS";
const DEFAULT_PROFILER_LEVEL: i32 = 1;
const DEFAULT_PROFILER_SLOW_MS: i32 = 100;

struct ProfilerOutcome {
    previous_level: Option<i32>,
    new_level: i32,
    changed: bool,
}

async fn configure_profiler(
    client: &Client,
    desired_level: i32,
    desired_slow_ms: Option<i32>,
) -> Result<ProfilerOutcome, mongodb::error::Error> {
    let admin_db = client.database("admin");

    let mut previous_level = None;
    if let Ok(current_status) = admin_db.run_command(doc! { "profile": -1 }, None).await {
        previous_level = extract_level(&current_status);
        let current_slow_ms = extract_slow_ms(&current_status);

        let needs_slow_update = desired_slow_ms.map(|desired| current_slow_ms.map(|current| current != desired).unwrap_or(true));

        if previous_level == Some(desired_level) && !needs_slow_update.unwrap_or(false) {
            return Ok(ProfilerOutcome { previous_level, new_level: desired_level, changed: false });
        }
    }

    let mut command = doc! { "profile": desired_level };
    if let Some(slowms) = desired_slow_ms {
        command.insert("slowms", slowms);
    }

    let result = admin_db.run_command(command, None).await?;
    let now_level = extract_level(&result).unwrap_or(desired_level);

    Ok(ProfilerOutcome {
        previous_level,
        new_level: now_level,
        changed: previous_level != Some(now_level),
    })
}

fn desired_profiler_level() -> i32 {
    env::var(PROFILER_LEVEL_ENV)
        .ok()
        .and_then(|raw| raw.parse::<i32>().ok())
        .map(|level| level.clamp(0, 2))
        .unwrap_or(DEFAULT_PROFILER_LEVEL)
}

fn desired_profiler_slow_ms() -> Option<i32> {
    match env::var(PROFILER_SLOW_MS_ENV) {
        Ok(raw) => raw.parse::<i32>().ok().map(|value| value.max(0)).or(Some(DEFAULT_PROFILER_SLOW_MS)),
        Err(env::VarError::NotPresent) => Some(DEFAULT_PROFILER_SLOW_MS),
        Err(env::VarError::NotUnicode(_)) => Some(DEFAULT_PROFILER_SLOW_MS),
    }
}

fn extract_level(doc: &Document) -> Option<i32> {
    doc.get_i32("was")
        .ok()
        .or_else(|| doc.get_i32("newLevel").ok())
        .or_else(|| doc.get_i32("wasLevel").ok())
        .or_else(|| doc.get_i32("profile").ok())
}

fn extract_slow_ms(doc: &Document) -> Option<i32> {
    doc.get_i32("slowms").ok().or_else(|| doc.get_i64("slowms").ok().and_then(|value| i32::try_from(value).ok()))
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ContentType::JSON => "json",
                ContentType::EJSON => "ejson",
            }
        )
    }
}

#[cfg(test)]
mod tests {

    use super::{AcceptType, ContentType, MongoConfig};

    #[test]
    fn serde_config() {
        let mongo_config = Box::new(MongoConfig {
            auth: None,
            target: Default::default(),
            read_credentials: None,
            write_credentials: None,
            admin_credentials: None,
            system_credentials: None,
            content: ContentType::JSON,
            accept: AcceptType::JSON,
            api_key: "".to_string(),
        });

        let out = serde_json::to_string(&mongo_config).unwrap_or_default();

        println!("{out}")
    }
}
