use crate::LlmAsync;
use crate::connection::{LlmConnection, LlmCredentials, LlmTarget};
use crate::credential::{LlmCredential, ResolvedLlmConnection};

use super::comm::LlmClient;
use borsh::{BorshDeserialize, BorshSerialize};
use deadpool::unmanaged::Pool;
use ep_core::ep::{EpConfig, EpConnection, RWPool};
use ep_core::impl_ep_config_target_auth;
use error::{ConnectError, EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::{Arc, RwLock};
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;
use uuid::Uuid;

/// Default number of tool passes to allow in a single LLM turn.
///
/// Raised from 12 to 25 after browser E2E testing showed real migration and
/// multi-step workflows saturating the cap mid-run. Per-endpoint overrides
/// can still lower this when a tight budget is desired.
pub const DEFAULT_MAX_TOOL_PASSES: usize = 25;

#[derive(Debug, Clone, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(title = "LlmConfig")]
pub struct LlmConfig {
    pub target: LlmTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<LlmCredentials>,
    /// Optional model override used only for orchestration planning calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planner_model_override: Option<String>,
    /// Optional model override used only for orchestration sub-agent execution calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_agent_model_override: Option<String>,
    /// Optional model override used only for orchestration synthesis/aggregation calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synthesis_model_override: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<LlmCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<LlmCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<LlmCredentials>,
    /// Maximum tool passes to run when the model repeatedly requests tools in a single turn.
    /// Defaults to `DEFAULT_MAX_TOOL_PASSES` (25) if not specified.
    #[serde(default = "default_max_tool_passes")]
    pub max_tool_passes: usize,
    #[serde(default, skip_serializing)]
    #[borsh(skip)]
    pub credentials: HashMap<Uuid, LlmCredential>,
}

fn default_max_tool_passes() -> usize {
    DEFAULT_MAX_TOOL_PASSES
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            target: LlmTarget::default(),
            read_credentials: None,
            write_credentials: None,
            admin_credentials: None,
            system_credentials: None,
            planner_model_override: None,
            sub_agent_model_override: None,
            synthesis_model_override: None,
            max_tool_passes: default_max_tool_passes(),
            credentials: HashMap::new(),
        }
    }
}

impl_ep_config_target_auth!(LlmConfig, LlmConnection, LlmTarget, LlmCredentials, EpKind::Llm);

impl fmt::Display for LlmConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "target: {:?}, read: {:?}, write: {:?}, admin: {:?}, system: {:?}",
            self.target, self.read_credentials, self.write_credentials, self.admin_credentials, self.system_credentials
        )
    }
}

// ---------------------------------------------------------------------------
// Backward-compatible deserialization
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct LlmConfigRaw {
    #[serde(default)]
    target: Option<LlmTarget>,
    #[serde(default)]
    read_credentials: Option<LlmCredentials>,
    #[serde(default)]
    write_credentials: Option<LlmCredentials>,
    #[serde(default)]
    admin_credentials: Option<LlmCredentials>,
    #[serde(default)]
    system_credentials: Option<LlmCredentials>,
    #[serde(default)]
    planner_model_override: Option<String>,
    #[serde(default)]
    sub_agent_model_override: Option<String>,
    #[serde(default)]
    synthesis_model_override: Option<String>,

    #[serde(default)]
    read_conn: Option<LlmConnection>,
    #[serde(default)]
    write_conn: Option<LlmConnection>,
    #[serde(default)]
    admin_conn: Option<LlmConnection>,
    #[serde(default)]
    system_conn: Option<LlmConnection>,

    // Extra fields
    #[serde(default = "default_max_tool_passes")]
    max_tool_passes: usize,
    #[serde(default)]
    credentials: HashMap<Uuid, LlmCredential>,
}

impl<'de> Deserialize<'de> for LlmConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = LlmConfigRaw::deserialize(deserializer)?;

        let has_target = raw.target.is_some();
        let has_legacy = raw.read_conn.is_some() || raw.write_conn.is_some() || raw.admin_conn.is_some() || raw.system_conn.is_some();

        if has_target && has_legacy {
            return Err(serde::de::Error::custom(
                "Ambiguous config: provide either 'target' or legacy 'read_conn'/'write_conn' fields, not both",
            ));
        }

        if let Some(target) = raw.target {
            Ok(LlmConfig {
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
                planner_model_override: raw.planner_model_override,
                sub_agent_model_override: raw.sub_agent_model_override,
                synthesis_model_override: raw.synthesis_model_override,
                max_tool_passes: raw.max_tool_passes,
                credentials: raw.credentials,
            })
        } else if has_legacy {
            let first = raw.read_conn.as_ref().or(raw.write_conn.as_ref()).or(raw.admin_conn.as_ref()).or(raw.system_conn.as_ref());
            let (target, _) = first.map(|c| c.split()).transpose().map_err(serde::de::Error::custom)?.unwrap_or_default();

            let extract = |c: &Option<LlmConnection>| c.as_ref().and_then(|c| c.split().ok().map(|(_, creds)| creds));

            Ok(LlmConfig {
                target,
                read_credentials: extract(&raw.read_conn),
                write_credentials: extract(&raw.write_conn),
                admin_credentials: extract(&raw.admin_conn),
                system_credentials: extract(&raw.system_conn),
                planner_model_override: raw.planner_model_override,
                sub_agent_model_override: raw.sub_agent_model_override,
                synthesis_model_override: raw.synthesis_model_override,
                max_tool_passes: raw.max_tool_passes,
                credentials: raw.credentials,
            })
        } else {
            Ok(LlmConfig {
                planner_model_override: raw.planner_model_override,
                sub_agent_model_override: raw.sub_agent_model_override,
                synthesis_model_override: raw.synthesis_model_override,
                max_tool_passes: raw.max_tool_passes,
                credentials: raw.credentials,
                ..Default::default()
            })
        }
    }
}

impl LlmConfig {
    fn normalized_override(value: Option<&String>) -> Option<&str> {
        value.map(String::as_str).map(str::trim).filter(|value| !value.is_empty())
    }

    pub fn register_credential(&mut self, credential: LlmCredential) {
        self.credentials.insert(credential.id, credential);
    }

    /// Resolve the configured tool-pass cap, clamping to at least 1.
    pub fn resolved_max_tool_passes(&self) -> usize {
        self.max_tool_passes.max(1)
    }

    pub fn planner_model_override(&self) -> Option<&str> {
        Self::normalized_override(self.planner_model_override.as_ref())
    }

    pub fn sub_agent_model_override(&self) -> Option<&str> {
        Self::normalized_override(self.sub_agent_model_override.as_ref())
    }

    pub fn synthesis_model_override(&self) -> Option<&str> {
        Self::normalized_override(self.synthesis_model_override.as_ref())
    }

    pub fn with_credentials<I>(mut self, credentials: I) -> Self
    where
        I: IntoIterator<Item = LlmCredential>,
    {
        for credential in credentials {
            self.register_credential(credential);
        }
        self
    }

    fn resolve_connection(&self, connection: &LlmConnection) -> Result<ResolvedLlmConnection, EpError> {
        let credential = match connection.credential_id {
            Some(id) => Some(
                self.credentials.get(&id).ok_or_else(|| EpError::connect(format!("Credential `{}` not registered in LlmConfig", id)))?,
            ),
            None => None,
        };

        connection.resolve(credential)
    }
}

impl RWPool<LlmAsync> for LlmConfig {
    #[named]
    async fn conn_async(
        &self,
        connection: Box<dyn EpConnection>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Pool<LlmClient>, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));

        let connection = match connection.as_any().downcast_ref::<LlmConnection>() {
            Some(http_config) => http_config.to_owned(),
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };

        let resolved = self.resolve_connection(&connection)?;
        let shared = Arc::new(RwLock::new(resolved));

        let mut clients = vec![];
        for _ in 0..4 {
            clients.push(LlmClient::new(shared.clone(), self.resolved_max_tool_passes())?)
        }
        Ok(deadpool::unmanaged::Pool::from(clients))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::LlmConnectionDefaults;

    fn defaults(model: &str) -> LlmConnectionDefaults {
        LlmConnectionDefaults { model: model.to_string(), ..Default::default() }
    }

    #[test]
    fn update_connection_rejects_mismatched_target() {
        let original_credentials = LlmCredentials {
            inline_api_key: Some("old-key".to_string()),
            ..Default::default()
        };
        let mut config = LlmConfig {
            target: LlmTarget::OpenAI { defaults: defaults("gpt-4o") },
            read_credentials: Some(original_credentials.clone()),
            ..Default::default()
        };

        let incoming_target = LlmTarget::Anthropic { defaults: defaults("claude-3-5-sonnet") };
        let incoming_credentials = LlmCredentials {
            inline_api_key: Some("new-key".to_string()),
            ..Default::default()
        };
        let incoming = LlmConnection::from_target_and_credentials(&incoming_target, &incoming_credentials);

        let err = config.update_read_conn(Box::new(incoming)).expect_err("target mismatch should fail");

        assert!(err.to_string().contains("provider/config mismatch on connection update; targets must match"));
        assert_eq!(config.read_credentials, Some(original_credentials));
    }
}
