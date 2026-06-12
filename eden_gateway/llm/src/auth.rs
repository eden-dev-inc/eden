use endpoint_core::llm_core::{LlmGatewayAuthScheme, LlmGatewayControlPlaneAuthMode, LlmGatewayCredential, LlmGatewayKeyKind};
use std::collections::BTreeSet;
use std::env;

use super::HttpRequest;

const AUTH_MODE_ENV: &str = "EDEN_LLM_GATEWAY_AUTH";
const LEGACY_REQUIRE_AUTH_ENV: &str = "EDEN_LLM_GATEWAY_REQUIRE_AUTH";
const KEY_HASHES_ENV: &str = "EDEN_LLM_GATEWAY_KEYS_SHA256";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LlmGatewayAuthMode {
    Disabled,
    Observe,
    Enforce,
}

impl LlmGatewayAuthMode {
    fn from_env() -> Self {
        match env::var(AUTH_MODE_ENV).ok().as_deref().map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("enforce" | "required" | "require" | "on" | "true") => Self::Enforce,
            Some("observe" | "audit") => Self::Observe,
            Some("disabled" | "off" | "false") => Self::Disabled,
            _ if Self::legacy_require_auth() => Self::Enforce,
            _ => Self::Disabled,
        }
    }

    fn legacy_require_auth() -> bool {
        matches!(
            env::var(LEGACY_REQUIRE_AUTH_ENV).ok().as_deref().map(str::trim).map(str::to_ascii_lowercase).as_deref(),
            Some("1" | "true" | "yes" | "on")
        )
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Observe => "observe",
            Self::Enforce => "enforce",
        }
    }
}

impl From<LlmGatewayControlPlaneAuthMode> for LlmGatewayAuthMode {
    fn from(value: LlmGatewayControlPlaneAuthMode) -> Self {
        match value {
            LlmGatewayControlPlaneAuthMode::Disabled => Self::Disabled,
            LlmGatewayControlPlaneAuthMode::Observe => Self::Observe,
            LlmGatewayControlPlaneAuthMode::Enforce => Self::Enforce,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LlmGatewayAuthPolicy {
    mode: LlmGatewayAuthMode,
    allowed_key_hashes: BTreeSet<String>,
}

impl LlmGatewayAuthPolicy {
    pub(super) fn from_env() -> Self {
        Self {
            mode: LlmGatewayAuthMode::from_env(),
            allowed_key_hashes: Self::key_hashes_from_env(),
        }
    }

    pub(super) fn from_control_plane(mode: LlmGatewayControlPlaneAuthMode, allowed_key_hashes: BTreeSet<String>) -> Self {
        Self { mode: mode.into(), allowed_key_hashes }
    }

    #[cfg(test)]
    fn new_enforcing(allowed_key_hashes: BTreeSet<String>) -> Self {
        Self { mode: LlmGatewayAuthMode::Enforce, allowed_key_hashes }
    }

    #[cfg(test)]
    fn new_observing(allowed_key_hashes: BTreeSet<String>) -> Self {
        Self { mode: LlmGatewayAuthMode::Observe, allowed_key_hashes }
    }

    pub(super) fn evaluate(&self, request: &HttpRequest) -> LlmGatewayAuthDecision {
        let auth_scheme =
            LlmGatewayAuthScheme::classify(request.header("authorization"), request.header("x-api-key"), request.header("api-key"));
        let api_key = LlmGatewayCredential::api_key_from_parts(
            request.header("authorization"),
            request.header("x-api-key"),
            request.header("api-key"),
        );
        let key_kind = api_key.and_then(LlmGatewayCredential::classify_api_key);

        if self.mode == LlmGatewayAuthMode::Disabled {
            return LlmGatewayAuthDecision::allow(self.mode, key_kind, "auth_disabled");
        }

        let Some(api_key) = api_key else {
            let reason = if auth_scheme == LlmGatewayAuthScheme::None {
                "missing_gateway_key"
            } else {
                "unsupported_gateway_key"
            };
            return self.decision_for_auth_failure(key_kind, 401, "authentication_error", reason, "missing or unsupported gateway API key");
        };

        if self.allowed_key_hashes.is_empty() {
            return self.decision_for_auth_failure(
                key_kind,
                503,
                "gateway_auth_unconfigured",
                "no_configured_gateway_keys",
                "LLM gateway auth is enabled but no gateway key hashes are configured",
            );
        }

        let key_hash = LlmGatewayCredential::hash_api_key(api_key);
        if self.allowed_key_hashes.contains(&key_hash) {
            return LlmGatewayAuthDecision::allow(self.mode, key_kind, "valid_gateway_key");
        }

        self.decision_for_auth_failure(key_kind, 401, "authentication_error", "invalid_gateway_key", "invalid gateway API key")
    }

    fn decision_for_auth_failure(
        &self,
        key_kind: Option<LlmGatewayKeyKind>,
        status: u16,
        error_type: &'static str,
        reason: &'static str,
        message: &'static str,
    ) -> LlmGatewayAuthDecision {
        match self.mode {
            LlmGatewayAuthMode::Observe => LlmGatewayAuthDecision::observe(self.mode, key_kind, reason),
            LlmGatewayAuthMode::Enforce => LlmGatewayAuthDecision::block(self.mode, key_kind, status, error_type, reason, message),
            LlmGatewayAuthMode::Disabled => LlmGatewayAuthDecision::allow(self.mode, key_kind, "auth_disabled"),
        }
    }

    fn key_hashes_from_env() -> BTreeSet<String> {
        env::var(KEY_HASHES_ENV)
            .ok()
            .into_iter()
            .flat_map(|raw| raw.split(',').map(str::trim).map(str::to_ascii_lowercase).collect::<Vec<_>>())
            .filter(|value| Self::is_sha256_hex(value))
            .collect()
    }

    fn is_sha256_hex(value: &str) -> bool {
        value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LlmGatewayAuthDecision {
    pub(super) mode: &'static str,
    pub(super) action: &'static str,
    pub(super) key_kind: Option<&'static str>,
    pub(super) reason: &'static str,
    block: Option<LlmGatewayAuthBlock>,
}

impl LlmGatewayAuthDecision {
    fn allow(mode: LlmGatewayAuthMode, key_kind: Option<LlmGatewayKeyKind>, reason: &'static str) -> Self {
        Self {
            mode: mode.as_str(),
            action: "allow",
            key_kind: key_kind.map(LlmGatewayKeyKind::as_str),
            reason,
            block: None,
        }
    }

    fn observe(mode: LlmGatewayAuthMode, key_kind: Option<LlmGatewayKeyKind>, reason: &'static str) -> Self {
        Self {
            mode: mode.as_str(),
            action: "observe",
            key_kind: key_kind.map(LlmGatewayKeyKind::as_str),
            reason,
            block: None,
        }
    }

    fn block(
        mode: LlmGatewayAuthMode,
        key_kind: Option<LlmGatewayKeyKind>,
        status: u16,
        error_type: &'static str,
        reason: &'static str,
        message: &'static str,
    ) -> Self {
        Self {
            mode: mode.as_str(),
            action: "block",
            key_kind: key_kind.map(LlmGatewayKeyKind::as_str),
            reason,
            block: Some(LlmGatewayAuthBlock { status, error_type, message }),
        }
    }

    pub(super) fn block_reason(&self) -> Option<&LlmGatewayAuthBlock> {
        self.block.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LlmGatewayAuthBlock {
    pub(super) status: u16,
    pub(super) error_type: &'static str,
    pub(super) message: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn request(headers: Vec<(&str, &str)>) -> HttpRequest {
        HttpRequest {
            method: "POST".to_string(),
            path: "/v1/chat/completions".to_string(),
            version: 1,
            headers: headers.into_iter().map(|(name, value)| (name.to_string(), value.to_string())).collect(),
            body: Bytes::new(),
        }
    }

    #[test]
    fn enforce_requires_supported_configured_key() {
        let key = "eden-gateway-test";
        let policy = LlmGatewayAuthPolicy::new_enforcing(BTreeSet::from([LlmGatewayCredential::hash_api_key(key)]));
        let decision = policy.evaluate(&request(vec![("authorization", "Bearer eden-gateway-test")]));

        assert_eq!(decision.action, "allow");
        assert_eq!(decision.reason, "valid_gateway_key");
        assert_eq!(decision.key_kind, Some("gateway"));
        assert!(decision.block_reason().is_none());
    }

    #[test]
    fn enforce_blocks_missing_and_invalid_keys() {
        let policy = LlmGatewayAuthPolicy::new_enforcing(BTreeSet::from([LlmGatewayCredential::hash_api_key("eden-gateway-test")]));

        let missing = policy.evaluate(&request(Vec::new()));
        assert_eq!(missing.action, "block");
        assert_eq!(missing.block_reason().map(|block| block.status), Some(401));
        assert_eq!(missing.reason, "missing_gateway_key");

        let invalid = policy.evaluate(&request(vec![("x-api-key", "eden-gateway-wrong")]));
        assert_eq!(invalid.action, "block");
        assert_eq!(invalid.block_reason().map(|block| block.status), Some(401));
        assert_eq!(invalid.reason, "invalid_gateway_key");
    }

    #[test]
    fn observe_records_failures_without_blocking() {
        let policy = LlmGatewayAuthPolicy::new_observing(BTreeSet::from([LlmGatewayCredential::hash_api_key("eden-gateway-test")]));
        let decision = policy.evaluate(&request(vec![("authorization", "Bearer eden-gateway-wrong")]));

        assert_eq!(decision.action, "observe");
        assert_eq!(decision.reason, "invalid_gateway_key");
        assert!(decision.block_reason().is_none());
    }
}
