use serde::{Deserialize, Serialize};

/// Configuration for ELS credential encryption-at-rest.
///
/// Controls the org-level key provider and the environment variable
/// (or KMS key reference) used to wrap per-endpoint DEKs.
///
/// ## Environment variables
///
/// ```text
/// EDEN__ENCRYPTION__ORG_KEY_ENV_VAR=EDEN_ORG_ENCRYPTION_KEY   # name of the env var holding the hex key
/// EDEN_ORG_ENCRYPTION_KEY__<ORG_UUID_NO_DASHES_UPPER>=...     # required per-org 256-bit key
/// ```
///
/// The config value tells Eden the base name used to derive each org's env var.
/// Eden then reads `{org_key_env_var}__{ORG_UUID_NO_DASHES_UPPER}` for every
/// tenant so multi-tenant deployments do not silently share one wrapping key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// Base name of the environment variable that holds each org's
    /// hex-encoded 256-bit encryption key. Default: `EDEN_ORG_ENCRYPTION_KEY`.
    ///
    /// Eden stores `{org_key_env_var}__{ORG_UUID}` (UUID uppercased with dashes
    /// removed) in `org_key_refs.key_ref` for the `env` provider.
    pub org_key_env_var: String,

    /// Whether ELS config encryption is enabled. When `false`, configs are
    /// stored as plaintext even if a key is available. Useful for debugging
    /// or gradual rollout.
    pub enabled: bool,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            org_key_env_var: "EDEN_ORG_ENCRYPTION_KEY".to_string(),
            enabled: true,
        }
    }
}
