use serde::{Deserialize, Serialize};

/// Organization transfer configuration.
///
/// Maps to the `[org_transfer]` section in `eden.toml`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct OrgTransferConfig {
    /// Directory for organization transfer artifact storage.
    pub dir: Option<String>,
}
