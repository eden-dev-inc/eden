use serde::{Deserialize, Deserializer, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InterlayTls {
    server_cert: String,
    server_key: String,
    client_ca_cert: Option<String>,
    #[serde(default)]
    require_client_certificate: bool,
}

impl InterlayTls {
    // Getters
    pub fn server_cert(&self) -> &String {
        &self.server_cert
    }
    pub fn server_key(&self) -> &String {
        &self.server_key
    }
    pub fn client_ca_cert(&self) -> Option<&String> {
        self.client_ca_cert.as_ref()
    }
    pub fn require_client_certificate(&self) -> bool {
        self.require_client_certificate
    }
}

pub fn deserialize_interlay_tls<'de, D>(deserializer: D) -> Result<Option<InterlayTls>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    use serde_json::Value;

    let value = Value::deserialize(deserializer)?;

    match value {
        Value::Bool(false) => Ok(None),
        Value::Bool(true) => Err(D::Error::custom("tls cannot be set to true, use an object instead")),
        Value::Null => Ok(None),
        _ => InterlayTls::deserialize(value).map(Some).map_err(D::Error::custom),
    }
}

/// Tri-state for PATCH operations: distinguishes between "field omitted"
/// (don't change), "explicit null/false" (clear), and "value provided" (set).
#[derive(Debug, Default, Clone, Serialize, ToSchema)]
pub enum PatchTls {
    /// Field was omitted from the request — no change.
    #[default]
    Absent,
    /// Caller explicitly sent `null` or `false` — clear TLS.
    Clear,
    /// Caller provided a TLS object — set TLS.
    Set(InterlayTls),
}

impl PatchTls {
    pub fn is_absent(&self) -> bool {
        matches!(self, PatchTls::Absent)
    }

    pub fn into_option(self) -> Option<Option<InterlayTls>> {
        match self {
            PatchTls::Absent => None,
            PatchTls::Clear => Some(None),
            PatchTls::Set(tls) => Some(Some(tls)),
        }
    }
}

pub fn deserialize_patch_tls<'de, D>(deserializer: D) -> Result<PatchTls, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    use serde_json::Value;

    let value = Value::deserialize(deserializer)?;

    match value {
        Value::Bool(false) | Value::Null => Ok(PatchTls::Clear),
        Value::Bool(true) => Err(D::Error::custom("tls cannot be set to true, use an object instead")),
        _ => InterlayTls::deserialize(value).map(PatchTls::Set).map_err(D::Error::custom),
    }
}
