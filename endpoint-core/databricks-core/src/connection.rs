use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct DatabricksConnection {
    /// Databricks workspace host (e.g., "adb-1234567890123456.7.azuredatabricks.net")
    pub host: String,

    /// HTTP path for the SQL warehouse or cluster (e.g., "/sql/1.0/warehouses/abc123")
    pub http_path: String,

    /// Personal access token or OAuth token for authentication
    pub access_token: String,

    /// Default catalog to use
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,

    /// Default schema to use
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// Query timeout in seconds (default: 600)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
}

impl_connection!(DatabricksConnection, EpKind::Databricks);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct DatabricksTarget {
    /// Databricks workspace host (e.g., "adb-1234567890123456.7.azuredatabricks.net")
    pub host: String,
    /// HTTP path for the SQL warehouse or cluster
    pub http_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
}

/// Connection credentials — WHO to authenticate as.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct DatabricksCredentials {
    /// Personal access token or OAuth token for authentication
    pub access_token: String,
}

impl DatabricksConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &DatabricksTarget, creds: &DatabricksCredentials) -> Self {
        Self {
            host: target.host.clone(),
            http_path: target.http_path.clone(),
            access_token: creds.access_token.clone(),
            catalog: target.catalog.clone(),
            schema: target.schema.clone(),
            timeout: target.timeout,
        }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(DatabricksTarget, DatabricksCredentials)> {
        Ok((
            DatabricksTarget {
                host: self.host.clone(),
                http_path: self.http_path.clone(),
                catalog: self.catalog.clone(),
                schema: self.schema.clone(),
                timeout: self.timeout,
            },
            DatabricksCredentials { access_token: self.access_token.clone() },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_connection() -> DatabricksConnection {
        DatabricksConnection {
            host: "my-workspace.databricks.com".to_string(),
            http_path: "/sql/1.0/warehouses/abc123".to_string(),
            access_token: "dapi1234567890".to_string(),
            catalog: Some("main".to_string()),
            schema: Some("default".to_string()),
            timeout: Some(300),
        }
    }

    #[test]
    fn serde_roundtrip() {
        let conn = sample_connection();
        let json = serde_json::to_value(&conn).expect("Failed to serialize");
        let deserialized: DatabricksConnection = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(conn, deserialized);
    }

    #[test]
    fn borsh_roundtrip() {
        let conn = sample_connection();
        let bytes = borsh::to_vec(&conn).expect("Failed to borsh serialize");
        let deserialized: DatabricksConnection = borsh::from_slice(&bytes).expect("Failed to borsh deserialize");
        assert_eq!(conn, deserialized);
    }

    #[test]
    fn optional_fields_omitted_in_json() {
        let conn = DatabricksConnection {
            host: "host.com".to_string(),
            http_path: "/path".to_string(),
            access_token: "token".to_string(),
            catalog: None,
            schema: None,
            timeout: None,
        };
        let json = serde_json::to_string(&conn).expect("Failed to serialize");
        assert!(!json.contains("catalog"));
        assert!(!json.contains("schema"));
        assert!(!json.contains("timeout"));
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "host": "ws.databricks.com",
            "http_path": "/sql/1.0/warehouses/wh1",
            "access_token": "tok123"
        });
        let conn: DatabricksConnection = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(conn.host, "ws.databricks.com");
        assert!(conn.catalog.is_none());
        assert!(conn.timeout.is_none());
    }

    #[test]
    fn connection_kind() {
        let conn = sample_connection();
        assert_eq!(conn.kind(), EpKind::Databricks);
    }
}
