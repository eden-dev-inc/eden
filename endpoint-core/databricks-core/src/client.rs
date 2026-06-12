use crate::connection::DatabricksConnection;
use error::EpError;
use reqwest::Client;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct DatabricksClient {
    client: Client,
    base_url: String,
    http_path: String,
    catalog: Option<String>,
    schema: Option<String>,
    timeout: u64,
}

/// A named parameter for parameterized SQL statements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementParameter {
    pub name: String,
    pub value: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub param_type: Option<String>,
}

/// Request body for the Databricks SQL Statement Execution API.
#[derive(Debug, Clone, Serialize)]
pub struct StatementRequest {
    pub statement: String,
    pub warehouse_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Vec<StatementParameter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_timeout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disposition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

/// Response from the Databricks SQL Statement Execution API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementResponse {
    pub statement_id: Option<String>,
    pub status: StatementStatus,
    pub manifest: Option<StatementManifest>,
    pub result: Option<StatementResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementStatus {
    pub state: String,
    pub error: Option<StatementError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementError {
    pub error_code: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementManifest {
    pub format: Option<String>,
    pub schema: Option<ManifestSchema>,
    pub total_row_count: Option<u64>,
    pub total_chunk_count: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSchema {
    pub columns: Option<Vec<ColumnInfo>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type_name")]
    pub type_name: Option<String>,
    pub position: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementResult {
    pub data_array: Option<Vec<Vec<Value>>>,
    pub row_count: Option<u64>,
}

impl StatementResponse {
    pub fn is_success(&self) -> bool {
        self.status.state == "SUCCEEDED"
    }

    pub fn is_running(&self) -> bool {
        matches!(self.status.state.as_str(), "PENDING" | "RUNNING")
    }
}

/// Response from the Databricks SQL Warehouses API (`GET /api/2.0/sql/warehouses/{id}`).
/// Used by metadata sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehouseInfo {
    pub id: String,
    pub name: String,
    pub state: String,
    #[serde(default)]
    pub cluster_size: Option<String>,
    #[serde(default)]
    pub min_num_clusters: Option<u32>,
    #[serde(default)]
    pub max_num_clusters: Option<u32>,
    #[serde(default)]
    pub num_clusters: Option<u32>,
    #[serde(default)]
    pub num_active_sessions: Option<u64>,
    #[serde(default)]
    pub auto_stop_mins: Option<u32>,
    #[serde(default)]
    pub warehouse_type: Option<String>,
    #[serde(default)]
    pub enable_serverless_compute: Option<bool>,
}

/// Extract the warehouse ID from an HTTP path like "/sql/1.0/warehouses/abc123"
fn extract_warehouse_id(http_path: &str) -> String {
    http_path.rsplit('/').next().unwrap_or(http_path).to_string()
}

async fn parse_json_response(resp: reqwest::Response, operation: &str, url: &str) -> Result<Value, EpError> {
    let status = resp.status();
    let body = resp.bytes().await.map_err(EpError::request)?;
    if !status.is_success() {
        let body_text = String::from_utf8_lossy(&body);
        return Err(EpError::request(format!("{operation} request to {url} failed with status {status}: {body_text}")));
    }

    serde_json::from_slice(&body).map_err(|e| EpError::request(format!("invalid JSON in {operation} response from {url}: {e}")))
}

impl DatabricksClient {
    pub async fn new(conn: &DatabricksConnection) -> Result<Self, EpError> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", conn.access_token))
                .map_err(|_| EpError::connect("invalid access token header value"))?,
        );
        default_headers.insert(reqwest::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let timeout = conn.timeout.unwrap_or(600);
        let client = Client::builder()
            .default_headers(default_headers)
            .timeout(std::time::Duration::from_secs(timeout))
            .build()
            .map_err(EpError::connect)?;

        let host = conn.host.trim_end_matches('/');
        let base_url = if host.starts_with("http://") || host.starts_with("https://") {
            host.to_string()
        } else {
            format!("https://{}", host)
        };

        Ok(Self {
            client,
            base_url,
            http_path: conn.http_path.clone(),
            catalog: conn.catalog.clone(),
            schema: conn.schema.clone(),
            timeout,
        })
    }

    pub fn warehouse_id(&self) -> String {
        extract_warehouse_id(&self.http_path)
    }

    pub async fn get_warehouse_info(&self) -> Result<WarehouseInfo, EpError> {
        let wh_id = self.warehouse_id();
        let path = format!("/api/2.0/sql/warehouses/{}", wh_id);
        let value = self.get(&path).await?;
        serde_json::from_value(value).map_err(|e| EpError::request(format!("Failed to parse warehouse info response: {e}")))
    }

    pub async fn health_check(&self) -> Result<(), EpError> {
        let result = self.execute_statement("SELECT 1").await?;
        if result.is_success() {
            Ok(())
        } else {
            let msg = result.status.error.as_ref().and_then(|e| e.message.as_deref()).unwrap_or("unknown error");
            Err(EpError::request(format!("Databricks health check failed: {}", msg)))
        }
    }

    pub async fn execute_statement(&self, sql: &str) -> Result<StatementResponse, EpError> {
        self.execute_statement_with_params(sql, None, None).await
    }

    pub async fn execute_statement_with_params(
        &self,
        sql: &str,
        catalog: Option<&str>,
        schema: Option<&str>,
    ) -> Result<StatementResponse, EpError> {
        self.execute_statement_full(sql, catalog, schema, None, None).await
    }

    pub async fn execute_statement_full(
        &self,
        sql: &str,
        catalog: Option<&str>,
        schema: Option<&str>,
        parameters: Option<Vec<StatementParameter>>,
        row_limit: Option<u64>,
    ) -> Result<StatementResponse, EpError> {
        let warehouse_id = extract_warehouse_id(&self.http_path);
        let url = format!("{}/api/2.0/sql/statements/", self.base_url);

        let request = StatementRequest {
            statement: sql.to_string(),
            warehouse_id,
            catalog: catalog.map(String::from).or_else(|| self.catalog.clone()),
            schema: schema.map(String::from).or_else(|| self.schema.clone()),
            parameters,
            row_limit,
            wait_timeout: Some(format!("{}s", self.timeout)),
            disposition: Some("INLINE".to_string()),
            format: Some("JSON_ARRAY".to_string()),
        };

        let resp = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| EpError::request(format!("Databricks SQL API request failed: {}", e)))?;

        let status = resp.status();
        let body = resp.bytes().await.map_err(EpError::request)?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&body);
            return Err(EpError::request(format!("Databricks SQL API error ({}): {}", status, body_text)));
        }

        let result: StatementResponse =
            serde_json::from_slice(&body).map_err(|e| EpError::request(format!("Failed to parse Databricks response: {}", e)))?;

        if let Some(ref error) = result.status.error {
            let code = error.error_code.as_deref().unwrap_or("UNKNOWN");
            let msg = error.message.as_deref().unwrap_or("unknown error");
            return Err(EpError::request(format!("Databricks query failed [{}]: {}", code, msg)));
        }

        Ok(result)
    }

    pub async fn get(&self, path: &str) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self.client.get(&url).send().await.map_err(EpError::request)?;
        parse_json_response(resp, "GET", &url).await
    }

    pub async fn post(&self, path: &str, body: Option<Value>) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.post(&url);
        if let Some(body) = body {
            req = req.json(&body);
        }
        let resp = req.send().await.map_err(EpError::request)?;
        parse_json_response(resp, "POST", &url).await
    }

    pub async fn delete(&self, path: &str) -> Result<Value, EpError> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self.client.delete(&url).send().await.map_err(EpError::request)?;
        parse_json_response(resp, "DELETE", &url).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_warehouse_id_from_path() {
        assert_eq!(extract_warehouse_id("/sql/1.0/warehouses/abc123"), "abc123");
    }

    #[test]
    fn extract_warehouse_id_simple() {
        assert_eq!(extract_warehouse_id("warehouse-id-456"), "warehouse-id-456");
    }

    #[test]
    fn extract_warehouse_id_trailing_slash_handled() {
        assert_eq!(extract_warehouse_id("/sql/1.0/warehouses/abc123/"), "");
    }

    #[test]
    fn statement_request_serialization() {
        let req = StatementRequest {
            statement: "SELECT 1".to_string(),
            warehouse_id: "wh123".to_string(),
            catalog: Some("main".to_string()),
            schema: None,
            parameters: None,
            row_limit: None,
            wait_timeout: Some("60s".to_string()),
            disposition: Some("INLINE".to_string()),
            format: Some("JSON_ARRAY".to_string()),
        };

        let json = serde_json::to_value(&req).expect("Failed to serialize");
        assert_eq!(json["statement"], "SELECT 1");
        assert_eq!(json["warehouse_id"], "wh123");
        assert_eq!(json["catalog"], "main");
        assert!(json.get("schema").is_none());
        assert!(json.get("parameters").is_none());
        assert!(json.get("row_limit").is_none());
        assert_eq!(json["wait_timeout"], "60s");
    }

    #[test]
    fn statement_request_with_parameters() {
        let req = StatementRequest {
            statement: "SELECT * FROM t WHERE id = :id".to_string(),
            warehouse_id: "wh1".to_string(),
            catalog: None,
            schema: None,
            parameters: Some(vec![StatementParameter {
                name: "id".to_string(),
                value: "42".to_string(),
                param_type: Some("INT".to_string()),
            }]),
            row_limit: Some(100),
            wait_timeout: None,
            disposition: None,
            format: None,
        };

        let json = serde_json::to_value(&req).expect("Failed to serialize");
        assert_eq!(json["parameters"][0]["name"], "id");
        assert_eq!(json["parameters"][0]["value"], "42");
        assert_eq!(json["parameters"][0]["type"], "INT");
        assert_eq!(json["row_limit"], 100);
    }

    #[test]
    fn statement_response_is_running() {
        let resp = StatementResponse {
            statement_id: Some("s1".to_string()),
            status: StatementStatus { state: "RUNNING".to_string(), error: None },
            manifest: None,
            result: None,
        };
        assert!(resp.is_running());
        assert!(!resp.is_success());

        let pending = StatementResponse {
            statement_id: Some("s2".to_string()),
            status: StatementStatus { state: "PENDING".to_string(), error: None },
            manifest: None,
            result: None,
        };
        assert!(pending.is_running());
    }

    #[test]
    fn statement_response_deserialization() {
        let json = serde_json::json!({
            "statement_id": "stmt-abc",
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "format": "JSON_ARRAY",
                "schema": {
                    "columns": [
                        {"name": "id", "type_name": "INT", "position": 0},
                        {"name": "name", "type_name": "STRING", "position": 1}
                    ]
                },
                "total_row_count": 2,
                "total_chunk_count": 1
            },
            "result": {
                "data_array": [[1, "alice"], [2, "bob"]],
                "row_count": 2
            }
        });

        let resp: StatementResponse = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(resp.statement_id, Some("stmt-abc".to_string()));
        assert!(resp.is_success());
        assert_eq!(resp.manifest.as_ref().unwrap().total_row_count, Some(2));
        let columns = resp.manifest.as_ref().unwrap().schema.as_ref().unwrap().columns.as_ref().unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].name, "name");
        assert_eq!(resp.result.as_ref().unwrap().row_count, Some(2));
    }

    #[test]
    fn statement_response_failed_state() {
        let json = serde_json::json!({
            "statement_id": "stmt-fail",
            "status": {
                "state": "FAILED",
                "error": { "error_code": "SYNTAX_ERROR", "message": "Invalid SQL syntax" }
            }
        });

        let resp: StatementResponse = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(!resp.is_success());
        let error = resp.status.error.unwrap();
        assert_eq!(error.error_code, Some("SYNTAX_ERROR".to_string()));
        assert_eq!(error.message, Some("Invalid SQL syntax".to_string()));
    }

    #[test]
    fn statement_response_minimal() {
        let json = serde_json::json!({ "status": { "state": "SUCCEEDED" } });
        let resp: StatementResponse = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(resp.is_success());
        assert!(resp.statement_id.is_none());
        assert!(resp.manifest.is_none());
        assert!(resp.result.is_none());
    }

    #[tokio::test]
    async fn client_new_adds_https_prefix() {
        let conn = DatabricksConnection {
            host: "my-workspace.databricks.com".to_string(),
            http_path: "/sql/1.0/warehouses/wh1".to_string(),
            access_token: "test-token".to_string(),
            catalog: Some("main".to_string()),
            schema: None,
            timeout: Some(30),
        };

        let client = DatabricksClient::new(&conn).await.expect("Failed to create client");
        assert_eq!(client.base_url, "https://my-workspace.databricks.com");
        assert_eq!(client.catalog, Some("main".to_string()));
        assert!(client.schema.is_none());
        assert_eq!(client.timeout, 30);
    }

    #[tokio::test]
    async fn client_new_preserves_explicit_scheme() {
        let conn = DatabricksConnection {
            host: "https://custom.databricks.net".to_string(),
            http_path: "/sql/1.0/warehouses/wh2".to_string(),
            access_token: "tok".to_string(),
            catalog: None,
            schema: None,
            timeout: None,
        };

        let client = DatabricksClient::new(&conn).await.expect("Failed to create client");
        assert_eq!(client.base_url, "https://custom.databricks.net");
        assert_eq!(client.timeout, 600);
    }

    #[tokio::test]
    async fn client_warehouse_id() {
        let conn = DatabricksConnection {
            host: "ws.databricks.com".to_string(),
            http_path: "/sql/1.0/warehouses/my-wh-id".to_string(),
            access_token: "tok".to_string(),
            catalog: None,
            schema: None,
            timeout: None,
        };
        let client = DatabricksClient::new(&conn).await.expect("Failed to create client");
        assert_eq!(client.warehouse_id(), "my-wh-id");
    }

    #[test]
    fn warehouse_info_deserialization_full() {
        let json = serde_json::json!({
            "id": "abc123", "name": "My Warehouse", "state": "RUNNING",
            "cluster_size": "Medium", "min_num_clusters": 1, "max_num_clusters": 4,
            "num_clusters": 2, "num_active_sessions": 5, "auto_stop_mins": 30,
            "warehouse_type": "PRO", "enable_serverless_compute": true
        });
        let info: WarehouseInfo = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(info.id, "abc123");
        assert_eq!(info.state, "RUNNING");
        assert_eq!(info.cluster_size, Some("Medium".to_string()));
        assert_eq!(info.num_clusters, Some(2));
        assert_eq!(info.warehouse_type, Some("PRO".to_string()));
    }

    #[test]
    fn warehouse_info_deserialization_minimal() {
        let json = serde_json::json!({"id": "wh-min", "name": "Basic", "state": "STOPPED"});
        let info: WarehouseInfo = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(info.id, "wh-min");
        assert_eq!(info.state, "STOPPED");
        assert!(info.cluster_size.is_none());
    }

    #[test]
    fn statement_parameter_serde() {
        let param = StatementParameter {
            name: "user_id".to_string(),
            value: "123".to_string(),
            param_type: Some("INT".to_string()),
        };
        let json = serde_json::to_value(&param).expect("Failed to serialize");
        assert_eq!(json["name"], "user_id");
        assert_eq!(json["type"], "INT");

        let no_type = StatementParameter {
            name: "name".to_string(),
            value: "alice".to_string(),
            param_type: None,
        };
        let json = serde_json::to_value(&no_type).expect("Failed to serialize");
        assert!(json.get("type").is_none());
    }
}
