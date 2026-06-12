//! Snowflake SQL API client implementation using reqwest.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use error::EpError;
use pkcs8::DecodePrivateKey;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use rsa::RsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::signature::{SignatureEncoding, Signer};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Snowflake SQL API client.
#[derive(Debug)]
pub struct SnowflakeClient {
    client: reqwest::Client,
    base_url: String,
    account: String,
    user: String,
    private_key: Option<RsaPrivateKey>,
    oauth_token: Option<String>,
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    role: Option<String>,
    timeout: u64,
}

/// Configuration for creating a Snowflake client.
#[derive(Debug, Clone, Default)]
pub struct SnowflakeClientConfig {
    pub account: String,
    pub user: String,
    pub private_key_pem: Option<String>,
    pub oauth_token: Option<String>,
    pub warehouse: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub role: Option<String>,
    pub timeout: u64,
    pub host: Option<String>,
}

/// Request to execute a SQL statement.
#[derive(Debug, Clone, Serialize)]
pub struct StatementRequest {
    pub statement: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bindings: Option<HashMap<String, Binding>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<SessionParameters>,
}

/// Parameter binding for prepared statements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Binding {
    #[serde(rename = "type")]
    pub binding_type: String,
    pub value: String,
}

/// Session parameters for query execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionParameters {
    #[serde(rename = "BINARY_OUTPUT_FORMAT", skip_serializing_if = "Option::is_none")]
    pub binary_output_format: Option<String>,
    #[serde(rename = "DATE_OUTPUT_FORMAT", skip_serializing_if = "Option::is_none")]
    pub date_output_format: Option<String>,
    #[serde(rename = "TIME_OUTPUT_FORMAT", skip_serializing_if = "Option::is_none")]
    pub time_output_format: Option<String>,
    #[serde(rename = "TIMESTAMP_OUTPUT_FORMAT", skip_serializing_if = "Option::is_none")]
    pub timestamp_output_format: Option<String>,
    #[serde(rename = "TIMESTAMP_LTZ_OUTPUT_FORMAT", skip_serializing_if = "Option::is_none")]
    pub timestamp_ltz_output_format: Option<String>,
    #[serde(rename = "TIMESTAMP_NTZ_OUTPUT_FORMAT", skip_serializing_if = "Option::is_none")]
    pub timestamp_ntz_output_format: Option<String>,
    #[serde(rename = "TIMESTAMP_TZ_OUTPUT_FORMAT", skip_serializing_if = "Option::is_none")]
    pub timestamp_tz_output_format: Option<String>,
}

impl StatementRequest {
    /// Create a new statement request.
    pub fn new(statement: String) -> Self {
        Self {
            statement,
            warehouse: None,
            database: None,
            schema: None,
            role: None,
            timeout: None,
            bindings: None,
            parameters: None,
        }
    }

    /// Set the warehouse for this request.
    pub fn with_warehouse(mut self, warehouse: String) -> Self {
        self.warehouse = Some(warehouse);
        self
    }

    /// Set the database for this request.
    pub fn with_database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }

    /// Set the schema for this request.
    pub fn with_schema(mut self, schema: String) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the role for this request.
    pub fn with_role(mut self, role: String) -> Self {
        self.role = Some(role);
        self
    }

    /// Set the timeout for this request.
    pub fn with_timeout(mut self, timeout: u64) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Response from the Snowflake SQL API.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultSet {
    pub code: String,
    #[serde(rename = "sqlState")]
    pub sql_state: String,
    pub message: String,
    #[serde(rename = "statementHandle")]
    pub statement_handle: String,
    #[serde(rename = "createdOn")]
    pub created_on: Option<u64>,
    #[serde(rename = "resultSetMetaData")]
    pub metadata: Option<ResultSetMetadata>,
    pub data: Option<Vec<Vec<serde_json::Value>>>,
}

impl ResultSet {
    /// Check if the query was successful.
    pub fn is_success(&self) -> bool {
        self.code == "090001" || self.code == "000000"
    }
}

/// Metadata about the result set.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultSetMetadata {
    #[serde(rename = "numRows")]
    pub num_rows: u64,
    pub format: Option<String>,
    #[serde(rename = "rowType")]
    pub row_type: Vec<ColumnMetadata>,
    #[serde(rename = "partitionInfo")]
    pub partition_info: Option<Vec<PartitionInfo>>,
}

/// Metadata about a column.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
    pub nullable: Option<bool>,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
    #[serde(rename = "byteLength")]
    pub byte_length: Option<i64>,
    pub length: Option<i64>,
}

/// Information about result partitions.
#[derive(Debug, Clone, Deserialize)]
pub struct PartitionInfo {
    #[serde(rename = "rowCount")]
    pub row_count: u64,
    #[serde(rename = "compressedSize")]
    pub compressed_size: Option<u64>,
    #[serde(rename = "uncompressedSize")]
    pub uncompressed_size: Option<u64>,
}

impl SnowflakeClient {
    /// Create a new Snowflake client.
    pub fn new(config: SnowflakeClientConfig) -> Result<Self, EpError> {
        let private_key = if let Some(ref pem) = config.private_key_pem {
            Some(RsaPrivateKey::from_pkcs8_pem(pem).map_err(|e| EpError::connect(format!("Failed to parse private key: {}", e)))?)
        } else {
            None
        };

        let host = config.host.unwrap_or_else(|| format!("{}.snowflakecomputing.com", config.account));
        let base_url = format!("https://{}/api/v2/statements", host);

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(if config.timeout > 0 { config.timeout } else { 300 }))
            .build()
            .map_err(|e| EpError::connect(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            client,
            base_url,
            account: config.account,
            user: config.user,
            private_key,
            oauth_token: config.oauth_token,
            warehouse: config.warehouse,
            database: config.database,
            schema: config.schema,
            role: config.role,
            timeout: config.timeout,
        })
    }

    /// Generate a JWT token for authentication.
    fn generate_jwt(&self) -> Result<String, EpError> {
        let private_key = self.private_key.as_ref().ok_or_else(|| EpError::connect("No private key configured for JWT auth"))?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|e| EpError::connect(format!("Time error: {}", e)))?.as_secs();

        // Calculate public key fingerprint
        let public_key = private_key.to_public_key();
        let public_key_der = rsa::pkcs8::EncodePublicKey::to_public_key_der(&public_key)
            .map_err(|e| EpError::connect(format!("Failed to encode public key: {}", e)))?;
        let fingerprint = {
            let mut hasher = Sha256::new();
            hasher.update(public_key_der.as_bytes());
            BASE64.encode(hasher.finalize())
        };

        let account_upper = self.account.to_uppercase();
        let user_upper = self.user.to_uppercase();
        let qualified_username = format!("{}.{}", account_upper, user_upper);
        let issuer = format!("{}.SHA256:{}", qualified_username, fingerprint);

        // Create JWT header
        let header = serde_json::json!({
            "alg": "RS256",
            "typ": "JWT"
        });
        let header_b64 = BASE64.encode(header.to_string().as_bytes());

        // Create JWT payload
        let payload = serde_json::json!({
            "iss": issuer,
            "sub": qualified_username,
            "iat": now,
            "exp": now + 3600
        });
        let payload_b64 = BASE64.encode(payload.to_string().as_bytes());

        // Sign the JWT
        let message = format!("{}.{}", header_b64, payload_b64);
        let signing_key: SigningKey<Sha256> = SigningKey::new(private_key.clone());
        let signature = signing_key.sign(message.as_bytes());
        let signature_b64 = BASE64.encode(signature.to_bytes());

        Ok(format!("{}.{}.{}", header_b64, payload_b64, signature_b64))
    }

    /// Get authorization header value.
    fn get_auth_header(&self) -> Result<String, EpError> {
        if let Some(ref token) = self.oauth_token {
            Ok(format!("Bearer {}", token))
        } else {
            let jwt = self.generate_jwt()?;
            Ok(format!("Bearer {}", jwt))
        }
    }

    /// Execute a SQL statement request.
    pub async fn execute_request(&self, mut request: StatementRequest) -> Result<ResultSet, EpError> {
        // Apply defaults from client config
        if request.warehouse.is_none() {
            request.warehouse = self.warehouse.clone();
        }
        if request.database.is_none() {
            request.database = self.database.clone();
        }
        if request.schema.is_none() {
            request.schema = self.schema.clone();
        }
        if request.role.is_none() {
            request.role = self.role.clone();
        }
        if request.timeout.is_none() && self.timeout > 0 {
            request.timeout = Some(self.timeout);
        }

        let auth_header = self.get_auth_header()?;

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth_header).map_err(|e| EpError::request(format!("Invalid auth header: {}", e)))?,
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert("X-Snowflake-Authorization-Token-Type", HeaderValue::from_static("KEYPAIR_JWT"));

        let response = self
            .client
            .post(&self.base_url)
            .headers(headers)
            .json(&request)
            .send()
            .await
            .map_err(|e| EpError::request(format!("HTTP request failed: {}", e)))?;

        let status = response.status();
        let body = response.text().await.map_err(|e| EpError::request(format!("Failed to read response: {}", e)))?;

        if !status.is_success() {
            return Err(EpError::request(format!("Snowflake API error ({}): {}", status, body)));
        }

        let result: ResultSet =
            serde_json::from_str(&body).map_err(|e| EpError::request(format!("Failed to parse response: {} - Body: {}", e, body)))?;

        if !result.is_success() {
            return Err(EpError::request(format!("Query failed: {} - {}", result.code, result.message)));
        }

        Ok(result)
    }

    /// Execute a simple SQL query.
    pub async fn execute(&self, sql: &str) -> Result<ResultSet, EpError> {
        self.execute_request(StatementRequest::new(sql.to_string())).await
    }
}
