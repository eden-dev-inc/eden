use aws_config::BehaviorVersion;
use aws_sdk_lambda::Client;
use aws_sdk_lambda::config::{Credentials, Region};
use aws_sdk_lambda::operation::RequestId;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{InvocationType, LogType};
use error::EpError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::connection::{FunctionConnection, FunctionProvider};

/// Invocation behavior for AWS Lambda.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FunctionInvocationType {
    RequestResponse,
    Event,
    DryRun,
}

/// Log response behavior for AWS Lambda.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FunctionLogType {
    None,
    Tail,
}

/// Provider-agnostic function invoke request.
#[derive(Debug, Clone, Default, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FunctionInvokeRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qualifier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_context_base64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_type: Option<FunctionInvocationType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_type: Option<FunctionLogType>,
}

/// Raw provider response from a function invoke.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionInvokeResponse {
    pub provider: FunctionProvider,
    pub function_name: String,
    pub status_code: i32,
    pub executed_version: Option<String>,
    pub function_error: Option<String>,
    pub log_result_base64: Option<String>,
    pub request_id: Option<String>,
    pub payload: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct FunctionClient {
    client: Client,
    provider: FunctionProvider,
    default_function_name: Option<String>,
}

impl FunctionClient {
    pub async fn new(connection: &FunctionConnection) -> Result<Self, EpError> {
        let region = normalize_optional_string(Some(connection.region.as_str()))
            .ok_or_else(|| EpError::connect("function connection region cannot be empty"))?;

        let mut config_loader = aws_config::defaults(BehaviorVersion::latest()).region(Region::new(region));

        let access_key_id = normalize_optional_string(connection.access_key_id.as_deref());
        let secret_access_key = normalize_optional_string(connection.secret_access_key.as_deref());

        if access_key_id.is_some() ^ secret_access_key.is_some() {
            return Err(EpError::connect(
                "both `access_key_id` and `secret_access_key` must be provided together for function connections",
            ));
        }

        if let (Some(access_key_id), Some(secret_access_key)) = (access_key_id, secret_access_key) {
            let session_token = normalize_optional_string(connection.session_token.as_deref());
            let credentials = Credentials::new(access_key_id, secret_access_key, session_token, None, "eden-function");
            config_loader = config_loader.credentials_provider(credentials);
        }

        let shared_config = config_loader.load().await;

        let mut lambda_config_builder = aws_sdk_lambda::config::Builder::from(&shared_config);
        if let Some(endpoint_url) = normalize_optional_string(connection.endpoint_url.as_deref()) {
            lambda_config_builder = lambda_config_builder.endpoint_url(endpoint_url);
        }

        Ok(Self {
            client: Client::from_conf(lambda_config_builder.build()),
            provider: connection.provider,
            default_function_name: normalize_optional_string(connection.default_function_name.as_deref()),
        })
    }

    pub fn provider(&self) -> FunctionProvider {
        self.provider
    }

    pub async fn invoke(&self, request: &FunctionInvokeRequest) -> Result<FunctionInvokeResponse, EpError> {
        let function_name = self.resolve_function_name(request.function_name.as_deref())?;

        let mut operation = self.client.invoke().function_name(function_name.clone());

        if let Some(payload) = request.payload.as_ref() {
            let payload_bytes = serde_json::to_vec(payload).map_err(EpError::serde)?;
            operation = operation.payload(Blob::new(payload_bytes));
        }

        if let Some(qualifier) = normalize_optional_string(request.qualifier.as_deref()) {
            operation = operation.qualifier(qualifier);
        }

        if let Some(client_context_base64) = normalize_optional_string(request.client_context_base64.as_deref()) {
            operation = operation.client_context(client_context_base64);
        }

        if let Some(invocation_type) = request.invocation_type {
            operation = operation.invocation_type(to_aws_invocation_type(invocation_type));
        }

        if let Some(log_type) = request.log_type {
            operation = operation.log_type(to_aws_log_type(log_type));
        }

        let response = operation
            .send()
            .await
            .map_err(|e| EpError::request(format!("failed to invoke AWS Lambda function `{function_name}`: {e}")))?;

        let status_code = response.status_code();

        Ok(FunctionInvokeResponse {
            provider: self.provider,
            function_name,
            status_code,
            executed_version: response.executed_version().map(|value| value.to_string()),
            function_error: response.function_error().map(|value| value.to_string()),
            log_result_base64: response.log_result().map(|value| value.to_string()),
            request_id: response.request_id().map(|value| value.to_string()),
            payload: response.payload().map(|value| value.as_ref().to_vec()),
        })
    }

    pub async fn health_check(&self) -> Result<(), EpError> {
        let Some(default_function_name) = self.default_function_name.as_deref() else {
            return Ok(());
        };

        self.client
            .invoke()
            .function_name(default_function_name)
            .invocation_type(InvocationType::DryRun)
            .send()
            .await
            .map_err(|e| EpError::request(format!("function health check failed for `{default_function_name}`: {e}")))?;

        Ok(())
    }

    fn resolve_function_name(&self, requested_function_name: Option<&str>) -> Result<String, EpError> {
        normalize_optional_string(requested_function_name)
            .or_else(|| self.default_function_name.clone())
            .ok_or_else(|| EpError::request("function invoke requires `function_name` in request or `default_function_name` in connection"))
    }
}

fn to_aws_invocation_type(value: FunctionInvocationType) -> InvocationType {
    match value {
        FunctionInvocationType::RequestResponse => InvocationType::RequestResponse,
        FunctionInvocationType::Event => InvocationType::Event,
        FunctionInvocationType::DryRun => InvocationType::DryRun,
    }
}

fn to_aws_log_type(value: FunctionLogType) -> LogType {
    match value {
        FunctionLogType::None => LogType::None,
        FunctionLogType::Tail => LogType::Tail,
    }
}

fn normalize_optional_string(value: Option<&str>) -> Option<String> {
    value.map(str::trim).filter(|candidate| !candidate.is_empty()).map(ToOwned::to_owned)
}
