use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, EndpointOperation, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, JsonTargetInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::JsonTarget,
    "Executes a JSON Target AWS API request (DynamoDB, Lambda, Kinesis, Step Functions, \
     CloudWatch Logs, CodePipeline, EMR, etc.) using X-Amz-Target header dispatch",
    ReqType::Write,
    true,
);

#[derive(Debug, Clone, Default, utoipa::ToSchema, schemars::JsonSchema, Deserialize)]
pub struct JsonTargetInput {
    pub service: String,
    /// Full X-Amz-Target value (e.g. "DynamoDB_20120810.ListTables", "Kinesis_20131202.ListStreams")
    pub target: String,
    #[serde(default)]
    pub body: Option<Value>,
    /// Content-type version: "1.0" (default for DynamoDB) or "1.1" (default for most others)
    #[serde(default)]
    pub ct_version: Option<String>,
}

impl Serialize for JsonTargetInput {
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("JsonTargetInput", 5)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("service", &self.service)?;
        state.serialize_field("target", &self.target)?;
        state.serialize_field("body", &self.body)?;
        state.serialize_field("ct_version", &self.ct_version)?;
        state.end()
    }
}

type SimpleInput = JsonTargetInput;

impl EndpointOperation for JsonTargetInput {}

#[allow(non_snake_case)]
#[ctor::ctor]
fn __register_aws_operation_for_json_target() {
    crate::serde::register_operation::<JsonTargetInput>();
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

/// Returns "1.0" for DynamoDB (which uses application/x-amz-json-1.0);
/// "1.1" for all other JSON target services.
fn default_ct_version(service: &str) -> &'static str {
    match service.to_lowercase().as_str() {
        "dynamodb" => "1.0",
        _ => "1.1",
    }
}

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;

        let ct_version = self.ct_version.as_deref().unwrap_or_else(|| default_ct_version(&self.service));

        let result = client.execute_json_target(&self.service, &self.target, self.body.as_ref(), ct_version).await?;

        span.add_event(
            "received json target result from aws",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );

        Ok(Box::new(AwsJsonOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        // AWS does not support transactions.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_adds_type_field() {
        let input = JsonTargetInput {
            service: "dynamodb".to_string(),
            target: "DynamoDB_20120810.ListTables".to_string(),
            body: Some(serde_json::json!({ "Limit": 10 })),
            ct_version: None,
        };
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "json_target");
        assert_eq!(json["service"], "dynamodb");
        assert_eq!(json["target"], "DynamoDB_20120810.ListTables");
    }

    #[test]
    fn deserialize_defaults_body_and_ct_version() {
        let json = serde_json::json!({
            "service": "kinesis",
            "target": "Kinesis_20131202.ListStreams"
        });
        let input: JsonTargetInput = serde_json::from_value(json).unwrap();
        assert_eq!(input.service, "kinesis");
        assert_eq!(input.target, "Kinesis_20131202.ListStreams");
        assert!(input.body.is_none());
        assert!(input.ct_version.is_none());
    }

    #[test]
    fn default_ct_version_dynamodb() {
        assert_eq!(default_ct_version("dynamodb"), "1.0");
        assert_eq!(default_ct_version("kinesis"), "1.1");
        assert_eq!(default_ct_version("lambda"), "1.1");
    }
}
