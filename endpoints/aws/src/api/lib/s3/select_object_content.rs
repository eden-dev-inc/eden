use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, S3SelectObjectContentInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::S3SelectObjectContent, "s3_select_object_content", ReqType::Read, true);

crate::aws_endpoint! {
    S3SelectObjectContent,
    API_INFO,
    struct {
        bucket: String,
        key: String,
        expression: String,
        expression_type: String,
        input_serialization: Value,
        output_serialization: Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/{}/{}?select&select-type=2", self.bucket, self.key);
        let body = serde_json::json!({
            "Expression": self.expression,
            "ExpressionType": self.expression_type,
            "InputSerialization": self.input_serialization,
            "OutputSerialization": self.output_serialization
        });
        let result = client.execute("s3", "POST", &path, None, Some(&body), Some("application/xml")).await?;

        span.add_event("received result from aws", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AwsJsonOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = S3SelectObjectContentInputBuilder::default()
            .bucket("b")
            .key("k")
            .expression("SELECT * FROM s3object")
            .expression_type("SQL")
            .input_serialization(serde_json::json!({}))
            .output_serialization(serde_json::json!({}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "s3_select_object_content");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "bucket": "b",
            "key": "k",
            "expression": "SELECT * FROM s3object",
            "expression_type": "SQL",
            "input_serialization": {},
            "output_serialization": {}
        });
        let _: S3SelectObjectContentInput = serde_json::from_value(json).unwrap();
    }
}
