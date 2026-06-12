use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, SyntheticsCreateCanaryInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SyntheticsCreateCanary, "synthetics_create_canary", ReqType::Write, true);

crate::aws_endpoint! {
    SyntheticsCreateCanary,
    API_INFO,
    struct {
        name: String,
        code: serde_json::Value,
        artifact_s3_location: String,
        execution_role_arn: String,
        schedule: serde_json::Value,
        runtime_version: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "Name": self.name,
            "Code": self.code,
            "ArtifactS3Location": self.artifact_s3_location,
            "ExecutionRoleArn": self.execution_role_arn,
            "Schedule": self.schedule,
            "RuntimeVersion": self.runtime_version
        });
        let result = client.execute("synthetics", "POST", "/canary", None, Some(&body_val), None).await?;

        span.add_event(
            "received result from aws synthetics",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = SyntheticsCreateCanaryInputBuilder::default()
            .name("my-canary")
            .code(serde_json::json!({"Handler": "index.handler", "S3Bucket": "my-bucket", "S3Key": "my-key"}))
            .artifact_s3_location("s3://my-bucket/artifacts")
            .execution_role_arn("arn:aws:iam::123456789012:role/CanaryRole")
            .schedule(serde_json::json!({"Expression": "rate(5 minutes)"}))
            .runtime_version("syn-nodejs-puppeteer-6.2")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "synthetics_create_canary");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "name": "my-canary",
            "code": {"Handler": "index.handler", "S3Bucket": "my-bucket", "S3Key": "my-key"},
            "artifact_s3_location": "s3://my-bucket/artifacts",
            "execution_role_arn": "arn:aws:iam::123456789012:role/CanaryRole",
            "schedule": {"Expression": "rate(5 minutes)"},
            "runtime_version": "syn-nodejs-puppeteer-6.2"
        });
        let _: SyntheticsCreateCanaryInput = serde_json::from_value(json).unwrap();
    }
}
