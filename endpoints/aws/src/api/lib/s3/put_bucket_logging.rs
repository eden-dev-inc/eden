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

const API_INFO: ApiInfo<AwsApi, S3PutBucketLoggingInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::S3PutBucketLogging, "s3_put_bucket_logging", ReqType::Write, true);

crate::aws_endpoint! {
    S3PutBucketLogging,
    API_INFO,
    struct {
        bucket: String,
        bucket_logging_status: Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/{}", self.bucket);
        let result = client.execute("s3", "PUT", &path, Some("logging"), Some(&self.bucket_logging_status), None).await?;

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
        let input = S3PutBucketLoggingInputBuilder::default().bucket("b").bucket_logging_status(serde_json::json!({})).build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "s3_put_bucket_logging");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"bucket": "b", "bucket_logging_status": {}});
        let _: S3PutBucketLoggingInput = serde_json::from_value(json).unwrap();
    }
}
