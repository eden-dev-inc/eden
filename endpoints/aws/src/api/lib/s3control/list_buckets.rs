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

const API_INFO: ApiInfo<AwsApi, S3ControlListBucketsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::S3ControlListBuckets, "s3control_list_buckets", ReqType::Read, true);

crate::aws_endpoint! {
    S3ControlListBuckets,
    API_INFO,
    struct {
        outpost_id: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let result = client.execute("s3control", "GET", "/v20180820/bucket", None, None, None).await?;

        span.add_event("received result from aws s3control", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = S3ControlListBucketsInputBuilder::default().outpost_id("op-01234567890abcdef").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "s3control_list_buckets");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"outpost_id": "op-01234567890abcdef"});
        let _: S3ControlListBucketsInput = serde_json::from_value(json).unwrap();
    }
}
