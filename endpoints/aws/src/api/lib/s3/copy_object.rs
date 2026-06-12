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

const API_INFO: ApiInfo<AwsApi, S3CopyObjectInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::S3CopyObject, "s3_copy_object", ReqType::Write, true);

crate::aws_endpoint! {
    S3CopyObject,
    API_INFO,
    struct {
        source_bucket: String,
        source_key: String,
        dest_bucket: String,
        dest_key: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/{}/{}", self.dest_bucket, self.dest_key);
        let query = format!("x-amz-copy-source=/{}/{}", self.source_bucket, self.source_key);
        let result = client.execute("s3", "PUT", &path, Some(&query), None, None).await?;

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
        let input = S3CopyObjectInputBuilder::default()
            .source_bucket("src-bucket")
            .source_key("src-key")
            .dest_bucket("dst-bucket")
            .dest_key("dst-key")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "s3_copy_object");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "source_bucket": "src",
            "source_key": "k1",
            "dest_bucket": "dst",
            "dest_key": "k2"
        });
        let _: S3CopyObjectInput = serde_json::from_value(json).unwrap();
    }
}
