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

const API_INFO: ApiInfo<AwsApi, S3ListObjectsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::S3ListObjects, "Lists objects in an S3 bucket", ReqType::Read, true);

crate::aws_endpoint! {
    S3ListObjects,
    API_INFO,
    struct {
        bucket: String,
        prefix: Option<String>,
        delimiter: Option<String>,
        max_keys: Option<i64>,
        continuation_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/{}", self.bucket);
        let mut query_parts = vec!["list-type=2".to_string()];
        if let Some(p) = &self.prefix {
            query_parts.push(format!("prefix={}", p));
        }
        if let Some(d) = &self.delimiter {
            query_parts.push(format!("delimiter={}", d));
        }
        if let Some(max) = self.max_keys {
            query_parts.push(format!("max-keys={}", max));
        }
        if let Some(ct) = &self.continuation_token {
            query_parts.push(format!("continuation-token={}", ct));
        }
        let query = query_parts.join("&");
        let result = client.execute("s3", "GET", &path, Some(&query), None, None).await?;

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
        let input = S3ListObjectsInputBuilder::default().bucket("my-bucket").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "s3_list_objects");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"bucket": "b"});
        let _: S3ListObjectsInput = serde_json::from_value(json).unwrap();
    }
}
