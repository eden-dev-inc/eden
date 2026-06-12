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

const API_INFO: ApiInfo<AwsApi, S3ListPartsInput> = ApiInfo::new(EpKind::Aws, AwsApi::S3ListParts, "s3_list_parts", ReqType::Read, true);

crate::aws_endpoint! {
    S3ListParts,
    API_INFO,
    struct {
        bucket: String,
        key: String,
        upload_id: String,
        max_parts: Option<i64>,
        part_number_marker: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/{}/{}", self.bucket, self.key);
        let mut query_parts = vec![format!("uploadId={}", self.upload_id)];
        if let Some(m) = self.max_parts {
            query_parts.push(format!("max-parts={}", m));
        }
        if let Some(p) = &self.part_number_marker {
            query_parts.push(format!("part-number-marker={}", p));
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
        let input = S3ListPartsInputBuilder::default().bucket("b").key("k").upload_id("uid").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "s3_list_parts");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"bucket": "b", "key": "k", "upload_id": "uid"});
        let _: S3ListPartsInput = serde_json::from_value(json).unwrap();
    }
}
