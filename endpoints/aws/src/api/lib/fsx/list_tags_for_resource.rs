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

const API_INFO: ApiInfo<AwsApi, FSxListTagsForResourceInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::FSxListTagsForResource, "fsx_list_tags_for_resource", ReqType::Read, true);

crate::aws_endpoint! {
    FSxListTagsForResource,
    API_INFO,
    struct {
        resource_arn: String,
        max_results: Option<i64>,
        next_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_val = serde_json::json!({ "ResourceARN": self.resource_arn });
        if let Some(v) = self.max_results {
            body_val["MaxResults"] = serde_json::json!(v);
        }
        if let Some(ref v) = self.next_token {
            body_val["NextToken"] = serde_json::Value::String(v.clone());
        }
        let result = client.execute_json_target("fsx", "AmazonFSx.ListTagsForResource", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws fsx", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = FSxListTagsForResourceInputBuilder::default()
            .resource_arn("arn:aws:fsx:us-east-1:123456789012:file-system/fs-123")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "fsx_list_tags_for_resource");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource_arn": "arn:aws:fsx:us-east-1:123456789012:file-system/fs-123"});
        let _: FSxListTagsForResourceInput = serde_json::from_value(json).unwrap();
    }
}
