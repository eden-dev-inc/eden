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

const API_INFO: ApiInfo<AwsApi, ResourceGroupsTaggingUntagResourcesInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ResourceGroupsTaggingUntagResources,
    "resourcegroupstagging_untag_resources",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    ResourceGroupsTaggingUntagResources,
    API_INFO,
    struct {
        resource_arn_list: Vec<String>,
        tag_keys: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"ResourceARNList": self.resource_arn_list, "TagKeys": self.tag_keys});
        let result = client.execute("tagging", "POST", "/tagging/UntagResources", None, Some(&body_val), None).await?;

        span.add_event("received result from aws tagging", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = ResourceGroupsTaggingUntagResourcesInputBuilder::default()
            .resource_arn_list(vec!["arn:aws:s3:::my-bucket".to_string()])
            .tag_keys(vec!["env".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "resourcegroupstagging_untag_resources");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource_arn_list": ["arn:aws:s3:::my-bucket"], "tag_keys": ["env"]});
        let _: ResourceGroupsTaggingUntagResourcesInput = serde_json::from_value(json).unwrap();
    }
}
