use crate::api::lib::AwsApi;
use crate::api::lib::params::build_query_body;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use std::collections::HashMap;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, CloudWatchListTagsForResourceInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CloudWatchListTagsForResource,
    "cloudwatch_list_tags_for_resource",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    CloudWatchListTagsForResource,
    API_INFO,
    struct {
        resource_arn: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("ResourceARN".to_string(), self.resource_arn.clone());
        let form_body = build_query_body("ListTagsForResource", "2010-08-01", &params);
        let result = client.execute_form("cloudwatch", &form_body).await?;

        span.add_event(
            "received result from aws cloudwatch",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
        Ok(Box::new(AwsJsonOutput(Value::String(result)).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = CloudWatchListTagsForResourceInputBuilder::default()
            .resource_arn("arn:aws:cloudwatch:us-east-1:123456789012:alarm:my-alarm")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudwatch_list_tags_for_resource");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource_arn": "arn:aws:cloudwatch:us-east-1:123456789012:alarm:my-alarm"});
        let _: CloudWatchListTagsForResourceInput = serde_json::from_value(json).unwrap();
    }
}
