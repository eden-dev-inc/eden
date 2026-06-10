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

const API_INFO: ApiInfo<AwsApi, CloudWatchTagResourceInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::CloudWatchTagResource, "cloudwatch_tag_resource", ReqType::Write, true);

crate::aws_endpoint! {
    CloudWatchTagResource,
    API_INFO,
    struct {
        resource_arn: String,
        tags: Vec<serde_json::Value>
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
        for (i, tag) in self.tags.iter().enumerate() {
            let idx = i + 1;
            if let Some(key) = tag.get("Key").and_then(|v| v.as_str()) {
                params.insert(format!("Tags.member.{}.Key", idx), key.to_string());
            }
            if let Some(value) = tag.get("Value").and_then(|v| v.as_str()) {
                params.insert(format!("Tags.member.{}.Value", idx), value.to_string());
            }
        }
        let form_body = build_query_body("TagResource", "2010-08-01", &params);
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
        let input = CloudWatchTagResourceInputBuilder::default()
            .resource_arn("arn:aws:cloudwatch:us-east-1:123456789012:alarm:my-alarm")
            .tags(vec![])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudwatch_tag_resource");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource_arn": "arn:aws:cloudwatch:us-east-1:123456789012:alarm:my-alarm", "tags": []});
        let _: CloudWatchTagResourceInput = serde_json::from_value(json).unwrap();
    }
}
