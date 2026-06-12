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

const API_INFO: ApiInfo<AwsApi, SnsTagResourceInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SnsTagResource, "sns_tag_resource", ReqType::Write, true);

crate::aws_endpoint! {
    SnsTagResource,
    API_INFO,
    struct {
        resource_arn: String,
        tag_key: String,
        tag_value: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("ResourceArn".to_string(), self.resource_arn.clone());
        params.insert("Tags.member.1.Key".to_string(), self.tag_key.clone());
        params.insert("Tags.member.1.Value".to_string(), self.tag_value.clone());
        let form_body = build_query_body("TagResource", "2010-03-31", &params);
        let result = client.execute_form("sns", &form_body).await?;

        span.add_event("received result from aws sns", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SnsTagResourceInputBuilder::default()
            .resource_arn("arn:aws:sns:us-east-1:123456789012:my-topic")
            .tag_key("env")
            .tag_value("prod")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sns_tag_resource");
    }

    #[test]
    fn deserialize_minimal() {
        let json =
            serde_json::json!({"resource_arn": "arn:aws:sns:us-east-1:123456789012:my-topic", "tag_key": "env", "tag_value": "prod"});
        let _: SnsTagResourceInput = serde_json::from_value(json).unwrap();
    }
}
