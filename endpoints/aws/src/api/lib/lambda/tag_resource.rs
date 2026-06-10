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

const API_INFO: ApiInfo<AwsApi, LambdaTagResourceInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::LambdaTagResource, "Adds tags to a Lambda function", ReqType::Write, true);

crate::aws_endpoint! {
    LambdaTagResource,
    API_INFO,
    struct {
        resource_arn: String,
        tags: Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "Tags": self.tags
        });
        let path = format!("/2017-03-31/tags/{}", self.resource_arn);
        let result = client.execute("lambda", "POST", &path, None, Some(&body), None).await?;

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
        let input = LambdaTagResourceInputBuilder::default()
            .resource_arn("arn:aws:lambda:us-east-1:123:function:f")
            .tags(serde_json::json!({"env": "prod"}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lambda_tag_resource");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource_arn": "arn", "tags": {"k": "v"}});
        let _: LambdaTagResourceInput = serde_json::from_value(json).unwrap();
    }
}
