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

const API_INFO: ApiInfo<AwsApi, LambdaUntagResourceInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::LambdaUntagResource, "lambda_untag_resource", ReqType::Write, true);

crate::aws_endpoint! {
    LambdaUntagResource,
    API_INFO,
    struct {
        resource: String,
        tag_keys: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/2017-03-31/tags/{}", self.resource);
        let query_parts: Vec<String> = self.tag_keys.iter().map(|k| format!("tagKeys={}", k)).collect();
        let query = if query_parts.is_empty() {
            None
        } else {
            Some(query_parts.join("&"))
        };
        let result = client.execute("lambda", "DELETE", &path, query.as_deref(), None, None).await?;

        span.add_event("received result from aws lambda", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = LambdaUntagResourceInputBuilder::default()
            .resource("arn:aws:lambda:us-east-1:123:function:f")
            .tag_keys(vec!["key1".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lambda_untag_resource");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource": "arn", "tag_keys": ["k1"]});
        let _: LambdaUntagResourceInput = serde_json::from_value(json).unwrap();
    }
}
