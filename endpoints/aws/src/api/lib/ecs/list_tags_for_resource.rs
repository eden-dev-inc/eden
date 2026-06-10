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

const API_INFO: ApiInfo<AwsApi, EcsListTagsForResourceInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::EcsListTagsForResource, "Lists tags for an ECS resource", ReqType::Read, true);

crate::aws_endpoint! {
    EcsListTagsForResource,
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

        let body = serde_json::json!({
            "resourceArn": self.resource_arn
        });
        let result = client.execute("ecs", "POST", "/tags/list", None, Some(&body), None).await?;

        span.add_event("received result from aws ecs", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = EcsListTagsForResourceInputBuilder::default().resource_arn("arn").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ecs_list_tags_for_resource");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource_arn": "arn"});
        let _: EcsListTagsForResourceInput = serde_json::from_value(json).unwrap();
    }
}
