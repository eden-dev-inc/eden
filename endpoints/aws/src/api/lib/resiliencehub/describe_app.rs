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

const API_INFO: ApiInfo<AwsApi, ResilienceHubDescribeAppInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::ResilienceHubDescribeApp, "resiliencehub_describe_app", ReqType::Read, true);

crate::aws_endpoint! {
    ResilienceHubDescribeApp,
    API_INFO,
    struct {
        app_arn: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({"appArn": self.app_arn});
        let result = client.execute("resiliencehub", "POST", "/describe-app", None, Some(&body), None).await?;

        span.add_event(
            "received result from aws resiliencehub",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = ResilienceHubDescribeAppInputBuilder::default()
            .app_arn("arn:aws:resiliencehub:us-east-1:123456789012:app/app-abc123")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "resiliencehub_describe_app");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"app_arn": "arn:aws:resiliencehub:us-east-1:123456789012:app/app-abc123"});
        let _: ResilienceHubDescribeAppInput = serde_json::from_value(json).unwrap();
    }
}
