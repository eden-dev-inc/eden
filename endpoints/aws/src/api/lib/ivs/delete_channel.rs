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

const API_INFO: ApiInfo<AwsApi, IvsDeleteChannelInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::IvsDeleteChannel, "ivs_delete_channel", ReqType::Write, true);

crate::aws_endpoint! {
    IvsDeleteChannel,
    API_INFO,
    struct {
        arn: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({ "arn": self.arn });
        let result = client.execute("ivs", "POST", "/DeleteChannel", None, Some(&body_val), None).await?;

        span.add_event("received result from aws ivs", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = IvsDeleteChannelInputBuilder::default().arn("arn:aws:ivs:us-east-1:123456789012:channel/abc123").build().unwrap();
        assert_eq!(serde_json::to_value(&input).unwrap()["type"], "ivs_delete_channel");
    }

    #[test]
    fn deserialize_minimal() {
        let _: IvsDeleteChannelInput =
            serde_json::from_value(serde_json::json!({"arn": "arn:aws:ivs:us-east-1:123456789012:channel/abc123"})).unwrap();
    }
}
