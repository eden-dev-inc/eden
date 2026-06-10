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

const API_INFO: ApiInfo<AwsApi, IvsCreateChannelInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::IvsCreateChannel, "ivs_create_channel", ReqType::Write, true);

crate::aws_endpoint! {
    IvsCreateChannel,
    API_INFO,
    struct {
        name: Option<String>,
        latency_mode: Option<String>,
        channel_type: Option<String>,
        tags: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({});
        let result = client.execute("ivs", "POST", "/CreateChannel", None, Some(&body_val), None).await?;

        span.add_event("received result from aws service", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = IvsCreateChannelInputBuilder::default().build().unwrap();
        assert_eq!(serde_json::to_value(&input).unwrap()["type"], "ivs_create_channel");
    }

    #[test]
    fn deserialize_minimal() {
        let _: IvsCreateChannelInput = serde_json::from_value(serde_json::json!({})).unwrap();
    }
}
