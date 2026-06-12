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

const API_INFO: ApiInfo<AwsApi, MediaLiveStartChannelInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::MediaLiveStartChannel, "medialive_start_channel", ReqType::Write, true);

crate::aws_endpoint! {
    MediaLiveStartChannel,
    API_INFO,
    struct {
        channel_id: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/prod/channels/{}/start", self.channel_id);
        let body_val = serde_json::json!({});
        let result = client.execute("medialive", "POST", &path, None, Some(&body_val), None).await?;

        span.add_event("received result from aws medialive", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = MediaLiveStartChannelInputBuilder::default().channel_id("12345").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "medialive_start_channel");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({ "channel_id": "12345" });
        let _: MediaLiveStartChannelInput = serde_json::from_value(json).unwrap();
    }
}
