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

const API_INFO: ApiInfo<AwsApi, RumDeleteAppMonitorInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::RumDeleteAppMonitor, "rum_delete_app_monitor", ReqType::Write, true);

crate::aws_endpoint! {
    RumDeleteAppMonitor,
    API_INFO,
    struct {
        name: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/appmonitor/{}", self.name);
        let result = client.execute("rum", "DELETE", &path, None, None, None).await?;

        span.add_event("received result from aws rum", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = RumDeleteAppMonitorInputBuilder::default().name("my-monitor").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "rum_delete_app_monitor");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "my-monitor"});
        let _: RumDeleteAppMonitorInput = serde_json::from_value(json).unwrap();
    }
}
