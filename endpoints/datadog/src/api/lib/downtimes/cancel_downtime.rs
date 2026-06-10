use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_downtimes::DowntimesAPI;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, CancelDowntimeInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::CancelDowntime,
    "Cancels a scheduled downtime in Datadog",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    CancelDowntime,
    API_INFO,
    struct {
        downtime_id: i64
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = DowntimesAPI::with_config(client.dd_config.clone());
        api.cancel_downtime(self.downtime_id).await.map_err(EpError::request)?;

        span.add_event("received result from datadog", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(DatadogJsonOutput(serde_json::json!({"success": true})).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut DatadogTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_downtime_builder_serde() {
        let input = CancelDowntimeInputBuilder::default().downtime_id(12345i64).build().expect("Failed to build CancelDowntimeInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "cancel_downtime");
        assert_eq!(json["downtime_id"], 12345);
    }

    #[test]
    fn cancel_downtime_deserialize() {
        let json = serde_json::json!({"downtime_id": 99});
        let input: CancelDowntimeInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.downtime_id, 99);
    }
}
