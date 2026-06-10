use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_on_call::{GetOnCallScheduleOptionalParams, OnCallAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, GetOnCallScheduleInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::GetOnCallSchedule,
    "Gets a specific on-call schedule from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    GetOnCallSchedule,
    API_INFO,
    struct {
        schedule_id: String
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = OnCallAPI::with_config(client.dd_config.clone());
        let result = api
            .get_on_call_schedule(self.schedule_id.clone(), GetOnCallScheduleOptionalParams::default())
            .await
            .map_err(EpError::request)?;

        span.add_event("received result from datadog", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(DatadogJsonOutput(serde_json::to_value(result).map_err(EpError::serde)?).to_output()) as Box<dyn EpOutput>)
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
    fn get_on_call_schedule_builder_serde() {
        let input = GetOnCallScheduleInputBuilder::default()
            .schedule_id("sched-123".to_string())
            .build()
            .expect("Failed to build GetOnCallScheduleInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "get_on_call_schedule");
        assert_eq!(json["schedule_id"], "sched-123");
    }

    #[test]
    fn get_on_call_schedule_deserialize() {
        let json = serde_json::json!({"schedule_id": "sched-456"});
        let input: GetOnCallScheduleInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.schedule_id, "sched-456");
    }
}
