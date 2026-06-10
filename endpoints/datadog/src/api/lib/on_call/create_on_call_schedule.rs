use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_on_call::{CreateOnCallScheduleOptionalParams, OnCallAPI};
use datadog_api_client::datadogV2::model::ScheduleCreateRequest;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, CreateOnCallScheduleInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::CreateOnCallSchedule,
    "Creates a new on-call schedule in Datadog",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    CreateOnCallSchedule,
    API_INFO,
    struct {
        body: Value
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = OnCallAPI::with_config(client.dd_config.clone());
        let typed_body: ScheduleCreateRequest = serde_json::from_value(self.body.clone()).map_err(EpError::serde)?;
        let result =
            api.create_on_call_schedule(typed_body, CreateOnCallScheduleOptionalParams::default()).await.map_err(EpError::request)?;

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
    fn create_on_call_schedule_builder_serde() {
        let body = serde_json::json!({"data": {"type": "schedules", "attributes": {"name": "My Schedule", "time_zone": "UTC"}}});
        let input = CreateOnCallScheduleInputBuilder::default()
            .body(body.clone())
            .build()
            .expect("Failed to build CreateOnCallScheduleInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "create_on_call_schedule");
        assert_eq!(json["body"], body);
    }

    #[test]
    fn create_on_call_schedule_deserialize() {
        let json = serde_json::json!({"body": {"data": {"type": "schedules"}}});
        let input: CreateOnCallScheduleInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.body["data"]["type"], "schedules");
    }
}
