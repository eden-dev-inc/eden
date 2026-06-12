use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_service_level_objectives::{GetSLOHistoryOptionalParams, ServiceLevelObjectivesAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, GetSloHistoryInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::GetSloHistory,
    "Retrieves the history and status for a specific SLO over a given time range",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    GetSloHistory,
    API_INFO,
    struct {
        slo_id: String,
        from_ts: i64,
        to_ts: i64
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = ServiceLevelObjectivesAPI::with_config(client.dd_config.clone());
        let result = api
            .get_slo_history(self.slo_id.clone(), self.from_ts, self.to_ts, GetSLOHistoryOptionalParams::default())
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
    fn get_slo_history_builder_serde() {
        let input = GetSloHistoryInputBuilder::default()
            .slo_id("abc123")
            .from_ts(1609459200i64)
            .to_ts(1609545600i64)
            .build()
            .expect("Failed to build GetSloHistoryInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "get_slo_history");
        assert_eq!(json["slo_id"], "abc123");
        assert_eq!(json["from_ts"], 1609459200);
        assert_eq!(json["to_ts"], 1609545600);
    }

    #[test]
    fn get_slo_history_deserialize() {
        let json = serde_json::json!({"slo_id": "xyz", "from_ts": 100, "to_ts": 200});
        let input: GetSloHistoryInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.slo_id, "xyz");
        assert_eq!(input.from_ts, 100);
        assert_eq!(input.to_ts, 200);
    }
}
