use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, MuteMonitorInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::MuteMonitor,
    "Mutes a specific monitor in Datadog, optionally until a given timestamp",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    MuteMonitor,
    API_INFO,
    struct {
        monitor_id: i64,
        body: Option<Value>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let path = format!("/api/v1/monitor/{}/mute", self.monitor_id);
        let result = client.post(&path, self.body.clone()).await?;

        span.add_event("received result from datadog", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(DatadogJsonOutput(result).to_output()) as Box<dyn EpOutput>)
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
    fn mute_monitor_builder_serde() {
        let input = MuteMonitorInputBuilder::default()
            .monitor_id(12345i64)
            .body(Some(serde_json::json!({"end": 1700000000})))
            .build()
            .expect("Failed to build MuteMonitorInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "mute_monitor");
        assert_eq!(json["monitor_id"], 12345);
    }

    #[test]
    fn mute_monitor_no_body() {
        let input = MuteMonitorInputBuilder::default()
            .monitor_id(99i64)
            .body(None::<Value>)
            .build()
            .expect("Failed to build MuteMonitorInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "mute_monitor");
        assert_eq!(json["monitor_id"], 99);
        assert!(json["body"].is_null());
    }

    #[test]
    fn mute_monitor_deserialize() {
        let json = serde_json::json!({"monitor_id": 42, "body": null});
        let input: MuteMonitorInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.monitor_id, 42);
        assert!(input.body.is_none());
    }
}
