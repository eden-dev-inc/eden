use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_monitors::{DeleteMonitorOptionalParams, MonitorsAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, DeleteMonitorInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::DeleteMonitor,
    "Deletes a monitor by ID from Datadog",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    DeleteMonitor,
    API_INFO,
    struct {
        monitor_id: i64,
        force: Option<String>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = MonitorsAPI::with_config(client.dd_config.clone());
        let mut params = DeleteMonitorOptionalParams::default();
        if let Some(f) = &self.force {
            params = params.force(f.clone());
        }
        let result = api.delete_monitor(self.monitor_id, params).await.map_err(EpError::request)?;

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
    fn delete_monitor_builder_serde() {
        let input = DeleteMonitorInputBuilder::default()
            .monitor_id(12345i64)
            .force(None::<String>)
            .build()
            .expect("Failed to build DeleteMonitorInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "delete_monitor");
        assert_eq!(json["monitor_id"], 12345);
    }

    #[test]
    fn delete_monitor_deserialize() {
        let json = serde_json::json!({"monitor_id": 99});
        let input: DeleteMonitorInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.monitor_id, 99);
    }
}
