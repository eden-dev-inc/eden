use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_dashboards::{DashboardsAPI, ListDashboardsOptionalParams};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, ListDashboardsInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::ListDashboards,
    "Retrieves all dashboards from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    ListDashboards,
    API_INFO,
    struct {}
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = DashboardsAPI::with_config(client.dd_config.clone());
        let result = api.list_dashboards(ListDashboardsOptionalParams::default()).await.map_err(EpError::request)?;

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
    fn list_dashboards_builder_serde() {
        let input = ListDashboardsInputBuilder::default().build().expect("Failed to build ListDashboardsInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "list_dashboards");
    }

    #[test]
    fn list_dashboards_deserialize() {
        let json = serde_json::json!({});
        let _input: ListDashboardsInput = serde_json::from_value(json).expect("Failed to deserialize");
    }
}
