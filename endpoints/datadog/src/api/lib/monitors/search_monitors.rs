use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_monitors::{MonitorsAPI, SearchMonitorsOptionalParams};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, SearchMonitorsInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::SearchMonitors,
    "Searches monitors in Datadog with optional query filter",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    SearchMonitors,
    API_INFO,
    struct {
        query: Option<String>,
        page: Option<i64>,
        per_page: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = MonitorsAPI::with_config(client.dd_config.clone());
        let mut params = SearchMonitorsOptionalParams::default();
        if let Some(q) = &self.query {
            params = params.query(q.clone());
        }
        if let Some(p) = self.page {
            params = params.page(p);
        }
        if let Some(pp) = self.per_page {
            params = params.per_page(pp);
        }
        let result = api.search_monitors(params).await.map_err(EpError::request)?;

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
    fn search_monitors_builder_serde() {
        let input = SearchMonitorsInputBuilder::default()
            .query(Some("status:Alert".to_string()))
            .page(Some(0i64))
            .per_page(Some(30i64))
            .build()
            .expect("Failed to build SearchMonitorsInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "search_monitors");
        assert_eq!(json["query"], "status:Alert");
    }

    #[test]
    fn search_monitors_deserialize() {
        let json = serde_json::json!({"query": "status:Alert", "page": 0, "per_page": 30});
        let input: SearchMonitorsInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.query, Some("status:Alert".to_string()));
    }
}
