use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_incidents::{IncidentsAPI, SearchIncidentsOptionalParams};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, SearchIncidentsInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::SearchIncidents,
    "Searches incidents in Datadog with optional filters",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    SearchIncidents,
    API_INFO,
    struct {
        query: String,
        sort: Option<String>,
        page_size: Option<i64>,
        page_offset: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = IncidentsAPI::with_config(client.dd_config.clone());

        let mut params = SearchIncidentsOptionalParams::default();
        if let Some(s) = &self.sort {
            let sort_order = serde_json::from_str(&format!("\"{}\"", s)).map_err(EpError::serde)?;
            params = params.sort(sort_order);
        }
        if let Some(ps) = self.page_size {
            params = params.page_size(ps);
        }
        if let Some(po) = self.page_offset {
            params = params.page_offset(po);
        }

        let result = api.search_incidents(self.query.clone(), params).await.map_err(EpError::request)?;

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
    fn search_incidents_builder_serde() {
        let input = SearchIncidentsInputBuilder::default()
            .query("state:active")
            .sort(Some("created".to_string()))
            .page_size(Some(25_i64))
            .page_offset(Some(0_i64))
            .build()
            .expect("Failed to build SearchIncidentsInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "search_incidents");
        assert_eq!(json["query"], "state:active");
        assert_eq!(json["sort"], "created");
        assert_eq!(json["page_size"], 25);
    }

    #[test]
    fn search_incidents_deserialize() {
        let json = serde_json::json!({"query": "state:active"});
        let input: SearchIncidentsInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.query, "state:active");
        assert!(input.sort.is_none());
    }
}
