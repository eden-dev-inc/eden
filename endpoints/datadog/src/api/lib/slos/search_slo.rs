use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_service_level_objectives::{SearchSLOOptionalParams, ServiceLevelObjectivesAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, SearchSloInput> =
    ApiInfo::new(EpKind::Datadog, DatadogApi::SearchSlo, "Searches SLOs in Datadog", ReqType::Read, true);

crate::datadog_endpoint! {
    SearchSlo,
    API_INFO,
    struct {
        query: Option<String>,
        page_size: Option<i64>,
        page_number: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = ServiceLevelObjectivesAPI::with_config(client.dd_config.clone());
        let mut params = SearchSLOOptionalParams::default();
        if let Some(q) = &self.query {
            params = params.query(q.clone());
        }
        let result = api.search_slo(params).await.map_err(EpError::request)?;

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
    fn search_slo_builder_serde() {
        let input = SearchSloInputBuilder::default()
            .query(Some("env:prod".to_string()))
            .page_size(Some(25i64))
            .page_number(Some(0i64))
            .build()
            .expect("Failed to build SearchSloInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "search_slo");
        assert_eq!(json["query"], "env:prod");
    }

    #[test]
    fn search_slo_deserialize() {
        let json = serde_json::json!({"query": "env:prod", "page_size": 25, "page_number": 0});
        let input: SearchSloInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.query, Some("env:prod".to_string()));
    }
}
