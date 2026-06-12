use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_ci_visibility_pipelines::{CIVisibilityPipelinesAPI, ListCIAppPipelineEventsOptionalParams};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, ListCiPipelineEventsInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::ListCiPipelineEvents,
    "Lists CI pipeline events from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    ListCiPipelineEvents,
    API_INFO,
    struct {
        filter_query: Option<String>,
        page_limit: Option<i32>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = CIVisibilityPipelinesAPI::with_config(client.dd_config.clone());

        let mut params = ListCIAppPipelineEventsOptionalParams::default();
        if let Some(ref v) = self.filter_query {
            params = params.filter_query(v.clone());
        }
        if let Some(v) = self.page_limit {
            params = params.page_limit(v);
        }

        let result = api.list_ci_app_pipeline_events(params).await.map_err(EpError::request)?;

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
    fn list_ci_pipeline_events_builder_serde() {
        let input = ListCiPipelineEventsInputBuilder::default()
            .filter_query(Some("@pipeline.status:error".to_string()))
            .page_limit(Some(25))
            .build()
            .expect("Failed to build ListCiPipelineEventsInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "list_ci_pipeline_events");
        assert_eq!(json["filter_query"], "@pipeline.status:error");
        assert_eq!(json["page_limit"], 25);
    }

    #[test]
    fn list_ci_pipeline_events_deserialize() {
        let json = serde_json::json!({});
        let input: ListCiPipelineEventsInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(input.filter_query.is_none());
        assert!(input.page_limit.is_none());
    }
}
