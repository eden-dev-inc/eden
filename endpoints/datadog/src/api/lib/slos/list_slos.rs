use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_service_level_objectives::{ListSLOsOptionalParams, ServiceLevelObjectivesAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, ListSlosInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::ListSlos,
    "Lists all SLOs from Datadog, optionally filtered by tags or IDs",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    ListSlos,
    API_INFO,
    struct {
        tags: Option<String>,
        slo_ids: Option<String>,
        limit: Option<i64>,
        offset: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = ServiceLevelObjectivesAPI::with_config(client.dd_config.clone());
        let mut params = ListSLOsOptionalParams::default();
        if let Some(ids) = &self.slo_ids {
            params = params.ids(ids.clone());
        }
        if let Some(tags) = &self.tags {
            params = params.query(tags.clone());
        }
        if let Some(limit) = self.limit {
            params = params.limit(limit);
        }
        if let Some(offset) = self.offset {
            params = params.offset(offset);
        }
        let result = api.list_slos(params).await.map_err(EpError::request)?;

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
    fn list_slos_builder_serde() {
        let input = ListSlosInputBuilder::default()
            .tags(Some("team:backend".to_string()))
            .slo_ids(None)
            .limit(None)
            .offset(None)
            .build()
            .expect("Failed to build ListSlosInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "list_slos");
        assert_eq!(json["tags"], "team:backend");
    }

    #[test]
    fn list_slos_deserialize() {
        let json = serde_json::json!({});
        let input: ListSlosInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(input.tags.is_none());
        assert!(input.slo_ids.is_none());
    }
}
