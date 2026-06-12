use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_incident_services::{IncidentServicesAPI, ListIncidentServicesOptionalParams};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, ListIncidentServicesInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::ListIncidentServices,
    "Lists all incident services in Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    ListIncidentServices,
    API_INFO,
    struct {
        query: Option<String>,
        page_size: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = IncidentServicesAPI::with_config(client.dd_config.clone());

        let mut params = ListIncidentServicesOptionalParams::default();
        if let Some(q) = &self.query {
            params = params.filter(q.clone());
        }
        if let Some(ps) = self.page_size {
            params = params.page_size(ps);
        }

        let result = api.list_incident_services(params).await.map_err(EpError::request)?;

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
    fn list_incident_services_builder_serde() {
        let input = ListIncidentServicesInputBuilder::default()
            .query(Some("web".to_string()))
            .page_size(Some(10_i64))
            .build()
            .expect("Failed to build ListIncidentServicesInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "list_incident_services");
        assert_eq!(json["query"], "web");
        assert_eq!(json["page_size"], 10);
    }

    #[test]
    fn list_incident_services_deserialize() {
        let json = serde_json::json!({});
        let input: ListIncidentServicesInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(input.query.is_none());
        assert!(input.page_size.is_none());
    }
}
