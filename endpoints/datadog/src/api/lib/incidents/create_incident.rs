use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_incidents::IncidentsAPI;
use datadog_api_client::datadogV2::model::IncidentCreateRequest;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, CreateIncidentInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::CreateIncident,
    "Creates a new incident in Datadog Incident Management",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    CreateIncident,
    API_INFO,
    struct {
        body: Value
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = IncidentsAPI::with_config(client.dd_config.clone());
        let typed_body: IncidentCreateRequest = serde_json::from_value(self.body.clone()).map_err(EpError::serde)?;
        let result = api.create_incident(typed_body).await.map_err(EpError::request)?;

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
    fn create_incident_builder_serde() {
        let body = serde_json::json!({"data": {"type": "incidents"}});
        let input = CreateIncidentInputBuilder::default().body(body.clone()).build().expect("Failed to build CreateIncidentInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "create_incident");
        assert_eq!(json["body"], body);
    }

    #[test]
    fn create_incident_deserialize() {
        let body = serde_json::json!({"data": {"type": "incidents"}});
        let json = serde_json::json!({"body": body});
        let input: CreateIncidentInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.body["data"]["type"], "incidents");
    }
}
