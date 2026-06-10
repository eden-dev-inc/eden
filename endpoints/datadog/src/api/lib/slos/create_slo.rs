use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_service_level_objectives::ServiceLevelObjectivesAPI;
use datadog_api_client::datadogV1::model::ServiceLevelObjectiveRequest;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, CreateSloInput> =
    ApiInfo::new(EpKind::Datadog, DatadogApi::CreateSlo, "Creates a new SLO in Datadog", ReqType::Write, true);

crate::datadog_endpoint! {
    CreateSlo,
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
        let api = ServiceLevelObjectivesAPI::with_config(client.dd_config.clone());
        let typed_body: ServiceLevelObjectiveRequest = serde_json::from_value(self.body.clone()).map_err(EpError::serde)?;
        let result = api.create_slo(typed_body).await.map_err(EpError::request)?;

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
    fn create_slo_builder_serde() {
        let body = serde_json::json!({
            "name": "API availability",
            "type": "metric",
            "thresholds": [{"target": 99.9, "timeframe": "7d"}],
            "query": {"numerator": "sum:requests.success{*}", "denominator": "sum:requests.total{*}"}
        });
        let input = CreateSloInputBuilder::default().body(body.clone()).build().expect("Failed to build CreateSloInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "create_slo");
        assert_eq!(json["body"], body);
    }

    #[test]
    fn create_slo_deserialize() {
        let json = serde_json::json!({"body": {"name": "test SLO", "type": "metric"}});
        let input: CreateSloInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.body["name"], "test SLO");
    }
}
