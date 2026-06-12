use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_service_level_objectives::ServiceLevelObjectivesAPI;
use datadog_api_client::datadogV1::model::ServiceLevelObjective;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, UpdateSloInput> =
    ApiInfo::new(EpKind::Datadog, DatadogApi::UpdateSlo, "Updates an existing SLO in Datadog", ReqType::Write, true);

crate::datadog_endpoint! {
    UpdateSlo,
    API_INFO,
    struct {
        slo_id: String,
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
        let typed_body: ServiceLevelObjective = serde_json::from_value(self.body.clone()).map_err(EpError::serde)?;
        let result = api.update_slo(self.slo_id.clone(), typed_body).await.map_err(EpError::request)?;

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
    fn update_slo_builder_serde() {
        let body = serde_json::json!({
            "name": "My SLO",
            "type": "metric"
        });
        let input = UpdateSloInputBuilder::default()
            .slo_id("abc-123".to_string())
            .body(body.clone())
            .build()
            .expect("Failed to build UpdateSloInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "update_slo");
        assert_eq!(json["slo_id"], "abc-123");
        assert_eq!(json["body"], body);
    }

    #[test]
    fn update_slo_deserialize() {
        let json = serde_json::json!({"slo_id": "abc-123", "body": {"name": "test", "type": "metric"}});
        let input: UpdateSloInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.slo_id, "abc-123");
        assert_eq!(input.body["name"], "test");
    }
}
