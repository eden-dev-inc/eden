use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_downtimes::DowntimesAPI;
use datadog_api_client::datadogV2::model::DowntimeUpdateRequest;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, UpdateDowntimeV2Input> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::UpdateDowntimeV2,
    "Updates an existing downtime (V2 API) in Datadog",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    UpdateDowntimeV2,
    API_INFO,
    struct {
        downtime_id: String,
        body: Value
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = DowntimesAPI::with_config(client.dd_config.clone());
        let typed_body: DowntimeUpdateRequest = serde_json::from_value(self.body.clone()).map_err(EpError::serde)?;
        let result = api.update_downtime(self.downtime_id.clone(), typed_body).await.map_err(EpError::request)?;

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
    fn update_downtime_v2_builder_serde() {
        let body = serde_json::json!({
            "data": {
                "type": "downtime",
                "attributes": {"message": "Updated maintenance"}
            }
        });
        let input = UpdateDowntimeV2InputBuilder::default()
            .downtime_id("abc-123".to_string())
            .body(body.clone())
            .build()
            .expect("Failed to build UpdateDowntimeV2Input");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "update_downtime_v2");
        assert_eq!(json["downtime_id"], "abc-123");
        assert_eq!(json["body"], body);
    }

    #[test]
    fn update_downtime_v2_deserialize() {
        let json = serde_json::json!({"downtime_id": "xyz-456", "body": {"data": {"type": "downtime"}}});
        let input: UpdateDowntimeV2Input = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.downtime_id, "xyz-456");
    }
}
