use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, EventBridgeRemoveTargetsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::EventBridgeRemoveTargets, "eventbridge_remove_targets", ReqType::Write, true);

crate::aws_endpoint! {
    EventBridgeRemoveTargets,
    API_INFO,
    struct {
        rule: String,
        ids: Vec<String>,
        event_bus_name: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("Rule".to_string(), Value::String(self.rule.clone()));
        body.insert("Ids".to_string(), Value::Array(self.ids.iter().map(|s| Value::String(s.clone())).collect()));
        if let Some(e) = &self.event_bus_name {
            body.insert("EventBusName".to_string(), Value::String(e.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("events", "AmazonEventBridge.RemoveTargets", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws events", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AwsJsonOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = EventBridgeRemoveTargetsInputBuilder::default().rule("r").ids(vec![]).build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "eventbridge_remove_targets");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"rule": "r", "ids": []});
        let _: EventBridgeRemoveTargetsInput = serde_json::from_value(json).unwrap();
    }
}
