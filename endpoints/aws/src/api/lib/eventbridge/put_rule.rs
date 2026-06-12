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

const API_INFO: ApiInfo<AwsApi, EventBridgePutRuleInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::EventBridgePutRule, "eventbridge_put_rule", ReqType::Write, true);

crate::aws_endpoint! {
    EventBridgePutRule,
    API_INFO,
    struct {
        name: String,
        schedule_expression: Option<String>,
        event_pattern: Option<String>,
        state: Option<String>,
        description: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("Name".to_string(), Value::String(self.name.clone()));
        if let Some(s) = &self.schedule_expression {
            body.insert("ScheduleExpression".to_string(), Value::String(s.clone()));
        }
        if let Some(p) = &self.event_pattern {
            body.insert("EventPattern".to_string(), Value::String(p.clone()));
        }
        if let Some(s) = &self.state {
            body.insert("State".to_string(), Value::String(s.clone()));
        }
        if let Some(d) = &self.description {
            body.insert("Description".to_string(), Value::String(d.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("events", "AmazonEventBridge.PutRule", Some(&body_val), "1.1").await?;

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
        let input = EventBridgePutRuleInputBuilder::default().name("r").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "eventbridge_put_rule");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "r"});
        let _: EventBridgePutRuleInput = serde_json::from_value(json).unwrap();
    }
}
