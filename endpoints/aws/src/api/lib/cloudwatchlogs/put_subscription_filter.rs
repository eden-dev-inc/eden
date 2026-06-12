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

const API_INFO: ApiInfo<AwsApi, CloudWatchLogsPutSubscriptionFilterInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CloudWatchLogsPutSubscriptionFilter,
    "cloudwatchlogs_put_subscription_filter",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    CloudWatchLogsPutSubscriptionFilter,
    API_INFO,
    struct {
        log_group_name: String,
        filter_name: String,
        filter_pattern: String,
        destination_arn: String,
        role_arn: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("LogGroupName".to_string(), Value::String(self.log_group_name.clone()));
        body.insert("FilterName".to_string(), Value::String(self.filter_name.clone()));
        body.insert("FilterPattern".to_string(), Value::String(self.filter_pattern.clone()));
        body.insert("DestinationArn".to_string(), Value::String(self.destination_arn.clone()));
        if let Some(v) = &self.role_arn {
            body.insert("RoleArn".to_string(), Value::String(v.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("logs", "Logs_20140328.PutSubscriptionFilter", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws logs", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = CloudWatchLogsPutSubscriptionFilterInputBuilder::default()
            .log_group_name("test")
            .filter_name("filter")
            .filter_pattern("")
            .destination_arn("arn:aws:lambda:us-east-1:123:function:test")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudwatchlogs_put_subscription_filter");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "log_group_name": "test",
            "filter_name": "filter",
            "filter_pattern": "",
            "destination_arn": "arn:aws:lambda:us-east-1:123:function:test"
        });
        let _: CloudWatchLogsPutSubscriptionFilterInput = serde_json::from_value(json).unwrap();
    }
}
