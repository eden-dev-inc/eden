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

const API_INFO: ApiInfo<AwsApi, CloudWatchLogsDeleteRetentionPolicyInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CloudWatchLogsDeleteRetentionPolicy,
    "cloudwatchlogs_delete_retention_policy",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    CloudWatchLogsDeleteRetentionPolicy,
    API_INFO,
    struct {
        log_group_name: String
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
        let body_val = Value::Object(body);
        let result = client.execute_json_target("logs", "Logs_20140328.DeleteRetentionPolicy", Some(&body_val), "1.1").await?;

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
        let input = CloudWatchLogsDeleteRetentionPolicyInputBuilder::default().log_group_name("test").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudwatchlogs_delete_retention_policy");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"log_group_name": "test"});
        let _: CloudWatchLogsDeleteRetentionPolicyInput = serde_json::from_value(json).unwrap();
    }
}
