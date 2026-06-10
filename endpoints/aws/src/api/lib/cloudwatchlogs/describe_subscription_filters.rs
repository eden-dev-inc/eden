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

const API_INFO: ApiInfo<AwsApi, CloudWatchLogsDescribeSubscriptionFiltersInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CloudWatchLogsDescribeSubscriptionFilters,
    "cloudwatchlogs_describe_subscription_filters",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    CloudWatchLogsDescribeSubscriptionFilters,
    API_INFO,
    struct {
        log_group_name: String,
        filter_name_prefix: Option<String>,
        next_token: Option<String>,
        limit: Option<i64>
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
        if let Some(v) = &self.filter_name_prefix {
            body.insert("FilterNamePrefix".to_string(), Value::String(v.clone()));
        }
        if let Some(v) = &self.next_token {
            body.insert("NextToken".to_string(), Value::String(v.clone()));
        }
        if let Some(v) = self.limit {
            body.insert("Limit".to_string(), serde_json::json!(v));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("logs", "Logs_20140328.DescribeSubscriptionFilters", Some(&body_val), "1.1").await?;

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
        let input = CloudWatchLogsDescribeSubscriptionFiltersInputBuilder::default().log_group_name("test").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudwatchlogs_describe_subscription_filters");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"log_group_name": "test"});
        let _: CloudWatchLogsDescribeSubscriptionFiltersInput = serde_json::from_value(json).unwrap();
    }
}
