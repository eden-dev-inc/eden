use crate::api::lib::AwsApi;
use crate::api::lib::params::{build_query_body, indexed_list_params};
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use std::collections::HashMap;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, CloudWatchDescribeAlarmsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::CloudWatchDescribeAlarms, "cloudwatch_describe_alarms", ReqType::Read, true);

crate::aws_endpoint! {
    CloudWatchDescribeAlarms,
    API_INFO,
    struct {
        alarm_names: Option<Vec<String>>,
        state_value: Option<String>,
        next_token: Option<String>,
        max_records: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        if let Some(names) = &self.alarm_names {
            params.extend(indexed_list_params("AlarmNames.member", names));
        }
        if let Some(sv) = &self.state_value {
            params.insert("StateValue".to_string(), sv.clone());
        }
        if let Some(token) = &self.next_token {
            params.insert("NextToken".to_string(), token.clone());
        }
        if let Some(max) = self.max_records {
            params.insert("MaxRecords".to_string(), max.to_string());
        }
        let form_body = build_query_body("DescribeAlarms", "2010-08-01", &params);
        let result = client.execute_form("cloudwatch", &form_body).await?;

        span.add_event(
            "received result from aws cloudwatch",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
        Ok(Box::new(AwsJsonOutput(Value::String(result)).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = CloudWatchDescribeAlarmsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudwatch_describe_alarms");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: CloudWatchDescribeAlarmsInput = serde_json::from_value(json).unwrap();
    }
}
