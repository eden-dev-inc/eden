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

const API_INFO: ApiInfo<AwsApi, CloudWatchDeleteAlarmsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::CloudWatchDeleteAlarms, "cloudwatch_delete_alarms", ReqType::Write, true);

crate::aws_endpoint! {
    CloudWatchDeleteAlarms,
    API_INFO,
    struct {
        alarm_names: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.extend(indexed_list_params("AlarmNames.member", &self.alarm_names));
        let form_body = build_query_body("DeleteAlarms", "2010-08-01", &params);
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
        let input = CloudWatchDeleteAlarmsInputBuilder::default().alarm_names(vec![]).build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudwatch_delete_alarms");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"alarm_names": []});
        let _: CloudWatchDeleteAlarmsInput = serde_json::from_value(json).unwrap();
    }
}
