use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, DataSyncStartTaskExecutionInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::DataSyncStartTaskExecution,
    "datasync_start_task_execution",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    DataSyncStartTaskExecution,
    API_INFO,
    struct {
        task_arn: String,
        override_options: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;
        let mut body_map = serde_json::Map::new();
        body_map.insert("TaskArn".to_string(), serde_json::json!(self.task_arn));
        let body = serde_json::Value::Object(body_map);
        let result = client.execute_json_target("datasync", "FrontendService.StartTaskExecution", Some(&body), "1.1").await?;
        span.add_event("received result from aws datasync", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = DataSyncStartTaskExecutionInputBuilder::default()
            .task_arn("arn:aws:datasync:us-east-1:123456789012:task/t-abc")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "datasync_start_task_execution");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"task_arn": "arn:aws:datasync:us-east-1:123456789012:task/t-abc"});
        let _: DataSyncStartTaskExecutionInput = serde_json::from_value(json).unwrap();
    }
}
