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

const API_INFO: ApiInfo<AwsApi, EcsDescribeTasksInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::EcsDescribeTasks, "Describes ECS tasks", ReqType::Read, true);

crate::aws_endpoint! {
    EcsDescribeTasks,
    API_INFO,
    struct {
        cluster: Option<String>,
        tasks: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_map = serde_json::Map::new();
        body_map.insert("tasks".to_string(), serde_json::json!(self.tasks));
        if let Some(c) = &self.cluster {
            body_map.insert("cluster".to_string(), serde_json::json!(c));
        }
        let body = serde_json::Value::Object(body_map);
        let result = client.execute("ecs", "POST", "/tasks/describe", None, Some(&body), None).await?;

        span.add_event("received result from aws ecs", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = EcsDescribeTasksInputBuilder::default().tasks(vec![]).build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ecs_describe_tasks");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"tasks": []});
        let _: EcsDescribeTasksInput = serde_json::from_value(json).unwrap();
    }
}
