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

const API_INFO: ApiInfo<AwsApi, EcsRunTaskInput> = ApiInfo::new(EpKind::Aws, AwsApi::EcsRunTask, "Runs an ECS task", ReqType::Write, true);

crate::aws_endpoint! {
    EcsRunTask,
    API_INFO,
    struct {
        task_definition: String,
        cluster: Option<String>,
        count: Option<i64>,
        launch_type: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_map = serde_json::Map::new();
        body_map.insert("taskDefinition".to_string(), serde_json::json!(self.task_definition));
        if let Some(c) = &self.cluster {
            body_map.insert("cluster".to_string(), serde_json::json!(c));
        }
        if let Some(n) = self.count {
            body_map.insert("count".to_string(), serde_json::json!(n));
        }
        if let Some(l) = &self.launch_type {
            body_map.insert("launchType".to_string(), serde_json::json!(l));
        }
        let body = serde_json::Value::Object(body_map);
        let result = client.execute("ecs", "POST", "/tasks/run", None, Some(&body), None).await?;

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
        let input = EcsRunTaskInputBuilder::default().task_definition("td").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ecs_run_task");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"task_definition": "td"});
        let _: EcsRunTaskInput = serde_json::from_value(json).unwrap();
    }
}
