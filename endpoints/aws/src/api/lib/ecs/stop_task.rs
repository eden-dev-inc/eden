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

const API_INFO: ApiInfo<AwsApi, EcsStopTaskInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::EcsStopTask, "Stops an ECS task", ReqType::Write, true);

crate::aws_endpoint! {
    EcsStopTask,
    API_INFO,
    struct {
        task: String,
        cluster: Option<String>,
        reason: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_map = serde_json::Map::new();
        body_map.insert("task".to_string(), serde_json::json!(self.task));
        if let Some(c) = &self.cluster {
            body_map.insert("cluster".to_string(), serde_json::json!(c));
        }
        if let Some(r) = &self.reason {
            body_map.insert("reason".to_string(), serde_json::json!(r));
        }
        let body = serde_json::Value::Object(body_map);
        let result = client.execute("ecs", "POST", "/tasks/stop", None, Some(&body), None).await?;

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
        let input = EcsStopTaskInputBuilder::default().task("t").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ecs_stop_task");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"task": "t"});
        let _: EcsStopTaskInput = serde_json::from_value(json).unwrap();
    }
}
