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

const API_INFO: ApiInfo<AwsApi, EcsRegisterTaskDefinitionInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::EcsRegisterTaskDefinition,
    "Registers an ECS task definition",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    EcsRegisterTaskDefinition,
    API_INFO,
    struct {
        family: String,
        container_definitions: serde_json::Value,
        requires_compatibilities: Option<Vec<String>>,
        cpu: Option<String>,
        memory: Option<String>,
        network_mode: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_map = serde_json::Map::new();
        body_map.insert("family".to_string(), serde_json::json!(self.family));
        body_map.insert("containerDefinitions".to_string(), self.container_definitions.clone());
        if let Some(rc) = &self.requires_compatibilities {
            body_map.insert("requiresCompatibilities".to_string(), serde_json::json!(rc));
        }
        if let Some(c) = &self.cpu {
            body_map.insert("cpu".to_string(), serde_json::json!(c));
        }
        if let Some(m) = &self.memory {
            body_map.insert("memory".to_string(), serde_json::json!(m));
        }
        if let Some(nm) = &self.network_mode {
            body_map.insert("networkMode".to_string(), serde_json::json!(nm));
        }
        let body = serde_json::Value::Object(body_map);
        let result = client.execute("ecs", "POST", "/task-definitions/register", None, Some(&body), None).await?;

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
        let input = EcsRegisterTaskDefinitionInputBuilder::default()
            .family("my-family")
            .container_definitions(serde_json::json!([{"name": "web", "image": "nginx"}]))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ecs_register_task_definition");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"family": "my-family", "container_definitions": [{"name": "web", "image": "nginx"}]});
        let _: EcsRegisterTaskDefinitionInput = serde_json::from_value(json).unwrap();
    }
}
