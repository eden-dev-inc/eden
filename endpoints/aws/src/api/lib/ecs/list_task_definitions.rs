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

const API_INFO: ApiInfo<AwsApi, EcsListTaskDefinitionsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::EcsListTaskDefinitions, "Lists ECS task definitions", ReqType::Read, true);

crate::aws_endpoint! {
    EcsListTaskDefinitions,
    API_INFO,
    struct {
        family_prefix: Option<String>,
        status: Option<String>,
        max_results: Option<i64>,
        next_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_map = serde_json::Map::new();
        if let Some(fp) = &self.family_prefix {
            body_map.insert("familyPrefix".to_string(), serde_json::json!(fp));
        }
        if let Some(s) = &self.status {
            body_map.insert("status".to_string(), serde_json::json!(s));
        }
        if let Some(mr) = self.max_results {
            body_map.insert("maxResults".to_string(), serde_json::json!(mr));
        }
        if let Some(nt) = &self.next_token {
            body_map.insert("nextToken".to_string(), serde_json::json!(nt));
        }
        let body = serde_json::Value::Object(body_map);
        let result = client.execute("ecs", "POST", "/task-definitions/list", None, Some(&body), None).await?;

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
        let input = EcsListTaskDefinitionsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ecs_list_task_definitions");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: EcsListTaskDefinitionsInput = serde_json::from_value(json).unwrap();
    }
}
