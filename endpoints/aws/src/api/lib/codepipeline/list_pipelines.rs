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

const API_INFO: ApiInfo<AwsApi, CodePipelineListPipelinesInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::CodePipelineListPipelines, "codepipeline_list_pipelines", ReqType::Read, true);

crate::aws_endpoint! {
    CodePipelineListPipelines,
    API_INFO,
    struct {
        next_token: Option<String>,
        max_results: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        if let Some(n) = &self.next_token {
            body.insert("nextToken".to_string(), Value::String(n.clone()));
        }
        if let Some(m) = self.max_results {
            body.insert("maxResults".to_string(), serde_json::json!(m));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("codepipeline", "CodePipeline_20150709.ListPipelines", Some(&body_val), "1.1").await?;

        span.add_event(
            "received result from aws codepipeline",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = CodePipelineListPipelinesInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "codepipeline_list_pipelines");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: CodePipelineListPipelinesInput = serde_json::from_value(json).unwrap();
    }
}
