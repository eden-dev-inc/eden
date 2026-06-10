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

const API_INFO: ApiInfo<AwsApi, SageMakerListModelsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SageMakerListModels, "sagemaker_list_models", ReqType::Read, true);

crate::aws_endpoint! {
    SageMakerListModels,
    API_INFO,
    struct {
        max_results: Option<i64>,
        next_token: Option<String>,
        sort_by: Option<String>,
        sort_order: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        if let Some(max) = self.max_results {
            body.insert("MaxResults".to_string(), serde_json::Value::Number(max.into()));
        }
        if let Some(token) = &self.next_token {
            body.insert("NextToken".to_string(), serde_json::Value::String(token.clone()));
        }
        if let Some(sb) = &self.sort_by {
            body.insert("SortBy".to_string(), serde_json::Value::String(sb.clone()));
        }
        if let Some(so) = &self.sort_order {
            body.insert("SortOrder".to_string(), serde_json::Value::String(so.clone()));
        }
        let body_val = serde_json::Value::Object(body);
        let result = client.execute_json_target("sagemaker", "SageMaker.ListModels", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws sagemaker", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SageMakerListModelsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sagemaker_list_models");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: SageMakerListModelsInput = serde_json::from_value(json).unwrap();
    }
}
