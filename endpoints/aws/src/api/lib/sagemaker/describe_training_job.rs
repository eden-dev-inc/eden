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

const API_INFO: ApiInfo<AwsApi, SageMakerDescribeTrainingJobInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SageMakerDescribeTrainingJob,
    "sagemaker_describe_training_job",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    SageMakerDescribeTrainingJob,
    API_INFO,
    struct {
        training_job_name: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("TrainingJobName".to_string(), Value::String(self.training_job_name.clone()));
        let body_val = Value::Object(body);
        let result = client.execute_json_target("sagemaker", "SageMaker.DescribeTrainingJob", Some(&body_val), "1.1").await?;

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
        let input = SageMakerDescribeTrainingJobInputBuilder::default().training_job_name("job").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sagemaker_describe_training_job");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"training_job_name": "job"});
        let _: SageMakerDescribeTrainingJobInput = serde_json::from_value(json).unwrap();
    }
}
