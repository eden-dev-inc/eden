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

const API_INFO: ApiInfo<AwsApi, SageMakerCreateTransformJobInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SageMakerCreateTransformJob,
    "sagemaker_create_transform_job",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    SageMakerCreateTransformJob,
    API_INFO,
    struct {
        transform_job_name: String,
        model_name: String,
        transform_input: serde_json::Value,
        transform_output: serde_json::Value,
        transform_resources: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "TransformJobName": self.transform_job_name,
            "ModelName": self.model_name,
            "TransformInput": self.transform_input,
            "TransformOutput": self.transform_output,
            "TransformResources": self.transform_resources
        });
        let result = client.execute_json_target("sagemaker", "SageMaker.CreateTransformJob", Some(&body_val), "1.1").await?;

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
        let input = SageMakerCreateTransformJobInputBuilder::default()
            .transform_job_name("job")
            .model_name("model")
            .transform_input(serde_json::json!({"DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": "s3://bucket/input"}}}))
            .transform_output(serde_json::json!({"S3OutputPath": "s3://bucket/output"}))
            .transform_resources(serde_json::json!({"InstanceType": "ml.m5.xlarge", "InstanceCount": 1}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sagemaker_create_transform_job");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "transform_job_name": "job",
            "model_name": "model",
            "transform_input": {"DataSource": {}},
            "transform_output": {"S3OutputPath": "s3://bucket/output"},
            "transform_resources": {"InstanceType": "ml.m5.xlarge", "InstanceCount": 1}
        });
        let _: SageMakerCreateTransformJobInput = serde_json::from_value(json).unwrap();
    }
}
