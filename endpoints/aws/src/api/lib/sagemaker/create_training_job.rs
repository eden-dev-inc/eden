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

const API_INFO: ApiInfo<AwsApi, SageMakerCreateTrainingJobInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SageMakerCreateTrainingJob,
    "sagemaker_create_training_job",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    SageMakerCreateTrainingJob,
    API_INFO,
    struct {
        training_job_name: String,
        algorithm_specification: serde_json::Value,
        role_arn: String,
        output_data_config: serde_json::Value,
        resource_config: serde_json::Value,
        stopping_condition: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "TrainingJobName": self.training_job_name,
            "AlgorithmSpecification": self.algorithm_specification,
            "RoleArn": self.role_arn,
            "OutputDataConfig": self.output_data_config,
            "ResourceConfig": self.resource_config,
            "StoppingCondition": self.stopping_condition
        });
        let result = client.execute_json_target("sagemaker", "SageMaker.CreateTrainingJob", Some(&body_val), "1.1").await?;

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
        let input = SageMakerCreateTrainingJobInputBuilder::default()
            .training_job_name("job")
            .algorithm_specification(serde_json::json!({"TrainingImage": "img", "TrainingInputMode": "File"}))
            .role_arn("arn:aws:iam::123456789012:role/role")
            .output_data_config(serde_json::json!({"S3OutputPath": "s3://bucket/output"}))
            .resource_config(serde_json::json!({"InstanceType": "ml.m5.xlarge", "InstanceCount": 1, "VolumeSizeInGB": 10}))
            .stopping_condition(serde_json::json!({"MaxRuntimeInSeconds": 3600}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sagemaker_create_training_job");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "training_job_name": "job",
            "algorithm_specification": {"TrainingImage": "img", "TrainingInputMode": "File"},
            "role_arn": "arn:aws:iam::123456789012:role/role",
            "output_data_config": {"S3OutputPath": "s3://bucket/output"},
            "resource_config": {"InstanceType": "ml.m5.xlarge", "InstanceCount": 1, "VolumeSizeInGB": 10},
            "stopping_condition": {"MaxRuntimeInSeconds": 3600}
        });
        let _: SageMakerCreateTrainingJobInput = serde_json::from_value(json).unwrap();
    }
}
