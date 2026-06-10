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

const API_INFO: ApiInfo<AwsApi, SageMakerCreateProcessingJobInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SageMakerCreateProcessingJob,
    "sagemaker_create_processing_job",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    SageMakerCreateProcessingJob,
    API_INFO,
    struct {
        processing_job_name: String,
        role_arn: String,
        app_specification: serde_json::Value,
        processing_resources: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "ProcessingJobName": self.processing_job_name,
            "RoleArn": self.role_arn,
            "AppSpecification": self.app_specification,
            "ProcessingResources": self.processing_resources
        });
        let result = client.execute_json_target("sagemaker", "SageMaker.CreateProcessingJob", Some(&body_val), "1.1").await?;

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
        let input = SageMakerCreateProcessingJobInputBuilder::default()
            .processing_job_name("job")
            .role_arn("arn:aws:iam::123456789012:role/role")
            .app_specification(serde_json::json!({"ImageUri": "img"}))
            .processing_resources(
                serde_json::json!({"ClusterConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 10}}),
            )
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sagemaker_create_processing_job");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "processing_job_name": "job",
            "role_arn": "arn:aws:iam::123456789012:role/role",
            "app_specification": {"ImageUri": "img"},
            "processing_resources": {"ClusterConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 10}}
        });
        let _: SageMakerCreateProcessingJobInput = serde_json::from_value(json).unwrap();
    }
}
