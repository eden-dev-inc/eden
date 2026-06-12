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

const API_INFO: ApiInfo<AwsApi, BedrockCreateModelCustomizationJobInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::BedrockCreateModelCustomizationJob,
    "bedrock_create_model_customization_job",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    BedrockCreateModelCustomizationJob,
    API_INFO,
    struct {
        job_name: String,
        base_model_identifier: String,
        training_data_config: serde_json::Value,
        output_data_config: serde_json::Value,
        role_arn: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "jobName": self.job_name,
            "baseModelIdentifier": self.base_model_identifier,
            "trainingDataConfig": self.training_data_config,
            "outputDataConfig": self.output_data_config,
            "roleArn": self.role_arn
        });
        let result = client.execute("bedrock", "POST", "/model-customization-jobs", None, Some(&body), None).await?;

        span.add_event("received result from aws bedrock", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = BedrockCreateModelCustomizationJobInputBuilder::default()
            .job_name("job")
            .base_model_identifier("model")
            .training_data_config(serde_json::json!({"s3Uri": "s3://bucket"}))
            .output_data_config(serde_json::json!({"s3Uri": "s3://bucket"}))
            .role_arn("arn:aws:iam::role")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "bedrock_create_model_customization_job");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "job_name": "job",
            "base_model_identifier": "model",
            "training_data_config": {"s3Uri": "s3://bucket"},
            "output_data_config": {"s3Uri": "s3://bucket"},
            "role_arn": "arn:aws:iam::role"
        });
        let _: BedrockCreateModelCustomizationJobInput = serde_json::from_value(json).unwrap();
    }
}
