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

const API_INFO: ApiInfo<AwsApi, EmrServerlessStartJobRunInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::EmrServerlessStartJobRun, "emrserverless_start_job_run", ReqType::Write, true);

crate::aws_endpoint! {
    EmrServerlessStartJobRun,
    API_INFO,
    struct {
        application_id: String,
        execution_role_arn: String,
        job_driver: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/applications/{}/jobruns", self.application_id);
        let body_val = serde_json::json!({"executionRoleArn": self.execution_role_arn, "jobDriver": self.job_driver});
        let result = client.execute("emr-serverless", "POST", &path, None, Some(&body_val), None).await?;

        span.add_event(
            "received result from aws emr-serverless",
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
        let input = EmrServerlessStartJobRunInputBuilder::default()
            .application_id("app-abc123")
            .execution_role_arn("arn:aws:iam::123456789012:role/MyRole")
            .job_driver(serde_json::json!({"sparkSubmit": {"entryPoint": "s3://my-bucket/my-script.py"}}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "emrserverless_start_job_run");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"application_id": "app-abc123", "execution_role_arn": "arn:aws:iam::123456789012:role/MyRole", "job_driver": {}});
        let _: EmrServerlessStartJobRunInput = serde_json::from_value(json).unwrap();
    }
}
