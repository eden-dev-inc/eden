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

const API_INFO: ApiInfo<AwsApi, ElasticTranscoderCreatePipelineInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ElasticTranscoderCreatePipeline,
    "elastictranscoder_create_pipeline",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    ElasticTranscoderCreatePipeline,
    API_INFO,
    struct {
        name: String,
        input_bucket: String,
        role: String,
        output_bucket: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "Name": self.name,
            "InputBucket": self.input_bucket,
            "Role": self.role
        });
        let result = client.execute("elastictranscoder", "POST", "/2012-09-25/pipelines", None, Some(&body), None).await?;

        span.add_event(
            "received result from aws elastictranscoder",
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
        let input = ElasticTranscoderCreatePipelineInputBuilder::default()
            .name("my-pipeline")
            .input_bucket("my-input-bucket")
            .role("arn:aws:iam::123456789012:role/role")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "elastictranscoder_create_pipeline");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "name": "my-pipeline",
            "input_bucket": "my-input-bucket",
            "role": "arn:aws:iam::123456789012:role/role"
        });
        let _: ElasticTranscoderCreatePipelineInput = serde_json::from_value(json).unwrap();
    }
}
