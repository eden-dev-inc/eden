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

const API_INFO: ApiInfo<AwsApi, ProtonCreateEnvironmentInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::ProtonCreateEnvironment, "proton_create_environment", ReqType::Write, true);

crate::aws_endpoint! {
    ProtonCreateEnvironment,
    API_INFO,
    struct {
        name: String,
        spec: String,
        template_name: String,
        template_major_version: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "name": self.name,
            "spec": self.spec,
            "templateName": self.template_name,
            "templateMajorVersion": self.template_major_version
        });
        let result = client.execute("proton", "POST", "/environments", None, Some(&body), None).await?;

        span.add_event("received result from aws proton", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = ProtonCreateEnvironmentInputBuilder::default()
            .name("my-env")
            .spec("spec-content")
            .template_name("my-template")
            .template_major_version("1")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "proton_create_environment");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "name": "my-env",
            "spec": "spec-content",
            "template_name": "my-template",
            "template_major_version": "1"
        });
        let _: ProtonCreateEnvironmentInput = serde_json::from_value(json).unwrap();
    }
}
