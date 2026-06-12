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
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<AwsApi, CustomInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::Custom,
    "Executes a custom AWS API request with user-specified service, method, path, and optional body",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    Custom,
    API_INFO,
    struct {
        service: String,
        method: String,
        path: String,
        query: Option<String>,
        body: Option<Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;

        let result = client.execute(&self.service, &self.method, &self.path, self.query.as_deref(), self.body.as_ref(), None).await?;

        span.add_event("received result from aws", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(AwsJsonOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        // AWS does not support transactions (EpKind::Aws returns support_tx() == false).
        // This path is unreachable under normal operation.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_builder_serde() {
        let input = CustomInputBuilder::default()
            .service("ec2")
            .method("GET")
            .path("/")
            .query(Some("Action=DescribeInstances&Version=2016-11-15".to_string()))
            .body(None::<Value>)
            .build()
            .expect("Failed to build CustomInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "custom");
        assert_eq!(json["service"], "ec2");
        assert_eq!(json["method"], "GET");
    }

    #[test]
    fn custom_deserialize() {
        let json = serde_json::json!({
            "service": "s3",
            "method": "PUT",
            "path": "/my-bucket/my-key",
            "body": null
        });
        let input: CustomInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.service, "s3");
        assert_eq!(input.method, "PUT");
        assert_eq!(input.path, "/my-bucket/my-key");
    }
}
