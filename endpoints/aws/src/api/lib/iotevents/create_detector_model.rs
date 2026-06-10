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

const API_INFO: ApiInfo<AwsApi, IotEventsCreateDetectorModelInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::IotEventsCreateDetectorModel,
    "iotevents_create_detector_model",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    IotEventsCreateDetectorModel,
    API_INFO,
    struct {
        detector_model_name: String,
        detector_model_definition: serde_json::Value,
        role_arn: String,
        evaluation_method: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "detectorModelName": self.detector_model_name,
            "detectorModelDefinition": self.detector_model_definition,
            "roleArn": self.role_arn
        });
        let result = client.execute("iotevents", "POST", "/detector-models", None, Some(&body_val), None).await?;

        span.add_event("received result from aws iotevents", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = IotEventsCreateDetectorModelInputBuilder::default()
            .detector_model_name("my-model")
            .detector_model_definition(serde_json::json!({}))
            .role_arn("arn:aws:iam::123456789012:role/my-role")
            .evaluation_method(None::<String>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "iotevents_create_detector_model");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "detector_model_name": "my-model",
            "detector_model_definition": {},
            "role_arn": "arn:aws:iam::123456789012:role/my-role"
        });
        let _: IotEventsCreateDetectorModelInput = serde_json::from_value(json).unwrap();
    }
}
