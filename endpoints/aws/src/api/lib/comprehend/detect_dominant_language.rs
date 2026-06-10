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

const API_INFO: ApiInfo<AwsApi, ComprehendDetectDominantLanguageInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ComprehendDetectDominantLanguage,
    "comprehend_detect_dominant_language",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    ComprehendDetectDominantLanguage,
    API_INFO,
    struct {
        text: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "Text": self.text
        });
        let result = client.execute_json_target("comprehend", "Comprehend_20171127.DetectDominantLanguage", Some(&body_val), "1.1").await?;

        span.add_event(
            "received result from aws comprehend",
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
        let input = ComprehendDetectDominantLanguageInputBuilder::default().text("Bonjour le monde").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "comprehend_detect_dominant_language");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "text": "Bonjour le monde"
        });
        let _: ComprehendDetectDominantLanguageInput = serde_json::from_value(json).unwrap();
    }
}
