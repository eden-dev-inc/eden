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

const API_INFO: ApiInfo<AwsApi, ComprehendBatchDetectSentimentInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ComprehendBatchDetectSentiment,
    "comprehend_batch_detect_sentiment",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    ComprehendBatchDetectSentiment,
    API_INFO,
    struct {
        text_list: Vec<String>,
        language_code: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "TextList": self.text_list,
            "LanguageCode": self.language_code
        });
        let result = client.execute_json_target("comprehend", "Comprehend_20171127.BatchDetectSentiment", Some(&body_val), "1.1").await?;

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
        let input = ComprehendBatchDetectSentimentInputBuilder::default()
            .text_list(vec!["I love this!".to_string(), "This is terrible.".to_string()])
            .language_code("en")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "comprehend_batch_detect_sentiment");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "text_list": ["I love this!", "This is terrible."],
            "language_code": "en"
        });
        let _: ComprehendBatchDetectSentimentInput = serde_json::from_value(json).unwrap();
    }
}
