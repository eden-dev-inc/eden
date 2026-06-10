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

const API_INFO: ApiInfo<AwsApi, PollySynthesizeSpeechInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::PollySynthesizeSpeech, "polly_synthesize_speech", ReqType::Write, true);

crate::aws_endpoint! {
    PollySynthesizeSpeech,
    API_INFO,
    struct {
        text: String,
        voice_id: String,
        output_format: String,
        engine: Option<String>,
        language_code: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"Text": self.text, "VoiceId": self.voice_id, "OutputFormat": self.output_format});
        let result = client.execute("polly", "POST", "/v1/speech", None, Some(&body_val), None).await?;

        span.add_event("received result from aws polly", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = PollySynthesizeSpeechInputBuilder::default()
            .text("Hello world")
            .voice_id("Joanna")
            .output_format("mp3")
            .engine(None::<String>)
            .language_code(None::<String>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "polly_synthesize_speech");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"text": "Hello world", "voice_id": "Joanna", "output_format": "mp3"});
        let _: PollySynthesizeSpeechInput = serde_json::from_value(json).unwrap();
    }
}
