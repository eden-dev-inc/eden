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

const API_INFO: ApiInfo<AwsApi, TranscribeStartTranscriptionJobInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::TranscribeStartTranscriptionJob,
    "transcribe_start_transcription_job",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    TranscribeStartTranscriptionJob,
    API_INFO,
    struct {
        transcription_job_name: String,
        language_code: String,
        media_format: String,
        media: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "TranscriptionJobName": self.transcription_job_name,
            "LanguageCode": self.language_code,
            "MediaFormat": self.media_format,
            "Media": self.media
        });
        let result = client.execute_json_target("transcribe", "Transcribe.StartTranscriptionJob", Some(&body_val), "1.1").await?;

        span.add_event(
            "received result from aws transcribe",
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
        let input = TranscribeStartTranscriptionJobInputBuilder::default()
            .transcription_job_name("my-job")
            .language_code("en-US")
            .media_format("mp3")
            .media(serde_json::json!({"MediaFileUri": "s3://my-bucket/audio.mp3"}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "transcribe_start_transcription_job");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "transcription_job_name": "my-job",
            "language_code": "en-US",
            "media_format": "mp3",
            "media": {"MediaFileUri": "s3://my-bucket/audio.mp3"}
        });
        let _: TranscribeStartTranscriptionJobInput = serde_json::from_value(json).unwrap();
    }
}
