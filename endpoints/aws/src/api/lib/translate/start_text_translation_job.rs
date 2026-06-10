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
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, TranslateStartTextTranslationJobInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::TranslateStartTextTranslationJob,
    "translate_start_text_translation_job",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    TranslateStartTextTranslationJob,
    API_INFO,
    struct {
        job_name: Option<String>,
        input_data_config: serde_json::Value,
        output_data_config: serde_json::Value,
        data_access_role_arn: String,
        source_language_code: String,
        target_language_codes: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("InputDataConfig".to_string(), self.input_data_config.clone());
        body.insert("OutputDataConfig".to_string(), self.output_data_config.clone());
        body.insert("DataAccessRoleArn".to_string(), Value::String(self.data_access_role_arn.clone()));
        body.insert("SourceLanguageCode".to_string(), Value::String(self.source_language_code.clone()));
        body.insert("TargetLanguageCodes".to_string(), serde_json::json!(self.target_language_codes));
        if let Some(name) = &self.job_name {
            body.insert("JobName".to_string(), Value::String(name.clone()));
        }
        let body_val = Value::Object(body);
        let result = client
            .execute_json_target("translate", "AWSShineFrontendService_20170701.StartTextTranslationJob", Some(&body_val), "1.1")
            .await?;

        span.add_event("received result from aws translate", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = TranslateStartTextTranslationJobInputBuilder::default()
            .job_name(None::<String>)
            .input_data_config(serde_json::json!({"S3Uri": "s3://my-bucket/input/", "ContentType": "text/plain"}))
            .output_data_config(serde_json::json!({"S3Uri": "s3://my-bucket/output/"}))
            .data_access_role_arn("arn:aws:iam::123456789012:role/TranslateRole")
            .source_language_code("en")
            .target_language_codes(vec!["es".to_string(), "fr".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "translate_start_text_translation_job");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "input_data_config": {"S3Uri": "s3://my-bucket/input/", "ContentType": "text/plain"},
            "output_data_config": {"S3Uri": "s3://my-bucket/output/"},
            "data_access_role_arn": "arn:aws:iam::123456789012:role/TranslateRole",
            "source_language_code": "en",
            "target_language_codes": ["es", "fr"]
        });
        let _: TranslateStartTextTranslationJobInput = serde_json::from_value(json).unwrap();
    }
}
