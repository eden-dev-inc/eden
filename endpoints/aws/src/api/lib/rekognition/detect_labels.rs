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

const API_INFO: ApiInfo<AwsApi, RekognitionDetectLabelsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::RekognitionDetectLabels, "rekognition_detect_labels", ReqType::Read, true);

crate::aws_endpoint! {
    RekognitionDetectLabels,
    API_INFO,
    struct {
        image: serde_json::Value,
        max_labels: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("Image".to_string(), self.image.clone());
        if let Some(max) = self.max_labels {
            body.insert("MaxLabels".to_string(), serde_json::json!(max));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("rekognition", "RekognitionService.DetectLabels", Some(&body_val), "1.1").await?;

        span.add_event(
            "received result from aws rekognition",
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
        let input = RekognitionDetectLabelsInputBuilder::default()
            .image(serde_json::json!({"S3Object": {"Bucket": "my-bucket", "Name": "image.jpg"}}))
            .max_labels(None::<i64>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "rekognition_detect_labels");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "image": {"S3Object": {"Bucket": "my-bucket", "Name": "image.jpg"}}
        });
        let _: RekognitionDetectLabelsInput = serde_json::from_value(json).unwrap();
    }
}
