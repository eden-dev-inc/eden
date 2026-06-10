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

const API_INFO: ApiInfo<AwsApi, RekognitionIndexFacesInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::RekognitionIndexFaces, "rekognition_index_faces", ReqType::Write, true);

crate::aws_endpoint! {
    RekognitionIndexFaces,
    API_INFO,
    struct {
        collection_id: String,
        image: serde_json::Value,
        external_image_id: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("CollectionId".to_string(), Value::String(self.collection_id.clone()));
        body.insert("Image".to_string(), self.image.clone());
        if let Some(ext_id) = &self.external_image_id {
            body.insert("ExternalImageId".to_string(), Value::String(ext_id.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("rekognition", "RekognitionService.IndexFaces", Some(&body_val), "1.1").await?;

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
        let input = RekognitionIndexFacesInputBuilder::default()
            .collection_id("my-collection")
            .image(serde_json::json!({"S3Object": {"Bucket": "my-bucket", "Name": "image.jpg"}}))
            .external_image_id(None::<String>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "rekognition_index_faces");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "collection_id": "my-collection",
            "image": {"S3Object": {"Bucket": "my-bucket", "Name": "image.jpg"}}
        });
        let _: RekognitionIndexFacesInput = serde_json::from_value(json).unwrap();
    }
}
