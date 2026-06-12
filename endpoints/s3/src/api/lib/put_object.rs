use crate::api::lib::S3Api;
use crate::output::{S3PayloadSchema, S3PutObjectOutput};
use crate::request::S3Request;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use s3_core::{S3Async, S3ObjectBody, S3PutObjectRequest, S3Tx};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<S3Api, PutObjectInput> =
    ApiInfo::new(EpKind::S3, S3Api::PutObject, "Upload an object to S3-compatible storage", ReqType::Write, true);

crate::s3_endpoint! {
    PutObject,
    API_INFO,
    struct {
        #[builder(default)]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bucket: Option<String>,
        key: String,
        #[builder(default)]
        #[serde(default)]
        body: S3PayloadSchema,
        #[builder(default)]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
        #[builder(default)]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<HashMap<String, String>>,
    }
}

impl_simple_operation!(SimpleInput, S3Async, S3Tx, S3Api, S3Request);

impl SimpleInput {
    async fn run_async_generic(&self, context: S3Async, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("s3.{}.run_async_generic", API_INFO.api()));
        let client = context.get().await.map_err(EpError::connect)?;
        let output = client
            .put_object(&S3PutObjectRequest {
                bucket: self.bucket().clone(),
                key: self.key().clone(),
                body: self.body().clone().into(),
                content_type: self.content_type().clone(),
                metadata: self.metadata().clone(),
            })
            .await?;
        Ok(Box::new(S3PutObjectOutput::from(output).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, _context: &mut S3Tx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

impl From<S3PayloadSchema> for S3ObjectBody {
    fn from(value: S3PayloadSchema) -> Self {
        match value {
            S3PayloadSchema::Empty => Self::Empty,
            S3PayloadSchema::Json(value) => Self::Json(value),
            S3PayloadSchema::Text(value) => Self::Text(value),
            S3PayloadSchema::Base64(value) => Self::Base64(value),
        }
    }
}
