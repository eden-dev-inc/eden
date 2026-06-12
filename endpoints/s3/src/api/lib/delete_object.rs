use crate::api::lib::S3Api;
use crate::output::S3DeleteObjectOutput;
use crate::request::S3Request;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use s3_core::{S3Async, S3Tx};
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<S3Api, DeleteObjectInput> =
    ApiInfo::new(EpKind::S3, S3Api::DeleteObject, "Delete an object from S3-compatible storage", ReqType::Write, true);

crate::s3_endpoint! {
    DeleteObject,
    API_INFO,
    struct {
        #[builder(default)]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bucket: Option<String>,
        key: String,
    }
}

impl_simple_operation!(SimpleInput, S3Async, S3Tx, S3Api, S3Request);

impl SimpleInput {
    async fn run_async_generic(&self, context: S3Async, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("s3.{}.run_async_generic", API_INFO.api()));
        let client = context.get().await.map_err(EpError::connect)?;
        let bucket = client.resolve_bucket(self.bucket().as_deref())?;
        client.delete_object(Some(bucket.as_str()), self.key()).await?;
        let output = S3DeleteObjectOutput {
            provider: client.provider(),
            bucket,
            key: self.key().clone(),
            deleted: true,
        };
        Ok(Box::new(output.to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, _context: &mut S3Tx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}
