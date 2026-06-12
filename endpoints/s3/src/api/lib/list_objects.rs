use crate::api::lib::S3Api;
use crate::output::S3ListObjectsOutput;
use crate::request::S3Request;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use s3_core::{S3Async, S3Tx};
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<S3Api, ListObjectsInput> =
    ApiInfo::new(EpKind::S3, S3Api::ListObjects, "List objects in an S3-compatible bucket", ReqType::Read, true);

crate::s3_endpoint! {
    ListObjects,
    API_INFO,
    struct {
        #[builder(default)]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bucket: Option<String>,
        #[builder(default)]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        #[builder(default)]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        continuation_token: Option<String>,
        #[builder(default)]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_keys: Option<i32>,
    }
}

impl_simple_operation!(SimpleInput, S3Async, S3Tx, S3Api, S3Request);

impl SimpleInput {
    async fn run_async_generic(&self, context: S3Async, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("s3.{}.run_async_generic", API_INFO.api()));
        let client = context.get().await.map_err(EpError::connect)?;
        let output = client
            .list_objects(
                self.bucket().as_deref(),
                self.prefix().as_deref(),
                self.continuation_token().as_deref(),
                *self.max_keys(),
            )
            .await?;
        Ok(Box::new(S3ListObjectsOutput::from(output).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, _context: &mut S3Tx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}
