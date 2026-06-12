use crate::api::lib::S3Api;
use crate::output::S3ListBucketsOutput;
use crate::request::S3Request;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use s3_core::{S3Async, S3Tx};
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<S3Api, ListBucketsInput> =
    ApiInfo::new(EpKind::S3, S3Api::ListBuckets, "List available S3-compatible buckets", ReqType::Read, true);

crate::s3_endpoint! {
    ListBuckets,
    API_INFO,
    struct {}
}

impl_simple_operation!(SimpleInput, S3Async, S3Tx, S3Api, S3Request);

impl SimpleInput {
    async fn run_async_generic(&self, context: S3Async, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("s3.{}.run_async_generic", API_INFO.api()));
        let client = context.get().await.map_err(EpError::connect)?;
        let output = client.list_buckets().await?;
        Ok(Box::new(S3ListBucketsOutput::from(output).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, _context: &mut S3Tx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}
