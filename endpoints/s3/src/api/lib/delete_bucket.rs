use crate::api::lib::S3Api;
use crate::output::S3BucketMutationOutput;
use crate::request::S3Request;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use s3_core::{S3Async, S3Tx};
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<S3Api, DeleteBucketInput> =
    ApiInfo::new(EpKind::S3, S3Api::DeleteBucket, "Delete an empty S3-compatible bucket", ReqType::Write, true);

crate::s3_endpoint! {
    DeleteBucket,
    API_INFO,
    struct {
        bucket: String,
    }
}

impl_simple_operation!(SimpleInput, S3Async, S3Tx, S3Api, S3Request);

impl SimpleInput {
    async fn run_async_generic(&self, context: S3Async, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("s3.{}.run_async_generic", API_INFO.api()));
        let client = context.get().await.map_err(EpError::connect)?;
        client.delete_bucket(self.bucket()).await?;
        let output = S3BucketMutationOutput {
            provider: client.provider(),
            bucket: self.bucket().clone(),
            success: true,
        };
        Ok(Box::new(output.to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, _context: &mut S3Tx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}
