use crate::api::lib::{DatabaseApi, GridfsBucketApi, MongoApi};
use crate::api::wrapper::GridFsBucketOptionsWrapper;
use crate::output::{GridfsBucketOutput, ReadConcernOutput};
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::GridFsBucket;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

// Declared for consistency with other endpoint modules
#[allow(dead_code)]
const REQUEST_TYPE: ReqType = ReqType::Read;

type PreviousOutput = GridFsBucket;

const API_INFO: ApiInfo<MongoApi, GridfsReadConcernInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::GridfsBucket(Some(GridfsBucketApi::ReadConcern)))),
    "Gets the read concern of the bucket",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct GridfsReadConcernInput {
        database: String,
        gridfs: Option<GridFsBucketOptionsWrapper>,
    }
}

type OutputWrapper = ReadConcernOutput;
type ExpectedInput = GridfsBucketOutput;

impl_simple_operation!(GridfsReadConcernInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl GridfsReadConcernInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_read_concern(&context.database(&self.database).gridfs_bucket(self.gridfs.to_owned().map(Into::into))).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_read_concern(&self, context: &GridFsBucket) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(ReadConcernOutput(context.read_concern().map(|r| r.to_owned())).to_output()) as Box<dyn EpOutput>)
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    #![allow(unexpected_cfgs)]
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_gridfs_read_concern_default() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.gridfs_read_concern().await;

        // Default connection has no explicit read concern set, so expect null or a valid object
        assert!(result.is_null() || result.is_object(), "result should be null or an object");

        ctx.stop().await;
    }
}
