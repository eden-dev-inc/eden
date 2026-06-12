use crate::api::lib::{DatabaseApi, GridfsBucketApi, MongoApi};
use crate::api::wrapper::GridFsBucketOptionsWrapper;
use crate::output::{EmptyOutput, GridfsBucketOutput};
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

type PreviousOutput = GridFsBucket;

const API_INFO: ApiInfo<MongoApi, DropInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::GridfsBucket(Some(GridfsBucketApi::Drop)))),
    "Removes all of the files and their associated chunks from this bucket",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DropInput {
        database: String,
        gridfs: Option<GridFsBucketOptionsWrapper>,
    }
}

type OutputWrapper = EmptyOutput;
type ExpectedInput = GridfsBucketOutput;

impl_simple_operation!(DropInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DropInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_drop(&context.database(&self.database).gridfs_bucket(self.gridfs.to_owned().map(Into::into))).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_drop(&self, context: &GridFsBucket) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(EmptyOutput(context.drop().await.map_err(EpError::database)?).to_output()) as Box<dyn EpOutput>)
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    #![allow(unexpected_cfgs)]
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::{DateTime, doc, oid::ObjectId};
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_gridfs_drop_basic() {
        let mut ctx = MongoTestContext::new().await;

        for i in 0..3 {
            let file_id = ObjectId::new();
            ctx.insert_one(
                "fs.files",
                doc! {
                    "_id": file_id,
                    "filename": format!("drop_file_{}.txt", i),
                    "length": 256_i64,
                    "chunkSize": 261120,
                    "uploadDate": DateTime::now(),
                },
            )
            .await;
        }

        // Verify files exist
        let before = ctx.gridfs_find(doc! {}).await;
        let arr = before.as_array().expect("gridfs_find should return an array");
        assert_eq!(arr.len(), 3, "should have 3 files before drop");

        // Drop the bucket
        ctx.gridfs_drop().await;

        // Verify files are gone
        let after = ctx.gridfs_find(doc! {}).await;
        let arr = after.as_array().expect("gridfs_find should return an array");
        assert_eq!(arr.len(), 0, "bucket should be empty after drop");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_gridfs_drop_empty() {
        let mut ctx = MongoTestContext::new().await;

        // Dropping an empty bucket should succeed without error
        ctx.gridfs_drop().await;

        ctx.stop().await;
    }
}
