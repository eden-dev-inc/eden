use crate::api::lib::{DatabaseApi, GridfsBucketApi, MongoApi};
use crate::api::wrapper::{BsonWrapper, GridFsBucketOptionsWrapper};
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

const API_INFO: ApiInfo<MongoApi, DeleteInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::GridfsBucket(Some(GridfsBucketApi::Delete)))),
    "Deletes the FilesCollectionDocument with the given id and its associated chunks from this bucket. This method returns an error if the id does not match any files in the bucket",
    ReqType::Write,
    true,
);

type PreviousOutput = GridFsBucket;

crate::mongo_endpoint! {
    API_INFO,
    struct DeleteInput {
        database: String,
        gridfs: Option<GridFsBucketOptionsWrapper>,
        id: BsonWrapper,
    }
}

type OutputWrapper = EmptyOutput;
type ExpectedInput = GridfsBucketOutput;

impl_simple_operation!(DeleteInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DeleteInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_delete(&context.database(&self.database).gridfs_bucket(self.gridfs.to_owned().map(Into::into))).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_delete(&self, context: &GridFsBucket) -> ResultEP<Box<dyn EpOutput>> {
        Ok(
            Box::new(EmptyOutput(context.delete(self.id.to_owned().into()).await.map_err(EpError::database)?).to_output())
                as Box<dyn EpOutput>,
        )
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    #![allow(unexpected_cfgs)]
    use crate::api::wrapper::{BsonWrapper, ObjectIdWrapper};
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::{DateTime, doc, oid::ObjectId};
    use serial_test::serial;

    /// Helper to create a `BsonWrapper::ObjectId` from a raw `ObjectId`.
    fn bson_oid(oid: ObjectId) -> BsonWrapper {
        let wrapper: ObjectIdWrapper =
            serde_json::from_value(serde_json::json!({ "id": oid.bytes() })).expect("failed to deserialize ObjectIdWrapper");
        BsonWrapper::ObjectId(wrapper)
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_gridfs_delete_basic() {
        let mut ctx = MongoTestContext::new().await;

        let file_id = ObjectId::new();
        ctx.insert_one(
            "fs.files",
            doc! {
                "_id": file_id,
                "filename": "delete_me.txt",
                "length": 1024_i64,
                "chunkSize": 261120,
                "uploadDate": DateTime::now(),
            },
        )
        .await;

        // Verify file exists
        let before = ctx.gridfs_find(doc! { "filename": "delete_me.txt" }).await;
        let arr = before.as_array().expect("gridfs_find should return an array");
        assert_eq!(arr.len(), 1, "file should exist before deletion");

        // Delete the file
        ctx.gridfs_delete(bson_oid(file_id)).await;

        // Verify file is gone
        let after = ctx.gridfs_find(doc! { "filename": "delete_me.txt" }).await;
        let arr = after.as_array().expect("gridfs_find should return an array");
        assert_eq!(arr.len(), 0, "file should be gone after deletion");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_gridfs_delete_nonexistent() {
        let mut ctx = MongoTestContext::new().await;

        let nonexistent_id = ObjectId::new();
        let result = ctx.gridfs_delete_result(bson_oid(nonexistent_id)).await;
        assert!(result.is_err(), "deleting a non-existent file should return an error");

        ctx.stop().await;
    }
}
