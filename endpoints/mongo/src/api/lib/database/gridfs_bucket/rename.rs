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

const API_INFO: ApiInfo<MongoApi, GridfsRenameInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::GridfsBucket(Some(GridfsBucketApi::GridfsRename)))),
    "Renames the file with the given 'id' to the provided new_filename. This method returns an error if the id does not match any files in the bucket",
    ReqType::Write,
    true,
);

type PreviousOutput = GridFsBucket;

crate::mongo_endpoint! {
    API_INFO,
    struct GridfsRenameInput {
        database: String,
        gridfs: Option<GridFsBucketOptionsWrapper>,
        id: BsonWrapper,
        new_filename: String,
    }
}

type OutputWrapper = EmptyOutput;
type ExpectedInput = GridfsBucketOutput;

impl_simple_operation!(GridfsRenameInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl GridfsRenameInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_rename(&context.database(&self.database).gridfs_bucket(self.gridfs.to_owned().map(Into::into))).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_rename(&self, context: &GridFsBucket) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            EmptyOutput(context.rename(self.id.to_owned().into(), &self.new_filename).await.map_err(EpError::database)?).to_output(),
        ) as Box<dyn EpOutput>)
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
    async fn test_gridfs_rename_basic() {
        let mut ctx = MongoTestContext::new().await;

        let file_id = ObjectId::new();
        ctx.insert_one(
            "fs.files",
            doc! {
                "_id": file_id,
                "filename": "original_name.txt",
                "length": 1024_i64,
                "chunkSize": 261120,
                "uploadDate": DateTime::now(),
            },
        )
        .await;

        // Rename the file
        ctx.gridfs_rename(bson_oid(file_id), "renamed_file.txt".to_string()).await;

        // Verify the old filename no longer matches
        let old_result = ctx.gridfs_find(doc! { "filename": "original_name.txt" }).await;
        let arr = old_result.as_array().expect("gridfs_find should return an array");
        assert_eq!(arr.len(), 0, "old filename should no longer exist");

        // Verify the new filename matches
        let new_result = ctx.gridfs_find(doc! { "filename": "renamed_file.txt" }).await;
        let arr = new_result.as_array().expect("gridfs_find should return an array");
        assert_eq!(arr.len(), 1, "renamed file should be found with the new filename");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_gridfs_rename_verify_old_gone() {
        let mut ctx = MongoTestContext::new().await;

        let file_id = ObjectId::new();
        ctx.insert_one(
            "fs.files",
            doc! {
                "_id": file_id,
                "filename": "before_rename.txt",
                "length": 512_i64,
                "chunkSize": 261120,
                "uploadDate": DateTime::now(),
            },
        )
        .await;

        ctx.gridfs_rename(bson_oid(file_id), "after_rename.txt".to_string()).await;

        // Old name should not be findable
        let old = ctx.gridfs_find(doc! { "filename": "before_rename.txt" }).await;
        let old_arr = old.as_array().expect("gridfs_find should return an array");
        assert_eq!(old_arr.len(), 0, "old filename should no longer exist after rename");

        // New name should be findable
        let new = ctx.gridfs_find(doc! { "filename": "after_rename.txt" }).await;
        let new_arr = new.as_array().expect("gridfs_find should return an array");
        assert_eq!(new_arr.len(), 1, "new filename should be findable after rename");

        ctx.stop().await;
    }
}
