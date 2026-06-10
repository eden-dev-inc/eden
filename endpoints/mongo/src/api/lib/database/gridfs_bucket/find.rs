use crate::api::lib::{DatabaseApi, GridfsBucketApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, GridFsBucketOptionsWrapper, GridFsFindOptionsWrapper};
use crate::output::{GridfsBucketOutput, VecFilesCollectionDocumentOutput};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use futures_util::TryStreamExt;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::GridFsBucket;
use telemetry::TelemetryWrapper;

struct SimpleGridFind;
struct ComplexGridFind;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, FindInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::GridfsBucket(Some(GridfsBucketApi::Find)))),
    "Finds and returns the FilesCollectionDocuments within this bucket that match the given filter",
    ReqType::Read,
    true,
);

type PreviousOutput = GridFsBucket;

crate::mongo_endpoint! {
    API_INFO,
    struct FindInput {
        database: String,
        gridfs: Option<GridFsBucketOptionsWrapper>,
        filter: DocumentWrapperType,
        options: Option<GridFsFindOptionsWrapper>,
    }
}

type OutputWrapper = VecFilesCollectionDocumentOutput;
type ExpectedInput = GridfsBucketOutput;

impl_simple_operation!(FindInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl FindInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_find(&context.database(&self.database).gridfs_bucket(self.gridfs.to_owned().map(Into::into))).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_find(&self, context: &GridFsBucket) -> ResultEP<Box<dyn EpOutput>> {
        let mut cursor = context
            .find(self.filter.to_owned().into_document(), self.options.to_owned().map(Into::into))
            .await
            .map_err(EpError::database)?;
        let mut results = vec![];
        while let Some(doc) = cursor.try_next().await.map_err(|e| EpError::request(e.to_string()))? {
            results.push(doc)
        }

        Ok(Box::new(VecFilesCollectionDocumentOutput(results).to_output()) as Box<dyn EpOutput>)
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
    async fn test_gridfs_find_empty() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.gridfs_find(doc! {}).await;
        let arr = result.as_array().expect("gridfs_find should return an array");
        assert_eq!(arr.len(), 0, "find on empty bucket should return empty array");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_gridfs_find_by_filename() {
        let mut ctx = MongoTestContext::new().await;

        let file_id = ObjectId::new();
        ctx.insert_one(
            "fs.files",
            doc! {
                "_id": file_id,
                "filename": "test_file.txt",
                "length": 1024_i64,
                "chunkSize": 261120,
                "uploadDate": DateTime::now(),
            },
        )
        .await;

        let result = ctx.gridfs_find(doc! { "filename": "test_file.txt" }).await;
        let arr = result.as_array().expect("gridfs_find should return an array");
        assert_eq!(arr.len(), 1, "should find the inserted file by filename");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_gridfs_find_all() {
        let mut ctx = MongoTestContext::new().await;

        for i in 0..3 {
            let file_id = ObjectId::new();
            ctx.insert_one(
                "fs.files",
                doc! {
                    "_id": file_id,
                    "filename": format!("file_{}.txt", i),
                    "length": 512_i64,
                    "chunkSize": 261120,
                    "uploadDate": DateTime::now(),
                },
            )
            .await;
        }

        let result = ctx.gridfs_find(doc! {}).await;
        let arr = result.as_array().expect("gridfs_find should return an array");
        assert_eq!(arr.len(), 3, "should find all 3 inserted file records");

        ctx.stop().await;
    }
}
