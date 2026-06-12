use crate::api::lib::{DatabaseApi, GridfsBucketApi, MongoApi};
use crate::api::wrapper::{DocumentWrapperType, GridFsBucketOptionsWrapper, GridFsFindOptionsWrapper};
use crate::output::{GridfsBucketOutput, VecFilesCollectionDocumentOutput};
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::GridFsBucket;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

type PreviousOutput = GridFsBucket;

const API_INFO: ApiInfo<MongoApi, FindOneInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::GridfsBucket(Some(GridfsBucketApi::FindOne)))),
    "Finds and returns a single FilesCollectionDocument within this bucket that matches the given filter",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct FindOneInput {
        database: String,
        gridfs: Option<GridFsBucketOptionsWrapper>,
        filter: DocumentWrapperType,
        options: Option<GridFsFindOptionsWrapper>,
    }
}

type OutputWrapper = VecFilesCollectionDocumentOutput;
type ExpectedInput = GridfsBucketOutput;

impl_simple_operation!(FindOneInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl FindOneInput {
    #[named]
    async fn run_async_generic(&self, _context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));
        todo!("find_one not implemented")
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_find_one(&self, _context: &GridFsBucket) -> ResultEP<Box<dyn EpOutput>> {
        todo!("implement find_one")
    }
}
