use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentWrapperType, InsertOneOptionsWrapper};
use crate::output::{CollectionDocumentOutput, InsertOneResultOutput};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;

struct SimpleInsertOneModel;
struct ComplexInsertOneModel;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, InsertOneModelInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::InsertOneModel)))),
    "Creates an insert one model for bulk operations",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct InsertOneModelInput {
        database: String,
        collection: String,
        doc: DocumentWrapperType,
        options: Option<InsertOneOptionsWrapper>,
    }
}

type OutputWrapper = InsertOneResultOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(InsertOneModelInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl InsertOneModelInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_insert_one_model(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    #[allow(unreachable_code)]
    async fn run_insert_one_model(&self, _context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(InsertOneResultOutput(todo!("insert_one_model not implemented")).to_output()) as Box<dyn EpOutput>)
    }
}
