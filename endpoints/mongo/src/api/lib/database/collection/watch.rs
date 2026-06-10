use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{ChangeStreamOptionsWrapper, DocumentFunction, DocumentWrapperType};
use crate::output::{ChangeStreamOutput, CollectionDocumentOutput};
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, CollectionWatchInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::Watch)))),
    "Starts a new ChangeStream that receives events for all changes in this collection. A ChangeStream cannot be started on system collections",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct CollectionWatchInput {
        database: String,
        collection: String,
        pipeline: Vec<DocumentWrapperType>,
        options: Option<ChangeStreamOptionsWrapper>,
    }
}

type OutputWrapper = ChangeStreamOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(CollectionWatchInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl CollectionWatchInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_watch(&context.database(&self.database)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_watch(&self, database: &Database) -> ResultEP<Box<dyn EpOutput>> {
        let _change_stream = database
            .collection::<Document>(&self.collection)
            .watch(
                self.pipeline.iter().cloned().map(DocumentFunction::into_document).collect::<Vec<Document>>(),
                self.options.to_owned().map(Into::into),
            )
            .await
            .map_err(EpError::database)?;

        todo!("return change stream");
        // Ok(ChangeStreamOutput(Mutex::new(change_stream).to_output()) as Box<dyn EpOutput>)
    }
}
