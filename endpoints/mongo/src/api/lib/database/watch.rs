use crate::api::lib::{DatabaseApi, MongoApi};
use crate::api::wrapper::{ChangeStreamOptionsWrapper, DocumentFunction, DocumentWrapperType};
use crate::output::{ChangeStreamOutput, DatabaseOutput};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;
use tokio::sync::Mutex;

pub struct SimpleWatch;
pub struct ComplexWatch;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, DatabaseWatchInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Watch)),
    "Starts a new ChangeStream that receives events for all changes in this database. The stream does not observe changes from system collections",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DatabaseWatchInput {
        database: String,
        pipeline: Vec<DocumentWrapperType>,
        options: Option<ChangeStreamOptionsWrapper>,
    }
}

type OutputWrapper = ChangeStreamOutput;
type ExpectedInput = DatabaseOutput;

impl_simple_operation!(DatabaseWatchInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DatabaseWatchInput {
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
    async fn run_watch(&self, context: &Database) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            ChangeStreamOutput(Mutex::new(
                context
                    .watch(
                        self.pipeline.clone().into_iter().map(DocumentFunction::into_document).collect::<Vec<Document>>(),
                        self.options.clone().map(Into::into),
                    )
                    .await
                    .map_err(EpError::database)?,
            ))
            .to_output(),
        ) as Box<dyn EpOutput>)
    }
}
