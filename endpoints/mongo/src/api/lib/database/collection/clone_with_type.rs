use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::output::CollectionDocumentOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, CloneWithTypeInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::CloneWithType)))),
    "Gets a clone of the Collection with a different type U",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct CloneWithTypeInput {
        database: String,
        collection: String,
    }
}

type OutputWrapper = CollectionDocumentOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(CloneWithTypeInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl CloneWithTypeInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_clone_with_type(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_clone_with_type(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(CollectionDocumentOutput(context.clone_with_type()).to_output()) as Box<dyn EpOutput>)
    }
}
