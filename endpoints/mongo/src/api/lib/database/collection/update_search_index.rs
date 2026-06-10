use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, UpdateSearchIndexOptionsWrapper};
use crate::output::{CollectionDocumentOutput, EmptyOutput};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;

struct SimpleUpdateSearchIndex;
struct ComplexUpdateSearchIndex;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, UpdateSearchIndexInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::UpdateSearchIndex)))),
    "Updates the search index with the given name to use the provided definition",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct UpdateSearchIndexInput {
        database: String,
        collection: String,
        name: String,
        definition: DocumentWrapperType,
        options: Option<UpdateSearchIndexOptionsWrapper>,
    }
}

type OutputWrapper = EmptyOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(UpdateSearchIndexInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl UpdateSearchIndexInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_update_search_index(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_update_search_index(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            EmptyOutput(
                context
                    .update_search_index(&self.name, self.definition.to_owned().into_document(), self.options.to_owned().map(Into::into))
                    .await
                    .map_err(EpError::database)?,
            )
            .to_output(),
        ) as Box<dyn EpOutput>)
    }
}
