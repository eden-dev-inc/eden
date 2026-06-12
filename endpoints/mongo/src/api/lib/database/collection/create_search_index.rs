use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{CreateSearchIndexOptionsWrapper, SearchIndexModelWrapper};
use crate::output::{CollectionDocumentOutput, StringOutput};
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

const API_INFO: ApiInfo<MongoApi, CreateSearchIndexInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::CreateSearchIndex)))),
    "Convenience method for creating a single search index",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct CreateSearchIndexInput {
        database: String,
        collection: String,
        model: SearchIndexModelWrapper,
        options: Option<CreateSearchIndexOptionsWrapper>,
    }
}

type OutputWrapper = StringOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(CreateSearchIndexInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl CreateSearchIndexInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_create_search_index(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_create_search_index(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            StringOutput(
                context
                    .create_search_index(self.model.to_owned().into(), self.options.to_owned().map(Into::into))
                    .await
                    .map_err(EpError::database)?,
            )
            .to_output(),
        ) as Box<dyn EpOutput>)
    }
}
