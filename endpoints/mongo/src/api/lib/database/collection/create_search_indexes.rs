use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{CreateSearchIndexOptionsWrapper, SearchIndexModelWrapper};
use crate::output::{CollectionDocumentOutput, VecStringOutput};
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::bson::Document;
use mongodb::{Collection, SearchIndexModel};
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, CreateSearchIndexesInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::CreateSearchIndexes)))),
    "Creates multiple search indexes on the collection",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct CreateSearchIndexesInput {
        database: String,
        collection: String,
        models: Vec<SearchIndexModelWrapper>,
        options: Option<CreateSearchIndexOptionsWrapper>,
    }
}

type OutputWrapper = VecStringOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(CreateSearchIndexesInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl CreateSearchIndexesInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_create_search_indexes(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_create_search_indexes(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            VecStringOutput(
                context
                    .create_search_indexes(
                        self.models.iter().cloned().map(Into::into).collect::<Vec<SearchIndexModel>>(),
                        self.options.to_owned().map(Into::into),
                    )
                    .await
                    .map_err(EpError::database)?,
            )
            .to_output(),
        ) as Box<dyn EpOutput>)
    }
}
