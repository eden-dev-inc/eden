use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{AggregateOptionsWrapper, ListSearchIndexOptionsWrapper};
use crate::output::{CollectionDocumentOutput, VecDocumentOutput};
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use futures_util::TryStreamExt;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, ListSearchIndexesInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::ListSearchIndexes)))),
    "Gets index information for one or more search indexes in the collection",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct ListSearchIndexesInput {
        database: String,
        collection: String,
        name: Option<String>,
        aggregation_options: Option<AggregateOptionsWrapper>,
        list_index_options: Option<ListSearchIndexOptionsWrapper>,
    }
}

type OutputWrapper = VecDocumentOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(ListSearchIndexesInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl ListSearchIndexesInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_list_search_indexes(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_list_search_indexes(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        let mut cursor = context
            .list_search_indexes(
                self.name.as_deref(),
                self.aggregation_options.to_owned().map(Into::into),
                self.list_index_options.to_owned().map(Into::into),
            )
            .await
            .map_err(EpError::database)?;
        let mut results = vec![];
        while let Some(index) = cursor.try_next().await.map_err(EpError::request)? {
            results.push(index)
        }

        Ok(Box::new(VecDocumentOutput(results).to_output()) as Box<dyn EpOutput>)
    }
}
