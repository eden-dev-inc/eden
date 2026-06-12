use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::DropSearchIndexOptionsWrapper;
use crate::output::EmptyOutput;
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

const API_INFO: ApiInfo<MongoApi, DropSearchIndexInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::DropSearchIndex)))),
    "Drops the search index with the given name",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DropSearchIndexInput {
        database: String,
        collection: String,
        name: String,
        options: Option<DropSearchIndexOptionsWrapper>,
    }
}

impl_simple_operation!(DropSearchIndexInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DropSearchIndexInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("mongo.{}.{}", self.kind(), function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_drop_search_index(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_drop_search_index(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            EmptyOutput(context.drop_search_index(&self.name, self.options.to_owned().map(Into::into)).await.map_err(EpError::database)?)
                .to_output(),
        ) as Box<dyn EpOutput>)
    }
}
