use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::output::ClientOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, CollectionClientInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::CollectionClient)))),
    "Get the Client that this collection descended from",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct CollectionClientInput {
        database: String,
        collection: String,
    }
}

impl_simple_operation!(CollectionClientInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl CollectionClientInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        Ok(self.run_client(&context.database(&self.database)).await)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_client(&self, database: &Database) -> Box<dyn EpOutput> {
        Box::new(ClientOutput(database.collection::<Document>(&self.collection).client().clone()).to_output()).as_output()
    }
}
