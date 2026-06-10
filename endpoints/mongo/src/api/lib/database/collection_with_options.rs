use crate::api::lib::{DatabaseApi, MongoApi};
use crate::api::wrapper::CollectionOptionsWrapper;
use crate::output::{CollectionDocumentOutput, DatabaseOutput};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use telemetry::TelemetryWrapper;

struct SimpleCollectionWithOptions;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, CollectionWithOptionsInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::CollectionWithOptions(None))),
    "Gets a handle to a collection in this database with the provided name. Operations done with this Collection will use the options specified by options and will otherwise default to those of this Database",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct CollectionWithOptionsInput {
        database: String,
        collection: String,
        options: CollectionOptionsWrapper,
    }
}

type OutputWrapper = CollectionDocumentOutput;
type ExpectedInput = DatabaseOutput;

impl_simple_operation!(CollectionWithOptionsInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl CollectionWithOptionsInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        Ok(Box::new(
            CollectionDocumentOutput(
                context.database(&self.database).collection_with_options(&self.collection, self.options.to_owned().into()),
            )
            .to_output(),
        ) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }

    async fn run_collection_with_options(&self, database: &Database) -> ResultEP<OutputWrapper> {
        let result = database.collection_with_options(&self.collection, self.options.to_owned().into());

        Ok(CollectionDocumentOutput(result))
    }
}
