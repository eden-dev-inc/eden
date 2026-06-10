use crate::api::lib::MongoApi;
use crate::api::wrapper::DatabaseOptionsWrapper;
use crate::output::DatabaseOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::MongoAsync;
use mongo_core::MongoTx;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, DatabaseWithOptionsInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::DatabaseWithOptions(None),
    "Gets a handle to a database specified by name in the cluster the Client is connected to. Operations done with this Database will use the options specified by options by default and will otherwise default to those of the Client",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DatabaseWithOptionsInput {
        database: String,
        options: DatabaseOptionsWrapper,
    }
}

type OutputWrapper = DatabaseOutput;

impl_simple_operation!(DatabaseWithOptionsInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DatabaseWithOptionsInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("mongo.{}.{}", self.kind(), function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        let database = context.database_with_options(&self.database, self.options.to_owned().into());
        Ok(Box::new(DatabaseOutput(database).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        context.client().database_with_options(&self.database, self.options.to_owned().into());
    }
}
