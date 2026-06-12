use crate::api::lib::MongoApi;
use crate::output::DatabaseOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use std::borrow::Cow;
use telemetry::FastSpanStatus;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<MongoApi, DefaultDatabaseInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::DefaultDatabase(None),
    "Gets a handle to the default database specified in the ClientOptions or MongoDB connection string used to construct this Client",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DefaultDatabaseInput {}
}

type OutputWrapper = DatabaseOutput;

impl_simple_operation!(DefaultDatabaseInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DefaultDatabaseInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        let database = match context.default_database() {
            Some(database) => database,
            None => {
                span.set_status(FastSpanStatus::Error {
                    message: Cow::Owned("No default database available.".to_string()),
                });
                return Err(EpError::database("No default database available."));
            }
        };
        Ok(Box::new(DatabaseOutput(database).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        context.client().default_database();
    }
}
