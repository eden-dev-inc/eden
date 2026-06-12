use crate::api::lib::MongoApi;
use crate::output::EmptyOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<MongoApi, WarmConnectionPoolInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::WarmConnectionPool,
    "Add connections to the connection pool up to min_pool_size",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct WarmConnectionPoolInput {}
}

type OutputWrapper = EmptyOutput;

impl_simple_operation!(WarmConnectionPoolInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl WarmConnectionPoolInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));
        let context = context.get().await.map_err(EpError::connect)?;

        context.warm_connection_pool().await;
        Ok(Box::new(EmptyOutput(()).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        // TODO: Await the future or use tokio::spawn to avoid silently dropping it
        #[allow(clippy::let_underscore_future)]
        let _ = context.client().warm_connection_pool();
    }
}
