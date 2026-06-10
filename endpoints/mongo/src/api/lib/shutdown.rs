use crate::api::lib::MongoApi;
use crate::output::EmptyOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<MongoApi, ShutdownInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Shutdown,
    "Shut down this Client, terminating background thread workers and closing connections",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct ShutdownInput {}
}

type OutputWrapper = EmptyOutput;

impl_simple_operation!(ShutdownInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl ShutdownInput {
    #[named]
    async fn run_async_generic(&self, _context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));
        todo!("async shutdown not implemented");
        // let _ = context.shutdown();
        // Ok(Box::new(ShutdownOutput) as Box<dyn MongoOutput<()>>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("async shutdown not implemented");
    }
}
