use crate::api::lib::MongoApi;
use crate::output::ReadConcernOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<MongoApi, ReadConcernInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::ReadConcern,
    "Gets the default read concern the Client uses for operations",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct ReadConcernInput {}
}

type OutputWrapper = ReadConcernOutput;

impl_simple_operation!(ReadConcernInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl ReadConcernInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        let result = context.read_concern().map(|r| r.to_owned());
        Ok(Box::new(ReadConcernOutput(result).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        context.client().read_concern();
    }
}
