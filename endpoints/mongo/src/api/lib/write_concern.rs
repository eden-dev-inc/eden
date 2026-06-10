use crate::api::lib::MongoApi;
use crate::output::WriteConcernOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<MongoApi, WriteConcernInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::WriteConcern,
    "Gets the default write concern the Client uses for operations",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct WriteConcernInput {}
}

type OutputWrapper = WriteConcernOutput;

impl_simple_operation!(WriteConcernInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl WriteConcernInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));
        let context = context.get().await.map_err(EpError::connect)?;

        Ok(Box::new(WriteConcernOutput(context.write_concern().map(|r| r.to_owned())).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        context.client().write_concern();
    }
}
