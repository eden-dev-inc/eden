use crate::api::lib::MongoApi;
use crate::output::BoolOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, ReqType, RunOutput};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<MongoApi, WithOptionsInput> = ApiInfo::new(EpKind::Mongo, MongoApi::WithOptions, "", ReqType::Read, true);

crate::mongo_endpoint! {
    API_INFO,
    struct WithOptionsInput {}
}

type SimpleInput = WithOptionsInput;
type ComplexInput = WithOptionsInput;
type OutputWrapper = BoolOutput;

impl_simple_operation!(WithOptionsInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl WithOptionsInput {
    #[named]
    async fn run_async_generic(&self, _context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span_context = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));
        todo!("with_options not yet supported")
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("with_options not yet supported")
    }
}
