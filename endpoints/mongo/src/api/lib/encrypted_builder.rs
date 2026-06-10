use crate::api::lib::MongoApi;
use crate::output::DatabaseOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, ReqType, RunOutput};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<MongoApi, EncryptedBuilderInput> = ApiInfo::new(EpKind::Mongo, MongoApi::EncryptedBuilder, "", ReqType::Read, true);

crate::mongo_endpoint! {
    API_INFO,
    struct EncryptedBuilderInput {}
}

type OutputWrapper = DatabaseOutput;

impl_simple_operation!(EncryptedBuilderInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl EncryptedBuilderInput {
    #[named]
    async fn run_async_generic(&self, _context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));
        Err(EpError::database("encrypted_builder not yet supported"))
    }

    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("encrypted_builder not yet supported")
    }
}
