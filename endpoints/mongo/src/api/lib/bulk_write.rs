use crate::api::lib::MongoApi;
use crate::api::wrapper::{AggregateOptionsWrapper, DocumentWrapperType};
use crate::request::MongoRequest;
use crate::{ApiInfo, RunOutput};
use ep_core::{EpOutput, ReqType, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::MongoAsync;
use mongo_core::MongoTx;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, BulkWriteInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::BulkWrite,
    "Execute multiple write operations (insert, update, delete) in a single request to improve performance and reduce network round trips",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct BulkWriteInput {
        collection: Option<String>,
        aggregates: Vec<DocumentWrapperType>,
        options: Option<AggregateOptionsWrapper>,
    }
}

impl_simple_operation!(BulkWriteInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl BulkWriteInput {
    #[named]
    async fn run_async_generic(&self, _context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        Err(EpError::metadata("bulk_write not yet supported"))
    }

    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("bulk_write not yet supported")
    }
}
