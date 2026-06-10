use crate::api::lib::MongoApi;
use crate::output::SelectionCriteriaOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<MongoApi, SelectionCriteriaInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::SelectionCriteria,
    "Gets the default selection criteria the Client uses for operations",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct SelectionCriteriaInput {}
}

type OutputWrapper = SelectionCriteriaOutput;

impl_simple_operation!(SelectionCriteriaInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl SelectionCriteriaInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));
        let context = context.get().await.map_err(EpError::connect)?;

        let result = context.selection_criteria().map(|s| s.to_owned());
        Ok(Box::new(SelectionCriteriaOutput(result).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        context.client().selection_criteria();
    }
}
