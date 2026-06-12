use crate::api::lib::{DatabaseApi, MongoApi};
use crate::output::{DatabaseOutput, SelectionCriteriaOutput};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use telemetry::TelemetryWrapper;
pub struct SimpleSelectionCriteria;
pub struct ComplexSelectionCriteria;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, DatabaseSelectionCriteriaInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::SelectionCriteria)),
    "Gets the server selection criteria and read preference settings of a MongoDB database",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DatabaseSelectionCriteriaInput {
        database: String,
    }
}

type OutputWrapper = SelectionCriteriaOutput;
type ExpectedInput = DatabaseOutput;

impl_simple_operation!(DatabaseSelectionCriteriaInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DatabaseSelectionCriteriaInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_selection_criteria(&context.database(&self.database)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_selection_criteria(&self, context: &Database) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(SelectionCriteriaOutput(context.selection_criteria().map(|s| s.to_owned())).to_output()) as Box<dyn EpOutput>)
    }
}
// NOTE: SelectionCriteria does not implement serde Serialize, so integration tests cannot
// serialize the output. This API would need a custom serialization impl before testing.
