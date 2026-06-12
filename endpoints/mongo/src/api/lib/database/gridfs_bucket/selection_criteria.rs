use crate::api::lib::{DatabaseApi, GridfsBucketApi, MongoApi};
use crate::api::wrapper::GridFsBucketOptionsWrapper;
use crate::output::{GridfsBucketOutput, SelectionCriteriaOutput};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::GridFsBucket;
use telemetry::TelemetryWrapper;

struct SimpleSelectionCriteria;
struct ComplexSelectionCriteria;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

// Declared for consistency with other endpoint modules
#[allow(dead_code)]
const REQUEST_TYPE: ReqType = ReqType::Read;

type PreviousOutput = GridFsBucket;

const API_INFO: ApiInfo<MongoApi, GridfsSelectionCriteriaInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::GridfsBucket(Some(GridfsBucketApi::SelectionCriteria)))),
    "Gets the selection criteria of the bucket",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct GridfsSelectionCriteriaInput {
        database: String,
        gridfs: Option<GridFsBucketOptionsWrapper>,
    }
}

type OutputWrapper = SelectionCriteriaOutput;
type ExpectedInput = GridfsBucketOutput;

impl_simple_operation!(GridfsSelectionCriteriaInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl GridfsSelectionCriteriaInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_selection_criteria(&context.database(&self.database).gridfs_bucket(self.gridfs.to_owned().map(Into::into)))
            .await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_selection_criteria(&self, context: &GridFsBucket) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(SelectionCriteriaOutput(context.selection_criteria().map(|s| s.to_owned())).to_output()) as Box<dyn EpOutput>)
    }
}
// NOTE: SelectionCriteria does not implement serde Serialize, so integration tests cannot
// serialize the output. This API would need a custom serialization impl before testing.
