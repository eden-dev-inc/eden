pub mod delete;
pub mod drop;
pub mod find;
pub mod find_one;
pub mod open_download_stream;
pub mod open_download_stream_by_name;
pub mod open_upload_stream;
pub mod read_concern;
pub mod rename;
pub mod selection_criteria;
pub mod write_concern;

pub use delete::*;
pub use drop::*;
pub use find::*;
pub use find_one::*;
pub use read_concern::*;
pub use rename::*;
pub use selection_criteria::*;
pub use write_concern::*;

use crate::api::lib::{DatabaseApi, MongoApi};
use crate::api::wrapper::GridFsBucketOptionsWrapper;
use crate::output::{DatabaseOutput, GridfsBucketOutput};
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, GridfsBucketInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::GridfsBucket(None))),
    "Creates a new GridFS bucket for storing and retrieving large files (>16MB) in MongoDB. GridFS automatically splits large files into chunks and stores metadata, enabling efficient file operations and streaming",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct GridfsBucketInput {
        database: String,
        options: Option<GridFsBucketOptionsWrapper>,
    }
}

type OutputWrapper = GridfsBucketOutput;
type ExpectedInput = DatabaseOutput;

impl_simple_operation!(GridfsBucketInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl GridfsBucketInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        Ok(
            Box::new(
                GridfsBucketOutput(context.database(&self.database).gridfs_bucket(self.options.to_owned().map(Into::into))).to_output(),
            ) as Box<dyn EpOutput>,
        )
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_gridfs_bucket(&self, database: &Database) -> ResultEP<OutputWrapper> {
        let result = database.gridfs_bucket(self.options.to_owned().map(Into::into));

        Ok(GridfsBucketOutput(result))
    }
}
