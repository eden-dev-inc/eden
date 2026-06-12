pub mod aggregate;
pub mod client;
pub mod collection;
pub mod collection_with_options;
pub mod create_collection;
pub mod drop;
pub mod gridfs_bucket;
pub mod lib;
pub mod list_collection_names;
pub mod list_collections;
pub mod name;
pub mod read_concern;
pub mod run_command;
pub mod run_cursor_command;
pub mod selection_criteria;
pub mod watch;
pub mod write_concern;

pub use aggregate::*;
pub use client::*;
pub use collection_with_options::*;
pub use create_collection::*;
use ep_core::impl_simple_operation;
pub use list_collection_names::*;
pub use list_collections::*;
pub use name::*;
pub use read_concern::*;
pub use run_command::*;
pub use run_cursor_command::*;
pub use selection_criteria::*;
pub use watch::*;
pub use write_concern::*;

use crate::api::lib::MongoApi;
use crate::output::DatabaseOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, DatabaseInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(None),
    "Gets a handle to a database specified by name in the cluster the Client is connected to",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DatabaseInput {
        database: String,
    }
}

type OutputWrapper = DatabaseOutput;

impl_simple_operation!(DatabaseInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DatabaseInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("mongo.{}.{}", self.kind(), function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        let database = context.database(&self.database);

        Ok(Box::new(DatabaseOutput(database).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        context.client().database(&self.database);
    }
}
