pub mod aggregate;
pub mod client;
pub mod clone_with_type;
pub mod count_documents;
pub mod create_index;
pub mod create_indexes;
pub mod create_search_index;
pub mod create_search_indexes;
pub mod delete_many;
pub mod delete_one;
pub mod distinct;
pub mod drop;
pub mod drop_index;
pub mod drop_indexes;
pub mod drop_search_index;
pub mod estimate_document_count;
pub mod find;
pub mod find_one;
pub mod find_one_and_delete;
pub mod find_one_and_replace;
pub mod find_one_and_update;
pub mod insert_many;
pub mod insert_one;
pub mod insert_one_model;
pub mod list_index_names;
pub mod list_indexes;
pub mod list_search_indexes;
pub mod name;
pub mod namespace;
pub mod read_concern;
pub mod replace_one;
pub mod replace_one_model;
pub mod selection_criteria;
pub mod update_many;
pub mod update_one;
pub mod update_search_index;
pub mod watch;
pub mod write_concern;

pub use aggregate::*;
pub use client::*;
pub use clone_with_type::*;
pub use count_documents::*;
pub use create_index::*;
pub use create_indexes::*;
pub use create_search_index::*;
pub use create_search_indexes::*;
pub use delete_many::*;
pub use delete_one::*;
pub use distinct::*;
pub use drop::*;
pub use drop_index::*;
pub use drop_indexes::*;
pub use drop_search_index::*;
pub use estimate_document_count::*;
pub use find::*;
pub use find_one::*;
pub use find_one_and_delete::*;
pub use find_one_and_replace::*;
pub use find_one_and_update::*;
pub use insert_many::*;
pub use insert_one::*;
pub use insert_one_model::*;
pub use list_index_names::*;
pub use list_indexes::*;
pub use list_search_indexes::*;
pub use name::*;
pub use namespace::*;
pub use read_concern::*;
pub use replace_one::*;
pub use replace_one_model::*;
pub use selection_criteria::*;
pub use update_many::*;
pub use update_one::*;
pub use update_search_index::*;
pub use watch::*;
pub use write_concern::*;

use crate::api::lib::{DatabaseApi, MongoApi};
use crate::output::{CollectionDocumentOutput, DatabaseOutput};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use telemetry::TelemetryWrapper;

pub struct SimpleDatabaseCollection;
pub struct ComplexDatabaseCollection;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, CollectionInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(None))),
    "Gets a handle to a collection in this database with the provided name",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct CollectionInput {
        database: String,
        collection: String,
    }
}

type OutputWrapper = CollectionDocumentOutput;
type ExpectedInput = DatabaseOutput;

impl_simple_operation!(CollectionInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl CollectionInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));
        let context = context.get().await.map_err(EpError::connect)?;

        let collection = context.database(&self.database).collection(&self.collection);
        Ok(Box::new(CollectionDocumentOutput(collection).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }

    async fn run_collection(&self, database: &Database) -> ResultEP<OutputWrapper> {
        let result = database.collection(&self.collection);

        Ok(CollectionDocumentOutput(result))
    }
}
