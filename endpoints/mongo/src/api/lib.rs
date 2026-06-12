///MONGO API NOTES
///
/// This directory includes wrappers for all MongoDB functions.
///
/// The execution path for each request utilizes either a "Simple" method, where requests are called
/// directly with all required fields provided in a single structure, or a "Complex" method, where
/// requests are chained as individual nodes, with subsequent requests utilizing the output of the
/// previous request.
///
/// For generic requests we recommend "Simple" methods due to the reduced cost when running a
/// defined sequence; however, when one would like to make multiple requests from the same resource
/// (for example: making multiple requests from a single collection), the "Complex" method can allow
/// multiple requests to call the same shared Collection. This can be more resource performant that
/// making several independent asynchronous requests.
///
/// All request outputs are wrapper in a unique data structure. These structures all implement a
/// shared trait `MongoOutput` to ensure consistency. In the case of a "Complex" request, all
/// output structures can be downcast to a defined type `T`. While T is a dynamic value, each
/// request has an expected input type, ensuring that the request chain is successful or returns an
/// error.
mod bulk_write;
pub mod database;
mod database_with_options;
mod default_database;
mod encrypted_builder;
#[allow(clippy::module_inception)]
pub mod lib;
mod list_database_names;
mod list_databases;
mod raw_command;
mod raw_command_read_only;
#[allow(hidden_glob_reexports)]
mod read_concern;
#[allow(hidden_glob_reexports)]
mod selection_criteria;
mod shutdown;
mod start_session;
mod warm_connection_pool;
#[allow(hidden_glob_reexports)]
mod watch;
mod with_options;
mod with_uri_str;
#[allow(hidden_glob_reexports)]
mod write_concern;

use endpoint_derive::{ApiBuilder, DocumentAPI};
use mongo_core::MongoAsync;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};

pub use bulk_write::*;
pub use database::*;
pub use database_with_options::*;
pub use default_database::*;
pub use encrypted_builder::*;
pub use list_database_names::*;
pub use list_databases::*;
pub use raw_command::*;
pub use raw_command_read_only::*;
pub use read_concern::*;
pub use selection_criteria::*;
pub use shutdown::*;
pub use start_session::*;
pub use warm_connection_pool::*;
pub use watch::*;
pub use with_options::*;
pub use with_uri_str::*;
pub use write_concern::*;

#[derive(Debug, Serialize, Deserialize, Clone, DocumentAPI, ApiBuilder)]
#[api_builder(builder_name = "MongoApiBuilder")]
pub enum MongoApi {
    BulkWrite,
    Database(Option<DatabaseApi>),
    DatabaseWithOptions(Option<DatabaseApi>),
    DefaultDatabase(Option<DatabaseApi>),
    EncryptedBuilder,
    ListDatabaseNames,
    ListDatabases,
    RawCommand,
    RawCommandReadOnly,
    ReadConcern,
    SelectionCriteria,
    Shutdown,
    StartSession,
    WarmConnectionPool,
    Watch,
    WithOptions,
    WithUriStr,
    WriteConcern,
    #[noinput]
    ComplexPlaceholder,
}

impl MongoApi {
    pub fn name() -> String {
        "MongoApi".to_string()
    }

    pub fn db_kind() -> String {
        "mongo".to_string()
    }
}

impl Display for MongoApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ComplexPlaceholder => write!(f, "complex_placeholder"),
            Self::BulkWrite => write!(f, "bulk_write"),
            Self::Database(kind) => match kind {
                Some(kind) => write!(f, "database.{}", kind),
                None => write!(f, "database"),
            },
            Self::DatabaseWithOptions(kind) => match kind {
                Some(kind) => write!(f, "database_with_options.{}", kind),
                None => write!(f, "database_with_options"),
            },
            Self::DefaultDatabase(kind) => match kind {
                Some(kind) => write!(f, "default_database.{}", kind),
                None => write!(f, "default_database"),
            },
            Self::EncryptedBuilder => write!(f, "encrypted_builder"),
            Self::ListDatabaseNames => write!(f, "list_database_names"),
            Self::ListDatabases => write!(f, "list_databases"),
            Self::RawCommand => write!(f, "raw_command"),
            Self::RawCommandReadOnly => write!(f, "raw_command_read_only"),
            Self::ReadConcern => write!(f, "read_concern"),
            Self::SelectionCriteria => write!(f, "selection_criteria"),
            Self::Shutdown => write!(f, "shutdown"),
            Self::StartSession => write!(f, "start_session"),
            Self::WarmConnectionPool => write!(f, "warm_connection_pool"),
            Self::Watch => write!(f, "watch"),
            Self::WithOptions => write!(f, "with_options"),
            Self::WithUriStr => write!(f, "with_uri_str"),
            Self::WriteConcern => write!(f, "write_concern"),
        }
    }
}

mod database_api {
    use super::*;
    use crate::api::lib::database::{
        aggregate::{DatabaseAggregateInput, DatabaseAggregateInputBuilder},
        client::{DatabaseClientInput, DatabaseClientInputBuilder},
        create_collection::{CreateCollectionInput, CreateCollectionInputBuilder},
        drop::{DropInput, DropInputBuilder},
        list_collection_names::{ListCollectionNamesInput, ListCollectionNamesInputBuilder},
        list_collections::{ListCollectionsInput, ListCollectionsInputBuilder},
        name::{NameInput, NameInputBuilder},
        run_command::{RunCommandInput, RunCommandInputBuilder},
        run_cursor_command::{RunCursorCommandInput, RunCursorCommandInputBuilder},
    };

    #[derive(Debug, Serialize, Deserialize, Clone, DocumentAPI, ApiBuilder)]
    #[api_builder(builder_name = "DatabaseApiBuilder")]
    pub enum DatabaseApi {
        DatabaseAggregate,
        DatabaseClient,
        Collection(Option<CollectionApi>),
        CollectionWithOptions(Option<CollectionApi>),
        CreateCollection,
        Drop,
        GridfsBucket(Option<GridfsBucketApi>),
        ListCollectionNames,
        ListCollections,
        Name,
        ReadConcern,
        RunCommand,
        RunCursorCommand,
        SelectionCriteria,
        Watch,
        WriteConcern,
    }

    impl DatabaseApi {
        pub fn name() -> String {
            "DatabaseApi".to_string()
        }

        pub fn db_kind() -> String {
            "mongo".to_string()
        }
    }

    impl Display for DatabaseApi {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            match self {
                Self::DatabaseAggregate => write!(f, "aggregate"),
                Self::DatabaseClient => write!(f, "client"),
                Self::Collection(kind) => match kind {
                    Some(kind) => write!(f, "collection.{}", kind),
                    None => write!(f, "collection"),
                },
                Self::CollectionWithOptions(kind) => match kind {
                    Some(kind) => write!(f, "collection_with_options.{}", kind),
                    None => write!(f, "collection_with_options"),
                },
                Self::CreateCollection => write!(f, "create_collection"),
                Self::Drop => write!(f, "drop"),
                Self::GridfsBucket(kind) => match kind {
                    Some(kind) => write!(f, "gridfs_bucket.{}", kind),
                    None => write!(f, "gridfsbucket"),
                },
                Self::ListCollectionNames => write!(f, "list_collection_names"),
                Self::ListCollections => write!(f, "list_collections"),
                Self::Name => write!(f, "name"),
                Self::ReadConcern => write!(f, "read_concern"),
                Self::RunCommand => write!(f, "run_command"),
                Self::RunCursorCommand => write!(f, "run_cursor_command"),
                Self::SelectionCriteria => write!(f, "selection_criteria"),
                Self::Watch => write!(f, "watch"),
                Self::WriteConcern => write!(f, "write_concern"),
            }
        }
    }
}
pub use database_api::{DatabaseApi, DatabaseApiBuilder};

mod collection_api {
    use super::*;
    use crate::api::lib::database::collection::{
        aggregate::{CollectionAggregateInput, CollectionAggregateInputBuilder},
        client::{CollectionClientInput, CollectionClientInputBuilder},
        clone_with_type::{CloneWithTypeInput, CloneWithTypeInputBuilder},
        count_documents::{CountDocumentsInput, CountDocumentsInputBuilder},
        create_index::{CreateIndexInput, CreateIndexInputBuilder},
        create_indexes::{CreateIndexesInput, CreateIndexesInputBuilder},
        create_search_index::{CreateSearchIndexInput, CreateSearchIndexInputBuilder},
        create_search_indexes::{CreateSearchIndexesInput, CreateSearchIndexesInputBuilder},
        delete_many::{DeleteManyInput, DeleteManyInputBuilder},
        delete_one::{DeleteOneInput, DeleteOneInputBuilder},
        distinct::{DistinctInput, DistinctInputBuilder},
        drop::{DropInput, DropInputBuilder},
        drop_index::{DropIndexInput, DropIndexInputBuilder},
        drop_indexes::{DropIndexesInput, DropIndexesInputBuilder},
        drop_search_index::{DropSearchIndexInput, DropSearchIndexInputBuilder},
        estimate_document_count::{EstimateDocumentCountInput, EstimateDocumentCountInputBuilder},
        find::{FindInput, FindInputBuilder},
        find_one::{FindOneInput, FindOneInputBuilder},
        find_one_and_delete::{FindOneAndDeleteInput, FindOneAndDeleteInputBuilder},
        find_one_and_replace::{FindOneAndReplaceInput, FindOneAndReplaceInputBuilder},
        find_one_and_update::{FindOneAndUpdateInput, FindOneAndUpdateInputBuilder},
        insert_many::{InsertManyInput, InsertManyInputBuilder},
        insert_one::{InsertOneInput, InsertOneInputBuilder},
        insert_one_model::{InsertOneModelInput, InsertOneModelInputBuilder},
        list_index_names::{ListIndexNamesInput, ListIndexNamesInputBuilder},
        list_indexes::{ListIndexesInput, ListIndexesInputBuilder},
        list_search_indexes::{ListSearchIndexesInput, ListSearchIndexesInputBuilder},
        name::{NameInput, NameInputBuilder},
        namespace::{NamespaceInput, NamespaceInputBuilder},
        replace_one::{ReplaceOneInput, ReplaceOneInputBuilder},
        replace_one_model::{ReplaceOneModelInput, ReplaceOneModelInputBuilder},
        update_many::{UpdateManyInput, UpdateManyInputBuilder},
        update_one::{UpdateOneInput, UpdateOneInputBuilder},
        update_search_index::{UpdateSearchIndexInput, UpdateSearchIndexInputBuilder},
    };

    #[derive(Debug, Serialize, Deserialize, Clone, DocumentAPI, ApiBuilder)]
    #[api_builder(builder_name = "CollectionApiBuilder")]
    pub enum CollectionApi {
        CollectionAggregate,
        CollectionClient,
        CloneWithType,
        CountDocuments,
        CreateIndex,
        CreateIndexes,
        CreateSearchIndex,
        CreateSearchIndexes,
        DeleteMany,
        DeleteOne,
        Distinct,
        Drop,
        DropIndex,
        DropIndexes,
        DropSearchIndex,
        EstimateDocumentCount,
        Find,
        FindOne,
        FindOneAndDelete,
        FindOneAndReplace,
        FindOneAndUpdate,
        InsertMany,
        InsertOne,
        InsertOneModel,
        ListIndexNames,
        ListIndexes,
        ListSearchIndexes,
        Name,
        Namespace,
        ReadConcern,
        ReplaceOne,
        ReplaceOneModel,
        SelectionCriteria,
        UpdateMany,
        UpdateOne,
        UpdateSearchIndex,
        Watch,
        WriteConcern,
    }

    impl CollectionApi {
        pub fn name() -> String {
            "CollectionApi".to_string()
        }

        pub fn db_kind() -> String {
            "mongo".to_string()
        }
    }

    impl Display for CollectionApi {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            match self {
                Self::CollectionAggregate => write!(f, "aggregate"),
                Self::CollectionClient => write!(f, "client"),
                Self::CloneWithType => write!(f, "clone_with_type"),
                Self::CountDocuments => write!(f, "count_documents"),
                Self::CreateIndex => write!(f, "create_index"),
                Self::CreateIndexes => write!(f, "create_indexes"),
                Self::CreateSearchIndex => write!(f, "create_search_index"),
                Self::CreateSearchIndexes => write!(f, "create_search_indexes"),
                Self::DeleteMany => write!(f, "delete_many"),
                Self::DeleteOne => write!(f, "delete_one"),
                Self::Distinct => write!(f, "distinct"),
                Self::Drop => write!(f, "drop"),
                Self::DropIndex => write!(f, "drop_index"),
                Self::DropIndexes => write!(f, "drop_indexes"),
                Self::DropSearchIndex => write!(f, "drop_search_index"),
                Self::EstimateDocumentCount => write!(f, "estimate_document_count"),
                Self::Find => write!(f, "find"),
                Self::FindOne => write!(f, "find_one"),
                Self::FindOneAndDelete => write!(f, "find_one_and_delete"),
                Self::FindOneAndReplace => write!(f, "find_one_and_replace"),
                Self::FindOneAndUpdate => write!(f, "find_one_and_update"),
                Self::InsertMany => write!(f, "insert_many"),
                Self::InsertOne => write!(f, "insert_one"),
                Self::InsertOneModel => write!(f, "insert_one_model"),
                Self::ListIndexNames => write!(f, "list_index_names"),
                Self::ListIndexes => write!(f, "list_indexes"),
                Self::ListSearchIndexes => write!(f, "list_search_indexes"),
                Self::Name => write!(f, "name"),
                Self::Namespace => write!(f, "namespace"),
                Self::ReadConcern => write!(f, "read_concern"),
                Self::ReplaceOne => write!(f, "replace_one"),
                Self::ReplaceOneModel => write!(f, "replace_one_model"),
                Self::SelectionCriteria => write!(f, "selection_criteria"),
                Self::UpdateMany => write!(f, "update_many"),
                Self::UpdateOne => write!(f, "update_one"),
                Self::UpdateSearchIndex => write!(f, "update_search_index"),
                Self::Watch => write!(f, "watch"),
                Self::WriteConcern => write!(f, "write_concern"),
            }
        }
    }
}
pub use collection_api::{CollectionApi, CollectionApiBuilder};

mod gridfs_bucket_api {
    use super::*;
    use crate::api::lib::database::gridfs_bucket::{
        delete::{DeleteInput, DeleteInputBuilder},
        drop::{DropInput, DropInputBuilder},
        find::{FindInput, FindInputBuilder},
        find_one::{FindOneInput, FindOneInputBuilder},
        rename::{GridfsRenameInput, GridfsRenameInputBuilder},
    };

    #[derive(Debug, Serialize, Deserialize, Clone, DocumentAPI, ApiBuilder)]
    #[api_builder(builder_name = "GridfsBucketApiBuilder")]
    pub enum GridfsBucketApi {
        Delete,
        Drop,
        Find,
        FindOne,
        #[noinput]
        OpenDownloadStream,
        #[noinput]
        OpenDownloadStreamByName,
        #[noinput]
        OpenUploadStream,
        ReadConcern,
        GridfsRename,
        SelectionCriteria,
        WriteConcern,
    }

    impl GridfsBucketApi {
        pub fn name() -> String {
            "GridfsBucketApi".to_string()
        }

        pub fn db_kind() -> String {
            "mongo".to_string()
        }
    }

    impl Display for GridfsBucketApi {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            match self {
                Self::Delete => write!(f, "delete"),
                Self::Drop => write!(f, "drop"),
                Self::Find => write!(f, "find"),
                Self::FindOne => write!(f, "find_one"),
                Self::OpenDownloadStream => write!(f, "open_download_stream"),
                Self::OpenDownloadStreamByName => write!(f, "open_download_stream_by_name"),
                Self::OpenUploadStream => write!(f, "open_upload_stream"),
                Self::ReadConcern => write!(f, "read_concern"),
                Self::GridfsRename => write!(f, "rename"),
                Self::SelectionCriteria => write!(f, "selection_criteria"),
                Self::WriteConcern => write!(f, "write_concern"),
            }
        }
    }
}
pub use gridfs_bucket_api::{GridfsBucketApi, GridfsBucketApiBuilder};
