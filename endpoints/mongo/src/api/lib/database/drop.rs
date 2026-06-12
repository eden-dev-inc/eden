use crate::api::lib::{DatabaseApi, MongoApi};
use crate::api::wrapper::DropDatabaseOptionsWrapper;
use crate::output::EmptyOutput;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use telemetry::TelemetryWrapper;

pub struct MongoDrop;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, DropInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Drop)),
    "Permanently drops an entire MongoDB database, deleting all collections, documents, indexes, and metadata. This is an irreversible operation that removes all data associated with the specified database",
    ReqType::Write,
    true,
);

type CurrentOutput = ();

crate::mongo_endpoint! {
    API_INFO,
    struct DropInput {
        database: String,
        options: Option<DropDatabaseOptionsWrapper>,
    }
}

impl_simple_operation!(DropInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DropInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_drop(&context.database(&self.database)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }

    async fn run_drop(&self, database: &Database) -> ResultEP<Box<dyn EpOutput>> {
        database.drop(self.options.to_owned().map(Into::into)).await.map_err(EpError::database)?;

        Ok(Box::new(EmptyOutput(()).to_output()) as Box<dyn EpOutput>)
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    #![allow(unexpected_cfgs)]
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_database_drop_basic() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("drop_basic_coll", doc! { "_id": "d1", "name": "Alice" }).await;
        ctx.insert_one("drop_basic_coll", doc! { "_id": "d2", "name": "Bob" }).await;

        ctx.database_drop().await;

        let names = ctx.list_collection_names().await;
        let arr = names.as_array().expect("list_collection_names should return an array");
        assert!(arr.is_empty(), "collection names should be empty after dropping the database");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_database_drop_empty() {
        let mut ctx = MongoTestContext::new().await;

        // Dropping a fresh database with no data should succeed without error
        ctx.database_drop().await;

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_database_drop_verify_recreatable() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("drop_recreate_coll", doc! { "_id": "r1", "name": "Alice" }).await;

        ctx.database_drop().await;

        // Insert new data after dropping - the database should be recreated implicitly
        ctx.insert_one("drop_recreate_coll", doc! { "_id": "r2", "name": "Bob" }).await;

        let result = ctx.find("drop_recreate_coll", None).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 1, "should find exactly the newly inserted document");
        assert_eq!(arr[0]["_id"], "r2");
        assert_eq!(arr[0]["name"], "Bob");

        ctx.stop().await;
    }
}
