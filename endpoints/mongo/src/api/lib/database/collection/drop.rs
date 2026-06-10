use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::DropCollectionOptionsWrapper;
use crate::output::EmptyOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, DropInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::Drop)))),
    "Drops the collection, deleting all data and indexes stored in it",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DropInput {
        database: String,
        collection: String,
        options: Option<DropCollectionOptionsWrapper>,
    }
}

impl_simple_operation!(DropInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DropInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_drop(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_drop(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(
            Box::new(EmptyOutput(context.drop(self.options.to_owned().map(Into::into)).await.map_err(EpError::database)?).to_output())
                as Box<dyn EpOutput>,
        )
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
    async fn test_drop_collection_basic() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("dc_basic", doc! { "_id": "d1", "name": "Alice" }).await;
        ctx.insert_one("dc_basic", doc! { "_id": "d2", "name": "Bob" }).await;

        ctx.drop_collection("dc_basic").await;

        let count = ctx.count_documents("dc_basic", None).await;
        assert_eq!(count, 0, "collection should have no documents after drop");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_drop_collection_nonexistent() {
        let mut ctx = MongoTestContext::new().await;

        // Dropping a non-existent collection should not error
        ctx.drop_collection("dc_nonexistent").await;

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_drop_collection_verify_data_gone() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("dc_verify", doc! { "_id": "v1", "name": "Alice" }).await;
        ctx.insert_one("dc_verify", doc! { "_id": "v2", "name": "Bob" }).await;
        ctx.insert_one("dc_verify", doc! { "_id": "v3", "name": "Charlie" }).await;

        ctx.drop_collection("dc_verify").await;

        let result = ctx.find("dc_verify", None).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 0, "find after drop should return empty array");

        ctx.stop().await;
    }
}
