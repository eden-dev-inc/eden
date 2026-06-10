use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::DropIndexOptionsWrapper;
use crate::output::{CollectionDocumentOutput, EmptyOutput};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;

struct SimpleDropIndexes;
struct ComplexDropIndexes;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, DropIndexesInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::DropIndexes)))),
    "Drops all indexes associated with this collection",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DropIndexesInput {
        database: String,
        collection: String,
        options: Option<DropIndexOptionsWrapper>,
    }
}

type OutputWrapper = EmptyOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(DropIndexesInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DropIndexesInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_drop_indexes(&context.database(&self.database)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_drop_indexes(&self, database: &Database) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            EmptyOutput(
                database
                    .collection::<Document>(&self.collection)
                    .drop_indexes(self.options.to_owned().map(Into::into))
                    .await
                    .map_err(EpError::database)?,
            )
            .to_output(),
        ) as Box<dyn EpOutput>)
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    #![allow(unexpected_cfgs)]
    use crate::api::wrapper::IndexOptionsWrapper;
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_drop_indexes_basic() {
        let mut ctx = MongoTestContext::new().await;

        // Create two custom indexes.
        let opts_a = IndexOptionsWrapper { name: Some("dis_idx_a".to_string()), ..Default::default() };
        ctx.create_index("dis_basic", doc! { "a": 1 }, Some(opts_a)).await;

        let opts_b = IndexOptionsWrapper { name: Some("dis_idx_b".to_string()), ..Default::default() };
        ctx.create_index("dis_basic", doc! { "b": 1 }, Some(opts_b)).await;

        // Verify both indexes exist.
        let before = ctx.list_index_names("dis_basic").await;
        let before_arr = before.as_array().expect("should be array");
        assert!(before_arr.len() >= 3, "should have _id_ plus 2 custom indexes");

        // Drop all non-_id indexes.
        ctx.drop_indexes("dis_basic").await;

        // Verify only _id_ remains.
        let after = ctx.list_index_names("dis_basic").await;
        let after_arr = after.as_array().expect("should be array");
        assert_eq!(after_arr.len(), 1, "only _id_ index should remain after drop_indexes");
        assert_eq!(after_arr[0].as_str().expect("should be string"), "_id_");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_drop_indexes_empty() {
        let mut ctx = MongoTestContext::new().await;

        // Insert a doc to create the collection (only _id index).
        ctx.insert_one("dis_empty", doc! { "x": 1 }).await;

        // Drop indexes when there are no custom indexes -- should succeed.
        ctx.drop_indexes("dis_empty").await;

        // Verify _id_ still exists.
        let after = ctx.list_index_names("dis_empty").await;
        let after_arr = after.as_array().expect("should be array");
        assert_eq!(after_arr.len(), 1, "only _id_ index should remain");
        assert_eq!(after_arr[0].as_str().expect("should be string"), "_id_");

        ctx.stop().await;
    }
}
