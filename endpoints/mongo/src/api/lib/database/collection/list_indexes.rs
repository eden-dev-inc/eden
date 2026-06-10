use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::ListIndexesOptionsWrapper;
use crate::output::{CollectionDocumentOutput, VecIndexModelOutput};
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use futures_util::TryStreamExt;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, ListIndexesInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::ListIndexes)))),
    "Lists all indexes on this collection",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct ListIndexesInput {
        database: String,
        collection: String,
        options: Option<ListIndexesOptionsWrapper>,
    }
}

type OutputWrapper = VecIndexModelOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(ListIndexesInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl ListIndexesInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_list_indexes(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_list_indexes(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        let mut cursor = context.list_indexes(self.options.to_owned().map(Into::into)).await.map_err(EpError::database)?;
        let mut results = vec![];
        while let Some(index) = cursor.try_next().await.map_err(EpError::request)? {
            results.push(index)
        }

        Ok(Box::new(VecIndexModelOutput(results).to_output()) as Box<dyn EpOutput>)
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
    async fn test_list_indexes_default() {
        let mut ctx = MongoTestContext::new().await;

        // Insert a document to ensure the collection exists with a default _id index.
        ctx.insert_one("li_default", doc! { "x": 1 }).await;

        let result = ctx.list_indexes("li_default").await;

        assert!(result.is_array(), "list_indexes should return an array");
        let indexes = result.as_array().expect("should be array");
        assert!(!indexes.is_empty(), "should have at least the _id index");

        // Verify the _id index is present.
        let has_id_index = indexes.iter().any(|idx| idx["key"]["_id"] == 1);
        assert!(has_id_index, "should contain the default _id index");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_indexes_after_create() {
        let mut ctx = MongoTestContext::new().await;

        let opts = IndexOptionsWrapper {
            name: Some("li_custom_idx".to_string()),
            ..Default::default()
        };
        ctx.create_index("li_after_create", doc! { "status": 1 }, Some(opts)).await;

        let result = ctx.list_indexes("li_after_create").await;

        assert!(result.is_array(), "list_indexes should return an array");
        let indexes = result.as_array().expect("should be array");
        assert!(indexes.len() >= 2, "should have at least _id and the custom index");

        let has_custom = indexes.iter().any(|idx| idx["name"] == "li_custom_idx");
        assert!(has_custom, "custom index should appear in list_indexes output");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_indexes_multiple() {
        let mut ctx = MongoTestContext::new().await;

        let opts_a = IndexOptionsWrapper { name: Some("li_idx_a".to_string()), ..Default::default() };
        ctx.create_index("li_multiple", doc! { "a": 1 }, Some(opts_a)).await;

        let opts_b = IndexOptionsWrapper { name: Some("li_idx_b".to_string()), ..Default::default() };
        ctx.create_index("li_multiple", doc! { "b": -1 }, Some(opts_b)).await;

        let result = ctx.list_indexes("li_multiple").await;

        assert!(result.is_array(), "list_indexes should return an array");
        let indexes = result.as_array().expect("should be array");
        assert!(indexes.len() >= 3, "should have _id plus 2 custom indexes");

        let has_a = indexes.iter().any(|idx| idx["name"] == "li_idx_a");
        let has_b = indexes.iter().any(|idx| idx["name"] == "li_idx_b");
        assert!(has_a, "index li_idx_a should be present");
        assert!(has_b, "index li_idx_b should be present");

        ctx.stop().await;
    }
}
