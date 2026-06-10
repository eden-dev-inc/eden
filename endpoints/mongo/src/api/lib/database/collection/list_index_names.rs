use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::output::{CollectionDocumentOutput, VecStringOutput};
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

const API_INFO: ApiInfo<MongoApi, ListIndexNamesInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::ListIndexNames)))),
    "Gets the names of all indexes on the collection",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct ListIndexNamesInput {
        database: String,
        collection: String,
    }
}

type OutputWrapper = VecStringOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(ListIndexNamesInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl ListIndexNamesInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_list_index_names(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_list_index_names(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(VecStringOutput(context.list_index_names().await.map_err(EpError::database)?).to_output()) as Box<dyn EpOutput>)
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
    async fn test_list_index_names_default() {
        let mut ctx = MongoTestContext::new().await;

        // Insert a document to ensure the collection exists.
        ctx.insert_one("lin_default", doc! { "x": 1 }).await;

        let result = ctx.list_index_names("lin_default").await;

        assert!(result.is_array(), "list_index_names should return an array");
        let names = result.as_array().expect("should be array");
        let has_id = names.iter().any(|n| n.as_str() == Some("_id_"));
        assert!(has_id, "should contain the default _id_ index name");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_index_names_after_create() {
        let mut ctx = MongoTestContext::new().await;

        let opts = IndexOptionsWrapper {
            name: Some("lin_named_idx".to_string()),
            ..Default::default()
        };
        ctx.create_index("lin_after_create", doc! { "field": 1 }, Some(opts)).await;

        let result = ctx.list_index_names("lin_after_create").await;

        assert!(result.is_array(), "list_index_names should return an array");
        let names = result.as_array().expect("should be array");
        let has_custom = names.iter().any(|n| n.as_str() == Some("lin_named_idx"));
        assert!(has_custom, "custom index name should appear in list");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_index_names_multiple() {
        let mut ctx = MongoTestContext::new().await;

        let opts_a = IndexOptionsWrapper {
            name: Some("lin_idx_alpha".to_string()),
            ..Default::default()
        };
        ctx.create_index("lin_multiple", doc! { "alpha": 1 }, Some(opts_a)).await;

        let opts_b = IndexOptionsWrapper { name: Some("lin_idx_beta".to_string()), ..Default::default() };
        ctx.create_index("lin_multiple", doc! { "beta": 1 }, Some(opts_b)).await;

        let result = ctx.list_index_names("lin_multiple").await;

        assert!(result.is_array(), "list_index_names should return an array");
        let names = result.as_array().expect("should be array");
        assert!(names.len() >= 3, "should have _id_ plus 2 custom index names");

        let has_alpha = names.iter().any(|n| n.as_str() == Some("lin_idx_alpha"));
        let has_beta = names.iter().any(|n| n.as_str() == Some("lin_idx_beta"));
        assert!(has_alpha, "lin_idx_alpha should be in the list");
        assert!(has_beta, "lin_idx_beta should be in the list");

        ctx.stop().await;
    }
}
