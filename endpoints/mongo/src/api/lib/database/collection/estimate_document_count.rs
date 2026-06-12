use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::EstimatedDocumentCountOptionsWrapper;
use crate::output::U64Output;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;

struct SimpleEstimateDocumentCount;
struct ComplexEstimateDocumentCount;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, EstimateDocumentCountInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::EstimateDocumentCount)))),
    "Estimates the number of documents in the collection using collection metadata",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct EstimateDocumentCountInput {
        database: String,
        collection: String,
        options: Option<EstimatedDocumentCountOptionsWrapper>,
    }
}

impl_simple_operation!(EstimateDocumentCountInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl EstimateDocumentCountInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_estimate_document_count(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_estimate_document_count(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            U64Output(context.estimated_document_count(self.options.to_owned().map(Into::into)).await.map_err(EpError::database)?)
                .to_output(),
        ) as Box<dyn EpOutput>)
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
    async fn test_estimated_count_basic() {
        let mut ctx = MongoTestContext::new().await;

        for i in 0..10 {
            ctx.insert_one("edc_basic", doc! { "_id": format!("doc{}", i), "value": i }).await;
        }

        let result = ctx.estimated_document_count("edc_basic").await;
        let count = result.as_u64().expect("estimated count should be a number");
        // estimated_document_count uses collection metadata and may not be perfectly exact
        // in all edge cases, but for a freshly populated collection it should be accurate
        assert!((8..=12).contains(&count), "estimated count should be approximately 10, got {}", count);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_estimated_count_empty() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.estimated_document_count("edc_empty").await;
        let count = result.as_u64().expect("estimated count should be a number");
        assert_eq!(count, 0, "estimated count of empty/non-existent collection should be 0");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_estimated_count_after_inserts() {
        let mut ctx = MongoTestContext::new().await;

        let docs: Vec<mongodb::bson::Document> = (0..25).map(|i| doc! { "_id": format!("doc{}", i), "index": i }).collect();
        ctx.insert_many("edc_batch", docs).await;

        let result = ctx.estimated_document_count("edc_batch").await;
        let count = result.as_u64().expect("estimated count should be a number");
        assert!((23..=27).contains(&count), "estimated count should be approximately 25, got {}", count);

        ctx.stop().await;
    }
}
