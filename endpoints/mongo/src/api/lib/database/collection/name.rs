use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::output::{CollectionDocumentOutput, StringOutput};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;

struct SimpleName;
struct ComplexName;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, NameInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::Name)))),
    "Gets the name of the Collection",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct NameInput {
        database: String,
        collection: String,
    }
}

type OutputWrapper = StringOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(NameInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl NameInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_name(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_name(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(StringOutput(context.name().to_string()).to_output()) as Box<dyn EpOutput>)
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    #![allow(unexpected_cfgs)]
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_collection_name_basic() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.collection_name("cn_basic").await;
        assert_eq!(result, "cn_basic", "collection name should match the requested name");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_collection_name_special_chars() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.collection_name("cn_special_123_test").await;
        assert_eq!(result, "cn_special_123_test", "collection name with underscores and numbers should match");

        ctx.stop().await;
    }
}
