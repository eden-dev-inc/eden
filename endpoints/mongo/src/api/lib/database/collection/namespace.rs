use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::output::{CollectionDocumentOutput, NamespaceOutput};
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

const API_INFO: ApiInfo<MongoApi, NamespaceInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::Namespace)))),
    "Gets the namespace of the Collection",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct NamespaceInput {
        database: String,
        collection: String,
    }
}

type OutputWrapper = NamespaceOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(NamespaceInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl NamespaceInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_namespace(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_namespace(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(NamespaceOutput(context.namespace()).to_output()) as Box<dyn EpOutput>)
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
    async fn test_namespace_basic() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.collection_namespace("ns_basic").await;
        // Namespace serializes as "db.coll" string
        let ns_str = result.as_str().expect("namespace should be a string");
        assert!(ns_str.contains("ns_basic"), "namespace should contain collection name");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_namespace_matches_context() {
        let mut ctx = MongoTestContext::new().await;

        let db_name = ctx.db.clone();
        let result = ctx.collection_namespace("ns_ctx").await;
        // Namespace serializes as "db.coll" string
        let ns_str = result.as_str().expect("namespace should be a string");
        let expected = format!("{}.ns_ctx", db_name);
        assert_eq!(ns_str, expected, "namespace should be db.coll format");

        ctx.stop().await;
    }
}
