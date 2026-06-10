use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::output::{CollectionDocumentOutput, WriteConcernOutput};
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

const API_INFO: ApiInfo<MongoApi, CollectionWriteConcernInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::WriteConcern)))),
    "Gets the write concern of the Collection",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
   struct CollectionWriteConcernInput {
        database: String,
        collection: String,
    }
}

type OutputWrapper = WriteConcernOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(CollectionWriteConcernInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl CollectionWriteConcernInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_write_concern(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_write_concern(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(WriteConcernOutput(context.write_concern().map(|w| w.to_owned())).to_output()) as Box<dyn EpOutput>)
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
    async fn test_collection_write_concern_default() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.collection_write_concern("test_wc_coll").await;

        // Default connection has no explicit write concern set, so expect null or a valid object
        assert!(result.is_null() || result.is_object(), "result should be null or an object");

        ctx.stop().await;
    }
}
