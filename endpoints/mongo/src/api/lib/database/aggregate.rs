use crate::api::lib::{DatabaseApi, MongoApi, MongoAsync};
use crate::api::wrapper::{AggregateOptionsWrapper, DocumentFunction, DocumentWrapperType};
use crate::output::{DatabaseOutput, VecDocumentOutput};
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use futures_util::TryStreamExt;
use mongo_core::MongoTx;
use mongodb::Database;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, DatabaseAggregateInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::DatabaseAggregate)),
    "Runs an aggregation operation",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DatabaseAggregateInput {
        database: String,
        pipeline: Vec<DocumentWrapperType>,
        options: Option<AggregateOptionsWrapper>,
    }
}

type OutputWrapper = VecDocumentOutput;
type ExpectedInput = DatabaseOutput;

impl_simple_operation!(DatabaseAggregateInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DatabaseAggregateInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_aggregate(&context.database(&self.database)).await
    }

    fn run_transaction_generic(&self, context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        // TODO: Await the future or use tokio::spawn to avoid silently dropping it
        #[allow(clippy::let_underscore_future)]
        let _ = Box::pin(async {
            // context.client().database(&self.database).aggregate_with_session()
            let _ = self.run_aggregate(&context.client().database(&self.database)).await;
        });
    }

    async fn run_aggregate(&self, database: &Database) -> ResultEP<Box<dyn EpOutput>> {
        let mut cursor = database
            .aggregate(
                self.clone().pipeline.clone().into_iter().map(DocumentFunction::into_document).collect::<Vec<Document>>(),
                self.clone().options.map(Into::into),
            )
            .await
            .map_err(EpError::database)?;

        let mut results = vec![];
        while let Some(doc) = cursor.try_next().await.map_err(EpError::request)? {
            results.push(doc)
        }

        Ok(Box::new(VecDocumentOutput(results).to_output()) as Box<dyn EpOutput>)
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
    async fn test_database_aggregate_list_local_sessions() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.database_aggregate(vec![doc! { "$listLocalSessions": { "allUsers": true } }]).await;
        assert!(result.is_array(), "$listLocalSessions aggregate should return an array");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_database_aggregate_with_limit() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.database_aggregate(vec![doc! { "$listLocalSessions": {} }, doc! { "$limit": 10 }]).await;
        let arr = result.as_array().expect("database aggregate with $limit should return an array");
        assert!(arr.len() <= 10, "$limit 10 should return at most 10 results");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_database_aggregate_with_count() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.database_aggregate(vec![doc! { "$listLocalSessions": {} }, doc! { "$count": "total" }]).await;
        let arr = result.as_array().expect("database aggregate with $count should return an array");
        if !arr.is_empty() {
            assert!(arr[0].get("total").is_some(), "count result should have a 'total' field");
        }

        ctx.stop().await;
    }
}
