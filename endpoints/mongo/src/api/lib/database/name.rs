use crate::api::lib::{DatabaseApi, MongoApi};
use crate::output::StringOutput;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use telemetry::TelemetryWrapper;

pub struct MongoName;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, NameInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Name)),
    "Retrieves and returns the name of the specified MongoDB database. This is a lightweight operation used for database name validation, confirmation, and introspection in database management workflows",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct NameInput {
        database: String,
    }
}

impl_simple_operation!(NameInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl NameInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_name(&context.database(&self.database)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_name(&self, context: &Database) -> ResultEP<Box<dyn EpOutput>> {
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
    async fn test_database_name_basic() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.database_name().await;
        assert_eq!(result, "test_db", "database name should equal 'test_db'");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_database_name_returns_string() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.database_name().await;
        assert!(result.is_string(), "database name result should be a string type");

        ctx.stop().await;
    }
}
