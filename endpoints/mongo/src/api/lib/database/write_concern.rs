use crate::api::lib::{DatabaseApi, MongoApi};
use crate::output::{DatabaseOutput, WriteConcernOutput};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use telemetry::TelemetryWrapper;

pub struct SimpleWriteConcern;
pub struct ComplexWriteConcern;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, DatabaseWriteConcernInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::WriteConcern)),
    "Gets the write concern configuration of a MongoDB database",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DatabaseWriteConcernInput {
        database: String,
    }
}

type OutputWrapper = WriteConcernOutput;
type ExpectedInput = DatabaseOutput;

impl_simple_operation!(DatabaseWriteConcernInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DatabaseWriteConcernInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_write_concern(&context.database(&self.database)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_write_concern(&self, context: &Database) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(WriteConcernOutput(context.write_concern().map(|r| r.to_owned())).to_output()) as Box<dyn EpOutput>)
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
    async fn test_database_write_concern_default() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.database_write_concern().await;

        // Default connection has no explicit write concern set, so expect null or a valid object
        assert!(result.is_null() || result.is_object(), "result should be null or an object");

        ctx.stop().await;
    }
}
