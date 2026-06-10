use crate::api::lib::{DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, SelectionCriteriaWrapper};
use crate::output::{DatabaseOutput, DocumentOutput};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use telemetry::TelemetryWrapper;

pub struct SimpleRunCommand;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, RunCommandInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::RunCommand)),
    "Runs a database-level command against a MongoDB database",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct RunCommandInput {
        database: String,
        command: DocumentWrapperType,
        selection_criteria: Option<SelectionCriteriaWrapper>,
    }
}

type OutputWrapper = DocumentOutput;
type ExpectedInput = DatabaseOutput;

impl_simple_operation!(RunCommandInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl RunCommandInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_run_command(&context.database(&self.database)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_run_command(&self, context: &Database) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            DocumentOutput(
                context
                    .run_command(self.command.to_owned().into_document(), self.selection_criteria.clone().map(Into::into))
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
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_run_command_ping() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.run_command(doc! { "ping": 1 }).await;
        assert_eq!(result["ok"], 1.0, "ping command should return ok=1");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_run_command_server_status() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.run_command(doc! { "serverStatus": 1 }).await;
        assert!(result.get("version").is_some(), "serverStatus should contain a 'version' field");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_run_command_build_info() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.run_command(doc! { "buildInfo": 1 }).await;
        assert!(result.get("version").is_some(), "buildInfo should contain a 'version' field");

        ctx.stop().await;
    }
}
