use crate::api::lib::{DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, RunCursorCommandOptionsWrapper};
use crate::output::{DatabaseOutput, VecDocumentOutput};
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use futures_util::TryStreamExt;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, RunCursorCommandInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::RunCursorCommand)),
    "Runs a database-level command and returns a cursor to iterate through the response",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct RunCursorCommandInput {
        database: String,
        command: DocumentWrapperType,
        options: Option<RunCursorCommandOptionsWrapper>,
    }
}

type OutputWrapper = VecDocumentOutput;
type ExpectedInput = DatabaseOutput;

impl_simple_operation!(RunCursorCommandInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl RunCursorCommandInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_run_cursor_command(&context.database(&self.database)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_run_cursor_command(&self, context: &Database) -> ResultEP<Box<dyn EpOutput>> {
        let mut cursor = context
            .run_cursor_command(self.command.to_owned().into_document(), self.options.to_owned().map(Into::into))
            .await
            .map_err(EpError::database)?;

        let mut results = vec![];
        while let Some(doc) = cursor.try_next().await.map_err(|e| EpError::request(e.to_string()))? {
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

    // NOTE: run_cursor_command passes through DocumentWrapperType (HashMap) which doesn't
    // preserve key ordering. MongoDB uses the first key as the command name. Commands with
    // multiple keys may fail if the command key doesn't sort first alphabetically.
    // Use single-key commands or commands where the name sorts first.

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_run_cursor_command_list_collections() {
        let mut ctx = MongoTestContext::new().await;

        ctx.create_collection("rcc_lc_coll").await;

        let result = ctx.run_cursor_command(doc! { "listCollections": 1 }).await;
        let arr = result.as_array().expect("run_cursor_command listCollections should return an array");

        let found = arr.iter().any(|entry| entry.get("name").and_then(|n| n.as_str()) == Some("rcc_lc_coll"));
        assert!(found, "rcc_lc_coll should appear in listCollections cursor command result");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_run_cursor_command_list_collections_multiple() {
        let mut ctx = MongoTestContext::new().await;

        ctx.create_collection("rcc_multi_a").await;
        ctx.create_collection("rcc_multi_b").await;

        let result = ctx.run_cursor_command(doc! { "listCollections": 1 }).await;
        let arr = result.as_array().expect("run_cursor_command should return an array");

        let names: Vec<&str> = arr.iter().filter_map(|e| e.get("name").and_then(|n| n.as_str())).collect();

        assert!(names.contains(&"rcc_multi_a"), "rcc_multi_a should appear");
        assert!(names.contains(&"rcc_multi_b"), "rcc_multi_b should appear");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_run_cursor_command_returns_array() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.run_cursor_command(doc! { "listCollections": 1 }).await;
        assert!(result.is_array(), "run_cursor_command should return an array");

        ctx.stop().await;
    }
}
