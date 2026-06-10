use crate::api::lib::MongoApi;
use crate::api::wrapper::{DocumentWrapper, ListDatabasesOptionsWrapper};
use crate::output::VecStringOutput;
use crate::request::MongoRequest;
use crate::{ApiExample, ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use std::borrow::Cow;
use telemetry::FastSpanStatus;
use telemetry::TelemetryWrapper;
use tokio::sync::OnceCell;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, ListDatabaseNamesInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::ListDatabaseNames,
    "Gets the names of the databases present in the cluster the Client is connected to",
    ReqType::Read,
    true,
);

static EXAMPLES: OnceCell<Vec<ApiExample<ListDatabaseNamesInput>>> = OnceCell::const_new();

async fn examples() -> &'static [ApiExample<ListDatabaseNamesInput>] {
    EXAMPLES.get_or_init(|| async { vec![] }).await
}

crate::mongo_endpoint! {
    API_INFO,
    struct ListDatabaseNamesInput {
        filter: Option<DocumentWrapper>,
        options: Option<ListDatabasesOptionsWrapper>,
    }
}

type OutputWrapper = VecStringOutput;

impl_simple_operation!(ListDatabaseNamesInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl ListDatabaseNamesInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        let result = context
            .list_database_names(self.filter.to_owned().map(Into::into), self.options.to_owned().map(Into::into))
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(Box::new(VecStringOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        // TODO: Await the future or use tokio::spawn to avoid silently dropping it
        #[allow(clippy::let_underscore_future)]
        let _ = context.client().list_database_names(self.filter.to_owned().map(Into::into), self.options.to_owned().map(Into::into));
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
    async fn test_list_database_names_basic() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.list_database_names().await;
        let arr = result.as_array().expect("list_database_names should return an array");

        let has_system_db = arr.iter().any(|v| v == "admin" || v == "local");
        assert!(has_system_db, "list_database_names should contain at least 'admin' or 'local'");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_database_names_includes_test_db() {
        let mut ctx = MongoTestContext::new().await;

        // Insert a document to ensure the test_db is created
        ctx.insert_one("ldb_probe", doc! { "_id": "p1", "data": "probe" }).await;

        let result = ctx.list_database_names().await;
        let arr = result.as_array().expect("list_database_names should return an array");

        let db_name = &ctx.db;
        let found = arr.iter().any(|v| v.as_str() == Some(db_name.as_str()));
        assert!(found, "test database '{}' should appear in list_database_names after inserting data", db_name);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_database_names_returns_array() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.list_database_names().await;
        assert!(result.is_array(), "list_database_names result should be an array");

        let arr = result.as_array().expect("should be an array");
        assert!(!arr.is_empty(), "list_database_names should return at least one database");

        for name in arr {
            assert!(name.is_string(), "each database name should be a string");
        }

        ctx.stop().await;
    }
}
