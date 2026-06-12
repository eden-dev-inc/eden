use crate::api::lib::MongoApi;
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, ListDatabasesOptionsWrapper};
use crate::output::VecDatabaseSpecificationOutput;
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

const API_INFO: ApiInfo<MongoApi, ListDatabasesInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::ListDatabases,
    "Gets information about each database present in the cluster the Client is connected to",
    ReqType::Read,
    true,
);

static EXAMPLES: OnceCell<Vec<ApiExample<ListDatabasesInput>>> = OnceCell::const_new();

async fn examples() -> &'static [ApiExample<ListDatabasesInput>] {
    EXAMPLES
        .get_or_init(|| async {
            vec![ApiExample {
                name: "Basic List Databases",
                description: "List all databases",
                request: ListDatabasesInput { filter: None, options: None },
                response: Ok(Some(serde_json::Value::default())), // Returns 1 as the number of elements added
            }]
        })
        .await
}

crate::mongo_endpoint! {
    API_INFO,
    struct ListDatabasesInput {
        filter: Option<DocumentWrapperType>,
        options: Option<ListDatabasesOptionsWrapper>,
    }
}

impl_simple_operation!(ListDatabasesInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl ListDatabasesInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        let result = context
            .list_databases(self.filter.to_owned().map(DocumentFunction::into_document), self.options.to_owned().map(Into::into))
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(Box::new(VecDatabaseSpecificationOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        // TODO: Await the future or use tokio::spawn to avoid silently dropping it
        #[allow(clippy::let_underscore_future)]
        let _ = Box::pin(async {
            context
                .client()
                .list_databases(self.filter.to_owned().map(DocumentFunction::into_document), self.options.to_owned().map(Into::into))
                .await
        });
    }
}

#[cfg(test)]
mod list_databases_test {
    #![allow(unexpected_cfgs)]

    use super::*;
    use crate::request::MongoRequest;
    use crate::test_utils::database_test_utils::connect_to_mongo;
    use endpoint_test_utils::database_test_utils::generic_read;

    #[tokio::test]
    async fn sync_test() {
        let (container, endpoint_uuid, ep, mut test_telemetry) = connect_to_mongo().await;

        let test_telemetry = &mut test_telemetry;

        let output = generic_read(
            &mut MongoRequest::new(Box::new(ListDatabasesInput { filter: None, options: None })),
            &endpoint_uuid,
            ep,
            test_telemetry,
            true,
        )
        .await;

        println!("{:?}", output);

        container.stop().await.expect("Failed to stop database");
    }

    #[tokio::test]
    async fn async_test() {
        let (container, endpoint_uuid, ep, mut test_telemetry) = connect_to_mongo().await;

        let test_telemetry = &mut test_telemetry;

        let output = generic_read(
            &mut MongoRequest::new(Box::new(ListDatabasesInput { filter: None, options: None })),
            &endpoint_uuid,
            ep,
            test_telemetry,
            false,
        )
        .await;

        println!("{:?}", output);

        container.stop().await.expect("Failed to stop database");
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
    async fn test_list_databases_basic() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.list_databases().await;
        let arr = result.as_array().expect("list_databases should return an array");
        assert!(!arr.is_empty(), "list_databases should return at least one entry");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_databases_has_system_dbs() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.list_databases().await;
        let arr = result.as_array().expect("list_databases should return an array");

        let names: Vec<&str> = arr.iter().filter_map(|entry| entry.get("name").and_then(|n| n.as_str())).collect();

        let has_admin = names.contains(&"admin");
        let has_local = names.contains(&"local");
        assert!(has_admin || has_local, "list_databases should contain 'admin' or 'local', found: {:?}", names);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_databases_includes_test_db() {
        let mut ctx = MongoTestContext::new().await;

        // Insert data to ensure the test database is materialized
        ctx.insert_one("ld_probe", doc! { "_id": "p1", "data": "probe" }).await;

        let result = ctx.list_databases().await;
        let arr = result.as_array().expect("list_databases should return an array");

        let db_name = &ctx.db;
        let found = arr.iter().any(|entry| entry.get("name").and_then(|n| n.as_str()) == Some(db_name.as_str()));
        assert!(found, "test database '{}' should appear in list_databases after inserting data", db_name);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_databases_has_name_field() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.list_databases().await;
        let arr = result.as_array().expect("list_databases should return an array");
        assert!(!arr.is_empty(), "list_databases should return at least one entry");

        for entry in arr {
            let name = entry.get("name");
            assert!(name.is_some(), "each database specification should have a 'name' field");
            assert!(name.expect("checked above").is_string(), "the 'name' field should be a string");
        }

        ctx.stop().await;
    }
}
