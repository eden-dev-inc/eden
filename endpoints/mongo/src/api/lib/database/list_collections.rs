use crate::api::lib::{DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, ListCollectionsOptionsWrapper};
use crate::output::{DatabaseOutput, VecCollectionSpecificationOutput};
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

const API_INFO: ApiInfo<MongoApi, ListCollectionsInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::ListCollections)),
    "Retrieves detailed information about all collections in the specified database, including metadata, options, and statistics. Returns comprehensive collection documents with names, types, creation options, indexes, and size information via a cursor",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct ListCollectionsInput {
        database: String,
        filter: Option<DocumentWrapperType>,
        options: Option<ListCollectionsOptionsWrapper>,
    }
}

type OutputWrapper = VecCollectionSpecificationOutput;
type ExpectedInput = DatabaseOutput;

impl_simple_operation!(ListCollectionsInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl ListCollectionsInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_list_collections(&context.database(&self.database)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_list_collections(&self, context: &Database) -> ResultEP<Box<dyn EpOutput>> {
        let mut cursor = context
            .list_collections(self.filter.to_owned().map(DocumentFunction::into_document), self.options.to_owned().map(Into::into))
            .await
            .map_err(EpError::database)?;

        let mut results = vec![];
        while let Some(doc) = cursor.try_next().await.map_err(EpError::request)? {
            results.push(doc)
        }

        Ok(Box::new(VecCollectionSpecificationOutput(results).to_output()) as Box<dyn EpOutput>)
    }
}

#[cfg(test)]
mod list_collections_test {
    #![allow(unexpected_cfgs)]

    use super::*;
    use crate::api::lib::create_collection_examples;
    use crate::request::MongoRequest;
    use crate::test_utils::database_test_utils::{connect_to_mongo, generic_write_async_test};
    use endpoint_test_utils::database_test_utils::generic_read;

    #[tokio::test]
    async fn sync_test() {
        let (container, endpoint_uuid, ep, mut test_telemetry) = connect_to_mongo().await;

        let test_telemetry = &mut test_telemetry;

        generic_write_async_test(create_collection_examples().await).await;

        let output = generic_read(
            &mut MongoRequest::new(Box::new(ListCollectionsInput { database: "test_db".to_string(), filter: None, options: None })),
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

        generic_write_async_test(create_collection_examples().await).await;

        let output = generic_read(
            &mut MongoRequest::new(Box::new(ListCollectionsInput { database: "test_db".to_string(), filter: None, options: None })),
            &endpoint_uuid,
            ep,
            test_telemetry,
            true,
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
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_collections_basic() {
        let mut ctx = MongoTestContext::new().await;

        ctx.create_collection("lc_basic").await;

        let result = ctx.list_collections().await;
        let arr = result.as_array().expect("list_collections should return an array");
        let found = arr.iter().any(|entry| entry.get("name").and_then(|n| n.as_str()) == Some("lc_basic"));
        assert!(found, "lc_basic should appear in list_collections after creation");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_collections_multiple() {
        let mut ctx = MongoTestContext::new().await;

        ctx.create_collection("lc_multi_a").await;
        ctx.create_collection("lc_multi_b").await;
        ctx.create_collection("lc_multi_c").await;

        let result = ctx.list_collections().await;
        let arr = result.as_array().expect("list_collections should return an array");

        let names: Vec<&str> = arr.iter().filter_map(|entry| entry.get("name").and_then(|n| n.as_str())).collect();

        assert!(names.contains(&"lc_multi_a"), "lc_multi_a should be listed");
        assert!(names.contains(&"lc_multi_b"), "lc_multi_b should be listed");
        assert!(names.contains(&"lc_multi_c"), "lc_multi_c should be listed");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_collections_empty_db() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.list_collections().await;
        let arr = result.as_array().expect("list_collections should return an array");

        // A fresh database should have no user-created collections
        let user_collections: Vec<&str> = arr
            .iter()
            .filter_map(|entry| entry.get("name").and_then(|n| n.as_str()))
            .filter(|name| !name.starts_with("system."))
            .collect();
        assert!(
            user_collections.is_empty(),
            "fresh db should have no user-created collections, found: {:?}",
            user_collections
        );

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_collections_has_name_field() {
        let mut ctx = MongoTestContext::new().await;

        ctx.create_collection("lc_name_check").await;

        let result = ctx.list_collections().await;
        let arr = result.as_array().expect("list_collections should return an array");
        assert!(!arr.is_empty(), "list_collections should return at least one entry");

        for entry in arr {
            let name = entry.get("name");
            assert!(name.is_some(), "each collection specification should have a 'name' field");
            assert!(name.expect("checked above").is_string(), "the 'name' field should be a string");
        }

        ctx.stop().await;
    }
}
