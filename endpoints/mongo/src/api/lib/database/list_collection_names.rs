use crate::api::lib::{DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType};
use crate::output::{DatabaseOutput, VecStringOutput};
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, ListCollectionNamesInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::ListCollectionNames)),
    "Retrieves the names of all collections in the specified database, with optional filtering to find collections matching specific criteria. Returns only collection names without metadata for lightweight discovery operations",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct ListCollectionNamesInput {
        database: String,
        filter: Option<DocumentWrapperType>,
    }
}

type OutputWrapper = VecStringOutput;
type ExpectedInput = DatabaseOutput;

impl_simple_operation!(ListCollectionNamesInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl ListCollectionNamesInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_list_collection_names(&context.database(&self.database)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_list_collection_names(&self, context: &Database) -> ResultEP<Box<dyn EpOutput>> {
        let result = context
            .list_collection_names(self.filter.to_owned().map(DocumentFunction::into_document))
            .await
            .map_err(EpError::database)?;

        Ok(Box::new(VecStringOutput(result).to_output()) as Box<dyn EpOutput>)
    }
}

#[cfg(test)]
mod list_collection_names_test {
    #![allow(unexpected_cfgs)]

    use super::*;
    use crate::api::lib::create_collection_examples;
    use crate::request::MongoRequest;
    use crate::test_utils::database_test_utils::{connect_to_mongo, generic_write_async_test, generic_write_sync_test};
    use endpoint_test_utils::database_test_utils::generic_read;

    #[tokio::test]
    async fn sync_test() {
        let (container, endpoint_uuid, ep, mut test_telemetry) = connect_to_mongo().await;

        let test_telemetry = &mut test_telemetry;

        generic_write_sync_test(create_collection_examples().await).await;

        let output = generic_read(
            &mut MongoRequest::new(Box::new(ListCollectionNamesInput { database: "local".to_string(), filter: None })),
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
            &mut MongoRequest::new(Box::new(ListCollectionNamesInput { database: "local".to_string(), filter: None })),
            &endpoint_uuid,
            ep,
            test_telemetry,
            false,
        )
        .await;

        println!("{:?}", output);

        container.stop().await.expect("Failed to stop database");
    }

    #[cfg(feature = "integration")]
    use crate::test_utils::integration_test_utils::MongoTestContext;
    #[cfg(feature = "integration")]
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_collection_names_basic() {
        let mut ctx = MongoTestContext::new().await;

        ctx.create_collection("lcn_basic").await;

        let names = ctx.list_collection_names().await;
        let arr = names.as_array().expect("list_collection_names should return an array");
        let found = arr.iter().any(|v| v == "lcn_basic");
        assert!(found, "lcn_basic should appear in collection names after creation");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_collection_names_multiple() {
        let mut ctx = MongoTestContext::new().await;

        ctx.create_collection("lcn_multi_a").await;
        ctx.create_collection("lcn_multi_b").await;
        ctx.create_collection("lcn_multi_c").await;

        let names = ctx.list_collection_names().await;
        let arr = names.as_array().expect("list_collection_names should return an array");

        assert!(arr.iter().any(|v| v == "lcn_multi_a"), "lcn_multi_a should be listed");
        assert!(arr.iter().any(|v| v == "lcn_multi_b"), "lcn_multi_b should be listed");
        assert!(arr.iter().any(|v| v == "lcn_multi_c"), "lcn_multi_c should be listed");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_list_collection_names_empty_db() {
        let mut ctx = MongoTestContext::new().await;

        let names = ctx.list_collection_names().await;
        let arr = names.as_array().expect("list_collection_names should return an array");
        // A fresh database may have no collections or only system collections
        assert!(
            arr.iter().all(|v| {
                let s = v.as_str().unwrap_or("");
                !s.starts_with("lcn_")
            }),
            "fresh db should have no user-created lcn_ collections"
        );

        ctx.stop().await;
    }
}
