use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{CountOptionsWrapper, DocumentFunction, DocumentWrapperType};
use crate::output::U64Output;
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

const API_INFO: ApiInfo<MongoApi, CountDocumentsInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::CountDocuments)))),
    "Gets the number of documents matching filter",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct CountDocumentsInput {
        database: String,
        collection: String,
        filter: Option<DocumentWrapperType>,
        options: Option<CountOptionsWrapper>,
    }
}

impl_simple_operation!(CountDocumentsInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl CountDocumentsInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_count_documents(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_count_documents(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        let count = context
            .count_documents(self.filter.to_owned().map(DocumentFunction::into_document), self.options.to_owned().map(Into::into))
            .await
            .map_err(EpError::database)?;

        Ok(Box::new(U64Output(count).to_output()) as Box<dyn EpOutput>)
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
    async fn test_count_documents_basic() {
        let mut ctx = MongoTestContext::new().await;

        for i in 0..5 {
            ctx.insert_one("cd_basic", doc! { "_id": format!("doc{}", i), "value": i }).await;
        }

        let result = ctx.count_documents("cd_basic", None).await;
        assert_eq!(result.as_u64().expect("count should be a number"), 5, "should count all 5 inserted documents");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_count_documents_with_filter() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("cd_filter", doc! { "_id": "a", "status": "active" }).await;
        ctx.insert_one("cd_filter", doc! { "_id": "b", "status": "inactive" }).await;
        ctx.insert_one("cd_filter", doc! { "_id": "c", "status": "active" }).await;
        ctx.insert_one("cd_filter", doc! { "_id": "d", "status": "active" }).await;
        ctx.insert_one("cd_filter", doc! { "_id": "e", "status": "inactive" }).await;

        let result = ctx.count_documents("cd_filter", Some(doc! { "status": "active" })).await;
        assert_eq!(result.as_u64().expect("count should be a number"), 3, "should count only active documents");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_count_documents_empty_collection() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.count_documents("cd_empty", None).await;
        assert_eq!(result.as_u64().expect("count should be a number"), 0, "empty collection should have count 0");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_count_documents_after_delete() {
        let mut ctx = MongoTestContext::new().await;

        for i in 0..5 {
            ctx.insert_one("cd_delete", doc! { "_id": format!("doc{}", i), "value": i }).await;
        }

        // Delete documents where value < 3
        ctx.delete_many("cd_delete", doc! { "value": { "$lt": 3 } }).await;

        let result = ctx.count_documents("cd_delete", None).await;
        assert_eq!(
            result.as_u64().expect("count should be a number"),
            2,
            "should count 2 remaining documents after deleting 3"
        );

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_count_documents_no_match_filter() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("cd_no_match", doc! { "_id": "a", "color": "red" }).await;
        ctx.insert_one("cd_no_match", doc! { "_id": "b", "color": "blue" }).await;
        ctx.insert_one("cd_no_match", doc! { "_id": "c", "color": "green" }).await;

        let result = ctx.count_documents("cd_no_match", Some(doc! { "color": "purple" })).await;
        assert_eq!(result.as_u64().expect("count should be a number"), 0, "filter matching nothing should return 0");

        ctx.stop().await;
    }

    /// Inventory report with a complex compound filter: count products that are
    /// in a specific category AND currently in stock AND priced above a threshold.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_count_documents_complex_filter() {
        let mut ctx = MongoTestContext::new().await;

        // Seed 10 products across categories with varying prices and stock status
        ctx.insert_one(
            "cd_inventory",
            doc! { "_id": "p1",  "name": "Widget A",  "category": "electronics", "price": 49, "in_stock": true },
        )
        .await;
        ctx.insert_one(
            "cd_inventory",
            doc! { "_id": "p2",  "name": "Widget B",  "category": "electronics", "price": 15, "in_stock": true },
        )
        .await;
        ctx.insert_one(
            "cd_inventory",
            doc! { "_id": "p3",  "name": "Widget C",  "category": "electronics", "price": 99, "in_stock": false },
        )
        .await;
        ctx.insert_one(
            "cd_inventory",
            doc! { "_id": "p4",  "name": "Gadget X",  "category": "home",        "price": 35, "in_stock": true },
        )
        .await;
        ctx.insert_one(
            "cd_inventory",
            doc! { "_id": "p5",  "name": "Gadget Y",  "category": "home",        "price": 75, "in_stock": true },
        )
        .await;
        ctx.insert_one(
            "cd_inventory",
            doc! { "_id": "p6",  "name": "Tool A",    "category": "electronics", "price": 30, "in_stock": true },
        )
        .await;
        ctx.insert_one(
            "cd_inventory",
            doc! { "_id": "p7",  "name": "Tool B",    "category": "electronics", "price": 20, "in_stock": true },
        )
        .await;
        ctx.insert_one(
            "cd_inventory",
            doc! { "_id": "p8",  "name": "Supply Q",  "category": "home",        "price": 10, "in_stock": false },
        )
        .await;
        ctx.insert_one(
            "cd_inventory",
            doc! { "_id": "p9",  "name": "Part Z",    "category": "electronics", "price": 55, "in_stock": true },
        )
        .await;
        ctx.insert_one(
            "cd_inventory",
            doc! { "_id": "p10", "name": "Part W",    "category": "electronics", "price": 25, "in_stock": true },
        )
        .await;

        // Count electronics that are in stock AND priced > 25
        // Matches: p1 (49), p6 (30), p9 (55) = 3
        // Not matching: p2 (15, too cheap), p3 (99 but out of stock), p7 (20, too cheap), p10 (25, not > 25)
        let result = ctx
            .count_documents(
                "cd_inventory",
                Some(doc! {
                    "category": "electronics",
                    "in_stock": true,
                    "price": { "$gt": 25 }
                }),
            )
            .await;
        assert_eq!(
            result.as_u64().expect("count should be a number"),
            3,
            "should count 3 electronics products that are in stock and priced above 25"
        );

        ctx.stop().await;
    }

    /// Status tracking workflow: seed tasks as pending, update some to completed,
    /// then verify the counts change correctly after each mutation.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_count_documents_after_updates() {
        let mut ctx = MongoTestContext::new().await;

        // Seed 8 tasks all starting as "pending"
        for i in 0..8 {
            ctx.insert_one(
                "cd_status",
                doc! {
                    "_id": format!("task_{i}"),
                    "title": format!("Task {i}"),
                    "status": "pending",
                    "assignee": format!("user_{}", i % 3)
                },
            )
            .await;
        }

        // All 8 should be pending
        let pending_count = ctx.count_documents("cd_status", Some(doc! { "status": "pending" })).await;
        assert_eq!(
            pending_count.as_u64().expect("count should be a number"),
            8,
            "all 8 tasks should initially be pending"
        );

        let completed_count = ctx.count_documents("cd_status", Some(doc! { "status": "completed" })).await;
        assert_eq!(
            completed_count.as_u64().expect("count should be a number"),
            0,
            "no tasks should be completed initially"
        );

        // Update 3 tasks to "completed"
        ctx.update_one("cd_status", doc! { "_id": "task_0" }, doc! { "$set": { "status": "completed" } }).await;
        ctx.update_one("cd_status", doc! { "_id": "task_3" }, doc! { "$set": { "status": "completed" } }).await;
        ctx.update_one("cd_status", doc! { "_id": "task_7" }, doc! { "$set": { "status": "completed" } }).await;

        // Verify pending count dropped to 5
        let pending_after = ctx.count_documents("cd_status", Some(doc! { "status": "pending" })).await;
        assert_eq!(
            pending_after.as_u64().expect("count should be a number"),
            5,
            "5 tasks should remain pending after completing 3"
        );

        // Verify completed count is now 3
        let completed_after = ctx.count_documents("cd_status", Some(doc! { "status": "completed" })).await;
        assert_eq!(completed_after.as_u64().expect("count should be a number"), 3, "3 tasks should now be completed");

        // Total should still be 8
        let total = ctx.count_documents("cd_status", None).await;
        assert_eq!(total.as_u64().expect("count should be a number"), 8, "total document count should remain 8");

        ctx.stop().await;
    }
}
