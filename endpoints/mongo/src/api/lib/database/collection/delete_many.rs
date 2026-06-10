use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DeleteOptionsWrapper, DocumentFunction, DocumentWrapperType};
use crate::output::{CollectionDocumentOutput, DeleteResultOutput, DeleteResultWrapper};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;

struct SimpleDeleteMany;
struct ComplexDeleteMany;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, DeleteManyInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::DeleteMany)))),
    "Deletes all documents stored in the collection matching query",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DeleteManyInput {
        database: String,
        collection: String,
        query: DocumentWrapperType,
        options: Option<DeleteOptionsWrapper>,
    }
}

type OutputWrapper = DeleteResultOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(DeleteManyInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DeleteManyInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("mongo.{}.{}", self.kind(), function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_delete_many(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_delete_many(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            DeleteResultOutput(DeleteResultWrapper::from(
                context
                    .delete_many(self.query.to_owned().into_document(), self.options.to_owned().map(Into::into))
                    .await
                    .map_err(EpError::database)?,
            ))
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
    async fn test_delete_many_basic() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("dm_basic", doc! { "_id": "a", "status": "inactive" }).await;
        ctx.insert_one("dm_basic", doc! { "_id": "b", "status": "active" }).await;
        ctx.insert_one("dm_basic", doc! { "_id": "c", "status": "inactive" }).await;
        ctx.insert_one("dm_basic", doc! { "_id": "d", "status": "inactive" }).await;
        ctx.insert_one("dm_basic", doc! { "_id": "e", "status": "active" }).await;

        let result = ctx.delete_many("dm_basic", doc! { "status": "inactive" }).await;
        assert_eq!(result["deleted_count"], 3, "should delete all 3 inactive documents");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_many_all() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("dm_all", doc! { "_id": "a", "name": "Alice" }).await;
        ctx.insert_one("dm_all", doc! { "_id": "b", "name": "Bob" }).await;
        ctx.insert_one("dm_all", doc! { "_id": "c", "name": "Charlie" }).await;

        let result = ctx.delete_many("dm_all", doc! {}).await;
        assert_eq!(result["deleted_count"], 3, "empty filter should delete all documents");

        let remaining = ctx.find("dm_all", None).await;
        let arr = remaining.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 0, "collection should be empty after deleting all");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_many_no_match() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("dm_no_match", doc! { "_id": "a", "color": "red" }).await;
        ctx.insert_one("dm_no_match", doc! { "_id": "b", "color": "blue" }).await;

        let result = ctx.delete_many("dm_no_match", doc! { "color": "green" }).await;
        assert_eq!(result["deleted_count"], 0, "non-matching filter should delete nothing");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_many_empty_collection() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.delete_many("dm_empty", doc! { "anything": "value" }).await;
        assert_eq!(result["deleted_count"], 0, "deleting from empty collection should return 0");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_many_by_comparison() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("dm_comparison", doc! { "_id": "a", "score": 10 }).await;
        ctx.insert_one("dm_comparison", doc! { "_id": "b", "score": 25 }).await;
        ctx.insert_one("dm_comparison", doc! { "_id": "c", "score": 50 }).await;
        ctx.insert_one("dm_comparison", doc! { "_id": "d", "score": 75 }).await;
        ctx.insert_one("dm_comparison", doc! { "_id": "e", "score": 100 }).await;

        let result = ctx.delete_many("dm_comparison", doc! { "score": { "$gt": 30 } }).await;
        assert_eq!(result["deleted_count"], 3, "should delete documents with score > 30");

        let remaining = ctx.find("dm_comparison", None).await;
        let arr = remaining.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "two documents with score <= 30 should remain");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_many_verify_remaining() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("dm_verify", doc! { "_id": "a", "team": "red", "name": "Alice" }).await;
        ctx.insert_one("dm_verify", doc! { "_id": "b", "team": "blue", "name": "Bob" }).await;
        ctx.insert_one("dm_verify", doc! { "_id": "c", "team": "red", "name": "Charlie" }).await;
        ctx.insert_one("dm_verify", doc! { "_id": "d", "team": "red", "name": "Dave" }).await;
        ctx.insert_one("dm_verify", doc! { "_id": "e", "team": "blue", "name": "Eve" }).await;

        let result = ctx.delete_many("dm_verify", doc! { "team": "red" }).await;
        assert_eq!(result["deleted_count"], 3, "should delete all 3 red team documents");

        let remaining = ctx.find("dm_verify", None).await;
        let arr = remaining.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "two blue team documents should remain");

        let names: Vec<&str> = arr.iter().map(|d| d["name"].as_str().expect("name should be a string")).collect();
        assert!(names.contains(&"Bob"), "Bob should remain");
        assert!(names.contains(&"Eve"), "Eve should remain");

        ctx.stop().await;
    }

    /// Batch cleanup using `$in` operator: seed 8 documents with various status
    /// values, then delete all documents whose status is one of the "terminal" states.
    /// Verify only documents with valid (non-terminal) statuses survive.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_many_with_in_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("dm_in_op", doc! { "_id": "o1", "order": "ORD-001", "status": "pending" }).await;
        ctx.insert_one("dm_in_op", doc! { "_id": "o2", "order": "ORD-002", "status": "expired" }).await;
        ctx.insert_one("dm_in_op", doc! { "_id": "o3", "order": "ORD-003", "status": "completed" }).await;
        ctx.insert_one("dm_in_op", doc! { "_id": "o4", "order": "ORD-004", "status": "cancelled" }).await;
        ctx.insert_one("dm_in_op", doc! { "_id": "o5", "order": "ORD-005", "status": "processing" }).await;
        ctx.insert_one("dm_in_op", doc! { "_id": "o6", "order": "ORD-006", "status": "rejected" }).await;
        ctx.insert_one("dm_in_op", doc! { "_id": "o7", "order": "ORD-007", "status": "expired" }).await;
        ctx.insert_one("dm_in_op", doc! { "_id": "o8", "order": "ORD-008", "status": "completed" }).await;

        // Delete all orders with terminal statuses
        let result = ctx
            .delete_many(
                "dm_in_op",
                doc! {
                    "status": { "$in": ["expired", "cancelled", "rejected"] }
                },
            )
            .await;
        assert_eq!(result["deleted_count"], 4, "should delete the 4 documents with terminal statuses");

        // Verify remaining documents all have valid statuses
        let remaining = ctx.find("dm_in_op", None).await;
        let arr = remaining.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 4, "4 documents should remain");

        let statuses: Vec<&str> = arr.iter().map(|d| d["status"].as_str().expect("status should be a string")).collect();
        for status in &statuses {
            assert!(
                ["pending", "completed", "processing"].contains(status),
                "remaining document has unexpected status: {status}"
            );
        }
        assert!(!statuses.contains(&"expired"), "expired documents should have been deleted");
        assert!(!statuses.contains(&"cancelled"), "cancelled documents should have been deleted");
        assert!(!statuses.contains(&"rejected"), "rejected documents should have been deleted");

        ctx.stop().await;
    }

    /// Data retention policy: seed log entries with timestamps, delete entries
    /// older than a cutoff date using `$lt` with `mongodb::bson::DateTime`,
    /// and verify only recent entries survive.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_many_with_date_comparison() {
        use mongodb::bson::DateTime;

        let mut ctx = MongoTestContext::new().await;

        // Timestamps: old entries (before cutoff) and recent entries (after cutoff)
        // Cutoff: 2025-06-01T00:00:00Z (epoch ms = 1748736000000)
        let old_date_1 = DateTime::from_millis(1704067200000); // 2024-01-01T00:00:00Z
        let old_date_2 = DateTime::from_millis(1711929600000); // 2024-04-01T00:00:00Z
        let old_date_3 = DateTime::from_millis(1719792000000); // 2024-07-01T00:00:00Z
        let recent_date_1 = DateTime::from_millis(1751328000000); // 2025-07-01T00:00:00Z
        let recent_date_2 = DateTime::from_millis(1756598400000); // 2025-09-01T00:00:00Z
        let recent_date_3 = DateTime::from_millis(1764547200000); // 2025-12-01T00:00:00Z
        let cutoff = DateTime::from_millis(1748736000000); // 2025-06-01T00:00:00Z

        ctx.insert_one("dm_dates", doc! { "_id": "log1", "message": "old event 1", "created_at": old_date_1 }).await;
        ctx.insert_one("dm_dates", doc! { "_id": "log2", "message": "old event 2", "created_at": old_date_2 }).await;
        ctx.insert_one("dm_dates", doc! { "_id": "log3", "message": "old event 3", "created_at": old_date_3 }).await;
        ctx.insert_one("dm_dates", doc! { "_id": "log4", "message": "recent event 1", "created_at": recent_date_1 }).await;
        ctx.insert_one("dm_dates", doc! { "_id": "log5", "message": "recent event 2", "created_at": recent_date_2 }).await;
        ctx.insert_one("dm_dates", doc! { "_id": "log6", "message": "recent event 3", "created_at": recent_date_3 }).await;

        // Delete all log entries with created_at before the cutoff
        let result = ctx
            .delete_many(
                "dm_dates",
                doc! {
                    "created_at": { "$lt": cutoff }
                },
            )
            .await;
        assert_eq!(result["deleted_count"], 3, "should delete the 3 old log entries");

        // Verify only recent entries remain
        let remaining = ctx.find("dm_dates", None).await;
        let arr = remaining.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 3, "3 recent log entries should remain");

        let messages: Vec<&str> = arr.iter().map(|d| d["message"].as_str().expect("message should be a string")).collect();
        assert!(messages.contains(&"recent event 1"), "recent event 1 should remain");
        assert!(messages.contains(&"recent event 2"), "recent event 2 should remain");
        assert!(messages.contains(&"recent event 3"), "recent event 3 should remain");

        ctx.stop().await;
    }

    /// Multi-collection cascading cleanup: insert related documents in "dm_orders"
    /// and "dm_order_items", delete cancelled orders, then cascade-delete their
    /// associated order items. Verify correct counts in both collections.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_many_cascading_cleanup() {
        let mut ctx = MongoTestContext::new().await;

        // Insert orders
        ctx.insert_one(
            "dm_orders",
            doc! {
                "_id": "ord_1", "customer": "Alice", "status": "completed", "total": 150
            },
        )
        .await;
        ctx.insert_one(
            "dm_orders",
            doc! {
                "_id": "ord_2", "customer": "Bob", "status": "cancelled", "total": 75
            },
        )
        .await;
        ctx.insert_one(
            "dm_orders",
            doc! {
                "_id": "ord_3", "customer": "Charlie", "status": "cancelled", "total": 200
            },
        )
        .await;
        ctx.insert_one(
            "dm_orders",
            doc! {
                "_id": "ord_4", "customer": "Diana", "status": "processing", "total": 99
            },
        )
        .await;

        // Insert order items referencing orders
        ctx.insert_one(
            "dm_order_items",
            doc! {
                "_id": "item_1", "order_id": "ord_1", "product": "Widget A", "qty": 2
            },
        )
        .await;
        ctx.insert_one(
            "dm_order_items",
            doc! {
                "_id": "item_2", "order_id": "ord_1", "product": "Widget B", "qty": 1
            },
        )
        .await;
        ctx.insert_one(
            "dm_order_items",
            doc! {
                "_id": "item_3", "order_id": "ord_2", "product": "Gadget X", "qty": 3
            },
        )
        .await;
        ctx.insert_one(
            "dm_order_items",
            doc! {
                "_id": "item_4", "order_id": "ord_3", "product": "Gadget Y", "qty": 1
            },
        )
        .await;
        ctx.insert_one(
            "dm_order_items",
            doc! {
                "_id": "item_5", "order_id": "ord_3", "product": "Gadget Z", "qty": 5
            },
        )
        .await;
        ctx.insert_one(
            "dm_order_items",
            doc! {
                "_id": "item_6", "order_id": "ord_4", "product": "Part Q", "qty": 10
            },
        )
        .await;

        // Step 1: Delete all cancelled orders
        let orders_result = ctx.delete_many("dm_orders", doc! { "status": "cancelled" }).await;
        assert_eq!(orders_result["deleted_count"], 2, "should delete 2 cancelled orders");

        // Step 2: Cascade-delete order items for the cancelled order IDs
        let items_result = ctx
            .delete_many(
                "dm_order_items",
                doc! {
                    "order_id": { "$in": ["ord_2", "ord_3"] }
                },
            )
            .await;
        assert_eq!(items_result["deleted_count"], 3, "should delete 3 order items belonging to cancelled orders");

        // Verify remaining orders
        let remaining_orders = ctx.find("dm_orders", None).await;
        let orders_arr = remaining_orders.as_array().expect("find should return an array");
        assert_eq!(orders_arr.len(), 2, "2 non-cancelled orders should remain");
        let order_ids: Vec<&str> = orders_arr.iter().map(|d| d["_id"].as_str().expect("_id should be a string")).collect();
        assert!(order_ids.contains(&"ord_1"), "completed order should remain");
        assert!(order_ids.contains(&"ord_4"), "processing order should remain");

        // Verify remaining order items
        let remaining_items = ctx.find("dm_order_items", None).await;
        let items_arr = remaining_items.as_array().expect("find should return an array");
        assert_eq!(items_arr.len(), 3, "3 order items for non-cancelled orders should remain");
        let item_order_ids: Vec<&str> = items_arr.iter().map(|d| d["order_id"].as_str().expect("order_id should be a string")).collect();
        for oid in &item_order_ids {
            assert!(
                ["ord_1", "ord_4"].contains(oid),
                "remaining item should belong to a non-cancelled order, got: {oid}"
            );
        }

        ctx.stop().await;
    }
}
