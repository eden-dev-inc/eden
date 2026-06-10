use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DeleteOptionsWrapper, DocumentFunction, DocumentWrapperType};
use crate::output::{CollectionDocumentOutput, DeleteResultOutput, DeleteResultWrapper};
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

const API_INFO: ApiInfo<MongoApi, DeleteOneInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::DeleteOne)))),
    "Deletes up to one document found matching query",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DeleteOneInput {
        database: String,
        collection: String,
        filter: DocumentWrapperType,
        options: Option<DeleteOptionsWrapper>,
    }
}

type OutputWrapper = DeleteResultOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(DeleteOneInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DeleteOneInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("mongo.{}.{}", self.kind(), function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_delete_one(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_delete_one(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            DeleteResultOutput(DeleteResultWrapper::from(
                context
                    .delete_one(self.filter.to_owned().into_document(), self.options.to_owned().map(Into::into))
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
    async fn test_delete_one_basic() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("do_basic", doc! { "_id": "a", "name": "Alice" }).await;
        ctx.insert_one("do_basic", doc! { "_id": "b", "name": "Bob" }).await;
        ctx.insert_one("do_basic", doc! { "_id": "c", "name": "Charlie" }).await;

        let result = ctx.delete_one("do_basic", doc! { "name": "Bob" }).await;
        assert_eq!(result["deleted_count"], 1, "should delete exactly one document");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_one_no_match() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.delete_one("do_no_match", doc! { "name": "Nobody" }).await;
        assert_eq!(result["deleted_count"], 0, "should delete nothing from empty collection");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_one_by_id() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("do_by_id", doc! { "_id": "custom_id_1", "value": 42 }).await;

        let result = ctx.delete_one("do_by_id", doc! { "_id": "custom_id_1" }).await;
        assert_eq!(result["deleted_count"], 1, "should delete the document by _id");

        let remaining = ctx.find("do_by_id", None).await;
        let arr = remaining.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 0, "collection should be empty after deleting the only document");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_one_only_deletes_first() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("do_first_only", doc! { "_id": "a", "status": "active" }).await;
        ctx.insert_one("do_first_only", doc! { "_id": "b", "status": "active" }).await;
        ctx.insert_one("do_first_only", doc! { "_id": "c", "status": "active" }).await;

        let result = ctx.delete_one("do_first_only", doc! { "status": "active" }).await;
        assert_eq!(result["deleted_count"], 1, "delete_one should remove only one document even when multiple match");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_one_verify_remaining() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("do_remaining", doc! { "_id": "a", "name": "Alice" }).await;
        ctx.insert_one("do_remaining", doc! { "_id": "b", "name": "Bob" }).await;
        ctx.insert_one("do_remaining", doc! { "_id": "c", "name": "Charlie" }).await;

        ctx.delete_one("do_remaining", doc! { "name": "Bob" }).await;

        let remaining = ctx.find("do_remaining", None).await;
        let arr = remaining.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "two documents should remain after deleting one");

        let names: Vec<&str> = arr.iter().map(|d| d["name"].as_str().expect("name should be a string")).collect();
        assert!(names.contains(&"Alice"), "Alice should remain");
        assert!(names.contains(&"Charlie"), "Charlie should remain");
        assert!(!names.contains(&"Bob"), "Bob should have been deleted");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_one_nested_filter() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "do_nested",
            doc! {
                "_id": "a",
                "user": { "role": "admin", "level": 5 }
            },
        )
        .await;
        ctx.insert_one(
            "do_nested",
            doc! {
                "_id": "b",
                "user": { "role": "editor", "level": 3 }
            },
        )
        .await;
        ctx.insert_one(
            "do_nested",
            doc! {
                "_id": "c",
                "user": { "role": "admin", "level": 10 }
            },
        )
        .await;

        let result = ctx.delete_one("do_nested", doc! { "user.role": "editor" }).await;
        assert_eq!(result["deleted_count"], 1, "should delete the editor document using nested field filter");

        let remaining = ctx.find("do_nested", None).await;
        let arr = remaining.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "two documents should remain");
        for doc in arr {
            assert_eq!(doc["user"]["role"], "admin", "only admin documents should remain");
        }

        ctx.stop().await;
    }

    /// Delete a single user matching a compound `$and` filter across multiple fields.
    /// Seeds 5 users with different roles, statuses, and departments, then deletes
    /// the one user who is both a "temp" role AND "inactive" status.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_one_with_complex_filter() {
        let mut ctx = MongoTestContext::new().await;

        // Seed 5 users with varying roles, statuses, and departments
        ctx.insert_one(
            "do_complex",
            doc! {
                "_id": "u1", "name": "Alice", "role": "admin", "status": "active", "department": "engineering"
            },
        )
        .await;
        ctx.insert_one(
            "do_complex",
            doc! {
                "_id": "u2", "name": "Bob", "role": "temp", "status": "active", "department": "marketing"
            },
        )
        .await;
        ctx.insert_one(
            "do_complex",
            doc! {
                "_id": "u3", "name": "Charlie", "role": "temp", "status": "inactive", "department": "engineering"
            },
        )
        .await;
        ctx.insert_one(
            "do_complex",
            doc! {
                "_id": "u4", "name": "Diana", "role": "editor", "status": "inactive", "department": "sales"
            },
        )
        .await;
        ctx.insert_one(
            "do_complex",
            doc! {
                "_id": "u5", "name": "Eve", "role": "admin", "status": "active", "department": "sales"
            },
        )
        .await;

        // Delete the user who matches BOTH conditions: role=temp AND status=inactive
        let result = ctx
            .delete_one(
                "do_complex",
                doc! {
                    "$and": [
                        { "role": "temp" },
                        { "status": "inactive" }
                    ]
                },
            )
            .await;
        assert_eq!(result["deleted_count"], 1, "should delete exactly one document matching compound $and filter");

        // Verify total count reduced from 5 to 4
        let remaining = ctx.find("do_complex", None).await;
        let arr = remaining.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 4, "four documents should remain after deleting one");

        // Verify Charlie (the only temp+inactive user) was removed
        let names: Vec<&str> = arr.iter().map(|d| d["name"].as_str().expect("name should be a string")).collect();
        assert!(!names.contains(&"Charlie"), "Charlie (temp+inactive) should have been deleted");
        assert!(names.contains(&"Alice"), "Alice should remain");
        assert!(names.contains(&"Bob"), "Bob (temp but active) should remain");
        assert!(names.contains(&"Diana"), "Diana (inactive but editor) should remain");
        assert!(names.contains(&"Eve"), "Eve should remain");

        ctx.stop().await;
    }

    /// Shopping cart cleanup workflow: seed cart items for multiple users, delete
    /// an expired item by a compound key, then verify the correct item was removed
    /// while other items for the same user remain intact.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_one_in_workflow() {
        let mut ctx = MongoTestContext::new().await;

        // Seed shopping cart items for two users
        ctx.insert_one(
            "do_cart",
            doc! {
                "_id": "cart1", "user_id": "user_a", "product_id": "prod_101",
                "quantity": 2, "added_at": "2025-01-15T10:00:00Z"
            },
        )
        .await;
        ctx.insert_one(
            "do_cart",
            doc! {
                "_id": "cart2", "user_id": "user_a", "product_id": "prod_202",
                "quantity": 1, "added_at": "2025-02-20T14:30:00Z"
            },
        )
        .await;
        ctx.insert_one(
            "do_cart",
            doc! {
                "_id": "cart3", "user_id": "user_a", "product_id": "prod_303",
                "quantity": 3, "added_at": "2024-11-01T08:00:00Z"
            },
        )
        .await;
        ctx.insert_one(
            "do_cart",
            doc! {
                "_id": "cart4", "user_id": "user_b", "product_id": "prod_101",
                "quantity": 1, "added_at": "2025-03-10T16:00:00Z"
            },
        )
        .await;

        // Delete the expired cart item for user_a with product_id prod_303
        let result = ctx
            .delete_one(
                "do_cart",
                doc! {
                    "user_id": "user_a",
                    "product_id": "prod_303"
                },
            )
            .await;
        assert_eq!(result["deleted_count"], 1, "should delete the expired cart item");

        // Verify user_a's other items remain
        let user_a_items = ctx.find("do_cart", Some(doc! { "user_id": "user_a" })).await;
        let arr = user_a_items.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "user_a should still have 2 cart items after removing 1");

        let product_ids: Vec<&str> = arr.iter().map(|d| d["product_id"].as_str().expect("product_id should be a string")).collect();
        assert!(product_ids.contains(&"prod_101"), "prod_101 should remain for user_a");
        assert!(product_ids.contains(&"prod_202"), "prod_202 should remain for user_a");
        assert!(!product_ids.contains(&"prod_303"), "prod_303 should have been deleted");

        // Verify the deleted item cannot be found at all
        let deleted = ctx.find_one("do_cart", Some(doc! { "_id": "cart3" })).await;
        assert!(deleted.is_null(), "deleted cart item should not be findable");

        // Verify user_b's items are unaffected
        let user_b_items = ctx.find("do_cart", Some(doc! { "user_id": "user_b" })).await;
        let arr_b = user_b_items.as_array().expect("find should return an array");
        assert_eq!(arr_b.len(), 1, "user_b should still have their 1 cart item");

        ctx.stop().await;
    }
}
