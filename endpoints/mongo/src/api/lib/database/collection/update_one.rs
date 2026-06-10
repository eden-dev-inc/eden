use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, UpdateModificationsWrapper, UpdateOptionsWrapper};
use crate::output::{CollectionDocumentOutput, UpdateResultOutput, UpdateResultWrapper};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;

struct SimpleUpdateOne;
struct ComplexUpdateOne;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, UpdateOneInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::UpdateOne)))),
    "Updates up to one document matching query in the collection",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct UpdateOneInput {
        database: String,
        collection: String,
        filter: DocumentWrapperType,
        update: UpdateModificationsWrapper,
        #[builder(default = "None")]
        options: Option<UpdateOptionsWrapper>,
    }
}

type OutputWrapper = UpdateResultOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(UpdateOneInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl UpdateOneInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_update_one(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_update_one(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            UpdateResultOutput(UpdateResultWrapper::from(
                context
                    .update_one(
                        self.filter.to_owned().into_document(),
                        self.update.to_owned(),
                        self.options.to_owned().map(Into::into),
                    )
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
    use crate::api::wrapper::UpdateOptionsWrapper;
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_set() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("uo_set", doc! { "_id": "a1", "name": "Alice", "age": 30 }).await;

        let result = ctx.update_one("uo_set", doc! { "_id": "a1" }, doc! { "$set": { "name": "Alicia" } }).await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_set", Some(doc! { "_id": "a1" })).await;
        assert_eq!(found["name"], "Alicia");
        assert_eq!(found["age"], 30);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_no_match() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("uo_no_match", doc! { "_id": "a1", "name": "Alice" }).await;

        let result = ctx.update_one("uo_no_match", doc! { "_id": "nonexistent" }, doc! { "$set": { "name": "Bob" } }).await;
        assert_eq!(result["matched_count"], 0);
        assert_eq!(result["modified_count"], 0);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_inc() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("uo_inc", doc! { "_id": "c1", "name": "Counter", "count": 10 }).await;

        let result = ctx.update_one("uo_inc", doc! { "_id": "c1" }, doc! { "$inc": { "count": 5 } }).await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_inc", Some(doc! { "_id": "c1" })).await;
        assert_eq!(found["count"], 15);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_unset() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("uo_unset", doc! { "_id": "u1", "name": "Alice", "temp_field": "remove_me" }).await;

        let result = ctx.update_one("uo_unset", doc! { "_id": "u1" }, doc! { "$unset": { "temp_field": "" } }).await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_unset", Some(doc! { "_id": "u1" })).await;
        assert_eq!(found["name"], "Alice");
        assert!(found.get("temp_field").is_none(), "temp_field should have been removed by $unset");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_multiple_operators() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("uo_multi_op", doc! { "_id": "m1", "name": "Alice", "score": 50 }).await;

        let result = ctx
            .update_one("uo_multi_op", doc! { "_id": "m1" }, doc! { "$set": { "name": "Alicia" }, "$inc": { "score": 10 } })
            .await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_multi_op", Some(doc! { "_id": "m1" })).await;
        assert_eq!(found["name"], "Alicia");
        assert_eq!(found["score"], 60);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_upsert_insert() {
        let mut ctx = MongoTestContext::new().await;

        let options = UpdateOptionsWrapper { upsert: Some(true), ..Default::default() };

        let result = ctx
            .update_one_with_options(
                "uo_upsert_ins",
                doc! { "_id": "new1" },
                doc! { "$set": { "name": "Upserted", "value": 42 } },
                options,
            )
            .await;
        assert_eq!(result["matched_count"], 0);
        assert!(
            result["upserted_id"].is_string() || result["upserted_id"].is_object(),
            "upserted_id should not be null when upserting a new document"
        );

        let found = ctx.find_one("uo_upsert_ins", Some(doc! { "_id": "new1" })).await;
        assert_eq!(found["name"], "Upserted");
        assert_eq!(found["value"], 42);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_upsert_update() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("uo_upsert_upd", doc! { "_id": "exist1", "name": "Original" }).await;

        let options = UpdateOptionsWrapper { upsert: Some(true), ..Default::default() };

        let result = ctx
            .update_one_with_options("uo_upsert_upd", doc! { "_id": "exist1" }, doc! { "$set": { "name": "Updated" } }, options)
            .await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);
        assert!(result["upserted_id"].is_null(), "upserted_id should be null when updating an existing document");

        let found = ctx.find_one("uo_upsert_upd", Some(doc! { "_id": "exist1" })).await;
        assert_eq!(found["name"], "Updated");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_push() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("uo_push", doc! { "_id": "arr1", "tags": ["a", "b"] }).await;

        let result = ctx.update_one("uo_push", doc! { "_id": "arr1" }, doc! { "$push": { "tags": "c" } }).await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_push", Some(doc! { "_id": "arr1" })).await;
        let tags = found["tags"].as_array().expect("tags should be an array");
        assert_eq!(tags.len(), 3);
        assert_eq!(tags[0], "a");
        assert_eq!(tags[1], "b");
        assert_eq!(tags[2], "c");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_pull() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("uo_pull", doc! { "_id": "arr2", "tags": ["x", "y", "z"] }).await;

        let result = ctx.update_one("uo_pull", doc! { "_id": "arr2" }, doc! { "$pull": { "tags": "y" } }).await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_pull", Some(doc! { "_id": "arr2" })).await;
        let tags = found["tags"].as_array().expect("tags should be an array");
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0], "x");
        assert_eq!(tags[1], "z");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_only_modifies_first_match() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("uo_first", doc! { "_id": "d1", "status": "pending", "order": 1 }).await;
        ctx.insert_one("uo_first", doc! { "_id": "d2", "status": "pending", "order": 2 }).await;
        ctx.insert_one("uo_first", doc! { "_id": "d3", "status": "pending", "order": 3 }).await;

        let result = ctx.update_one("uo_first", doc! { "status": "pending" }, doc! { "$set": { "status": "done" } }).await;
        assert_eq!(result["matched_count"], 1, "update_one should match exactly one document");
        assert_eq!(result["modified_count"], 1, "update_one should modify exactly one document");

        let all = ctx.find("uo_first", Some(doc! { "status": "done" })).await;
        let done_docs = all.as_array().expect("find should return an array");
        assert_eq!(done_docs.len(), 1, "only one document should have been updated to done");

        let remaining = ctx.find("uo_first", Some(doc! { "status": "pending" })).await;
        let pending_docs = remaining.as_array().expect("find should return an array");
        assert_eq!(pending_docs.len(), 2, "two documents should still be pending");

        ctx.stop().await;
    }

    /// Inventory management: $min lowers a value only when the new value is less than the
    /// current one, and $max raises it only when the new value is greater.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_min_max_operators() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "uo_minmax",
            doc! {
                "_id": "p1",
                "name": "Widget",
                "stock": 50,
                "min_stock": 10,
                "max_stock": 100
            },
        )
        .await;

        // $min: 30 < 50 so stock should drop to 30
        let result = ctx.update_one("uo_minmax", doc! { "_id": "p1" }, doc! { "$min": { "stock": 30 } }).await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_minmax", Some(doc! { "_id": "p1" })).await;
        assert_eq!(found["stock"], 30, "$min should have lowered stock from 50 to 30");

        // $max: 20 < 30 so stock should stay at 30 (no change)
        let result = ctx.update_one("uo_minmax", doc! { "_id": "p1" }, doc! { "$max": { "stock": 20 } }).await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 0, "$max with a lower value should not modify the document");

        let found = ctx.find_one("uo_minmax", Some(doc! { "_id": "p1" })).await;
        assert_eq!(found["stock"], 30, "stock should remain 30 after $max with 20");

        // $max: 80 > 30 so stock should rise to 80
        let result = ctx.update_one("uo_minmax", doc! { "_id": "p1" }, doc! { "$max": { "stock": 80 } }).await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_minmax", Some(doc! { "_id": "p1" })).await;
        assert_eq!(found["stock"], 80, "$max should have raised stock from 30 to 80");

        // Verify the rest of the document is untouched
        assert_eq!(found["name"], "Widget");
        assert_eq!(found["min_stock"], 10);
        assert_eq!(found["max_stock"], 100);

        ctx.stop().await;
    }

    /// Price adjustment: $mul multiplies a numeric field by the given factor.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_mul_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "uo_mul",
            doc! {
                "_id": "p1",
                "name": "Widget",
                "price": 100.0
            },
        )
        .await;

        // Increase price by 10%: 100.0 * 1.1 = 110.0
        let result = ctx.update_one("uo_mul", doc! { "_id": "p1" }, doc! { "$mul": { "price": 1.1 } }).await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_mul", Some(doc! { "_id": "p1" })).await;
        let price = found["price"].as_f64().expect("price should be a number");
        assert!((price - 110.0).abs() < 0.01, "price should be ~110.0 after 10% increase, got {price}");

        // Halve the price: 110.0 * 0.5 = 55.0
        let result = ctx.update_one("uo_mul", doc! { "_id": "p1" }, doc! { "$mul": { "price": 0.5 } }).await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_mul", Some(doc! { "_id": "p1" })).await;
        let price = found["price"].as_f64().expect("price should be a number");
        assert!((price - 55.0).abs() < 0.01, "price should be ~55.0 after halving, got {price}");

        ctx.stop().await;
    }

    /// Schema migration: $rename moves field values under new keys, removing the old keys.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_rename_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "uo_rename",
            doc! {
                "_id": "d1",
                "first_name": "Alice",
                "last_name": "Smith",
                "old_field": "legacy"
            },
        )
        .await;

        let result = ctx
            .update_one(
                "uo_rename",
                doc! { "_id": "d1" },
                doc! { "$rename": { "first_name": "given_name", "old_field": "migrated_field" } },
            )
            .await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_rename", Some(doc! { "_id": "d1" })).await;

        // Renamed fields should exist with the original values
        assert_eq!(found["given_name"], "Alice", "given_name should contain the former first_name value");
        assert_eq!(found["migrated_field"], "legacy", "migrated_field should contain the former old_field value");

        // Old field names should be gone
        assert!(found.get("first_name").is_none(), "first_name should no longer exist after $rename");
        assert!(found.get("old_field").is_none(), "old_field should no longer exist after $rename");

        // Untouched fields remain
        assert_eq!(found["last_name"], "Smith");

        ctx.stop().await;
    }

    /// Audit trail: $currentDate sets fields to the current date/time on the server.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_current_date() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "uo_curdate",
            doc! {
                "_id": "d1",
                "name": "Task",
                "status": "pending"
            },
        )
        .await;

        let result = ctx
            .update_one(
                "uo_curdate",
                doc! { "_id": "d1" },
                doc! {
                    "$set": { "status": "completed" },
                    "$currentDate": {
                        "completed_at": true,
                        "updated_at": { "$type": "date" }
                    }
                },
            )
            .await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_curdate", Some(doc! { "_id": "d1" })).await;
        assert_eq!(found["status"], "completed");

        // $currentDate fields should be present and non-null
        assert!(
            !found["completed_at"].is_null() && found.get("completed_at").is_some(),
            "completed_at should exist and be non-null after $currentDate"
        );
        assert!(
            !found["updated_at"].is_null() && found.get("updated_at").is_some(),
            "updated_at should exist and be non-null after $currentDate"
        );

        ctx.stop().await;
    }

    /// Leaderboard: $push with $each, $sort, and $slice keeps only the top N scores
    /// in descending order.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_push_with_each_sort_slice() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "uo_leaderboard",
            doc! {
                "_id": "board1",
                "scores": [
                    { "name": "Alice", "score": 90 },
                    { "name": "Bob", "score": 80 }
                ]
            },
        )
        .await;

        // Push two new entries, sort descending by score, keep only top 3
        let result = ctx
            .update_one(
                "uo_leaderboard",
                doc! { "_id": "board1" },
                doc! {
                    "$push": {
                        "scores": {
                            "$each": [
                                { "name": "Charlie", "score": 95 },
                                { "name": "Diana", "score": 70 }
                            ],
                            "$sort": { "score": -1 },
                            "$slice": 3
                        }
                    }
                },
            )
            .await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_leaderboard", Some(doc! { "_id": "board1" })).await;
        let scores = found["scores"].as_array().expect("scores should be an array");
        assert_eq!(scores.len(), 3, "only top 3 scores should remain after $slice");

        // Verify ordering: Charlie(95), Alice(90), Bob(80); Diana(70) sliced off
        assert_eq!(scores[0]["name"], "Charlie");
        assert_eq!(scores[0]["score"], 95);
        assert_eq!(scores[1]["name"], "Alice");
        assert_eq!(scores[1]["score"], 90);
        assert_eq!(scores[2]["name"], "Bob");
        assert_eq!(scores[2]["score"], 80);

        ctx.stop().await;
    }

    /// Tag management: $addToSet with $each adds only values that are not already present.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_add_to_set_with_each() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "uo_addtoset",
            doc! {
                "_id": "a1",
                "tags": ["rust", "backend"]
            },
        )
        .await;

        let result = ctx
            .update_one(
                "uo_addtoset",
                doc! { "_id": "a1" },
                doc! { "$addToSet": { "tags": { "$each": ["frontend", "rust", "devops"] } } },
            )
            .await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_addtoset", Some(doc! { "_id": "a1" })).await;
        let tags = found["tags"].as_array().expect("tags should be an array");
        assert_eq!(tags.len(), 4, "should have 4 unique tags: rust, backend, frontend, devops");

        // Verify all expected tags are present
        let tag_strings: Vec<&str> = tags.iter().map(|t| t.as_str().expect("tag should be a string")).collect();
        assert!(tag_strings.contains(&"rust"), "tags should contain rust");
        assert!(tag_strings.contains(&"backend"), "tags should contain backend");
        assert!(tag_strings.contains(&"frontend"), "tags should contain frontend");
        assert!(tag_strings.contains(&"devops"), "tags should contain devops");

        ctx.stop().await;
    }

    /// Queue management: $pop removes elements from the front (-1) or back (1) of an array.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_pop_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "uo_pop",
            doc! {
                "_id": "q1",
                "queue": ["first", "second", "third", "fourth"]
            },
        )
        .await;

        // Pop from front: remove "first"
        let result = ctx.update_one("uo_pop", doc! { "_id": "q1" }, doc! { "$pop": { "queue": -1 } }).await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_pop", Some(doc! { "_id": "q1" })).await;
        let queue = found["queue"].as_array().expect("queue should be an array");
        assert_eq!(queue.len(), 3);
        assert_eq!(queue[0], "second");
        assert_eq!(queue[1], "third");
        assert_eq!(queue[2], "fourth");

        // Pop from back: remove "fourth"
        let result = ctx.update_one("uo_pop", doc! { "_id": "q1" }, doc! { "$pop": { "queue": 1 } }).await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_pop", Some(doc! { "_id": "q1" })).await;
        let queue = found["queue"].as_array().expect("queue should be an array");
        assert_eq!(queue.len(), 2);
        assert_eq!(queue[0], "second");
        assert_eq!(queue[1], "third");

        ctx.stop().await;
    }

    /// User registration: $setOnInsert fields are applied only during an upsert-insert,
    /// not during a subsequent upsert-update of an existing document.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_set_on_insert_with_upsert() {
        let mut ctx = MongoTestContext::new().await;

        let options = UpdateOptionsWrapper { upsert: Some(true), ..Default::default() };

        // First upsert: document does not exist, so both $setOnInsert and $set apply
        let result = ctx
            .update_one_with_options(
                "uo_setoninsert",
                doc! { "_id": "user1" },
                doc! {
                    "$setOnInsert": { "created_at": "2024-01-01", "role": "user" },
                    "$set": { "last_login": "2024-06-15" }
                },
                options,
            )
            .await;
        assert_eq!(result["matched_count"], 0, "no existing doc, so matched_count should be 0");
        assert!(
            result["upserted_id"].is_string() || result["upserted_id"].is_object(),
            "upserted_id should be present for a new insert"
        );

        let found = ctx.find_one("uo_setoninsert", Some(doc! { "_id": "user1" })).await;
        assert_eq!(found["created_at"], "2024-01-01", "setOnInsert should set created_at on insert");
        assert_eq!(found["role"], "user", "setOnInsert should set role on insert");
        assert_eq!(found["last_login"], "2024-06-15", "$set should set last_login");

        // Second upsert: document exists, so $setOnInsert fields should NOT change
        let options = UpdateOptionsWrapper { upsert: Some(true), ..Default::default() };

        let result = ctx
            .update_one_with_options(
                "uo_setoninsert",
                doc! { "_id": "user1" },
                doc! {
                    "$setOnInsert": { "created_at": "2025-01-01", "role": "admin" },
                    "$set": { "last_login": "2024-12-25" }
                },
                options,
            )
            .await;
        assert_eq!(result["matched_count"], 1, "existing doc should be matched");
        assert_eq!(result["modified_count"], 1, "last_login should be modified");

        let found = ctx.find_one("uo_setoninsert", Some(doc! { "_id": "user1" })).await;
        assert_eq!(found["created_at"], "2024-01-01", "created_at should remain unchanged on upsert-update");
        assert_eq!(found["role"], "user", "role should remain unchanged on upsert-update");
        assert_eq!(found["last_login"], "2024-12-25", "last_login should be updated by $set");

        ctx.stop().await;
    }

    /// User profile: dot-notation updates nested fields without affecting sibling fields
    /// in the same sub-document.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_nested_field_update() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "uo_nested",
            doc! {
                "_id": "u1",
                "profile": {
                    "name": "Alice",
                    "address": {
                        "city": "NYC",
                        "zip": "10001"
                    },
                    "preferences": {
                        "theme": "dark",
                        "notifications": true
                    }
                }
            },
        )
        .await;

        let result = ctx
            .update_one(
                "uo_nested",
                doc! { "_id": "u1" },
                doc! { "$set": {
                    "profile.address.city": "Boston",
                    "profile.preferences.theme": "light"
                }},
            )
            .await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_nested", Some(doc! { "_id": "u1" })).await;

        // Updated nested fields
        assert_eq!(found["profile"]["address"]["city"], "Boston", "city should be updated to Boston");
        assert_eq!(found["profile"]["preferences"]["theme"], "light", "theme should be updated to light");

        // Untouched nested fields
        assert_eq!(found["profile"]["address"]["zip"], "10001", "zip should remain unchanged");
        assert_eq!(found["profile"]["preferences"]["notifications"], true, "notifications should remain unchanged");
        assert_eq!(found["profile"]["name"], "Alice", "name should remain unchanged");

        ctx.stop().await;
    }

    /// Order line items: positional array update via dot-notation targets a specific
    /// element by its index.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_one_array_element_by_position() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "uo_arr_pos",
            doc! {
                "_id": "order1",
                "items": [
                    { "sku": "AAA", "qty": 2, "status": "pending" },
                    { "sku": "BBB", "qty": 1, "status": "pending" },
                    { "sku": "CCC", "qty": 5, "status": "pending" }
                ]
            },
        )
        .await;

        // Update only the second element (index 1)
        let result = ctx.update_one("uo_arr_pos", doc! { "_id": "order1" }, doc! { "$set": { "items.1.status": "shipped" } }).await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        let found = ctx.find_one("uo_arr_pos", Some(doc! { "_id": "order1" })).await;
        let items = found["items"].as_array().expect("items should be an array");
        assert_eq!(items.len(), 3, "all three items should still be present");

        // Only index 1 should have changed
        assert_eq!(items[0]["status"], "pending", "first item should remain pending");
        assert_eq!(items[1]["status"], "shipped", "second item should be shipped");
        assert_eq!(items[1]["sku"], "BBB", "second item sku should be unchanged");
        assert_eq!(items[2]["status"], "pending", "third item should remain pending");

        ctx.stop().await;
    }
}
