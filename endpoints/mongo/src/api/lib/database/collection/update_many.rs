use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, UpdateModificationsWrapper, UpdateOptionsWrapper};
use crate::output::{CollectionDocumentOutput, UpdateResultOutput, UpdateResultWrapper};
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

const API_INFO: ApiInfo<MongoApi, UpdateManyInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::UpdateMany)))),
    "Updates all documents matching query in the collection",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct UpdateManyInput {
        database: String,
        collection: String,
        query: DocumentWrapperType,
        update: UpdateModificationsWrapper,
        options: Option<UpdateOptionsWrapper>,
    }
}

type OutputWrapper = UpdateResultOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(UpdateManyInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl UpdateManyInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_update_many(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_update_many(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            UpdateResultOutput(UpdateResultWrapper::from(
                context
                    .update_many(
                        self.query.to_owned().into_document(),
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
    async fn test_update_many_set() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("um_set", doc! { "_id": "a1", "status": "pending", "value": 1 }).await;
        ctx.insert_one("um_set", doc! { "_id": "a2", "status": "pending", "value": 2 }).await;
        ctx.insert_one("um_set", doc! { "_id": "a3", "status": "pending", "value": 3 }).await;

        let result = ctx.update_many("um_set", doc! { "status": "pending" }, doc! { "$set": { "status": "done" } }).await;
        assert_eq!(result["matched_count"], 3);
        assert_eq!(result["modified_count"], 3);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_many_no_match() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("um_no_match", doc! { "_id": "a1", "status": "active" }).await;

        let result = ctx.update_many("um_no_match", doc! { "status": "archived" }, doc! { "$set": { "status": "deleted" } }).await;
        assert_eq!(result["matched_count"], 0);
        assert_eq!(result["modified_count"], 0);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_many_partial_match() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("um_partial", doc! { "_id": "p1", "category": "A", "value": 1 }).await;
        ctx.insert_one("um_partial", doc! { "_id": "p2", "category": "A", "value": 2 }).await;
        ctx.insert_one("um_partial", doc! { "_id": "p3", "category": "A", "value": 3 }).await;
        ctx.insert_one("um_partial", doc! { "_id": "p4", "category": "B", "value": 4 }).await;
        ctx.insert_one("um_partial", doc! { "_id": "p5", "category": "B", "value": 5 }).await;

        let result = ctx.update_many("um_partial", doc! { "category": "A" }, doc! { "$set": { "updated": true } }).await;
        assert_eq!(result["matched_count"], 3);
        assert_eq!(result["modified_count"], 3);

        let category_b = ctx.find("um_partial", Some(doc! { "category": "B" })).await;
        let b_docs = category_b.as_array().expect("find should return an array");
        for d in b_docs {
            assert!(d.get("updated").is_none(), "category B docs should not have been updated");
        }

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_many_inc() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("um_inc", doc! { "_id": "i1", "group": "x", "counter": 10 }).await;
        ctx.insert_one("um_inc", doc! { "_id": "i2", "group": "x", "counter": 20 }).await;
        ctx.insert_one("um_inc", doc! { "_id": "i3", "group": "y", "counter": 30 }).await;

        let result = ctx.update_many("um_inc", doc! { "group": "x" }, doc! { "$inc": { "counter": 5 } }).await;
        assert_eq!(result["matched_count"], 2);
        assert_eq!(result["modified_count"], 2);

        let found1 = ctx.find_one("um_inc", Some(doc! { "_id": "i1" })).await;
        assert_eq!(found1["counter"], 15);

        let found2 = ctx.find_one("um_inc", Some(doc! { "_id": "i2" })).await;
        assert_eq!(found2["counter"], 25);

        let found3 = ctx.find_one("um_inc", Some(doc! { "_id": "i3" })).await;
        assert_eq!(found3["counter"], 30, "group y document should be unchanged");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_many_upsert() {
        let mut ctx = MongoTestContext::new().await;

        let options = UpdateOptionsWrapper { upsert: Some(true), ..Default::default() };

        let result = ctx
            .update_many_with_options(
                "um_upsert",
                doc! { "category": "phantom" },
                doc! { "$set": { "category": "phantom", "created": true } },
                options,
            )
            .await;
        assert_eq!(result["matched_count"], 0);
        assert!(
            result["upserted_id"].is_string() || result["upserted_id"].is_object(),
            "upserted_id should not be null when upserting a new document"
        );

        let found = ctx.find("um_upsert", Some(doc! { "category": "phantom" })).await;
        let docs = found.as_array().expect("find should return an array");
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["created"], true);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_many_add_to_set() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("um_addtoset", doc! { "_id": "s1", "role": "user", "perms": ["read"] }).await;
        ctx.insert_one("um_addtoset", doc! { "_id": "s2", "role": "user", "perms": ["read", "write"] }).await;

        let result = ctx.update_many("um_addtoset", doc! { "role": "user" }, doc! { "$addToSet": { "perms": "write" } }).await;
        assert_eq!(result["matched_count"], 2);

        let found1 = ctx.find_one("um_addtoset", Some(doc! { "_id": "s1" })).await;
        let perms1 = found1["perms"].as_array().expect("perms should be an array");
        assert_eq!(perms1.len(), 2, "s1 should have gained 'write'");
        assert!(perms1.iter().any(|v| v == "write"));

        let found2 = ctx.find_one("um_addtoset", Some(doc! { "_id": "s2" })).await;
        let perms2 = found2["perms"].as_array().expect("perms should be an array");
        assert_eq!(perms2.len(), 2, "s2 already had 'write', so $addToSet should not duplicate");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_many_empty_collection() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.update_many("um_empty", doc! { "status": "any" }, doc! { "$set": { "status": "updated" } }).await;
        assert_eq!(result["matched_count"], 0);
        assert_eq!(result["modified_count"], 0);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_many_verify_with_find() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("um_verify", doc! { "_id": "v1", "level": 1, "label": "low" }).await;
        ctx.insert_one("um_verify", doc! { "_id": "v2", "level": 2, "label": "low" }).await;
        ctx.insert_one("um_verify", doc! { "_id": "v3", "level": 3, "label": "low" }).await;

        ctx.update_many("um_verify", doc! { "label": "low" }, doc! { "$set": { "label": "high" } }).await;

        let all = ctx.find("um_verify", None).await;
        let docs = all.as_array().expect("find should return an array");
        assert_eq!(docs.len(), 3);
        for d in docs {
            assert_eq!(d["label"], "high", "every document should have label updated to high");
        }

        let low = ctx.find("um_verify", Some(doc! { "label": "low" })).await;
        let low_docs = low.as_array().expect("find should return an array");
        assert_eq!(low_docs.len(), 0, "no documents should still have label=low");

        ctx.stop().await;
    }

    /// Batch notification: $or filter matches documents where any condition is true,
    /// and only those documents receive the update.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_many_complex_filter_with_or() {
        let mut ctx = MongoTestContext::new().await;

        // Seed 8 users with varying roles and statuses
        ctx.insert_one(
            "um_or",
            doc! { "_id": "u1", "name": "Alice",   "role": "admin",  "status": "active",  "notified": false },
        )
        .await;
        ctx.insert_one(
            "um_or",
            doc! { "_id": "u2", "name": "Bob",     "role": "user",   "status": "premium", "notified": false },
        )
        .await;
        ctx.insert_one(
            "um_or",
            doc! { "_id": "u3", "name": "Charlie", "role": "user",   "status": "active",  "notified": false },
        )
        .await;
        ctx.insert_one(
            "um_or",
            doc! { "_id": "u4", "name": "Diana",   "role": "admin",  "status": "premium", "notified": false },
        )
        .await;
        ctx.insert_one(
            "um_or",
            doc! { "_id": "u5", "name": "Eve",     "role": "user",   "status": "active",  "notified": false },
        )
        .await;
        ctx.insert_one(
            "um_or",
            doc! { "_id": "u6", "name": "Frank",   "role": "user",   "status": "premium", "notified": false },
        )
        .await;
        ctx.insert_one(
            "um_or",
            doc! { "_id": "u7", "name": "Grace",   "role": "admin",  "status": "active",  "notified": false },
        )
        .await;
        ctx.insert_one(
            "um_or",
            doc! { "_id": "u8", "name": "Hank",    "role": "user",   "status": "active",  "notified": false },
        )
        .await;

        // Update docs where role=admin OR status=premium
        // Matches: u1(admin), u2(premium), u4(admin+premium), u6(premium), u7(admin) = 5 docs
        let result = ctx
            .update_many(
                "um_or",
                doc! { "$or": [ { "role": "admin" }, { "status": "premium" } ] },
                doc! { "$set": { "notified": true } },
            )
            .await;
        assert_eq!(result["matched_count"], 5, "should match all admins and premium users");
        assert_eq!(result["modified_count"], 5);

        // Verify notified users
        let notified = ctx.find("um_or", Some(doc! { "notified": true })).await;
        let notified_docs = notified.as_array().expect("find should return an array");
        assert_eq!(notified_docs.len(), 5, "5 users should have been notified");

        // Verify non-notified users (u3, u5, u8 are regular active users)
        let not_notified = ctx.find("um_or", Some(doc! { "notified": false })).await;
        let not_notified_docs = not_notified.as_array().expect("find should return an array");
        assert_eq!(not_notified_docs.len(), 3, "3 regular active users should not have been notified");

        ctx.stop().await;
    }

    /// Analytics: $inc increments numeric fields; using $in filter targets specific documents.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_many_increment_counters() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("um_inc_ctr", doc! { "_id": "pg1", "page": "/home",    "views": 100, "unique_visitors": 50 }).await;
        ctx.insert_one("um_inc_ctr", doc! { "_id": "pg2", "page": "/about",   "views": 80,  "unique_visitors": 40 }).await;
        ctx.insert_one("um_inc_ctr", doc! { "_id": "pg3", "page": "/contact", "views": 60,  "unique_visitors": 30 }).await;
        ctx.insert_one("um_inc_ctr", doc! { "_id": "pg4", "page": "/home",    "views": 200, "unique_visitors": 90 }).await;
        ctx.insert_one("um_inc_ctr", doc! { "_id": "pg5", "page": "/blog",    "views": 150, "unique_visitors": 70 }).await;

        // Increment views by 1 for /home and /about pages; unique_visitors by 0 (no-op for that field)
        let result = ctx
            .update_many(
                "um_inc_ctr",
                doc! { "page": { "$in": ["/home", "/about"] } },
                doc! { "$inc": { "views": 1, "unique_visitors": 0 } },
            )
            .await;
        assert_eq!(result["matched_count"], 3, "should match 2 /home docs + 1 /about doc");

        // Verify /home pages got incremented
        let found1 = ctx.find_one("um_inc_ctr", Some(doc! { "_id": "pg1" })).await;
        assert_eq!(found1["views"], 101, "/home pg1 views should be incremented");
        assert_eq!(found1["unique_visitors"], 50, "unique_visitors should be unchanged ($inc 0)");

        let found4 = ctx.find_one("um_inc_ctr", Some(doc! { "_id": "pg4" })).await;
        assert_eq!(found4["views"], 201, "/home pg4 views should be incremented");

        // Verify /about got incremented
        let found2 = ctx.find_one("um_inc_ctr", Some(doc! { "_id": "pg2" })).await;
        assert_eq!(found2["views"], 81, "/about views should be incremented");

        // Verify unmatched pages are unchanged
        let found3 = ctx.find_one("um_inc_ctr", Some(doc! { "_id": "pg3" })).await;
        assert_eq!(found3["views"], 60, "/contact should be unchanged");

        let found5 = ctx.find_one("um_inc_ctr", Some(doc! { "_id": "pg5" })).await;
        assert_eq!(found5["views"], 150, "/blog should be unchanged");

        ctx.stop().await;
    }

    /// Clean up: $pull removes all array elements matching a condition from every
    /// matched document.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_many_pull_from_arrays() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "um_pull",
            doc! {
                "_id": "n1",
                "user": "Alice",
                "notifications": [
                    { "msg": "Welcome", "status": "read" },
                    { "msg": "Update available", "status": "unread" },
                    { "msg": "Sale ended", "status": "read" }
                ]
            },
        )
        .await;
        ctx.insert_one(
            "um_pull",
            doc! {
                "_id": "n2",
                "user": "Bob",
                "notifications": [
                    { "msg": "New message", "status": "unread" },
                    { "msg": "Reminder", "status": "read" }
                ]
            },
        )
        .await;
        ctx.insert_one(
            "um_pull",
            doc! {
                "_id": "n3",
                "user": "Charlie",
                "notifications": [
                    { "msg": "Alert", "status": "read" },
                    { "msg": "Info", "status": "read" }
                ]
            },
        )
        .await;
        ctx.insert_one(
            "um_pull",
            doc! {
                "_id": "n4",
                "user": "Diana",
                "notifications": [
                    { "msg": "Promo", "status": "unread" },
                    { "msg": "News", "status": "unread" }
                ]
            },
        )
        .await;

        // Remove all read notifications from all documents
        let result = ctx.update_many("um_pull", doc! {}, doc! { "$pull": { "notifications": { "status": "read" } } }).await;
        assert_eq!(result["matched_count"], 4, "all 4 docs should be matched");

        // Alice: should only have "Update available" (unread)
        let found1 = ctx.find_one("um_pull", Some(doc! { "_id": "n1" })).await;
        let notifs1 = found1["notifications"].as_array().expect("notifications should be an array");
        assert_eq!(notifs1.len(), 1, "Alice should have 1 unread notification remaining");
        assert_eq!(notifs1[0]["msg"], "Update available");

        // Bob: should only have "New message" (unread)
        let found2 = ctx.find_one("um_pull", Some(doc! { "_id": "n2" })).await;
        let notifs2 = found2["notifications"].as_array().expect("notifications should be an array");
        assert_eq!(notifs2.len(), 1, "Bob should have 1 unread notification remaining");
        assert_eq!(notifs2[0]["msg"], "New message");

        // Charlie: all were read, so array should be empty
        let found3 = ctx.find_one("um_pull", Some(doc! { "_id": "n3" })).await;
        let notifs3 = found3["notifications"].as_array().expect("notifications should be an array");
        assert_eq!(notifs3.len(), 0, "Charlie should have no notifications remaining");

        // Diana: both were unread, so both should remain
        let found4 = ctx.find_one("um_pull", Some(doc! { "_id": "n4" })).await;
        let notifs4 = found4["notifications"].as_array().expect("notifications should be an array");
        assert_eq!(notifs4.len(), 2, "Diana should still have both unread notifications");

        ctx.stop().await;
    }

    /// Salary adjustment: $mul with a range filter applies a multiplier only to
    /// documents matching the condition.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_many_conditional_update_by_range() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("um_salary", doc! { "_id": "e1", "name": "Alice",   "salary": 45000.0 }).await;
        ctx.insert_one("um_salary", doc! { "_id": "e2", "name": "Bob",     "salary": 55000.0 }).await;
        ctx.insert_one("um_salary", doc! { "_id": "e3", "name": "Charlie", "salary": 40000.0 }).await;
        ctx.insert_one("um_salary", doc! { "_id": "e4", "name": "Diana",   "salary": 60000.0 }).await;
        ctx.insert_one("um_salary", doc! { "_id": "e5", "name": "Eve",     "salary": 48000.0 }).await;
        ctx.insert_one("um_salary", doc! { "_id": "e6", "name": "Frank",   "salary": 50000.0 }).await;

        // 5% raise for employees earning less than 50000
        // Matches: e1(45000), e3(40000), e5(48000) = 3 docs
        let result = ctx.update_many("um_salary", doc! { "salary": { "$lt": 50000.0 } }, doc! { "$mul": { "salary": 1.05 } }).await;
        assert_eq!(result["matched_count"], 3, "should match 3 employees below 50k");
        assert_eq!(result["modified_count"], 3);

        // Verify raises applied
        let e1 = ctx.find_one("um_salary", Some(doc! { "_id": "e1" })).await;
        let salary1 = e1["salary"].as_f64().expect("salary should be a number");
        assert!((salary1 - 47250.0).abs() < 0.01, "Alice should earn ~47250.0, got {salary1}");

        let e3 = ctx.find_one("um_salary", Some(doc! { "_id": "e3" })).await;
        let salary3 = e3["salary"].as_f64().expect("salary should be a number");
        assert!((salary3 - 42000.0).abs() < 0.01, "Charlie should earn ~42000.0, got {salary3}");

        let e5 = ctx.find_one("um_salary", Some(doc! { "_id": "e5" })).await;
        let salary5 = e5["salary"].as_f64().expect("salary should be a number");
        assert!((salary5 - 50400.0).abs() < 0.01, "Eve should earn ~50400.0, got {salary5}");

        // Verify employees at or above 50k are unchanged
        let e2 = ctx.find_one("um_salary", Some(doc! { "_id": "e2" })).await;
        let salary2 = e2["salary"].as_f64().expect("salary should be a number");
        assert!((salary2 - 55000.0).abs() < 0.01, "Bob should still earn 55000.0, got {salary2}");

        let e4 = ctx.find_one("um_salary", Some(doc! { "_id": "e4" })).await;
        let salary4 = e4["salary"].as_f64().expect("salary should be a number");
        assert!((salary4 - 60000.0).abs() < 0.01, "Diana should still earn 60000.0, got {salary4}");

        let e6 = ctx.find_one("um_salary", Some(doc! { "_id": "e6" })).await;
        let salary6 = e6["salary"].as_f64().expect("salary should be a number");
        assert!((salary6 - 50000.0).abs() < 0.01, "Frank should still earn 50000.0, got {salary6}");

        ctx.stop().await;
    }

    /// Config update: dot-notation targets a nested field across all matched documents,
    /// leaving sibling fields unchanged.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_many_nested_field_bulk() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("um_nested", doc! { "_id": "s1", "service": "auth",    "config": { "timeout": 30, "retries": 3 } }).await;
        ctx.insert_one("um_nested", doc! { "_id": "s2", "service": "payment", "config": { "timeout": 60, "retries": 2 } }).await;
        ctx.insert_one("um_nested", doc! { "_id": "s3", "service": "email",   "config": { "timeout": 15, "retries": 1 } }).await;
        ctx.insert_one("um_nested", doc! { "_id": "s4", "service": "search",  "config": { "timeout": 45, "retries": 4 } }).await;
        ctx.insert_one("um_nested", doc! { "_id": "s5", "service": "cache",   "config": { "timeout": 10, "retries": 0 } }).await;

        // Set retries to 5 for all services
        let result = ctx.update_many("um_nested", doc! {}, doc! { "$set": { "config.retries": 5 } }).await;
        assert_eq!(result["matched_count"], 5);
        assert_eq!(result["modified_count"], 5);

        // Verify all docs have retries=5 but timeout is unchanged
        let all = ctx.find("um_nested", None).await;
        let docs = all.as_array().expect("find should return an array");
        assert_eq!(docs.len(), 5);

        for d in docs {
            assert_eq!(d["config"]["retries"], 5, "retries should be 5 for service {}", d["service"]);
        }

        // Spot-check that timeouts are preserved
        let s1 = ctx.find_one("um_nested", Some(doc! { "_id": "s1" })).await;
        assert_eq!(s1["config"]["timeout"], 30, "auth timeout should remain 30");

        let s3 = ctx.find_one("um_nested", Some(doc! { "_id": "s3" })).await;
        assert_eq!(s3["config"]["timeout"], 15, "email timeout should remain 15");

        let s5 = ctx.find_one("um_nested", Some(doc! { "_id": "s5" })).await;
        assert_eq!(s5["config"]["timeout"], 10, "cache timeout should remain 10");

        ctx.stop().await;
    }

    /// Schema migration: empty filter matches all documents, allowing bulk addition
    /// of new fields to every document in the collection.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_many_add_field_to_all() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("um_migrate", doc! { "_id": "d1", "name": "Alpha",   "value": 1 }).await;
        ctx.insert_one("um_migrate", doc! { "_id": "d2", "name": "Bravo",   "value": 2 }).await;
        ctx.insert_one("um_migrate", doc! { "_id": "d3", "name": "Charlie", "value": 3 }).await;
        ctx.insert_one("um_migrate", doc! { "_id": "d4", "name": "Delta",   "value": 4 }).await;
        ctx.insert_one("um_migrate", doc! { "_id": "d5", "name": "Echo",    "value": 5 }).await;
        ctx.insert_one("um_migrate", doc! { "_id": "d6", "name": "Foxtrot", "value": 6 }).await;

        // Add version and migrated fields to all documents
        let result = ctx.update_many("um_migrate", doc! {}, doc! { "$set": { "version": 2, "migrated": true } }).await;
        assert_eq!(result["matched_count"], 6, "all 6 documents should be matched");
        assert_eq!(result["modified_count"], 6, "all 6 documents should be modified");

        // Verify all documents now have both new fields
        let all = ctx.find("um_migrate", None).await;
        let docs = all.as_array().expect("find should return an array");
        assert_eq!(docs.len(), 6);

        for d in docs {
            assert_eq!(d["version"], 2, "version should be 2 for doc {}", d["_id"]);
            assert_eq!(d["migrated"], true, "migrated should be true for doc {}", d["_id"]);
        }

        // Verify original fields are still intact
        let d1 = ctx.find_one("um_migrate", Some(doc! { "_id": "d1" })).await;
        assert_eq!(d1["name"], "Alpha", "original name should be preserved");
        assert_eq!(d1["value"], 1, "original value should be preserved");

        ctx.stop().await;
    }
}
