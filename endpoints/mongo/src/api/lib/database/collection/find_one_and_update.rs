use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, FindOneAndUpdateOptionsWrapper, UpdateModificationsWrapper};
use crate::output::OptionDocumentOutput;
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

const API_INFO: ApiInfo<MongoApi, FindOneAndUpdateInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::FindOneAndUpdate)))),
    "Atomically finds up to one document in the collection matching filter and updates it",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
   struct FindOneAndUpdateInput {
        database: String,
        collection: String,
        filter: DocumentWrapperType,
        update: UpdateModificationsWrapper,
        options: Option<FindOneAndUpdateOptionsWrapper>,
    }
}

impl_simple_operation!(FindOneAndUpdateInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl FindOneAndUpdateInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_find_one_and_update(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_find_one_and_update(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            OptionDocumentOutput(
                context
                    .find_one_and_update(
                        self.filter.to_owned().into_document(),
                        self.update.to_owned(),
                        self.options.to_owned().map(Into::into),
                    )
                    .await
                    .map_err(EpError::database)?,
            )
            .to_output(),
        ) as Box<dyn EpOutput>)
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    #![allow(unexpected_cfgs)]
    use crate::api::wrapper::{DocumentWrapper, FindOneAndUpdateOptionsWrapper, ReturnDocumentWrapper};
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_update_return_before() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("foau_ret_before", doc! { "_id": "a", "name": "Alice", "age": 30 }).await;

        let result = ctx.find_one_and_update("foau_ret_before", doc! { "_id": "a" }, doc! { "$set": { "age": 31 } }, None).await;

        assert!(!result.is_null(), "find_one_and_update should return the original document by default");
        assert_eq!(result["_id"], "a");
        assert_eq!(result["name"], "Alice");
        assert_eq!(result["age"], 30, "default return should be the document before update");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_update_return_after() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("foau_ret_after", doc! { "_id": "a", "name": "Alice", "age": 30 }).await;

        let options = FindOneAndUpdateOptionsWrapper {
            return_document: Some(ReturnDocumentWrapper::After),
            array_filters: None,
            bypass_document_validation: None,
            max_time: None,
            projection: None,
            sort: None,
            upsert: None,
            write_concern: None,
            collation: None,
            hint: None,
            let_vars: None,
            comment: None,
        };

        let result = ctx.find_one_and_update("foau_ret_after", doc! { "_id": "a" }, doc! { "$set": { "age": 31 } }, Some(options)).await;

        assert!(!result.is_null(), "find_one_and_update with After should return the updated document");
        assert_eq!(result["_id"], "a");
        assert_eq!(result["name"], "Alice");
        assert_eq!(result["age"], 31, "return_document=After should return the document after update");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_update_no_match() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .find_one_and_update("foau_no_match", doc! { "_id": "nonexistent" }, doc! { "$set": { "name": "Ghost" } }, None)
            .await;

        assert!(result.is_null(), "find_one_and_update with no matching doc should return null");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_update_inc() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("foau_inc", doc! { "_id": "counter1", "count": 10 }).await;

        let options = FindOneAndUpdateOptionsWrapper {
            return_document: Some(ReturnDocumentWrapper::After),
            array_filters: None,
            bypass_document_validation: None,
            max_time: None,
            projection: None,
            sort: None,
            upsert: None,
            write_concern: None,
            collation: None,
            hint: None,
            let_vars: None,
            comment: None,
        };

        let result = ctx.find_one_and_update("foau_inc", doc! { "_id": "counter1" }, doc! { "$inc": { "count": 5 } }, Some(options)).await;

        assert!(!result.is_null(), "find_one_and_update with $inc should return the document");
        assert_eq!(result["_id"], "counter1");
        assert_eq!(result["count"], 15, "$inc by 5 should change count from 10 to 15");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_update_verify_changed() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("foau_verify", doc! { "_id": "v1", "status": "pending", "priority": 1 }).await;

        ctx.find_one_and_update("foau_verify", doc! { "_id": "v1" }, doc! { "$set": { "status": "completed", "priority": 3 } }, None)
            .await;

        let result = ctx.find_one("foau_verify", Some(doc! { "_id": "v1" })).await;
        assert!(!result.is_null(), "document should still exist after update");
        assert_eq!(result["status"], "completed", "status should be updated in the database");
        assert_eq!(result["priority"], 3, "priority should be updated in the database");

        ctx.stop().await;
    }

    /// Rate limiting via atomic counter: use find_one_and_update with `$inc` to
    /// atomically increment a request counter, returning the document after update
    /// to check the new value.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_update_atomic_counter() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "foau_counter",
            doc! {
                "_id": "api_key_1",
                "requests_today": 99,
                "limit": 100
            },
        )
        .await;

        let after_options = FindOneAndUpdateOptionsWrapper {
            return_document: Some(ReturnDocumentWrapper::After),
            array_filters: None,
            bypass_document_validation: None,
            max_time: None,
            projection: None,
            sort: None,
            upsert: None,
            write_concern: None,
            collation: None,
            hint: None,
            let_vars: None,
            comment: None,
        };

        // First increment: 99 -> 100
        let result1 = ctx
            .find_one_and_update(
                "foau_counter",
                doc! { "_id": "api_key_1" },
                doc! { "$inc": { "requests_today": 1 } },
                Some(after_options.clone()),
            )
            .await;
        assert!(!result1.is_null(), "should return the updated document");
        assert_eq!(result1["requests_today"], 100, "requests_today should be 100 after first increment");
        assert_eq!(result1["limit"], 100, "limit should remain unchanged");

        // Second increment: 100 -> 101
        let result2 = ctx
            .find_one_and_update(
                "foau_counter",
                doc! { "_id": "api_key_1" },
                doc! { "$inc": { "requests_today": 1 } },
                Some(after_options),
            )
            .await;
        assert!(!result2.is_null(), "should return the updated document");
        assert_eq!(result2["requests_today"], 101, "requests_today should be 101 after second increment");

        ctx.stop().await;
    }

    /// Job queue dequeue pattern: use find_one_and_update with sort to atomically
    /// claim the highest-priority pending job by setting its status to "processing".
    /// Repeated calls should process jobs in priority order.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_update_queue_dequeue() {
        let mut ctx = MongoTestContext::new().await;

        // Seed 3 pending jobs with different priorities (higher number = higher priority)
        ctx.insert_one(
            "foau_queue",
            doc! {
                "_id": "job_a", "status": "pending", "priority": 1,
                "payload": { "task": "send_email", "to": "user@example.com" }
            },
        )
        .await;
        ctx.insert_one(
            "foau_queue",
            doc! {
                "_id": "job_b", "status": "pending", "priority": 10,
                "payload": { "task": "generate_report", "format": "pdf" }
            },
        )
        .await;
        ctx.insert_one(
            "foau_queue",
            doc! {
                "_id": "job_c", "status": "pending", "priority": 5,
                "payload": { "task": "resize_image", "width": 800 }
            },
        )
        .await;

        // Dequeue highest priority pending job (priority desc = -1)
        let options_high_priority = FindOneAndUpdateOptionsWrapper {
            return_document: Some(ReturnDocumentWrapper::After),
            array_filters: None,
            bypass_document_validation: None,
            max_time: None,
            projection: None,
            sort: Some(DocumentWrapper::from(doc! { "priority": -1 })),
            upsert: None,
            write_concern: None,
            collation: None,
            hint: None,
            let_vars: None,
            comment: None,
        };

        let first_job = ctx
            .find_one_and_update(
                "foau_queue",
                doc! { "status": "pending" },
                doc! { "$set": { "status": "processing" } },
                Some(options_high_priority.clone()),
            )
            .await;
        assert!(!first_job.is_null(), "should return the highest-priority job");
        assert_eq!(first_job["_id"], "job_b", "job_b (priority 10) should be dequeued first");
        assert_eq!(first_job["status"], "processing", "status should be set to processing");
        assert_eq!(first_job["priority"], 10);

        // Dequeue next highest priority pending job
        let second_job = ctx
            .find_one_and_update(
                "foau_queue",
                doc! { "status": "pending" },
                doc! { "$set": { "status": "processing" } },
                Some(options_high_priority),
            )
            .await;
        assert!(!second_job.is_null(), "should return the next highest-priority job");
        assert_eq!(second_job["_id"], "job_c", "job_c (priority 5) should be dequeued second");
        assert_eq!(second_job["status"], "processing", "status should be set to processing");

        // Verify remaining pending count
        let pending_count = ctx.count_documents("foau_queue", Some(doc! { "status": "pending" })).await;
        assert_eq!(
            pending_count.as_u64().expect("count should be a number"),
            1,
            "1 pending job should remain after dequeuing 2"
        );

        ctx.stop().await;
    }

    /// Inventory reservation with conditional update: decrement stock and increment
    /// reserved count, but only when stock > 0. After exhausting stock, the filter
    /// should stop matching and return null.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_update_conditional_update() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "foau_reserve",
            doc! {
                "_id": "p1",
                "name": "Limited Edition Widget",
                "stock": 5,
                "reserved": 0
            },
        )
        .await;

        let after_options = FindOneAndUpdateOptionsWrapper {
            return_document: Some(ReturnDocumentWrapper::After),
            array_filters: None,
            bypass_document_validation: None,
            max_time: None,
            projection: None,
            sort: None,
            upsert: None,
            write_concern: None,
            collation: None,
            hint: None,
            let_vars: None,
            comment: None,
        };

        // Reserve 5 items one at a time
        for i in 1..=5 {
            let result = ctx
                .find_one_and_update(
                    "foau_reserve",
                    doc! { "_id": "p1", "stock": { "$gt": 0 } },
                    doc! { "$inc": { "stock": -1, "reserved": 1 } },
                    Some(after_options.clone()),
                )
                .await;
            assert!(!result.is_null(), "reservation {i} should succeed");
            assert_eq!(result["stock"], 5 - i as u64, "stock should be {} after reservation {i}", 5 - i);
            assert_eq!(result["reserved"], i as u64, "reserved should be {i} after reservation {i}");
        }

        // 6th attempt: stock is now 0, filter should not match
        let result_exhausted = ctx
            .find_one_and_update(
                "foau_reserve",
                doc! { "_id": "p1", "stock": { "$gt": 0 } },
                doc! { "$inc": { "stock": -1, "reserved": 1 } },
                Some(after_options),
            )
            .await;
        assert!(result_exhausted.is_null(), "6th reservation attempt should return null because stock is 0");

        // Verify final state of the document
        let final_state = ctx.find_one("foau_reserve", Some(doc! { "_id": "p1" })).await;
        assert_eq!(final_state["stock"], 0, "stock should be 0 after all reservations");
        assert_eq!(final_state["reserved"], 5, "reserved should be 5 after all reservations");

        ctx.stop().await;
    }
}
