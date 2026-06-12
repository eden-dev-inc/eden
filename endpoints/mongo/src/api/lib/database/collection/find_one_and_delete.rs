use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, FindOneAndDeleteOptionsWrapper};
use crate::output::{CollectionDocumentOutput, OptionDocumentOutput};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;

struct SimpleFindOneAndDelete;
struct ComplexFindOneAndDelete;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, FindOneAndDeleteInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::FindOneAndDelete)))),
    "Atomically finds up to one document in the collection matching filter and deletes it",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct FindOneAndDeleteInput {
        database: String,
        collection: String,
        filter: DocumentWrapperType,
        options: Option<FindOneAndDeleteOptionsWrapper>,
    }
}

type OutputWrapper = OptionDocumentOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(FindOneAndDeleteInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl FindOneAndDeleteInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_find_one_and_delete(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_find_one_and_delete(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            OptionDocumentOutput(
                context
                    .find_one_and_delete(self.filter.to_owned().into_document(), self.options.to_owned().map(Into::into))
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
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_delete_basic() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("foad_basic", doc! { "_id": "a", "name": "Alice", "age": 30 }).await;

        let result = ctx.find_one_and_delete("foad_basic", doc! { "_id": "a" }, None).await;

        assert!(!result.is_null(), "find_one_and_delete should return the deleted document");
        assert_eq!(result["_id"], "a");
        assert_eq!(result["name"], "Alice");
        assert_eq!(result["age"], 30);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_delete_no_match() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.find_one_and_delete("foad_no_match", doc! { "_id": "nonexistent" }, None).await;

        assert!(result.is_null(), "find_one_and_delete with no matching doc should return null");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_delete_verify_removed() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("foad_verify", doc! { "_id": "a", "name": "Alice" }).await;
        ctx.insert_one("foad_verify", doc! { "_id": "b", "name": "Bob" }).await;

        ctx.find_one_and_delete("foad_verify", doc! { "_id": "a" }, None).await;

        let count = ctx.count_documents("foad_verify", None).await;
        assert_eq!(count, 1, "after deleting one of two documents, count should be 1");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_delete_returns_correct_doc() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("foad_correct", doc! { "_id": "x", "color": "red", "size": 10 }).await;
        ctx.insert_one("foad_correct", doc! { "_id": "y", "color": "blue", "size": 20 }).await;
        ctx.insert_one("foad_correct", doc! { "_id": "z", "color": "green", "size": 30 }).await;

        let result = ctx.find_one_and_delete("foad_correct", doc! { "color": "blue" }, None).await;

        assert!(!result.is_null(), "find_one_and_delete should return the deleted document");
        assert_eq!(result["_id"], "y");
        assert_eq!(result["color"], "blue");
        assert_eq!(result["size"], 20);

        ctx.stop().await;
    }

    /// FIFO message queue: use find_one_and_delete with sort by timestamp ascending
    /// to atomically dequeue the oldest message. Repeated calls should drain the
    /// queue in chronological order.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_delete_fifo_queue() {
        use crate::api::wrapper::{DocumentWrapper, FindOneAndDeleteOptionsWrapper};
        use mongodb::bson::DateTime;

        let mut ctx = MongoTestContext::new().await;

        // Seed 4 messages with ascending timestamps
        let ts1 = DateTime::from_millis(1704067200000); // 2024-01-01T00:00:00Z
        let ts2 = DateTime::from_millis(1706745600000); // 2024-02-01T00:00:00Z
        let ts3 = DateTime::from_millis(1709251200000); // 2024-03-01T00:00:00Z
        let ts4 = DateTime::from_millis(1711929600000); // 2024-04-01T00:00:00Z

        ctx.insert_one("foad_fifo", doc! { "_id": "msg1", "body": "first message",  "timestamp": ts1 }).await;
        ctx.insert_one("foad_fifo", doc! { "_id": "msg2", "body": "second message", "timestamp": ts2 }).await;
        ctx.insert_one("foad_fifo", doc! { "_id": "msg3", "body": "third message",  "timestamp": ts3 }).await;
        ctx.insert_one("foad_fifo", doc! { "_id": "msg4", "body": "fourth message", "timestamp": ts4 }).await;

        // Sort by timestamp ascending to get oldest first (FIFO)
        let fifo_options = FindOneAndDeleteOptionsWrapper {
            sort: Some(DocumentWrapper::from(doc! { "timestamp": 1 })),
            max_time: None,
            projection: None,
            write_concern: None,
            collation: None,
            hint: None,
            let_vars: None,
            comment: None,
        };

        // Dequeue first (oldest) message
        let first = ctx.find_one_and_delete("foad_fifo", doc! {}, Some(fifo_options.clone())).await;
        assert!(!first.is_null(), "first dequeue should return a document");
        assert_eq!(first["_id"], "msg1", "oldest message should be dequeued first");
        assert_eq!(first["body"], "first message");

        // Dequeue second message
        let second = ctx.find_one_and_delete("foad_fifo", doc! {}, Some(fifo_options.clone())).await;
        assert!(!second.is_null(), "second dequeue should return a document");
        assert_eq!(second["_id"], "msg2", "second oldest message should be dequeued second");
        assert_eq!(second["body"], "second message");

        // Verify remaining count
        let count = ctx.count_documents("foad_fifo", None).await;
        assert_eq!(count.as_u64().expect("count should be a number"), 2, "2 messages should remain after dequeuing 2");

        // Dequeue third
        let third = ctx.find_one_and_delete("foad_fifo", doc! {}, Some(fifo_options.clone())).await;
        assert_eq!(third["_id"], "msg3", "third message should be dequeued next");

        // Dequeue fourth (last)
        let fourth = ctx.find_one_and_delete("foad_fifo", doc! {}, Some(fifo_options.clone())).await;
        assert_eq!(fourth["_id"], "msg4", "fourth message should be dequeued last");

        // Queue should now be empty
        let empty = ctx.find_one_and_delete("foad_fifo", doc! {}, Some(fifo_options)).await;
        assert!(empty.is_null(), "dequeue from empty queue should return null");

        ctx.stop().await;
    }

    /// Targeted session cleanup: find and delete an inactive session for a specific
    /// user using a compound filter. Verify the correct session is returned and
    /// removed while other sessions for the same and different users remain intact.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_delete_with_complex_filter() {
        let mut ctx = MongoTestContext::new().await;

        // Seed 5 session documents for different users with active/inactive states
        ctx.insert_one(
            "foad_sessions",
            doc! {
                "_id": "sess_1", "user_id": "user_a", "active": true,
                "last_activity": "2025-06-15T10:00:00Z", "ip": "192.168.1.10"
            },
        )
        .await;
        ctx.insert_one(
            "foad_sessions",
            doc! {
                "_id": "sess_2", "user_id": "user_a", "active": false,
                "last_activity": "2025-01-01T08:00:00Z", "ip": "192.168.1.11"
            },
        )
        .await;
        ctx.insert_one(
            "foad_sessions",
            doc! {
                "_id": "sess_3", "user_id": "user_b", "active": true,
                "last_activity": "2025-06-14T14:30:00Z", "ip": "10.0.0.5"
            },
        )
        .await;
        ctx.insert_one(
            "foad_sessions",
            doc! {
                "_id": "sess_4", "user_id": "user_b", "active": false,
                "last_activity": "2025-03-20T16:00:00Z", "ip": "10.0.0.6"
            },
        )
        .await;
        ctx.insert_one(
            "foad_sessions",
            doc! {
                "_id": "sess_5", "user_id": "user_a", "active": true,
                "last_activity": "2025-06-15T12:00:00Z", "ip": "192.168.1.12"
            },
        )
        .await;

        // Find and delete the inactive session for user_a
        let deleted = ctx.find_one_and_delete("foad_sessions", doc! { "user_id": "user_a", "active": false }, None).await;

        // Verify the correct session was returned
        assert!(!deleted.is_null(), "should return the deleted session");
        assert_eq!(deleted["_id"], "sess_2", "sess_2 is user_a's only inactive session");
        assert_eq!(deleted["user_id"], "user_a");
        assert_eq!(deleted["active"], false);
        assert_eq!(deleted["ip"], "192.168.1.11");

        // Verify user_a's active sessions remain
        let user_a_sessions = ctx.find("foad_sessions", Some(doc! { "user_id": "user_a" })).await;
        let arr_a = user_a_sessions.as_array().expect("find should return an array");
        assert_eq!(arr_a.len(), 2, "user_a should have 2 remaining sessions (both active)");
        for session in arr_a {
            assert_eq!(session["active"], true, "all remaining user_a sessions should be active");
        }

        // Verify user_b's sessions are completely unaffected
        let user_b_sessions = ctx.find("foad_sessions", Some(doc! { "user_id": "user_b" })).await;
        let arr_b = user_b_sessions.as_array().expect("find should return an array");
        assert_eq!(arr_b.len(), 2, "user_b should still have both sessions");

        // Total sessions: 5 - 1 = 4
        let total = ctx.count_documents("foad_sessions", None).await;
        assert_eq!(total.as_u64().expect("count should be a number"), 4, "total sessions should be 4 after deleting 1");

        ctx.stop().await;
    }
}
