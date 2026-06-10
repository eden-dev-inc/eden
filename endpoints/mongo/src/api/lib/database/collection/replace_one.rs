use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, ReplaceOptionsWrapper};
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

const API_INFO: ApiInfo<MongoApi, ReplaceOneInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::ReplaceOne)))),
    "Replaces up to one document matching query in the collection with replacement",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct ReplaceOneInput {
        database: String,
        collection: String,
        query: DocumentWrapperType,
        replacement: DocumentWrapperType,
        options: Option<ReplaceOptionsWrapper>,
    }
}

type OutputWrapper = UpdateResultOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(ReplaceOneInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl ReplaceOneInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_replace_one(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_replace_one(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            UpdateResultOutput(UpdateResultWrapper::from(
                context
                    .replace_one(
                        self.query.to_owned().into_document(),
                        &self.replacement.to_owned().into_document(),
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
    use crate::api::wrapper::ReplaceOptionsWrapper;
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_replace_one_basic() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("ro_basic", doc! { "_id": "r1", "name": "Alice", "age": 30 }).await;

        let result = ctx.replace_one("ro_basic", doc! { "_id": "r1" }, doc! { "_id": "r1", "name": "Bob", "age": 25 }).await;

        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_replace_one_no_match() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.replace_one("ro_nomatch", doc! { "_id": "nonexistent" }, doc! { "_id": "nonexistent", "name": "Ghost" }).await;

        assert_eq!(result["matched_count"], 0);
        assert_eq!(result["modified_count"], 0);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_replace_one_upsert() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .replace_one_with_options(
                "ro_upsert",
                doc! { "_id": "u1" },
                doc! { "_id": "u1", "name": "Upserted" },
                ReplaceOptionsWrapper { upsert: Some(true), ..Default::default() },
            )
            .await;

        assert!(
            result["upserted_id"].is_object() || result["upserted_id"].is_string(),
            "upserted_id should not be null when upserting a new document"
        );

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_replace_one_verify_data() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("ro_verify", doc! { "_id": "v1", "name": "Alice", "age": 30 }).await;

        ctx.replace_one("ro_verify", doc! { "_id": "v1" }, doc! { "_id": "v1", "name": "Bob", "age": 25 }).await;

        let found = ctx.find_one("ro_verify", Some(doc! { "_id": "v1" })).await;
        assert_eq!(found["name"], "Bob");
        assert_eq!(found["age"], 25);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_replace_one_preserves_id() {
        let mut ctx = MongoTestContext::new().await;

        let insert_result = ctx.insert_one("ro_preserve_id", doc! { "name": "Alice", "age": 30 }).await;
        let original_id = insert_result["inserted_id"].clone();

        ctx.replace_one("ro_preserve_id", doc! { "name": "Alice" }, doc! { "name": "Bob", "age": 25 }).await;

        let found = ctx.find_one("ro_preserve_id", None).await;
        assert_eq!(found["_id"], original_id, "_id should be preserved after replace");
        assert_eq!(found["name"], "Bob");

        ctx.stop().await;
    }

    /// Schema migration simulation: seed a v1 document with old field names, then
    /// replace it entirely with a v2 document that has a completely different structure.
    /// Verify old fields are gone, new fields are present, and `_id` is preserved.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_replace_one_full_document_swap() {
        let mut ctx = MongoTestContext::new().await;

        // Seed a v1 document with legacy field names
        ctx.insert_one(
            "ro_schema_migrate",
            doc! {
                "_id": "cfg_001",
                "user_name": "jdoe",
                "user_email": "jdoe@example.com",
                "prefs": { "color_theme": "blue", "font_size": 14 },
                "schema_version": 1
            },
        )
        .await;

        // Replace with v2 document: completely different structure
        let result = ctx
            .replace_one(
                "ro_schema_migrate",
                doc! { "_id": "cfg_001" },
                doc! {
                    "_id": "cfg_001",
                    "username": "jdoe",
                    "contact": { "email": "jdoe@example.com", "phone": "+1-555-0100" },
                    "settings": {
                        "theme": "dark",
                        "font": { "size": 16, "family": "monospace" },
                        "notifications": true
                    },
                    "schema_version": 2
                },
            )
            .await;
        assert_eq!(result["matched_count"], 1);
        assert_eq!(result["modified_count"], 1);

        // Verify the replacement
        let found = ctx.find_one("ro_schema_migrate", Some(doc! { "_id": "cfg_001" })).await;
        assert!(!found.is_null(), "document should still exist after replacement");

        // _id preserved
        assert_eq!(found["_id"], "cfg_001", "_id should be preserved");

        // New v2 fields present
        assert_eq!(found["username"], "jdoe", "new username field should be present");
        assert_eq!(found["contact"]["email"], "jdoe@example.com", "nested contact.email should be present");
        assert_eq!(found["contact"]["phone"], "+1-555-0100", "nested contact.phone should be present");
        assert_eq!(found["settings"]["theme"], "dark", "settings.theme should be present");
        assert_eq!(found["settings"]["font"]["size"], 16, "settings.font.size should be present");
        assert_eq!(found["settings"]["notifications"], true, "settings.notifications should be present");
        assert_eq!(found["schema_version"], 2, "schema_version should be updated to 2");

        // Old v1 fields should be gone
        assert!(
            found.get("user_name").is_none() || found["user_name"].is_null(),
            "old user_name field should not exist in replaced document"
        );
        assert!(
            found.get("user_email").is_none() || found["user_email"].is_null(),
            "old user_email field should not exist in replaced document"
        );
        assert!(
            found.get("prefs").is_none() || found["prefs"].is_null(),
            "old prefs field should not exist in replaced document"
        );

        ctx.stop().await;
    }

    /// Config management via upsert: first upsert creates a new config doc (no match),
    /// then a second upsert replaces the existing doc. Verify only 1 doc exists
    /// with the latest configuration values.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_replace_one_upsert_create_then_update() {
        let mut ctx = MongoTestContext::new().await;

        // First upsert: no existing doc, should create
        let result1 = ctx
            .replace_one_with_options(
                "ro_upsert_cfg",
                doc! { "_id": "app_config" },
                doc! {
                    "_id": "app_config",
                    "max_retries": 3,
                    "timeout_ms": 5000,
                    "feature_flags": { "dark_mode": false, "beta_features": false }
                },
                ReplaceOptionsWrapper { upsert: Some(true), ..Default::default() },
            )
            .await;

        // First upsert should report an upserted_id (new document created)
        assert!(
            result1["upserted_id"].is_object() || result1["upserted_id"].is_string(),
            "first upsert should create a new document and return upserted_id"
        );

        // Verify the document was created
        let found1 = ctx.find_one("ro_upsert_cfg", Some(doc! { "_id": "app_config" })).await;
        assert_eq!(found1["max_retries"], 3, "initial config max_retries should be 3");
        assert_eq!(found1["timeout_ms"], 5000, "initial config timeout_ms should be 5000");

        // Second upsert: doc exists now, should replace it
        let result2 = ctx
            .replace_one_with_options(
                "ro_upsert_cfg",
                doc! { "_id": "app_config" },
                doc! {
                    "_id": "app_config",
                    "max_retries": 5,
                    "timeout_ms": 10000,
                    "feature_flags": { "dark_mode": true, "beta_features": true },
                    "log_level": "debug"
                },
                ReplaceOptionsWrapper { upsert: Some(true), ..Default::default() },
            )
            .await;
        assert_eq!(result2["matched_count"], 1, "second upsert should match the existing document");
        assert_eq!(result2["modified_count"], 1, "second upsert should modify the existing document");

        // Verify only 1 document exists with latest config
        let count = ctx.count_documents("ro_upsert_cfg", None).await;
        assert_eq!(
            count.as_u64().expect("count should be a number"),
            1,
            "should have exactly 1 config document after two upserts"
        );

        let found2 = ctx.find_one("ro_upsert_cfg", Some(doc! { "_id": "app_config" })).await;
        assert_eq!(found2["max_retries"], 5, "updated config max_retries should be 5");
        assert_eq!(found2["timeout_ms"], 10000, "updated config timeout_ms should be 10000");
        assert_eq!(found2["feature_flags"]["dark_mode"], true, "dark_mode should be enabled");
        assert_eq!(found2["feature_flags"]["beta_features"], true, "beta_features should be enabled");
        assert_eq!(found2["log_level"], "debug", "new log_level field should be present");

        ctx.stop().await;
    }
}
