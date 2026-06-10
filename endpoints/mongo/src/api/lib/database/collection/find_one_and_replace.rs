use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, FindOneAndReplaceOptionsWrapper};
use crate::output::{CollectionDocumentOutput, OptionDocumentOutput};
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

const API_INFO: ApiInfo<MongoApi, FindOneAndReplaceInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::FindOneAndReplace)))),
    "Atomically finds up to one document in the collection matching filter and replaces it with replacement",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct FindOneAndReplaceInput {
        database: String,
        collection: String,
        filter: DocumentWrapperType,
        replace: DocumentWrapperType,
        options: Option<FindOneAndReplaceOptionsWrapper>,
    }
}

type OutputWrapper = OptionDocumentOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(FindOneAndReplaceInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl FindOneAndReplaceInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_find_one_and_replace(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_find_one_and_replace(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            OptionDocumentOutput(
                context
                    .find_one_and_replace(
                        self.filter.to_owned().into_document(),
                        &self.replace.to_owned().into_document(),
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
    use crate::api::wrapper::{FindOneAndReplaceOptionsWrapper, ReturnDocumentWrapper};
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_replace_return_before() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("foar_ret_before", doc! { "_id": "a", "name": "Alice", "age": 30 }).await;

        let result = ctx
            .find_one_and_replace(
                "foar_ret_before",
                doc! { "_id": "a" },
                doc! { "_id": "a", "name": "Alice Updated", "age": 31, "role": "admin" },
                None,
            )
            .await;

        assert!(!result.is_null(), "find_one_and_replace should return the original document by default");
        assert_eq!(result["_id"], "a");
        assert_eq!(result["name"], "Alice");
        assert_eq!(result["age"], 30, "default return should be the document before replacement");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_replace_return_after() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("foar_ret_after", doc! { "_id": "a", "name": "Alice", "age": 30 }).await;

        let options = FindOneAndReplaceOptionsWrapper {
            return_document: Some(ReturnDocumentWrapper::After),
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

        let result = ctx
            .find_one_and_replace(
                "foar_ret_after",
                doc! { "_id": "a" },
                doc! { "_id": "a", "name": "Alice Replaced", "age": 31 },
                Some(options),
            )
            .await;

        assert!(!result.is_null(), "find_one_and_replace with After should return the replacement document");
        assert_eq!(result["_id"], "a");
        assert_eq!(result["name"], "Alice Replaced");
        assert_eq!(result["age"], 31, "return_document=After should return the document after replacement");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_replace_no_match() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .find_one_and_replace("foar_no_match", doc! { "_id": "nonexistent" }, doc! { "_id": "nonexistent", "name": "Ghost" }, None)
            .await;

        assert!(result.is_null(), "find_one_and_replace with no matching doc should return null");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_replace_verify_data() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("foar_verify", doc! { "_id": "r1", "version": 1, "content": "original" }).await;

        ctx.find_one_and_replace(
            "foar_verify",
            doc! { "_id": "r1" },
            doc! { "_id": "r1", "version": 2, "content": "replaced", "extra": "new_field" },
            None,
        )
        .await;

        let result = ctx.find_one("foar_verify", Some(doc! { "_id": "r1" })).await;
        assert!(!result.is_null(), "document should still exist after replacement");
        assert_eq!(result["version"], 2, "version should be updated in the database");
        assert_eq!(result["content"], "replaced", "content should be replaced in the database");
        assert_eq!(result["extra"], "new_field", "new field should be present in the replaced document");

        ctx.stop().await;
    }

    /// Document versioning workflow: replace a config document through multiple
    /// versions, alternating between ReturnDocument::After and ReturnDocument::Before
    /// to verify both return modes work correctly across successive replacements.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_and_replace_version_update() {
        let mut ctx = MongoTestContext::new().await;

        // Seed a v1 config document
        ctx.insert_one(
            "foar_version",
            doc! {
                "_id": "cfg1",
                "version": 1,
                "settings": { "theme": "dark", "notifications": true }
            },
        )
        .await;

        // Replace with v2, return After to see the new document
        let after_options = FindOneAndReplaceOptionsWrapper {
            return_document: Some(ReturnDocumentWrapper::After),
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

        let result_v2 = ctx
            .find_one_and_replace(
                "foar_version",
                doc! { "_id": "cfg1" },
                doc! {
                    "_id": "cfg1",
                    "version": 2,
                    "settings": { "theme": "light", "lang": "en", "notifications": false }
                },
                Some(after_options),
            )
            .await;

        assert!(!result_v2.is_null(), "should return the replacement document (v2)");
        assert_eq!(result_v2["_id"], "cfg1", "_id should be preserved");
        assert_eq!(result_v2["version"], 2, "version should be 2 after first replacement");
        assert_eq!(result_v2["settings"]["theme"], "light", "theme should be updated to light");
        assert_eq!(result_v2["settings"]["lang"], "en", "new lang field should be present");
        assert_eq!(result_v2["settings"]["notifications"], false, "notifications should be false");

        // Replace with v3, return Before to see v2 (the document before this replacement)
        let before_options = FindOneAndReplaceOptionsWrapper {
            return_document: Some(ReturnDocumentWrapper::Before),
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

        let result_before_v3 = ctx
            .find_one_and_replace(
                "foar_version",
                doc! { "_id": "cfg1" },
                doc! {
                    "_id": "cfg1",
                    "version": 3,
                    "settings": { "theme": "system", "lang": "fr", "notifications": true },
                    "migrated_at": "2025-06-15T00:00:00Z"
                },
                Some(before_options),
            )
            .await;

        // ReturnDocument::Before should give us the v2 document (before the v3 replacement)
        assert!(!result_before_v3.is_null(), "should return the document before replacement (v2)");
        assert_eq!(result_before_v3["version"], 2, "return_document=Before should return v2");
        assert_eq!(result_before_v3["settings"]["theme"], "light", "v2 theme should be light");
        assert_eq!(result_before_v3["settings"]["lang"], "en", "v2 lang should be en");

        // Verify the database now holds v3
        let current = ctx.find_one("foar_version", Some(doc! { "_id": "cfg1" })).await;
        assert_eq!(current["version"], 3, "database should now contain v3");
        assert_eq!(current["settings"]["theme"], "system", "v3 theme should be system");
        assert_eq!(current["settings"]["lang"], "fr", "v3 lang should be fr");
        assert_eq!(current["migrated_at"], "2025-06-15T00:00:00Z", "v3 migrated_at should be present");

        // Verify only 1 document exists (no duplicates from replacements)
        let count = ctx.count_documents("foar_version", None).await;
        assert_eq!(
            count.as_u64().expect("count should be a number"),
            1,
            "should still have exactly 1 config document after multiple replacements"
        );

        ctx.stop().await;
    }
}
