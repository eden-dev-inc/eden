use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{CreateIndexOptionsWrapper, IndexModelWrapper};
use crate::output::StringOutput;
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

const API_INFO: ApiInfo<MongoApi, CreateIndexInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::CreateIndex)))),
    "Creates the given index on this collection",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct CreateIndexInput {
        database: String,
        collection: String,
        model: IndexModelWrapper,
        options: Option<CreateIndexOptionsWrapper>,
    }
}

impl_simple_operation!(CreateIndexInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl CreateIndexInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_create_index(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_create_index(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            StringOutput(
                context
                    .create_index(self.model.to_owned().into(), self.options.to_owned().map(Into::into))
                    .await
                    .map_err(EpError::database)?
                    .index_name,
            )
            .to_output(),
        ) as Box<dyn EpOutput>)
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    #![allow(unexpected_cfgs)]
    use crate::api::wrapper::IndexOptionsWrapper;
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_create_index_single_field() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.create_index("ci_single", doc! { "name": 1 }, None).await;

        assert!(result.is_string(), "create_index should return an index name string");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_create_index_compound() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.create_index("ci_compound", doc! { "a": 1, "b": -1 }, None).await;

        assert!(result.is_string(), "create_index should return an index name string");
        let name = result.as_str().expect("index name should be a string");
        assert!(!name.is_empty(), "index name should not be empty");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_create_index_unique() {
        let mut ctx = MongoTestContext::new().await;

        let opts = IndexOptionsWrapper { unique: Some(true), ..Default::default() };
        let result = ctx.create_index("ci_unique", doc! { "email": 1 }, Some(opts)).await;

        assert!(result.is_string(), "create_index should return an index name string");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_create_index_with_name() {
        let mut ctx = MongoTestContext::new().await;

        let opts = IndexOptionsWrapper {
            name: Some("my_custom_index".to_string()),
            ..Default::default()
        };
        let result = ctx.create_index("ci_named", doc! { "status": 1 }, Some(opts)).await;

        assert!(result.is_string(), "create_index should return an index name string");
        assert_eq!(result.as_str().expect("should be string"), "my_custom_index");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_create_index_sparse() {
        let mut ctx = MongoTestContext::new().await;

        let opts = IndexOptionsWrapper { sparse: Some(true), ..Default::default() };
        let result = ctx.create_index("ci_sparse", doc! { "optional_field": 1 }, Some(opts)).await;

        assert!(result.is_string(), "create_index should return an index name string");

        ctx.stop().await;
    }

    // ---- Multi-step workflow integration tests ----

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_unique_index_enforcement() {
        let mut ctx = MongoTestContext::new().await;

        // Step 1: Create unique index on email field
        let idx_result = ctx
            .create_index(
                "idx_unique_email",
                doc! { "email": 1 },
                Some(IndexOptionsWrapper {
                    unique: Some(true),
                    name: Some("unique_email".to_string()),
                    ..Default::default()
                }),
            )
            .await;
        assert_eq!(idx_result.as_str().expect("should be string"), "unique_email");

        // Step 2: Insert doc with email - should succeed
        let first = ctx
            .insert_one(
                "idx_unique_email",
                doc! {
                    "name": "Alice",
                    "email": "alice@test.com"
                },
            )
            .await;
        assert!(first["inserted_id"].is_object(), "first insert should succeed");

        // Step 3: Try inserting another doc with the same email - should fail
        let _err = ctx
            .insert_one_err(
                "idx_unique_email",
                doc! {
                    "name": "Alice Duplicate",
                    "email": "alice@test.com"
                },
            )
            .await;

        // Step 4: Insert doc with a different email - should succeed
        let third = ctx
            .insert_one(
                "idx_unique_email",
                doc! {
                    "name": "Bob",
                    "email": "bob@test.com"
                },
            )
            .await;
        assert!(third["inserted_id"].is_object(), "insert with different email should succeed");

        // Step 5: Verify 2 docs in collection
        let count = ctx.count_documents("idx_unique_email", None).await;
        assert_eq!(count.as_u64().expect("count should be a number"), 2, "should have exactly 2 documents");

        ctx.drop_collection("idx_unique_email").await;

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_index_compound_query_usage() {
        let mut ctx = MongoTestContext::new().await;

        // Step 1: Create compound index on {category: 1, price: -1}
        let idx_result = ctx
            .create_index(
                "idx_compound_query",
                doc! { "category": 1, "price": -1 },
                Some(IndexOptionsWrapper {
                    name: Some("category_price_idx".to_string()),
                    ..Default::default()
                }),
            )
            .await;
        assert_eq!(idx_result.as_str().expect("should be string"), "category_price_idx");

        // Step 2: Insert 10 products with varying categories and prices
        ctx.insert_one(
            "idx_compound_query",
            doc! { "_id": "p1",  "name": "Laptop",     "category": "electronics", "price": 999  },
        )
        .await;
        ctx.insert_one(
            "idx_compound_query",
            doc! { "_id": "p2",  "name": "Phone",      "category": "electronics", "price": 699  },
        )
        .await;
        ctx.insert_one(
            "idx_compound_query",
            doc! { "_id": "p3",  "name": "Tablet",     "category": "electronics", "price": 499  },
        )
        .await;
        ctx.insert_one(
            "idx_compound_query",
            doc! { "_id": "p4",  "name": "Headphones", "category": "electronics", "price": 30   },
        )
        .await;
        ctx.insert_one(
            "idx_compound_query",
            doc! { "_id": "p5",  "name": "Novel",      "category": "books",       "price": 15   },
        )
        .await;
        ctx.insert_one(
            "idx_compound_query",
            doc! { "_id": "p6",  "name": "Textbook",   "category": "books",       "price": 80   },
        )
        .await;
        ctx.insert_one(
            "idx_compound_query",
            doc! { "_id": "p7",  "name": "T-Shirt",    "category": "clothing",    "price": 25   },
        )
        .await;
        ctx.insert_one(
            "idx_compound_query",
            doc! { "_id": "p8",  "name": "Jacket",     "category": "clothing",    "price": 120  },
        )
        .await;
        ctx.insert_one(
            "idx_compound_query",
            doc! { "_id": "p9",  "name": "Monitor",    "category": "electronics", "price": 350  },
        )
        .await;
        ctx.insert_one(
            "idx_compound_query",
            doc! { "_id": "p10", "name": "Keyboard",   "category": "electronics", "price": 50   },
        )
        .await;

        // Step 3: Query with filter matching the index: electronics with price >= 50
        let filtered = ctx
            .find(
                "idx_compound_query",
                Some(doc! {
                    "category": "electronics",
                    "price": { "$gte": 50 }
                }),
            )
            .await;
        let filtered_arr = filtered.as_array().expect("find should return an array");
        assert_eq!(filtered_arr.len(), 5, "should match Laptop, Phone, Tablet, Monitor, and Keyboard (price >= 50)");
        for item in filtered_arr {
            assert_eq!(item["category"], "electronics");
            let price = item["price"].as_i64().expect("price should be a number");
            assert!(price >= 50, "all returned products should have price >= 50, got {price}");
        }

        // Step 4: Query with sort by category ascending (single-key to avoid HashMap ordering issues)
        let options = crate::api::wrapper::FindOptionsWrapper {
            sort: Some(crate::api::wrapper::DocumentWrapper::from(doc! { "category": 1 })),
            ..Default::default()
        };
        let sorted = ctx.find_with_options("idx_compound_query", None, options).await;
        let sorted_arr = sorted.as_array().expect("find should return an array");
        assert_eq!(sorted_arr.len(), 10, "should return all 10 products");

        // Verify sorted by category ascending: books first, then clothing, then electronics
        let categories: Vec<&str> = sorted_arr.iter().filter_map(|d| d["category"].as_str()).collect();
        let books_end = categories.iter().position(|c| *c != "books").unwrap_or(categories.len());
        let clothing_end = categories[books_end..].iter().position(|c| *c != "clothing").map(|p| p + books_end).unwrap_or(categories.len());
        assert_eq!(books_end, 2, "should have 2 books");
        assert_eq!(clothing_end - books_end, 2, "should have 2 clothing items");
        assert_eq!(categories.len() - clothing_end, 6, "should have 6 electronics");

        ctx.drop_collection("idx_compound_query").await;

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_index_lifecycle_create_use_drop() {
        let mut ctx = MongoTestContext::new().await;

        // Step 1: Create a named index on status field
        let idx_result = ctx
            .create_index(
                "idx_lifecycle",
                doc! { "status": 1 },
                Some(IndexOptionsWrapper { name: Some("status_idx".to_string()), ..Default::default() }),
            )
            .await;
        assert_eq!(idx_result.as_str().expect("should be string"), "status_idx");

        // Step 2: Verify index appears in list_index_names
        let names = ctx.list_index_names("idx_lifecycle").await;
        let names_arr = names.as_array().expect("list_index_names should return an array");
        let has_status_idx = names_arr.iter().any(|n| n.as_str() == Some("status_idx"));
        assert!(has_status_idx, "status_idx should appear in index names");
        let has_id_idx = names_arr.iter().any(|n| n.as_str() == Some("_id_"));
        assert!(has_id_idx, "default _id_ index should appear in index names");

        // Step 3: Insert docs and query by status field
        ctx.insert_one("idx_lifecycle", doc! { "_id": "t1", "status": "active", "name": "Task A" }).await;
        ctx.insert_one("idx_lifecycle", doc! { "_id": "t2", "status": "inactive", "name": "Task B" }).await;
        ctx.insert_one("idx_lifecycle", doc! { "_id": "t3", "status": "active", "name": "Task C" }).await;
        ctx.insert_one("idx_lifecycle", doc! { "_id": "t4", "status": "pending", "name": "Task D" }).await;
        ctx.insert_one("idx_lifecycle", doc! { "_id": "t5", "status": "active", "name": "Task E" }).await;

        let active = ctx.find("idx_lifecycle", Some(doc! { "status": "active" })).await;
        let active_arr = active.as_array().expect("find should return an array");
        assert_eq!(active_arr.len(), 3, "should find 3 active tasks");

        // Step 4: Drop the index
        ctx.drop_index("idx_lifecycle", "status_idx").await;

        // Step 5: Verify index is gone from list_index_names
        let names_after = ctx.list_index_names("idx_lifecycle").await;
        let names_after_arr = names_after.as_array().expect("list_index_names should return an array");
        let has_status_idx_after = names_after_arr.iter().any(|n| n.as_str() == Some("status_idx"));
        assert!(!has_status_idx_after, "status_idx should no longer appear in index names after drop");

        // Step 6: Query still works (just not indexed)
        let active_after = ctx.find("idx_lifecycle", Some(doc! { "status": "active" })).await;
        let active_after_arr = active_after.as_array().expect("find should return an array");
        assert_eq!(active_after_arr.len(), 3, "query should still return 3 active tasks after index dropped");

        ctx.drop_collection("idx_lifecycle").await;

        ctx.stop().await;
    }
}
