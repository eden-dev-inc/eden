use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, FindOptionsWrapper};
use crate::output::VecDocumentOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use futures_util::TryStreamExt;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, FindInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::Find)))),
    "Finds the documents in the collection matching filter",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct FindInput {
        database: String,
        collection: String,
        #[builder(default = "None")]
        filter: Option<DocumentWrapperType>,
        #[builder(default = "None")]
        options: Option<FindOptionsWrapper>,
    }
}

impl_simple_operation!(FindInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl FindInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_find(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_find(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        let mut cursor = context
            .find(self.filter.to_owned().map(DocumentFunction::into_document), self.options.to_owned().map(Into::into))
            .await
            .map_err(EpError::database)?;

        let mut results = vec![];
        while let Some(doc) = cursor.try_next().await.map_err(EpError::request)? {
            results.push(doc)
        }

        Ok(Box::new(VecDocumentOutput(results).to_output()) as Box<dyn EpOutput>)
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    #![allow(unexpected_cfgs)]
    use crate::api::wrapper::*;
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_all() {
        let mut ctx = MongoTestContext::new().await;

        for i in 0..5 {
            ctx.insert_one("find_all", doc! { "_id": format!("doc{}", i), "value": i }).await;
        }

        let result = ctx.find("find_all", None).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 5, "should find all 5 inserted documents");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_with_equality_filter() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_eq", doc! { "_id": "a", "name": "Alice", "age": 30 }).await;
        ctx.insert_one("find_eq", doc! { "_id": "b", "name": "Bob", "age": 25 }).await;
        ctx.insert_one("find_eq", doc! { "_id": "c", "name": "Alice", "age": 35 }).await;

        let result = ctx.find("find_eq", Some(doc! { "name": "Alice" })).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "should find 2 documents with name Alice");
        for doc in arr {
            assert_eq!(doc["name"], "Alice");
        }

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_with_comparison_operators() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_cmp", doc! { "_id": "a", "score": 10 }).await;
        ctx.insert_one("find_cmp", doc! { "_id": "b", "score": 20 }).await;
        ctx.insert_one("find_cmp", doc! { "_id": "c", "score": 30 }).await;
        ctx.insert_one("find_cmp", doc! { "_id": "d", "score": 40 }).await;
        ctx.insert_one("find_cmp", doc! { "_id": "e", "score": 50 }).await;

        // $gt: score > 30
        let result = ctx.find("find_cmp", Some(doc! { "score": { "$gt": 30 } })).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "$gt 30 should match scores 40 and 50");

        // $lt: score < 20
        let result = ctx.find("find_cmp", Some(doc! { "score": { "$lt": 20 } })).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 1, "$lt 20 should match score 10");

        // $gte: score >= 30
        let result = ctx.find("find_cmp", Some(doc! { "score": { "$gte": 30 } })).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 3, "$gte 30 should match scores 30, 40, and 50");

        // $lte: score <= 20
        let result = ctx.find("find_cmp", Some(doc! { "score": { "$lte": 20 } })).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "$lte 20 should match scores 10 and 20");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_with_in_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_in", doc! { "_id": "a", "status": "active" }).await;
        ctx.insert_one("find_in", doc! { "_id": "b", "status": "inactive" }).await;
        ctx.insert_one("find_in", doc! { "_id": "c", "status": "pending" }).await;
        ctx.insert_one("find_in", doc! { "_id": "d", "status": "active" }).await;

        let result = ctx.find("find_in", Some(doc! { "status": { "$in": ["active", "pending"] } })).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 3, "$in should match active and pending statuses");
        for doc in arr {
            let status = doc["status"].as_str().expect("status should be a string");
            assert!(status == "active" || status == "pending", "status should be active or pending, got {}", status);
        }

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_empty_collection() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.find("find_empty_coll", None).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 0, "find on empty collection should return empty array");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_no_matches() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_no_match", doc! { "_id": "a", "name": "Alice" }).await;
        ctx.insert_one("find_no_match", doc! { "_id": "b", "name": "Bob" }).await;

        let result = ctx.find("find_no_match", Some(doc! { "name": "Zara" })).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 0, "find with non-matching filter should return empty array");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_with_projection() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_proj", doc! { "_id": "a", "name": "Alice", "age": 30, "email": "alice@test.com" }).await;
        ctx.insert_one("find_proj", doc! { "_id": "b", "name": "Bob", "age": 25, "email": "bob@test.com" }).await;

        let options = FindOptionsWrapper {
            projection: Some(DocumentWrapper::from(doc! { "name": 1, "_id": 0 })),
            ..Default::default()
        };
        let result = ctx.find_with_options("find_proj", None, options).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2);
        for doc in arr {
            assert!(doc.get("name").is_some(), "projected field 'name' should be present");
            assert!(doc.get("_id").is_none(), "_id should be excluded by projection");
            assert!(doc.get("age").is_none(), "non-projected field 'age' should be excluded");
            assert!(doc.get("email").is_none(), "non-projected field 'email' should be excluded");
        }

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_with_sort() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_sort", doc! { "_id": "a", "name": "Charlie", "rank": 3 }).await;
        ctx.insert_one("find_sort", doc! { "_id": "b", "name": "Alice", "rank": 1 }).await;
        ctx.insert_one("find_sort", doc! { "_id": "c", "name": "Bob", "rank": 2 }).await;

        // Sort ascending by rank
        let options = FindOptionsWrapper {
            sort: Some(DocumentWrapper::from(doc! { "rank": 1 })),
            ..Default::default()
        };
        let result = ctx.find_with_options("find_sort", None, options).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0]["name"], "Alice");
        assert_eq!(arr[1]["name"], "Bob");
        assert_eq!(arr[2]["name"], "Charlie");

        // Sort descending by rank
        let options = FindOptionsWrapper {
            sort: Some(DocumentWrapper::from(doc! { "rank": -1 })),
            ..Default::default()
        };
        let result = ctx.find_with_options("find_sort", None, options).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr[0]["name"], "Charlie");
        assert_eq!(arr[1]["name"], "Bob");
        assert_eq!(arr[2]["name"], "Alice");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_with_limit() {
        let mut ctx = MongoTestContext::new().await;

        for i in 0..5 {
            ctx.insert_one("find_limit", doc! { "_id": format!("doc{}", i), "index": i }).await;
        }

        let options = FindOptionsWrapper { limit: Some(3), ..Default::default() };
        let result = ctx.find_with_options("find_limit", None, options).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 3, "limit should restrict results to 3 documents");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_with_skip() {
        let mut ctx = MongoTestContext::new().await;

        for i in 0..5 {
            ctx.insert_one("find_skip", doc! { "_id": format!("doc{}", i), "rank": i }).await;
        }

        let options = FindOptionsWrapper {
            sort: Some(DocumentWrapper::from(doc! { "rank": 1 })),
            skip: Some(2),
            ..Default::default()
        };
        let result = ctx.find_with_options("find_skip", None, options).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 3, "skip 2 of 5 should return 3 documents");
        assert_eq!(arr[0]["rank"], 2);
        assert_eq!(arr[1]["rank"], 3);
        assert_eq!(arr[2]["rank"], 4);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_with_skip_and_limit() {
        let mut ctx = MongoTestContext::new().await;

        for i in 0..10 {
            ctx.insert_one("find_page", doc! { "_id": format!("doc{}", i), "rank": i }).await;
        }

        // Page 2: skip 3, limit 3 (should get ranks 3, 4, 5)
        let options = FindOptionsWrapper {
            sort: Some(DocumentWrapper::from(doc! { "rank": 1 })),
            skip: Some(3),
            limit: Some(3),
            ..Default::default()
        };
        let result = ctx.find_with_options("find_page", None, options).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 3, "skip 3 + limit 3 should return exactly 3 documents");
        assert_eq!(arr[0]["rank"], 3);
        assert_eq!(arr[1]["rank"], 4);
        assert_eq!(arr[2]["rank"], 5);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_nested_field_filter() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "find_nested",
            doc! {
                "_id": "u1",
                "name": "Alice",
                "address": { "city": "Portland", "state": "OR" }
            },
        )
        .await;
        ctx.insert_one(
            "find_nested",
            doc! {
                "_id": "u2",
                "name": "Bob",
                "address": { "city": "Seattle", "state": "WA" }
            },
        )
        .await;
        ctx.insert_one(
            "find_nested",
            doc! {
                "_id": "u3",
                "name": "Charlie",
                "address": { "city": "Portland", "state": "ME" }
            },
        )
        .await;

        let result = ctx.find("find_nested", Some(doc! { "address.city": "Portland" })).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "dot notation filter should match 2 Portland documents");
        for doc in arr {
            assert_eq!(doc["address"]["city"], "Portland");
        }

        ctx.stop().await;
    }

    /// E-commerce scenario: find products that are both in stock AND priced above 50.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_logical_and_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_and", doc! { "_id": "p1", "name": "Budget Widget",      "in_stock": true,  "price": 25 }).await;
        ctx.insert_one("find_and", doc! { "_id": "p2", "name": "Premium Widget",     "in_stock": true,  "price": 75 }).await;
        ctx.insert_one("find_and", doc! { "_id": "p3", "name": "Luxury Widget",      "in_stock": true,  "price": 150 }).await;
        ctx.insert_one("find_and", doc! { "_id": "p4", "name": "Discontinued Gizmo", "in_stock": false, "price": 200 }).await;
        ctx.insert_one("find_and", doc! { "_id": "p5", "name": "Cheap Gizmo",        "in_stock": false, "price": 10 }).await;
        ctx.insert_one("find_and", doc! { "_id": "p6", "name": "Standard Gadget",    "in_stock": true,  "price": 50 }).await;

        let result = ctx
            .find(
                "find_and",
                Some(doc! {
                    "$and": [
                        { "in_stock": true },
                        { "price": { "$gt": 50 } }
                    ]
                }),
            )
            .await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "$and should match only in-stock products priced above 50");
        for doc in arr {
            assert_eq!(doc["in_stock"], true);
            assert!(doc["price"].as_i64().expect("price should be numeric") > 50);
        }

        ctx.stop().await;
    }

    /// User search: find users who are either admins OR premium members.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_logical_or_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_or", doc! { "_id": "u1", "name": "Alice",   "role": "admin",  "membership": "basic" }).await;
        ctx.insert_one("find_or", doc! { "_id": "u2", "name": "Bob",     "role": "user",   "membership": "premium" }).await;
        ctx.insert_one("find_or", doc! { "_id": "u3", "name": "Charlie", "role": "user",   "membership": "basic" }).await;
        ctx.insert_one("find_or", doc! { "_id": "u4", "name": "Diana",   "role": "admin",  "membership": "premium" }).await;
        ctx.insert_one("find_or", doc! { "_id": "u5", "name": "Eve",     "role": "editor", "membership": "basic" }).await;

        let result = ctx
            .find(
                "find_or",
                Some(doc! {
                    "$or": [
                        { "role": "admin" },
                        { "membership": "premium" }
                    ]
                }),
            )
            .await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 3, "$or should match Alice (admin), Bob (premium), and Diana (admin+premium)");
        let names: Vec<&str> = arr.iter().map(|d| d["name"].as_str().expect("name should be string")).collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Bob"));
        assert!(names.contains(&"Diana"));

        ctx.stop().await;
    }

    /// Content filter: find articles NOT in draft status.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_logical_not_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_not", doc! { "_id": "a1", "title": "Getting Started",   "status": "published" }).await;
        ctx.insert_one("find_not", doc! { "_id": "a2", "title": "Work In Progress",  "status": "draft" }).await;
        ctx.insert_one("find_not", doc! { "_id": "a3", "title": "Under Review",      "status": "review" }).await;
        ctx.insert_one("find_not", doc! { "_id": "a4", "title": "Another Draft",     "status": "draft" }).await;

        let result = ctx
            .find(
                "find_not",
                Some(doc! {
                    "status": { "$not": { "$eq": "draft" } }
                }),
            )
            .await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "$not should exclude draft articles");
        for doc in arr {
            let status = doc["status"].as_str().expect("status should be a string");
            assert_ne!(status, "draft", "no draft articles should appear in results");
        }

        ctx.stop().await;
    }

    /// Schema migration scenario: find documents that have an "email" field.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_exists_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_exists", doc! { "_id": "u1", "name": "Alice", "email": "alice@example.com" }).await;
        ctx.insert_one("find_exists", doc! { "_id": "u2", "name": "Bob" }).await;
        ctx.insert_one("find_exists", doc! { "_id": "u3", "name": "Charlie", "email": "charlie@example.com" }).await;
        ctx.insert_one("find_exists", doc! { "_id": "u4", "name": "Diana" }).await;

        let result = ctx
            .find(
                "find_exists",
                Some(doc! {
                    "email": { "$exists": true }
                }),
            )
            .await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "$exists should match only documents with email field");
        let ids: Vec<&str> = arr.iter().map(|d| d["_id"].as_str().expect("_id should be string")).collect();
        assert!(ids.contains(&"u1"));
        assert!(ids.contains(&"u3"));

        ctx.stop().await;
    }

    /// Search scenario: find products whose name matches a regex pattern.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_regex_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_regex", doc! { "_id": "p1", "name": "Premium Headphones", "price": 199 }).await;
        ctx.insert_one("find_regex", doc! { "_id": "p2", "name": "Basic Speaker",      "price": 49 }).await;
        ctx.insert_one("find_regex", doc! { "_id": "p3", "name": "premium keyboard",   "price": 129 }).await;
        ctx.insert_one("find_regex", doc! { "_id": "p4", "name": "Standard Mouse",     "price": 29 }).await;
        ctx.insert_one("find_regex", doc! { "_id": "p5", "name": "PREMIUM Monitor",    "price": 399 }).await;

        let result = ctx
            .find(
                "find_regex",
                Some(doc! {
                    "name": { "$regex": "^Premium", "$options": "i" }
                }),
            )
            .await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 3, "case-insensitive regex ^Premium should match 3 products");
        for doc in arr {
            let name = doc["name"].as_str().expect("name should be string");
            assert!(
                name.to_lowercase().starts_with("premium"),
                "matched product name should start with 'premium' (case-insensitive), got: {}",
                name
            );
        }

        ctx.stop().await;
    }

    /// Order items: find orders where at least one line item has quantity > 5 AND price > 100.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_elem_match_on_subdocuments() {
        let mut ctx = MongoTestContext::new().await;

        // Order 1: one item with high qty but low price, another with low qty but high price
        ctx.insert_one(
            "find_em",
            doc! {
                "_id": "ord1",
                "customer": "Alice",
                "items": [
                    { "name": "Bolts",   "quantity": 10, "price": 5 },
                    { "name": "Wrench",  "quantity": 1,  "price": 250 }
                ]
            },
        )
        .await;
        // Order 2: one item that satisfies BOTH conditions
        ctx.insert_one(
            "find_em",
            doc! {
                "_id": "ord2",
                "customer": "Bob",
                "items": [
                    { "name": "Server Rack", "quantity": 8, "price": 500 },
                    { "name": "Cable",       "quantity": 20, "price": 3 }
                ]
            },
        )
        .await;
        // Order 3: no items satisfy both conditions
        ctx.insert_one(
            "find_em",
            doc! {
                "_id": "ord3",
                "customer": "Charlie",
                "items": [
                    { "name": "Pen",    "quantity": 2, "price": 3 },
                    { "name": "Eraser", "quantity": 4, "price": 1 }
                ]
            },
        )
        .await;

        let result = ctx
            .find(
                "find_em",
                Some(doc! {
                    "items": { "$elemMatch": { "quantity": { "$gt": 5 }, "price": { "$gt": 100 } } }
                }),
            )
            .await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 1, "$elemMatch should match only order with a single item meeting both criteria");
        assert_eq!(arr[0]["_id"], "ord2");

        ctx.stop().await;
    }

    /// Tag matching: find documents that have ALL specified tags.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_array_all_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_all_tags", doc! { "_id": "t1", "title": "Task A", "tags": ["urgent", "backend", "bug"] }).await;
        ctx.insert_one("find_all_tags", doc! { "_id": "t2", "title": "Task B", "tags": ["frontend", "urgent"] }).await;
        ctx.insert_one("find_all_tags", doc! { "_id": "t3", "title": "Task C", "tags": ["backend", "feature"] }).await;
        ctx.insert_one("find_all_tags", doc! { "_id": "t4", "title": "Task D", "tags": ["urgent", "backend", "feature"] }).await;

        let result = ctx
            .find(
                "find_all_tags",
                Some(doc! {
                    "tags": { "$all": ["urgent", "backend"] }
                }),
            )
            .await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "$all should match docs containing both 'urgent' and 'backend'");
        let ids: Vec<&str> = arr.iter().map(|d| d["_id"].as_str().expect("_id should be string")).collect();
        assert!(ids.contains(&"t1"));
        assert!(ids.contains(&"t4"));

        ctx.stop().await;
    }

    /// Find documents whose array field has exactly N elements.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_array_size_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_size", doc! { "_id": "d1", "name": "Alice",   "skills": ["rust", "python", "go"] }).await;
        ctx.insert_one("find_size", doc! { "_id": "d2", "name": "Bob",     "skills": ["java", "kotlin"] }).await;
        ctx.insert_one("find_size", doc! { "_id": "d3", "name": "Charlie", "skills": ["rust", "c++", "haskell"] }).await;
        ctx.insert_one("find_size", doc! { "_id": "d4", "name": "Diana",   "skills": ["javascript"] }).await;

        let result = ctx
            .find(
                "find_size",
                Some(doc! {
                    "skills": { "$size": 3 }
                }),
            )
            .await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "$size 3 should match docs with exactly 3 skills");
        let ids: Vec<&str> = arr.iter().map(|d| d["_id"].as_str().expect("_id should be string")).collect();
        assert!(ids.contains(&"d1"));
        assert!(ids.contains(&"d3"));

        ctx.stop().await;
    }

    /// Analytics: find events within a specific date range using BSON DateTime.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_date_range_query() {
        use mongodb::bson::DateTime;

        let mut ctx = MongoTestContext::new().await;

        // Timestamps: Jan 1, Jan 15, Feb 1, Feb 15, Mar 1 of 2025
        let jan_01 = DateTime::parse_rfc3339_str("2025-01-01T00:00:00Z").expect("valid date");
        let jan_15 = DateTime::parse_rfc3339_str("2025-01-15T12:00:00Z").expect("valid date");
        let feb_01 = DateTime::parse_rfc3339_str("2025-02-01T00:00:00Z").expect("valid date");
        let feb_15 = DateTime::parse_rfc3339_str("2025-02-15T08:30:00Z").expect("valid date");
        let mar_01 = DateTime::parse_rfc3339_str("2025-03-01T00:00:00Z").expect("valid date");

        ctx.insert_one("find_date", doc! { "_id": "e1", "event": "signup",   "created_at": jan_01 }).await;
        ctx.insert_one("find_date", doc! { "_id": "e2", "event": "purchase", "created_at": jan_15 }).await;
        ctx.insert_one("find_date", doc! { "_id": "e3", "event": "login",    "created_at": feb_01 }).await;
        ctx.insert_one("find_date", doc! { "_id": "e4", "event": "refund",   "created_at": feb_15 }).await;
        ctx.insert_one("find_date", doc! { "_id": "e5", "event": "logout",   "created_at": mar_01 }).await;

        // Query for events in February (>= Feb 1, < Mar 1)
        let result = ctx
            .find(
                "find_date",
                Some(doc! {
                    "created_at": {
                        "$gte": feb_01,
                        "$lt": mar_01
                    }
                }),
            )
            .await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "date range Feb 1 to Mar 1 should match 2 events");
        let ids: Vec<&str> = arr.iter().map(|d| d["_id"].as_str().expect("_id should be string")).collect();
        assert!(ids.contains(&"e3"));
        assert!(ids.contains(&"e4"));

        ctx.stop().await;
    }

    /// Config data: find documents by deeply nested dot-notation path.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_deeply_nested_dot_notation() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "find_deep",
            doc! {
                "_id": "cfg1",
                "name": "production",
                "config": { "database": { "connection": { "pool_size": 10 } } }
            },
        )
        .await;
        ctx.insert_one(
            "find_deep",
            doc! {
                "_id": "cfg2",
                "name": "staging",
                "config": { "database": { "connection": { "pool_size": 5 } } }
            },
        )
        .await;
        ctx.insert_one(
            "find_deep",
            doc! {
                "_id": "cfg3",
                "name": "development",
                "config": { "database": { "connection": { "pool_size": 2 } } }
            },
        )
        .await;
        ctx.insert_one(
            "find_deep",
            doc! {
                "_id": "cfg4",
                "name": "load-test",
                "config": { "database": { "connection": { "pool_size": 20 } } }
            },
        )
        .await;

        let result = ctx
            .find(
                "find_deep",
                Some(doc! {
                    "config.database.connection.pool_size": { "$gt": 5 }
                }),
            )
            .await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "deeply nested dot-notation filter should match pool_size > 5");
        let names: Vec<&str> = arr.iter().map(|d| d["name"].as_str().expect("name should be string")).collect();
        assert!(names.contains(&"production"));
        assert!(names.contains(&"load-test"));

        ctx.stop().await;
    }

    /// Real pagination workflow: insert 20 products, paginate through all 4 pages of 5.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_pagination_workflow() {
        use crate::api::wrapper::FindOptionsWrapper;
        use std::collections::HashSet;

        let mut ctx = MongoTestContext::new().await;

        for i in 0..20 {
            ctx.insert_one(
                "find_page_wf",
                doc! {
                    "_id": format!("prod_{:03}", i),
                    "name": format!("Product {}", i),
                    "seq": i
                },
            )
            .await;
        }

        let mut all_ids: HashSet<String> = HashSet::new();

        for page in 0..4u64 {
            let options = FindOptionsWrapper {
                sort: Some(DocumentWrapper::from(doc! { "_id": 1 })),
                skip: Some(page * 5),
                limit: Some(5),
                ..Default::default()
            };
            let result = ctx.find_with_options("find_page_wf", None, options).await;
            let arr = result.as_array().expect("find should return an array");
            assert_eq!(arr.len(), 5, "page {} should have exactly 5 items", page);

            for doc in arr {
                let id = doc["_id"].as_str().expect("_id should be string").to_string();
                assert!(all_ids.insert(id.clone()), "duplicate _id found across pages: {}", id);
            }
        }

        assert_eq!(all_ids.len(), 20, "all 20 products should be covered across 4 pages");

        ctx.stop().await;
    }

    /// Dashboard query: filter active users, project name + last_login, sort by last_login desc.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_combined_filter_projection_sort() {
        use crate::api::wrapper::FindOptionsWrapper;
        use mongodb::bson::DateTime;

        let mut ctx = MongoTestContext::new().await;

        let ts1 = DateTime::parse_rfc3339_str("2025-01-10T08:00:00Z").expect("valid date");
        let ts2 = DateTime::parse_rfc3339_str("2025-01-20T14:30:00Z").expect("valid date");
        let ts3 = DateTime::parse_rfc3339_str("2025-01-05T09:00:00Z").expect("valid date");
        let ts4 = DateTime::parse_rfc3339_str("2025-01-25T18:00:00Z").expect("valid date");
        let ts5 = DateTime::parse_rfc3339_str("2025-01-15T12:00:00Z").expect("valid date");
        let ts6 = DateTime::parse_rfc3339_str("2025-01-30T20:00:00Z").expect("valid date");

        ctx.insert_one(
            "find_dash",
            doc! { "_id": "u1", "name": "Alice",   "status": "active",   "last_login": ts1, "email": "alice@test.com" },
        )
        .await;
        ctx.insert_one(
            "find_dash",
            doc! { "_id": "u2", "name": "Bob",     "status": "inactive", "last_login": ts2, "email": "bob@test.com" },
        )
        .await;
        ctx.insert_one(
            "find_dash",
            doc! { "_id": "u3", "name": "Charlie", "status": "active",   "last_login": ts3, "email": "charlie@test.com" },
        )
        .await;
        ctx.insert_one(
            "find_dash",
            doc! { "_id": "u4", "name": "Diana",   "status": "active",   "last_login": ts4, "email": "diana@test.com" },
        )
        .await;
        ctx.insert_one(
            "find_dash",
            doc! { "_id": "u5", "name": "Eve",     "status": "inactive", "last_login": ts5, "email": "eve@test.com" },
        )
        .await;
        ctx.insert_one(
            "find_dash",
            doc! { "_id": "u6", "name": "Frank",   "status": "active",   "last_login": ts6, "email": "frank@test.com" },
        )
        .await;

        let options = FindOptionsWrapper {
            projection: Some(DocumentWrapper::from(doc! { "name": 1, "last_login": 1, "_id": 0 })),
            sort: Some(DocumentWrapper::from(doc! { "last_login": -1 })),
            ..Default::default()
        };
        let result = ctx.find_with_options("find_dash", Some(doc! { "status": "active" }), options).await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 4, "should return only the 4 active users");

        // Verify correct fields and no extra fields
        for doc in arr {
            assert!(doc.get("name").is_some(), "projected field 'name' should be present");
            assert!(doc.get("last_login").is_some(), "projected field 'last_login' should be present");
            assert!(doc.get("_id").is_none(), "_id should be excluded");
            assert!(doc.get("email").is_none(), "non-projected 'email' should be excluded");
            assert!(doc.get("status").is_none(), "non-projected 'status' should be excluded");
        }

        // Verify descending order by last_login: Frank, Diana, Alice, Charlie
        assert_eq!(arr[0]["name"], "Frank");
        assert_eq!(arr[1]["name"], "Diana");
        assert_eq!(arr[2]["name"], "Alice");
        assert_eq!(arr[3]["name"], "Charlie");

        ctx.stop().await;
    }

    /// Exclusion filter: find products NOT in specific categories using $nin.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_nin_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("find_nin", doc! { "_id": "p1", "name": "Laptop",      "category": "electronics" }).await;
        ctx.insert_one("find_nin", doc! { "_id": "p2", "name": "T-Shirt",     "category": "clothing" }).await;
        ctx.insert_one("find_nin", doc! { "_id": "p3", "name": "Novel",       "category": "books" }).await;
        ctx.insert_one("find_nin", doc! { "_id": "p4", "name": "Headphones",  "category": "electronics" }).await;
        ctx.insert_one("find_nin", doc! { "_id": "p5", "name": "Blender",     "category": "kitchen" }).await;
        ctx.insert_one("find_nin", doc! { "_id": "p6", "name": "Jacket",      "category": "clothing" }).await;

        let result = ctx
            .find(
                "find_nin",
                Some(doc! {
                    "category": { "$nin": ["electronics", "clothing"] }
                }),
            )
            .await;
        let arr = result.as_array().expect("find should return an array");
        assert_eq!(arr.len(), 2, "$nin should exclude electronics and clothing");
        for doc in arr {
            let category = doc["category"].as_str().expect("category should be string");
            assert!(category != "electronics" && category != "clothing", "excluded category found: {}", category);
        }
        let ids: Vec<&str> = arr.iter().map(|d| d["_id"].as_str().expect("_id should be string")).collect();
        assert!(ids.contains(&"p3"));
        assert!(ids.contains(&"p5"));

        ctx.stop().await;
    }
}
