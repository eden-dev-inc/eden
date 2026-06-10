use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentWrapper, FindOneOptionsWrapper};
use crate::output::{CollectionDocumentOutput, OptionDocumentOutput};
use crate::request::MongoRequest;
use crate::{ApiExample, ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::{Document, doc};
use serde_json;
use telemetry::TelemetryWrapper;
use tokio::sync::OnceCell;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, FindOneInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::FindOne)))),
    "Finds a single document in the collection matching filter",
    ReqType::Read,
    true,
);

static EXAMPLES: OnceCell<Vec<ApiExample<FindOneInput>>> = OnceCell::const_new();

async fn find_one_examples() -> &'static [ApiExample<FindOneInput>] {
    EXAMPLES
        .get_or_init(|| async {
            vec![
                // Example 1: Basic find one by field
                ApiExample {
                    name: "Basic Find One",
                    description: "Find a single user document by username",
                    request: FindOneInput {
                        database: "test_db".to_string(),
                        collection: "users".to_string(),
                        filter: Some(DocumentWrapper::from(doc! {
                            "username": "john_doe"
                        })),
                        options: None,
                    },
                    response: Ok(Some(serde_json::json!({
                        "username": "john_doe",
                        "email": "john@example.com",
                        "age": 30,
                        "created_at": { "$date": "2025-01-15T10:30:00Z" }
                    }))),
                },
                // Example 2: Find one with no filter (returns first document)
                ApiExample {
                    name: "Find First Document",
                    description: "Find the first document in a collection",
                    request: FindOneInput {
                        database: "test_db".to_string(),
                        collection: "products".to_string(),
                        filter: None,
                        options: None,
                    },
                    response: Ok(Some(serde_json::json!({
                        "_id": "PROD-12345",
                        "name": "Laptop Pro",
                        "price": 1299.99,
                        "category": "Electronics"
                    }))),
                },
                // // Example 3: Find one with complex filter
                // ApiExample {
                //     name: "Complex Filter Query",
                //     description: "Find a product with multiple conditions",
                //     request: FindOneInput {
                //         database: "test_db".to_string(),
                //         collection: "products".to_string(),
                //         filter: Some(DocumentWrapper::from(doc! {
                //             "category": "Electronics",
                //             "price": { "$gte": 1000, "$lte": 2000 },
                //             "stock": { "$gt": 0 },
                //             "tags": { "$in": ["laptop", "computer"] }
                //         })),
                //         options: None,
                //     },
                //     response: Ok(Some(serde_json::json!({
                //         "_id": "PROD-67890",
                //         "name": "Gaming Laptop",
                //         "price": 1599.99,
                //         "category": "Electronics",
                //         "stock": 25,
                //         "tags": ["laptop", "gaming", "computer"]
                //     }))),
                // },
                // // Example 4: Find one with projection
                // ApiExample {
                //     name: "Find with Projection",
                //     description: "Find a document and return only specific fields",
                //     request: FindOneInput {
                //         database: "test_db".to_string(),
                //         collection: "users".to_string(),
                //         filter: Some(DocumentWrapper::from(doc! {
                //             "email": "jane@example.com"
                //         })),
                //         options: Some(FindOneOptionsWrapper {
                //             projection: Some(DocumentWrapper::from(doc! {
                //                 "username": 1,
                //                 "email": 1,
                //                 "_id": 0
                //             })),
                //             sort: None,
                //             skip: None,
                //             hint: None,
                //             max: None,
                //             read_concern: None,
                //             max_time: None,
                //             selection_criteria: None,
                //             allow_partial_results: None,
                //             collation: None,
                //             comment: None,
                //             show_record_id: None,
                //             return_key: None,
                //             let_vars: None,
                //             comment_bson: None,
                //             max_scan: None,
                //             min: None,
                //         }),
                //     },
                //     response: Ok(Some(serde_json::json!({
                //         "username": "jane_smith",
                //         "email": "jane@example.com"
                //     }))),
                // },
                // // Example 5: Find one with sort
                // ApiExample {
                //     name: "Find with Sort",
                //     description: "Find the most recent order",
                //     request: FindOneInput {
                //         database: "test_db".to_string(),
                //         collection: "orders".to_string(),
                //         filter: Some(DocumentWrapper::from(doc! {
                //             "status": "completed"
                //         })),
                //         options: Some(FindOneOptionsWrapper {
                //             sort: Some(DocumentWrapper::from(doc! {
                //                 "created_at": -1
                //             })),
                //             projection: None,
                //             skip: None,
                //             hint: None,
                //             max: None,
                //             read_concern: None,
                //             max_time: None,
                //             selection_criteria: None,
                //             allow_partial_results: None,
                //             collation: None,
                //             comment: None,
                //             show_record_id: None,
                //             return_key: None,
                //             let_vars: None,
                //             comment_bson: None,
                //             max_scan: None,
                //             min: None,
                //         }),
                //     },
                //     response: Ok(Some(serde_json::json!({
                //         "order_id": "ORD-2025-100",
                //         "total": 2649.97,
                //         "status": "completed",
                //         "created_at": { "$date": "2025-01-20T15:45:00Z" }
                //     }))),
                // },
                // // Example 6: Find one with skip
                // ApiExample {
                //     name: "Find with Skip",
                //     description: "Skip documents and find the nth document",
                //     request: FindOneInput {
                //         database: "test_db".to_string(),
                //         collection: "blog_posts".to_string(),
                //         filter: Some(DocumentWrapper::from(doc! {
                //             "published": true
                //         })),
                //         options: Some(FindOneOptionsWrapper {
                //             skip: Some(5), // Skip first 5 documents
                //             sort: Some(DocumentWrapper::from(doc! {
                //                 "created_at": 1
                //             })),
                //             projection: None,
                //             hint: None,
                //             max: None,
                //             read_concern: None,
                //             max_time: None,
                //             selection_criteria: None,
                //             allow_partial_results: None,
                //             collation: None,
                //             comment: None,
                //             show_record_id: None,
                //             return_key: None,
                //             let_vars: None,
                //             comment_bson: None,
                //             max_scan: None,
                //             min: None,
                //         }),
                //     },
                //     response: Ok(Some(serde_json::json!({
                //         "title": "Advanced MongoDB Techniques",
                //         "author": "Jane Doe",
                //         "published": true
                //     }))),
                // },
                // // Example 7: Find one with hint
                // ApiExample {
                //     name: "Find with Index Hint",
                //     description: "Force the use of a specific index",
                //     request: FindOneInput {
                //         database: "test_db".to_string(),
                //         collection: "users".to_string(),
                //         filter: Some(DocumentWrapper::from(doc! {
                //             "email": "user@example.com",
                //             "status": "active"
                //         })),
                //         options: Some(FindOneOptionsWrapper {
                //             hint: Some(HintWrapper::Name("email_status_idx".to_string())),
                //             max: None,
                //             projection: None,
                //             sort: None,
                //             skip: None,
                //             read_concern: None,
                //             max_time: None,
                //             selection_criteria: None,
                //             allow_partial_results: None,
                //             collation: None,
                //             comment: None,
                //             show_record_id: None,
                //             return_key: None,
                //             let_vars: None,
                //             comment_bson: None,
                //             max_scan: None,
                //             min: None,
                //         }),
                //     },
                //     response: Ok(Some(serde_json::json!({
                //         "username": "active_user",
                //         "email": "user@example.com",
                //         "status": "active"
                //     }))),
                // },
                // // Example 8: Find one with read concern
                // ApiExample {
                //     name: "Find with Read Concern",
                //     description: "Find with majority read concern for consistency",
                //     request: FindOneInput {
                //         database: "test_db".to_string(),
                //         collection: "transactions".to_string(),
                //         filter: Some(DocumentWrapper::from(doc! {
                //             "transaction_id": "TXN-2025-001"
                //         })),
                //         options: Some(FindOneOptionsWrapper {
                //             read_concern: Some(ReadConcernWrapper {
                //                 level: ReadConcernLevelWrapper::Majority,
                //             }),
                //             projection: None,
                //             sort: None,
                //             skip: None,
                //             hint: None,
                //             max: None,
                //             max_time: None,
                //             selection_criteria: None,
                //             allow_partial_results: None,
                //             collation: None,
                //             comment: None,
                //             show_record_id: None,
                //             return_key: None,
                //             let_vars: None,
                //             comment_bson: None,
                //             max_scan: None,
                //             min: None,
                //         }),
                //     },
                //     response: Ok(Some(serde_json::json!({
                //         "transaction_id": "TXN-2025-001",
                //         "amount": 5000.00,
                //         "status": "completed"
                //     }))),
                // },
                // // Example 9: Find one with collation
                // ApiExample {
                //     name: "Find with Collation",
                //     description: "Find with case-insensitive string comparison",
                //     request: FindOneInput {
                //         database: "test_db".to_string(),
                //         collection: "products".to_string(),
                //         filter: Some(DocumentWrapper::from(doc! {
                //             "name": "laptop pro" // lowercase search
                //         })),
                //         options: Some(FindOneOptionsWrapper {
                //             collation: Some(CollationWrapper {
                //                 locale: "en".to_string(),
                //                 strength: Some(CollationStrengthWrapper::Primary), // Case-insensitive
                //                 case_level: None,
                //                 case_first: None,
                //                 numeric_ordering: None,
                //                 alternate: None,
                //                 max_variable: None,
                //                 backwards: None,
                //                 normalization: None,
                //             }),
                //             projection: None,
                //             sort: None,
                //             skip: None,
                //             hint: None,
                //             max: None,
                //             read_concern: None,
                //             max_time: None,
                //             selection_criteria: None,
                //             allow_partial_results: None,
                //             comment: None,
                //             show_record_id: None,
                //             return_key: None,
                //             let_vars: None,
                //             comment_bson: None,
                //             max_scan: None,
                //             min: None,
                //         }),
                //     },
                //     response: Ok(Some(serde_json::json!({
                //         "_id": "PROD-12345",
                //         "name": "Laptop Pro", // Matches despite case difference
                //         "price": 1299.99
                //     }))),
                // },
                // // Example 10: Find one with comment
                // ApiExample {
                //     name: "Find with Comment",
                //     description: "Add a comment for debugging/logging purposes",
                //     request: FindOneInput {
                //         database: "test_db".to_string(),
                //         collection: "audit_logs".to_string(),
                //         filter: Some(DocumentWrapper::from(doc! {
                //             "event": "security_breach",
                //             "severity": "critical"
                //         })),
                //         options: Some(FindOneOptionsWrapper {
                //             comment: Some("Security investigation query".to_string()),
                //             projection: None,
                //             sort: None,
                //             skip: None,
                //             hint: None,
                //             max: None,
                //             read_concern: None,
                //             max_time: None,
                //             selection_criteria: None,
                //             allow_partial_results: None,
                //             collation: None,
                //             show_record_id: None,
                //             return_key: None,
                //             let_vars: None,
                //             comment_bson: None,
                //             max_scan: None,
                //             min: None,
                //         }),
                //     },
                //     response: Ok(Some(serde_json::json!({
                //         "event": "security_breach",
                //         "severity": "critical",
                //         "timestamp": { "$date": "2025-01-20T02:15:00Z" }
                //     }))),
                // },
                // // Example 11: Find one with regex filter
                // ApiExample {
                //     name: "Find with Regex",
                //     description: "Find document using regular expression",
                //     request: FindOneInput {
                //         database: "test_db".to_string(),
                //         collection: "users".to_string(),
                //         filter: Some(DocumentWrapper::from(doc! {
                //             "email": { "$regex": ".*@company\\.com$", "$options": "i" }
                //         })),
                //         options: None,
                //     },
                //     response: Ok(Some(serde_json::json!({
                //         "username": "employee1",
                //         "email": "john@company.com"
                //     }))),
                // },
                // // Example 12: Find one with array query
                // ApiExample {
                //     name: "Find with Array Query",
                //     description: "Find document where array contains specific element",
                //     request: FindOneInput {
                //         database: "test_db".to_string(),
                //         collection: "blog_posts".to_string(),
                //         filter: Some(DocumentWrapper::from(doc! {
                //             "tags": "mongodb",
                //             "comments.user": "Alice"
                //         })),
                //         options: None,
                //     },
                //     response: Ok(Some(serde_json::json!({
                //         "title": "MongoDB Best Practices",
                //         "tags": ["mongodb", "database", "nosql"],
                //         "comments": [
                //             {
                //                 "user": "Alice",
                //                 "text": "Great article!"
                //             }
                //         ]
                //     }))),
                // },
                // // Example 13: Find one with date range
                // ApiExample {
                //     name: "Find with Date Range",
                //     description: "Find the most recent document within a date range",
                //     request: FindOneInput {
                //         database: "test_db".to_string(),
                //         collection: "events".to_string(),
                //         filter: Some(DocumentWrapper::from(doc! {
                //             "timestamp": {
                //                 "$gte": { "$date": "2025-01-01T00:00:00Z" },
                //                 "$lt": { "$date": "2025-02-01T00:00:00Z" }
                //             },
                //             "type": "user_activity"
                //         })),
                //         options: Some(FindOneOptionsWrapper {
                //             sort: Some(DocumentWrapper::from(doc! {
                //                 "timestamp": -1
                //             })),
                //             projection: None,
                //             skip: None,
                //             hint: None,
                //             max: None,
                //             read_concern: None,
                //             max_time: None,
                //             selection_criteria: None,
                //             allow_partial_results: None,
                //             collation: None,
                //             comment: None,
                //             show_record_id: None,
                //             return_key: None,
                //             let_vars: None,
                //             comment_bson: None,
                //             max_scan: None,
                //             min: None,
                //         }),
                //     },
                //     response: Ok(Some(serde_json::json!({
                //         "type": "user_activity",
                //         "timestamp": { "$date": "2025-01-31T23:59:59Z" },
                //         "action": "login"
                //     }))),
                // },
                // // Example 14: Find one with nested field query
                // ApiExample {
                //     name: "Find with Nested Fields",
                //     description: "Find document by querying nested object fields",
                //     request: FindOneInput {
                //         database: "test_db".to_string(),
                //         collection: "orders".to_string(),
                //         filter: Some(DocumentWrapper::from(doc! {
                //             "customer.address.city": "New York",
                //             "customer.address.zip": "10001",
                //             "total": { "$gt": 1000 }
                //         })),
                //         options: None,
                //     },
                //     response: Ok(Some(serde_json::json!({
                //         "order_id": "ORD-2025-050",
                //         "customer": {
                //             "name": "John Smith",
                //             "address": {
                //                 "street": "123 Main St",
                //                 "city": "New York",
                //                 "zip": "10001"
                //             }
                //         },
                //         "total": 1250.00
                //     }))),
                // },
                // // Example 15: Find one returns null when no match
                // ApiExample {
                //     name: "Find One No Match",
                //     description: "Returns null when no document matches the filter",
                //     request: FindOneInput {
                //         database: "test_db".to_string(),
                //         collection: "users".to_string(),
                //         filter: Some(DocumentWrapper::from(doc! {
                //             "username": "non_existent_user"
                //         })),
                //         options: None,
                //     },
                //     response: Ok(None),
                // },
            ]
        })
        .await
}

crate::mongo_endpoint! {
    API_INFO,
    struct FindOneInput {
        database: String,
        collection: String,
        #[builder(default = "None")]
        filter: Option<DocumentWrapper>,
        #[builder(default = "None")]
        options: Option<FindOneOptionsWrapper>,
    }
}

type OutputWrapper = OptionDocumentOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(FindOneInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl FindOneInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_find_one(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_find_one(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            OptionDocumentOutput(
                context
                    .find_one(self.filter.to_owned().map(Into::into), self.options.to_owned().map(Into::into))
                    .await
                    .map_err(EpError::database)?,
            )
            .to_output(),
        ) as Box<dyn EpOutput>)
    }
}

#[cfg(test)]
mod find_one_test {
    #![allow(unexpected_cfgs)]

    use super::*;
    use crate::api::lib::collection::insert_one_examples;
    use crate::test_utils::database_test_utils::generic_write_read_async_test;
    //
    // #[tokio::test]
    // async fn sync_test() {
    //     generic_write_sync_test(insert_one_examples().await).await;
    // }

    #[tokio::test]
    async fn async_test() {
        generic_write_read_async_test(insert_one_examples().await, find_one_examples().await).await;
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
    async fn test_find_one_basic() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("fo_basic", doc! { "_id": "a", "name": "Alice", "age": 30 }).await;
        ctx.insert_one("fo_basic", doc! { "_id": "b", "name": "Bob", "age": 25 }).await;

        let result = ctx.find_one("fo_basic", Some(doc! { "name": "Bob" })).await;
        assert!(result.is_object(), "find_one should return a single document object");
        assert_eq!(result["_id"], "b");
        assert_eq!(result["name"], "Bob");
        assert_eq!(result["age"], 25);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_no_filter() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("fo_no_filter", doc! { "_id": "first", "value": 1 }).await;
        ctx.insert_one("fo_no_filter", doc! { "_id": "second", "value": 2 }).await;

        let result = ctx.find_one("fo_no_filter", None).await;
        assert!(result.is_object(), "find_one with no filter should return a document");
        assert!(result.get("_id").is_some(), "returned document should have _id");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_no_match() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("fo_no_match", doc! { "_id": "a", "name": "Alice" }).await;

        let result = ctx.find_one("fo_no_match", Some(doc! { "name": "Zara" })).await;
        assert!(result.is_null(), "find_one with no matching filter should return null");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_by_id() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("fo_by_id", doc! { "_id": "user42", "name": "Diana", "role": "admin" }).await;
        ctx.insert_one("fo_by_id", doc! { "_id": "user43", "name": "Eve", "role": "viewer" }).await;

        let result = ctx.find_one("fo_by_id", Some(doc! { "_id": "user42" })).await;
        assert!(result.is_object(), "find_one by _id should return a document");
        assert_eq!(result["_id"], "user42");
        assert_eq!(result["name"], "Diana");
        assert_eq!(result["role"], "admin");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_with_projection() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "fo_proj",
            doc! {
                "_id": "p1",
                "name": "Frank",
                "age": 40,
                "email": "frank@test.com"
            },
        )
        .await;

        let options = FindOneOptionsWrapper {
            projection: Some(DocumentWrapper::from(doc! { "name": 1, "_id": 0 })),
            sort: None,
            skip: None,
            hint: None,
            max: None,
            read_concern: None,
            max_time: None,
            selection_criteria: None,
            allow_partial_results: None,
            collation: None,
            comment: None,
            show_record_id: None,
            return_key: None,
            let_vars: None,
            comment_bson: None,
            max_scan: None,
            min: None,
        };
        let result = ctx.find_one_with_options("fo_proj", Some(doc! { "_id": "p1" }), options).await;
        assert!(result.is_object(), "find_one with projection should return a document");
        assert!(result.get("name").is_some(), "projected field 'name' should be present");
        assert_eq!(result["name"], "Frank");
        assert!(result.get("_id").is_none(), "_id should be excluded by projection");
        assert!(result.get("age").is_none(), "non-projected field 'age' should be excluded");
        assert!(result.get("email").is_none(), "non-projected field 'email' should be excluded");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_with_sort() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("fo_sort", doc! { "_id": "a", "name": "Alice", "score": 80 }).await;
        ctx.insert_one("fo_sort", doc! { "_id": "b", "name": "Bob", "score": 95 }).await;
        ctx.insert_one("fo_sort", doc! { "_id": "c", "name": "Charlie", "score": 70 }).await;

        // Sort descending by score: should return Bob (highest score)
        let options = FindOneOptionsWrapper {
            sort: Some(DocumentWrapper::from(doc! { "score": -1 })),
            projection: None,
            skip: None,
            hint: None,
            max: None,
            read_concern: None,
            max_time: None,
            selection_criteria: None,
            allow_partial_results: None,
            collation: None,
            comment: None,
            show_record_id: None,
            return_key: None,
            let_vars: None,
            comment_bson: None,
            max_scan: None,
            min: None,
        };
        let result = ctx.find_one_with_options("fo_sort", None, options).await;
        assert!(result.is_object(), "find_one with sort should return a document");
        assert_eq!(result["name"], "Bob", "descending sort by score should return Bob first");
        assert_eq!(result["score"], 95);

        // Sort ascending by score: should return Charlie (lowest score)
        let options = FindOneOptionsWrapper {
            sort: Some(DocumentWrapper::from(doc! { "score": 1 })),
            projection: None,
            skip: None,
            hint: None,
            max: None,
            read_concern: None,
            max_time: None,
            selection_criteria: None,
            allow_partial_results: None,
            collation: None,
            comment: None,
            show_record_id: None,
            return_key: None,
            let_vars: None,
            comment_bson: None,
            max_scan: None,
            min: None,
        };
        let result = ctx.find_one_with_options("fo_sort", None, options).await;
        assert_eq!(result["name"], "Charlie", "ascending sort by score should return Charlie first");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_nested_field() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "fo_nested",
            doc! {
                "_id": "n1",
                "name": "Grace",
                "address": { "city": "Portland", "state": "OR" }
            },
        )
        .await;
        ctx.insert_one(
            "fo_nested",
            doc! {
                "_id": "n2",
                "name": "Hank",
                "address": { "city": "Seattle", "state": "WA" }
            },
        )
        .await;

        let result = ctx.find_one("fo_nested", Some(doc! { "address.city": "Seattle" })).await;
        assert!(result.is_object(), "find_one on nested field should return a document");
        assert_eq!(result["_id"], "n2");
        assert_eq!(result["name"], "Hank");
        assert_eq!(result["address"]["city"], "Seattle");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_returns_full_document() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "fo_full",
            doc! {
                "_id": "full1",
                "name": "Ivy",
                "age": 28,
                "email": "ivy@test.com",
                "tags": ["dev", "rust"],
                "profile": { "bio": "Hello", "active": true }
            },
        )
        .await;

        let result = ctx.find_one("fo_full", Some(doc! { "_id": "full1" })).await;
        assert!(result.is_object(), "find_one should return a document");
        assert_eq!(result["_id"], "full1");
        assert_eq!(result["name"], "Ivy");
        assert_eq!(result["age"], 28);
        assert_eq!(result["email"], "ivy@test.com");
        let tags = result["tags"].as_array().expect("tags should be an array");
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0], "dev");
        assert_eq!(tags[1], "rust");
        assert_eq!(result["profile"]["bio"], "Hello");
        assert_eq!(result["profile"]["active"], true);

        ctx.stop().await;
    }

    /// Compound query: find an admin user in either engineering or security department.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_complex_and_or() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("fo_andor", doc! { "_id": "u1", "name": "Alice",   "role": "admin", "department": "marketing" }).await;
        ctx.insert_one("fo_andor", doc! { "_id": "u2", "name": "Bob",     "role": "user",  "department": "engineering" }).await;
        ctx.insert_one("fo_andor", doc! { "_id": "u3", "name": "Charlie", "role": "admin", "department": "engineering" }).await;
        ctx.insert_one("fo_andor", doc! { "_id": "u4", "name": "Diana",   "role": "admin", "department": "security" }).await;
        ctx.insert_one("fo_andor", doc! { "_id": "u5", "name": "Eve",     "role": "user",  "department": "security" }).await;

        let result = ctx
            .find_one(
                "fo_andor",
                Some(doc! {
                    "$and": [
                        { "role": "admin" },
                        { "$or": [
                            { "department": "engineering" },
                            { "department": "security" }
                        ]}
                    ]
                }),
            )
            .await;
        assert!(result.is_object(), "find_one should return a document");
        let name = result["name"].as_str().expect("name should be string");
        // MongoDB returns the first matching document in natural order; Charlie or Diana are valid
        assert!(
            name == "Charlie" || name == "Diana",
            "should find an admin in engineering or security, got: {}",
            name
        );
        assert_eq!(result["role"], "admin");
        let dept = result["department"].as_str().expect("department should be string");
        assert!(dept == "engineering" || dept == "security");

        ctx.stop().await;
    }

    /// Array contains: find a document where a permissions array contains a specific value.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_with_array_contains() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "fo_arrcon",
            doc! {
                "_id": "r1", "name": "Viewer Role",
                "permissions": ["read_access", "view_reports"]
            },
        )
        .await;
        ctx.insert_one(
            "fo_arrcon",
            doc! {
                "_id": "r2", "name": "Editor Role",
                "permissions": ["read_access", "write_access", "view_reports"]
            },
        )
        .await;
        ctx.insert_one(
            "fo_arrcon",
            doc! {
                "_id": "r3", "name": "Auditor Role",
                "permissions": ["read_access", "view_reports", "export_data"]
            },
        )
        .await;

        let result = ctx.find_one("fo_arrcon", Some(doc! { "permissions": "write_access" })).await;
        assert!(result.is_object(), "find_one should return a document for array contains");
        assert_eq!(result["_id"], "r2");
        assert_eq!(result["name"], "Editor Role");

        ctx.stop().await;
    }

    /// Regex email pattern: find user by email domain using regex.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_regex_email_pattern() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("fo_remail", doc! { "_id": "u1", "name": "Alice", "email": "alice@gmail.com" }).await;
        ctx.insert_one("fo_remail", doc! { "_id": "u2", "name": "Bob",   "email": "bob@company.com" }).await;
        ctx.insert_one("fo_remail", doc! { "_id": "u3", "name": "Charlie", "email": "charlie@yahoo.com" }).await;
        ctx.insert_one("fo_remail", doc! { "_id": "u4", "name": "Diana", "email": "diana@outlook.com" }).await;

        let result = ctx
            .find_one(
                "fo_remail",
                Some(doc! {
                    "email": { "$regex": "@company\\.com$" }
                }),
            )
            .await;
        assert!(result.is_object(), "find_one should return a document for regex match");
        assert_eq!(result["_id"], "u2");
        assert_eq!(result["name"], "Bob");
        let email = result["email"].as_str().expect("email should be string");
        assert!(email.ends_with("@company.com"));

        ctx.stop().await;
    }

    /// Null vs missing: querying for null matches both explicit null and missing field.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_null_vs_missing() {
        use mongodb::bson::Bson;

        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("fo_null", doc! { "_id": "d1", "name": "Alice", "phone": Bson::Null }).await;
        ctx.insert_one("fo_null", doc! { "_id": "d2", "name": "Bob",   "phone": "555-1234" }).await;
        ctx.insert_one("fo_null", doc! { "_id": "d3", "name": "Charlie" }).await; // phone field missing entirely

        // Querying {"phone": null} matches both null-valued AND missing-field documents
        let result = ctx.find_one("fo_null", Some(doc! { "phone": Bson::Null })).await;
        assert!(result.is_object(), "find_one with null filter should return a document");
        let id = result["_id"].as_str().expect("_id should be string");
        // MongoDB returns one of the matching docs (d1 with null or d3 with missing field)
        assert!(
            id == "d1" || id == "d3",
            "null query should match either null-valued or missing-field doc, got _id: {}",
            id
        );

        ctx.stop().await;
    }

    /// Type operator: find a document where a field is a specific BSON type.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_find_one_type_operator() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("fo_type", doc! { "_id": "d1", "label": "config_a", "value": "enabled" }).await;
        ctx.insert_one("fo_type", doc! { "_id": "d2", "label": "config_b", "value": 42_i32 }).await;
        ctx.insert_one("fo_type", doc! { "_id": "d3", "label": "config_c", "value": std::f64::consts::PI }).await;

        let result = ctx
            .find_one(
                "fo_type",
                Some(doc! {
                    "value": { "$type": "string" }
                }),
            )
            .await;
        assert!(result.is_object(), "find_one should return a document for $type match");
        assert_eq!(result["_id"], "d1");
        assert_eq!(result["value"], "enabled");

        ctx.stop().await;
    }
}
