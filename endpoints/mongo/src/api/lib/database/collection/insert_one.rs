use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{
    AcknowledgmentWrapper, BsonWrapper, DocumentWrapper, DurationWrapper, InsertOneOptionsWrapper, WriteConcernWrapper,
};
use crate::output::{InsertOneResultOutput, InsertOneResultWrapper};
use crate::{ApiExample, ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::{Bson, DateTime, Document, doc};
use telemetry::TelemetryWrapper;
use tokio::sync::OnceCell;

struct SimpleInsertOne;
struct ComplexInsertOne;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, InsertOneInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::InsertOne)))),
    "Inserts doc into the collection",
    ReqType::Write,
    true,
);

static EXAMPLES: OnceCell<Vec<ApiExample<InsertOneInput>>> = OnceCell::const_new();

pub(crate) async fn insert_one_examples() -> &'static [ApiExample<InsertOneInput>] {
    EXAMPLES
        .get_or_init(|| async {
            vec![
                // Example 1: Basic document insertion
                ApiExample {
                    name: "Basic Document Insertion",
                    description: "Insert a simple user document with default options",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "users".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "username": "john_doe",
                            "email": "john@example.com",
                            "age": 30,
                            "created_at": Bson::DateTime(DateTime::now())
                        }),
                        options: None,
                    },
                    response: Ok(None),
                },
                // Example 2: Insert with custom _id
                ApiExample {
                    name: "Insert with Custom ID",
                    description: "Insert a document with a specified _id field",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "products".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "_id": "PROD-12345",
                            "name": "Laptop Pro",
                            "price": 1299.99,
                            "category": "Electronics",
                            "stock": 50
                        }),
                        options: None,
                    },
                    response: Ok(Some(serde_json::json!({
                        "inserted_id": "PROD-12345"
                    }))),
                },
                // Example 3: Insert nested document
                ApiExample {
                    name: "Insert Nested Document",
                    description: "Insert a document with nested objects and arrays",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "orders".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "order_id": "ORD-2025-001",
                            "customer": {
                                "name": "Jane Smith",
                                "email": "jane@example.com",
                                "address": {
                                    "street": "123 Main St",
                                    "city": "New York",
                                    "zip": "10001"
                                }
                            },
                            "items": [
                                {
                                    "product_id": "PROD-12345",
                                    "quantity": 2,
                                    "price": 1299.99
                                },
                                {
                                    "product_id": "PROD-67890",
                                    "quantity": 1,
                                    "price": 49.99
                                }
                            ],
                            "total": 2649.97,
                            "status": "pending",
                            "created_at": Bson::DateTime(DateTime::now())
                        }),
                        options: None,
                    },
                    response: Ok(None),
                },
                // Example 4: Insert with bypass document validation
                ApiExample {
                    name: "Insert Bypassing Validation",
                    description: "Insert a document bypassing collection validation rules",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "products".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "name": "Special Product",
                            "price": -10.0, // Normally invalid due to validation
                            "category": "Special"
                        }),
                        options: Some(InsertOneOptionsWrapper {
                            bypass_document_validation: Some(true),
                            comment: None,
                            write_concern: None,
                        }),
                    },
                    response: Ok(None),
                },
                // Example 5: Insert with write concern
                ApiExample {
                    name: "Insert with Write Concern",
                    description: "Insert a critical document with majority write concern",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "transactions".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "transaction_id": "TXN-2025-001",
                            "amount": 5000.00,
                            "from_account": "ACC-123",
                            "to_account": "ACC-456",
                            "currency": "USD",
                            "status": "completed",
                            "timestamp": Bson::DateTime(DateTime::now())
                        }),
                        options: Some(InsertOneOptionsWrapper {
                            write_concern: Some(WriteConcernWrapper {
                                w: Some(AcknowledgmentWrapper::Majority),
                                journal: Some(true),
                                w_timeout: Some(DurationWrapper::from_millis(5000)),
                            }),
                            bypass_document_validation: None,
                            comment: None,
                        }),
                    },
                    response: Ok(None),
                },
                // Example 6: Insert with comment
                ApiExample {
                    name: "Insert with Comment",
                    description: "Insert a document with a comment for debugging/logging",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "audit_logs".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "event": "user_login",
                            "user_id": "USER-123",
                            "ip_address": "192.168.1.100",
                            "user_agent": "Mozilla/5.0...",
                            "timestamp": Bson::DateTime(DateTime::now()),
                            "success": true
                        }),
                        options: Some(InsertOneOptionsWrapper {
                            comment: Some(BsonWrapper::String("Audit log entry from authentication service".to_string())),
                            bypass_document_validation: None,
                            write_concern: None,
                        }),
                    },
                    response: Ok(None),
                },
                // Example 7: Insert time series data
                ApiExample {
                    name: "Insert Time Series Data",
                    description: "Insert a document into a time series collection",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "server_metrics".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "timestamp": Bson::DateTime(DateTime::now()),
                            "metadata": {
                                "server_id": "srv-001",
                                "region": "us-east-1"
                            },
                            "cpu_usage": 45.5,
                            "memory_usage": 78.2,
                            "disk_usage": 62.8,
                            "network_in": 1024.5,
                            "network_out": 2048.7
                        }),
                        options: None,
                    },
                    response: Ok(None),
                },
                // Example 8: Insert with array fields
                ApiExample {
                    name: "Insert Document with Arrays",
                    description: "Insert a blog post with tags and comments arrays",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "blog_posts".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "title": "Getting Started with MongoDB",
                            "author": "John Doe",
                            "content": "MongoDB is a NoSQL database...",
                            "tags": ["mongodb", "database", "tutorial", "nosql"],
                            "comments": [
                                {
                                    "user": "Alice",
                                    "text": "Great tutorial!",
                                    "date": Bson::DateTime(DateTime::now())
                                },
                                {
                                    "user": "Bob",
                                    "text": "Very helpful, thanks!",
                                    "date": Bson::DateTime(DateTime::now())
                                }
                            ],
                            "likes": 42,
                            "published": true,
                            "created_at": Bson::DateTime(DateTime::now())
                        }),
                        options: None,
                    },
                    response: Ok(None),
                },
                // Example 9: Insert with binary data
                ApiExample {
                    name: "Insert with Binary Data",
                    description: "Insert a document containing binary data (e.g., file metadata)",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "files".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "filename": "document.pdf",
                            "content_type": "application/pdf",
                            "size": 1048576,
                            "checksum": Bson::Binary(mongodb::bson::Binary {
                                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                                bytes: vec![0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0],
                            }),
                            "uploaded_by": "USER-123",
                            "uploaded_at": Bson::DateTime(DateTime::now()),
                            "metadata": {
                                "pages": 10,
                                "author": "John Smith",
                                "created": "2025-01-01"
                            }
                        }),
                        options: None,
                    },
                    response: Ok(None),
                },
                // Example 10: Insert with geospatial data
                ApiExample {
                    name: "Insert Geospatial Data",
                    description: "Insert a location document with GeoJSON data",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "locations".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "name": "Central Park",
                            "type": "park",
                            "location": {
                                "type": "Point",
                                "coordinates": [-73.965355, 40.782865]
                            },
                            "address": {
                                "street": "59th St to 110th St",
                                "city": "New York",
                                "state": "NY",
                                "country": "USA"
                            },
                            "amenities": ["playground", "restrooms", "bike_rental"],
                            "rating": 4.8,
                            "created_at": Bson::DateTime(DateTime::now())
                        }),
                        options: None,
                    },
                    response: Ok(None),
                },
                // Example 11: Insert with mixed data types
                ApiExample {
                    name: "Insert Mixed Type Document",
                    description: "Insert a document with various BSON data types",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "mixed_data".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "string_field": "Hello World",
                            "number_field": 42,
                            "double_field": std::f64::consts::PI,
                            "boolean_field": true,
                            "null_field": Bson::Null,
                            "date_field": Bson::DateTime(DateTime::now()),
                            "regex_field": Bson::RegularExpression(mongodb::bson::Regex {
                                pattern: "^test.*".to_string(),
                                options: "i".to_string(),
                            }),
                            "object_id_field": Bson::ObjectId(mongodb::bson::oid::ObjectId::new()),
                            "array_field": [1, 2, 3, 4, 5],
                            "nested_object": {
                                "key1": "value1",
                                "key2": 123
                            }
                        }),
                        options: None,
                    },
                    response: Ok(None),
                },
                // Example 12: Insert with selection criteria
                ApiExample {
                    name: "Insert with Selection Criteria",
                    description: "Insert with specific server selection criteria",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "replicated_data".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "data": "Important replicated information",
                            "priority": "high",
                            "timestamp": Bson::DateTime(DateTime::now())
                        }),
                        options: Some(InsertOneOptionsWrapper {
                            bypass_document_validation: None,
                            comment: None,
                            write_concern: None,
                        }),
                    },
                    response: Ok(None),
                },
                // Example 13: Insert with all options
                ApiExample {
                    name: "Insert with All Options",
                    description: "Insert using all available options for maximum control",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "critical_data".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "operation": "critical_update",
                            "target": "production_system",
                            "performed_by": "admin_user",
                            "changes": {
                                "before": "value_old",
                                "after": "value_new"
                            },
                            "timestamp": Bson::DateTime(DateTime::now()),
                            "approved": true
                        }),
                        options: Some(InsertOneOptionsWrapper {
                            bypass_document_validation: Some(false),
                            comment: Some(BsonWrapper::String("Critical operation logged with full audit trail".to_string())),
                            write_concern: Some(WriteConcernWrapper {
                                w: Some(AcknowledgmentWrapper::Majority),
                                journal: Some(true),
                                w_timeout: Some(DurationWrapper::from_millis(10000)),
                            }),
                        }),
                    },
                    response: Ok(None),
                },
                // Example 14: Insert empty document
                ApiExample {
                    name: "Insert Empty Document",
                    description: "Insert a minimal document (MongoDB will auto-generate _id)",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "test_collection".to_string(),
                        doc: DocumentWrapper::from(doc! {}),
                        options: None,
                    },
                    response: Ok(None),
                },
                // Example 15: Insert IoT sensor data
                ApiExample {
                    name: "Insert IoT Sensor Data",
                    description: "Insert real-time sensor readings from IoT devices",
                    request: InsertOneInput {
                        database: "test_db".to_string(),
                        collection: "sensor_readings".to_string(),
                        doc: DocumentWrapper::from(doc! {
                            "device_id": "sensor-temp-001",
                            "location": {
                                "building": "A",
                                "floor": 3,
                                "room": "301"
                            },
                            "readings": {
                                "temperature": 22.5,
                                "humidity": 45.2,
                                "pressure": 1013.25
                            },
                            "battery_level": 87,
                            "signal_strength": -45,
                            "timestamp": Bson::DateTime(DateTime::now()),
                            "anomaly_detected": false
                        }),
                        options: None,
                    },
                    response: Ok(None),
                },
            ]
        })
        .await
}

crate::mongo_endpoint! {
    API_INFO,
    struct InsertOneInput {
        database: String,
        collection: String,
        doc: DocumentWrapper,
        options: Option<InsertOneOptionsWrapper>,
    }
}

impl_simple_operation!(InsertOneInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl InsertOneInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_insert_one(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_insert_one(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            InsertOneResultOutput(InsertOneResultWrapper::from(
                context.insert_one(&self.doc.clone().into(), self.options.to_owned().map(Into::into)).await.map_err(EpError::database)?,
            ))
            .to_output(),
        ) as Box<dyn EpOutput>)
    }
}

#[cfg(test)]
mod create_collection_test {
    #![allow(unexpected_cfgs)]

    use super::*;
    use crate::test_utils::database_test_utils::{generic_write_async_test, generic_write_sync_test};

    #[tokio::test]
    async fn sync_test() {
        generic_write_sync_test(insert_one_examples().await).await;
    }

    #[tokio::test]
    async fn async_test() {
        generic_write_async_test(insert_one_examples().await).await;
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    #![allow(unexpected_cfgs)]
    use super::*;
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_basic() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .insert_one(
                "insert_basic",
                doc! {
                    "name": "Alice",
                    "age": 30
                },
            )
            .await;

        assert!(result["inserted_id"].is_object(), "inserted_id should be an auto-generated ObjectId");
        assert!(result["inserted_id"]["$oid"].is_string(), "inserted_id should contain $oid field");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_custom_id() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .insert_one(
                "insert_custom_id",
                doc! {
                    "_id": "custom123",
                    "name": "Bob"
                },
            )
            .await;

        assert_eq!(result["inserted_id"], "custom123");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_nested_document() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .insert_one(
                "insert_nested",
                doc! {
                    "_id": "nested1",
                    "name": "Charlie",
                    "address": {
                        "city": "Portland",
                        "state": "OR"
                    }
                },
            )
            .await;

        assert_eq!(result["inserted_id"], "nested1");

        let found = ctx.find_one("insert_nested", Some(doc! { "_id": "nested1" })).await;
        assert_eq!(found["address"]["city"], "Portland");
        assert_eq!(found["address"]["state"], "OR");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_with_arrays() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .insert_one(
                "insert_arrays",
                doc! {
                    "_id": "arr1",
                    "name": "Dave",
                    "tags": ["a", "b", "c"]
                },
            )
            .await;

        assert_eq!(result["inserted_id"], "arr1");

        let found = ctx.find_one("insert_arrays", Some(doc! { "_id": "arr1" })).await;
        assert_eq!(found["tags"][0], "a");
        assert_eq!(found["tags"][1], "b");
        assert_eq!(found["tags"][2], "c");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_empty_document() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.insert_one("insert_empty", doc! {}).await;

        assert!(result["inserted_id"].is_object(), "inserted_id should be an auto-generated ObjectId");
        assert!(result["inserted_id"]["$oid"].is_string(), "inserted_id should contain $oid field");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_numeric_types() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .insert_one(
                "insert_numeric",
                doc! {
                    "_id": "num1",
                    "int32_field": 42_i32,
                    "int64_field": 9_999_999_999_i64,
                    "float64_field": std::f64::consts::PI
                },
            )
            .await;

        assert_eq!(result["inserted_id"], "num1");

        let found = ctx.find_one("insert_numeric", Some(doc! { "_id": "num1" })).await;
        assert_eq!(found["int32_field"], 42);
        assert_eq!(found["int64_field"], 9_999_999_999_i64);
        let found_float = found["float64_field"].as_f64().expect("float64_field should be f64");
        assert!((found_float - std::f64::consts::PI).abs() < 0.000_001);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_boolean_and_null() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .insert_one(
                "insert_bool_null",
                doc! {
                    "_id": "bn1",
                    "active": true,
                    "deleted": false,
                    "middle_name": Bson::Null
                },
            )
            .await;

        assert_eq!(result["inserted_id"], "bn1");

        let found = ctx.find_one("insert_bool_null", Some(doc! { "_id": "bn1" })).await;
        assert_eq!(found["active"], true);
        assert_eq!(found["deleted"], false);
        assert!(found["middle_name"].is_null());

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_datetime() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .insert_one(
                "insert_datetime",
                doc! {
                    "_id": "dt1",
                    "created_at": Bson::DateTime(DateTime::now())
                },
            )
            .await;

        assert_eq!(result["inserted_id"], "dt1");

        let found = ctx.find_one("insert_datetime", Some(doc! { "_id": "dt1" })).await;
        assert!(found["created_at"].is_object(), "created_at should be a datetime object");
        assert!(
            found["created_at"]["$date"].is_object() || found["created_at"]["$date"].is_string(),
            "created_at should contain $date field"
        );

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_binary_data() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .insert_one(
                "insert_binary",
                doc! {
                    "_id": "bin1",
                    "payload": Bson::Binary(mongodb::bson::Binary {
                        subtype: mongodb::bson::spec::BinarySubtype::Generic,
                        bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
                    })
                },
            )
            .await;

        assert_eq!(result["inserted_id"], "bin1");

        let found = ctx.find_one("insert_binary", Some(doc! { "_id": "bin1" })).await;
        assert!(!found["payload"].is_null(), "payload should be present");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_duplicate_id_fails() {
        let mut ctx = MongoTestContext::new().await;

        let _first = ctx
            .insert_one(
                "insert_dup",
                doc! {
                    "_id": "dup1",
                    "name": "First"
                },
            )
            .await;

        let _err = ctx
            .insert_one_err(
                "insert_dup",
                doc! {
                    "_id": "dup1",
                    "name": "Second"
                },
            )
            .await;

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_large_document() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .insert_one(
                "insert_large",
                doc! {
                    "_id": "large1",
                    "field_01": "value_01",
                    "field_02": "value_02",
                    "field_03": "value_03",
                    "field_04": "value_04",
                    "field_05": "value_05",
                    "field_06": "value_06",
                    "field_07": "value_07",
                    "field_08": "value_08",
                    "field_09": "value_09",
                    "field_10": "value_10",
                    "field_11": "value_11",
                    "field_12": "value_12",
                    "field_13": "value_13",
                    "field_14": "value_14",
                    "field_15": "value_15",
                    "field_16": "value_16",
                    "field_17": "value_17",
                    "field_18": "value_18",
                    "field_19": "value_19",
                    "field_20": "value_20",
                    "field_21": "value_21",
                    "field_22": "value_22"
                },
            )
            .await;

        assert_eq!(result["inserted_id"], "large1");

        let found = ctx.find_one("insert_large", Some(doc! { "_id": "large1" })).await;
        assert_eq!(found["field_01"], "value_01");
        assert_eq!(found["field_22"], "value_22");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_unicode_strings() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .insert_one(
                "insert_unicode",
                doc! {
                    "_id": "uni1",
                    "greeting_jp": "\u{3053}\u{3093}\u{306b}\u{3061}\u{306f}",
                    "greeting_cn": "\u{4f60}\u{597d}",
                    "greeting_kr": "\u{c548}\u{b155}\u{d558}\u{c138}\u{c694}",
                    "emoji": "\u{1f600}\u{1f680}\u{1f30d}"
                },
            )
            .await;

        assert_eq!(result["inserted_id"], "uni1");

        let found = ctx.find_one("insert_unicode", Some(doc! { "_id": "uni1" })).await;
        assert_eq!(found["greeting_jp"], "\u{3053}\u{3093}\u{306b}\u{3061}\u{306f}");
        assert_eq!(found["emoji"], "\u{1f600}\u{1f680}\u{1f30d}");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_special_chars() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .insert_one(
                "insert_special",
                doc! {
                    "_id": "spec1",
                    "with_quotes": "He said \"hello\"",
                    "with_newline": "line1\nline2",
                    "with_backslash": "path\\to\\file",
                    "with_tab": "col1\tcol2"
                },
            )
            .await;

        assert_eq!(result["inserted_id"], "spec1");

        let found = ctx.find_one("insert_special", Some(doc! { "_id": "spec1" })).await;
        assert_eq!(found["with_quotes"], "He said \"hello\"");
        assert_eq!(found["with_newline"], "line1\nline2");
        assert_eq!(found["with_backslash"], "path\\to\\file");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_deeply_nested() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .insert_one(
                "insert_deep",
                doc! {
                    "_id": "deep1",
                    "level1": {
                        "level2": {
                            "level3": {
                                "level4": {
                                    "level5": "deep_value"
                                }
                            }
                        }
                    }
                },
            )
            .await;

        assert_eq!(result["inserted_id"], "deep1");

        let found = ctx.find_one("insert_deep", Some(doc! { "_id": "deep1" })).await;
        assert_eq!(found["level1"]["level2"]["level3"]["level4"]["level5"], "deep_value");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_one_verify_with_find() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx
            .insert_one(
                "insert_verify",
                doc! {
                    "_id": "verify1",
                    "name": "Eve",
                    "age": 28,
                    "email": "eve@example.com",
                    "active": true,
                    "scores": [95, 87, 92]
                },
            )
            .await;

        assert_eq!(result["inserted_id"], "verify1");

        let found = ctx.find_one("insert_verify", Some(doc! { "_id": "verify1" })).await;
        assert_eq!(found["_id"], "verify1");
        assert_eq!(found["name"], "Eve");
        assert_eq!(found["age"], 28);
        assert_eq!(found["email"], "eve@example.com");
        assert_eq!(found["active"], true);
        assert_eq!(found["scores"][0], 95);
        assert_eq!(found["scores"][1], 87);
        assert_eq!(found["scores"][2], 92);

        ctx.stop().await;
    }

    // ---- Multi-step workflow integration tests ----

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_ecommerce_order_lifecycle() {
        let mut ctx = MongoTestContext::new().await;

        // Step 1: Insert a customer
        let cust_result = ctx
            .insert_one(
                "wf_customers",
                doc! {
                    "_id": "cust1",
                    "name": "Alice Johnson",
                    "email": "alice@example.com",
                    "balance": 500.0
                },
            )
            .await;
        assert_eq!(cust_result["inserted_id"], "cust1");

        // Step 2: Insert 3 products
        let prod_results = ctx
            .insert_many(
                "wf_products",
                vec![
                    doc! { "_id": "prod1", "name": "Widget A", "price": 29.99, "stock": 100 },
                    doc! { "_id": "prod2", "name": "Widget B", "price": 49.99, "stock": 50 },
                    doc! { "_id": "prod3", "name": "Widget C", "price": 19.99, "stock": 200 },
                ],
            )
            .await;
        assert!(prod_results["inserted_ids"].is_object(), "insert_many should return inserted_ids");

        // Step 3: Insert an order
        let order_result = ctx
            .insert_one(
                "wf_orders",
                doc! {
                    "_id": "ord1",
                    "customer_id": "cust1",
                    "items": [
                        { "product_id": "prod1", "qty": 2, "price": 29.99 },
                        { "product_id": "prod2", "qty": 1, "price": 49.99 }
                    ],
                    "status": "pending",
                    "total": 109.97
                },
            )
            .await;
        assert_eq!(order_result["inserted_id"], "ord1");

        // Step 4: Update order status to "processing"
        let update_result = ctx.update_one("wf_orders", doc! { "_id": "ord1" }, doc! { "$set": { "status": "processing" } }).await;
        assert_eq!(update_result["matched_count"], 1);
        assert_eq!(update_result["modified_count"], 1);

        // Step 5: Decrement product stock for prod1 (ordered qty: 2)
        let stock_result = ctx.update_one("wf_products", doc! { "_id": "prod1" }, doc! { "$inc": { "stock": -2 } }).await;
        assert_eq!(stock_result["matched_count"], 1);
        assert_eq!(stock_result["modified_count"], 1);

        // Step 6: Update customer balance
        let balance_result = ctx.update_one("wf_customers", doc! { "_id": "cust1" }, doc! { "$inc": { "balance": -109.97 } }).await;
        assert_eq!(balance_result["matched_count"], 1);
        assert_eq!(balance_result["modified_count"], 1);

        // Step 7: Update order status to "completed"
        let complete_result = ctx.update_one("wf_orders", doc! { "_id": "ord1" }, doc! { "$set": { "status": "completed" } }).await;
        assert_eq!(complete_result["matched_count"], 1);
        assert_eq!(complete_result["modified_count"], 1);

        // Step 8: Verify order status is completed
        let order = ctx.find_one("wf_orders", Some(doc! { "_id": "ord1" })).await;
        assert_eq!(order["status"], "completed");
        assert_eq!(order["customer_id"], "cust1");

        // Step 9: Verify customer balance reduced
        let customer = ctx.find_one("wf_customers", Some(doc! { "_id": "cust1" })).await;
        let balance = customer["balance"].as_f64().expect("balance should be a number");
        assert!((balance - 390.03).abs() < 0.01, "balance should be approximately 390.03, got {balance}");

        // Step 10: Verify product stock reduced
        let product = ctx.find_one("wf_products", Some(doc! { "_id": "prod1" })).await;
        assert_eq!(product["stock"], 98, "stock should be reduced from 100 to 98");

        // Step 11: Aggregate orders to compute total revenue from completed orders
        let revenue = ctx
            .aggregate(
                "wf_orders",
                vec![
                    doc! { "$match": { "status": "completed" } },
                    doc! { "$group": { "_id": null, "total_revenue": { "$sum": "$total" } } },
                ],
            )
            .await;
        let rev_arr = revenue.as_array().expect("aggregate should return an array");
        assert_eq!(rev_arr.len(), 1);
        let total_revenue = rev_arr[0]["total_revenue"].as_f64().expect("total_revenue should be a number");
        assert!(
            (total_revenue - 109.97).abs() < 0.01,
            "total revenue should be approximately 109.97, got {total_revenue}"
        );

        // Clean up all 3 collections
        ctx.drop_collection("wf_customers").await;
        ctx.drop_collection("wf_products").await;
        ctx.drop_collection("wf_orders").await;

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_user_crud_lifecycle() {
        let mut ctx = MongoTestContext::new().await;

        // Step 1: Insert a user
        let insert_result = ctx
            .insert_one(
                "wf_users",
                doc! {
                    "_id": "user1",
                    "name": "Bob",
                    "email": "bob@test.com",
                    "role": "viewer",
                    "login_count": 0,
                    "created_at": "2024-01-01"
                },
            )
            .await;
        assert_eq!(insert_result["inserted_id"], "user1");

        // Step 2: Find user by email, verify exists
        let found = ctx.find_one("wf_users", Some(doc! { "email": "bob@test.com" })).await;
        assert_eq!(found["_id"], "user1");
        assert_eq!(found["name"], "Bob");
        assert_eq!(found["role"], "viewer");

        // Step 3: Update role to "editor"
        let role_result = ctx.update_one("wf_users", doc! { "_id": "user1" }, doc! { "$set": { "role": "editor" } }).await;
        assert_eq!(role_result["matched_count"], 1);
        assert_eq!(role_result["modified_count"], 1);

        // Step 4: Increment login_count
        let login_result = ctx.update_one("wf_users", doc! { "_id": "user1" }, doc! { "$inc": { "login_count": 1 } }).await;
        assert_eq!(login_result["matched_count"], 1);
        assert_eq!(login_result["modified_count"], 1);

        // Step 5: Find user again, verify role changed and login_count incremented
        let updated = ctx.find_one("wf_users", Some(doc! { "_id": "user1" })).await;
        assert_eq!(updated["role"], "editor");
        assert_eq!(updated["login_count"], 1);

        // Step 6: Replace user doc entirely with new version (keeping _id)
        let replace_result = ctx
            .replace_one(
                "wf_users",
                doc! { "_id": "user1" },
                doc! {
                    "name": "Bob Smith",
                    "email": "bob.smith@test.com",
                    "role": "admin",
                    "login_count": 5,
                    "created_at": "2024-01-01",
                    "updated_at": "2024-06-15"
                },
            )
            .await;
        assert_eq!(replace_result["matched_count"], 1);
        assert_eq!(replace_result["modified_count"], 1);

        // Step 7: Find again, verify all fields from replacement
        let replaced = ctx.find_one("wf_users", Some(doc! { "_id": "user1" })).await;
        assert_eq!(replaced["name"], "Bob Smith");
        assert_eq!(replaced["email"], "bob.smith@test.com");
        assert_eq!(replaced["role"], "admin");
        assert_eq!(replaced["login_count"], 5);
        assert_eq!(replaced["updated_at"], "2024-06-15");

        // Step 8: Delete user
        let delete_result = ctx.delete_one("wf_users", doc! { "_id": "user1" }).await;
        assert_eq!(delete_result["deleted_count"], 1);

        // Step 9: Find user again, verify returns null/empty
        let deleted = ctx.find_one("wf_users", Some(doc! { "_id": "user1" })).await;
        assert!(deleted.is_null(), "deleted user should not be found");

        // Step 10: Count documents, verify 0
        let count = ctx.count_documents("wf_users", None).await;
        assert_eq!(count.as_u64().expect("count should be a number"), 0, "collection should be empty after delete");

        ctx.drop_collection("wf_users").await;

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_blog_post_workflow() {
        let mut ctx = MongoTestContext::new().await;

        // Step 1: Insert 5 blog posts
        ctx.insert_one(
            "wf_blog_posts",
            doc! {
                "_id": "post1",
                "title": "Getting Started with Rust",
                "author": "alice",
                "tags": ["rust", "programming", "tutorial"],
                "comments": [{ "user": "bob", "text": "Great post!" }],
                "published": true,
                "view_count": 0
            },
        )
        .await;
        ctx.insert_one(
            "wf_blog_posts",
            doc! {
                "_id": "post2",
                "title": "Advanced MongoDB Queries",
                "author": "alice",
                "tags": ["mongodb", "database", "tutorial"],
                "comments": [],
                "published": true,
                "view_count": 0
            },
        )
        .await;
        ctx.insert_one(
            "wf_blog_posts",
            doc! {
                "_id": "post3",
                "title": "Rust and WebAssembly",
                "author": "charlie",
                "tags": ["rust", "wasm", "web"],
                "comments": [{ "user": "dave", "text": "Very informative" }],
                "published": true,
                "view_count": 0
            },
        )
        .await;
        ctx.insert_one(
            "wf_blog_posts",
            doc! {
                "_id": "post4",
                "title": "Draft: Python Tips",
                "author": "bob",
                "tags": ["python", "tips"],
                "comments": [],
                "published": false,
                "view_count": 0
            },
        )
        .await;
        ctx.insert_one(
            "wf_blog_posts",
            doc! {
                "_id": "post5",
                "title": "Draft: Go Concurrency",
                "author": "charlie",
                "tags": ["go", "concurrency"],
                "comments": [],
                "published": false,
                "view_count": 0
            },
        )
        .await;

        // Step 2: Find all published posts
        let published = ctx.find("wf_blog_posts", Some(doc! { "published": true })).await;
        let pub_arr = published.as_array().expect("find should return an array");
        assert_eq!(pub_arr.len(), 3, "should have 3 published posts");

        // Step 3: Update view_count for a specific post
        let view_result = ctx.update_one("wf_blog_posts", doc! { "_id": "post1" }, doc! { "$inc": { "view_count": 10 } }).await;
        assert_eq!(view_result["matched_count"], 1);
        assert_eq!(view_result["modified_count"], 1);

        let viewed = ctx.find_one("wf_blog_posts", Some(doc! { "_id": "post1" })).await;
        assert_eq!(viewed["view_count"], 10, "view_count should be incremented to 10");

        // Step 4: Add a comment to a post via $push
        let comment_result = ctx
            .update_one(
                "wf_blog_posts",
                doc! { "_id": "post2" },
                doc! { "$push": { "comments": { "user": "eve", "text": "Thanks for sharing!" } } },
            )
            .await;
        assert_eq!(comment_result["matched_count"], 1);
        assert_eq!(comment_result["modified_count"], 1);

        let commented = ctx.find_one("wf_blog_posts", Some(doc! { "_id": "post2" })).await;
        let comments = commented["comments"].as_array().expect("comments should be an array");
        assert_eq!(comments.len(), 1, "post2 should now have 1 comment");
        assert_eq!(comments[0]["user"], "eve");

        // Step 5: Find posts by tag using {"tags": "rust"}
        let rust_posts = ctx.find("wf_blog_posts", Some(doc! { "tags": "rust" })).await;
        let rust_arr = rust_posts.as_array().expect("find should return an array");
        assert_eq!(rust_arr.len(), 2, "should have 2 posts tagged with rust");

        // Step 6: Aggregate - count posts per author, sort by count descending
        let author_counts = ctx
            .aggregate(
                "wf_blog_posts",
                vec![
                    doc! { "$group": { "_id": "$author", "post_count": { "$sum": 1 } } },
                    doc! { "$sort": { "post_count": -1 } },
                ],
            )
            .await;
        let counts_arr = author_counts.as_array().expect("aggregate should return an array");
        assert_eq!(counts_arr.len(), 3, "should have 3 distinct authors");
        // alice and charlie each have 2, bob has 1; alice or charlie should be first
        assert_eq!(counts_arr[0]["post_count"], 2, "top author should have 2 posts");
        assert_eq!(counts_arr[2]["post_count"], 1, "last author should have 1 post");

        // Step 7: Delete draft posts
        let delete_result = ctx.delete_many("wf_blog_posts", doc! { "published": false }).await;
        assert_eq!(delete_result["deleted_count"], 2, "should delete 2 draft posts");

        // Step 8: Verify remaining count equals published count
        let remaining = ctx.count_documents("wf_blog_posts", None).await;
        assert_eq!(remaining.as_u64().expect("count should be a number"), 3, "only 3 published posts should remain");

        ctx.drop_collection("wf_blog_posts").await;

        ctx.stop().await;
    }
}
