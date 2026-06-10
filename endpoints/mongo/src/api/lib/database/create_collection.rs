use crate::api::lib::{DatabaseApi, MongoApi};
use crate::api::wrapper::{
    AcknowledgmentWrapper, BsonWrapper, ChangeStreamPreAndPostImagesWrapper, ClusteredIndexWrapper, CollationStrengthWrapper,
    CollationWrapper, CreateCollectionOptionsWrapper, DocumentWrapper, DurationWrapper, IndexOptionDefaultsWrapper,
    TimeseriesGranularityWrapper, TimeseriesOptionsWrapper, ValidationActionWrapper, ValidationLevelWrapper, WriteConcernWrapper,
};
use crate::output::{DatabaseOutput, EmptyOutput, StringOutput};
use crate::{ApiExample, ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Database;
use mongodb::bson::doc;
use telemetry::TelemetryWrapper;
use tokio::sync::OnceCell;

pub struct SimpleCreateCollection;
pub struct ComplexCreateCollection;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, CreateCollectionInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::CreateCollection)),
    "Creates a new collection in the database with the given name and options",
    ReqType::Read,
    true,
);

static EXAMPLES: OnceCell<Vec<ApiExample<CreateCollectionInput>>> = OnceCell::const_new();

pub(crate) async fn create_collection_examples() -> &'static [ApiExample<CreateCollectionInput>] {
    EXAMPLES
        .get_or_init(|| async {
            vec![
                // Example 1: Basic collection creation
                ApiExample {
                    name: "Basic Collection Creation",
                    description: "Create a simple collection with default options",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "users".to_string(),
                        options: None,
                    },
                    response: Ok(Some(serde_json::Value::from("Collection users created successfully"))),
                },
                // Example 2: Create capped collection
                ApiExample {
                    name: "Create Capped Collection",
                    description: "Create a capped collection with size limit",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "system_logs".to_string(),
                        options: Some(CreateCollectionOptionsWrapper {
                            capped: Some(true),
                            size: Some(1048576), // 1MB size limit
                            max: Some(1000),     // Maximum 1000 documents
                            ..Default::default()
                        }),
                    },
                    response: Ok(Some(serde_json::Value::from("Collection system_logs created successfully"))),
                },
                // Example 3: Create collection with validation rules
                ApiExample {
                    name: "Collection with Schema Validation",
                    description: "Create a collection with document validation schema",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "products".to_string(),
                        options: Some(CreateCollectionOptionsWrapper {
                            validator: Some(DocumentWrapper::from(doc! {
                                "$jsonSchema": {
                                    "bsonType": "object",
                                    "required": ["name", "price", "category"],
                                    "properties": {
                                        "name": {
                                            "bsonType": "string",
                                            "description": "must be a string and is required"
                                        },
                                        "price": {
                                            "bsonType": "double",
                                            "minimum": 0,
                                            "description": "must be a positive number and is required"
                                        },
                                        "category": {
                                            "bsonType": "string",
                                            "description": "must be a string and is required"
                                        }
                                    }
                                }
                            })),
                            validation_level: Some(ValidationLevelWrapper::Strict),
                            validation_action: Some(ValidationActionWrapper::Error),
                            ..Default::default()
                        }),
                    },
                    response: Ok(Some(serde_json::Value::from("Collection products created successfully"))),
                },
                // Example 4: Create collection with collation settings
                ApiExample {
                    name: "Collection with Collation",
                    description: "Create a collection with specific language collation settings",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "french_documents".to_string(),
                        options: Some(CreateCollectionOptionsWrapper {
                            collation: Some(CollationWrapper {
                                locale: "fr".to_string(),
                                case_level: Some(true),
                                strength: Some(CollationStrengthWrapper::Secondary),
                                alternate: None,
                                backwards: None,
                                case_first: None,
                                max_variable: None,
                                normalization: None,
                                numeric_ordering: None,
                            }),
                            ..Default::default()
                        }),
                    },
                    response: Ok(Some(serde_json::Value::from("Collection french_documents created successfully"))),
                },
                // Example 5: Create time series collection
                ApiExample {
                    name: "Time Series Collection",
                    description: "Create a time series collection for time-based data",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "server_metrics".to_string(),
                        options: Some(CreateCollectionOptionsWrapper {
                            timeseries: Some(TimeseriesOptionsWrapper {
                                time_field: "timestamp".to_string(),
                                meta_field: Some("metadata".to_string()),
                                granularity: Some(TimeseriesGranularityWrapper::Hours),
                                bucket_max_span: None,
                                bucket_rounding: None,
                            }),
                            ..Default::default()
                        }),
                    },
                    response: Ok(Some(serde_json::Value::from("Collection server_metrics created successfully"))),
                },
                // Example 7: Create view collection
                ApiExample {
                    name: "Create View",
                    description: "Create a view based on an existing collection with a filter",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "active_users_view".to_string(),
                        options: Some(CreateCollectionOptionsWrapper {
                            view_on: Some("users".to_string()),
                            pipeline: Some(vec![
                                DocumentWrapper::from(doc! {
                                    "$match": {
                                        "status": "active",
                                        "last_login": { "$gt": { "$date": "2025-01-01T00:00:00Z" } }
                                    }
                                }),
                                DocumentWrapper::from(doc! {
                                    "$project": {
                                        "_id": 1,
                                        "username": 1,
                                        "email": 1,
                                        "last_login": 1
                                    }
                                }),
                            ]),
                            ..Default::default()
                        }),
                    },
                    response: Ok(Some(serde_json::Value::from("Collection active_users_view created successfully"))),
                },
                // Example 8: Create change stream enabled collection
                ApiExample {
                    name: "Change Stream Collection",
                    description: "Create a collection with change stream pre and post images enabled",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "entity_changes".to_string(),
                        options: Some(CreateCollectionOptionsWrapper {
                            change_stream_pre_and_post_images: Some(ChangeStreamPreAndPostImagesWrapper::ENABLED),
                            ..Default::default()
                        }),
                    },
                    response: Ok(Some(serde_json::Value::from("Collection entity_changes created successfully"))),
                },
                // Example 9: Create collection with expiration index
                ApiExample {
                    name: "TTL Collection",
                    description: "Create a time-series collection with time-to-live index for automatic document expiration",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "user_sessions".to_string(),
                        options: Some(CreateCollectionOptionsWrapper {
                            expire_after_seconds: Some(DurationWrapper::from_secs(3600)), // Documents expire after 1 hour
                            timeseries: Some(TimeseriesOptionsWrapper {
                                time_field: "created_at".to_string(),
                                meta_field: Some("user_id".to_string()),
                                granularity: Some(TimeseriesGranularityWrapper::Minutes),
                                bucket_max_span: None,
                                bucket_rounding: None,
                            }),
                            ..Default::default()
                        }),
                    },
                    response: Ok(Some(serde_json::Value::from("Collection user_sessions created successfully"))),
                },
                // Example 10: Create collection with index option defaults
                ApiExample {
                    name: "Collection with Index Option Defaults",
                    description: "Create a collection with default options for all indexes",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "indexed_data".to_string(),
                        options: Some(CreateCollectionOptionsWrapper {
                            index_option_defaults: Some(IndexOptionDefaultsWrapper {
                                storage_engine: DocumentWrapper::from(doc! {
                                    "wiredTiger": {
                                        "configString": "prefix_compression=true,prefix_compression_min=4"
                                    }
                                }),
                            }),
                            ..Default::default()
                        }),
                    },
                    response: Ok(Some(serde_json::Value::from("Collection indexed_data created successfully"))),
                },
                // Example 11: Create collection with clustered index
                ApiExample {
                    name: "Clustered Index Collection",
                    description: "Create a collection with a clustered index",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "customer_orders".to_string(),
                        options: Some(CreateCollectionOptionsWrapper {
                            clustered_index: Some(ClusteredIndexWrapper {
                                key: DocumentWrapper::from(doc! { "customer_id": 1, "order_date": -1 }),
                                unique: false,
                                name: Some("customer_order_idx".to_string()),
                                v: None,
                            }),
                            ..Default::default()
                        }),
                    },
                    response: Ok(Some(serde_json::Value::from("Collection customer_orders created successfully"))),
                },
                // Example 12: Create collection with write concern options
                ApiExample {
                    name: "High Durability Collection",
                    description: "Create a collection with strict write concern for critical data",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "transactions".to_string(),
                        options: Some(CreateCollectionOptionsWrapper {
                            write_concern: Some(WriteConcernWrapper {
                                w: Some(AcknowledgmentWrapper::Majority),
                                journal: Some(true),
                                w_timeout: Some(DurationWrapper::from_millis(5000)),
                            }),
                            ..Default::default()
                        }),
                    },
                    response: Ok(Some(serde_json::Value::from("Collection transactions created successfully"))),
                },
                // Example 13: Create collection with comment
                ApiExample {
                    name: "Collection with Comment",
                    description: "Create a collection with a comment for documentation",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "e_commerce".to_string(),
                        options: Some(CreateCollectionOptionsWrapper {
                            comment: Some(BsonWrapper::String("Product catalog for e-commerce system v2".to_string())),
                            ..Default::default()
                        }),
                    },
                    response: Ok(Some(serde_json::Value::from("Collection e_commerce created successfully"))),
                },
                // Example 14: Create collection with all options combined
                ApiExample {
                    name: "Advanced Configuration Collection",
                    description: "Create a collection with multiple configuration options for complex use cases",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "complex_data".to_string(),
                        options: Some(CreateCollectionOptionsWrapper {
                            capped: Some(true),
                            size: Some(10485760), // 10MB
                            max: Some(5000),
                            validator: Some(DocumentWrapper::from(doc! {
                                "$jsonSchema": {
                                    "bsonType": "object",
                                    "required": ["timestamp", "source", "data"],
                                    "properties": {
                                        "timestamp": {
                                            "bsonType": "date",
                                            "description": "must be a date and is required"
                                        },
                                        "source": {
                                            "bsonType": "string",
                                            "description": "must be a string and is required"
                                        },
                                        "data": {
                                            "bsonType": "object",
                                            "description": "must be an object and is required"
                                        }
                                    }
                                }
                            })),
                            validation_level: Some(ValidationLevelWrapper::Moderate),
                            validation_action: Some(ValidationActionWrapper::Warn),
                            collation: Some(CollationWrapper {
                                locale: "en".to_string(),
                                strength: Some(CollationStrengthWrapper::Tertiary),
                                case_level: Some(false),
                                numeric_ordering: Some(true),
                                ..Default::default()
                            }),
                            write_concern: Some(WriteConcernWrapper {
                                w: Some(AcknowledgmentWrapper::Majority),
                                journal: Some(true),
                                w_timeout: Some(DurationWrapper::from_millis(2000)),
                            }),
                            comment: Some(BsonWrapper::String("Complex configuration example".to_string())),
                            ..Default::default()
                        }),
                    },
                    response: Ok(Some(serde_json::Value::from("Collection complex_data created successfully"))),
                },
                // Example 15: Create collection with minimal required options
                ApiExample {
                    name: "Minimal Collection with Name Only",
                    description: "Create a collection with only the required database and name parameters",
                    request: CreateCollectionInput {
                        database: "test_db".to_string(),
                        name: "simple_collection".to_string(),
                        options: None,
                    },
                    response: Ok(Some(serde_json::Value::from("Collection simple_collection created successfully"))),
                },
            ]
        })
        .await
}

crate::mongo_endpoint! {
    API_INFO,
    struct CreateCollectionInput {
        database: String,
        name: String,
        options: Option<CreateCollectionOptionsWrapper>,
    }
}

type OutputWrapper = EmptyOutput;
type ExpectedInput = DatabaseOutput;

impl_simple_operation!(CreateCollectionInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl CreateCollectionInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));
        let context = context.get().await.map_err(EpError::connect)?;

        self.run_create_collection(&context.database(&self.database)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }

    async fn run_create_collection(&self, database: &Database) -> ResultEP<Box<dyn EpOutput>> {
        database.create_collection(&self.name, self.options.to_owned().map(Into::into)).await.map_err(EpError::database)?;

        Ok(Box::new(StringOutput(format!("Collection {} created successfully", &self.name)).to_output()).as_output())
    }
}

#[cfg(test)]
mod create_collection_test {
    #![allow(unexpected_cfgs)]

    use super::*;
    use crate::test_utils::database_test_utils::{generic_write_async_test, generic_write_sync_test};

    #[tokio::test]
    async fn sync_test() {
        generic_write_sync_test(create_collection_examples().await).await;
    }

    #[tokio::test]
    async fn async_test() {
        generic_write_async_test(create_collection_examples().await).await;
    }

    #[cfg(feature = "integration")]
    use crate::api::wrapper::CreateCollectionOptionsWrapper;
    #[cfg(feature = "integration")]
    use crate::test_utils::integration_test_utils::MongoTestContext;
    #[cfg(feature = "integration")]
    use mongodb::bson::doc;
    #[cfg(feature = "integration")]
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_create_collection_basic() {
        let mut ctx = MongoTestContext::new().await;

        ctx.create_collection("cc_basic").await;

        let names = ctx.list_collection_names().await;
        let arr = names.as_array().expect("list_collection_names should return an array");
        let found = arr.iter().any(|v| v == "cc_basic");
        assert!(found, "created collection cc_basic should appear in list_collection_names");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_create_collection_capped() {
        let mut ctx = MongoTestContext::new().await;

        ctx.create_collection_with_options(
            "cc_capped",
            CreateCollectionOptionsWrapper {
                capped: Some(true),
                size: Some(1048576),
                ..Default::default()
            },
        )
        .await;

        let names = ctx.list_collection_names().await;
        let arr = names.as_array().expect("list_collection_names should return an array");
        let found = arr.iter().any(|v| v == "cc_capped");
        assert!(found, "capped collection cc_capped should appear in list_collection_names");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_create_collection_already_exists() {
        let mut ctx = MongoTestContext::new().await;

        ctx.create_collection("cc_exists").await;
        // Creating a collection that already exists should not panic
        // MongoDB may return an error or silently succeed depending on the version
        let _result = ctx.create_collection("cc_exists").await;

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_create_collection_and_use() {
        let mut ctx = MongoTestContext::new().await;

        ctx.create_collection("cc_use").await;

        ctx.insert_one("cc_use", doc! { "_id": "u1", "name": "Alice" }).await;

        let found = ctx.find_one("cc_use", Some(doc! { "_id": "u1" })).await;
        assert_eq!(found["name"], "Alice", "should be able to insert and query after explicit creation");

        ctx.stop().await;
    }
}
