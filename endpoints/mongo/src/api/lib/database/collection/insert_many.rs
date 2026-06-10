use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DocumentFunction, DocumentWrapperType, InsertManyOptionsWrapper};
use crate::output::{CollectionDocumentOutput, InsertManyResultOutput, InsertManyResultWrapper};
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;

struct SimpleInsertMany;
struct ComplexInsertMany;
use crate::request::MongoRequest;
use format::endpoint::EpKind;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, InsertManyInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::InsertMany)))),
    "Inserts the data in docs into the collection",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct InsertManyInput {
        database: String,
        collection: String,
        docs: Vec<DocumentWrapperType>,
        options: Option<InsertManyOptionsWrapper>,
    }
}

type OutputWrapper = InsertManyResultOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(InsertManyInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl InsertManyInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_insert_many(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_insert_many(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        let docs: Vec<Document> = self.docs.clone().into_iter().map(DocumentFunction::into_document).collect();

        Ok(Box::new(
            InsertManyResultOutput(InsertManyResultWrapper::from(
                context.insert_many(&docs, self.options.to_owned().map(Into::into)).await.map_err(EpError::database)?,
            ))
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
    async fn test_insert_many_basic() {
        let mut ctx = MongoTestContext::new().await;

        let docs = vec![
            doc! { "name": "Alice", "age": 30 },
            doc! { "name": "Bob", "age": 25 },
            doc! { "name": "Charlie", "age": 35 },
        ];

        let result = ctx.insert_many("im_basic", docs).await;
        let inserted_ids = result.get("inserted_ids").expect("missing inserted_ids");
        let ids_obj = inserted_ids.as_object().expect("inserted_ids should be an object");
        assert_eq!(ids_obj.len(), 3, "should have 3 inserted_ids");
        assert!(ids_obj.contains_key("0"));
        assert!(ids_obj.contains_key("1"));
        assert!(ids_obj.contains_key("2"));

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_many_with_custom_ids() {
        let mut ctx = MongoTestContext::new().await;

        let docs = vec![
            doc! { "_id": "custom_1", "value": "first" },
            doc! { "_id": "custom_2", "value": "second" },
            doc! { "_id": "custom_3", "value": "third" },
        ];

        let result = ctx.insert_many("im_custom", docs).await;
        let inserted_ids = result.get("inserted_ids").expect("missing inserted_ids");
        let ids_obj = inserted_ids.as_object().expect("inserted_ids should be an object");
        assert_eq!(ids_obj.len(), 3);
        assert_eq!(ids_obj.get("0").and_then(|v| v.as_str()), Some("custom_1"));
        assert_eq!(ids_obj.get("1").and_then(|v| v.as_str()), Some("custom_2"));
        assert_eq!(ids_obj.get("2").and_then(|v| v.as_str()), Some("custom_3"));

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_many_empty_vec() {
        use crate::api::lib::database::collection::insert_many::InsertManyInput;
        use crate::api::wrapper::*;
        use crate::request::MongoRequest;
        use crate::test_utils::integration_test_utils::MongoTestContext;
        use crate::{EP, EpRequest};
        use ep_core::settings::EdenSettings;

        let mut ctx = MongoTestContext::new().await;

        // MongoDB rejects insert_many with an empty document list
        let wrapper_docs: Vec<DocumentWrapperType> = vec![];
        let input = InsertManyInput::new(ctx.db.clone(), "im_empty".to_string(), wrapper_docs, None);
        let request = Box::new(MongoRequest(Box::new(input))) as Box<dyn EpRequest>;
        let result = ctx.ep.write(&ctx.endpoint_cache_uuid, &*request, EdenSettings::default(), &mut ctx.telemetry).await;
        assert!(result.is_err(), "insert_many with empty vec should return an error");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_many_single_doc() {
        let mut ctx = MongoTestContext::new().await;

        let docs = vec![doc! { "title": "only_one", "score": 99 }];

        let result = ctx.insert_many("im_single", docs).await;
        let inserted_ids = result.get("inserted_ids").expect("missing inserted_ids");
        let ids_obj = inserted_ids.as_object().expect("inserted_ids should be an object");
        assert_eq!(ids_obj.len(), 1, "should have exactly 1 inserted_id");
        assert!(ids_obj.contains_key("0"));

        let count = ctx.count_documents("im_single", None).await;
        assert_eq!(count, 1);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_many_mixed_schemas() {
        let mut ctx = MongoTestContext::new().await;

        let docs = vec![
            doc! { "name": "Alice", "age": 30 },
            doc! { "title": "Engineer", "department": "R&D", "active": true },
            doc! { "x": 1, "y": 2, "z": 3 },
        ];

        let result = ctx.insert_many("im_mixed", docs).await;
        let inserted_ids = result.get("inserted_ids").expect("missing inserted_ids");
        let ids_obj = inserted_ids.as_object().expect("inserted_ids should be an object");
        assert_eq!(ids_obj.len(), 3, "should have 3 inserted_ids for mixed-schema docs");

        let count = ctx.count_documents("im_mixed", None).await;
        assert_eq!(count, 3);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_many_large_batch() {
        let mut ctx = MongoTestContext::new().await;

        let docs: Vec<mongodb::bson::Document> = (0..100).map(|i| doc! { "index": i, "label": format!("item_{}", i) }).collect();

        let result = ctx.insert_many("im_large", docs).await;
        let inserted_ids = result.get("inserted_ids").expect("missing inserted_ids");
        let ids_obj = inserted_ids.as_object().expect("inserted_ids should be an object");
        assert_eq!(ids_obj.len(), 100, "should have 100 inserted_ids");

        let count = ctx.count_documents("im_large", None).await;
        assert_eq!(count, 100);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_many_with_nested_docs() {
        let mut ctx = MongoTestContext::new().await;

        let docs = vec![
            doc! {
                "user": "Alice",
                "address": {
                    "street": "123 Main St",
                    "city": "Springfield",
                    "zip": "62701"
                },
                "tags": ["admin", "user"]
            },
            doc! {
                "user": "Bob",
                "address": {
                    "street": "456 Oak Ave",
                    "city": "Shelbyville",
                    "zip": "62702"
                },
                "tags": ["user"],
                "metadata": {
                    "created_by": "system",
                    "priority": 5
                }
            },
        ];

        let result = ctx.insert_many("im_nested", docs).await;
        let inserted_ids = result.get("inserted_ids").expect("missing inserted_ids");
        let ids_obj = inserted_ids.as_object().expect("inserted_ids should be an object");
        assert_eq!(ids_obj.len(), 2, "should have 2 inserted_ids for nested docs");

        let count = ctx.count_documents("im_nested", None).await;
        assert_eq!(count, 2);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_insert_many_verify_with_find() {
        let mut ctx = MongoTestContext::new().await;

        let docs = vec![
            doc! { "_id": "find_1", "color": "red", "count": 10 },
            doc! { "_id": "find_2", "color": "blue", "count": 20 },
            doc! { "_id": "find_3", "color": "green", "count": 30 },
        ];

        ctx.insert_many("im_find", docs).await;

        let found = ctx.find("im_find", None).await;
        let docs_array = found.as_array().expect("find should return an array");
        assert_eq!(docs_array.len(), 3, "should find all 3 inserted documents");

        let ids: Vec<&str> = docs_array.iter().filter_map(|d| d.get("_id").and_then(|v| v.as_str())).collect();
        assert!(ids.contains(&"find_1"));
        assert!(ids.contains(&"find_2"));
        assert!(ids.contains(&"find_3"));

        let colors: Vec<&str> = docs_array.iter().filter_map(|d| d.get("color").and_then(|v| v.as_str())).collect();
        assert!(colors.contains(&"red"));
        assert!(colors.contains(&"blue"));
        assert!(colors.contains(&"green"));

        ctx.stop().await;
    }
}
