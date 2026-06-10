use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{DistinctOptionsWrapper, DocumentFunction, DocumentWrapperType};
use crate::output::{CollectionDocumentOutput, VecBsonOutput};
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

const API_INFO: ApiInfo<MongoApi, DistinctInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::Distinct)))),
    "Finds the distinct values of the field specified by field_name across the collection",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DistinctInput {
        database: String,
        collection: String,
        field_name: String,
        filter: Option<DocumentWrapperType>,
        options: Option<DistinctOptionsWrapper>,
    }
}

type OutputWrapper = VecBsonOutput;
type ExpectedInput = CollectionDocumentOutput;

impl_simple_operation!(DistinctInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DistinctInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_distinct(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_distinct(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            VecBsonOutput(
                context
                    .distinct(
                        &self.field_name,
                        self.filter.clone().map(DocumentFunction::into_document),
                        self.options.clone().map(Into::into),
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
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_distinct_basic() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("dist_basic", doc! { "_id": "a", "color": "red" }).await;
        ctx.insert_one("dist_basic", doc! { "_id": "b", "color": "blue" }).await;
        ctx.insert_one("dist_basic", doc! { "_id": "c", "color": "red" }).await;
        ctx.insert_one("dist_basic", doc! { "_id": "d", "color": "green" }).await;
        ctx.insert_one("dist_basic", doc! { "_id": "e", "color": "blue" }).await;

        let result = ctx.distinct("dist_basic", "color", None).await;
        let arr = result.as_array().expect("distinct should return an array");
        assert_eq!(arr.len(), 3, "should return 3 unique color values");

        let mut values: Vec<String> = arr.iter().map(|v| v.as_str().expect("value should be a string").to_string()).collect();
        values.sort();
        assert_eq!(values, vec!["blue", "green", "red"]);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_distinct_with_filter() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("dist_filter", doc! { "_id": "a", "dept": "eng", "level": "senior" }).await;
        ctx.insert_one("dist_filter", doc! { "_id": "b", "dept": "eng", "level": "junior" }).await;
        ctx.insert_one("dist_filter", doc! { "_id": "c", "dept": "sales", "level": "senior" }).await;
        ctx.insert_one("dist_filter", doc! { "_id": "d", "dept": "eng", "level": "senior" }).await;

        let result = ctx.distinct("dist_filter", "level", Some(doc! { "dept": "eng" })).await;
        let arr = result.as_array().expect("distinct should return an array");
        assert_eq!(arr.len(), 2, "eng dept should have 2 distinct levels");

        let mut values: Vec<String> = arr.iter().map(|v| v.as_str().expect("value should be a string").to_string()).collect();
        values.sort();
        assert_eq!(values, vec!["junior", "senior"]);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_distinct_no_values() {
        let mut ctx = MongoTestContext::new().await;

        let result = ctx.distinct("dist_empty", "field", None).await;
        let arr = result.as_array().expect("distinct should return an array");
        assert_eq!(arr.len(), 0, "distinct on empty collection should return empty array");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_distinct_single_value() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("dist_single", doc! { "_id": "a", "type": "widget" }).await;
        ctx.insert_one("dist_single", doc! { "_id": "b", "type": "widget" }).await;
        ctx.insert_one("dist_single", doc! { "_id": "c", "type": "widget" }).await;

        let result = ctx.distinct("dist_single", "type", None).await;
        let arr = result.as_array().expect("distinct should return an array");
        assert_eq!(arr.len(), 1, "all same values should return array with 1 element");
        assert_eq!(arr[0], "widget");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_distinct_nested_field() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("dist_nested", doc! { "_id": "a", "address": { "city": "Portland", "state": "OR" } }).await;
        ctx.insert_one("dist_nested", doc! { "_id": "b", "address": { "city": "Seattle", "state": "WA" } }).await;
        ctx.insert_one("dist_nested", doc! { "_id": "c", "address": { "city": "Portland", "state": "ME" } }).await;
        ctx.insert_one("dist_nested", doc! { "_id": "d", "address": { "city": "Seattle", "state": "WA" } }).await;

        let result = ctx.distinct("dist_nested", "address.city", None).await;
        let arr = result.as_array().expect("distinct should return an array");
        assert_eq!(arr.len(), 2, "should return 2 unique cities via dot notation");

        let mut values: Vec<String> = arr.iter().map(|v| v.as_str().expect("value should be a string").to_string()).collect();
        values.sort();
        assert_eq!(values, vec!["Portland", "Seattle"]);

        ctx.stop().await;
    }

    /// Find unique tags across blog posts where tags are stored as arrays.
    /// MongoDB's distinct operation flattens array fields, returning individual
    /// elements rather than whole arrays.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_distinct_on_array_field() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one(
            "tag_posts",
            doc! {
                "_id": "p1", "title": "Intro to Rust",
                "tags": ["rust", "programming", "beginner"]
            },
        )
        .await;
        ctx.insert_one(
            "tag_posts",
            doc! {
                "_id": "p2", "title": "Advanced Rust Patterns",
                "tags": ["rust", "programming", "advanced"]
            },
        )
        .await;
        ctx.insert_one(
            "tag_posts",
            doc! {
                "_id": "p3", "title": "MongoDB Tips",
                "tags": ["mongodb", "database", "beginner"]
            },
        )
        .await;
        ctx.insert_one(
            "tag_posts",
            doc! {
                "_id": "p4", "title": "Full Stack Rust",
                "tags": ["rust", "web", "fullstack"]
            },
        )
        .await;

        let result = ctx.distinct("tag_posts", "tags", None).await;
        let arr = result.as_array().expect("distinct should return an array");

        // Expected unique tags: rust, programming, beginner, advanced, mongodb, database, web, fullstack = 8
        let mut values: Vec<String> = arr.iter().map(|v| v.as_str().expect("tag value should be a string").to_string()).collect();
        values.sort();

        assert_eq!(values.len(), 8, "should return 8 unique tag values across all posts");
        assert_eq!(
            values,
            vec![
                "advanced",
                "beginner",
                "database",
                "fullstack",
                "mongodb",
                "programming",
                "rust",
                "web"
            ]
        );

        ctx.stop().await;
    }

    /// Get distinct departments for active employees only by applying a status
    /// filter. Verify that departments which only have inactive employees are
    /// excluded from the results.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_distinct_with_complex_filter() {
        let mut ctx = MongoTestContext::new().await;

        // Seed 8 employees across various departments with active/inactive statuses
        ctx.insert_one("dist_emp", doc! { "_id": "e1", "name": "Alice",   "department": "engineering", "status": "active" })
            .await;
        ctx.insert_one("dist_emp", doc! { "_id": "e2", "name": "Bob",     "department": "engineering", "status": "active" })
            .await;
        ctx.insert_one(
            "dist_emp",
            doc! { "_id": "e3", "name": "Charlie", "department": "marketing",   "status": "inactive" },
        )
        .await;
        ctx.insert_one("dist_emp", doc! { "_id": "e4", "name": "Diana",   "department": "sales",       "status": "active" })
            .await;
        ctx.insert_one(
            "dist_emp",
            doc! { "_id": "e5", "name": "Eve",     "department": "marketing",   "status": "inactive" },
        )
        .await;
        ctx.insert_one("dist_emp", doc! { "_id": "e6", "name": "Frank",   "department": "hr",          "status": "active" })
            .await;
        ctx.insert_one(
            "dist_emp",
            doc! { "_id": "e7", "name": "Grace",   "department": "hr",          "status": "inactive" },
        )
        .await;
        ctx.insert_one("dist_emp", doc! { "_id": "e8", "name": "Hank",    "department": "sales",       "status": "active" })
            .await;

        // Get distinct departments for ACTIVE employees only
        let result = ctx.distinct("dist_emp", "department", Some(doc! { "status": "active" })).await;
        let arr = result.as_array().expect("distinct should return an array");

        let mut departments: Vec<String> = arr.iter().map(|v| v.as_str().expect("department should be a string").to_string()).collect();
        departments.sort();

        // Active employees exist in: engineering, sales, hr. Marketing has only inactive employees.
        assert_eq!(departments.len(), 3, "should return 3 departments with active employees");
        assert_eq!(departments, vec!["engineering", "hr", "sales"]);
        assert!(
            !departments.contains(&"marketing".to_string()),
            "marketing should not appear since it only has inactive employees"
        );

        ctx.stop().await;
    }

    /// Analytics scenario: seed event documents with event_type, browser, and os fields.
    /// Use distinct to get unique browsers and event types, including filtered queries.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_distinct_counts_real_world() {
        let mut ctx = MongoTestContext::new().await;

        // Seed 10 analytics event documents
        ctx.insert_one(
            "dist_events",
            doc! { "_id": "ev1",  "event_type": "page_view",    "browser": "Chrome",  "os": "Windows" },
        )
        .await;
        ctx.insert_one(
            "dist_events",
            doc! { "_id": "ev2",  "event_type": "click",        "browser": "Firefox", "os": "macOS" },
        )
        .await;
        ctx.insert_one(
            "dist_events",
            doc! { "_id": "ev3",  "event_type": "page_view",    "browser": "Safari",  "os": "macOS" },
        )
        .await;
        ctx.insert_one(
            "dist_events",
            doc! { "_id": "ev4",  "event_type": "purchase",     "browser": "Chrome",  "os": "Windows" },
        )
        .await;
        ctx.insert_one(
            "dist_events",
            doc! { "_id": "ev5",  "event_type": "click",        "browser": "Chrome",  "os": "Linux" },
        )
        .await;
        ctx.insert_one(
            "dist_events",
            doc! { "_id": "ev6",  "event_type": "page_view",    "browser": "Edge",    "os": "Windows" },
        )
        .await;
        ctx.insert_one(
            "dist_events",
            doc! { "_id": "ev7",  "event_type": "signup",       "browser": "Firefox", "os": "Linux" },
        )
        .await;
        ctx.insert_one(
            "dist_events",
            doc! { "_id": "ev8",  "event_type": "click",        "browser": "Safari",  "os": "macOS" },
        )
        .await;
        ctx.insert_one(
            "dist_events",
            doc! { "_id": "ev9",  "event_type": "page_view",    "browser": "Chrome",  "os": "macOS" },
        )
        .await;
        ctx.insert_one(
            "dist_events",
            doc! { "_id": "ev10", "event_type": "purchase",     "browser": "Firefox", "os": "Windows" },
        )
        .await;

        // Get distinct browsers across all events
        let browsers = ctx.distinct("dist_events", "browser", None).await;
        let browsers_arr = browsers.as_array().expect("distinct should return an array");
        assert_eq!(browsers_arr.len(), 4, "should have 4 unique browsers: Chrome, Firefox, Safari, Edge");

        // Get distinct event types filtered to macOS only
        let macos_events = ctx.distinct("dist_events", "event_type", Some(doc! { "os": "macOS" })).await;
        let macos_arr = macos_events.as_array().expect("distinct should return an array");

        let mut macos_types: Vec<String> =
            macos_arr.iter().map(|v| v.as_str().expect("event_type should be a string").to_string()).collect();
        macos_types.sort();

        // macOS events: click (ev2, ev8), page_view (ev3, ev9) = 2 distinct types
        assert_eq!(macos_types.len(), 2, "macOS should have 2 distinct event types");
        assert_eq!(macos_types, vec!["click", "page_view"]);

        // Get distinct OS values for purchase events
        let purchase_os = ctx.distinct("dist_events", "os", Some(doc! { "event_type": "purchase" })).await;
        let purchase_os_arr = purchase_os.as_array().expect("distinct should return an array");
        // purchase events: ev4 (Windows), ev10 (Windows) = 1 distinct OS
        assert_eq!(purchase_os_arr.len(), 1, "purchase events should come from 1 distinct OS");
        assert_eq!(purchase_os_arr[0].as_str().expect("os should be a string"), "Windows");

        ctx.stop().await;
    }
}
