use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{AggregateOptionsWrapper, DocumentFunction, DocumentWrapperType};
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
use mongodb::bson::{Document, doc};
use std::borrow::Cow;
use telemetry::FastSpanStatus;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, CollectionAggregateInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::CollectionAggregate)))),
    "Runs an aggregation operation on a collection",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct CollectionAggregateInput {
        database: String,
        collection: String,
        pipeline: Vec<DocumentWrapperType>,
        options: Option<AggregateOptionsWrapper>,
    }
}

impl_simple_operation!(CollectionAggregateInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl CollectionAggregateInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::connect(e)
        })?;

        self.run_aggregate(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _session: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        // let collection: Collection<Document> = session
        //     .client()
        //     .database(&self.database)
        //     .collection(&self.collection);
        //
        // Box::pin(async move {
        //     let mut cursor = collection
        //         .aggregate_with_session(
        //             self.pipeline
        //                 .clone()
        //                 .into_iter()
        //                 .map(DocumentFunction::into_document)
        //                 .collect::<Vec<Document>>(),
        //             self.options.clone().map(Into::into),
        //             session,
        //         )
        //         .await
        //         .map_err(EpError::database)?;
        //
        //     let mut results = vec![];
        //
        //     while let Some(doc) = cursor.next(session).await {
        //         results.push(doc.map_err(EpError::transaction)?)
        //     }
        //
        //     Ok(Box::new(VecDocumentOutput(results).to_output()) as Box<dyn EpOutput>)
        // })

        todo!("implement pipeline")
    }
    async fn run_aggregate(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        let mut cursor = context
            .aggregate(
                self.pipeline.clone().into_iter().map(DocumentFunction::into_document).collect::<Vec<Document>>(),
                self.options.clone().map(Into::into),
            )
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
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_match() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("agg_match", doc! { "_id": "a", "status": "active", "name": "Alice" }).await;
        ctx.insert_one("agg_match", doc! { "_id": "b", "status": "inactive", "name": "Bob" }).await;
        ctx.insert_one("agg_match", doc! { "_id": "c", "status": "active", "name": "Charlie" }).await;

        let result = ctx.aggregate("agg_match", vec![doc! { "$match": { "status": "active" } }]).await;
        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 2, "$match should return only active documents");
        for doc in arr {
            assert_eq!(doc["status"], "active");
        }

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_group_sum() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("agg_group_sum", doc! { "_id": "a", "category": "A", "amount": 10 }).await;
        ctx.insert_one("agg_group_sum", doc! { "_id": "b", "category": "B", "amount": 20 }).await;
        ctx.insert_one("agg_group_sum", doc! { "_id": "c", "category": "A", "amount": 30 }).await;
        ctx.insert_one("agg_group_sum", doc! { "_id": "d", "category": "B", "amount": 40 }).await;

        let result = ctx
            .aggregate(
                "agg_group_sum",
                vec![
                    doc! { "$group": { "_id": "$category", "total": { "$sum": "$amount" } } },
                    doc! { "$sort": { "_id": 1 } },
                ],
            )
            .await;
        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 2, "$group should produce 2 groups");
        assert_eq!(arr[0]["_id"], "A");
        assert_eq!(arr[0]["total"], 40);
        assert_eq!(arr[1]["_id"], "B");
        assert_eq!(arr[1]["total"], 60);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_group_avg() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("agg_group_avg", doc! { "_id": "a", "dept": "eng", "salary": 100 }).await;
        ctx.insert_one("agg_group_avg", doc! { "_id": "b", "dept": "eng", "salary": 200 }).await;
        ctx.insert_one("agg_group_avg", doc! { "_id": "c", "dept": "sales", "salary": 150 }).await;

        let result = ctx
            .aggregate(
                "agg_group_avg",
                vec![
                    doc! { "$group": { "_id": "$dept", "avg_salary": { "$avg": "$salary" } } },
                    doc! { "$sort": { "_id": 1 } },
                ],
            )
            .await;
        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 2, "$group should produce 2 departments");
        assert_eq!(arr[0]["_id"], "eng");
        assert_eq!(arr[0]["avg_salary"], 150.0);
        assert_eq!(arr[1]["_id"], "sales");
        assert_eq!(arr[1]["avg_salary"], 150.0);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_sort() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("agg_sort", doc! { "_id": "a", "name": "Charlie", "rank": 3 }).await;
        ctx.insert_one("agg_sort", doc! { "_id": "b", "name": "Alice", "rank": 1 }).await;
        ctx.insert_one("agg_sort", doc! { "_id": "c", "name": "Bob", "rank": 2 }).await;

        let result = ctx.aggregate("agg_sort", vec![doc! { "$sort": { "rank": 1 } }]).await;
        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0]["name"], "Alice");
        assert_eq!(arr[1]["name"], "Bob");
        assert_eq!(arr[2]["name"], "Charlie");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_limit() {
        let mut ctx = MongoTestContext::new().await;

        for i in 0..10 {
            ctx.insert_one("agg_limit", doc! { "_id": format!("doc{}", i), "index": i }).await;
        }

        let result = ctx.aggregate("agg_limit", vec![doc! { "$sort": { "index": 1 } }, doc! { "$limit": 3 }]).await;
        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 3, "$limit should restrict results to 3 documents");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_skip() {
        let mut ctx = MongoTestContext::new().await;

        for i in 0..5 {
            ctx.insert_one("agg_skip", doc! { "_id": format!("doc{}", i), "rank": i }).await;
        }

        let result = ctx.aggregate("agg_skip", vec![doc! { "$sort": { "rank": 1 } }, doc! { "$skip": 3 }]).await;
        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 2, "$skip 3 of 5 should return 2 documents");
        assert_eq!(arr[0]["rank"], 3);
        assert_eq!(arr[1]["rank"], 4);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_project() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("agg_project", doc! { "_id": "a", "name": "Alice", "age": 30, "email": "alice@test.com" }).await;
        ctx.insert_one("agg_project", doc! { "_id": "b", "name": "Bob", "age": 25, "email": "bob@test.com" }).await;

        let result = ctx.aggregate("agg_project", vec![doc! { "$project": { "name": 1, "_id": 0 } }]).await;
        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 2);
        for doc in arr {
            assert!(doc.get("name").is_some(), "projected field 'name' should be present");
            assert!(doc.get("_id").is_none(), "_id should be excluded by $project");
            assert!(doc.get("age").is_none(), "non-projected field 'age' should be excluded");
            assert!(doc.get("email").is_none(), "non-projected field 'email' should be excluded");
        }

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_count() {
        let mut ctx = MongoTestContext::new().await;

        for i in 0..7 {
            ctx.insert_one("agg_count", doc! { "_id": format!("doc{}", i), "value": i }).await;
        }

        let result = ctx.aggregate("agg_count", vec![doc! { "$count": "total" }]).await;
        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 1, "$count should return a single document");
        assert_eq!(arr[0]["total"], 7);

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_match_and_group() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("agg_match_group", doc! { "_id": "a", "status": "active", "category": "X", "amount": 10 }).await;
        ctx.insert_one("agg_match_group", doc! { "_id": "b", "status": "active", "category": "Y", "amount": 20 }).await;
        ctx.insert_one("agg_match_group", doc! { "_id": "c", "status": "inactive", "category": "X", "amount": 50 }).await;
        ctx.insert_one("agg_match_group", doc! { "_id": "d", "status": "active", "category": "X", "amount": 30 }).await;

        let result = ctx
            .aggregate(
                "agg_match_group",
                vec![
                    doc! { "$match": { "status": "active" } },
                    doc! { "$group": { "_id": "$category", "total": { "$sum": "$amount" } } },
                    doc! { "$sort": { "_id": 1 } },
                ],
            )
            .await;
        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 2, "should have 2 groups after filtering active");
        assert_eq!(arr[0]["_id"], "X");
        assert_eq!(arr[0]["total"], 40, "category X active total should be 10+30=40");
        assert_eq!(arr[1]["_id"], "Y");
        assert_eq!(arr[1]["total"], 20, "category Y active total should be 20");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_unwind() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("agg_unwind", doc! { "_id": "a", "name": "Alice", "tags": ["rust", "python"] }).await;
        ctx.insert_one("agg_unwind", doc! { "_id": "b", "name": "Bob", "tags": ["go"] }).await;

        let result = ctx.aggregate("agg_unwind", vec![doc! { "$unwind": "$tags" }, doc! { "$sort": { "tags": 1 } }]).await;
        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 3, "$unwind should produce one doc per array element");
        assert_eq!(arr[0]["tags"], "go");
        assert_eq!(arr[1]["tags"], "python");
        assert_eq!(arr[2]["tags"], "rust");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_add_fields() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("agg_add_fields", doc! { "_id": "a", "price": 100, "quantity": 3 }).await;
        ctx.insert_one("agg_add_fields", doc! { "_id": "b", "price": 50, "quantity": 5 }).await;

        let result = ctx
            .aggregate(
                "agg_add_fields",
                vec![
                    doc! { "$addFields": { "total_cost": { "$multiply": ["$price", "$quantity"] } } },
                    doc! { "$sort": { "_id": 1 } },
                ],
            )
            .await;
        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["total_cost"], 300, "100 * 3 should be 300");
        assert_eq!(arr[1]["total_cost"], 250, "50 * 5 should be 250");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_empty_pipeline() {
        let mut ctx = MongoTestContext::new().await;

        ctx.insert_one("agg_empty", doc! { "_id": "a", "value": 1 }).await;
        ctx.insert_one("agg_empty", doc! { "_id": "b", "value": 2 }).await;
        ctx.insert_one("agg_empty", doc! { "_id": "c", "value": 3 }).await;

        let result = ctx.aggregate("agg_empty", vec![]).await;
        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 3, "empty pipeline should return all documents");

        ctx.stop().await;
    }

    /// E-commerce analytics: match completed orders, group by category computing revenue
    /// via $multiply, count orders per category, and sort by total revenue descending.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_ecommerce_revenue_by_category() {
        let mut ctx = MongoTestContext::new().await;

        let orders = vec![
            doc! { "_id": "ord1",  "category": "Electronics", "product": "Laptop",       "quantity": 2, "unit_price": 999,  "status": "completed" },
            doc! { "_id": "ord2",  "category": "Electronics", "product": "Phone",         "quantity": 5, "unit_price": 699,  "status": "completed" },
            doc! { "_id": "ord3",  "category": "Electronics", "product": "Tablet",        "quantity": 1, "unit_price": 499,  "status": "pending" },
            doc! { "_id": "ord4",  "category": "Clothing",    "product": "Jacket",        "quantity": 3, "unit_price": 120,  "status": "completed" },
            doc! { "_id": "ord5",  "category": "Clothing",    "product": "Sneakers",      "quantity": 2, "unit_price": 85,   "status": "completed" },
            doc! { "_id": "ord6",  "category": "Clothing",    "product": "T-Shirt",       "quantity": 10,"unit_price": 25,   "status": "cancelled" },
            doc! { "_id": "ord7",  "category": "Books",       "product": "Rust in Action", "quantity": 4, "unit_price": 45,  "status": "completed" },
            doc! { "_id": "ord8",  "category": "Books",       "product": "SICP",          "quantity": 1, "unit_price": 60,   "status": "completed" },
            doc! { "_id": "ord9",  "category": "Home",        "product": "Blender",       "quantity": 1, "unit_price": 150,  "status": "completed" },
            doc! { "_id": "ord10", "category": "Home",        "product": "Toaster",       "quantity": 2, "unit_price": 40,   "status": "pending" },
        ];
        ctx.insert_many("agg_ecommerce", orders).await;

        // Pipeline: filter completed -> group by category -> sort by revenue desc
        let result = ctx
            .aggregate(
                "agg_ecommerce",
                vec![
                    doc! { "$match": { "status": "completed" } },
                    doc! { "$group": {
                        "_id": "$category",
                        "total_revenue": { "$sum": { "$multiply": ["$quantity", "$unit_price"] } },
                        "order_count": { "$sum": 1 }
                    }},
                    doc! { "$sort": { "total_revenue": -1 } },
                ],
            )
            .await;

        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 4, "should have 4 categories with completed orders");

        // Electronics: Laptop 2*999=1998, Phone 5*699=3495 => 5493 (2 orders)
        assert_eq!(arr[0]["_id"], "Electronics");
        assert_eq!(arr[0]["total_revenue"], 5493);
        assert_eq!(arr[0]["order_count"], 2);

        // Clothing: Jacket 3*120=360, Sneakers 2*85=170 => 530 (2 orders)
        assert_eq!(arr[1]["_id"], "Clothing");
        assert_eq!(arr[1]["total_revenue"], 530);
        assert_eq!(arr[1]["order_count"], 2);

        // Books: Rust in Action 4*45=180, SICP 1*60=60 => 240 (2 orders)
        assert_eq!(arr[2]["_id"], "Books");
        assert_eq!(arr[2]["total_revenue"], 240);
        assert_eq!(arr[2]["order_count"], 2);

        // Home: Blender 1*150=150 (1 order)
        assert_eq!(arr[3]["_id"], "Home");
        assert_eq!(arr[3]["total_revenue"], 150);
        assert_eq!(arr[3]["order_count"], 1);

        ctx.stop().await;
    }

    /// Join between collections using $lookup: orders reference customers by customer_id,
    /// then $unwind and $project to denormalize customer names onto order documents.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_lookup_join() {
        let mut ctx = MongoTestContext::new().await;

        // Seed customers
        let customers = vec![
            doc! { "_id": "cust1", "name": "Alice Johnson", "email": "alice@example.com" },
            doc! { "_id": "cust2", "name": "Bob Smith",     "email": "bob@example.com" },
            doc! { "_id": "cust3", "name": "Carol White",   "email": "carol@example.com" },
        ];
        ctx.insert_many("agg_customers", customers).await;

        // Seed orders referencing customers
        let orders = vec![
            doc! { "_id": "o1", "customer_id": "cust1", "product": "Widget",    "amount": 29.99 },
            doc! { "_id": "o2", "customer_id": "cust2", "product": "Gadget",    "amount": 49.95 },
            doc! { "_id": "o3", "customer_id": "cust1", "product": "Sprocket",  "amount": 15.00 },
            doc! { "_id": "o4", "customer_id": "cust3", "product": "Doohickey", "amount": 99.50 },
        ];
        ctx.insert_many("agg_orders", orders).await;

        // Pipeline: lookup customers, unwind, project
        let result = ctx
            .aggregate(
                "agg_orders",
                vec![
                    doc! { "$lookup": {
                        "from": "agg_customers",
                        "localField": "customer_id",
                        "foreignField": "_id",
                        "as": "customer_info"
                    }},
                    doc! { "$unwind": "$customer_info" },
                    doc! { "$project": {
                        "product": 1,
                        "amount": 1,
                        "customer_name": "$customer_info.name"
                    }},
                    doc! { "$sort": { "_id": 1 } },
                ],
            )
            .await;

        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 4, "all 4 orders should have joined customer data");

        assert_eq!(arr[0]["product"], "Widget");
        assert_eq!(arr[0]["customer_name"], "Alice Johnson");

        assert_eq!(arr[1]["product"], "Gadget");
        assert_eq!(arr[1]["customer_name"], "Bob Smith");

        assert_eq!(arr[2]["product"], "Sprocket");
        assert_eq!(arr[2]["customer_name"], "Alice Johnson");

        assert_eq!(arr[3]["product"], "Doohickey");
        assert_eq!(arr[3]["customer_name"], "Carol White");

        ctx.stop().await;
    }

    /// Multi-faceted search: $facet with price_ranges ($bucket), by_category ($group),
    /// and top_rated ($sort + $limit) to return multiple analytical dimensions at once.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_facet_multi_dimension() {
        let mut ctx = MongoTestContext::new().await;

        let products = vec![
            doc! { "_id": "p1",  "name": "USB Cable",       "category": "Electronics", "price": 8,    "rating": 4.2 },
            doc! { "_id": "p2",  "name": "Mouse",           "category": "Electronics", "price": 25,   "rating": 4.5 },
            doc! { "_id": "p3",  "name": "Keyboard",        "category": "Electronics", "price": 75,   "rating": 4.8 },
            doc! { "_id": "p4",  "name": "Monitor",         "category": "Electronics", "price": 350,  "rating": 4.6 },
            doc! { "_id": "p5",  "name": "Notebook",        "category": "Office",      "price": 5,    "rating": 3.9 },
            doc! { "_id": "p6",  "name": "Pen Set",         "category": "Office",      "price": 12,   "rating": 4.1 },
            doc! { "_id": "p7",  "name": "Desk Lamp",       "category": "Office",      "price": 45,   "rating": 4.3 },
            doc! { "_id": "p8",  "name": "Ergonomic Chair", "category": "Furniture",   "price": 450,  "rating": 4.9 },
            doc! { "_id": "p9",  "name": "Standing Desk",   "category": "Furniture",   "price": 300,  "rating": 4.7 },
            doc! { "_id": "p10", "name": "Bookshelf",       "category": "Furniture",   "price": 120,  "rating": 4.0 },
            doc! { "_id": "p11", "name": "Coffee Mug",      "category": "Kitchen",     "price": 10,   "rating": 3.8 },
            doc! { "_id": "p12", "name": "Water Bottle",    "category": "Kitchen",     "price": 18,   "rating": 4.4 },
            doc! { "_id": "p13", "name": "Lunchbox",        "category": "Kitchen",     "price": 22,   "rating": 3.5 },
            doc! { "_id": "p14", "name": "Headphones",      "category": "Electronics", "price": 90,   "rating": 4.7 },
            doc! { "_id": "p15", "name": "Webcam",          "category": "Electronics", "price": 60,   "rating": 4.1 },
        ];
        ctx.insert_many("agg_facet_products", products).await;

        let result = ctx
            .aggregate(
                "agg_facet_products",
                vec![doc! { "$facet": {
                    "price_ranges": [
                        { "$bucket": {
                            "groupBy": "$price",
                            "boundaries": [0, 25, 50, 100, 500],
                            "default": "Other",
                            "output": { "count": { "$sum": 1 }, "products": { "$push": "$name" } }
                        }}
                    ],
                    "by_category": [
                        { "$group": { "_id": "$category", "count": { "$sum": 1 } } },
                        { "$sort": { "_id": 1 } }
                    ],
                    "top_rated": [
                        { "$sort": { "rating": -1 } },
                        { "$limit": 3 },
                        { "$project": { "name": 1, "rating": 1, "_id": 0 } }
                    ]
                }}],
            )
            .await;

        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 1, "$facet returns a single document");

        let facet_doc = &arr[0];

        // Verify price_ranges facet
        let price_ranges = facet_doc["price_ranges"].as_array().expect("price_ranges should be an array");
        assert_eq!(price_ranges.len(), 4, "should have 4 price buckets: [0-25), [25-50), [50-100), [100-500)");

        // Bucket [0,25): USB Cable(8), Notebook(5), Pen Set(12), Coffee Mug(10), Water Bottle(18), Lunchbox(22) = 6 items
        assert_eq!(price_ranges[0]["_id"], 0);
        assert_eq!(price_ranges[0]["count"], 6);

        // Bucket [25,50): Mouse(25), Desk Lamp(45) = 2 items
        assert_eq!(price_ranges[1]["_id"], 25);
        assert_eq!(price_ranges[1]["count"], 2);

        // Bucket [50,100): Keyboard(75), Headphones(90), Webcam(60) = 3 items
        assert_eq!(price_ranges[2]["_id"], 50);
        assert_eq!(price_ranges[2]["count"], 3);

        // Bucket [100,500): Monitor(350), Ergonomic Chair(450), Standing Desk(300), Bookshelf(120) = 4 items
        assert_eq!(price_ranges[3]["_id"], 100);
        assert_eq!(price_ranges[3]["count"], 4);

        // Verify by_category facet (sorted alphabetically)
        let by_category = facet_doc["by_category"].as_array().expect("by_category should be an array");
        assert_eq!(by_category.len(), 4, "should have 4 categories");
        assert_eq!(by_category[0]["_id"], "Electronics");
        assert_eq!(by_category[0]["count"], 6);
        assert_eq!(by_category[1]["_id"], "Furniture");
        assert_eq!(by_category[1]["count"], 3);
        assert_eq!(by_category[2]["_id"], "Kitchen");
        assert_eq!(by_category[2]["count"], 3);
        assert_eq!(by_category[3]["_id"], "Office");
        assert_eq!(by_category[3]["count"], 3);

        // Verify top_rated facet: Ergonomic Chair(4.9), Keyboard(4.8), Monitor(4.6)/Standing Desk(4.7)/Headphones(4.7)
        let top_rated = facet_doc["top_rated"].as_array().expect("top_rated should be an array");
        assert_eq!(top_rated.len(), 3, "top_rated should return exactly 3 products");
        assert_eq!(top_rated[0]["name"], "Ergonomic Chair");
        assert_eq!(top_rated[1]["name"], "Keyboard");
        // Third could be Standing Desk or Headphones (both 4.7) - just check rating
        let third_rating = top_rated[2]["rating"].as_f64().expect("rating should be a number");
        assert!(third_rating >= 4.6, "third top-rated product should have rating >= 4.6");

        ctx.stop().await;
    }

    /// Time-series analytics: seed events with DateTime timestamps, match by event_type,
    /// group by user computing total_value, event_count, first_event, last_event, then sort.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_date_based_analytics() {
        use mongodb::bson::DateTime;

        let mut ctx = MongoTestContext::new().await;

        // Create timestamps: millis since epoch for specific dates
        // 2024-01-15 = 1705276800000, 2024-02-10 = 1707523200000, etc.
        let ts_jan_15 = DateTime::from_millis(1705276800000);
        let ts_feb_10 = DateTime::from_millis(1707523200000);
        let ts_mar_05 = DateTime::from_millis(1709596800000);
        let ts_apr_20 = DateTime::from_millis(1713571200000);
        let ts_may_12 = DateTime::from_millis(1715472000000);
        let ts_jun_01 = DateTime::from_millis(1717200000000);
        let ts_jul_18 = DateTime::from_millis(1721260800000);
        let ts_aug_25 = DateTime::from_millis(1724544000000);

        let events = vec![
            doc! { "_id": "e1", "event_type": "purchase", "timestamp": ts_jan_15, "user_id": "user_a", "value": 120 },
            doc! { "_id": "e2", "event_type": "purchase", "timestamp": ts_feb_10, "user_id": "user_b", "value": 85 },
            doc! { "_id": "e3", "event_type": "purchase", "timestamp": ts_mar_05, "user_id": "user_a", "value": 200 },
            doc! { "_id": "e4", "event_type": "refund",   "timestamp": ts_apr_20, "user_id": "user_a", "value": 50 },
            doc! { "_id": "e5", "event_type": "purchase", "timestamp": ts_may_12, "user_id": "user_c", "value": 310 },
            doc! { "_id": "e6", "event_type": "purchase", "timestamp": ts_jun_01, "user_id": "user_b", "value": 175 },
            doc! { "_id": "e7", "event_type": "purchase", "timestamp": ts_jul_18, "user_id": "user_a", "value": 90 },
            doc! { "_id": "e8", "event_type": "purchase", "timestamp": ts_aug_25, "user_id": "user_c", "value": 60 },
        ];
        ctx.insert_many("agg_events", events).await;

        // Pipeline: match purchases, group by user
        let result = ctx
            .aggregate(
                "agg_events",
                vec![
                    doc! { "$match": { "event_type": "purchase" } },
                    doc! { "$group": {
                        "_id": "$user_id",
                        "total_value": { "$sum": "$value" },
                        "event_count": { "$sum": 1 },
                        "first_event": { "$min": "$timestamp" },
                        "last_event": { "$max": "$timestamp" }
                    }},
                    doc! { "$sort": { "total_value": -1 } },
                ],
            )
            .await;

        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 3, "should have 3 users with purchase events");

        // user_a: 120+200+90 = 410, 3 purchases, first=jan15, last=jul18
        assert_eq!(arr[0]["_id"], "user_a");
        assert_eq!(arr[0]["total_value"], 410);
        assert_eq!(arr[0]["event_count"], 3);

        // user_c: 310+60 = 370, 2 purchases
        assert_eq!(arr[1]["_id"], "user_c");
        assert_eq!(arr[1]["total_value"], 370);
        assert_eq!(arr[1]["event_count"], 2);

        // user_b: 85+175 = 260, 2 purchases
        assert_eq!(arr[2]["_id"], "user_b");
        assert_eq!(arr[2]["total_value"], 260);
        assert_eq!(arr[2]["event_count"], 2);

        ctx.stop().await;
    }

    /// Flatten nested arrays and re-aggregate: orders with items arrays are unwound, then
    /// grouped by product to compute per-product total quantity and revenue.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_nested_array_unwind_regroup() {
        let mut ctx = MongoTestContext::new().await;

        let orders = vec![
            doc! { "_id": "order1", "customer": "Alice", "items": [
                { "product": "Widget",  "quantity": 3, "price": 10 },
                { "product": "Gadget",  "quantity": 1, "price": 50 },
            ]},
            doc! { "_id": "order2", "customer": "Bob", "items": [
                { "product": "Widget",  "quantity": 5, "price": 10 },
                { "product": "Gizmo",   "quantity": 2, "price": 30 },
            ]},
            doc! { "_id": "order3", "customer": "Carol", "items": [
                { "product": "Gadget",  "quantity": 2, "price": 50 },
                { "product": "Gizmo",   "quantity": 1, "price": 30 },
                { "product": "Widget",  "quantity": 4, "price": 10 },
            ]},
            doc! { "_id": "order4", "customer": "Dave", "items": [
                { "product": "Gadget",  "quantity": 1, "price": 50 },
            ]},
        ];
        ctx.insert_many("agg_nested_orders", orders).await;

        let result = ctx
            .aggregate(
                "agg_nested_orders",
                vec![
                    doc! { "$unwind": "$items" },
                    doc! { "$group": {
                        "_id": "$items.product",
                        "total_quantity": { "$sum": "$items.quantity" },
                        "total_revenue": { "$sum": { "$multiply": ["$items.quantity", "$items.price"] } }
                    }},
                    doc! { "$sort": { "total_revenue": -1 } },
                ],
            )
            .await;

        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 3, "should have 3 distinct products");

        // Gadget: 1*50 + 2*50 + 1*50 = 200, qty=4
        assert_eq!(arr[0]["_id"], "Gadget");
        assert_eq!(arr[0]["total_quantity"], 4);
        assert_eq!(arr[0]["total_revenue"], 200);

        // Widget: 3*10 + 5*10 + 4*10 = 120, qty=12
        assert_eq!(arr[1]["_id"], "Widget");
        assert_eq!(arr[1]["total_quantity"], 12);
        assert_eq!(arr[1]["total_revenue"], 120);

        // Gizmo: 2*30 + 1*30 = 90, qty=3
        assert_eq!(arr[2]["_id"], "Gizmo");
        assert_eq!(arr[2]["total_quantity"], 3);
        assert_eq!(arr[2]["total_revenue"], 90);

        ctx.stop().await;
    }

    /// Computed fields with conditions: $addFields computes bonus based on tenure,
    /// then $project adds total_compensation and results are sorted.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_conditional_fields() {
        let mut ctx = MongoTestContext::new().await;

        let employees = vec![
            doc! { "_id": "emp1", "name": "Alice",   "salary": 80000,  "department": "Engineering", "years": 8 },
            doc! { "_id": "emp2", "name": "Bob",     "salary": 95000,  "department": "Engineering", "years": 3 },
            doc! { "_id": "emp3", "name": "Carol",   "salary": 72000,  "department": "Marketing",   "years": 12 },
            doc! { "_id": "emp4", "name": "Dave",    "salary": 110000, "department": "Engineering", "years": 5 },
            doc! { "_id": "emp5", "name": "Eve",     "salary": 65000,  "department": "Marketing",   "years": 1 },
            doc! { "_id": "emp6", "name": "Frank",   "salary": 88000,  "department": "Sales",       "years": 6 },
        ];
        ctx.insert_many("agg_employees", employees).await;

        // Pipeline: compute bonus (10% if years>=5, else 5%), then project total_compensation
        let result = ctx
            .aggregate(
                "agg_employees",
                vec![
                    doc! { "$addFields": {
                        "bonus": {
                            "$cond": [
                                { "$gte": ["$years", 5] },
                                { "$multiply": ["$salary", 0.1] },
                                { "$multiply": ["$salary", 0.05] }
                            ]
                        }
                    }},
                    doc! { "$project": {
                        "name": 1,
                        "salary": 1,
                        "bonus": 1,
                        "total_compensation": { "$add": ["$salary", "$bonus"] }
                    }},
                    doc! { "$sort": { "total_compensation": -1 } },
                ],
            )
            .await;

        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 6, "should return all 6 employees");

        // Dave: 110000 + 11000 (10%) = 121000
        assert_eq!(arr[0]["name"], "Dave");
        assert_eq!(arr[0]["bonus"].as_f64().expect("bonus"), 11000.0);
        assert_eq!(arr[0]["total_compensation"].as_f64().expect("total_comp"), 121000.0);

        // Bob: 95000 + 4750 (5%) = 99750
        assert_eq!(arr[1]["name"], "Bob");
        assert_eq!(arr[1]["bonus"].as_f64().expect("bonus"), 4750.0);
        assert_eq!(arr[1]["total_compensation"].as_f64().expect("total_comp"), 99750.0);

        // Frank: 88000 + 8800 (10%) = 96800
        assert_eq!(arr[2]["name"], "Frank");
        assert_eq!(arr[2]["bonus"].as_f64().expect("bonus"), 8800.0);
        assert_eq!(arr[2]["total_compensation"].as_f64().expect("total_comp"), 96800.0);

        // Alice: 80000 + 8000 (10%) = 88000
        assert_eq!(arr[3]["name"], "Alice");
        assert_eq!(arr[3]["bonus"].as_f64().expect("bonus"), 8000.0);
        assert_eq!(arr[3]["total_compensation"].as_f64().expect("total_comp"), 88000.0);

        // Carol: 72000 + 7200 (10%) = 79200
        assert_eq!(arr[4]["name"], "Carol");
        assert_eq!(arr[4]["bonus"].as_f64().expect("bonus"), 7200.0);
        assert_eq!(arr[4]["total_compensation"].as_f64().expect("total_comp"), 79200.0);

        // Eve: 65000 + 3250 (5%) = 68250
        assert_eq!(arr[5]["name"], "Eve");
        assert_eq!(arr[5]["bonus"].as_f64().expect("bonus"), 3250.0);
        assert_eq!(arr[5]["total_compensation"].as_f64().expect("total_comp"), 68250.0);

        ctx.stop().await;
    }

    /// Log analysis: two-stage $group to produce per-service breakdown of log levels,
    /// yielding a nested array of {level, count} for each service.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_text_stats_pipeline() {
        use mongodb::bson::DateTime;

        let mut ctx = MongoTestContext::new().await;

        let logs = vec![
            doc! { "_id": "l1",  "level": "error", "message": "Connection refused",    "service": "auth",    "timestamp": DateTime::from_millis(1700000000000_i64) },
            doc! { "_id": "l2",  "level": "warn",  "message": "Slow query detected",   "service": "auth",    "timestamp": DateTime::from_millis(1700000100000_i64) },
            doc! { "_id": "l3",  "level": "info",  "message": "User logged in",         "service": "auth",    "timestamp": DateTime::from_millis(1700000200000_i64) },
            doc! { "_id": "l4",  "level": "info",  "message": "User logged in",         "service": "auth",    "timestamp": DateTime::from_millis(1700000300000_i64) },
            doc! { "_id": "l5",  "level": "error", "message": "Null pointer exception", "service": "payment", "timestamp": DateTime::from_millis(1700000400000_i64) },
            doc! { "_id": "l6",  "level": "error", "message": "Timeout exceeded",       "service": "payment", "timestamp": DateTime::from_millis(1700000500000_i64) },
            doc! { "_id": "l7",  "level": "warn",  "message": "Retry attempt 3",        "service": "payment", "timestamp": DateTime::from_millis(1700000600000_i64) },
            doc! { "_id": "l8",  "level": "info",  "message": "Payment processed",      "service": "payment", "timestamp": DateTime::from_millis(1700000700000_i64) },
            doc! { "_id": "l9",  "level": "info",  "message": "Order created",          "service": "orders",  "timestamp": DateTime::from_millis(1700000800000_i64) },
            doc! { "_id": "l10", "level": "info",  "message": "Order shipped",          "service": "orders",  "timestamp": DateTime::from_millis(1700000900000_i64) },
            doc! { "_id": "l11", "level": "warn",  "message": "Inventory low",          "service": "orders",  "timestamp": DateTime::from_millis(1700001000000_i64) },
            doc! { "_id": "l12", "level": "error", "message": "Fulfillment failed",     "service": "orders",  "timestamp": DateTime::from_millis(1700001100000_i64) },
        ];
        ctx.insert_many("agg_logs", logs).await;

        // Two-stage grouping: first by (level, service), then by service pushing level counts
        let result = ctx
            .aggregate(
                "agg_logs",
                vec![
                    doc! { "$group": {
                        "_id": { "level": "$level", "service": "$service" },
                        "count": { "$sum": 1 }
                    }},
                    doc! { "$group": {
                        "_id": "$_id.service",
                        "level_counts": { "$push": { "level": "$_id.level", "count": "$count" } }
                    }},
                    doc! { "$sort": { "_id": 1 } },
                ],
            )
            .await;

        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 3, "should have 3 services");

        // auth service
        assert_eq!(arr[0]["_id"], "auth");
        let auth_levels = arr[0]["level_counts"].as_array().expect("level_counts should be array");
        assert_eq!(auth_levels.len(), 3, "auth should have error, warn, info levels");
        // Sum of counts should be 4 (1 error + 1 warn + 2 info)
        let auth_total: i64 = auth_levels.iter().map(|lc| lc["count"].as_i64().expect("count")).sum();
        assert_eq!(auth_total, 4, "auth service should have 4 total log entries");

        // orders service
        assert_eq!(arr[1]["_id"], "orders");
        let orders_levels = arr[1]["level_counts"].as_array().expect("level_counts should be array");
        let orders_total: i64 = orders_levels.iter().map(|lc| lc["count"].as_i64().expect("count")).sum();
        assert_eq!(orders_total, 4, "orders service should have 4 total log entries");

        // payment service
        assert_eq!(arr[2]["_id"], "payment");
        let payment_levels = arr[2]["level_counts"].as_array().expect("level_counts should be array");
        let payment_total: i64 = payment_levels.iter().map(|lc| lc["count"].as_i64().expect("count")).sum();
        assert_eq!(payment_total, 4, "payment service should have 4 total log entries");

        ctx.stop().await;
    }

    /// Document restructuring: $replaceRoot with $mergeObjects to flatten nested user
    /// and metadata sub-documents into top-level fields.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_with_replace_root() {
        let mut ctx = MongoTestContext::new().await;

        let documents = vec![
            doc! { "_id": "d1", "user": { "name": "Alice", "email": "alice@test.com" }, "metadata": { "created_at": "2024-01-15", "source": "web" } },
            doc! { "_id": "d2", "user": { "name": "Bob",   "email": "bob@test.com" },   "metadata": { "created_at": "2024-02-20", "source": "mobile" } },
            doc! { "_id": "d3", "user": { "name": "Carol", "email": "carol@test.com" }, "metadata": { "created_at": "2024-03-10", "source": "api" } },
            doc! { "_id": "d4", "user": { "name": "Dave",  "email": "dave@test.com" },  "metadata": { "created_at": "2024-04-05", "source": "web" } },
            doc! { "_id": "d5", "user": { "name": "Eve",   "email": "eve@test.com" },   "metadata": { "created_at": "2024-05-18", "source": "mobile" } },
        ];
        ctx.insert_many("agg_replace_root", documents).await;

        let result = ctx
            .aggregate(
                "agg_replace_root",
                vec![
                    doc! { "$replaceRoot": {
                        "newRoot": { "$mergeObjects": ["$user", "$metadata", { "original_id": "$_id" }] }
                    }},
                    doc! { "$sort": { "original_id": 1 } },
                ],
            )
            .await;

        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 5, "should return all 5 flattened documents");

        for doc in arr.iter() {
            // Verify all expected top-level fields exist
            assert!(doc.get("name").is_some(), "flattened doc should have 'name'");
            assert!(doc.get("email").is_some(), "flattened doc should have 'email'");
            assert!(doc.get("created_at").is_some(), "flattened doc should have 'created_at'");
            assert!(doc.get("source").is_some(), "flattened doc should have 'source'");
            assert!(doc.get("original_id").is_some(), "flattened doc should have 'original_id'");
            // Verify nested structures are gone
            assert!(doc.get("user").is_none(), "flattened doc should not have nested 'user'");
            assert!(doc.get("metadata").is_none(), "flattened doc should not have nested 'metadata'");
        }

        // Spot-check specific values
        assert_eq!(arr[0]["name"], "Alice");
        assert_eq!(arr[0]["email"], "alice@test.com");
        assert_eq!(arr[0]["source"], "web");
        assert_eq!(arr[0]["original_id"], "d1");

        assert_eq!(arr[4]["name"], "Eve");
        assert_eq!(arr[4]["email"], "eve@test.com");
        assert_eq!(arr[4]["source"], "mobile");
        assert_eq!(arr[4]["original_id"], "d5");

        ctx.stop().await;
    }

    /// Running total computation: sort transactions by date, then group all into a single
    /// array to verify ordering is preserved through the pipeline.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_running_total() {
        use mongodb::bson::DateTime;

        let mut ctx = MongoTestContext::new().await;

        let transactions = vec![
            doc! { "_id": "t1", "date": DateTime::from_millis(1704067200000_i64), "amount": 500,  "type": "credit" },
            doc! { "_id": "t2", "date": DateTime::from_millis(1704153600000_i64), "amount": 200,  "type": "debit" },
            doc! { "_id": "t3", "date": DateTime::from_millis(1704240000000_i64), "amount": 1000, "type": "credit" },
            doc! { "_id": "t4", "date": DateTime::from_millis(1704326400000_i64), "amount": 150,  "type": "debit" },
            doc! { "_id": "t5", "date": DateTime::from_millis(1704412800000_i64), "amount": 300,  "type": "credit" },
            doc! { "_id": "t6", "date": DateTime::from_millis(1704499200000_i64), "amount": 75,   "type": "debit" },
        ];
        ctx.insert_many("agg_transactions", transactions).await;

        // Sort by date, then collect all into a single document with an ordered transactions array
        let result = ctx
            .aggregate(
                "agg_transactions",
                vec![
                    doc! { "$sort": { "date": 1 } },
                    doc! { "$group": {
                        "_id": null,
                        "transactions": { "$push": { "date": "$date", "amount": "$amount", "type": "$type" } }
                    }},
                ],
            )
            .await;

        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 1, "grouping with _id: null produces a single document");

        let txns = arr[0]["transactions"].as_array().expect("transactions should be an array");
        assert_eq!(txns.len(), 6, "all 6 transactions should be collected");

        // Verify order is preserved (sorted by date ascending)
        assert_eq!(txns[0]["amount"], 500);
        assert_eq!(txns[0]["type"], "credit");

        assert_eq!(txns[1]["amount"], 200);
        assert_eq!(txns[1]["type"], "debit");

        assert_eq!(txns[2]["amount"], 1000);
        assert_eq!(txns[2]["type"], "credit");

        assert_eq!(txns[3]["amount"], 150);
        assert_eq!(txns[3]["type"], "debit");

        assert_eq!(txns[4]["amount"], 300);
        assert_eq!(txns[4]["type"], "credit");

        assert_eq!(txns[5]["amount"], 75);
        assert_eq!(txns[5]["type"], "debit");

        // Verify credit/debit breakdown
        let total_credits: i64 = txns.iter().filter(|t| t["type"] == "credit").map(|t| t["amount"].as_i64().expect("amount")).sum();
        let total_debits: i64 = txns.iter().filter(|t| t["type"] == "debit").map(|t| t["amount"].as_i64().expect("amount")).sum();
        assert_eq!(total_credits, 1800, "total credits should be 500+1000+300=1800");
        assert_eq!(total_debits, 425, "total debits should be 200+150+75=425");

        ctx.stop().await;
    }

    /// Array filtering within projection: use $filter to keep only scores >= 80 from
    /// each student's scores array.
    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_aggregate_filter_array_elements() {
        let mut ctx = MongoTestContext::new().await;

        let students = vec![
            doc! { "_id": "s1", "name": "Alice", "scores": [
                { "subject": "Math",    "grade": 95 },
                { "subject": "English", "grade": 72 },
                { "subject": "Science", "grade": 88 },
                { "subject": "History", "grade": 65 },
            ]},
            doc! { "_id": "s2", "name": "Bob", "scores": [
                { "subject": "Math",    "grade": 60 },
                { "subject": "English", "grade": 85 },
                { "subject": "Science", "grade": 79 },
                { "subject": "History", "grade": 91 },
            ]},
            doc! { "_id": "s3", "name": "Carol", "scores": [
                { "subject": "Math",    "grade": 100 },
                { "subject": "English", "grade": 92 },
                { "subject": "Science", "grade": 80 },
                { "subject": "History", "grade": 87 },
            ]},
        ];
        ctx.insert_many("agg_students", students).await;

        let result = ctx
            .aggregate(
                "agg_students",
                vec![
                    doc! { "$project": {
                        "name": 1,
                        "high_scores": {
                            "$filter": {
                                "input": "$scores",
                                "as": "s",
                                "cond": { "$gte": ["$$s.grade", 80] }
                            }
                        }
                    }},
                    doc! { "$sort": { "_id": 1 } },
                ],
            )
            .await;

        let arr = result.as_array().expect("aggregate should return an array");
        assert_eq!(arr.len(), 3, "should return all 3 students");

        // Alice: Math(95), Science(88) >= 80, so 2 high scores
        assert_eq!(arr[0]["name"], "Alice");
        let alice_scores = arr[0]["high_scores"].as_array().expect("high_scores should be array");
        assert_eq!(alice_scores.len(), 2, "Alice should have 2 high scores");
        assert_eq!(alice_scores[0]["subject"], "Math");
        assert_eq!(alice_scores[0]["grade"], 95);
        assert_eq!(alice_scores[1]["subject"], "Science");
        assert_eq!(alice_scores[1]["grade"], 88);

        // Bob: English(85), History(91) >= 80, so 2 high scores
        assert_eq!(arr[1]["name"], "Bob");
        let bob_scores = arr[1]["high_scores"].as_array().expect("high_scores should be array");
        assert_eq!(bob_scores.len(), 2, "Bob should have 2 high scores");
        assert_eq!(bob_scores[0]["subject"], "English");
        assert_eq!(bob_scores[0]["grade"], 85);
        assert_eq!(bob_scores[1]["subject"], "History");
        assert_eq!(bob_scores[1]["grade"], 91);

        // Carol: Math(100), English(92), Science(80), History(87) all >= 80, so 4 high scores
        assert_eq!(arr[2]["name"], "Carol");
        let carol_scores = arr[2]["high_scores"].as_array().expect("high_scores should be array");
        assert_eq!(carol_scores.len(), 4, "Carol should have all 4 as high scores");
        for score in carol_scores.iter() {
            let grade = score["grade"].as_i64().expect("grade should be a number");
            assert!(grade >= 80, "all of Carol's filtered scores should be >= 80, got {}", grade);
        }

        ctx.stop().await;
    }
}
