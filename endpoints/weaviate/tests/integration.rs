#![cfg(feature = "integration")]

use endpoint_test_utils::telemetry_test_utils::test_telemetry;
use endpoint_types::{EP, EpRequest, RequestConstructor};
use ep_weaviate::api::lib::*;
use ep_weaviate::ep::WeaviateEp;
use ep_weaviate::request::WeaviateRequest;
use error::ResultEP;
use format::cache_uuid::EndpointCacheUuid;
use format::{CacheUuid, EndpointUuid, OrganizationCacheUuid, OrganizationUuid};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use telemetry::TelemetryWrapper;
use testcontainers_modules::testcontainers::core::IntoContainerPort;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use weaviate_core::config::WeaviateConfig;
use weaviate_core::connection::{WeaviateCredentials, WeaviateTarget};

struct TestContext {
    container: ContainerAsync<GenericImage>,
    endpoint_uuid: EndpointCacheUuid,
    ep: WeaviateEp,
    telemetry: TelemetryWrapper,
}

impl TestContext {
    async fn stop(self) {
        let _ = self.container.stop().await;
    }

    async fn read(&mut self, req: WeaviateRequest) -> ResultEP<Value> {
        let mut req = Box::new(req) as Box<dyn EpRequest>;
        self.ep.read(&self.endpoint_uuid, &mut *req, ep_core::settings::EdenSettings::default(), &mut self.telemetry).await
    }

    async fn write(&mut self, req: WeaviateRequest) -> ResultEP<Value> {
        let req = Box::new(req) as Box<dyn EpRequest>;
        self.ep.write(&self.endpoint_uuid, &*req, ep_core::settings::EdenSettings::default(), &mut self.telemetry).await
    }

    async fn base_url(&self) -> String {
        let host = self.container.get_host().await.unwrap();
        let port = self.container.get_host_port_ipv4(8080).await.unwrap();
        format!("http://{}:{}", host, port)
    }

    async fn create_class(&self, class_def: Value) {
        let base_url = self.base_url().await;
        reqwest::Client::new()
            .post(format!("{}/v1/schema", base_url))
            .json(&class_def)
            .send()
            .await
            .expect("create class")
            .error_for_status()
            .expect("create class status");
    }
}

async fn wait_for_weaviate_ready(url: &str) {
    let client = reqwest::Client::new();
    let ready_url = format!("{}/v1/.well-known/ready", url);
    let t0 = Instant::now();

    for attempt in 0..60 {
        match client.get(&ready_url).timeout(Duration::from_secs(2)).send().await {
            Ok(resp) if resp.status().is_success() => {
                println!("Weaviate ready: {} ms (attempt {})", t0.elapsed().as_millis(), attempt + 1);
                return;
            }
            _ => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    panic!("Weaviate failed to become ready after {} ms", t0.elapsed().as_millis());
}

async fn setup() -> TestContext {
    let mut telemetry = test_telemetry();

    let container = GenericImage::new("semitechnologies/weaviate", "1.28.4")
        .with_env_var("AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED", "true")
        .with_env_var("PERSISTENCE_DATA_PATH", "/var/lib/weaviate")
        .with_mapped_port(0, 8080.tcp())
        .start()
        .await
        .expect("Failed to start Weaviate container");

    let host = container.get_host().await.expect("Failed to get host");
    let port = container.get_host_port_ipv4(8080).await.expect("Failed to get port");
    let url = format!("http://{}:{}", host, port);

    wait_for_weaviate_ready(&url).await;

    let config = Box::new(WeaviateConfig {
        target: WeaviateTarget { url: url.clone() },
        read_credentials: Some(WeaviateCredentials { token: String::new() }),
        write_credentials: Some(WeaviateCredentials { token: String::new() }),
        ..Default::default()
    });

    let endpoint_uuid =
        EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid())), EndpointUuid::new_uuid());

    let mut ep = WeaviateEp::new();
    ep.connect_async(&endpoint_uuid, config, &mut telemetry).await.expect("Failed to connect to Weaviate");

    TestContext { container, endpoint_uuid, ep, telemetry }
}

fn make_req(input: impl Into<Box<dyn ep_weaviate::serde::WeaviateOperation>>) -> WeaviateRequest {
    WeaviateRequest::new(input.into())
}

#[tokio::test]
async fn health_check() {
    let mut ctx = setup().await;

    ctx.ep.health_check(&ctx.endpoint_uuid, &mut ctx.telemetry).await.expect("health check should pass");

    ctx.stop().await;
}

#[tokio::test]
async fn schema_operations() {
    let mut ctx = setup().await;

    let result = ctx.read(make_req(GetSchemaInput::default())).await.expect("get_schema should succeed");
    assert!(result.get("classes").is_some(), "schema should contain 'classes' key: {result}");

    ctx.stop().await;
}

#[tokio::test]
async fn object_crud_lifecycle() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "TestArticle",
        "properties": [
            { "name": "title", "dataType": ["text"] },
            { "name": "content", "dataType": ["text"] }
        ]
    }))
    .await;

    // Create an object
    let create_body = json!({
        "class": "TestArticle",
        "properties": {
            "title": "Hello World",
            "content": "This is a test article."
        }
    });
    let input = CreateObjectInputBuilder::default().body(create_body.to_string()).build().unwrap();
    let created = ctx.write(make_req(input)).await.expect("create object should succeed");

    let object_id = created.get("id").and_then(|v| v.as_str()).expect("created object should have id");

    // Get the object
    let input = GetObjectInputBuilder::default().class("TestArticle").id(object_id).build().unwrap();
    let fetched = ctx.read(make_req(input)).await.expect("get object should succeed");
    assert_eq!(fetched.get("id").and_then(|v| v.as_str()), Some(object_id));

    // Update the object (PATCH with just properties)
    let update_body = json!({
        "properties": {
            "title": "Updated Title",
            "content": "Updated content."
        }
    });
    let input = UpdateObjectInputBuilder::default().class("TestArticle").id(object_id).body(update_body.to_string()).build().unwrap();
    ctx.write(make_req(input)).await.expect("update object should succeed");

    // Verify update
    let input = GetObjectInputBuilder::default().class("TestArticle").id(object_id).build().unwrap();
    let updated = ctx.read(make_req(input)).await.expect("get updated object should succeed");
    let title = updated.pointer("/properties/title").and_then(|v| v.as_str()).expect("should have title");
    assert_eq!(title, "Updated Title");

    // Delete the object
    let input = DeleteObjectInputBuilder::default().class("TestArticle").id(object_id).build().unwrap();
    ctx.write(make_req(input)).await.expect("delete object should succeed");

    // Verify deletion (get should fail or return error)
    let input = GetObjectInputBuilder::default().class("TestArticle").id(object_id).build().unwrap();
    let get_deleted = ctx.read(make_req(input)).await;
    assert!(
        get_deleted.is_err() || get_deleted.unwrap().get("id").is_none(),
        "deleted object should not be found"
    );

    ctx.stop().await;
}

#[tokio::test]
async fn list_objects() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "ListTest",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;

    // Create a few objects
    for i in 0..3 {
        let input = CreateObjectInputBuilder::default()
            .body(
                json!({
                    "class": "ListTest",
                    "properties": { "name": format!("item-{i}") }
                })
                .to_string(),
            )
            .build()
            .unwrap();
        ctx.write(make_req(input)).await.expect("create object");
    }

    // List with limit
    let input = ListObjectsInputBuilder::default().class(Some("ListTest".into())).limit(Some(2)).build().unwrap();
    let result = ctx.read(make_req(input)).await.expect("list objects should succeed");

    let objects = result.get("objects").and_then(|v| v.as_array()).expect("should have objects array");
    assert_eq!(objects.len(), 2, "should respect limit");

    ctx.stop().await;
}

#[tokio::test]
async fn batch_objects() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "BatchTest",
        "properties": [{ "name": "value", "dataType": ["text"] }]
    }))
    .await;

    // Batch insert
    let batch_body = json!({
        "objects": [
            { "class": "BatchTest", "properties": { "value": "batch-1" } },
            { "class": "BatchTest", "properties": { "value": "batch-2" } },
            { "class": "BatchTest", "properties": { "value": "batch-3" } },
        ]
    });

    let input = BatchObjectsInputBuilder::default().body(batch_body.to_string()).build().unwrap();
    let result = ctx.write(make_req(input)).await.expect("batch objects should succeed");

    // EP framework wraps non-object responses in {"kind":"weaviate","data":[...]}
    let results = result.get("data").and_then(|v| v.as_array()).expect("batch response should have data array");
    assert_eq!(results.len(), 3, "should have 3 batch results");

    ctx.stop().await;
}

#[tokio::test]
async fn graphql_query() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "GqlTest",
        "properties": [{ "name": "title", "dataType": ["text"] }]
    }))
    .await;

    // Create an object
    let input = CreateObjectInputBuilder::default()
        .body(
            json!({
                "class": "GqlTest",
                "properties": { "title": "GraphQL Test" }
            })
            .to_string(),
        )
        .build()
        .unwrap();
    ctx.write(make_req(input)).await.expect("create object");

    // Query via GraphQL
    let query = json!({ "query": "{ Get { GqlTest { title } } }" });
    let input = GraphQLInputBuilder::default().body(query.to_string()).build().unwrap();
    let result = ctx.read(make_req(input)).await.expect("graphql query should succeed");

    assert!(result.get("data").is_some(), "GraphQL response should have 'data' key: {result}");

    ctx.stop().await;
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
async fn get_nonexistent_object() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "ErrGetTest",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;

    // get_object doesn't check HTTP status — Weaviate 404 returns JSON error parsed as Ok
    let input = GetObjectInputBuilder::default().class("ErrGetTest").id("00000000-0000-0000-0000-000000000000").build().unwrap();
    let result = ctx.read(make_req(input)).await;

    // Should either be Err or Ok with error payload (no "id" field)
    match result {
        Err(_) => {} // acceptable
        Ok(val) => {
            assert!(
                val.get("error").is_some() || val.get("id").is_none(),
                "nonexistent object should return error or have no id: {val}"
            );
        }
    }

    ctx.stop().await;
}

#[tokio::test]
async fn delete_nonexistent_object() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "ErrDelTest",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;

    // Weaviate DELETE is idempotent — returns 204 even for non-existent objects
    let input = DeleteObjectInputBuilder::default().class("ErrDelTest").id("00000000-0000-0000-0000-000000000000").build().unwrap();
    let result = ctx.write(make_req(input)).await;
    assert!(result.is_ok(), "delete of non-existent object should be idempotent (204)");

    ctx.stop().await;
}

#[tokio::test]
async fn update_nonexistent_object() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "ErrUpdTest",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;

    // update_object checks HTTP status — should return Err on 404
    let input = UpdateObjectInputBuilder::default()
        .class("ErrUpdTest")
        .id("00000000-0000-0000-0000-000000000000")
        .body(json!({"properties": {"name": "updated"}}).to_string())
        .build()
        .unwrap();
    let result = ctx.write(make_req(input)).await;
    assert!(result.is_err(), "updating nonexistent object should fail");

    ctx.stop().await;
}

#[tokio::test]
async fn create_object_invalid_body() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "ErrCreateTest",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;

    // Send invalid JSON body (missing class) — Weaviate returns error
    let input = CreateObjectInputBuilder::default().body(json!({"properties": {"name": "no class field"}}).to_string()).build().unwrap();
    let result = ctx.write(make_req(input)).await;

    // create_object doesn't check status — Weaviate returns JSON error body
    match result {
        Err(_) => {} // acceptable
        Ok(val) => {
            // Weaviate should return an error about missing class
            assert!(
                val.get("error").is_some() || val.get("class").is_none(),
                "creating object without class should return error or have no class: {val}"
            );
        }
    }

    ctx.stop().await;
}

#[tokio::test]
async fn graphql_invalid_query() {
    let mut ctx = setup().await;

    // Create a class so GraphQL provider is present
    ctx.create_class(json!({
        "class": "GqlErrTest",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;

    // Query a class that doesn't exist — GraphQL returns 200 with errors array
    let query = json!({ "query": "{ Get { NonExistentClass { name } } }" });
    let input = GraphQLInputBuilder::default().body(query.to_string()).build().unwrap();
    let result = ctx.read(make_req(input)).await.expect("graphql should return Ok even with errors");

    // GraphQL standard: errors are in "errors" key
    assert!(result.get("errors").is_some(), "invalid GraphQL query should have 'errors' key: {result}");

    ctx.stop().await;
}

#[tokio::test]
async fn graphql_syntax_error() {
    let mut ctx = setup().await;

    // Create a class so GraphQL provider is present
    ctx.create_class(json!({
        "class": "GqlSyntaxTest",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;

    let query = json!({ "query": "this is not valid graphql {" });
    let input = GraphQLInputBuilder::default().body(query.to_string()).build().unwrap();
    let result = ctx.read(make_req(input)).await.expect("graphql should return Ok even with syntax errors");

    // GraphQL standard: errors are in "errors" key
    assert!(result.get("errors").is_some(), "GraphQL syntax error should have 'errors' key: {result}");

    ctx.stop().await;
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[tokio::test]
async fn list_objects_empty_collection() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "EmptyTest",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;

    let input = ListObjectsInputBuilder::default().class(Some("EmptyTest".into())).limit(Some(10)).build().unwrap();
    let result = ctx.read(make_req(input)).await.expect("list should succeed");

    let objects = result.get("objects").and_then(|v| v.as_array()).expect("should have objects array");
    assert!(objects.is_empty(), "empty collection should return empty array");

    ctx.stop().await;
}

#[tokio::test]
async fn list_objects_pagination() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "PageTest",
        "properties": [{ "name": "idx", "dataType": ["text"] }]
    }))
    .await;

    for i in 0..5 {
        let input = CreateObjectInputBuilder::default()
            .body(json!({"class": "PageTest", "properties": {"idx": format!("{i}")}}).to_string())
            .build()
            .unwrap();
        ctx.write(make_req(input)).await.expect("create");
    }

    // Limit=2 should return 2
    let input = ListObjectsInputBuilder::default().class(Some("PageTest".into())).limit(Some(2)).build().unwrap();
    let result = ctx.read(make_req(input)).await.expect("list");
    let objects = result.get("objects").and_then(|v| v.as_array()).expect("objects array");
    assert_eq!(objects.len(), 2, "limit=2 should return 2 objects");

    // Limit=10 should return all 5
    let input = ListObjectsInputBuilder::default().class(Some("PageTest".into())).limit(Some(10)).build().unwrap();
    let result = ctx.read(make_req(input)).await.expect("list all");
    let objects = result.get("objects").and_then(|v| v.as_array()).expect("objects array");
    assert_eq!(objects.len(), 5, "limit=10 should return all 5 objects");

    ctx.stop().await;
}

#[tokio::test]
async fn create_object_with_explicit_uuid() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "UuidTest",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;

    let explicit_uuid = "550e8400-e29b-41d4-a716-446655440000";
    let input = CreateObjectInputBuilder::default()
        .body(
            json!({
                "class": "UuidTest",
                "id": explicit_uuid,
                "properties": { "name": "explicit id" }
            })
            .to_string(),
        )
        .build()
        .unwrap();
    let created = ctx.write(make_req(input)).await.expect("create with explicit UUID");
    assert_eq!(
        created.get("id").and_then(|v| v.as_str()),
        Some(explicit_uuid),
        "created object id should match explicit UUID"
    );

    // Fetch it back
    let input = GetObjectInputBuilder::default().class("UuidTest").id(explicit_uuid).build().unwrap();
    let fetched = ctx.read(make_req(input)).await.expect("get by explicit UUID");
    assert_eq!(fetched.get("id").and_then(|v| v.as_str()), Some(explicit_uuid));

    ctx.stop().await;
}

#[tokio::test]
async fn create_duplicate_uuid() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "DupTest",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;

    let uuid = "660e8400-e29b-41d4-a716-446655440001";
    let body = json!({
        "class": "DupTest",
        "id": uuid,
        "properties": { "name": "first" }
    });

    let input = CreateObjectInputBuilder::default().body(body.to_string()).build().unwrap();
    ctx.write(make_req(input)).await.expect("first create should succeed");

    // Second create with same UUID should fail or return error
    let input = CreateObjectInputBuilder::default().body(body.to_string()).build().unwrap();
    let result = ctx.write(make_req(input)).await;

    match result {
        Err(_) => {} // acceptable
        Ok(val) => {
            assert!(val.get("error").is_some(), "duplicate UUID create should return error: {val}");
        }
    }

    ctx.stop().await;
}

#[tokio::test]
async fn object_with_special_characters() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "SpecCharTest",
        "properties": [{ "name": "content", "dataType": ["text"] }]
    }))
    .await;

    let special_text = r#"title with "quotes" & <angle> brackets and unicode: 日本語 émojis 🎉"#;
    let input = CreateObjectInputBuilder::default()
        .body(
            json!({
                "class": "SpecCharTest",
                "properties": { "content": special_text }
            })
            .to_string(),
        )
        .build()
        .unwrap();
    let created = ctx.write(make_req(input)).await.expect("create with special chars");
    let object_id = created.get("id").and_then(|v| v.as_str()).expect("should have id");

    let input = GetObjectInputBuilder::default().class("SpecCharTest").id(object_id).build().unwrap();
    let fetched = ctx.read(make_req(input)).await.expect("get special char object");
    let content = fetched.pointer("/properties/content").and_then(|v| v.as_str()).expect("should have content");
    assert_eq!(content, special_text, "special characters should round-trip exactly");

    ctx.stop().await;
}

#[tokio::test]
async fn batch_objects_invalid_uuid_rejected() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "BatchMixTest",
        "properties": [{ "name": "val", "dataType": ["text"] }]
    }))
    .await;

    // Weaviate validates UUIDs upfront — invalid UUID rejects the entire batch
    let batch_body = json!({
        "objects": [
            { "class": "BatchMixTest", "properties": { "val": "ok-1" } },
            { "class": "BatchMixTest", "id": "not-a-valid-uuid", "properties": { "val": "bad" } },
            { "class": "BatchMixTest", "properties": { "val": "ok-2" } },
        ]
    });

    let input = BatchObjectsInputBuilder::default().body(batch_body.to_string()).build().unwrap();
    let result = ctx.write(make_req(input)).await.expect("batch should return Ok (error in body)");

    // Weaviate rejects the whole batch with validation error (code 601)
    assert!(
        result.get("code").is_some() || result.get("error").is_some() || result.get("message").is_some(),
        "batch with invalid UUID should return validation error: {result}"
    );

    // Verify none of the valid objects were created
    let input = ListObjectsInputBuilder::default().class(Some("BatchMixTest".into())).limit(Some(100)).build().unwrap();
    let list_result = ctx.read(make_req(input)).await.expect("list");
    let objects = list_result.get("objects").and_then(|v| v.as_array()).expect("objects");
    assert_eq!(objects.len(), 0, "no objects should be created when batch validation fails");

    ctx.stop().await;
}

// ============================================================================
// Response Structure Tests
// ============================================================================

#[tokio::test]
async fn create_object_response_structure() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "StructCreateTest",
        "properties": [{ "name": "title", "dataType": ["text"] }]
    }))
    .await;

    let input = CreateObjectInputBuilder::default()
        .body(json!({"class": "StructCreateTest", "properties": {"title": "test"}}).to_string())
        .build()
        .unwrap();
    let result = ctx.write(make_req(input)).await.expect("create");

    // EP framework flattens object responses and adds "kind"
    assert_eq!(result.get("kind").and_then(|v| v.as_str()), Some("weaviate"));
    assert!(result.get("id").and_then(|v| v.as_str()).is_some(), "should have id");
    assert_eq!(result.get("class").and_then(|v| v.as_str()), Some("StructCreateTest"));
    assert!(result.get("properties").is_some(), "should have properties");
    assert!(result.get("creationTimeUnix").is_some(), "should have creationTimeUnix");

    ctx.stop().await;
}

#[tokio::test]
async fn get_object_response_structure() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "StructGetTest",
        "properties": [{ "name": "title", "dataType": ["text"] }]
    }))
    .await;

    let input = CreateObjectInputBuilder::default()
        .body(json!({"class": "StructGetTest", "properties": {"title": "test"}}).to_string())
        .build()
        .unwrap();
    let created = ctx.write(make_req(input)).await.expect("create");
    let id = created.get("id").and_then(|v| v.as_str()).expect("id");

    let input = GetObjectInputBuilder::default().class("StructGetTest").id(id).build().unwrap();
    let result = ctx.read(make_req(input)).await.expect("get");

    assert_eq!(result.get("kind").and_then(|v| v.as_str()), Some("weaviate"));
    assert_eq!(result.get("id").and_then(|v| v.as_str()), Some(id));
    assert_eq!(result.get("class").and_then(|v| v.as_str()), Some("StructGetTest"));
    assert!(result.get("properties").is_some(), "should have properties");

    ctx.stop().await;
}

#[tokio::test]
async fn list_objects_response_structure() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "StructListTest",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;

    let input = CreateObjectInputBuilder::default()
        .body(json!({"class": "StructListTest", "properties": {"name": "item"}}).to_string())
        .build()
        .unwrap();
    ctx.write(make_req(input)).await.expect("create");

    let input = ListObjectsInputBuilder::default().class(Some("StructListTest".into())).limit(Some(10)).build().unwrap();
    let result = ctx.read(make_req(input)).await.expect("list");

    // List response is an object -> EP framework flattens with "kind"
    assert_eq!(result.get("kind").and_then(|v| v.as_str()), Some("weaviate"));
    assert!(result.get("objects").and_then(|v| v.as_array()).is_some(), "should have objects array");
    assert!(result.get("totalResults").is_some(), "should have totalResults");

    ctx.stop().await;
}

#[tokio::test]
async fn batch_response_structure() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "StructBatchTest",
        "properties": [{ "name": "val", "dataType": ["text"] }]
    }))
    .await;

    let batch_body = json!({
        "objects": [
            { "class": "StructBatchTest", "properties": { "val": "a" } },
            { "class": "StructBatchTest", "properties": { "val": "b" } },
        ]
    });
    let input = BatchObjectsInputBuilder::default().body(batch_body.to_string()).build().unwrap();
    let result = ctx.write(make_req(input)).await.expect("batch");

    // Batch returns array -> EP wraps in {"kind":"weaviate","data":[...]}
    assert_eq!(result.get("kind").and_then(|v| v.as_str()), Some("weaviate"));
    let items = result.get("data").and_then(|v| v.as_array()).expect("should have data array");
    assert_eq!(items.len(), 2);

    for item in items {
        assert!(item.get("id").and_then(|v| v.as_str()).is_some(), "each item should have id");
        assert_eq!(item.get("class").and_then(|v| v.as_str()), Some("StructBatchTest"));
        assert_eq!(item.pointer("/result/status").and_then(|v| v.as_str()), Some("SUCCESS"));
    }

    ctx.stop().await;
}

// ============================================================================
// Multi-Operation & Schema Tests
// ============================================================================

#[tokio::test]
async fn multiple_classes_isolation() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "IsoClassA",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;
    ctx.create_class(json!({
        "class": "IsoClassB",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;

    // Insert 2 into A, 3 into B
    for i in 0..2 {
        let input = CreateObjectInputBuilder::default()
            .body(json!({"class": "IsoClassA", "properties": {"name": format!("a-{i}")}}).to_string())
            .build()
            .unwrap();
        ctx.write(make_req(input)).await.expect("create A");
    }
    for i in 0..3 {
        let input = CreateObjectInputBuilder::default()
            .body(json!({"class": "IsoClassB", "properties": {"name": format!("b-{i}")}}).to_string())
            .build()
            .unwrap();
        ctx.write(make_req(input)).await.expect("create B");
    }

    // List A should return 2
    let input = ListObjectsInputBuilder::default().class(Some("IsoClassA".into())).limit(Some(100)).build().unwrap();
    let result = ctx.read(make_req(input)).await.expect("list A");
    let objects = result.get("objects").and_then(|v| v.as_array()).expect("objects");
    assert_eq!(objects.len(), 2, "class A should have 2 objects");

    // List B should return 3
    let input = ListObjectsInputBuilder::default().class(Some("IsoClassB".into())).limit(Some(100)).build().unwrap();
    let result = ctx.read(make_req(input)).await.expect("list B");
    let objects = result.get("objects").and_then(|v| v.as_array()).expect("objects");
    assert_eq!(objects.len(), 3, "class B should have 3 objects");

    ctx.stop().await;
}

#[tokio::test]
async fn rapid_create_delete_cycle() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "CycleTest",
        "properties": [{ "name": "idx", "dataType": ["text"] }]
    }))
    .await;

    for i in 0..10 {
        let input = CreateObjectInputBuilder::default()
            .body(json!({"class": "CycleTest", "properties": {"idx": format!("{i}")}}).to_string())
            .build()
            .unwrap();
        let created = ctx.write(make_req(input)).await.expect("create");
        let id = created.get("id").and_then(|v| v.as_str()).expect("id");

        let input = DeleteObjectInputBuilder::default().class("CycleTest").id(id).build().unwrap();
        ctx.write(make_req(input)).await.expect("delete");
    }

    // Verify 0 objects remain
    let input = ListObjectsInputBuilder::default().class(Some("CycleTest".into())).limit(Some(100)).build().unwrap();
    let result = ctx.read(make_req(input)).await.expect("list");
    let objects = result.get("objects").and_then(|v| v.as_array()).expect("objects");
    assert_eq!(objects.len(), 0, "all objects should be deleted");

    ctx.stop().await;
}

#[tokio::test]
async fn update_preserves_unmodified_fields() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "PreserveTest",
        "properties": [
            { "name": "title", "dataType": ["text"] },
            { "name": "count", "dataType": ["int"] }
        ]
    }))
    .await;

    let input = CreateObjectInputBuilder::default()
        .body(
            json!({
                "class": "PreserveTest",
                "properties": { "title": "original", "count": 42 }
            })
            .to_string(),
        )
        .build()
        .unwrap();
    let created = ctx.write(make_req(input)).await.expect("create");
    let id = created.get("id").and_then(|v| v.as_str()).expect("id");

    // PATCH only title
    let input = UpdateObjectInputBuilder::default()
        .class("PreserveTest")
        .id(id)
        .body(json!({"properties": {"title": "updated"}}).to_string())
        .build()
        .unwrap();
    ctx.write(make_req(input)).await.expect("update");

    // Verify count is preserved
    let input = GetObjectInputBuilder::default().class("PreserveTest").id(id).build().unwrap();
    let result = ctx.read(make_req(input)).await.expect("get after update");
    let title = result.pointer("/properties/title").and_then(|v| v.as_str()).expect("title");
    let count = result.pointer("/properties/count").and_then(|v| v.as_f64()).expect("count");
    assert_eq!(title, "updated");
    assert_eq!(count, 42.0, "unmodified field should be preserved after PATCH");

    ctx.stop().await;
}

#[tokio::test]
async fn schema_reflects_created_classes() {
    let mut ctx = setup().await;

    ctx.create_class(json!({
        "class": "SchemaTestA",
        "properties": [{ "name": "name", "dataType": ["text"] }]
    }))
    .await;
    ctx.create_class(json!({
        "class": "SchemaTestB",
        "properties": [{ "name": "val", "dataType": ["int"] }]
    }))
    .await;

    let result = ctx.read(make_req(GetSchemaInput::default())).await.expect("get schema");
    let classes = result.get("classes").and_then(|v| v.as_array()).expect("classes array");

    let class_names: Vec<&str> = classes.iter().filter_map(|c| c.get("class").and_then(|v| v.as_str())).collect();

    assert!(class_names.contains(&"SchemaTestA"), "schema should contain SchemaTestA");
    assert!(class_names.contains(&"SchemaTestB"), "schema should contain SchemaTestB");

    ctx.stop().await;
}
