use weaviate_wire::OperationType;
use weaviate_wire::grpc::metadata::GrpcMetadata;
use weaviate_wire::grpc::method::{self, WeaviateGrpcMethod, classify_grpc_method};
use weaviate_wire::http::headers::WeaviateRequestHeaders;
use weaviate_wire::http::query_params::QueryParams;
use weaviate_wire::http::route::{WeaviateRoute, parse_route};

// ============================================================================
// HTTP Pipeline Tests
// ============================================================================

#[test]
fn classify_object_get_with_headers_and_params() {
    // Simulate: GET /v1/objects/Article/abc-123?include=vector
    // with Authorization + Content-Type + Tenant headers
    let route = parse_route("GET", "/v1/objects/Article/abc-123?include=vector");
    assert_eq!(route, WeaviateRoute::GetObject { class_name: Some("Article".into()), id: "abc-123".into() });
    assert_eq!(route.operation_type(), OperationType::Read);
    assert_eq!(route.class_name(), Some("Article"));
    assert_eq!(route.object_id(), Some("abc-123"));

    let headers = WeaviateRequestHeaders::parse(
        [
            ("Authorization", "Bearer my-token"),
            ("Content-Type", "application/json"),
            ("X-Weaviate-Tenant", "tenantA"),
        ]
        .into_iter(),
    );
    assert_eq!(headers.auth_token, Some("my-token".into()));
    assert_eq!(headers.content_type, Some("application/json".into()));
    assert_eq!(headers.tenant, Some("tenantA".into()));

    let params = QueryParams::parse_query_string("include=vector&limit=10");
    assert_eq!(params.include, Some("vector".into()));
    assert_eq!(params.limit, Some(10));
}

#[test]
fn classify_object_create_with_auth() {
    let route = parse_route("POST", "/v1/objects");
    assert_eq!(route, WeaviateRoute::CreateObject);
    assert_eq!(route.operation_type(), OperationType::Write);
    assert_eq!(route.class_name(), None);
    assert_eq!(route.object_id(), None);

    let headers =
        WeaviateRequestHeaders::parse([("Authorization", "Bearer create-token"), ("Content-Type", "application/json")].into_iter());
    assert_eq!(headers.auth_token, Some("create-token".into()));
}

#[test]
fn classify_batch_objects() {
    let route = parse_route("POST", "/v1/batch/objects");
    assert_eq!(route, WeaviateRoute::BatchObjects);
    assert_eq!(route.operation_type(), OperationType::Write);

    let headers = WeaviateRequestHeaders::parse([("Content-Type", "application/json")].into_iter());
    assert_eq!(headers.content_type, Some("application/json".into()));
    assert_eq!(headers.auth_token, None);
}

#[test]
fn classify_graphql_with_module_keys() {
    let route = parse_route("POST", "/v1/graphql");
    assert_eq!(route, WeaviateRoute::GraphQL);
    assert_eq!(route.operation_type(), OperationType::Read);

    let headers = WeaviateRequestHeaders::parse(
        [
            ("Authorization", "Bearer gql-token"),
            ("X-OpenAI-Api-Key", "sk-openai-123"),
            ("X-Cohere-Api-Key", "cohere-456"),
        ]
        .into_iter(),
    );
    assert_eq!(headers.auth_token, Some("gql-token".into()));
    assert_eq!(headers.module_api_keys.len(), 2);
    assert_eq!(headers.module_api_keys.get("x-openai-api-key"), Some(&"sk-openai-123".to_string()));
    assert_eq!(headers.module_api_keys.get("x-cohere-api-key"), Some(&"cohere-456".to_string()));
}

#[test]
fn classify_schema_tenant_operations() {
    let route = parse_route("GET", "/v1/schema/MyClass/tenants");
    assert_eq!(route, WeaviateRoute::GetTenants { class_name: "MyClass".into() });
    assert_eq!(route.operation_type(), OperationType::Read);
    assert_eq!(route.class_name(), Some("MyClass"));

    let headers = WeaviateRequestHeaders::parse([("X-Weaviate-Tenant", "tenantX")].into_iter());
    assert_eq!(headers.tenant, Some("tenantX".into()));

    let params = QueryParams::parse_query_string("consistency_level=QUORUM");
    assert_eq!(params.consistency_level, Some("QUORUM".into()));
}

#[test]
fn classify_backup_operations() {
    let route = parse_route("POST", "/v1/backups/s3/backup-123/restore");
    assert_eq!(route, WeaviateRoute::RestoreBackup { backend: "s3".into(), id: "backup-123".into() });
    assert_eq!(route.operation_type(), OperationType::Write);
    assert_eq!(route.object_id(), Some("backup-123"));
}

#[test]
fn classify_health_meta() {
    let ready = parse_route("GET", "/v1/.well-known/ready");
    assert_eq!(ready, WeaviateRoute::ReadyCheck);
    assert_eq!(ready.operation_type(), OperationType::Meta);

    let live = parse_route("GET", "/v1/.well-known/live");
    assert_eq!(live, WeaviateRoute::LiveCheck);
    assert_eq!(live.operation_type(), OperationType::Meta);

    let meta = parse_route("GET", "/v1/meta");
    assert_eq!(meta, WeaviateRoute::GetMeta);
    assert_eq!(meta.operation_type(), OperationType::Meta);

    let openid = parse_route("GET", "/v1/.well-known/openid-configuration");
    assert_eq!(openid, WeaviateRoute::OpenIDConfig);
    assert_eq!(openid.operation_type(), OperationType::Meta);
}

#[test]
fn classify_unknown_route() {
    let route = parse_route("POST", "/v1/unknown/path");
    assert!(matches!(route, WeaviateRoute::Unknown { .. }));
    // Unknown routes conservatively default to Write
    assert_eq!(route.operation_type(), OperationType::Write);
}

// ============================================================================
// gRPC Classification Tests
// ============================================================================

#[test]
fn grpc_search_with_metadata() {
    let method = classify_grpc_method(method::paths::SEARCH);
    assert_eq!(method, WeaviateGrpcMethod::Search);
    assert_eq!(method.operation_type(), OperationType::Read);

    let metadata = GrpcMetadata::parse([("authorization", "Bearer grpc-token"), ("x-weaviate-tenant", "tenantGrpc")].into_iter());
    assert_eq!(metadata.auth_token, Some("grpc-token".into()));
    assert_eq!(metadata.tenant, Some("tenantGrpc".into()));
}

#[test]
fn grpc_all_batch_methods_are_write() {
    let batch_paths = [
        (method::paths::BATCH_OBJECTS, WeaviateGrpcMethod::BatchObjects),
        (method::paths::BATCH_REFERENCES, WeaviateGrpcMethod::BatchReferences),
        (method::paths::BATCH_DELETE, WeaviateGrpcMethod::BatchDelete),
        (method::paths::BATCH_STREAM, WeaviateGrpcMethod::BatchStream),
    ];

    for (path, expected) in batch_paths {
        let method = classify_grpc_method(path);
        assert_eq!(method, expected, "path: {path}");
        assert_eq!(method.operation_type(), OperationType::Write, "path: {path} should be Write");
    }
}

#[test]
fn grpc_aggregate_and_tenants_are_read() {
    let read_methods = [
        (method::paths::AGGREGATE, WeaviateGrpcMethod::Aggregate),
        (method::paths::TENANTS_GET, WeaviateGrpcMethod::TenantsGet),
    ];

    for (path, expected) in read_methods {
        let method = classify_grpc_method(path);
        assert_eq!(method, expected, "path: {path}");
        assert_eq!(method.operation_type(), OperationType::Read, "path: {path} should be Read");
    }
}

#[test]
fn grpc_unknown_method_conservative() {
    let method = classify_grpc_method("/weaviate.v1.Weaviate/FutureMethod");
    assert!(matches!(method, WeaviateGrpcMethod::Unknown(_)));
    assert_eq!(method.operation_type(), OperationType::Write);
}

#[test]
fn grpc_known_paths_match_constants() {
    let all_known_paths = [
        method::paths::SEARCH,
        method::paths::AGGREGATE,
        method::paths::BATCH_OBJECTS,
        method::paths::BATCH_REFERENCES,
        method::paths::BATCH_DELETE,
        method::paths::BATCH_STREAM,
        method::paths::TENANTS_GET,
    ];

    for path in all_known_paths {
        let method = classify_grpc_method(path);
        assert!(
            !matches!(method, WeaviateGrpcMethod::Unknown(_)),
            "path {path} should classify to a known method, got Unknown"
        );
    }
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[test]
fn query_params_roundtrip() {
    let original = QueryParams::new()
        .class_name("MyClass")
        .consistency_level("QUORUM")
        .tenant("tenantA")
        .limit(100)
        .offset(50)
        .after("cursor-abc")
        .include("vector");

    let query_string = original.to_query_string();
    let parsed = QueryParams::parse_query_string(&query_string);

    assert_eq!(parsed.class_name, Some("MyClass".into()));
    assert_eq!(parsed.consistency_level, Some("QUORUM".into()));
    assert_eq!(parsed.tenant, Some("tenantA".into()));
    assert_eq!(parsed.limit, Some(100));
    assert_eq!(parsed.offset, Some(50));
    assert_eq!(parsed.after, Some("cursor-abc".into()));
    assert_eq!(parsed.include, Some("vector".into()));
}

#[test]
fn query_params_invalid_limit_ignored() {
    let params = QueryParams::parse_query_string("limit=abc&offset=10");
    assert_eq!(params.limit, None);
    assert_eq!(params.offset, Some(10));
}

#[test]
fn headers_multiple_module_api_keys() {
    let headers = WeaviateRequestHeaders::parse(
        [
            ("X-OpenAI-Api-Key", "openai-key"),
            ("X-Cohere-Api-Key", "cohere-key"),
            ("X-HuggingFace-Api-Key", "hf-key"),
            ("X-Azure-Api-Key", "azure-key"),
            ("X-Google-Api-Key", "google-key"),
        ]
        .into_iter(),
    );

    assert_eq!(headers.module_api_keys.len(), 5);
    assert_eq!(headers.module_api_keys.get("x-openai-api-key"), Some(&"openai-key".into()));
    assert_eq!(headers.module_api_keys.get("x-cohere-api-key"), Some(&"cohere-key".into()));
    assert_eq!(headers.module_api_keys.get("x-huggingface-api-key"), Some(&"hf-key".into()));
    assert_eq!(headers.module_api_keys.get("x-azure-api-key"), Some(&"azure-key".into()));
    assert_eq!(headers.module_api_keys.get("x-google-api-key"), Some(&"google-key".into()));
}

#[test]
fn headers_empty_returns_defaults() {
    let headers = WeaviateRequestHeaders::parse(std::iter::empty());
    assert_eq!(headers.auth_token, None);
    assert_eq!(headers.content_type, None);
    assert_eq!(headers.tenant, None);
    assert!(headers.module_api_keys.is_empty());
}
