use endpoint_types::RequestConstructor;
use ep_weaviate::api::lib::*;
use ep_weaviate::request::WeaviateRequest;

fn roundtrip(req: WeaviateRequest, expected: WeaviateApi) {
    let value = serde_json::to_value(&req).expect("serialize request");
    let decoded: WeaviateRequest = serde_json::from_value(value).expect("deserialize request");
    assert_eq!(decoded.0.kind(), expected);
}

#[test]
fn graphql_request_roundtrip() {
    let input = GraphQLInput::default();
    roundtrip(WeaviateRequest::new(Box::new(input)), WeaviateApi::GraphQL);
}

#[test]
fn create_object_request_roundtrip() {
    let input = CreateObjectInput::default();
    roundtrip(WeaviateRequest::new(Box::new(input)), WeaviateApi::CreateObject);
}

#[test]
fn get_object_request_roundtrip() {
    let input = GetObjectInput::default();
    roundtrip(WeaviateRequest::new(Box::new(input)), WeaviateApi::GetObject);
}

#[test]
fn list_objects_request_roundtrip() {
    let input = ListObjectsInput::default();
    roundtrip(WeaviateRequest::new(Box::new(input)), WeaviateApi::ListObjects);
}

#[test]
fn update_object_request_roundtrip() {
    let input = UpdateObjectInput::default();
    roundtrip(WeaviateRequest::new(Box::new(input)), WeaviateApi::UpdateObject);
}

#[test]
fn delete_object_request_roundtrip() {
    let input = DeleteObjectInput::default();
    roundtrip(WeaviateRequest::new(Box::new(input)), WeaviateApi::DeleteObject);
}

#[test]
fn batch_objects_request_roundtrip() {
    let input = BatchObjectsInput::default();
    roundtrip(WeaviateRequest::new(Box::new(input)), WeaviateApi::BatchObjects);
}

#[test]
fn get_schema_request_roundtrip() {
    let input = GetSchemaInput::default();
    roundtrip(WeaviateRequest::new(Box::new(input)), WeaviateApi::GetSchema);
}
