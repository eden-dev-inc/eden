use ep_elasticache::api::control_plane::{CreateUserInput, ElasticacheApi};
use ep_elasticache::request::ElasticacheRequest;
use ep_redis::api::RedisApi;
use ep_redis::request::RedisRequest;

#[test]
fn control_plane_request_roundtrip() {
    let input = CreateUserInput {
        user_id: "user-id".to_string(),
        user_name: "user-name".to_string(),
        engine: "redis".to_string(),
        access_string: "on ~* +@all".to_string(),
        passwords: None,
        no_password_required: Some(true),
        tags: None,
    };

    let req = ElasticacheRequest(Box::new(input));
    let value = serde_json::to_value(&req).expect("serialize control-plane request");
    let decoded: ElasticacheRequest = serde_json::from_value(value).expect("deserialize control-plane request");

    assert_eq!(decoded.0.kind(), ElasticacheApi::CreateUser);
}

#[test]
fn redis_request_serialization_matches_elasticache() {
    let ping_a = RedisApi::Ping.decode_from_args(vec![]).expect("decode ping");
    let ping_b = RedisApi::Ping.decode_from_args(vec![]).expect("decode ping");

    let redis_value = serde_json::to_value(RedisRequest(ping_a)).expect("serialize redis request");
    let elasticache_value =
        serde_json::to_value(ElasticacheRequest::from(RedisRequest(ping_b))).expect("serialize elasticache redis request");

    assert_eq!(elasticache_value, redis_value);
}
