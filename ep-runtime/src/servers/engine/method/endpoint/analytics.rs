#[cfg(any(feature = "mongo", feature = "postgres", feature = "redis"))]
use eden_core::format::EndpointUuid;
#[cfg(any(feature = "mongo", feature = "postgres", feature = "redis"))]
use endpoint_types::EpRequest;
#[cfg(feature = "mongo")]
use serde_json::Value;

#[cfg(any(feature = "mongo", feature = "postgres", feature = "redis"))]
pub(crate) fn usize_to_u32(value: usize) -> u32 {
    match u32::try_from(value) {
        Ok(value) => value,
        Err(_) => u32::MAX,
    }
}

#[cfg(feature = "mongo")]
#[derive(Debug, Clone, Default)]
pub struct MongoRequestFacts;

#[cfg(feature = "mongo")]
#[derive(Debug, Clone, Default)]
pub struct MongoResponseFacts;

#[cfg(feature = "redis")]
#[derive(Debug, Clone, Default)]
pub struct RedisRequestFacts;

#[cfg(feature = "postgres")]
#[derive(Debug, Clone, Default)]
pub struct PostgresRequestFacts;

#[cfg(feature = "mongo")]
pub fn extract_mongo_request_facts(_request: &dyn EpRequest) -> Option<MongoRequestFacts> {
    None
}

#[cfg(feature = "mongo")]
pub fn extract_response_facts(_response: &Value) -> MongoResponseFacts {
    MongoResponseFacts
}

#[cfg(feature = "mongo")]
#[allow(clippy::too_many_arguments)]
pub fn record_mongo_operation(
    _endpoint_uuid: &EndpointUuid,
    _organization_uuid: &str,
    _facts: &MongoRequestFacts,
    _response_facts: Option<&MongoResponseFacts>,
    _latency_us: u64,
    _is_error: bool,
    _response_bytes: u32,
    _user_uuid: Option<&str>,
) {
}

#[cfg(feature = "redis")]
pub fn extract_redis_request_facts(_request: &dyn EpRequest) -> Option<RedisRequestFacts> {
    None
}

#[cfg(feature = "redis")]
pub fn record_redis_operation(
    _endpoint_uuid: &EndpointUuid,
    _organization_uuid: &str,
    _facts: &RedisRequestFacts,
    _latency_us: u64,
    _is_error: bool,
    _response_bytes: u32,
    _user_uuid: Option<&str>,
) {
}

#[cfg(feature = "postgres")]
pub fn extract_postgres_request_facts(_request: &dyn EpRequest) -> Option<PostgresRequestFacts> {
    None
}

#[cfg(feature = "postgres")]
pub fn record_postgres_operation(
    _endpoint_uuid: &EndpointUuid,
    _organization_uuid: &str,
    _facts: &PostgresRequestFacts,
    _latency_us: u64,
    _is_error: bool,
    _response_bytes: u32,
    _user_uuid: Option<&str>,
) {
}
