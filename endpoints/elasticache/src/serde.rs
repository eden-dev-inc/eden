use crate::api::control_plane::ElasticacheApi;
use ep_core::{define_operation_types, implement_operation_registry};
use redis_core::{RedisAsync, RedisTx};

define_operation_types!();

implement_operation_registry!(ElasticacheOperation<RedisAsync, ElasticacheApi, RedisTx>);
