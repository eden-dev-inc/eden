use crate::api::lib::RedisApi;
use ep_core::{define_operation_types, implement_operation_registry};
use redis_core::{RedisAsync, RedisTx};

define_operation_types!();

implement_operation_registry!(RedisOperation<RedisAsync, RedisApi, RedisTx>);
