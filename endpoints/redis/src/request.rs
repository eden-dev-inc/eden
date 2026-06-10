use crate::RedisOperation;
use crate::api::lib::RedisApi;
use endpoint_types::{EpRequest, Operation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use redis_core::{RedisAsync, RedisTx};

define_request!(EpKind::Redis => Redis, RedisOperation, RedisAsync, RedisApi, RedisTx);

define_request_serializer_stuff!(EpKind::Redis => RedisRequest);
