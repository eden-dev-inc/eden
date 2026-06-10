use std::any::Any;
use std::io::{self, Read};

use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::{EndpointOperation, EpRequest, Operation, OperationExecutor, RequestConstructor, RunOutput, RunRequest};
use ep_core::define_request_serializer_stuff;
use ep_core::settings::EdenSettings;
use ep_core::{EndpointType, ReqType};
use ep_redis::request::RedisRequest;
use ep_redis::serde::RedisOperation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use redis_core::{RedisAsync, RedisTx};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use telemetry::TelemetryWrapper;

use crate::api::control_plane::ElasticacheApi;
use crate::control_plane_ep::ElasticacheControlPlaneEp;
use crate::serde::ElasticacheOperation;

#[derive(Debug)]
pub struct ElasticacheRequest(pub Box<dyn ElasticacheOperation>);

#[derive(Debug)]
pub struct ElasticacheRedisOperation {
    inner: Box<dyn RedisOperation>,
}

impl ElasticacheRedisOperation {
    pub fn new(inner: Box<dyn RedisOperation>) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &dyn RedisOperation {
        &*self.inner
    }

    pub fn inner_clone(&self) -> Box<dyn RedisOperation> {
        let value = serde_json::to_value(&self.inner).expect("serialize redis operation");
        serde_json::from_value(value).expect("deserialize redis operation")
    }
}

impl From<Box<dyn RedisOperation>> for ElasticacheRequest {
    fn from(op: Box<dyn RedisOperation>) -> Self {
        ElasticacheRequest(Box::new(ElasticacheRedisOperation::new(op)))
    }
}

impl From<RedisRequest> for ElasticacheRequest {
    fn from(req: RedisRequest) -> Self {
        req.0.into()
    }
}

impl ElasticacheRequest {
    pub fn redis_operation(&self) -> Option<&ElasticacheRedisOperation> {
        self.0.as_any().downcast_ref::<ElasticacheRedisOperation>()
    }

    pub fn as_redis_request(&self) -> Option<RedisRequest> {
        self.redis_operation().map(|op| RedisRequest(op.inner_clone()))
    }
}

impl RequestConstructor for ElasticacheRequest {
    type AsyncType = RedisAsync;
    type ApiKindType = ElasticacheApi;
    type TxType = RedisTx;
    type OperationType = dyn ElasticacheOperation;

    fn new(op: Box<Self::OperationType>) -> Self {
        Self(op)
    }
}

impl EndpointType for ElasticacheRequest {
    fn r#type() -> EpKind {
        EpKind::Elasticache
    }
}

impl RunRequest<RedisAsync, ElasticacheApi, RedisTx> for ElasticacheRequest {
    fn operation(&self) -> &dyn Operation<RedisAsync, ElasticacheApi, RedisTx> {
        &*self.0
    }

    async fn run_request(
        &self,
        context: RedisAsync,
        _settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Box<dyn ep_core::EpOutput>> {
        match self.operation().kind() {
            ElasticacheApi::Redis => match self.operation().as_exec() {
                Some(exec) => exec.run_operation_request(context, telemetry_wrapper.clone()).await,
                None => Err(EpError::database("Operation does not implement OperationExecutor")),
            },
            _ => {
                let control_plane = ElasticacheControlPlaneEp::new();
                control_plane.run(self.operation(), &context, telemetry_wrapper).await
            }
        }
    }
}

impl EpRequest for ElasticacheRequest {
    fn kind(&self) -> EpKind {
        EpKind::Elasticache
    }

    fn as_request(self: Box<Self>) -> Box<dyn EpRequest> {
        self
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_value(&self) -> serde_json::Result<Value> {
        serde_json::to_value(self)
    }

    fn borsh_serialize(&self, writer: &mut dyn io::Write) -> io::Result<()> {
        struct WriteWrapper<'a>(&'a mut dyn io::Write);

        impl<'a> io::Write for WriteWrapper<'a> {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.write(buf)
            }

            fn flush(&mut self) -> io::Result<()> {
                self.0.flush()
            }
        }

        BorshSerialize::serialize(self, &mut WriteWrapper(writer))
    }
}

impl Serialize for ElasticacheRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(redis) = self.redis_operation() {
            return Serialize::serialize(&RedisRequest(redis.inner_clone()), serializer);
        }

        Serialize::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for ElasticacheRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;

        if let Ok(redis_req) = serde_json::from_value::<RedisRequest>(value.clone()) {
            return Ok(redis_req.into());
        }

        let op: Box<dyn ElasticacheOperation> = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        Ok(ElasticacheRequest(op))
    }
}

impl BorshSerialize for ElasticacheRequest {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        if let Some(redis) = self.redis_operation() {
            return BorshSerialize::serialize(&RedisRequest(redis.inner_clone()), writer);
        }

        BorshSerialize::serialize(&self.0, writer)
    }
}

impl BorshDeserialize for ElasticacheRequest {
    fn deserialize(buf: &mut &[u8]) -> io::Result<Self> {
        let buffer = buf.to_vec();

        if let Ok(redis_req) = borsh::from_slice::<RedisRequest>(&buffer) {
            *buf = &[];
            return Ok(redis_req.into());
        }

        let op: Box<dyn ElasticacheOperation> = borsh::from_slice(&buffer)?;
        *buf = &[];
        Ok(ElasticacheRequest(op))
    }

    fn deserialize_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        let mut slice = buffer.as_slice();
        BorshDeserialize::deserialize(&mut slice)
    }
}

impl Operation<RedisAsync, ElasticacheApi, RedisTx> for ElasticacheRedisOperation {
    fn kind(&self) -> ElasticacheApi {
        ElasticacheApi::Redis
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn request_type(&self) -> ReqType {
        self.inner.request_type()
    }

    fn as_operation(self: Box<Self>) -> Box<dyn Operation<RedisAsync, ElasticacheApi, RedisTx>> {
        self
    }

    fn as_exec(&self) -> Option<&dyn OperationExecutor<RedisAsync, ElasticacheApi, RedisTx>> {
        Some(self)
    }

    fn clone_box(&self) -> Box<dyn Operation<RedisAsync, ElasticacheApi, RedisTx>> {
        Box::new(Self { inner: self.inner_clone() })
    }
}

impl EndpointOperation for ElasticacheRedisOperation {}

impl OperationExecutor<RedisAsync, ElasticacheApi, RedisTx> for ElasticacheRedisOperation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn run_operation_request(&self, context: RedisAsync, telemetry_wrapper: TelemetryWrapper) -> RunOutput<'_> {
        match self.inner.as_exec() {
            Some(exec) => exec.run_operation_request(context, telemetry_wrapper),
            None => Box::pin(async { Err(EpError::database("Operation does not implement OperationExecutor")) }),
        }
    }

    fn run_operation_transaction(&self, tx_context: &mut RedisTx, telemetry_wrapper: &mut TelemetryWrapper) {
        if let Some(exec) = self.inner.as_exec() {
            exec.run_operation_transaction(tx_context, telemetry_wrapper);
        }
    }
}

define_request_serializer_stuff!(EpKind::Elasticache => ElasticacheRequest);
