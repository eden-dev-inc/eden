use ep_core::{EndpointOutput, EndpointResponse, EpOutput, ToOutput};
use error::{EpError, ProtocolError, ResultEP};
use format::endpoint::EpKind;
use redis::{FromRedisValue, VerbatimFormat};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use utoipa::openapi::{Object, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

#[derive(ToSchema)]
pub enum RedisEndpointOutput {
    #[schema(title = "Redis empty output")]
    EmptyOutput(EmptyOutput),
    #[schema(title = "Redis value output")]
    ValueOutput(RedisValueOutput),
}

pub trait OutputDowncast {
    fn downcast_ref<T: 'static>(&self) -> Option<&T>;
}

impl<T: EpOutput + ?Sized> OutputDowncast for T {
    fn downcast_ref<U: 'static>(&self) -> Option<&U> {
        self.as_any().downcast_ref::<U>()
    }
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct EmptyOutput(pub ());

impl ToOutput for EmptyOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Redis, EndpointResponse::ok("success"))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    #[allow(clippy::unit_arg)]
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        borsh::to_vec(&self.0).map_err(EpError::serde)
    }
}

#[allow(dead_code)]
pub(crate) struct RedisOutput<RV: FromRedisValue>(pub RV);

impl<'de, RV> Deserialize<'de> for RedisOutput<RV>
where
    RV: FromRedisValue,
{
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(D::Error::custom("cannot deserialize Database"))
    }
}

pub struct RedisValueOutput(pub redis::Value);

impl RedisValueOutput {
    pub fn new(value: redis::Value) -> Self {
        RedisValueOutput(value)
    }
}

impl ToOutput for RedisValueOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Redis, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serialize_redis_value(&self.0)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Database"))
    }
}

fn serialize_redis_value(value: &redis::Value) -> Result<Value, EpError> {
    match value {
        redis::Value::Attribute { data, attributes } => {
            let mut attrs = Vec::with_capacity(attributes.len());
            for (attr, val) in attributes {
                attrs.push((serialize_redis_value(attr)?, serialize_redis_value(val)?));
            }
            serde_json::to_value((serialize_redis_value(data)?, attrs))
        }
        redis::Value::BulkString(v) => serde_json::to_value(std::str::from_utf8(v).map_err(EpError::serde)?),
        redis::Value::Boolean(b) => serde_json::to_value(b),
        redis::Value::Double(d) => serde_json::to_value(d),
        redis::Value::BigNumber(b) => serde_json::to_value(b.to_string()),
        redis::Value::SimpleString(s) => serde_json::to_value(s),
        redis::Value::ServerError(_) => {
            return Err(EpError::database("Redis response of `ServerError`"));
        }
        redis::Value::VerbatimString { format, text } => serde_json::to_value((
            match format {
                VerbatimFormat::Unknown(s) => serde_json::to_value(s).unwrap_or_default(),
                VerbatimFormat::Markdown => serde_json::to_value("Markdown").unwrap_or_default(),
                VerbatimFormat::Text => serde_json::to_value("Text").unwrap_or_default(),
            },
            serde_json::to_value(text).unwrap_or_default(),
        )),
        redis::Value::Int(inner) => Ok(serde_json::to_value(inner).unwrap_or_default()),
        redis::Value::Okay => serde_json::to_value("OK"),
        redis::Value::Nil => return Err(EpError::database("Redis responded with a value 'Nil'")),
        redis::Value::Array(arr) => {
            let mut elts = Vec::with_capacity(arr.len());
            for v in arr {
                elts.push(serialize_redis_value(v)?);
            }
            serde_json::to_value(&elts)
        }
        redis::Value::Map(m) => {
            let mut elts = Vec::with_capacity(m.len());
            for (key, val) in m {
                elts.push((serialize_redis_value(key)?, serialize_redis_value(val)?));
            }
            serde_json::to_value(&elts)
        }
        redis::Value::Set(set) => {
            let mut elts = Vec::with_capacity(set.len());
            for v in set {
                elts.push(serialize_redis_value(v)?);
            }
            serde_json::to_value(&elts)
        }
        redis::Value::Push { kind, data } => {
            let mut elts = Vec::with_capacity(data.len());
            for v in data {
                elts.push(serialize_redis_value(v)?);
            }
            serde_json::to_value((kind.to_string(), elts))
        }
    }
    .map_err(EpError::data)
}

impl ToSchema for RedisValueOutput {}
impl PartialSchema for RedisValueOutput {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(Object::default()))
    }
}
