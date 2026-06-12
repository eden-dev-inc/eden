use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ZdiffstoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zdiffstore,
    "Stores the difference of multiple sorted sets in a key",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `ZDIFFSTORE`
/// https://redis.io/docs/latest/commands/zdiffstore/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZdiffstoreInput {
    destination: RedisKey,
    keys: Vec<RedisKey>,
}

impl ZdiffstoreInput {
    pub fn new(destination: impl Into<RedisKey>, keys: Vec<impl Into<RedisKey>>) -> Self {
        Self {
            destination: destination.into(),
            keys: keys.into_iter().map(|k| k.into()).collect(),
        }
    }
}

impl Serialize for ZdiffstoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ZdiffstoreInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("destination", &self.destination)?;
        state.serialize_field("numkeys", &self.keys.len())?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(ZdiffstoreInput, API_INFO, { destination, keys });

impl RedisCommandInput for ZdiffstoreInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        let mut keys = vec![self.destination.clone()];
        keys.extend(self.keys.clone());
        keys
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.destination).arg(self.keys.len());
        for key in &self.keys {
            command.arg(key);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("ZDIFFSTORE requires at least 3 arguments, given {}", args.len())));
        }

        let destination = args[0].clone().try_into()?;
        let numkeys = match &args[1] {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("numkeys must be a valid integer"))?,
            _ => return Err(EpError::parse("numkeys must be integer")),
        };

        if args.len() < 2 + numkeys {
            return Err(EpError::request("Insufficient keys for ZDIFFSTORE"));
        }

        let mut keys = Vec::new();
        for key in args[2..2 + numkeys].iter() {
            keys.push(key.try_into()?);
        }

        Ok(Self { destination, keys })
    }
}

/// Output for Redis ZDIFFSTORE command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZdiffstoreOutput {
    count: i64,
}

impl ZdiffstoreOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    pub fn count(&self) -> i64 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => n,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => data,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("ZDIFFSTORE must return integer")),
        };

        Ok(Self { count })
    }
}

impl Serialize for ZdiffstoreOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZdiffstoreOutput", 1)?;
        state.serialize_field("count", &self.count)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ZdiffstoreInput::new(
                RedisKey::String("dest".into()),
                vec![RedisKey::String("zset1".into()), RedisKey::String("zset2".into())],
            );
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZDIFFSTORE"));
            assert!(cmd_str.contains("dest"));
            assert!(cmd_str.contains("2"));
        }

        #[test]
        fn test_decode_output_positive() {
            let output = ZdiffstoreOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.count(), 5);
        }

        #[test]
        fn test_decode_output_zero() {
            let output = ZdiffstoreOutput::decode(b":0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error() {
            let err = ZdiffstoreOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::Integer(2),
                RedisJsonValue::String("zset1".into()),
                RedisJsonValue::String("zset2".into()),
            ];
            let input = ZdiffstoreInput::decode(args).unwrap();
            assert_eq!(input.destination, RedisKey::String("dest".into()));
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_keys_includes_destination() {
            let input = ZdiffstoreInput::new(RedisKey::String("dest".into()), vec![RedisKey::String("zset1".into())]);
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], RedisKey::String("dest".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zdiffstore_basic() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nzds_set1\r\n").await.expect("del");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nzds_set2\r\n").await.expect("del");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nzds_dest\r\n").await.expect("del");

                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$8\r\nzds_set1\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n")
                        .await
                        .expect("zadd");
                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$8\r\nzds_set2\r\n$1\r\n2\r\n$1\r\nb\r\n").await.expect("zadd");

                    let result = ctx
                        .raw(
                            &ZdiffstoreInput::new(
                                RedisKey::String("zds_dest".into()),
                                vec![RedisKey::String("zds_set1".into()), RedisKey::String("zds_set2".into())],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZdiffstoreOutput::decode(&result).expect("decode");
                    assert_eq!(output.count(), 2);
                })
            })
            .await;
        }
    }
}
