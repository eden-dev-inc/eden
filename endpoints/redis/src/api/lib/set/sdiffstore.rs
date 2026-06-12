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

const API_INFO: ApiInfo<RedisApi, SdiffstoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Sdiffstore,
    "Stores the difference of multiple sets in a key",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `SDIFFSTORE`
/// https://redis.io/docs/latest/commands/sdiffstore/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SdiffstoreInput {
    destination: RedisKey,
    keys: Vec<RedisKey>,
}

impl SdiffstoreInput {
    pub fn new(destination: impl Into<RedisKey>, keys: Vec<impl Into<RedisKey>>) -> Self {
        Self {
            destination: destination.into(),
            keys: keys.into_iter().map(|k| k.into()).collect(),
        }
    }
}

impl Serialize for SdiffstoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SdiffstoreInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("destination", &self.destination)?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(
   SdiffstoreInput,
    API_INFO,
    {destination, keys }
);

impl RedisCommandInput for SdiffstoreInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        let mut keys = self.keys.clone();
        keys.push(self.destination.clone());
        keys
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.destination).arg(&self.keys);

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("SDIFFSTORE requires at least 2 arguments, given {}", args.len())));
        }

        let mut keys = vec![];
        for key in args.iter().skip(1) {
            keys.push(key.try_into()?);
        }

        Ok(Self { destination: args[0].clone().try_into()?, keys })
    }
}

/// Output for Redis SDIFFSTORE command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SdiffstoreOutput {
    count: i64,
}

impl SdiffstoreOutput {
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
            _ => return Err(EpError::parse("SDIFFSTORE must return integer")),
        };

        Ok(Self { count })
    }
}

impl Serialize for SdiffstoreOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SdiffstoreOutput", 1)?;
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
            let input = SdiffstoreInput::new(
                RedisKey::String("dest".into()),
                vec![RedisKey::String("set1".into()), RedisKey::String("set2".into())],
            );
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SDIFFSTORE"));
            assert!(cmd_str.contains("dest"));
            assert!(cmd_str.contains("set1"));
            assert!(cmd_str.contains("set2"));
        }

        #[test]
        fn test_decode_output_positive() {
            let output = SdiffstoreOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.count(), 3);
        }

        #[test]
        fn test_decode_output_zero() {
            let output = SdiffstoreOutput::decode(b":0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error() {
            let err = SdiffstoreOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("set1".into()),
                RedisJsonValue::String("set2".into()),
            ];
            let input = SdiffstoreInput::decode(args).unwrap();
            assert_eq!(input.destination, RedisKey::String("dest".into()));
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_keys_includes_destination() {
            let input = SdiffstoreInput::new(RedisKey::String("dest".into()), vec![RedisKey::String("set1".into())]);
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert!(keys.contains(&RedisKey::String("dest".into())));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sdiffstore_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nsds_set1\r\n").await.expect("del");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nsds_set2\r\n").await.expect("del");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nsds_dest\r\n").await.expect("del");

                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$8\r\nsds_set1\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").await.expect("sadd");
                    ctx.raw(b"*3\r\n$4\r\nSADD\r\n$8\r\nsds_set2\r\n$1\r\nb\r\n").await.expect("sadd");

                    let result = ctx
                        .raw(
                            &SdiffstoreInput::new(
                                RedisKey::String("sds_dest".into()),
                                vec![RedisKey::String("sds_set1".into()), RedisKey::String("sds_set2".into())],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SdiffstoreOutput::decode(&result).expect("decode");
                    assert_eq!(output.count(), 2);
                })
            })
            .await;
        }
    }
}
