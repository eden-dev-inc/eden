use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, SinterstoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Sinterstore,
    "Stores the intersect of multiple sets in a key",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `SINTERSTORE`
/// https://redis.io/docs/latest/commands/sinterstore/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SinterstoreInput {
    destination: RedisKey,
    keys: Vec<RedisKey>,
}

impl Serialize for SinterstoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SinterstoreInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("destination", &self.destination)?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(
   SinterstoreInput,
    API_INFO,
    { destination, keys }
);

impl RedisCommandInput for SinterstoreInput {
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
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("SINTERSTORE requires at least 2 arguments, given {}", args.len())));
        }

        let mut keys = vec![];
        for key in args.iter().skip(1) {
            keys.push(key.try_into()?);
        }

        Ok(Self { destination: args[0].clone().try_into()?, keys })
    }
}

/// Output for Redis SINTERSTORE command
///
/// Returns the number of elements in the resulting set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SinterstoreOutput {
    count: i64,
}

impl SinterstoreOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the number of elements in the resulting set
    pub fn count(&self) -> i64 {
        self.count
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => n,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => data,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("expected integer response")),
        };

        Ok(Self { count })
    }
}

impl Serialize for SinterstoreOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SinterstoreOutput", 1)?;
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
        fn test_encode_command_basic() {
            let input = SinterstoreInput {
                destination: RedisKey::String("dest".into()),
                keys: vec![RedisKey::String("set1".into()), RedisKey::String("set2".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SINTERSTORE"));
            assert!(cmd_str.contains("dest"));
            assert!(cmd_str.contains("set1"));
            assert!(cmd_str.contains("set2"));
        }

        #[test]
        fn test_decode_output_zero() {
            let output = SinterstoreOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_output_positive() {
            let output = SinterstoreOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.count(), 5);
        }

        #[test]
        fn test_decode_error() {
            let err = SinterstoreOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("set1".into()),
                RedisJsonValue::String("set2".into()),
            ];
            let input = SinterstoreInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("dest".into())];
            let err = SinterstoreInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_keys_includes_destination() {
            let input = SinterstoreInput {
                destination: RedisKey::String("dest".into()),
                keys: vec![RedisKey::String("set1".into())],
            };
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
        async fn test_sinterstore_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Clean up keys
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsinterstoretest1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsinterstoretest2\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nsinterstoreresult\r\n").await.expect("raw failed");

                    // SADD sinterstoretest1 a b c
                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$16\r\nsinterstoretest1\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").await.expect("raw failed");

                    // SADD sinterstoretest2 b c d
                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$16\r\nsinterstoretest2\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SinterstoreInput {
                                destination: RedisKey::String("sinterstoreresult".into()),
                                keys: vec![
                                    RedisKey::String("sinterstoretest1".into()),
                                    RedisKey::String("sinterstoretest2".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SinterstoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 2); // b, c
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sinterstore_empty_intersection() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nsistore_empty_dest\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsistore_empty_s1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsistore_empty_s2\r\n").await.expect("raw failed");

                    // SADD sistore_empty_s1 a
                    ctx.raw(b"*3\r\n$4\r\nSADD\r\n$16\r\nsistore_empty_s1\r\n$1\r\na\r\n").await.expect("raw failed");

                    // SADD sistore_empty_s2 b
                    ctx.raw(b"*3\r\n$4\r\nSADD\r\n$16\r\nsistore_empty_s2\r\n$1\r\nb\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SinterstoreInput {
                                destination: RedisKey::String("sistore_empty_dest".into()),
                                keys: vec![
                                    RedisKey::String("sistore_empty_s1".into()),
                                    RedisKey::String("sistore_empty_s2".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SinterstoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sinterstore_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set a string value
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$13\r\nsistore_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SinterstoreInput {
                                destination: RedisKey::String("sistore_dest".into()),
                                keys: vec![RedisKey::String("sistore_wrong".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = SinterstoreOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sinterstore_multiple_sets() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nsistore_multi_dest\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsistore_multi_s1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsistore_multi_s2\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsistore_multi_s3\r\n").await.expect("raw failed");

                    // SADD sistore_multi_s1 a b c d
                    ctx.raw(b"*6\r\n$4\r\nSADD\r\n$16\r\nsistore_multi_s1\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n")
                        .await
                        .expect("raw failed");

                    // SADD sistore_multi_s2 b c d e
                    ctx.raw(b"*6\r\n$4\r\nSADD\r\n$16\r\nsistore_multi_s2\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n")
                        .await
                        .expect("raw failed");

                    // SADD sistore_multi_s3 c d e f
                    ctx.raw(b"*6\r\n$4\r\nSADD\r\n$16\r\nsistore_multi_s3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &SinterstoreInput {
                                destination: RedisKey::String("sistore_multi_dest".into()),
                                keys: vec![
                                    RedisKey::String("sistore_multi_s1".into()),
                                    RedisKey::String("sistore_multi_s2".into()),
                                    RedisKey::String("sistore_multi_s3".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SinterstoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 2); // c, d
                })
            })
            .await;
        }
    }
}
