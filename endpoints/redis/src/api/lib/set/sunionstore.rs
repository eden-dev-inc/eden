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

const API_INFO: ApiInfo<RedisApi, SunionstoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Sunionstore,
    "Stores the union of multiple sets in a key",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `SUNIONSTORE`
/// https://redis.io/docs/latest/commands/sunionstore/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SunionstoreInput {
    destination: RedisKey,
    keys: Vec<RedisKey>,
}

impl SunionstoreInput {
    pub fn new(destination: impl Into<RedisKey>, keys: Vec<impl Into<RedisKey>>) -> Self {
        let keys: Vec<RedisKey> = keys.into_iter().map(|k| k.into()).collect();
        Self { destination: destination.into(), keys }
    }
}

impl Serialize for SunionstoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SunionstoreInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("destination", &self.destination)?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(
    SunionstoreInput,
    API_INFO,
    {destination, keys }
);

impl RedisCommandInput for SunionstoreInput {
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
        if args.is_empty() {
            return Err(EpError::request("SUNIONSTORE requires at least one argument, given None"));
        }

        let mut keys = vec![];
        for key in args.iter().skip(1) {
            keys.push(key.try_into()?);
        }

        Ok(Self { destination: args[0].clone().try_into()?, keys })
    }
}

/// Output for Redis SUNIONSTORE command
///
/// Returns the number of elements in the resulting set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SunionstoreOutput {
    count: i64,
}

impl SunionstoreOutput {
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

impl Serialize for SunionstoreOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SunionstoreOutput", 1)?;
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
            let input = SunionstoreInput::new(
                RedisKey::String("dest".into()),
                vec![RedisKey::String("set1".into()), RedisKey::String("set2".into())],
            );
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SUNIONSTORE"));
            assert!(cmd_str.contains("dest"));
        }

        #[test]
        fn test_decode_output() {
            let output = SunionstoreOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.count(), 3);
        }

        #[test]
        fn test_decode_output_zero() {
            let output = SunionstoreOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_error() {
            let err = SunionstoreOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("set1".into()),
                RedisJsonValue::String("set2".into()),
            ];
            let input = SunionstoreInput::decode(args).unwrap();
            assert_eq!(input.destination, RedisKey::String("dest".into()));
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_args() {
            let args = vec![];
            let err = SunionstoreInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least one argument"));
        }

        #[test]
        fn test_keys_includes_destination() {
            let input = SunionstoreInput::new(RedisKey::String("dest".into()), vec![RedisKey::String("src".into())]);
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert!(keys.contains(&RedisKey::String("dest".into())));
            assert!(keys.contains(&RedisKey::String("src".into())));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunionstore_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Clean up keys
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsunionstoretest1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsunionstoretest2\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nsunionstoreresult\r\n").await.expect("raw failed");

                    // SADD sunionstoretest1 a b
                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$16\r\nsunionstoretest1\r\n$1\r\na\r\n$1\r\nb\r\n").await.expect("raw failed");

                    // SADD sunionstoretest2 b c
                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$16\r\nsunionstoretest2\r\n$1\r\nb\r\n$1\r\nc\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SunionstoreInput::new(
                                RedisKey::String("sunionstoreresult".into()),
                                vec![
                                    RedisKey::String("sunionstoretest1".into()),
                                    RedisKey::String("sunionstoretest2".into()),
                                ],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SunionstoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 3); // a, b, c
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunionstore_single_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nsustore_single\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsustore_src_sngl\r\n").await.expect("raw failed");

                    // SADD sustore_src_sngl a b c
                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$16\r\nsustore_src_sngl\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SunionstoreInput::new(
                                RedisKey::String("sustore_single".into()),
                                vec![RedisKey::String("sustore_src_sngl".into())],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SunionstoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunionstore_empty_sets() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nsustore_empty_dest\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsustore_empty_s1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsustore_empty_s2\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SunionstoreInput::new(
                                RedisKey::String("sustore_empty_dest".into()),
                                vec![
                                    RedisKey::String("sustore_empty_s1".into()),
                                    RedisKey::String("sustore_empty_s2".into()),
                                ],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SunionstoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunionstore_overlapping_sets() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsustore_overlap1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsustore_overlap2\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nsustore_ovrlp_dest\r\n").await.expect("raw failed");

                    // SADD sustore_overlap1 a b c
                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$16\r\nsustore_overlap1\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").await.expect("raw failed");

                    // SADD sustore_overlap2 b c d
                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$16\r\nsustore_overlap2\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SunionstoreInput::new(
                                RedisKey::String("sustore_ovrlp_dest".into()),
                                vec![
                                    RedisKey::String("sustore_overlap1".into()),
                                    RedisKey::String("sustore_overlap2".into()),
                                ],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SunionstoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 4); // a, b, c, d

                    // Verify result set contains all elements
                    let members_result = ctx.raw(b"*2\r\n$8\r\nSMEMBERS\r\n$18\r\nsustore_ovrlp_dest\r\n").await.expect("raw failed");
                    let members_str = String::from_utf8_lossy(&members_result);
                    assert!(members_str.contains("a"));
                    assert!(members_str.contains("b"));
                    assert!(members_str.contains("c"));
                    assert!(members_str.contains("d"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunionstore_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$13\r\nsustore_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SunionstoreInput::new(RedisKey::String("sustore_dest".into()), vec![RedisKey::String("sustore_wrong".into())])
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = SunionstoreOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
