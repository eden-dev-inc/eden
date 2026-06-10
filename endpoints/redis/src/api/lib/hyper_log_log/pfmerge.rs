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

const API_INFO: ApiInfo<RedisApi, PfmergeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Pfmerge,
    "Merges one or more HyperLogLog values into a single key",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `PFMERGE`
/// https://redis.io/docs/latest/commands/pfmerge/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PfmergeInput {
    pub(crate) destkey: RedisKey,
    pub(crate) sourcekey: Option<Vec<RedisKey>>,
}

impl Serialize for PfmergeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.sourcekey.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("PfmergeInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("destkey", &self.destkey)?;
        if let Some(sourcekey) = &self.sourcekey {
            state.serialize_field("sourcekey", sourcekey)?;
        }
        state.end()
    }
}

impl_redis_operation!(PfmergeInput, API_INFO, { destkey, sourcekey });

impl RedisCommandInput for PfmergeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        let mut keys = vec![self.destkey.clone()];
        if let Some(source_keys) = &self.sourcekey {
            keys.extend(source_keys.clone());
        }
        keys
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.destkey);

        if let Some(source_keys) = &self.sourcekey {
            for key in source_keys {
                command.arg(key);
            }
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("PFMERGE requires at least 1 argument, given none"));
        }

        let destkey = args[0].clone().try_into()?;
        let sourcekey = if args.len() > 1 {
            let keys: Result<Vec<RedisKey>, _> = args[1..].iter().map(|k| k.try_into()).collect();
            Some(keys?)
        } else {
            None
        };

        Ok(Self { destkey, sourcekey })
    }
}

/// Output for Redis PFMERGE command
///
/// PFMERGE always returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PfmergeOutput {
    /// Whether the merge was successful
    success: bool,
}

impl PfmergeOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Returns true if the merge was successful
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a PfmergeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s == b"OK" => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected PFMERGE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data == b"OK" => Ok(Self { success: true }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?)),
                other => Err(EpError::parse(format!("unexpected PFMERGE response: {:?}", other))),
            },
        }
    }
}

impl Serialize for PfmergeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PfmergeOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_destkey_only() {
            let input = PfmergeInput { destkey: RedisKey::String("dest".into()), sourcekey: None };
            assert_eq!(input.command().to_vec(), b"*2\r\n$7\r\nPFMERGE\r\n$4\r\ndest\r\n");
        }

        #[test]
        fn test_encode_command_with_source_keys() {
            let input = PfmergeInput {
                destkey: RedisKey::String("dest".into()),
                sourcekey: Some(vec![RedisKey::String("src1".into()), RedisKey::String("src2".into())]),
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$7\r\nPFMERGE\r\n$4\r\ndest\r\n$4\r\nsrc1\r\n$4\r\nsrc2\r\n");
        }

        #[test]
        fn test_decode_output_ok() {
            let output = PfmergeOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = PfmergeOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_destkey_only() {
            let args = vec![RedisJsonValue::String("dest".into())];
            let input = PfmergeInput::decode(args).unwrap();
            assert_eq!(input.destkey, RedisKey::String("dest".into()));
            assert!(input.sourcekey.is_none());
        }

        #[test]
        fn test_decode_input_with_source_keys() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("src1".into()),
                RedisJsonValue::String("src2".into()),
            ];
            let input = PfmergeInput::decode(args).unwrap();
            assert_eq!(input.destkey, RedisKey::String("dest".into()));
            assert_eq!(input.sourcekey.as_ref().unwrap().len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = PfmergeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = PfmergeInput {
                destkey: RedisKey::String("dest".into()),
                sourcekey: Some(vec![RedisKey::String("src1".into()), RedisKey::String("src2".into())]),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 3);
            assert_eq!(keys[0], RedisKey::String("dest".into()));
            assert_eq!(keys[1], RedisKey::String("src1".into()));
            assert_eq!(keys[2], RedisKey::String("src2".into()));
        }

        #[test]
        fn test_keys_destkey_only() {
            let input = PfmergeInput { destkey: RedisKey::String("dest".into()), sourcekey: None };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("dest".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::{PfaddInput, PfcountInput, PfcountOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfmerge_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*4\r\n$3\r\nDEL\r\n$12\r\npfmerge_src1\r\n$12\r\npfmerge_src2\r\n$12\r\npfmerge_dest\r\n")
                        .await
                        .expect("raw failed");

                    // Add elements to first HLL
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfmerge_src1".into()),
                            elements: Some(vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Add elements to second HLL
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfmerge_src2".into()),
                            elements: Some(vec![RedisJsonValue::String("c".into()), RedisJsonValue::String("d".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Merge
                    let result = ctx
                        .raw(
                            &PfmergeInput {
                                destkey: RedisKey::String("pfmerge_dest".into()),
                                sourcekey: Some(vec![RedisKey::String("pfmerge_src1".into()), RedisKey::String("pfmerge_src2".into())]),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PfmergeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());

                    // Verify count
                    let count_result =
                        ctx.raw(&PfcountInput { keys: vec![RedisKey::String("pfmerge_dest".into())] }.command()).await.expect("raw failed");

                    let count_output = PfcountOutput::decode(&count_result).expect("decode failed");
                    assert_eq!(count_output.cardinality(), 4);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfmerge_with_overlap() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*4\r\n$3\r\nDEL\r\n$13\r\npfmerge_over1\r\n$13\r\npfmerge_over2\r\n$15\r\npfmerge_overdst\r\n")
                        .await
                        .expect("raw failed");

                    // Add overlapping elements
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfmerge_over1".into()),
                            elements: Some(vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfmerge_over2".into()),
                            elements: Some(vec![RedisJsonValue::String("b".into()), RedisJsonValue::String("c".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Merge
                    let result = ctx
                        .raw(
                            &PfmergeInput {
                                destkey: RedisKey::String("pfmerge_overdst".into()),
                                sourcekey: Some(vec![RedisKey::String("pfmerge_over1".into()), RedisKey::String("pfmerge_over2".into())]),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PfmergeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());

                    // Verify: a, b, c = 3 unique elements
                    let count_result = ctx
                        .raw(&PfcountInput { keys: vec![RedisKey::String("pfmerge_overdst".into())] }.command())
                        .await
                        .expect("raw failed");

                    let count_output = PfcountOutput::decode(&count_result).expect("decode failed");
                    assert_eq!(count_output.cardinality(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfmerge_destkey_only_creates_empty() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\npfmerge_destonly\r\n").await.expect("raw failed");

                    // PFMERGE with destkey only creates empty HLL
                    let result = ctx
                        .raw(
                            &PfmergeInput {
                                destkey: RedisKey::String("pfmerge_destonly".into()),
                                sourcekey: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PfmergeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());

                    // Verify it's empty
                    let count_result = ctx
                        .raw(&PfcountInput { keys: vec![RedisKey::String("pfmerge_destonly".into())] }.command())
                        .await
                        .expect("raw failed");

                    let count_output = PfcountOutput::decode(&count_result).expect("decode failed");
                    assert_eq!(count_output.cardinality(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfmerge_into_existing() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nDEL\r\n$16\r\npfmerge_existdst\r\n$16\r\npfmerge_existsrc\r\n").await.expect("raw failed");

                    // Create dest with initial elements
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfmerge_existdst".into()),
                            elements: Some(vec![RedisJsonValue::String("x".into()), RedisJsonValue::String("y".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Create source
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfmerge_existsrc".into()),
                            elements: Some(vec![RedisJsonValue::String("z".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Merge into existing dest
                    let result = ctx
                        .raw(
                            &PfmergeInput {
                                destkey: RedisKey::String("pfmerge_existdst".into()),
                                sourcekey: Some(vec![RedisKey::String("pfmerge_existsrc".into())]),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PfmergeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());

                    // Verify: x, y, z = 3 unique elements
                    let count_result = ctx
                        .raw(&PfcountInput { keys: vec![RedisKey::String("pfmerge_existdst".into())] }.command())
                        .await
                        .expect("raw failed");

                    let count_output = PfcountOutput::decode(&count_result).expect("decode failed");
                    assert_eq!(count_output.cardinality(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfmerge_wrong_type_error() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create a string key
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$17\r\npfmerge_wrongtype\r\n$5\r\nhello\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &PfmergeInput {
                                destkey: RedisKey::String("pfmerge_wrongtype".into()),
                                sourcekey: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = PfmergeOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"), "should fail with WRONGTYPE error");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfmerge_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\npfmerge_r2\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &PfmergeInput {
                        destkey: RedisKey::String("pfmerge_r2".into()),
                        sourcekey: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string format");
            let output = PfmergeOutput::decode(&result).expect("decode failed");
            assert!(output.is_success());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfmerge_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\npfmerge_r3\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &PfmergeInput {
                        destkey: RedisKey::String("pfmerge_r3".into()),
                        sourcekey: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string format");
            let output = PfmergeOutput::decode(&result).expect("decode failed");
            assert!(output.is_success());

            ctx.stop().await;
        }
    }
}
