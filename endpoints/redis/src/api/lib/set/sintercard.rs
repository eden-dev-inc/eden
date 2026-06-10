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

const API_INFO: ApiInfo<RedisApi, SintercardInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Sintercard,
    "Returns the number of members of the intersect of multiple sets",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `SINTERCARD`
/// https://redis.io/docs/latest/commands/sintercard/
///
/// Note: SINTERCARD was introduced in Redis 7.0.0.
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SintercardInput {
    numkeys: RedisJsonValue,
    keys: Vec<RedisKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<RedisJsonValue>,
}

impl SintercardInput {
    pub fn new(keys: Vec<impl Into<RedisKey>>) -> Self {
        let keys: Vec<RedisKey> = keys.into_iter().map(|k| k.into()).collect();
        let numkeys = RedisJsonValue::Integer(keys.len() as i64);
        Self { numkeys, keys, limit: None }
    }

    pub fn with_limit(mut self, limit: impl Into<RedisJsonValue>) -> Self {
        self.limit = Some(limit.into());
        self
    }
}

impl Serialize for SintercardInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.limit.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("SintercardInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("numkeys", &self.numkeys)?;
        state.serialize_field("keys", &self.keys)?;
        if let Some(limit) = &self.limit {
            state.serialize_field("limit", limit)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    SintercardInput,
    API_INFO,
    {numkeys, keys, limit}
);

impl RedisCommandInput for SintercardInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.numkeys);
        for key in &self.keys {
            command.arg(key);
        }

        if let Some(limit) = &self.limit {
            command.arg("LIMIT").arg(limit);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("SINTERCARD requires at least numkeys argument"));
        }

        let numkeys = args[0].clone();
        let num_keys = match &numkeys {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("numkeys must be an integer"))?,
            _ => return Err(EpError::parse("numkeys must be integer")),
        };

        if args.len() < 1 + num_keys {
            return Err(EpError::request("Insufficient keys"));
        }

        let mut keys = Vec::new();
        for i in 0..num_keys {
            keys.push(args[1 + i].clone().try_into()?);
        }

        let mut limit = None;
        let mut i = 1 + num_keys;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i]
                && s.to_uppercase() == "LIMIT"
                && i + 1 < args.len()
            {
                limit = Some(args[i + 1].clone());
                i += 2;
                continue;
            }
            i += 1;
        }

        Ok(Self { numkeys, keys, limit })
    }
}

/// Output for Redis SINTERCARD command
///
/// Returns the cardinality (number of elements) of the intersection.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SintercardOutput {
    cardinality: i64,
}

impl SintercardOutput {
    pub fn new(cardinality: i64) -> Self {
        Self { cardinality }
    }

    /// Get the cardinality of the intersection
    pub fn cardinality(&self) -> i64 {
        self.cardinality
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let cardinality = match frame {
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

        Ok(Self { cardinality })
    }
}

impl Serialize for SintercardOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SintercardOutput", 1)?;
        state.serialize_field("cardinality", &self.cardinality)?;
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
            let input = SintercardInput::new(vec![RedisKey::String("set1".into()), RedisKey::String("set2".into())]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SINTERCARD"));
            assert!(cmd_str.contains("2")); // numkeys
        }

        #[test]
        fn test_encode_command_with_limit() {
            let input = SintercardInput::new(vec![RedisKey::String("set1".into()), RedisKey::String("set2".into())]).with_limit(10);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LIMIT"));
            assert!(cmd_str.contains("10"));
        }

        #[test]
        fn test_decode_output_zero() {
            let output = SintercardOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.cardinality(), 0);
        }

        #[test]
        fn test_decode_output_positive() {
            let output = SintercardOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.cardinality(), 5);
        }

        #[test]
        fn test_decode_error() {
            let err = SintercardOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::Integer(2),
                RedisJsonValue::String("set1".into()),
                RedisJsonValue::String("set2".into()),
            ];
            let input = SintercardInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_with_limit() {
            let args = vec![
                RedisJsonValue::Integer(2),
                RedisJsonValue::String("set1".into()),
                RedisJsonValue::String("set2".into()),
                RedisJsonValue::String("LIMIT".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = SintercardInput::decode(args).unwrap();
            assert_eq!(input.limit, Some(RedisJsonValue::Integer(10)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::Integer(2)];
            let err = SintercardInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Insufficient"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = SintercardInput::new(vec![RedisKey::String("set1".into()), RedisKey::String("set2".into())]);
            assert_eq!(input.keys().len(), 2);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sintercard_basic() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nsintercard_set1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nsintercard_set2\r\n").await.expect("raw failed");

                    // SADD sintercard_set1 a b c
                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$15\r\nsintercard_set1\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").await.expect("raw failed");

                    // SADD sintercard_set2 b c d
                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$15\r\nsintercard_set2\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SintercardInput::new(vec![
                                RedisKey::String("sintercard_set1".into()),
                                RedisKey::String("sintercard_set2".into()),
                            ])
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SintercardOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.cardinality(), 2); // b, c
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sintercard_with_limit() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nsintercard_lim_s1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nsintercard_lim_s2\r\n").await.expect("raw failed");

                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$17\r\nsintercard_lim_s1\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$17\r\nsintercard_lim_s2\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &SintercardInput::new(vec![
                                RedisKey::String("sintercard_lim_s1".into()),
                                RedisKey::String("sintercard_lim_s2".into()),
                            ])
                            .with_limit(1)
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SintercardOutput::decode(&result).expect("decode failed");
                    // LIMIT 1 should stop counting at 1
                    assert_eq!(output.cardinality(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sintercard_empty_intersection() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$19\r\nsintercard_empty_s1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$19\r\nsintercard_empty_s2\r\n").await.expect("raw failed");

                    ctx.raw(b"*3\r\n$4\r\nSADD\r\n$19\r\nsintercard_empty_s1\r\n$1\r\na\r\n").await.expect("raw failed");

                    ctx.raw(b"*3\r\n$4\r\nSADD\r\n$19\r\nsintercard_empty_s2\r\n$1\r\nb\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SintercardInput::new(vec![
                                RedisKey::String("sintercard_empty_s1".into()),
                                RedisKey::String("sintercard_empty_s2".into()),
                            ])
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SintercardOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.cardinality(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sintercard_nonexistent_keys() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nsintercard_nokey1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nsintercard_nokey2\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SintercardInput::new(vec![
                                RedisKey::String("sintercard_nokey1".into()),
                                RedisKey::String("sintercard_nokey2".into()),
                            ])
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SintercardOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.cardinality(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sintercard_wrongtype() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$16\r\nsintercard_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&SintercardInput::new(vec![RedisKey::String("sintercard_wrong".into())]).command())
                        .await
                        .expect("raw failed");

                    let err = SintercardOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
