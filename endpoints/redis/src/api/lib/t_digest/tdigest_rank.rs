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

const API_INFO: ApiInfo<RedisApi, TdigestRankInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TdigestRank,
    "Returns, for each input value (floating-point), the estimated rank of the value (the number of observations in the sketch that are smaller than the value + half the number of observations that are equal to the value)",
    ReqType::Read,
    true,
);

/// Input for Redis `TDIGEST.RANK` command.
///
/// Returns, for each input value, the estimated rank of the value.
///
/// See official Redis documentation for `TDIGEST.RANK`:
/// https://redis.io/docs/latest/commands/tdigest.rank/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TdigestRankInput {
    /// The key name for the t-digest sketch
    pub(crate) key: RedisKey,
    /// One or more values to query ranks for
    pub(crate) value: Vec<RedisJsonValue>,
}

impl Serialize for TdigestRankInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestRankInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(
    TdigestRankInput,
    API_INFO,
    {key, value}
);

impl RedisCommandInput for TdigestRankInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);
        for v in &self.value {
            command.arg(v);
        }
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!(
                "TDIGEST.RANK requires at least 2 arguments (key, value...), given {}",
                args.len()
            )));
        }

        let key = args[0].clone().try_into()?;
        let value = args[1..].to_vec();

        if value.is_empty() {
            return Err(EpError::parse("TDIGEST.RANK requires at least one value to query"));
        }

        Ok(TdigestRankInput { key, value })
    }
}

/// Output for Redis `TDIGEST.RANK` command.
///
/// Contains the estimated ranks for each queried value.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TdigestRankOutput {
    /// Estimated ranks for each value (may be -1 for values below minimum)
    ranks: Vec<i64>,
}

impl TdigestRankOutput {
    pub fn new(ranks: Vec<i64>) -> Self {
        Self { ranks }
    }

    /// Get the estimated ranks
    pub fn ranks(&self) -> &[i64] {
        &self.ranks
    }

    /// Get the number of ranks
    pub fn len(&self) -> usize {
        self.ranks.len()
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.ranks.is_empty()
    }

    /// Decode the Redis protocol response into a TdigestRankOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let ranks = Self::parse_frame(frame)?;
        Ok(Self { ranks })
    }

    fn parse_frame(frame: DecoderRespFrame) -> Result<Vec<i64>, EpError> {
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3(resp3_frame),
        }
    }

    fn parse_resp2(frame: Resp2Frame) -> Result<Vec<i64>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut ranks = Vec::with_capacity(arr.len());
                for item in arr {
                    ranks.push(Self::parse_resp2_int(&item)?);
                }
                Ok(ranks)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected TDIGEST.RANK response: {:?}", other))),
        }
    }

    fn parse_resp2_int(frame: &Resp2Frame) -> Result<i64, EpError> {
        match frame {
            Resp2Frame::Integer(n) => Ok(*n),
            Resp2Frame::BulkString(data) => {
                let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                s.parse().map_err(EpError::parse)
            }
            Resp2Frame::Null => Ok(-1),
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }

    fn parse_resp3(frame: Resp3Frame) -> Result<Vec<i64>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut ranks = Vec::with_capacity(data.len());
                for item in data {
                    ranks.push(Self::parse_resp3_int(&item)?);
                }
                Ok(ranks)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected TDIGEST.RANK response: {:?}", other))),
        }
    }

    fn parse_resp3_int(frame: &Resp3Frame) -> Result<i64, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(*data),
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                s.parse::<i64>().map_err(EpError::parse)
            }
            Resp3Frame::Null => Ok(-1),
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }
}

impl Serialize for TdigestRankOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestRankOutput", 1)?;
        state.serialize_field("ranks", &self.ranks)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_value() {
            let input = TdigestRankInput {
                key: RedisKey::String("td".into()),
                value: vec![RedisJsonValue::Float(1.5)],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.RANK"));
            assert!(cmd_str.contains("td"));
        }

        #[test]
        fn test_encode_command_multiple_values() {
            let input = TdigestRankInput {
                key: RedisKey::String("td".into()),
                value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(2.0), RedisJsonValue::Float(3.0)],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.RANK"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = TdigestRankOutput::decode(b"*3\r\n:0\r\n:1\r\n:2\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.ranks()[0], 0);
            assert_eq!(output.ranks()[1], 1);
            assert_eq!(output.ranks()[2], 2);
        }

        #[test]
        fn test_decode_output_negative_rank() {
            let output = TdigestRankOutput::decode(b"*1\r\n:-1\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.ranks()[0], -1);
        }

        #[test]
        fn test_decode_output_error() {
            let err = TdigestRankOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Float(1.5),
                RedisJsonValue::Float(2.5),
            ];
            let input = TdigestRankInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.value.len(), 2);
        }

        #[test]
        fn test_decode_input_missing_value() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = TdigestRankInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TdigestRankInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = TdigestRankInput {
                key: RedisKey::String("mykey".into()),
                value: vec![RedisJsonValue::Float(1.0)],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_serialize_output() {
            let output = TdigestRankOutput::new(vec![0, 1, 2]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("ranks"));
        }

        #[test]
        fn test_new_output() {
            let output = TdigestRankOutput::new(vec![5, 10]);
            assert_eq!(output.len(), 2);
            assert!(!output.is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::t_digest::tdigest_add::TdigestAddInput;
        use crate::api::lib::t_digest::tdigest_create::TdigestCreateInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_rank_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_rank_test".into()),
                                compression: None,
                            }
                            .command(),
                        )
                        .await
                    else {
                        return;
                    };

                    if create_result.starts_with(b"-") {
                        return;
                    }

                    ctx.raw(
                        &TdigestAddInput {
                            key: RedisKey::String("td_rank_test".into()),
                            value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(2.0), RedisJsonValue::Float(3.0)],
                        }
                        .command(),
                    )
                    .await
                    .expect("add failed");

                    let result = ctx
                        .raw(
                            &TdigestRankInput {
                                key: RedisKey::String("td_rank_test".into()),
                                value: vec![RedisJsonValue::Float(2.0)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TdigestRankOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty());
                    // Rank should be >= 0
                    assert!(output.ranks()[0] >= 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_rank_multiple_values() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_rank_multi".into()),
                                compression: None,
                            }
                            .command(),
                        )
                        .await
                    else {
                        return;
                    };

                    if create_result.starts_with(b"-") {
                        return;
                    }

                    ctx.raw(
                        &TdigestAddInput {
                            key: RedisKey::String("td_rank_multi".into()),
                            value: vec![
                                RedisJsonValue::Float(1.0),
                                RedisJsonValue::Float(2.0),
                                RedisJsonValue::Float(3.0),
                                RedisJsonValue::Float(4.0),
                                RedisJsonValue::Float(5.0),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("add failed");

                    let result = ctx
                        .raw(
                            &TdigestRankInput {
                                key: RedisKey::String("td_rank_multi".into()),
                                value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(3.0), RedisJsonValue::Float(5.0)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TdigestRankOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    // Ranks should be monotonically increasing for increasing values
                    assert!(output.ranks()[0] <= output.ranks()[1]);
                    assert!(output.ranks()[1] <= output.ranks()[2]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_rank_nonexistent_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TdigestRankInput {
                                key: RedisKey::String("nonexistent_td".into()),
                                value: vec![RedisJsonValue::Float(1.0)],
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(result) = result
                        && result.starts_with(b"-")
                    {}
                })
            })
            .await;
        }
    }
}
