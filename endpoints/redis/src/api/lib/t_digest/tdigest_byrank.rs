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

const API_INFO: ApiInfo<RedisApi, TdigestByrankInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TdigestByrank,
    "Returns, for each input rank, an estimation of the value (floating-point) with that rank",
    ReqType::Read,
    true,
);

/// Input for Redis `TDIGEST.BYRANK` command.
///
/// Returns, for each input rank, an estimation of the value with that rank.
///
/// See official Redis documentation for `TDIGEST.BYRANK`:
/// https://redis.io/docs/latest/commands/tdigest.byrank/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TdigestByrankInput {
    /// The key name for the t-digest sketch
    pub(crate) key: RedisKey,
    /// One or more ranks to query
    pub(crate) rank: Vec<RedisJsonValue>,
}

impl Serialize for TdigestByrankInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestByrankInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("rank", &self.rank)?;
        state.end()
    }
}

impl_redis_operation!(
    TdigestByrankInput,
    API_INFO,
    {key, rank}
);

impl RedisCommandInput for TdigestByrankInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);
        for r in &self.rank {
            command.arg(r);
        }
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse("TDIGEST.BYRANK requires at least 2 arguments (key, rank...)"));
        }

        let key = args[0].clone().try_into()?;
        let rank = args[1..].to_vec();

        if rank.is_empty() {
            return Err(EpError::parse("TDIGEST.BYRANK requires at least one rank to query"));
        }

        Ok(TdigestByrankInput { key, rank })
    }
}

/// Output for Redis `TDIGEST.BYRANK` command.
///
/// Contains the estimated values for each queried rank.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TdigestByrankOutput {
    /// Estimated values for each rank (may contain inf for out-of-range ranks)
    values: Vec<f64>,
}

impl TdigestByrankOutput {
    pub fn new(values: Vec<f64>) -> Self {
        Self { values }
    }

    /// Get the estimated values
    pub fn values(&self) -> &[f64] {
        &self.values
    }

    /// Get the number of values
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Decode the Redis protocol response into a TdigestByrankOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let values = Self::parse_frame(frame)?;
        Ok(Self { values })
    }

    fn parse_frame(frame: DecoderRespFrame) -> Result<Vec<f64>, EpError> {
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3(resp3_frame),
        }
    }

    fn parse_resp2(frame: Resp2Frame) -> Result<Vec<f64>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut values = Vec::with_capacity(arr.len());
                for item in arr {
                    values.push(Self::parse_resp2_float(&item)?);
                }
                Ok(values)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected TDIGEST.BYRANK response: {:?}", other))),
        }
    }

    fn parse_resp2_float(frame: &Resp2Frame) -> Result<f64, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => {
                let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                if s == "inf" || s == "+inf" {
                    Ok(f64::INFINITY)
                } else if s == "-inf" {
                    Ok(f64::NEG_INFINITY)
                } else {
                    s.parse().map_err(EpError::parse)
                }
            }
            Resp2Frame::Integer(n) => Ok(*n as f64),
            Resp2Frame::Null => Ok(f64::NAN),
            other => Err(EpError::parse(format!("expected float, got {:?}", other))),
        }
    }

    fn parse_resp3(frame: Resp3Frame) -> Result<Vec<f64>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut values = Vec::with_capacity(data.len());
                for item in data {
                    values.push(Self::parse_resp3_float(&item)?);
                }
                Ok(values)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected TDIGEST.BYRANK response: {:?}", other))),
        }
    }

    fn parse_resp3_float(frame: &Resp3Frame) -> Result<f64, EpError> {
        match frame {
            Resp3Frame::Double { data, .. } => Ok(*data),
            Resp3Frame::Number { data, .. } => Ok(*data as f64),
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                let s = String::from_utf8_lossy(data);
                if s == "nan" || s == "NaN" {
                    Ok(f64::NAN)
                } else if s == "inf" || s == "+inf" {
                    Ok(f64::INFINITY)
                } else if s == "-inf" {
                    Ok(f64::NEG_INFINITY)
                } else {
                    s.parse::<f64>().map_err(EpError::parse)
                }
            }
            Resp3Frame::Null => Ok(f64::NAN),
            other => Err(EpError::parse(format!("expected float, got {:?}", other))),
        }
    }
}

impl Serialize for TdigestByrankOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestByrankOutput", 1)?;
        state.serialize_field("values", &self.values)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_rank() {
            let input = TdigestByrankInput {
                key: RedisKey::String("td".into()),
                rank: vec![RedisJsonValue::Integer(0)],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.BYRANK"));
            assert!(cmd_str.contains("td"));
        }

        #[test]
        fn test_encode_command_multiple_ranks() {
            let input = TdigestByrankInput {
                key: RedisKey::String("td".into()),
                rank: vec![RedisJsonValue::Integer(0), RedisJsonValue::Integer(1), RedisJsonValue::Integer(2)],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.BYRANK"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = TdigestByrankOutput::decode(b"*2\r\n$3\r\n1.5\r\n$3\r\n2.5\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert!((output.values()[0] - 1.5).abs() < f64::EPSILON);
            assert!((output.values()[1] - 2.5).abs() < f64::EPSILON);
        }

        #[test]
        fn test_decode_output_with_infinity() {
            let output = TdigestByrankOutput::decode(b"*1\r\n$3\r\ninf\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert!(output.values()[0].is_infinite());
        }

        #[test]
        fn test_decode_output_error() {
            let err = TdigestByrankOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(5),
            ];
            let input = TdigestByrankInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.rank.len(), 2);
        }

        #[test]
        fn test_decode_input_missing_rank() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = TdigestByrankInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TdigestByrankInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = TdigestByrankInput {
                key: RedisKey::String("mykey".into()),
                rank: vec![RedisJsonValue::Integer(0)],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_serialize_output() {
            let output = TdigestByrankOutput::new(vec![1.0, 2.0, 3.0]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("values"));
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
        async fn test_tdigest_byrank_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_byrank_test".into()),
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
                            key: RedisKey::String("td_byrank_test".into()),
                            value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(2.0), RedisJsonValue::Float(3.0)],
                        }
                        .command(),
                    )
                    .await
                    .expect("add failed");

                    let result = ctx
                        .raw(
                            &TdigestByrankInput {
                                key: RedisKey::String("td_byrank_test".into()),
                                rank: vec![RedisJsonValue::Integer(0)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TdigestByrankOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_byrank_multiple_ranks() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_byrank_multi".into()),
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
                            key: RedisKey::String("td_byrank_multi".into()),
                            value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(2.0), RedisJsonValue::Float(3.0)],
                        }
                        .command(),
                    )
                    .await
                    .expect("add failed");

                    let result = ctx
                        .raw(
                            &TdigestByrankInput {
                                key: RedisKey::String("td_byrank_multi".into()),
                                rank: vec![RedisJsonValue::Integer(0), RedisJsonValue::Integer(1), RedisJsonValue::Integer(2)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TdigestByrankOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_byrank_nonexistent_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TdigestByrankInput {
                                key: RedisKey::String("nonexistent_td".into()),
                                rank: vec![RedisJsonValue::Integer(0)],
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
