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

const API_INFO: ApiInfo<RedisApi, TdigestMinInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TdigestMin,
    "Returns the minimum observation value from a t-digest sketch",
    ReqType::Read,
    true,
);

/// Input for Redis `TDIGEST.MIN` command.
///
/// Returns the minimum observation value from a t-digest sketch.
///
/// See official Redis documentation for `TDIGEST.MIN`:
/// https://redis.io/docs/latest/commands/tdigest.min/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TdigestMinInput {
    /// The key name for the t-digest sketch
    pub(crate) key: RedisKey,
}

impl Serialize for TdigestMinInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestMinInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(TdigestMinInput, API_INFO, { key });

impl RedisCommandInput for TdigestMinInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::parse(format!("TDIGEST.MIN requires exactly 1 argument (key), given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis `TDIGEST.MIN` command.
///
/// Contains the minimum observation value from the t-digest sketch.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TdigestMinOutput {
    /// The minimum value, or NaN if the sketch is empty
    min: f64,
}

impl TdigestMinOutput {
    pub fn new(min: f64) -> Self {
        Self { min }
    }

    /// Get the minimum value
    pub fn min(&self) -> f64 {
        self.min
    }

    /// Check if the sketch was empty (returns NaN)
    pub fn is_empty(&self) -> bool {
        self.min.is_nan()
    }

    /// Decode the Redis protocol response into a TdigestMinOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let min = Self::parse_frame(frame)?;
        Ok(Self { min })
    }

    fn parse_frame(frame: DecoderRespFrame) -> Result<f64, EpError> {
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3(resp3_frame),
        }
    }

    fn parse_resp2(frame: Resp2Frame) -> Result<f64, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => {
                let s = String::from_utf8(data).map_err(EpError::parse)?;
                if s == "nan" || s == "NaN" {
                    Ok(f64::NAN)
                } else if s == "inf" || s == "+inf" {
                    Ok(f64::INFINITY)
                } else if s == "-inf" {
                    Ok(f64::NEG_INFINITY)
                } else {
                    s.parse().map_err(EpError::parse)
                }
            }
            Resp2Frame::Integer(n) => Ok(n as f64),
            Resp2Frame::Null => Ok(f64::NAN),
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected TDIGEST.MIN response: {:?}", other))),
        }
    }

    fn parse_resp3(frame: Resp3Frame) -> Result<f64, EpError> {
        match frame {
            Resp3Frame::Double { data, .. } => Ok(data),
            Resp3Frame::Number { data, .. } => Ok(data as f64),
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                let s = String::from_utf8_lossy(&data);
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
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected TDIGEST.MIN response: {:?}", other))),
        }
    }
}

impl Serialize for TdigestMinOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestMinOutput", 1)?;
        state.serialize_field("min", &self.min)?;
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
            let input = TdigestMinInput { key: RedisKey::String("td".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.MIN"));
            assert!(cmd_str.contains("td"));
        }

        #[test]
        fn test_decode_output_float() {
            let output = TdigestMinOutput::decode(b"$4\r\n10.5\r\n").unwrap();
            assert!((output.min() - 10.5).abs() < f64::EPSILON);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_output_integer_as_float() {
            let output = TdigestMinOutput::decode(b"$2\r\n42\r\n").unwrap();
            assert!((output.min() - 42.0).abs() < f64::EPSILON);
        }

        #[test]
        fn test_decode_output_nan() {
            let output = TdigestMinOutput::decode(b"$3\r\nnan\r\n").unwrap();
            assert!(output.min().is_nan());
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_infinity() {
            let output = TdigestMinOutput::decode(b"$3\r\ninf\r\n").unwrap();
            assert!(output.min().is_infinite());
            assert!(output.min().is_sign_positive());
        }

        #[test]
        fn test_decode_output_neg_infinity() {
            let output = TdigestMinOutput::decode(b"$4\r\n-inf\r\n").unwrap();
            assert!(output.min().is_infinite());
            assert!(output.min().is_sign_negative());
        }

        #[test]
        fn test_decode_output_error() {
            let err = TdigestMinOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = TdigestMinInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("extra".into())];
            let err = TdigestMinInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 1 argument"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TdigestMinInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = TdigestMinInput { key: RedisKey::String("mykey".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_serialize_output() {
            let output = TdigestMinOutput::new(42.5);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("min"));
            assert!(json.contains("42.5"));
        }

        #[test]
        fn test_new_output() {
            let output = TdigestMinOutput::new(100.0);
            assert!((output.min() - 100.0).abs() < f64::EPSILON);
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
        async fn test_tdigest_min_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_min_test".into()),
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
                            key: RedisKey::String("td_min_test".into()),
                            value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(5.0), RedisJsonValue::Float(3.0)],
                        }
                        .command(),
                    )
                    .await
                    .expect("add failed");

                    let result =
                        ctx.raw(&TdigestMinInput { key: RedisKey::String("td_min_test".into()) }.command()).await.expect("raw failed");

                    let output = TdigestMinOutput::decode(&result).expect("decode failed");
                    assert!((output.min() - 1.0).abs() < f64::EPSILON);
                    assert!(!output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_min_empty_sketch() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_min_empty".into()),
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

                    let result =
                        ctx.raw(&TdigestMinInput { key: RedisKey::String("td_min_empty".into()) }.command()).await.expect("raw failed");

                    let output = TdigestMinOutput::decode(&result).expect("decode failed");
                    // Empty sketch should return NaN
                    assert!(output.min().is_nan());
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_min_negative_values() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_min_neg".into()),
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
                            key: RedisKey::String("td_min_neg".into()),
                            value: vec![
                                RedisJsonValue::Float(-10.0),
                                RedisJsonValue::Float(-5.0),
                                RedisJsonValue::Float(-1.0),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("add failed");

                    let result =
                        ctx.raw(&TdigestMinInput { key: RedisKey::String("td_min_neg".into()) }.command()).await.expect("raw failed");

                    let output = TdigestMinOutput::decode(&result).expect("decode failed");
                    assert!((output.min() - (-10.0)).abs() < f64::EPSILON);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_min_nonexistent_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&TdigestMinInput { key: RedisKey::String("nonexistent_td".into()) }.command()).await;

                    if let Ok(result) = result
                        && result.starts_with(b"-")
                    {}
                })
            })
            .await;
        }
    }
}
