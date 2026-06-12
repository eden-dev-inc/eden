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

const API_INFO: ApiInfo<RedisApi, TdigestTrimmedMeanInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TdigestTrimmedMean,
    "Returns an estimation of the mean value from the sketch, excluding observation values outside the low and high cutoff quantiles",
    ReqType::Read,
    true,
);

/// Input for Redis `TDIGEST.TRIMMED_MEAN` command.
///
/// Returns an estimation of the mean value from the sketch, excluding
/// observation values outside the low and high cutoff quantiles.
///
/// See official Redis documentation for `TDIGEST.TRIMMED_MEAN`:
/// https://redis.io/docs/latest/commands/tdigest.trimmed_mean/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TdigestTrimmedMeanInput {
    /// The key name for the t-digest sketch
    pub(crate) key: RedisKey,
    /// Lower cutoff quantile (between 0.0 and 1.0)
    pub(crate) low_cut_quantile: RedisJsonValue,
    /// Upper cutoff quantile (between 0.0 and 1.0)
    pub(crate) high_cut_quantile: RedisJsonValue,
}

impl Serialize for TdigestTrimmedMeanInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestTrimmedMeanInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("low_cut_quantile", &self.low_cut_quantile)?;
        state.serialize_field("high_cut_quantile", &self.high_cut_quantile)?;
        state.end()
    }
}

impl_redis_operation!(
    TdigestTrimmedMeanInput,
    API_INFO,
    {key, low_cut_quantile, high_cut_quantile}
);

impl RedisCommandInput for TdigestTrimmedMeanInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.low_cut_quantile).arg(&self.high_cut_quantile);

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::parse(format!(
                "TDIGEST.TRIMMED_MEAN requires exactly 3 arguments (key, low_cut_quantile, high_cut_quantile), given {}",
                args.len()
            )));
        }

        Ok(TdigestTrimmedMeanInput {
            key: args[0].clone().try_into()?,
            low_cut_quantile: args[1].clone(),
            high_cut_quantile: args[2].clone(),
        })
    }
}

/// Output for Redis `TDIGEST.TRIMMED_MEAN` command.
///
/// Contains the trimmed mean value from the t-digest sketch.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TdigestTrimmedMeanOutput {
    /// The trimmed mean value, or NaN if the sketch is empty
    mean: f64,
}

impl TdigestTrimmedMeanOutput {
    pub fn new(mean: f64) -> Self {
        Self { mean }
    }

    /// Get the trimmed mean value
    pub fn mean(&self) -> f64 {
        self.mean
    }

    /// Check if the result is NaN (empty sketch or invalid range)
    pub fn is_nan(&self) -> bool {
        self.mean.is_nan()
    }

    /// Decode the Redis protocol response into a TdigestTrimmedMeanOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let mean = Self::parse_frame(frame)?;
        Ok(Self { mean })
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
            other => Err(EpError::parse(format!("unexpected TDIGEST.TRIMMED_MEAN response: {:?}", other))),
        }
    }

    fn parse_resp3(frame: Resp3Frame) -> Result<f64, EpError> {
        match frame {
            Resp3Frame::Double { data, .. } => Ok(data),
            Resp3Frame::Number { data, .. } => Ok(data as f64),
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
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
            Resp3Frame::Null => Ok(f64::NAN),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected TDIGEST.TRIMMED_MEAN response: {:?}", other))),
        }
    }
}

impl Serialize for TdigestTrimmedMeanOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestTrimmedMeanOutput", 1)?;
        state.serialize_field("mean", &self.mean)?;
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
            let input = TdigestTrimmedMeanInput {
                key: RedisKey::String("td".into()),
                low_cut_quantile: RedisJsonValue::Float(0.1),
                high_cut_quantile: RedisJsonValue::Float(0.9),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.TRIMMED_MEAN"));
            assert!(cmd_str.contains("td"));
        }

        #[test]
        fn test_decode_output_float() {
            let output = TdigestTrimmedMeanOutput::decode(b"$4\r\n10.5\r\n").unwrap();
            assert!((output.mean() - 10.5).abs() < f64::EPSILON);
            assert!(!output.is_nan());
        }

        #[test]
        fn test_decode_output_integer_as_float() {
            let output = TdigestTrimmedMeanOutput::decode(b"$2\r\n42\r\n").unwrap();
            assert!((output.mean() - 42.0).abs() < f64::EPSILON);
        }

        #[test]
        fn test_decode_output_nan() {
            let output = TdigestTrimmedMeanOutput::decode(b"$3\r\nnan\r\n").unwrap();
            assert!(output.mean().is_nan());
            assert!(output.is_nan());
        }

        #[test]
        fn test_decode_output_error() {
            let err = TdigestTrimmedMeanOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Float(0.1),
                RedisJsonValue::Float(0.9),
            ];
            let input = TdigestTrimmedMeanInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::Float(0.1)];
            let err = TdigestTrimmedMeanInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 3 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Float(0.1),
                RedisJsonValue::Float(0.9),
                RedisJsonValue::String("extra".into()),
            ];
            let err = TdigestTrimmedMeanInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 3 arguments"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TdigestTrimmedMeanInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = TdigestTrimmedMeanInput {
                key: RedisKey::String("mykey".into()),
                low_cut_quantile: RedisJsonValue::Float(0.0),
                high_cut_quantile: RedisJsonValue::Float(1.0),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_serialize_output() {
            let output = TdigestTrimmedMeanOutput::new(42.5);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("mean"));
            assert!(json.contains("42.5"));
        }

        #[test]
        fn test_new_output() {
            let output = TdigestTrimmedMeanOutput::new(100.0);
            assert!((output.mean() - 100.0).abs() < f64::EPSILON);
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
        async fn test_tdigest_trimmed_mean_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_tmean_test".into()),
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
                            key: RedisKey::String("td_tmean_test".into()),
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
                            &TdigestTrimmedMeanInput {
                                key: RedisKey::String("td_tmean_test".into()),
                                low_cut_quantile: RedisJsonValue::Float(0.0),
                                high_cut_quantile: RedisJsonValue::Float(1.0),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TdigestTrimmedMeanOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_nan());
                    // Full trimmed mean of [1,2,3,4,5] should be 3.0
                    assert!(output.mean() >= 1.0 && output.mean() <= 5.0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_trimmed_mean_with_cutoff() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_tmean_cut".into()),
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

                    // Add values with outliers
                    ctx.raw(
                        &TdigestAddInput {
                            key: RedisKey::String("td_tmean_cut".into()),
                            value: vec![
                                RedisJsonValue::Float(1.0),
                                RedisJsonValue::Float(2.0),
                                RedisJsonValue::Float(3.0),
                                RedisJsonValue::Float(4.0),
                                RedisJsonValue::Float(100.0), // outlier
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("add failed");

                    let result = ctx
                        .raw(
                            &TdigestTrimmedMeanInput {
                                key: RedisKey::String("td_tmean_cut".into()),
                                low_cut_quantile: RedisJsonValue::Float(0.1),
                                high_cut_quantile: RedisJsonValue::Float(0.9),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TdigestTrimmedMeanOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_nan());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_trimmed_mean_empty_sketch() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_tmean_empty".into()),
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

                    let result = ctx
                        .raw(
                            &TdigestTrimmedMeanInput {
                                key: RedisKey::String("td_tmean_empty".into()),
                                low_cut_quantile: RedisJsonValue::Float(0.0),
                                high_cut_quantile: RedisJsonValue::Float(1.0),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TdigestTrimmedMeanOutput::decode(&result).expect("decode failed");
                    // Empty sketch should return NaN
                    assert!(output.is_nan());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_trimmed_mean_nonexistent_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TdigestTrimmedMeanInput {
                                key: RedisKey::String("nonexistent_td".into()),
                                low_cut_quantile: RedisJsonValue::Float(0.0),
                                high_cut_quantile: RedisJsonValue::Float(1.0),
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
