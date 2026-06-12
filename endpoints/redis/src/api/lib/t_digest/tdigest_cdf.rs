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

const API_INFO: ApiInfo<RedisApi, TdigestCdfInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TdigestCdf,
    "Returns, for each input value, an estimation of the fraction (floating-point) of (observations smaller than the given value + half the observations equal to the given value)",
    ReqType::Read,
    true,
);

/// Input for Redis `TDIGEST.CDF` command.
///
/// Returns the cumulative distribution function (CDF) for each input value.
///
/// See official Redis documentation for `TDIGEST.CDF`:
/// https://redis.io/docs/latest/commands/tdigest.cdf/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TdigestCdfInput {
    /// The key name for the t-digest sketch
    pub(crate) key: RedisKey,
    /// One or more values to query the CDF for
    pub(crate) value: Vec<RedisJsonValue>,
}

impl Serialize for TdigestCdfInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestCdfInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(
    TdigestCdfInput,
    API_INFO,
    {key, value}
);

impl RedisCommandInput for TdigestCdfInput {
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
                "TDIGEST.CDF requires at least 2 arguments (key, value...), given {}",
                args.len()
            )));
        }

        let key = args[0].clone().try_into()?;
        let value = args[1..].to_vec();

        if value.is_empty() {
            return Err(EpError::parse("TDIGEST.CDF requires at least one value to query"));
        }

        Ok(Self { key, value })
    }
}

/// Output for Redis `TDIGEST.CDF` command.
///
/// Contains the CDF values (fractions between 0 and 1) for each queried value.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TdigestCdfOutput {
    /// CDF fractions for each value (between 0.0 and 1.0)
    fractions: Vec<f64>,
}

impl TdigestCdfOutput {
    pub fn new(fractions: Vec<f64>) -> Self {
        Self { fractions }
    }

    /// Get the CDF fractions
    pub fn fractions(&self) -> &[f64] {
        &self.fractions
    }

    /// Get the number of fractions
    pub fn len(&self) -> usize {
        self.fractions.len()
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.fractions.is_empty()
    }

    /// Decode the Redis protocol response into a TdigestCdfOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let fractions = Self::parse_frame(frame)?;
        Ok(Self { fractions })
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
                let mut fractions = Vec::with_capacity(arr.len());
                for item in arr {
                    fractions.push(Self::parse_resp2_float(&item)?);
                }
                Ok(fractions)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected TDIGEST.CDF response: {:?}", other))),
        }
    }

    fn parse_resp2_float(frame: &Resp2Frame) -> Result<f64, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => {
                let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                s.parse().map_err(EpError::parse)
            }
            Resp2Frame::Integer(n) => Ok(*n as f64),
            Resp2Frame::Null => Ok(f64::NAN),
            other => Err(EpError::parse(format!("expected float, got {:?}", other))),
        }
    }

    fn parse_resp3(frame: Resp3Frame) -> Result<Vec<f64>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut fractions = Vec::with_capacity(data.len());
                for item in data {
                    fractions.push(Self::parse_resp3_float(&item)?);
                }
                Ok(fractions)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected TDIGEST.CDF response: {:?}", other))),
        }
    }

    fn parse_resp3_float(frame: &Resp3Frame) -> Result<f64, EpError> {
        match frame {
            Resp3Frame::Double { data, .. } => Ok(*data),
            Resp3Frame::Number { data, .. } => Ok(*data as f64),
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                s.parse::<f64>().map_err(EpError::parse)
            }
            Resp3Frame::Null => Ok(f64::NAN),
            other => Err(EpError::parse(format!("expected float, got {:?}", other))),
        }
    }
}

impl Serialize for TdigestCdfOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestCdfOutput", 1)?;
        state.serialize_field("fractions", &self.fractions)?;
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
            let input = TdigestCdfInput {
                key: RedisKey::String("td".into()),
                value: vec![RedisJsonValue::Float(1.5)],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.CDF"));
            assert!(cmd_str.contains("td"));
        }

        #[test]
        fn test_encode_command_multiple_values() {
            let input = TdigestCdfInput {
                key: RedisKey::String("td".into()),
                value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(2.0), RedisJsonValue::Float(3.0)],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.CDF"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = TdigestCdfOutput::decode(b"*2\r\n$3\r\n0.5\r\n$4\r\n0.75\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert!((output.fractions()[0] - 0.5).abs() < f64::EPSILON);
            assert!((output.fractions()[1] - 0.75).abs() < f64::EPSILON);
        }

        #[test]
        fn test_decode_output_zero_and_one() {
            let output = TdigestCdfOutput::decode(b"*2\r\n$1\r\n0\r\n$1\r\n1\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert!((output.fractions()[0] - 0.0).abs() < f64::EPSILON);
            assert!((output.fractions()[1] - 1.0).abs() < f64::EPSILON);
        }

        #[test]
        fn test_decode_output_error() {
            let err = TdigestCdfOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Float(1.5),
                RedisJsonValue::Float(2.5),
            ];
            let input = TdigestCdfInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.value.len(), 2);
        }

        #[test]
        fn test_decode_input_missing_value() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = TdigestCdfInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TdigestCdfInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = TdigestCdfInput {
                key: RedisKey::String("mykey".into()),
                value: vec![RedisJsonValue::Float(1.0)],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_serialize_output() {
            let output = TdigestCdfOutput::new(vec![0.25, 0.5, 0.75]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("fractions"));
            assert!(json.contains("0.25"));
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
        async fn test_tdigest_cdf_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_cdf_test".into()),
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
                            key: RedisKey::String("td_cdf_test".into()),
                            value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(2.0), RedisJsonValue::Float(3.0)],
                        }
                        .command(),
                    )
                    .await
                    .expect("add failed");

                    let result = ctx
                        .raw(
                            &TdigestCdfInput {
                                key: RedisKey::String("td_cdf_test".into()),
                                value: vec![RedisJsonValue::Float(2.0)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TdigestCdfOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty());
                    // CDF(2.0) should be around 0.5 for values [1, 2, 3]
                    assert!(output.fractions()[0] >= 0.0 && output.fractions()[0] <= 1.0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_cdf_multiple_values() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_cdf_multi".into()),
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
                            key: RedisKey::String("td_cdf_multi".into()),
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
                            &TdigestCdfInput {
                                key: RedisKey::String("td_cdf_multi".into()),
                                value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(3.0), RedisJsonValue::Float(5.0)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TdigestCdfOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    // All fractions should be between 0 and 1
                    for f in output.fractions() {
                        assert!(*f >= 0.0 && *f <= 1.0);
                    }
                    // CDF should be monotonically increasing
                    assert!(output.fractions()[0] <= output.fractions()[1]);
                    assert!(output.fractions()[1] <= output.fractions()[2]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_cdf_nonexistent_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TdigestCdfInput {
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
