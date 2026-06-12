use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{ExpireOptions, FieldValue, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, HgetexInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hgetex,
    "Returns the value of a field and optionally sets its expiration",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `HGETEX`
/// https://redis.io/docs/latest/commands/hgetex/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HgetexInput {
    pub(crate) key: RedisKey,
    pub(crate) options: Option<ExpireOptions>,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Serialize for HgetexInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields_count = 3;
        if self.options.is_some() {
            fields_count += 1;
        }

        let mut state = serializer.serialize_struct("HgetexInput", fields_count)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(options) = &self.options {
            state.serialize_field("options", options)?;
        }
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl_redis_operation!(HgetexInput, API_INFO, { key, options, fields });

impl RedisCommandInput for HgetexInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(options) = &self.options {
            match options {
                ExpireOptions::EX(value) => command.arg("EX").arg(value),
                ExpireOptions::PX(value) => command.arg("PX").arg(value),
                ExpireOptions::EXAT(value) => command.arg("EXAT").arg(value),
                ExpireOptions::PXAT(value) => command.arg("PXAT").arg(value),
                ExpireOptions::PERSIST => command.arg("PERSIST"),
            };
        }

        command.arg("FIELDS").arg(self.fields.len()).arg(&self.fields);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 4 {
            return Err(EpError::request(format!("HGETEX requires at least 4 arguments, found {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let mut options = None;
        let mut i = 1;

        if let RedisJsonValue::String(s) = &args[i] {
            match s.to_uppercase().as_str() {
                "EX" => {
                    if i + 1 >= args.len() {
                        return Err(EpError::request("EX requires a value"));
                    }
                    options = Some(ExpireOptions::EX(args[i + 1].clone()));
                    i += 2;
                }
                "PX" => {
                    if i + 1 >= args.len() {
                        return Err(EpError::request("PX requires a value"));
                    }
                    options = Some(ExpireOptions::PX(args[i + 1].clone()));
                    i += 2;
                }
                "EXAT" => {
                    if i + 1 >= args.len() {
                        return Err(EpError::request("EXAT requires a value"));
                    }
                    options = Some(ExpireOptions::EXAT(args[i + 1].clone()));
                    i += 2;
                }
                "PXAT" => {
                    if i + 1 >= args.len() {
                        return Err(EpError::request("PXAT requires a value"));
                    }
                    options = Some(ExpireOptions::PXAT(args[i + 1].clone()));
                    i += 2;
                }
                "PERSIST" => {
                    options = Some(ExpireOptions::PERSIST);
                    i += 1;
                }
                "FIELDS" => {}
                _ => {
                    return Err(EpError::request(format!("Unknown option: {}", s)));
                }
            }
        }

        if i >= args.len() || !matches!(&args[i], RedisJsonValue::String(s) if s.to_uppercase() == "FIELDS") {
            return Err(EpError::request("Expected 'FIELDS' keyword"));
        }
        i += 1;

        if i >= args.len() {
            return Err(EpError::request("Missing field count"));
        }

        let numfields = match &args[i] {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::request("Field count must be an integer"))?,
            _ => return Err(EpError::request("Field count must be an integer")),
        };
        i += 1;

        let remaining_args = args.len() - i;
        if remaining_args != numfields {
            return Err(EpError::request(format!("Expected {} fields, found {}", numfields, remaining_args)));
        }

        let fields = args[i..].to_vec();

        Ok(HgetexInput { key, options, fields })
    }
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HgetexOutput {
    values: Vec<FieldValue>,
}

impl HgetexOutput {
    pub fn new(values: Vec<FieldValue>) -> Self {
        Self { values }
    }

    pub fn values(&self) -> &[FieldValue] {
        &self.values
    }

    pub fn get(&self, index: usize) -> Option<&FieldValue> {
        self.values.get(index)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn parse_value(frame: &Resp2Frame) -> Result<FieldValue, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => Ok(FieldValue::Value(RedisJsonValue::String(String::from_utf8_lossy(data).to_string()))),
            Resp2Frame::Null => Ok(FieldValue::NotFound),
            other => Err(EpError::parse(format!("unexpected HGETEX value: {:?}", other))),
        }
    }

    fn parse_value_resp3(frame: Resp3Frame) -> Result<FieldValue, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } => {
                Ok(FieldValue::Value(RedisJsonValue::String(String::from_utf8_lossy(&data).to_string())))
            }
            Resp3Frame::Null => Ok(FieldValue::NotFound),
            other => Err(EpError::parse(format!("unexpected HGETEX value: {:?}", other))),
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let values = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr.iter().map(Self::parse_value).collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HGETEX response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data.into_iter().map(Self::parse_value_resp3).collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HGETEX response: {:?}", other)));
                }
            },
        };

        Ok(Self { values })
    }
}

impl Serialize for HgetexOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HgetexOutput", 1)?;
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
        fn test_encode_command_no_options() {
            let input = HgetexInput {
                key: RedisKey::String("myhash".into()),
                options: None,
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HGETEX"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("FIELDS"));
            assert!(cmd_str.contains("field1"));
        }

        #[test]
        fn test_encode_command_with_ex() {
            let input = HgetexInput {
                key: RedisKey::String("myhash".into()),
                options: Some(ExpireOptions::EX(RedisJsonValue::Integer(60))),
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("EX"));
            assert!(cmd_str.contains("60"));
        }

        #[test]
        fn test_decode_output_with_value() {
            let output = HgetexOutput::decode(b"*1\r\n$5\r\nvalue\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.values()[0], FieldValue::Value(RedisJsonValue::String("value".into())));
        }

        #[test]
        fn test_decode_output_not_found() {
            let output = HgetexOutput::decode(b"*1\r\n$-1\r\n").unwrap();
            assert_eq!(output.values()[0], FieldValue::NotFound);
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HgetexInput {
                key: RedisKey::String("myhash".into()),
                options: None,
                fields: vec![RedisJsonValue::String("f".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::hash::Field;
        use crate::api::{FieldTtl, HsetInput, HttlInput, HttlOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hgetex_get_value() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nhgetex_test\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hgetex_test".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HgetexInput {
                                key: RedisKey::String("hgetex_test".into()),
                                options: None,
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HgetexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.values()[0], FieldValue::Value(RedisJsonValue::String("value1".into())));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hgetex_with_expiry() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nhgetex_expir\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hgetex_expir".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HgetexInput {
                                key: RedisKey::String("hgetex_expir".into()),
                                options: Some(ExpireOptions::EX(RedisJsonValue::Integer(60))),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HgetexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.values()[0], FieldValue::Value(RedisJsonValue::String("value1".into())));

                    let verify = ctx
                        .raw(
                            &HttlInput {
                                key: RedisKey::String("hgetex_expir".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let verify_output = HttlOutput::decode(&verify).expect("decode failed");
                    match &verify_output.ttls()[0] {
                        FieldTtl::Seconds(s) => assert!(*s > 0 && *s <= 60),
                        other => panic!("Expected Seconds, got {:?}", other),
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hgetex_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nhgetex_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hgetex_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HgetexInput {
                        key: RedisKey::String("hgetex_r2".into()),
                        options: None,
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HgetexOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }
    }
}
