use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Expiration, Field, FieldCondition, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, HsetexInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hsetex,
    "Set the value of one or more fields of a given hash key, and optionally set their expiration",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `HSETEX`
/// https://redis.io/docs/latest/commands/hsetex/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HsetexInput {
    pub(crate) key: RedisKey,
    pub(crate) field_condition: Option<FieldCondition>,
    pub(crate) expiration: Option<Expiration>,
    pub(crate) fields: Vec<Field>,
}

impl Serialize for HsetexInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut field_count = 3;
        if self.field_condition.is_some() {
            field_count += 1;
        }
        if self.expiration.is_some() {
            field_count += 1;
        }

        let mut state = serializer.serialize_struct("HsetexInput", field_count)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("fields", &self.fields)?;
        if let Some(fc) = &self.field_condition {
            state.serialize_field("field_condition", fc)?;
        }
        if let Some(exp) = &self.expiration {
            state.serialize_field("expiration", exp)?;
        }
        state.end()
    }
}

impl_redis_operation!(HsetexInput, API_INFO, {key, field_condition, expiration, fields});

impl RedisCommandInput for HsetexInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut cmd = crate::command::cmd(&API_INFO.api.to_string());
        cmd.arg(&self.key);

        if let Some(fc) = &self.field_condition {
            match fc {
                FieldCondition::FNX => cmd.arg("FNX"),
                FieldCondition::FXX => cmd.arg("FXX"),
            };
        }

        if let Some(exp) = &self.expiration {
            match exp {
                Expiration::EX(v) => cmd.arg("EX").arg(v),
                Expiration::PX(v) => cmd.arg("PX").arg(v),
                Expiration::EXAT(v) => cmd.arg("EXAT").arg(v),
                Expiration::PXAT(v) => cmd.arg("PXAT").arg(v),
                Expiration::KEEPTTL => cmd.arg("KEEPTTL"),
            };
        }

        cmd.arg("FIELDS").arg(self.fields.len());
        for fv in &self.fields {
            cmd.arg(&fv.field).arg(&fv.value);
        }
        cmd.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("HSETEX requires at least 3 arguments, found {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let mut field_condition = None;
        let mut expiration = None;
        let mut i = 1;

        // Parse optional field condition (FNX/FXX)
        if let Some(RedisJsonValue::String(s)) = args.get(i) {
            match s.to_uppercase().as_str() {
                "FNX" => {
                    field_condition = Some(FieldCondition::FNX);
                    i += 1;
                }
                "FXX" => {
                    field_condition = Some(FieldCondition::FXX);
                    i += 1;
                }
                _ => {}
            }
        }

        // Parse optional expiration
        if let Some(RedisJsonValue::String(s)) = args.get(i) {
            match s.to_uppercase().as_str() {
                "EX" => {
                    expiration = Some(Expiration::EX(args.get(i + 1).cloned().ok_or_else(|| EpError::request("EX requires a value"))?));
                    i += 2;
                }
                "PX" => {
                    expiration = Some(Expiration::PX(args.get(i + 1).cloned().ok_or_else(|| EpError::request("PX requires a value"))?));
                    i += 2;
                }
                "EXAT" => {
                    expiration = Some(Expiration::EXAT(args.get(i + 1).cloned().ok_or_else(|| EpError::request("EXAT requires a value"))?));
                    i += 2;
                }
                "PXAT" => {
                    expiration = Some(Expiration::PXAT(args.get(i + 1).cloned().ok_or_else(|| EpError::request("PXAT requires a value"))?));
                    i += 2;
                }
                "KEEPTTL" => {
                    expiration = Some(Expiration::KEEPTTL);
                    i += 1;
                }
                _ => {}
            }
        }

        // Skip FIELDS keyword if present
        if let Some(RedisJsonValue::String(s)) = args.get(i)
            && s.to_uppercase() == "FIELDS"
        {
            i += 1;
        }

        // Parse field count
        let field_count = match args.get(i) {
            Some(RedisJsonValue::Integer(n)) => {
                i += 1;
                *n as usize
            }
            Some(RedisJsonValue::String(s)) => {
                i += 1;
                s.parse::<usize>().map_err(|_| EpError::request("Field count must be an integer"))?
            }
            _ => return Err(EpError::request("Missing field count")),
        };

        // Parse field/value pairs
        let mut fields = Vec::with_capacity(field_count);
        for chunk in args[i..].chunks(2) {
            if chunk.len() != 2 {
                return Err(EpError::request("Invalid field/value pair"));
            }
            fields.push(Field { field: chunk[0].clone(), value: chunk[1].clone() });
        }

        if fields.len() != field_count {
            return Err(EpError::request(format!("Expected {} field/value pairs, found {}", field_count, fields.len())));
        }

        Ok(Self { key, field_condition, expiration, fields })
    }
}

/// Output for Redis HSETEX command
/// Returns 1 if all fields were set successfully, 0 if no fields were set
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HsetexOutput {
    fields_added: i64,
}

impl HsetexOutput {
    pub fn new(fields_added: i64) -> Self {
        Self { fields_added }
    }
    /// Returns 1 if all fields were set successfully, 0 if no fields were set
    pub fn fields_added(&self) -> i64 {
        self.fields_added
    }
    /// Returns true if the operation was successful (fields_added == 1)
    pub fn added_new_fields(&self) -> bool {
        self.fields_added > 0
    }
    /// Returns true if the operation was successful
    pub fn was_successful(&self) -> bool {
        self.fields_added == 1
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        let fields_added = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => n,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => data,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            other => {
                return Err(EpError::parse(format!("unexpected HSETEX response: {:?}", other)));
            }
        };
        Ok(Self { fields_added })
    }
}

impl Serialize for HsetexOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HsetexOutput", 1)?;
        state.serialize_field("fields_added", &self.fields_added)?;
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
            let input = HsetexInput {
                key: RedisKey::String("myhash".into()),
                field_condition: None,
                expiration: None,
                fields: vec![Field::new("f1".to_string().into(), "v1".to_string().into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HSETEX"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("FIELDS"));
        }

        #[test]
        fn test_encode_command_with_fnx() {
            let input = HsetexInput {
                key: RedisKey::String("myhash".into()),
                field_condition: Some(FieldCondition::FNX),
                expiration: None,
                fields: vec![Field::new("f1".to_string().into(), "v1".to_string().into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FNX"));
        }

        #[test]
        fn test_encode_command_with_ex() {
            let input = HsetexInput {
                key: RedisKey::String("myhash".into()),
                field_condition: None,
                expiration: Some(Expiration::EX(RedisJsonValue::Integer(60))),
                fields: vec![Field::new("f1".to_string().into(), "v1".to_string().into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("EX"));
            assert!(cmd_str.contains("60"));
        }

        #[test]
        fn test_decode_fields_added() {
            let output = HsetexOutput::decode(b":2\r\n").unwrap();
            assert_eq!(output.fields_added(), 2);
            assert!(output.added_new_fields());
        }

        #[test]
        fn test_decode_no_new_fields() {
            let output = HsetexOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.fields_added(), 0);
            assert!(!output.added_new_fields());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HsetexOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HsetexInput {
                key: RedisKey::String("myhash".into()),
                field_condition: None,
                expiration: None,
                fields: vec![],
            };
            assert_eq!(input.keys().len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // HSETEX requires Redis 8+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hsetex_basic() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nhsetex_hash\r\n").await.expect("raw failed");

                    let input = HsetexInput {
                        key: RedisKey::String("hsetex_hash".into()),
                        field_condition: None,
                        expiration: Some(Expiration::EX(RedisJsonValue::Integer(60))),
                        fields: vec![Field::new("f1".into(), "v1".into()), Field::new("f2".into(), "v2".into())],
                    };

                    let result = ctx.raw(&input.command()).await.expect("raw failed");

                    let output = HsetexOutput::decode(&result).expect("decode failed");
                    // HSETEX returns 1 if all fields were set, 0 if none were set
                    assert_eq!(output.fields_added(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hsetex_with_fnx() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nhsetex_fnx_key\r\n").await.expect("raw failed");

                    // First set should succeed
                    let result1 = ctx
                        .raw(
                            &HsetexInput {
                                key: RedisKey::String("hsetex_fnx_key".into()),
                                field_condition: None,
                                expiration: None,
                                fields: vec![Field::new("existing".into(), "old".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output1 = HsetexOutput::decode(&result1).expect("decode failed");
                    assert_eq!(output1.fields_added(), 1);

                    // FNX with existing field should fail (return 0)
                    // because FNX requires ALL fields to not exist
                    let result = ctx
                        .raw(
                            &HsetexInput {
                                key: RedisKey::String("hsetex_fnx_key".into()),
                                field_condition: Some(FieldCondition::FNX),
                                expiration: None,
                                fields: vec![
                                    Field::new("existing".into(), "new".into()),
                                    Field::new("new_field".into(), "value".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HsetexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.fields_added(), 0, "FNX should fail because 'existing' field already exists");

                    // FNX with only new fields should succeed
                    let result2 = ctx
                        .raw(
                            &HsetexInput {
                                key: RedisKey::String("hsetex_fnx_key".into()),
                                field_condition: Some(FieldCondition::FNX),
                                expiration: None,
                                fields: vec![
                                    Field::new("new1".into(), "value1".into()),
                                    Field::new("new2".into(), "value2".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output2 = HsetexOutput::decode(&result2).expect("decode failed");
                    assert_eq!(output2.fields_added(), 1, "FNX should succeed with all new fields");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hsetex_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;
            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nhsetex_r2\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &HsetexInput {
                        key: RedisKey::String("hsetex_r2".into()),
                        field_condition: None,
                        expiration: None,
                        fields: vec![Field::new("f".into(), "v".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"));
            ctx.stop().await;
        }
    }
}
