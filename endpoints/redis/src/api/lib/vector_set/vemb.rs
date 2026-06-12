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

const API_INFO: ApiInfo<RedisApi, VembInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Vemb, "Return the vector associated with an element", ReqType::Read, true);

/// See official Redis documentation for `VEMB`
/// https://redis.io/docs/latest/commands/vemb/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct VembInput {
    key: RedisKey,
    element: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    raw: Option<bool>,
}

impl VembInput {
    pub fn new(key: impl Into<RedisKey>, element: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), element: element.into(), raw: None }
    }

    pub fn with_raw(mut self, raw: bool) -> Self {
        self.raw = Some(raw);
        self
    }
}

impl Serialize for VembInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.raw.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("VembInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("element", &self.element)?;
        if let Some(raw) = self.raw {
            state.serialize_field("raw", &raw)?;
        }
        state.end()
    }
}

impl_redis_operation!(VembInput, API_INFO, { key, element, raw });

impl RedisCommandInput for VembInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.element);
        if self.raw.is_some() {
            command.arg("RAW");
        }
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("VEMB requires at least 2 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let element = args[1].clone();
        let raw = if args.len() > 2 {
            if let RedisJsonValue::String(s) = &args[2] {
                if s.to_uppercase() == "RAW" { Some(true) } else { None }
            } else {
                None
            }
        } else {
            None
        };

        Ok(VembInput { key, element, raw })
    }
}

/// Output for Redis VEMB command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct VembOutput {
    vector: Option<Vec<f64>>,
}

impl VembOutput {
    pub fn new(vector: Option<Vec<f64>>) -> Self {
        Self { vector }
    }

    pub fn vector(&self) -> Option<&[f64]> {
        self.vector.as_deref()
    }

    pub fn exists(&self) -> bool {
        self.vector.is_some()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let vector = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut vec = Vec::with_capacity(arr.len());
                    for item in arr {
                        vec.push(Self::resp2_to_f64(item)?);
                    }
                    Some(vec)
                }
                Resp2Frame::BulkString(b) => Some(Self::parse_raw_bytes(&b)?),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected VEMB response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut vec = Vec::with_capacity(data.len());
                    for item in data {
                        vec.push(Self::resp3_to_f64(item)?);
                    }
                    Some(vec)
                }
                Resp3Frame::BlobString { data, .. } => Some(Self::parse_raw_bytes(&data)?),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected VEMB response: {:?}", other)));
                }
            },
        };

        Ok(Self { vector })
    }

    fn resp2_to_f64(frame: Resp2Frame) -> Result<f64, EpError> {
        match frame {
            Resp2Frame::BulkString(s) => String::from_utf8(s).map_err(EpError::parse)?.parse::<f64>().map_err(EpError::parse),
            Resp2Frame::Integer(n) => Ok(n as f64),
            other => Err(EpError::parse(format!("cannot convert to f64: {:?}", other))),
        }
    }

    fn resp3_to_f64(frame: Resp3Frame) -> Result<f64, EpError> {
        match frame {
            Resp3Frame::Double { data, .. } => Ok(data),
            Resp3Frame::Number { data, .. } => Ok(data as f64),
            Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?.parse::<f64>().map_err(EpError::parse),
            other => Err(EpError::parse(format!("cannot convert to f64: {:?}", other))),
        }
    }

    fn parse_raw_bytes(bytes: &[u8]) -> Result<Vec<f64>, EpError> {
        if !bytes.len().is_multiple_of(4) {
            return Err(EpError::parse("RAW vector bytes must be multiple of 4"));
        }
        let mut vec = Vec::with_capacity(bytes.len() / 4);
        for chunk in bytes.chunks(4) {
            let arr: [u8; 4] = chunk.try_into().map_err(EpError::parse)?;
            vec.push(f32::from_le_bytes(arr) as f64);
        }
        Ok(vec)
    }
}

impl Serialize for VembOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("VembOutput", 1)?;
        state.serialize_field("vector", &self.vector)?;
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
            let input = VembInput::new("myvset", "elem1");
            assert_eq!(input.command().to_vec(), b"*3\r\n$4\r\nVEMB\r\n$6\r\nmyvset\r\n$5\r\nelem1\r\n");
        }

        #[test]
        fn test_encode_command_with_raw() {
            let input = VembInput::new("myvset", "elem1").with_raw(true);
            assert_eq!(input.command().to_vec(), b"*4\r\n$4\r\nVEMB\r\n$6\r\nmyvset\r\n$5\r\nelem1\r\n$3\r\nRAW\r\n");
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = VembOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = VembOutput::decode(b"_\r\n").unwrap();
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = VembOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("elem".into())];
            let input = VembInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = VembInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vemb_nonexistent() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&VembInput::new("missing", "elem").command()).await.expect("raw failed");
                    let output = VembOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vemb_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;
            let result = ctx.raw(&VembInput::new("missing", "elem").command()).await.expect("raw failed");
            let output = VembOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());
            ctx.stop().await;
        }
    }
}
