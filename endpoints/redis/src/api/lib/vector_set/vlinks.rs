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

const API_INFO: ApiInfo<RedisApi, VlinksInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Vlinks,
    "Return the neighbors of an element at each layer in HNSW graph",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `VLINKS`
/// https://redis.io/docs/latest/commands/vlinks/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct VlinksInput {
    key: RedisKey,
    element: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    with_scores: Option<bool>,
}

impl VlinksInput {
    pub fn new(key: impl Into<RedisKey>, element: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), element: element.into(), with_scores: None }
    }

    pub fn with_scores(mut self) -> Self {
        self.with_scores = Some(true);
        self
    }
}

impl Serialize for VlinksInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.with_scores.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("VlinksInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("element", &self.element)?;
        if let Some(with_scores) = &self.with_scores {
            state.serialize_field("with_scores", with_scores)?;
        }
        state.end()
    }
}

impl_redis_operation!(VlinksInput, API_INFO, { key, element, with_scores });

impl RedisCommandInput for VlinksInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.element);
        if self.with_scores.is_some() {
            command.arg("WITHSCORES");
        }
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("VLINKS requires at least 2 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let element = args[1].clone();
        let with_scores = if args.len() > 2 {
            if let RedisJsonValue::String(s) = &args[2] {
                if s.to_uppercase() == "WITHSCORES" { Some(true) } else { None }
            } else {
                None
            }
        } else {
            None
        };

        Ok(VlinksInput { key, element, with_scores })
    }
}

/// Output for Redis VLINKS command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct VlinksOutput {
    /// Neighbors at each layer, None if element doesn't exist
    layers: Option<Vec<Vec<RedisJsonValue>>>,
}

impl VlinksOutput {
    pub fn new(layers: Option<Vec<Vec<RedisJsonValue>>>) -> Self {
        Self { layers }
    }

    pub fn layers(&self) -> Option<&[Vec<RedisJsonValue>]> {
        self.layers.as_deref()
    }

    pub fn exists(&self) -> bool {
        self.layers.is_some()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let layers = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut layers = Vec::new();
                    for item in arr {
                        if let Resp2Frame::Array(layer_arr) = item {
                            let layer: Vec<RedisJsonValue> = layer_arr.into_iter().map(Self::resp2_to_json).collect::<Result<_, _>>()?;
                            layers.push(layer);
                        }
                    }
                    Some(layers)
                }
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected VLINKS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut layers = Vec::new();
                    for item in data {
                        if let Resp3Frame::Array { data: layer_data, .. } = item {
                            let layer: Vec<RedisJsonValue> = layer_data.into_iter().map(Self::resp3_to_json).collect::<Result<_, _>>()?;
                            layers.push(layer);
                        }
                    }
                    Some(layers)
                }
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected VLINKS response: {:?}", other)));
                }
            },
        };

        Ok(Self { layers })
    }

    fn resp2_to_json(frame: Resp2Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp2Frame::BulkString(s) => Ok(RedisJsonValue::String(String::from_utf8(s).map_err(EpError::parse)?)),
            Resp2Frame::Integer(n) => Ok(RedisJsonValue::Integer(n)),
            Resp2Frame::Null => Ok(RedisJsonValue::Null),
            other => Err(EpError::parse(format!("unexpected frame: {:?}", other))),
        }
    }

    fn resp3_to_json(frame: Resp3Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } => Ok(RedisJsonValue::String(String::from_utf8(data).map_err(EpError::parse)?)),
            Resp3Frame::Number { data, .. } => Ok(RedisJsonValue::Integer(data)),
            Resp3Frame::Double { data, .. } => Ok(RedisJsonValue::Float(data)),
            Resp3Frame::Null => Ok(RedisJsonValue::Null),
            other => Err(EpError::parse(format!("unexpected frame: {:?}", other))),
        }
    }
}

impl Serialize for VlinksOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("VlinksOutput", 1)?;
        state.serialize_field("layers", &self.layers)?;
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
            let input = VlinksInput::new("myvset", "elem1");
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nVLINKS\r\n$6\r\nmyvset\r\n$5\r\nelem1\r\n");
        }

        #[test]
        fn test_encode_command_with_scores() {
            let input = VlinksInput::new("myvset", "elem1").with_scores();
            assert_eq!(
                input.command().to_vec(),
                b"*4\r\n$6\r\nVLINKS\r\n$6\r\nmyvset\r\n$5\r\nelem1\r\n$10\r\nWITHSCORES\r\n"
            );
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = VlinksOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = VlinksOutput::decode(b"_\r\n").unwrap();
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = VlinksOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("elem".into())];
            let input = VlinksInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = VlinksInput::decode(args).unwrap_err();
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
        async fn test_vlinks_nonexistent() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&VlinksInput::new("missing", "elem").command()).await.expect("raw failed");
                    let output = VlinksOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vlinks_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;
            let result = ctx.raw(&VlinksInput::new("missing", "elem").command()).await.expect("raw failed");
            let output = VlinksOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());
            ctx.stop().await;
        }
    }
}
