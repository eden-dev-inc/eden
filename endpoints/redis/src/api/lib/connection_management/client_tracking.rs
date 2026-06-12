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

const API_INFO: ApiInfo<RedisApi, ClientTrackingInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClientTracking,
    "Controls server-assisted client-side caching for the connection",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `CLIENT TRACKING`
/// https://redis.io/docs/latest/commands/client-tracking/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientTrackingInput {
    tracking: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    redirect: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prefix: Option<Vec<RedisJsonValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bcast: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    optin: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    optout: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    noloop: Option<bool>,
}

impl Serialize for ClientTrackingInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClientTrackingInput", 8)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("tracking", &self.tracking)?;
        if let Some(r) = &self.redirect {
            state.serialize_field("redirect", r)?;
        }
        if let Some(p) = &self.prefix {
            state.serialize_field("prefix", p)?;
        }
        if let Some(b) = &self.bcast {
            state.serialize_field("bcast", b)?;
        }
        if let Some(o) = &self.optin {
            state.serialize_field("optin", o)?;
        }
        if let Some(o) = &self.optout {
            state.serialize_field("optout", o)?;
        }
        if let Some(n) = &self.noloop {
            state.serialize_field("noloop", n)?;
        }
        state.end()
    }
}

impl_redis_operation!(ClientTrackingInput, API_INFO, { tracking, redirect, prefix, bcast, optin, optout, noloop });

impl RedisCommandInput for ClientTrackingInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(if self.tracking { "ON" } else { "OFF" });
        if let Some(r) = &self.redirect {
            command.arg("REDIRECT").arg(r);
        }
        if let Some(p) = &self.prefix {
            for px in p {
                command.arg("PREFIX").arg(px);
            }
        }
        if self.bcast == Some(true) {
            command.arg("BCAST");
        }
        if self.optin == Some(true) {
            command.arg("OPTIN");
        }
        if self.optout == Some(true) {
            command.arg("OPTOUT");
        }
        if self.noloop == Some(true) {
            command.arg("NOLOOP");
        }
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("CLIENT TRACKING requires at least 1 argument"));
        }

        let tracking = match &args[0] {
            RedisJsonValue::String(s) => s.to_uppercase() == "ON",
            _ => return Err(EpError::request("First argument must be ON or OFF")),
        };

        let mut redirect = None;
        let mut prefix = None;
        let mut bcast = None;
        let mut optin = None;
        let mut optout = None;
        let mut noloop = None;
        let mut i = 1;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "REDIRECT" if i + 1 < args.len() => {
                        redirect = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "PREFIX" if i + 1 < args.len() => {
                        let mut px: Vec<RedisJsonValue> = prefix.unwrap_or_default();
                        px.push(args[i + 1].clone());
                        prefix = Some(px);
                        i += 2;
                    }
                    "BCAST" => {
                        bcast = Some(true);
                        i += 1;
                    }
                    "OPTIN" => {
                        optin = Some(true);
                        i += 1;
                    }
                    "OPTOUT" => {
                        optout = Some(true);
                        i += 1;
                    }
                    "NOLOOP" => {
                        noloop = Some(true);
                        i += 1;
                    }
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }

        Ok(Self { tracking, redirect, prefix, bcast, optin, optout, noloop })
    }
}

/// Output for Redis CLIENT TRACKING command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientTrackingOutput {
    result: String,
}

impl ClientTrackingOutput {
    pub fn is_ok(&self) -> bool {
        self.result == "OK"
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::SimpleString(s)) => String::from_utf8(s).map_err(EpError::parse)?,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleString { data, .. }) => String::from_utf8(data).map_err(EpError::parse)?,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
        };

        Ok(Self { result })
    }
}

impl Serialize for ClientTrackingOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientTrackingOutput", 1)?;
        state.serialize_field("result", &self.result)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_on() {
            let input = ClientTrackingInput {
                tracking: true,
                redirect: None,
                prefix: None,
                bcast: None,
                optin: None,
                optout: None,
                noloop: None,
            };
            assert!(input.command().windows(2).any(|w| w == b"ON"));
        }

        #[test]
        fn test_decode_ok() {
            assert!(ClientTrackingOutput::decode(b"+OK\r\n").unwrap().is_ok());
        }
    }
}
