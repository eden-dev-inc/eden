#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO, etc.)
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

const API_INFO: ApiInfo<RedisApi, ZrangestoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zrangestore,
    "Stores a range of members from sorted set in a key",
    ReqType::Write,
    true,
);

#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZrangestoreInput {
    dst: RedisKey,
    src: RedisKey,
    min: RedisJsonValue,
    max: RedisJsonValue,
    by: Option<By>,
    rev: Option<bool>,
    limit: Option<Limit>,
}

impl ZrangestoreInput {
    pub fn new(dst: impl Into<RedisKey>, src: impl Into<RedisKey>, min: impl Into<RedisJsonValue>, max: impl Into<RedisJsonValue>) -> Self {
        Self {
            dst: dst.into(),
            src: src.into(),
            min: min.into(),
            max: max.into(),
            by: None,
            rev: None,
            limit: None,
        }
    }

    pub fn by_score(mut self) -> Self {
        self.by = Some(By::BYSCORE);
        self
    }
    pub fn by_lex(mut self) -> Self {
        self.by = Some(By::BYLEX);
        self
    }
    pub fn rev(mut self) -> Self {
        self.rev = Some(true);
        self
    }

    pub fn with_limit(mut self, offset: impl Into<RedisJsonValue>, count: impl Into<RedisJsonValue>) -> Self {
        self.limit = Some(Limit { offset: offset.into(), count: count.into() });
        self
    }
}

impl Serialize for ZrangestoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 5;
        if self.by.is_some() {
            fields += 1;
        }
        if self.rev.is_some() {
            fields += 1;
        }
        if self.limit.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("ZrangestoreInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("dst", &self.dst)?;
        state.serialize_field("src", &self.src)?;
        state.serialize_field("min", &self.min)?;
        state.serialize_field("max", &self.max)?;
        if let Some(by) = &self.by {
            state.serialize_field("by", by)?;
        }
        if let Some(rev) = &self.rev {
            state.serialize_field("rev", rev)?;
        }
        if let Some(limit) = &self.limit {
            state.serialize_field("limit", limit)?;
        }
        state.end()
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
struct Limit {
    offset: RedisJsonValue,
    count: RedisJsonValue,
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
enum By {
    #[default]
    BYSCORE,
    BYLEX,
}

impl_redis_operation!(ZrangestoreInput, API_INFO, {dst, src, min, max, by, rev, limit});

impl RedisCommandInput for ZrangestoreInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.dst.clone(), self.src.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.dst).arg(&self.src).arg(&self.min).arg(&self.max);
        if let Some(by) = &self.by {
            match by {
                By::BYSCORE => {
                    command.arg("BYSCORE");
                }
                By::BYLEX => {
                    command.arg("BYLEX");
                }
            }
        }
        if self.rev == Some(true) {
            command.arg("REV");
        }
        if let Some(limit) = &self.limit {
            command.arg("LIMIT").arg(&limit.offset).arg(&limit.count);
        }
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 4 {
            return Err(EpError::request("ZRANGESTORE requires at least 4 arguments"));
        }
        let dst = args[0].clone().try_into()?;
        let src = args[1].clone().try_into()?;
        let min = args[2].clone();
        let max = args[3].clone();
        let mut by = None;
        let mut rev = None;
        let mut limit = None;

        let mut i = 4;
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "BYSCORE" => {
                        by = Some(By::BYSCORE);
                        i += 1;
                    }
                    "BYLEX" => {
                        by = Some(By::BYLEX);
                        i += 1;
                    }
                    "REV" => {
                        rev = Some(true);
                        i += 1;
                    }
                    "LIMIT" if i + 2 < args.len() => {
                        limit = Some(Limit { offset: args[i + 1].clone(), count: args[i + 2].clone() });
                        i += 3;
                    }
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }
        Ok(Self { dst, src, min, max, by, rev, limit })
    }
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZrangestoreOutput {
    stored: i64,
}

impl ZrangestoreOutput {
    pub fn new(stored: i64) -> Self {
        Self { stored }
    }
    pub fn stored(&self) -> i64 {
        self.stored
    }
    pub fn any_stored(&self) -> bool {
        self.stored > 0
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        let stored = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => n,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => data,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("ZRANGESTORE must return integer")),
        };
        Ok(Self { stored })
    }
}

impl Serialize for ZrangestoreOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZrangestoreOutput", 1)?;
        state.serialize_field("stored", &self.stored)?;
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
            let input = ZrangestoreInput::new(
                RedisKey::String("dst".into()),
                RedisKey::String("src".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(-1),
            );
            let cmd = input.command();
            assert!(String::from_utf8_lossy(&cmd).contains("ZRANGESTORE"));
        }

        #[test]
        fn test_encode_command_with_byscore() {
            let input = ZrangestoreInput::new(
                RedisKey::String("dst".into()),
                RedisKey::String("src".into()),
                RedisJsonValue::String("-inf".into()),
                RedisJsonValue::String("+inf".into()),
            )
            .by_score();
            let cmd = input.command();
            assert!(String::from_utf8_lossy(&cmd).contains("BYSCORE"));
        }

        #[test]
        fn test_encode_command_with_rev() {
            let input = ZrangestoreInput::new(
                RedisKey::String("dst".into()),
                RedisKey::String("src".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(-1),
            )
            .rev();
            let cmd = input.command();
            assert!(String::from_utf8_lossy(&cmd).contains("REV"));
        }

        #[test]
        fn test_decode_output() {
            let output = ZrangestoreOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.stored(), 5);
            assert!(output.any_stored());
        }

        #[test]
        fn test_decode_output_zero() {
            let output = ZrangestoreOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.stored(), 0);
            assert!(!output.any_stored());
        }

        #[test]
        fn test_decode_error() {
            let err = ZrangestoreOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("dst".into()),
                RedisJsonValue::String("src".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(-1),
            ];
            let input = ZrangestoreInput::decode(args).unwrap();
            assert_eq!(input.dst, RedisKey::String("dst".into()));
            assert_eq!(input.src, RedisKey::String("src".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![
                RedisJsonValue::String("dst".into()),
                RedisJsonValue::String("src".into()),
                RedisJsonValue::Integer(0),
            ];
            let err = ZrangestoreInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 4"));
        }

        #[test]
        fn test_keys_returns_both_keys() {
            let input = ZrangestoreInput::new(
                RedisKey::String("dst".into()),
                RedisKey::String("src".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(-1),
            );
            assert_eq!(input.keys().len(), 2);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrangestore_basic() {
            // ZRANGESTORE requires Redis 6.2+
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nzrangestore_src\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nzrangestore_dst\r\n").await.expect("raw failed");
                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$15\r\nzrangestore_src\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n").await.expect("raw failed");

                    let result = ctx.raw(&ZrangestoreInput::new(RedisKey::String("zrangestore_dst".into()), RedisKey::String("zrangestore_src".into()), RedisJsonValue::Integer(0), RedisJsonValue::Integer(-1)).command()).await.expect("raw failed");
                    let output = ZrangestoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.stored(), 3);
                })
            }).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrangestore_byscore() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzrangestore_bysrc\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzrangestore_bydst\r\n").await.expect("raw failed");
                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$17\r\nzrangestore_bysrc\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n").await.expect("raw failed");

                    let result = ctx.raw(&ZrangestoreInput::new(RedisKey::String("zrangestore_bydst".into()), RedisKey::String("zrangestore_bysrc".into()), RedisJsonValue::String("1".into()), RedisJsonValue::String("2".into())).by_score().command()).await.expect("raw failed");
                    let output = ZrangestoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.stored(), 2);
                })
            }).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrangestore_empty_source() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzrangestore_nosrc\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzrangestore_nodst\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZrangestoreInput::new(
                                RedisKey::String("zrangestore_nodst".into()),
                                RedisKey::String("zrangestore_nosrc".into()),
                                RedisJsonValue::Integer(0),
                                RedisJsonValue::Integer(-1),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let output = ZrangestoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.stored(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrangestore_wrongtype() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$17\r\nzrangestore_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$20\r\nzrangestore_wrongdst\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZrangestoreInput::new(
                                RedisKey::String("zrangestore_wrongdst".into()),
                                RedisKey::String("zrangestore_wrong".into()),
                                RedisJsonValue::Integer(0),
                                RedisJsonValue::Integer(-1),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let err = ZrangestoreOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
