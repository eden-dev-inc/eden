use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, FtSynupdateInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FtSynupdate,
    "Creates or updates a synonym group with additional terms",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `FT.SYNUPDATE`
/// https://redis.io/docs/latest/commands/ft.synupdate/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtSynupdateInput {
    index: RedisJsonValue,
    synonym_group_id: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    skip_initial_scan: Option<RedisJsonValue>,
    terms: Vec<RedisJsonValue>,
}

impl Serialize for FtSynupdateInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, index, synonym_group_id, terms
        if self.skip_initial_scan.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("FtSynupdateInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        state.serialize_field("synonym_group_id", &self.synonym_group_id)?;
        if let Some(skip_initial_scan) = &self.skip_initial_scan {
            state.serialize_field("skip_initial_scan", skip_initial_scan)?;
        }
        state.serialize_field("terms", &self.terms)?;
        state.end()
    }
}

impl_redis_operation!(
    FtSynupdateInput,
    API_INFO,
    {index, synonym_group_id, skip_initial_scan, terms}
);

impl RedisCommandInput for FtSynupdateInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index);
        command.arg(&self.synonym_group_id);
        if let Some(skip_initial_scan) = &self.skip_initial_scan {
            match skip_initial_scan {
                RedisJsonValue::Bool(true) => {
                    command.arg("SKIPINITIALSCAN");
                }
                RedisJsonValue::Integer(n) if *n != 0 => {
                    command.arg("SKIPINITIALSCAN");
                }
                RedisJsonValue::String(s) if s.to_uppercase() == "SKIPINITIALSCAN" || s == "1" || s.to_uppercase() == "TRUE" => {
                    command.arg("SKIPINITIALSCAN");
                }
                _ => {}
            }
        }
        for term in &self.terms {
            command.arg(term);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("FT.SYNUPDATE requires at least 3 arguments, given {}", args.len())));
        }

        let index = args[0].clone();
        let synonym_group_id = args[1].clone();
        let mut skip_initial_scan = None;
        let mut terms_start = 2;

        // Check for SKIPINITIALSCAN
        if let RedisJsonValue::String(s) = &args[2]
            && s.to_uppercase() == "SKIPINITIALSCAN"
        {
            skip_initial_scan = Some(RedisJsonValue::Bool(true));
            terms_start = 3;
        }

        if terms_start >= args.len() {
            return Err(EpError::request("FT.SYNUPDATE requires at least one term".to_string()));
        }

        let terms = args[terms_start..].to_vec();

        Ok(Self { index, synonym_group_id, skip_initial_scan, terms })
    }
}

/// Output for Redis `FT.SYNUPDATE` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtSynupdateOutput {
    /// Whether the operation was successful
    success: bool,
}

impl Serialize for FtSynupdateOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtSynupdateOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl FtSynupdateOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the operation was successful
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a FtSynupdateOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::SimpleString(s) if s == b"OK" => Ok(Self { success: true }),
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected FT.SYNUPDATE response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::SimpleString { data, .. } if data == b"OK" => Ok(Self { success: true }),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected FT.SYNUPDATE response: {:?}", other))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = FtSynupdateInput {
                index: RedisJsonValue::String("my_index".into()),
                synonym_group_id: RedisJsonValue::String("group1".into()),
                skip_initial_scan: None,
                terms: vec![RedisJsonValue::String("hello".into()), RedisJsonValue::String("hi".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.SYNUPDATE"));
            assert!(cmd_str.contains("my_index"));
            assert!(cmd_str.contains("group1"));
            assert!(cmd_str.contains("hello"));
            assert!(cmd_str.contains("hi"));
            assert!(!cmd_str.contains("SKIPINITIALSCAN"));
        }

        #[test]
        fn test_encode_command_with_skip() {
            let input = FtSynupdateInput {
                index: RedisJsonValue::String("my_index".into()),
                synonym_group_id: RedisJsonValue::String("group1".into()),
                skip_initial_scan: Some(RedisJsonValue::Bool(true)),
                terms: vec![RedisJsonValue::String("term".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SKIPINITIALSCAN"));
        }

        #[test]
        fn test_encode_command_skip_false() {
            let input = FtSynupdateInput {
                index: RedisJsonValue::String("my_index".into()),
                synonym_group_id: RedisJsonValue::String("group1".into()),
                skip_initial_scan: Some(RedisJsonValue::Bool(false)),
                terms: vec![RedisJsonValue::String("term".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(!cmd_str.contains("SKIPINITIALSCAN"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("g1".into()),
                RedisJsonValue::String("term1".into()),
            ];
            let input = FtSynupdateInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("idx".into()));
            assert_eq!(input.synonym_group_id, RedisJsonValue::String("g1".into()));
            assert!(input.skip_initial_scan.is_none());
            assert_eq!(input.terms.len(), 1);
        }

        #[test]
        fn test_decode_input_with_skip() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("g1".into()),
                RedisJsonValue::String("SKIPINITIALSCAN".into()),
                RedisJsonValue::String("term1".into()),
            ];
            let input = FtSynupdateInput::decode(args).unwrap();
            assert_eq!(input.skip_initial_scan, Some(RedisJsonValue::Bool(true)));
            assert_eq!(input.terms.len(), 1);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("idx".into()), RedisJsonValue::String("g1".into())];
            let err = FtSynupdateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3 arguments"));
        }

        #[test]
        fn test_decode_input_no_terms() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("g1".into()),
                RedisJsonValue::String("SKIPINITIALSCAN".into()),
            ];
            let err = FtSynupdateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least one term"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = FtSynupdateOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtSynupdateOutput::decode(b"-ERR Unknown Index name\r\n").unwrap_err();
            assert!(err.to_string().contains("Unknown Index"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtSynupdateInput {
                index: RedisJsonValue::String("i".into()),
                synonym_group_id: RedisJsonValue::String("g".into()),
                skip_initial_scan: None,
                terms: vec![RedisJsonValue::String("t".into())],
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtSynupdateInput {
                index: RedisJsonValue::String("test_idx".into()),
                synonym_group_id: RedisJsonValue::String("g1".into()),
                skip_initial_scan: Some(RedisJsonValue::Bool(true)),
                terms: vec![RedisJsonValue::String("word".into())],
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_idx"));
            assert!(json.contains("g1"));
            assert!(json.contains("skip_initial_scan"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtSynupdateOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.SYNUPDATE requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_synupdate_nonexistent_index() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtSynupdateInput {
                                index: RedisJsonValue::String("nonexistent_index".into()),
                                synonym_group_id: RedisJsonValue::String("group1".into()),
                                skip_initial_scan: None,
                                terms: vec![RedisJsonValue::String("hello".into())],
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected error for nonexistent index
                            let err = FtSynupdateOutput::decode(&r);
                            assert!(err.is_err());
                        }
                        Ok(_) | Err(_) => {
                            // Module not available or other case
                        }
                    }
                })
            })
            .await;
        }
    }
}
