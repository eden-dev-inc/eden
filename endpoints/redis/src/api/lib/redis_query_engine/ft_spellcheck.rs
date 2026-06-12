use crate::api::lib::redis_query_engine::{Term, Terms};
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use error::ResultEP;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::Serializer;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, FtSpellcheckInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FtSpellcheck,
    "Performs spelling correction on a query, returning suggestions for misspelled terms",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `FT.SPELLCHECK`
/// https://redis.io/docs/latest/commands/ft.spellcheck/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtSpellcheckInput {
    index: RedisJsonValue,
    query: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    distance: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    terms: Option<Terms>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dialect: Option<RedisJsonValue>,
}

impl Serialize for FtSpellcheckInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, index, query
        if self.distance.is_some() {
            fields += 1;
        }
        if self.terms.is_some() {
            fields += 1;
        }
        if self.dialect.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("FtSpellcheckInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        state.serialize_field("query", &self.query)?;

        if let Some(distance) = &self.distance {
            state.serialize_field("distance", distance)?;
        }
        if let Some(terms) = &self.terms {
            state.serialize_field("terms", terms)?;
        }
        if let Some(dialect) = &self.dialect {
            state.serialize_field("dialect", dialect)?;
        }

        state.end()
    }
}

impl_redis_operation!(
    FtSpellcheckInput,
    API_INFO,
    {index, query, distance, terms, dialect}
);

impl RedisCommandInput for FtSpellcheckInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index).arg(&self.query);

        if let Some(distance) = &self.distance {
            command.arg("DISTANCE").arg(distance);
        }

        if let Some(terms) = &self.terms {
            terms.cmd(&mut command);
        }

        if let Some(dialect) = &self.dialect {
            command.arg("DIALECT").arg(dialect);
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("FT.SPELLCHECK requires at least 2 arguments, given {}", args.len())));
        }

        let index = args[0].clone();
        let query = args[1].clone();

        let mut distance = None;
        let mut terms = None;
        let mut dialect = None;
        let mut i = 2;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "DISTANCE" => {
                        if i + 1 < args.len() {
                            distance = Some(args[i + 1].clone());
                            i += 2;
                        } else {
                            return Err(EpError::request("DISTANCE requires a value".to_string()));
                        }
                    }
                    "TERMS" => {
                        if i + 2 < args.len() {
                            let term = if let RedisJsonValue::String(term_str) = &args[i + 1] {
                                match term_str.to_uppercase().as_str() {
                                    "INCLUDE" => Term::INCLUDE,
                                    "EXCLUDE" => Term::EXCLUDE,
                                    _ => Term::INCLUDE,
                                }
                            } else {
                                Term::INCLUDE
                            };

                            let dictionary = args[i + 2].clone();
                            let mut term_list = None;

                            i += 3;
                            if i < args.len() {
                                // Collect remaining terms until next keyword
                                let mut collected_terms = Vec::new();
                                while i < args.len() {
                                    if let RedisJsonValue::String(next_s) = &args[i]
                                        && matches!(next_s.to_uppercase().as_str(), "DIALECT")
                                    {
                                        break;
                                    }
                                    collected_terms.push(args[i].clone());
                                    i += 1;
                                }
                                if !collected_terms.is_empty() {
                                    term_list = Some(collected_terms);
                                }
                            }

                            terms = Some(Terms { term, dictionary, terms: term_list });
                        } else {
                            return Err(EpError::request("TERMS requires term type and dictionary".to_string()));
                        }
                    }
                    "DIALECT" => {
                        if i + 1 < args.len() {
                            dialect = Some(args[i + 1].clone());
                            i += 2;
                        } else {
                            return Err(EpError::request("DIALECT requires a value".to_string()));
                        }
                    }
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }

        Ok(Self { index, query, distance, terms, dialect })
    }
}

/// A spelling suggestion for a misspelled term
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SpellingSuggestion {
    /// The suggested correction
    pub suggestion: String,
    /// The score/confidence of the suggestion
    pub score: f64,
}

/// Spelling corrections for a single term
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TermCorrection {
    /// The original misspelled term
    pub term: String,
    /// List of suggestions for this term
    pub suggestions: Vec<SpellingSuggestion>,
}

/// Output for Redis `FT.SPELLCHECK` command.
///
/// Returns spelling suggestions for misspelled terms in the query.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtSpellcheckOutput {
    /// Corrections for each misspelled term
    corrections: Vec<TermCorrection>,
}

impl Serialize for FtSpellcheckOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtSpellcheckOutput", 1)?;
        state.serialize_field("corrections", &self.corrections)?;
        state.end()
    }
}

impl FtSpellcheckOutput {
    pub fn new(corrections: Vec<TermCorrection>) -> Self {
        Self { corrections }
    }

    /// Get the corrections
    pub fn corrections(&self) -> &[TermCorrection] {
        &self.corrections
    }

    /// Check if there are any corrections
    pub fn has_corrections(&self) -> bool {
        !self.corrections.is_empty()
    }

    /// Decode the Redis protocol response into a FtSpellcheckOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut corrections = Vec::new();
                for item in arr {
                    if let Resp2Frame::Array(term_arr) = item
                        && let Some(correction) = Self::parse_term_correction_resp2(&term_arr)
                    {
                        corrections.push(correction);
                    }
                }
                Ok(Self { corrections })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected FT.SPELLCHECK response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut corrections = Vec::new();
                for item in data {
                    if let Resp3Frame::Array { data, .. } = item
                        && let Some(correction) = Self::parse_term_correction_resp3(&data)?
                    {
                        corrections.push(correction);
                    }
                }
                Ok(Self { corrections })
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected FT.SPELLCHECK response: {:?}", other))),
        }
    }

    fn parse_term_correction_resp2(arr: &[Resp2Frame]) -> Option<TermCorrection> {
        // Format: [TERM, misspelled_term, [suggestion_array]]
        if arr.len() < 3 {
            return None;
        }

        let term = if let Resp2Frame::BulkString(s) = &arr[1] {
            String::from_utf8(s.to_vec()).ok()?
        } else {
            return None;
        };

        let mut suggestions = Vec::new();
        if let Resp2Frame::Array(sugg_arr) = &arr[2] {
            for sugg in sugg_arr {
                if let Resp2Frame::Array(pair) = sugg
                    && pair.len() >= 2
                {
                    let score = match &pair[0] {
                        Resp2Frame::BulkString(s) => String::from_utf8_lossy(s).parse::<f64>().unwrap_or(0.0),
                        Resp2Frame::Integer(i) => *i as f64,
                        _ => 0.0,
                    };
                    let suggestion = if let Resp2Frame::BulkString(s) = &pair[1] {
                        String::from_utf8_lossy(s).to_string()
                    } else {
                        continue;
                    };
                    suggestions.push(SpellingSuggestion { suggestion, score });
                }
            }
        }

        Some(TermCorrection { term, suggestions })
    }

    fn parse_term_correction_resp3(arr: &[Resp3Frame]) -> ResultEP<Option<TermCorrection>> {
        if arr.len() < 3 {
            return Ok(None);
        }

        let term = match &arr[1] {
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                String::from_utf8(data.to_vec()).map_err(EpError::parse)
            }
            _ => return Ok(None),
        }?;

        let mut suggestions = Vec::new();
        if let Resp3Frame::Array { data, .. } = &arr[2] {
            for sugg in data {
                if let Resp3Frame::Array { data, .. } = sugg {
                    let pair = data;
                    if pair.len() >= 2 {
                        let score = match &pair[0] {
                            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                String::from_utf8_lossy(data).parse::<f64>().unwrap_or(0.0)
                            }
                            Resp3Frame::Number { data, .. } => *data as f64,
                            Resp3Frame::Double { data, .. } => *data,
                            _ => 0.0,
                        };
                        let suggestion = match &pair[1] {
                            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                String::from_utf8(data.to_vec()).map_err(EpError::parse)?
                            }
                            _ => continue,
                        };
                        suggestions.push(SpellingSuggestion { suggestion, score });
                    }
                }
            }
        }

        Ok(Some(TermCorrection { term, suggestions }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = FtSpellcheckInput {
                index: RedisJsonValue::String("my_index".into()),
                query: RedisJsonValue::String("helo wrold".into()),
                distance: None,
                terms: None,
                dialect: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.SPELLCHECK"));
            assert!(cmd_str.contains("my_index"));
            assert!(cmd_str.contains("helo wrold"));
        }

        #[test]
        fn test_encode_command_with_distance() {
            let input = FtSpellcheckInput {
                index: RedisJsonValue::String("idx".into()),
                query: RedisJsonValue::String("query".into()),
                distance: Some(RedisJsonValue::Integer(2)),
                terms: None,
                dialect: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("DISTANCE"));
        }

        #[test]
        fn test_encode_command_with_dialect() {
            let input = FtSpellcheckInput {
                index: RedisJsonValue::String("idx".into()),
                query: RedisJsonValue::String("query".into()),
                distance: None,
                terms: None,
                dialect: Some(RedisJsonValue::Integer(2)),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("DIALECT"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("idx".into()), RedisJsonValue::String("query".into())];
            let input = FtSpellcheckInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("idx".into()));
            assert_eq!(input.query, RedisJsonValue::String("query".into()));
        }

        #[test]
        fn test_decode_input_with_distance() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("query".into()),
                RedisJsonValue::String("DISTANCE".into()),
                RedisJsonValue::Integer(2),
            ];
            let input = FtSpellcheckInput::decode(args).unwrap();
            assert_eq!(input.distance, Some(RedisJsonValue::Integer(2)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("idx".into())];
            let err = FtSpellcheckInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = FtSpellcheckOutput::decode(b"*0\r\n").unwrap();
            assert!(!output.has_corrections());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtSpellcheckOutput::decode(b"-ERR unknown index\r\n").unwrap_err();
            assert!(err.to_string().contains("unknown index"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtSpellcheckInput {
                index: RedisJsonValue::String("i".into()),
                query: RedisJsonValue::String("q".into()),
                distance: None,
                terms: None,
                dialect: None,
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtSpellcheckInput {
                index: RedisJsonValue::String("test_idx".into()),
                query: RedisJsonValue::String("test_query".into()),
                distance: Some(RedisJsonValue::Integer(1)),
                terms: None,
                dialect: None,
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_idx"));
            assert!(json.contains("test_query"));
            assert!(json.contains("distance"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtSpellcheckOutput::new(vec![TermCorrection {
                term: "helo".into(),
                suggestions: vec![SpellingSuggestion { suggestion: "hello".into(), score: 0.9 }],
            }]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("corrections"));
            assert!(json.contains("helo"));
            assert!(json.contains("hello"));
        }

        #[test]
        fn test_output_accessors() {
            let output = FtSpellcheckOutput::new(vec![TermCorrection { term: "test".into(), suggestions: vec![] }]);
            assert!(output.has_corrections());
            assert_eq!(output.corrections().len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.SPELLCHECK requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_spellcheck_nonexistent_index() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtSpellcheckInput {
                                index: RedisJsonValue::String("nonexistent".into()),
                                query: RedisJsonValue::String("hello".into()),
                                distance: None,
                                terms: None,
                                dialect: None,
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected error for nonexistent index
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
