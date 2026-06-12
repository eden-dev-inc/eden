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

const API_INFO: ApiInfo<RedisApi, FtSuggetInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FtSugget, "Gets completion suggestions for a prefix", ReqType::Read, true);

/// See official Redis documentation for `FT.SUGGET`
/// https://redis.io/docs/latest/commands/ft.sugget/
///
/// Official example: `sug hell FUZZY MAX 3 WITHSCORES`
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema, PartialEq)]
pub struct FtSuggetInput {
    #[schema(example = sug)]
    pub(crate) key: RedisKey,
    #[schema(example = hell)]
    pub(crate) prefix: RedisJsonValue,
    #[schema(example = true)]
    pub(crate) fuzzy: Option<bool>,
    #[schema(example = true)]
    pub(crate) with_scores: Option<bool>,
    pub(crate) with_payloads: Option<bool>,
    #[schema(example = 3)]
    pub(crate) max: Option<RedisJsonValue>,
}

impl Serialize for FtSuggetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut field_count = 3; // type, key, prefix

        if self.fuzzy.is_some() {
            field_count += 1;
        }
        if self.with_scores.is_some() {
            field_count += 1;
        }
        if self.with_payloads.is_some() {
            field_count += 1;
        }
        if self.max.is_some() {
            field_count += 1;
        }

        let mut state = serializer.serialize_struct("FtSuggetInput", field_count)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("prefix", &self.prefix)?;

        if let Some(fuzzy) = &self.fuzzy {
            state.serialize_field("fuzzy", fuzzy)?;
        }

        if let Some(with_scores) = &self.with_scores {
            state.serialize_field("with_scores", with_scores)?;
        }

        if let Some(with_payloads) = &self.with_payloads {
            state.serialize_field("with_payloads", with_payloads)?;
        }

        if let Some(max) = &self.max {
            state.serialize_field("max", max)?;
        }

        state.end()
    }
}

impl_redis_operation!(
    FtSuggetInput,
    API_INFO,
    {key, prefix, fuzzy, with_scores, with_payloads, max}
);

impl RedisCommandInput for FtSuggetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.prefix);

        if let Some(fuzzy) = &self.fuzzy
            && *fuzzy
        {
            command.arg("FUZZY");
        }

        if let Some(with_scores) = &self.with_scores
            && *with_scores
        {
            command.arg("WITHSCORES");
        }

        if let Some(with_payloads) = &self.with_payloads
            && *with_payloads
        {
            command.arg("WITHPAYLOADS");
        }

        if let Some(max) = &self.max {
            command.arg("MAX").arg(max);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("FT.SUGGET requires at least 2 arguments, found {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let prefix = args[1].clone();
        let mut fuzzy = None;
        let mut with_scores = None;
        let mut with_payloads = None;
        let mut max = None;

        let mut i = 2;
        while i < args.len() {
            match &args[i] {
                RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                    "FUZZY" => {
                        fuzzy = Some(true);
                        i += 1;
                    }
                    "WITHSCORES" => {
                        with_scores = Some(true);
                        i += 1;
                    }
                    "WITHPAYLOADS" => {
                        with_payloads = Some(true);
                        i += 1;
                    }
                    "MAX" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("MAX requires a value"));
                        }
                        max = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => {
                        return Err(EpError::request(format!("Unknown FT.SUGGET option: {}", s)));
                    }
                },
                _ => {
                    return Err(EpError::request("FT.SUGGET options must be strings"));
                }
            }
        }

        Ok(FtSuggetInput { key, prefix, fuzzy, with_scores, with_payloads, max })
    }
}

/// A single suggestion returned by FT.SUGGET
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema, PartialEq)]
pub struct Suggestion {
    /// The suggestion string
    pub string: String,
    /// The score (present if WITHSCORES was used)
    pub score: Option<f64>,
    /// The payload (present if WITHPAYLOADS was used)
    pub payload: Option<String>,
}

impl Serialize for Suggestion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut field_count = 1;
        if self.score.is_some() {
            field_count += 1;
        }
        if self.payload.is_some() {
            field_count += 1;
        }

        let mut state = serializer.serialize_struct("Suggestion", field_count)?;
        state.serialize_field("string", &self.string)?;
        if let Some(score) = &self.score {
            state.serialize_field("score", score)?;
        }
        if let Some(payload) = &self.payload {
            state.serialize_field("payload", payload)?;
        }
        state.end()
    }
}

/// Output for Redis FT.SUGGET command
///
/// Returns a list of suggestions matching the prefix, or an empty list if
/// no matches are found or the key doesn't exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtSuggetOutput {
    /// The list of matching suggestions
    suggestions: Vec<Suggestion>,
}

impl FtSuggetOutput {
    pub fn new(suggestions: Vec<Suggestion>) -> Self {
        Self { suggestions }
    }

    /// Get the suggestions
    pub fn suggestions(&self) -> &[Suggestion] {
        &self.suggestions
    }

    /// Check if any suggestions were returned
    pub fn is_empty(&self) -> bool {
        self.suggestions.is_empty()
    }

    /// Get the number of suggestions
    pub fn len(&self) -> usize {
        self.suggestions.len()
    }

    /// Decode the Redis protocol response into a FtSuggetOutput
    ///
    /// The response format depends on the options used:
    /// - Basic: [string1, string2, ...]
    /// - WITHSCORES: [string1, score1, string2, score2, ...]
    /// - WITHPAYLOADS: [string1, payload1, string2, payload2, ...]
    /// - WITHSCORES + WITHPAYLOADS: [string1, score1, payload1, string2, score2, payload2, ...]
    pub fn decode(bytes: &[u8], with_scores: bool, with_payloads: bool) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let elements: Vec<DecoderRespFrame> = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr.into_iter().map(DecoderRespFrame::Resp2).collect(),
                Resp2Frame::Null => {
                    return Ok(Self { suggestions: vec![] });
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FT.SUGGET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data.into_iter().map(DecoderRespFrame::Resp3).collect(),
                Resp3Frame::Null => {
                    return Ok(Self { suggestions: vec![] });
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FT.SUGGET response: {:?}", other)));
                }
            },
        };

        // Calculate stride based on what data is included
        let stride = 1 + (with_scores as usize) + (with_payloads as usize);
        let mut suggestions = Vec::new();
        let mut i = 0;

        while i < elements.len() {
            let string = Self::extract_string(&elements[i])?;

            let score = if with_scores && i + 1 < elements.len() {
                Some(Self::extract_score(&elements[i + 1])?)
            } else {
                None
            };

            let payload_offset = if with_scores { 2 } else { 1 };
            let payload = if with_payloads && i + payload_offset < elements.len() {
                Some(Self::extract_string(&elements[i + payload_offset])?)
            } else {
                None
            };

            suggestions.push(Suggestion { string, score, payload });

            i += stride;
        }

        Ok(Self { suggestions })
    }

    fn extract_string(frame: &DecoderRespFrame) -> Result<String, EpError> {
        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::BulkString(bytes)) | DecoderRespFrame::Resp2(Resp2Frame::SimpleString(bytes)) => {
                String::from_utf8(bytes.clone()).map_err(EpError::parse)
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. })
            | DecoderRespFrame::Resp3(Resp3Frame::SimpleString { data, .. }) => String::from_utf8(data.clone()).map_err(EpError::parse),
            other => Err(EpError::parse(format!("expected string, got: {:?}", other))),
        }
    }

    fn extract_score(frame: &DecoderRespFrame) -> Result<f64, EpError> {
        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::BulkString(bytes)) | DecoderRespFrame::Resp2(Resp2Frame::SimpleString(bytes)) => {
                let s = String::from_utf8(bytes.clone()).map_err(EpError::parse)?;
                s.parse::<f64>().map_err(|e| EpError::parse(e.to_string()))
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => {
                let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                s.parse::<f64>().map_err(|e| EpError::parse(e.to_string()))
            }
            DecoderRespFrame::Resp3(Resp3Frame::Double { data, .. }) => Ok(*data),
            other => Err(EpError::parse(format!("expected score, got: {:?}", other))),
        }
    }
}

impl Serialize for FtSuggetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("FtSuggetOutput", 1)?;
        state.serialize_field("suggestions", &self.suggestions)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;
        use crate::protocol::RedisProtocol;
        use endpoint_types::request::EndpointRequestInput;
        use endpoint_types::request::EpRequest;
        use std::convert::TryInto;

        #[test]
        fn test_encode_command_basic() {
            let input = FtSuggetInput {
                key: RedisKey::String("mydict".into()),
                prefix: RedisJsonValue::String("hel".into()),
                fuzzy: None,
                with_scores: None,
                with_payloads: None,
                max: None,
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$9\r\nFT.SUGGET\r\n$6\r\nmydict\r\n$3\r\nhel\r\n");
        }

        #[test]
        fn test_encode_command_with_fuzzy() {
            let input = FtSuggetInput {
                key: RedisKey::String("mydict".into()),
                prefix: RedisJsonValue::String("hel".into()),
                fuzzy: Some(true),
                with_scores: None,
                with_payloads: None,
                max: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"FUZZY"));
        }

        #[test]
        fn test_encode_command_fuzzy_false_not_included() {
            let input = FtSuggetInput {
                key: RedisKey::String("mydict".into()),
                prefix: RedisJsonValue::String("hel".into()),
                fuzzy: Some(false),
                with_scores: None,
                with_payloads: None,
                max: None,
            };
            let cmd = input.command();
            assert!(!cmd.windows(5).any(|w| w == b"FUZZY"));
        }

        #[test]
        fn test_encode_command_with_withscores() {
            let input = FtSuggetInput {
                key: RedisKey::String("mydict".into()),
                prefix: RedisJsonValue::String("hel".into()),
                fuzzy: None,
                with_scores: Some(true),
                with_payloads: None,
                max: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(10).any(|w| w == b"WITHSCORES"));
        }

        #[test]
        fn test_encode_command_with_withpayloads() {
            let input = FtSuggetInput {
                key: RedisKey::String("mydict".into()),
                prefix: RedisJsonValue::String("hel".into()),
                fuzzy: None,
                with_scores: None,
                with_payloads: Some(true),
                max: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(12).any(|w| w == b"WITHPAYLOADS"));
        }

        #[test]
        fn test_encode_command_with_max() {
            let input = FtSuggetInput {
                key: RedisKey::String("mydict".into()),
                prefix: RedisJsonValue::String("hel".into()),
                fuzzy: None,
                with_scores: None,
                with_payloads: None,
                max: Some(RedisJsonValue::Integer(5)),
            };
            let cmd = input.command();
            assert!(cmd.windows(3).any(|w| w == b"MAX"));
        }

        #[test]
        fn test_decode_null_response() {
            let output = FtSuggetOutput::decode(b"*-1\r\n", false, false).unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_empty_array() {
            let output = FtSuggetOutput::decode(b"*0\r\n", false, false).unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_basic_suggestions() {
            // Array: ["hello", "help"]
            let resp = b"*2\r\n$5\r\nhello\r\n$4\r\nhelp\r\n";
            let output = FtSuggetOutput::decode(resp, false, false).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.suggestions()[0].string, "hello");
            assert_eq!(output.suggestions()[1].string, "help");
            assert!(output.suggestions()[0].score.is_none());
            assert!(output.suggestions()[0].payload.is_none());
        }

        #[test]
        fn test_decode_with_scores() {
            // Array: ["hello", "1.5", "help", "2.0"]
            let resp = b"*4\r\n$5\r\nhello\r\n$3\r\n1.5\r\n$4\r\nhelp\r\n$3\r\n2.0\r\n";
            let output = FtSuggetOutput::decode(resp, true, false).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.suggestions()[0].string, "hello");
            assert_eq!(output.suggestions()[0].score, Some(1.5));
            assert_eq!(output.suggestions()[1].string, "help");
            assert_eq!(output.suggestions()[1].score, Some(2.0));
        }

        #[test]
        fn test_decode_with_payloads() {
            // Array: ["hello", "payload1", "help", "payload2"]
            let resp = b"*4\r\n$5\r\nhello\r\n$8\r\npayload1\r\n$4\r\nhelp\r\n$8\r\npayload2\r\n";
            let output = FtSuggetOutput::decode(resp, false, true).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.suggestions()[0].string, "hello");
            assert_eq!(output.suggestions()[0].payload, Some("payload1".to_string()));
            assert_eq!(output.suggestions()[1].string, "help");
            assert_eq!(output.suggestions()[1].payload, Some("payload2".to_string()));
        }

        #[test]
        fn test_decode_with_scores_and_payloads() {
            // Array: ["hello", "1.5", "payload1", "help", "2.0", "payload2"]
            let resp = b"*6\r\n$5\r\nhello\r\n$3\r\n1.5\r\n$8\r\npayload1\r\n$4\r\nhelp\r\n$3\r\n2.0\r\n$8\r\npayload2\r\n";
            let output = FtSuggetOutput::decode(resp, true, true).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.suggestions()[0].string, "hello");
            assert_eq!(output.suggestions()[0].score, Some(1.5));
            assert_eq!(output.suggestions()[0].payload, Some("payload1".to_string()));
            assert_eq!(output.suggestions()[1].string, "help");
            assert_eq!(output.suggestions()[1].score, Some(2.0));
            assert_eq!(output.suggestions()[1].payload, Some("payload2".to_string()));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = FtSuggetOutput::decode(b"-ERR unknown key\r\n", false, false).unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("mydict".into()), RedisJsonValue::String("hel".into())];
            let input = FtSuggetInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mydict".into()));
            assert_eq!(input.prefix, RedisJsonValue::String("hel".into()));
            assert_eq!(input.fuzzy, None);
            assert_eq!(input.with_scores, None);
            assert_eq!(input.with_payloads, None);
            assert_eq!(input.max, None);
        }

        #[test]
        fn test_decode_input_with_all_options() {
            let args = vec![
                RedisJsonValue::String("mydict".into()),
                RedisJsonValue::String("hel".into()),
                RedisJsonValue::String("FUZZY".into()),
                RedisJsonValue::String("WITHSCORES".into()),
                RedisJsonValue::String("WITHPAYLOADS".into()),
                RedisJsonValue::String("MAX".into()),
                RedisJsonValue::Integer(5),
            ];
            let input = FtSuggetInput::decode(args).unwrap();
            assert_eq!(input.fuzzy, Some(true));
            assert_eq!(input.with_scores, Some(true));
            assert_eq!(input.with_payloads, Some(true));
            assert_eq!(input.max, Some(RedisJsonValue::Integer(5)));
        }

        #[test]
        fn test_decode_input_insufficient_args() {
            let args = vec![RedisJsonValue::String("mydict".into())];
            let err = FtSuggetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_max_missing_value() {
            let args = vec![
                RedisJsonValue::String("mydict".into()),
                RedisJsonValue::String("hel".into()),
                RedisJsonValue::String("MAX".into()),
            ];
            let err = FtSuggetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("MAX requires a value"));
        }

        #[test]
        fn test_decode_input_unknown_option() {
            let args = vec![
                RedisJsonValue::String("mydict".into()),
                RedisJsonValue::String("hel".into()),
                RedisJsonValue::String("UNKNOWN".into()),
            ];
            let err = FtSuggetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Unknown FT.SUGGET option"));
        }

        #[test]
        fn test_serialize_round_trip_ep_request() {
            let input = FtSuggetInput {
                key: RedisKey::String("test_key".into()),
                prefix: RedisJsonValue::String("hell".into()),
                fuzzy: Some(true),
                with_scores: Some(true),
                with_payloads: Some(true),
                max: Some(RedisJsonValue::Integer(5)),
            };

            let serialized = serde_json::to_value(&input).unwrap();

            assert_eq!(serialized["type"], "FT.SUGGET");
            assert_eq!(serialized["key"], "test_key");
            assert_eq!(serialized["prefix"], "hell");
            assert!(serialized["fuzzy"].as_bool().unwrap_or(false));
            assert!(serialized["with_scores"].as_bool().unwrap_or(false));
            assert!(serialized["with_payloads"].as_bool().unwrap_or(false));
            assert_eq!(serialized["max"], 5);

            let request_input = EndpointRequestInput::new(serialized.clone());
            let request: Box<dyn EpRequest> = TryInto::try_into((request_input, EpKind::Redis)).expect("construct EpRequest");

            let mut round_trip = serde_json::to_value(&request).expect("serialize EpRequest");
            // Verify EpRequest is tagged with correct kind
            assert_eq!(round_trip["kind"], "redis");
            // Remove kind field for remaining field comparison
            round_trip.as_object_mut().unwrap().remove("kind");
            assert_eq!(serialized, round_trip);
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = FtSuggetInput {
                key: RedisKey::String("testkey".into()),
                prefix: RedisJsonValue::String("test".into()),
                fuzzy: None,
                with_scores: None,
                with_payloads: None,
                max: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }

        #[test]
        fn test_parse_basic() {
            let resp_bytes = b"*3\r\n$9\r\nFT.SUGGET\r\n$5\r\nmykey\r\n$4\r\nhell\r\n";

            let (command, size) = RedisProtocol::parse_buffer(resp_bytes).expect("failed to parse RESP").expect("Expected Some found None");

            let input = FtSuggetInput::decode(command.args().to_vec()).expect("failed to parse FtSuggetInput");

            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.prefix, RedisJsonValue::from("hell"));
            assert_eq!(input.fuzzy, None);
            assert_eq!(size, resp_bytes.len());
        }

        #[test]
        fn test_parse_with_fuzzy() {
            let resp_bytes = b"*4\r\n$9\r\nFT.SUGGET\r\n$5\r\nmykey\r\n$4\r\nhell\r\n$5\r\nFUZZY\r\n";

            let (command, _) = RedisProtocol::parse_buffer(resp_bytes).expect("failed to parse RESP").expect("Expected Some found None");

            let input = FtSuggetInput::decode(command.args().to_vec()).expect("failed to parse FtSuggetInput");

            assert_eq!(input.fuzzy, Some(true));
        }

        #[test]
        fn test_parse_with_max() {
            let resp_bytes = b"*5\r\n$9\r\nFT.SUGGET\r\n$5\r\nmykey\r\n$4\r\nhell\r\n$3\r\nMAX\r\n:3\r\n";

            let (command, _) = RedisProtocol::parse_buffer(resp_bytes).expect("failed to parse RESP").expect("Expected Some found None");

            let input = FtSuggetInput::decode(command.args().to_vec()).expect("failed to parse FtSuggetInput");

            assert_eq!(input.max, Some(RedisJsonValue::Integer(3)));
        }

        #[test]
        fn test_parse_with_all_options() {
            let resp_bytes = b"*8\r\n$9\r\nFT.SUGGET\r\n$5\r\nmykey\r\n$4\r\nhell\r\n$5\r\nFUZZY\r\n$10\r\nWITHSCORES\r\n$12\r\nWITHPAYLOADS\r\n$3\r\nMAX\r\n:5\r\n";

            let (command, _) = RedisProtocol::parse_buffer(resp_bytes).expect("failed to parse RESP").expect("Expected Some found None");

            let input = FtSuggetInput::decode(command.args().to_vec()).expect("failed to parse FtSuggetInput");

            assert_eq!(input.fuzzy, Some(true));
            assert_eq!(input.with_scores, Some(true));
            assert_eq!(input.with_payloads, Some(true));
            assert_eq!(input.max, Some(RedisJsonValue::Integer(5)));
        }

        #[test]
        fn test_parse_with_withscores() {
            let resp_bytes = b"*4\r\n$9\r\nFT.SUGGET\r\n$5\r\nmykey\r\n$4\r\nhell\r\n$10\r\nWITHSCORES\r\n";

            let (command, _) = RedisProtocol::parse_buffer(resp_bytes).expect("failed to parse RESP").expect("Expected Some found None");

            let input = FtSuggetInput::decode(command.args().to_vec()).expect("failed to parse FtSuggetInput");

            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.prefix, RedisJsonValue::from("hell"));
            assert_eq!(input.fuzzy, None);
            assert_eq!(input.with_scores, Some(true));
            assert_eq!(input.with_payloads, None);
            assert_eq!(input.max, None);
        }

        #[test]
        fn test_parse_with_withpayloads() {
            let resp_bytes = b"*4\r\n$9\r\nFT.SUGGET\r\n$5\r\nmykey\r\n$4\r\nhell\r\n$12\r\nWITHPAYLOADS\r\n";

            let (command, _) = RedisProtocol::parse_buffer(resp_bytes).expect("failed to parse RESP").expect("Expected Some found None");

            let input = FtSuggetInput::decode(command.args().to_vec()).expect("failed to parse FtSuggetInput");

            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.prefix, RedisJsonValue::from("hell"));
            assert_eq!(input.fuzzy, None);
            assert_eq!(input.with_scores, None);
            assert_eq!(input.with_payloads, Some(true));
            assert_eq!(input.max, None);
        }

        #[test]
        fn test_parse_insufficient_args() {
            let resp_bytes = b"*2\r\n$9\r\nFT.SUGGET\r\n$5\r\nmykey\r\n";

            let (command, _) = RedisProtocol::parse_buffer(resp_bytes).expect("failed to parse RESP").expect("Expected Some found None");

            let result = FtSuggetInput::decode(command.args().to_vec());
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("requires at least 2 arguments"));
        }

        #[test]
        fn test_parse_max_without_value() {
            let resp_bytes = b"*4\r\n$9\r\nFT.SUGGET\r\n$5\r\nmykey\r\n$4\r\nhell\r\n$3\r\nMAX\r\n";

            let (command, _) = RedisProtocol::parse_buffer(resp_bytes).expect("failed to parse RESP").expect("Expected Some found None");

            let result = FtSuggetInput::decode(command.args().to_vec());
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("MAX requires a value"));
        }

        #[test]
        fn test_parse_unknown_option() {
            let resp_bytes = b"*4\r\n$9\r\nFT.SUGGET\r\n$5\r\nmykey\r\n$4\r\nhell\r\n$7\r\nUNKNOWN\r\n";

            let (command, _) = RedisProtocol::parse_buffer(resp_bytes).expect("failed to parse RESP").expect("Expected Some found None");

            let result = FtSuggetInput::decode(command.args().to_vec());
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("Unknown FT.SUGGET option"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::FtSugaddInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugget_empty() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtSuggetInput {
                                key: RedisKey::String("empty_get".into()),
                                prefix: RedisJsonValue::String("hel".into()),
                                fuzzy: None,
                                with_scores: None,
                                with_payloads: None,
                                max: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = FtSuggetOutput::decode(&result, false, false).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugget_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Add suggestions
                    ctx.raw(
                        &FtSugaddInput {
                            key: RedisKey::String("get_sug".into()),
                            string: RedisJsonValue::String("hello".into()),
                            score: RedisJsonValue::Float(1.0),
                            incr: None,
                            payload: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &FtSugaddInput {
                            key: RedisKey::String("get_sug".into()),
                            string: RedisJsonValue::String("help".into()),
                            score: RedisJsonValue::Float(2.0),
                            incr: None,
                            payload: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &FtSugaddInput {
                            key: RedisKey::String("get_sug".into()),
                            string: RedisJsonValue::String("world".into()),
                            score: RedisJsonValue::Float(1.0),
                            incr: None,
                            payload: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &FtSuggetInput {
                                key: RedisKey::String("get_sug".into()),
                                prefix: RedisJsonValue::String("hel".into()),
                                fuzzy: None,
                                with_scores: None,
                                with_payloads: None,
                                max: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = FtSuggetOutput::decode(&result, false, false).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    let strings: Vec<&str> = output.suggestions().iter().map(|s| s.string.as_str()).collect();
                    assert!(strings.contains(&"hello"));
                    assert!(strings.contains(&"help"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugget_with_scores() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &FtSugaddInput {
                            key: RedisKey::String("score_sug".into()),
                            string: RedisJsonValue::String("hello".into()),
                            score: RedisJsonValue::Float(1.5),
                            incr: None,
                            payload: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &FtSuggetInput {
                                key: RedisKey::String("score_sug".into()),
                                prefix: RedisJsonValue::String("hel".into()),
                                fuzzy: None,
                                with_scores: Some(true),
                                with_payloads: None,
                                max: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = FtSuggetOutput::decode(&result, true, false).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.suggestions()[0].string, "hello");
                    assert!(output.suggestions()[0].score.is_some());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugget_with_max() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Add several suggestions
                    for word in ["hello", "help", "helicopter", "helm", "held"] {
                        ctx.raw(
                            &FtSugaddInput {
                                key: RedisKey::String("max_sug".into()),
                                string: RedisJsonValue::String(word.into()),
                                score: RedisJsonValue::Float(1.0),
                                incr: None,
                                payload: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    }

                    let result = ctx
                        .raw(
                            &FtSuggetInput {
                                key: RedisKey::String("max_sug".into()),
                                prefix: RedisJsonValue::String("hel".into()),
                                fuzzy: None,
                                with_scores: None,
                                with_payloads: None,
                                max: Some(RedisJsonValue::Integer(2)),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = FtSuggetOutput::decode(&result, false, false).expect("decode failed");
                    assert!(output.len() <= 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugget_fuzzy() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &FtSugaddInput {
                            key: RedisKey::String("fuzzy_sug".into()),
                            string: RedisJsonValue::String("hello".into()),
                            score: RedisJsonValue::Float(1.0),
                            incr: None,
                            payload: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Search with a typo using FUZZY
                    let result = ctx
                        .raw(
                            &FtSuggetInput {
                                key: RedisKey::String("fuzzy_sug".into()),
                                prefix: RedisJsonValue::String("helo".into()), // typo
                                fuzzy: Some(true),
                                with_scores: None,
                                with_payloads: None,
                                max: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = FtSuggetOutput::decode(&result, false, false).expect("decode failed");
                    // FUZZY should find "hello" despite the typo
                    assert!(!output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugget_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &FtSugaddInput {
                    key: RedisKey::String("resp2_get".into()),
                    string: RedisJsonValue::String("hello".into()),
                    score: RedisJsonValue::Float(1.0),
                    incr: None,
                    payload: None,
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &FtSuggetInput {
                        key: RedisKey::String("resp2_get".into()),
                        prefix: RedisJsonValue::String("hel".into()),
                        fuzzy: None,
                        with_scores: None,
                        with_payloads: None,
                        max: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 array format");
            let output = FtSuggetOutput::decode(&result, false, false).expect("decode failed");
            assert_eq!(output.len(), 1);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugget_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            ctx.raw(
                &FtSugaddInput {
                    key: RedisKey::String("resp3_get".into()),
                    string: RedisJsonValue::String("hello".into()),
                    score: RedisJsonValue::Float(1.0),
                    incr: None,
                    payload: None,
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &FtSuggetInput {
                        key: RedisKey::String("resp3_get".into()),
                        prefix: RedisJsonValue::String("hel".into()),
                        fuzzy: None,
                        with_scores: None,
                        with_payloads: None,
                        max: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = FtSuggetOutput::decode(&result, false, false).expect("decode failed");
            assert_eq!(output.len(), 1);
            ctx.stop().await;
        }
    }
}
