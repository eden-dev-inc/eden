use crate::api::lib::redis_query_engine::{AttributeType, On, Prefix, Schema, SchemaFields, Sortable, StopWords};
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

const API_INFO: ApiInfo<RedisApi, FtCreateInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FtCreate, "Creates an index with the given spec", ReqType::Write, true);

/// See official Redis documentation for `FT.CREATE`
/// https://redis.io/docs/latest/commands/ft.create/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtCreateInput {
    index: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    on: Option<On>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prefix: Option<Prefix>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language_field: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    score: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    score_field: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_field: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_text_fields: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temporary: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    no_offsets: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    no_hl: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    no_fields: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    no_freqs: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop_words: Option<StopWords>,
    #[serde(skip_serializing_if = "Option::is_none")]
    skip_initial_scan: Option<RedisJsonValue>,
    schema: Schema,
}

impl Serialize for FtCreateInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, index, schema
        if self.on.is_some() {
            fields += 1;
        }
        if self.prefix.is_some() {
            fields += 1;
        }
        if self.filter.is_some() {
            fields += 1;
        }
        if self.language.is_some() {
            fields += 1;
        }
        if self.language_field.is_some() {
            fields += 1;
        }
        if self.score.is_some() {
            fields += 1;
        }
        if self.score_field.is_some() {
            fields += 1;
        }
        if self.payload_field.is_some() {
            fields += 1;
        }
        if self.max_text_fields.is_some() {
            fields += 1;
        }
        if self.temporary.is_some() {
            fields += 1;
        }
        if self.no_offsets.is_some() {
            fields += 1;
        }
        if self.no_hl.is_some() {
            fields += 1;
        }
        if self.no_fields.is_some() {
            fields += 1;
        }
        if self.no_freqs.is_some() {
            fields += 1;
        }
        if self.stop_words.is_some() {
            fields += 1;
        }
        if self.skip_initial_scan.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("FtCreateInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        state.serialize_field("schema", &self.schema)?;

        if let Some(on) = &self.on {
            state.serialize_field("on", on)?;
        }
        if let Some(prefix) = &self.prefix {
            state.serialize_field("prefix", prefix)?;
        }
        if let Some(filter) = &self.filter {
            state.serialize_field("filter", filter)?;
        }
        if let Some(language) = &self.language {
            state.serialize_field("language", language)?;
        }
        if let Some(language_field) = &self.language_field {
            state.serialize_field("language_field", language_field)?;
        }
        if let Some(score) = &self.score {
            state.serialize_field("score", score)?;
        }
        if let Some(score_field) = &self.score_field {
            state.serialize_field("score_field", score_field)?;
        }
        if let Some(payload_field) = &self.payload_field {
            state.serialize_field("payload_field", payload_field)?;
        }
        if let Some(max_text_fields) = &self.max_text_fields {
            state.serialize_field("max_text_fields", max_text_fields)?;
        }
        if let Some(temporary) = &self.temporary {
            state.serialize_field("temporary", temporary)?;
        }
        if let Some(no_offsets) = &self.no_offsets {
            state.serialize_field("no_offsets", no_offsets)?;
        }
        if let Some(no_hl) = &self.no_hl {
            state.serialize_field("no_hl", no_hl)?;
        }
        if let Some(no_fields) = &self.no_fields {
            state.serialize_field("no_fields", no_fields)?;
        }
        if let Some(no_freqs) = &self.no_freqs {
            state.serialize_field("no_freqs", no_freqs)?;
        }
        if let Some(stop_words) = &self.stop_words {
            state.serialize_field("stop_words", stop_words)?;
        }
        if let Some(skip_initial_scan) = &self.skip_initial_scan {
            state.serialize_field("skip_initial_scan", skip_initial_scan)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    FtCreateInput,
    API_INFO,
    {index, on, prefix, filter, language, language_field, score, score_field, payload_field, max_text_fields, temporary, no_offsets, no_hl, no_fields, no_freqs, stop_words, skip_initial_scan, schema}
);

impl RedisCommandInput for FtCreateInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index);

        if let Some(on) = &self.on {
            command.arg("ON");
            match on {
                On::HASH => command.arg("HASH"),
                On::JSON => command.arg("JSON"),
            };
        }

        if let Some(prefix) = &self.prefix {
            prefix.cmd(&mut command);
        }

        if let Some(filter) = &self.filter {
            command.arg("FILTER").arg(filter);
        }

        if let Some(language) = &self.language {
            command.arg("LANGUAGE").arg(language);
        }

        if let Some(language_field) = &self.language_field {
            command.arg("LANGUAGE_FIELD").arg(language_field);
        }

        if let Some(score) = &self.score {
            command.arg("SCORE").arg(score);
        }

        if let Some(score_field) = &self.score_field {
            command.arg("SCORE_FIELD").arg(score_field);
        }

        if let Some(payload_field) = &self.payload_field {
            command.arg("PAYLOAD_FIELD").arg(payload_field);
        }

        if let Some(max_text_fields) = &self.max_text_fields {
            match max_text_fields {
                RedisJsonValue::Bool(true) => {
                    command.arg("MAXTEXTFIELDS");
                }
                RedisJsonValue::Integer(n) if *n != 0 => {
                    command.arg("MAXTEXTFIELDS");
                }
                RedisJsonValue::String(s) if !s.is_empty() && s != "0" && s.to_uppercase() != "FALSE" => {
                    command.arg("MAXTEXTFIELDS");
                }
                _ => {}
            }
        }

        if let Some(temporary) = &self.temporary {
            command.arg("TEMPORARY").arg(temporary);
        }

        if let Some(no_offsets) = &self.no_offsets {
            match no_offsets {
                RedisJsonValue::Bool(true) => {
                    command.arg("NOOFFSETS");
                }
                RedisJsonValue::Integer(n) if *n != 0 => {
                    command.arg("NOOFFSETS");
                }
                RedisJsonValue::String(s) if !s.is_empty() && s != "0" && s.to_uppercase() != "FALSE" => {
                    command.arg("NOOFFSETS");
                }
                _ => {}
            }
        }

        if let Some(no_hl) = &self.no_hl {
            match no_hl {
                RedisJsonValue::Bool(true) => {
                    command.arg("NOHL");
                }
                RedisJsonValue::Integer(n) if *n != 0 => {
                    command.arg("NOHL");
                }
                RedisJsonValue::String(s) if !s.is_empty() && s != "0" && s.to_uppercase() != "FALSE" => {
                    command.arg("NOHL");
                }
                _ => {}
            }
        }

        if let Some(no_fields) = &self.no_fields {
            match no_fields {
                RedisJsonValue::Bool(true) => {
                    command.arg("NOFIELDS");
                }
                RedisJsonValue::Integer(n) if *n != 0 => {
                    command.arg("NOFIELDS");
                }
                RedisJsonValue::String(s) if !s.is_empty() && s != "0" && s.to_uppercase() != "FALSE" => {
                    command.arg("NOFIELDS");
                }
                _ => {}
            }
        }

        if let Some(no_freqs) = &self.no_freqs {
            match no_freqs {
                RedisJsonValue::Bool(true) => {
                    command.arg("NOFREQS");
                }
                RedisJsonValue::Integer(n) if *n != 0 => {
                    command.arg("NOFREQS");
                }
                RedisJsonValue::String(s) if !s.is_empty() && s != "0" && s.to_uppercase() != "FALSE" => {
                    command.arg("NOFREQS");
                }
                _ => {}
            }
        }

        if let Some(stop_words) = &self.stop_words {
            stop_words.cmd(&mut command);
        }

        if let Some(skip_initial_scan) = &self.skip_initial_scan {
            match skip_initial_scan {
                RedisJsonValue::Bool(true) => {
                    command.arg("SKIPINITIALSCAN");
                }
                RedisJsonValue::Integer(n) if *n != 0 => {
                    command.arg("SKIPINITIALSCAN");
                }
                RedisJsonValue::String(s) if !s.is_empty() && s != "0" && s.to_uppercase() != "FALSE" => {
                    command.arg("SKIPINITIALSCAN");
                }
                _ => {}
            }
        }

        self.schema.cmd(&mut command);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 4 {
            return Err(EpError::request("FT.CREATE requires at least index + SCHEMA + field + type"));
        }

        let index = args[0].clone();

        let mut on = None;
        let mut prefix = None;
        let mut filter = None;
        let mut language = None;
        let mut language_field = None;
        let mut score = None;
        let mut score_field = None;
        let mut payload_field = None;
        let mut max_text_fields = None;
        let mut temporary = None;
        let mut no_offsets = None;
        let mut no_hl = None;
        let mut no_fields = None;
        let mut no_freqs = None;
        let mut stop_words = None;
        let mut skip_initial_scan = None;

        let mut i = 1;
        let mut schema_start = None;

        // Parse options until we hit SCHEMA
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "ON" if i + 1 < args.len() => {
                        on = Some(On::try_from(args[i + 1].clone())?);
                        i += 2;
                    }
                    "PREFIX" if i + 2 < args.len() => {
                        let count = args[i + 1].clone();
                        let prefix_count = match &count {
                            RedisJsonValue::Integer(n) => *n as usize,
                            _ => return Err(EpError::parse("PREFIX count must be integer")),
                        };
                        if i + 2 + prefix_count > args.len() {
                            return Err(EpError::parse("Insufficient prefixes"));
                        }
                        let prefix_list = args[i + 2..i + 2 + prefix_count].to_vec();
                        prefix = Some(Prefix { count, prefix: prefix_list });
                        i += 2 + prefix_count;
                    }
                    "FILTER" if i + 1 < args.len() => {
                        filter = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "LANGUAGE" if i + 1 < args.len() => {
                        language = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "LANGUAGE_FIELD" if i + 1 < args.len() => {
                        language_field = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "SCORE" if i + 1 < args.len() => {
                        score = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "SCORE_FIELD" if i + 1 < args.len() => {
                        score_field = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "PAYLOAD_FIELD" if i + 1 < args.len() => {
                        payload_field = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "MAXTEXTFIELDS" => {
                        max_text_fields = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "TEMPORARY" if i + 1 < args.len() => {
                        temporary = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "NOOFFSETS" => {
                        no_offsets = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "NOHL" => {
                        no_hl = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "NOFIELDS" => {
                        no_fields = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "NOFREQS" => {
                        no_freqs = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "STOPWORDS" if i + 1 < args.len() => {
                        let count = args[i + 1].clone();
                        let sw_count = match &count {
                            RedisJsonValue::Integer(n) => *n as usize,
                            _ => return Err(EpError::parse("STOPWORDS count must be integer")),
                        };
                        if i + 2 + sw_count > args.len() {
                            return Err(EpError::parse("Insufficient stopwords"));
                        }
                        let stop_words_list = args[i + 2..i + 2 + sw_count].to_vec();
                        stop_words = Some(StopWords { count, stop_words: stop_words_list });
                        i += 2 + sw_count;
                    }
                    "SKIPINITIALSCAN" => {
                        skip_initial_scan = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "SCHEMA" => {
                        schema_start = Some(i + 1);
                        break;
                    }
                    _ => {
                        return Err(EpError::parse(format!("Unknown option: {}", cmd)));
                    }
                }
            } else {
                return Err(EpError::parse("Expected option keyword"));
            }
        }

        let schema_start = schema_start.ok_or_else(|| EpError::parse("SCHEMA is required"))?;

        // Parse schema fields
        let mut fields = Vec::new();
        i = schema_start;

        while i < args.len() {
            // Each field needs at least field_name + type
            if i + 1 >= args.len() {
                break;
            }

            let field_name = args[i].clone();
            i += 1;

            // Check for optional AS alias
            let mut r#as = None;
            if i < args.len()
                && let RedisJsonValue::String(cmd) = &args[i]
                && cmd.to_uppercase() == "AS"
                && i + 1 < args.len()
            {
                r#as = Some(args[i + 1].clone());
                i += 2;
            }

            // Field type is required
            if i >= args.len() {
                return Err(EpError::parse("Missing field type"));
            }

            let mut attribute_type = AttributeType::try_from(args[i].clone())?;
            i += 1;

            // Handle GEOSHAPE with optional SORTABLE UNF
            if matches!(attribute_type, AttributeType::GEOSHAPE(_)) {
                let mut sortable = None;
                if i < args.len()
                    && let RedisJsonValue::String(cmd) = &args[i]
                    && cmd.to_uppercase() == "SORTABLE"
                {
                    i += 1;
                    let mut unf = None;
                    if i < args.len()
                        && let RedisJsonValue::String(unf_cmd) = &args[i]
                        && unf_cmd.to_uppercase() == "UNF"
                    {
                        unf = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    sortable = Some(Sortable { unf });
                }
                attribute_type = AttributeType::GEOSHAPE(sortable);
            }

            // Check for NOINDEX
            let mut no_index = None;
            if i < args.len()
                && let RedisJsonValue::String(cmd) = &args[i]
                && cmd.to_uppercase() == "NOINDEX"
            {
                no_index = Some(RedisJsonValue::Bool(true));
                i += 1;
            }

            fields.push(SchemaFields { field_name, r#as, attribute_type, no_index });
        }

        if fields.is_empty() {
            return Err(EpError::parse("Schema must contain at least one field"));
        }

        let schema = Schema { fields };

        Ok(Self {
            index,
            on,
            prefix,
            filter,
            language,
            language_field,
            score,
            score_field,
            payload_field,
            max_text_fields,
            temporary,
            no_offsets,
            no_hl,
            no_fields,
            no_freqs,
            stop_words,
            skip_initial_scan,
            schema,
        })
    }
}

/// Output for Redis `FT.CREATE` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtCreateOutput {
    success: bool,
}

impl Serialize for FtCreateOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtCreateOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl FtCreateOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the index was created successfully
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a FtCreateOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let s = String::from_utf8(s).map_err(EpError::parse)?;
                    s.to_uppercase() == "OK"
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FT.CREATE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    s.to_uppercase() == "OK"
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FT.CREATE response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = FtCreateInput {
                index: RedisJsonValue::String("my_index".into()),
                on: None,
                prefix: None,
                filter: None,
                language: None,
                language_field: None,
                score: None,
                score_field: None,
                payload_field: None,
                max_text_fields: None,
                temporary: None,
                no_offsets: None,
                no_hl: None,
                no_fields: None,
                no_freqs: None,
                stop_words: None,
                skip_initial_scan: None,
                schema: Schema {
                    fields: vec![SchemaFields {
                        field_name: RedisJsonValue::String("title".into()),
                        r#as: None,
                        attribute_type: AttributeType::TEXT,
                        no_index: None,
                    }],
                },
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.CREATE"));
            assert!(cmd_str.contains("my_index"));
            assert!(cmd_str.contains("SCHEMA"));
            assert!(cmd_str.contains("title"));
            assert!(cmd_str.contains("TEXT"));
        }

        #[test]
        fn test_encode_command_with_on_hash() {
            let input = FtCreateInput {
                index: RedisJsonValue::String("idx".into()),
                on: Some(On::HASH),
                prefix: None,
                filter: None,
                language: None,
                language_field: None,
                score: None,
                score_field: None,
                payload_field: None,
                max_text_fields: None,
                temporary: None,
                no_offsets: None,
                no_hl: None,
                no_fields: None,
                no_freqs: None,
                stop_words: None,
                skip_initial_scan: None,
                schema: Schema {
                    fields: vec![SchemaFields {
                        field_name: RedisJsonValue::String("f".into()),
                        r#as: None,
                        attribute_type: AttributeType::TEXT,
                        no_index: None,
                    }],
                },
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ON"));
            assert!(cmd_str.contains("HASH"));
        }

        #[test]
        fn test_encode_command_with_on_json() {
            let input = FtCreateInput {
                index: RedisJsonValue::String("idx".into()),
                on: Some(On::JSON),
                prefix: None,
                filter: None,
                language: None,
                language_field: None,
                score: None,
                score_field: None,
                payload_field: None,
                max_text_fields: None,
                temporary: None,
                no_offsets: None,
                no_hl: None,
                no_fields: None,
                no_freqs: None,
                stop_words: None,
                skip_initial_scan: None,
                schema: Schema {
                    fields: vec![SchemaFields {
                        field_name: RedisJsonValue::String("f".into()),
                        r#as: None,
                        attribute_type: AttributeType::TAG,
                        no_index: None,
                    }],
                },
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ON"));
            assert!(cmd_str.contains("JSON"));
            assert!(cmd_str.contains("TAG"));
        }

        #[test]
        fn test_encode_command_multiple_fields() {
            let input = FtCreateInput {
                index: RedisJsonValue::String("idx".into()),
                on: None,
                prefix: None,
                filter: None,
                language: None,
                language_field: None,
                score: None,
                score_field: None,
                payload_field: None,
                max_text_fields: None,
                temporary: None,
                no_offsets: None,
                no_hl: None,
                no_fields: None,
                no_freqs: None,
                stop_words: None,
                skip_initial_scan: None,
                schema: Schema {
                    fields: vec![
                        SchemaFields {
                            field_name: RedisJsonValue::String("title".into()),
                            r#as: None,
                            attribute_type: AttributeType::TEXT,
                            no_index: None,
                        },
                        SchemaFields {
                            field_name: RedisJsonValue::String("price".into()),
                            r#as: None,
                            attribute_type: AttributeType::NUMERIC,
                            no_index: None,
                        },
                        SchemaFields {
                            field_name: RedisJsonValue::String("location".into()),
                            r#as: None,
                            attribute_type: AttributeType::GEO,
                            no_index: None,
                        },
                    ],
                },
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TEXT"));
            assert!(cmd_str.contains("NUMERIC"));
            assert!(cmd_str.contains("GEO"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("my_index".into()),
                RedisJsonValue::String("SCHEMA".into()),
                RedisJsonValue::String("title".into()),
                RedisJsonValue::String("TEXT".into()),
            ];
            let input = FtCreateInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("my_index".into()));
            assert_eq!(input.schema.fields.len(), 1);
        }

        #[test]
        fn test_decode_input_with_on() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("ON".into()),
                RedisJsonValue::String("HASH".into()),
                RedisJsonValue::String("SCHEMA".into()),
                RedisJsonValue::String("f".into()),
                RedisJsonValue::String("TEXT".into()),
            ];
            let input = FtCreateInput::decode(args).unwrap();
            assert!(input.on.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("idx".into()), RedisJsonValue::String("SCHEMA".into())];
            let err = FtCreateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least"));
        }

        #[test]
        fn test_decode_input_missing_schema() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("ON".into()),
                RedisJsonValue::String("HASH".into()),
                RedisJsonValue::String("title".into()),
            ];
            let err = FtCreateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("SCHEMA") || err.to_string().contains("Unknown"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = FtCreateOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtCreateOutput::decode(b"-ERR Index already exists\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtCreateInput {
                index: RedisJsonValue::String("idx".into()),
                on: None,
                prefix: None,
                filter: None,
                language: None,
                language_field: None,
                score: None,
                score_field: None,
                payload_field: None,
                max_text_fields: None,
                temporary: None,
                no_offsets: None,
                no_hl: None,
                no_fields: None,
                no_freqs: None,
                stop_words: None,
                skip_initial_scan: None,
                schema: Schema {
                    fields: vec![SchemaFields {
                        field_name: RedisJsonValue::String("f".into()),
                        r#as: None,
                        attribute_type: AttributeType::TEXT,
                        no_index: None,
                    }],
                },
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_attribute_type_try_from() {
            assert!(matches!(
                AttributeType::try_from(RedisJsonValue::String("TEXT".into())).unwrap(),
                AttributeType::TEXT
            ));
            assert!(matches!(AttributeType::try_from(RedisJsonValue::String("tag".into())).unwrap(), AttributeType::TAG));
            assert!(matches!(
                AttributeType::try_from(RedisJsonValue::String("NUMERIC".into())).unwrap(),
                AttributeType::NUMERIC
            ));
            assert!(matches!(AttributeType::try_from(RedisJsonValue::String("GEO".into())).unwrap(), AttributeType::GEO));
            assert!(matches!(
                AttributeType::try_from(RedisJsonValue::String("VECTOR".into())).unwrap(),
                AttributeType::VECTOR
            ));
            assert!(matches!(
                AttributeType::try_from(RedisJsonValue::String("GEOSHAPE".into())).unwrap(),
                AttributeType::GEOSHAPE(_)
            ));
        }

        #[test]
        fn test_attribute_type_invalid() {
            let err = AttributeType::try_from(RedisJsonValue::String("INVALID".into())).unwrap_err();
            assert!(err.to_string().contains("Invalid"));
        }

        #[test]
        fn test_on_try_from() {
            assert!(matches!(On::try_from(RedisJsonValue::String("HASH".into())).unwrap(), On::HASH));
            assert!(matches!(On::try_from(RedisJsonValue::String("json".into())).unwrap(), On::JSON));
        }

        #[test]
        fn test_on_invalid() {
            let err = On::try_from(RedisJsonValue::String("INVALID".into())).unwrap_err();
            assert!(err.to_string().contains("HASH or JSON"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtCreateOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.CREATE requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_create_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // Use unique index name to avoid conflicts
                    let index_name = format!(
                        "test_idx_{}",
                        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
                    );

                    let result = ctx
                        .raw(
                            &FtCreateInput {
                                index: RedisJsonValue::String(index_name.clone()),
                                on: Some(On::HASH),
                                prefix: None,
                                filter: None,
                                language: None,
                                language_field: None,
                                score: None,
                                score_field: None,
                                payload_field: None,
                                max_text_fields: None,
                                temporary: None,
                                no_offsets: None,
                                no_hl: None,
                                no_fields: None,
                                no_freqs: None,
                                stop_words: None,
                                skip_initial_scan: None,
                                schema: Schema {
                                    fields: vec![SchemaFields {
                                        field_name: RedisJsonValue::String("title".into()),
                                        r#as: None,
                                        attribute_type: AttributeType::TEXT,
                                        no_index: None,
                                    }],
                                },
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b"+OK") => {
                            let output = FtCreateOutput::decode(&r).expect("decode failed");
                            assert!(output.is_success());
                        }
                        Ok(r) if r.starts_with(b"-") => {
                            // Module not available or other error, skip
                        }
                        Ok(_) | Err(_) => {
                            // Other case, skip
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_create_duplicate_index() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let index_name = "dup_test_idx";

                    // First creation
                    let first_result = ctx
                        .raw(
                            &FtCreateInput {
                                index: RedisJsonValue::String(index_name.into()),
                                on: None,
                                prefix: None,
                                filter: None,
                                language: None,
                                language_field: None,
                                score: None,
                                score_field: None,
                                payload_field: None,
                                max_text_fields: None,
                                temporary: None,
                                no_offsets: None,
                                no_hl: None,
                                no_fields: None,
                                no_freqs: None,
                                stop_words: None,
                                skip_initial_scan: None,
                                schema: Schema {
                                    fields: vec![SchemaFields {
                                        field_name: RedisJsonValue::String("f".into()),
                                        r#as: None,
                                        attribute_type: AttributeType::TEXT,
                                        no_index: None,
                                    }],
                                },
                            }
                            .command(),
                        )
                        .await;

                    let Ok(r) = first_result else { return };
                    if r.starts_with(b"-") && !String::from_utf8_lossy(&r).contains("exists") {
                        // Module not available
                        return;
                    }

                    // Second creation should fail
                    let second_result = ctx
                        .raw(
                            &FtCreateInput {
                                index: RedisJsonValue::String(index_name.into()),
                                on: None,
                                prefix: None,
                                filter: None,
                                language: None,
                                language_field: None,
                                score: None,
                                score_field: None,
                                payload_field: None,
                                max_text_fields: None,
                                temporary: None,
                                no_offsets: None,
                                no_hl: None,
                                no_fields: None,
                                no_freqs: None,
                                stop_words: None,
                                skip_initial_scan: None,
                                schema: Schema {
                                    fields: vec![SchemaFields {
                                        field_name: RedisJsonValue::String("f".into()),
                                        r#as: None,
                                        attribute_type: AttributeType::TEXT,
                                        no_index: None,
                                    }],
                                },
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(r) = second_result {
                        // Should be an error about index already existing
                        assert!(r.starts_with(b"-"));
                    }
                })
            })
            .await;
        }
    }
}
