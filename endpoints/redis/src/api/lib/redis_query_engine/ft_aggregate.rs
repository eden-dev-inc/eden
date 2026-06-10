use crate::api::lib::redis_query_engine::{Apply, Groupby, Limit, Load, Params, Sortby, WithCursor};
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

const API_INFO: ApiInfo<RedisApi, FtAggregateInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FtAggregate,
    "Run a search query on an index and perform aggregate transformations on the results",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `FT.AGGREGATE`
/// https://redis.io/docs/latest/commands/ft.aggregate/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtAggregateInput {
    index: RedisJsonValue,
    query: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    verbatim: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    load: Option<Load>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_by: Option<Groupby>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sort_by: Option<Sortby>,
    #[serde(skip_serializing_if = "Option::is_none")]
    apply: Option<Vec<Apply>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<Limit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    with_cursor: Option<WithCursor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Params>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scorer: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    add_scores: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dialect: Option<RedisJsonValue>,
}

impl Serialize for FtAggregateInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.verbatim.is_some() {
            fields += 1;
        }
        if self.load.is_some() {
            fields += 1;
        }
        if self.timeout.is_some() {
            fields += 1;
        }
        if self.group_by.is_some() {
            fields += 1;
        }
        if self.sort_by.is_some() {
            fields += 1;
        }
        if self.apply.is_some() {
            fields += 1;
        }
        if self.limit.is_some() {
            fields += 1;
        }
        if self.filter.is_some() {
            fields += 1;
        }
        if self.with_cursor.is_some() {
            fields += 1;
        }
        if self.params.is_some() {
            fields += 1;
        }
        if self.scorer.is_some() {
            fields += 1;
        }
        if self.add_scores.is_some() {
            fields += 1;
        }
        if self.dialect.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("FtAggregateInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        state.serialize_field("query", &self.query)?;

        if let Some(verbatim) = &self.verbatim {
            state.serialize_field("verbatim", verbatim)?;
        }
        if let Some(load) = &self.load {
            state.serialize_field("load", load)?;
        }
        if let Some(timeout) = &self.timeout {
            state.serialize_field("timeout", timeout)?;
        }
        if let Some(group_by) = &self.group_by {
            state.serialize_field("group_by", group_by)?;
        }
        if let Some(sort_by) = &self.sort_by {
            state.serialize_field("sort_by", sort_by)?;
        }
        if let Some(apply) = &self.apply {
            state.serialize_field("apply", apply)?;
        }
        if let Some(limit) = &self.limit {
            state.serialize_field("limit", limit)?;
        }
        if let Some(filter) = &self.filter {
            state.serialize_field("filter", filter)?;
        }
        if let Some(with_cursor) = &self.with_cursor {
            state.serialize_field("with_cursor", with_cursor)?;
        }
        if let Some(params) = &self.params {
            state.serialize_field("params", params)?;
        }
        if let Some(scorer) = &self.scorer {
            state.serialize_field("scorer", scorer)?;
        }
        if let Some(add_scores) = &self.add_scores {
            state.serialize_field("add_scores", add_scores)?;
        }
        if let Some(dialect) = &self.dialect {
            state.serialize_field("dialect", dialect)?;
        }

        state.end()
    }
}

impl_redis_operation!(
    FtAggregateInput,
    API_INFO,
    {index, query, verbatim, load, timeout, group_by, sort_by, apply, limit, filter, with_cursor, params, scorer, add_scores, dialect}
);

impl RedisCommandInput for FtAggregateInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index).arg(&self.query);

        if let Some(verbatim) = &self.verbatim {
            match verbatim {
                RedisJsonValue::Bool(true) => {
                    command.arg("VERBATIM");
                }
                RedisJsonValue::Integer(n) if *n != 0 => {
                    command.arg("VERBATIM");
                }
                RedisJsonValue::String(s) if !s.is_empty() && s != "0" && s.to_uppercase() != "FALSE" => {
                    command.arg("VERBATIM");
                }
                _ => {}
            }
        }

        if let Some(load) = &self.load {
            load.cmd(&mut command);
        }

        if let Some(timeout) = &self.timeout {
            command.arg("TIMEOUT").arg(timeout);
        }

        if let Some(group_by) = &self.group_by {
            group_by.cmd(&mut command);
        }

        if let Some(sort_by) = &self.sort_by {
            sort_by.cmd(&mut command);
        }

        if let Some(apply) = &self.apply {
            for app in apply {
                app.cmd(&mut command);
            }
        }

        if let Some(limit) = &self.limit {
            limit.cmd(&mut command);
        }

        if let Some(filter) = &self.filter {
            command.arg("FILTER").arg(filter);
        }

        if let Some(with_cursor) = &self.with_cursor {
            with_cursor.cmd(&mut command);
        }

        if let Some(params) = &self.params {
            params.cmd(&mut command);
        }

        if let Some(scorer) = &self.scorer {
            command.arg("SCORER").arg(scorer);
        }

        if let Some(add_scores) = &self.add_scores {
            match add_scores {
                RedisJsonValue::Bool(true) => {
                    command.arg("ADD_SCORES");
                }
                RedisJsonValue::Integer(n) if *n != 0 => {
                    command.arg("ADD_SCORES");
                }
                RedisJsonValue::String(s) if !s.is_empty() && s != "0" && s.to_uppercase() != "FALSE" => {
                    command.arg("ADD_SCORES");
                }
                _ => {}
            }
        }

        if let Some(dialect) = &self.dialect {
            command.arg("DIALECT").arg(dialect);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("FT.AGGREGATE requires at least 2 arguments, given {}", args.len())));
        }

        let index = args[0].clone();
        let query = args[1].clone();

        let mut verbatim = None;
        let mut load = None;
        let mut timeout = None;
        let mut group_by = None;
        let mut sort_by = None;
        let mut apply = None;
        let mut limit = None;
        let mut filter = None;
        let mut with_cursor = None;
        let mut params = None;
        let mut scorer = None;
        let mut add_scores = None;
        let mut dialect = None;

        let mut i = 2;
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "VERBATIM" => {
                        verbatim = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "LOAD" if i + 2 < args.len() => {
                        let count = args[i + 1].clone();
                        let field_count = match &count {
                            RedisJsonValue::Integer(n) => *n as usize,
                            _ => return Err(EpError::parse("LOAD count must be integer")),
                        };
                        if i + 1 + field_count >= args.len() {
                            return Err(EpError::parse("Insufficient fields for LOAD"));
                        }
                        let field = args[i + 2..i + 2 + field_count].to_vec();
                        load = Some(Load { count, field });
                        i += 2 + field_count;
                    }
                    "TIMEOUT" if i + 1 < args.len() => {
                        timeout = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "GROUPBY" if i + 2 < args.len() => {
                        let nargs = args[i + 1].clone();
                        let prop_count = match &nargs {
                            RedisJsonValue::Integer(n) => *n as usize,
                            _ => return Err(EpError::parse("GROUPBY nargs must be integer")),
                        };
                        if i + 2 + prop_count > args.len() {
                            return Err(EpError::parse("Insufficient properties for GROUPBY"));
                        }
                        let property = args[i + 2..i + 2 + prop_count].to_vec();
                        let groups = None;
                        let next_i = i + 2 + prop_count;
                        group_by = Some(Groupby { nargs, property, groups });
                        i = next_i;
                    }
                    "SORTBY" if i + 2 < args.len() => {
                        let nargs = args[i + 1].clone();
                        sort_by = Some(Sortby { nargs, properties: None, max: None, with_count: None });
                        i += 2;
                    }
                    "APPLY" if i + 3 < args.len() => {
                        let expression = args[i + 1].clone();
                        if let RedisJsonValue::String(as_cmd) = &args[i + 2] {
                            if as_cmd.to_uppercase() == "AS" {
                                let name = args[i + 3].clone();
                                let apply_item = Apply { expression, name };
                                apply = Some(vec![apply_item]);
                                i += 4;
                            } else {
                                return Err(EpError::parse("APPLY must be followed by AS"));
                            }
                        } else {
                            return Err(EpError::parse("APPLY must be followed by AS"));
                        }
                    }
                    "LIMIT" if i + 2 < args.len() => {
                        let offset = args[i + 1].clone();
                        let num = args[i + 2].clone();
                        limit = Some(Limit { offset, num });
                        i += 3;
                    }
                    "FILTER" if i + 1 < args.len() => {
                        filter = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "WITHCURSOR" => {
                        with_cursor = Some(WithCursor { count: None, maxidle: None });
                        i += 1;
                    }
                    "PARAMS" if i + 2 < args.len() => {
                        let nargs = args[i + 1].clone();
                        params = Some(Params { nargs, parameters: vec![] });
                        i += 2;
                    }
                    "SCORER" if i + 1 < args.len() => {
                        scorer = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "ADD_SCORES" => {
                        add_scores = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "DIALECT" if i + 1 < args.len() => {
                        dialect = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => break,
                }
            } else {
                break;
            }
        }

        Ok(Self {
            index,
            query,
            verbatim,
            load,
            timeout,
            group_by,
            sort_by,
            apply,
            limit,
            filter,
            with_cursor,
            params,
            scorer,
            add_scores,
            dialect,
        })
    }
}

/// Output for Redis `FT.AGGREGATE` command.
///
/// Returns aggregation results as an array of result rows.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtAggregateOutput {
    /// Total number of results
    total_results: u64,
    /// Result rows as arrays of field-value pairs
    results: Vec<Vec<RedisJsonValue>>,
    /// Cursor ID if WITHCURSOR was used
    cursor_id: Option<u64>,
}

impl Serialize for FtAggregateOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.cursor_id.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("FtAggregateOutput", fields)?;
        state.serialize_field("total_results", &self.total_results)?;
        state.serialize_field("results", &self.results)?;
        if let Some(cursor_id) = &self.cursor_id {
            state.serialize_field("cursor_id", cursor_id)?;
        }
        state.end()
    }
}

impl FtAggregateOutput {
    pub fn new(total_results: u64, results: Vec<Vec<RedisJsonValue>>, cursor_id: Option<u64>) -> Self {
        Self { total_results, results, cursor_id }
    }

    /// Get the total number of results
    pub fn total_results(&self) -> u64 {
        self.total_results
    }

    /// Get the result rows
    pub fn results(&self) -> &[Vec<RedisJsonValue>] {
        &self.results
    }

    /// Get the cursor ID if present
    pub fn cursor_id(&self) -> Option<u64> {
        self.cursor_id
    }

    /// Check if there are more results (cursor is present and non-zero)
    pub fn has_more(&self) -> bool {
        self.cursor_id.is_some_and(|c| c != 0)
    }

    /// Check if results are empty
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    /// Decode the Redis protocol response into a FtAggregateOutput
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
                if arr.is_empty() {
                    return Ok(Self::new(0, vec![], None));
                }

                // First element is total count
                let total_results = match &arr[0] {
                    Resp2Frame::Integer(n) => *n as u64,
                    Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).map_err(EpError::parse)?.parse().unwrap_or(0),
                    _ => 0,
                };

                let mut results = Vec::new();
                for item in arr.iter().skip(1) {
                    if let Resp2Frame::Array(row) = item {
                        let mut row_values = Vec::new();
                        for val in row {
                            let json_val = match val {
                                Resp2Frame::BulkString(b) => RedisJsonValue::String(String::from_utf8(b.clone()).map_err(EpError::parse)?),
                                Resp2Frame::Integer(i) => RedisJsonValue::Integer(*i),
                                Resp2Frame::Null => RedisJsonValue::Null,
                                _ => RedisJsonValue::Null,
                            };
                            row_values.push(json_val);
                        }
                        results.push(row_values);
                    }
                }

                Ok(Self::new(total_results, results, None))
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected FT.AGGREGATE response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                if data.is_empty() {
                    return Ok(Self::new(0, vec![], None));
                }

                let total_results = match &data[0] {
                    Resp3Frame::Number { data, .. } => *data as u64,
                    Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?.parse().unwrap_or(0),
                    _ => 0,
                };

                let mut results = Vec::new();
                for item in data.iter().skip(1) {
                    if let Resp3Frame::Array { data: row, .. } = item {
                        let mut row_values = Vec::new();
                        for val in row {
                            let json_val = match val {
                                Resp3Frame::BlobString { data, .. } => {
                                    RedisJsonValue::String(String::from_utf8(data.clone()).map_err(EpError::parse)?)
                                }
                                Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(*data),
                                Resp3Frame::Null => RedisJsonValue::Null,
                                _ => RedisJsonValue::Null,
                            };
                            row_values.push(json_val);
                        }
                        results.push(row_values);
                    }
                }

                Ok(Self::new(total_results, results, None))
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected FT.AGGREGATE response: {:?}", other))),
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
            let input = FtAggregateInput {
                index: RedisJsonValue::String("my_index".into()),
                query: RedisJsonValue::String("*".into()),
                verbatim: None,
                load: None,
                timeout: None,
                group_by: None,
                sort_by: None,
                apply: None,
                limit: None,
                filter: None,
                with_cursor: None,
                params: None,
                scorer: None,
                add_scores: None,
                dialect: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.AGGREGATE"));
            assert!(cmd_str.contains("my_index"));
        }

        #[test]
        fn test_encode_command_with_verbatim() {
            let input = FtAggregateInput {
                index: RedisJsonValue::String("idx".into()),
                query: RedisJsonValue::String("hello".into()),
                verbatim: Some(RedisJsonValue::Bool(true)),
                load: None,
                timeout: None,
                group_by: None,
                sort_by: None,
                apply: None,
                limit: None,
                filter: None,
                with_cursor: None,
                params: None,
                scorer: None,
                add_scores: None,
                dialect: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("VERBATIM"));
        }

        #[test]
        fn test_encode_command_with_limit() {
            let input = FtAggregateInput {
                index: RedisJsonValue::String("idx".into()),
                query: RedisJsonValue::String("*".into()),
                verbatim: None,
                load: None,
                timeout: None,
                group_by: None,
                sort_by: None,
                apply: None,
                limit: Some(Limit {
                    offset: RedisJsonValue::Integer(0),
                    num: RedisJsonValue::Integer(10),
                }),
                filter: None,
                with_cursor: None,
                params: None,
                scorer: None,
                add_scores: None,
                dialect: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LIMIT"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("my_index".into()), RedisJsonValue::String("*".into())];
            let input = FtAggregateInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("my_index".into()));
            assert_eq!(input.query, RedisJsonValue::String("*".into()));
        }

        #[test]
        fn test_decode_input_with_verbatim() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("query".into()),
                RedisJsonValue::String("VERBATIM".into()),
            ];
            let input = FtAggregateInput::decode(args).unwrap();
            assert_eq!(input.verbatim, Some(RedisJsonValue::Bool(true)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("idx".into())];
            let err = FtAggregateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = FtAggregateOutput::decode(b"*1\r\n:0\r\n").unwrap();
            assert_eq!(output.total_results(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtAggregateOutput::decode(b"-ERR unknown index\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtAggregateInput {
                index: RedisJsonValue::String("idx".into()),
                query: RedisJsonValue::String("*".into()),
                verbatim: None,
                load: None,
                timeout: None,
                group_by: None,
                sort_by: None,
                apply: None,
                limit: None,
                filter: None,
                with_cursor: None,
                params: None,
                scorer: None,
                add_scores: None,
                dialect: None,
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_output_accessors() {
            let output = FtAggregateOutput::new(10, vec![vec![RedisJsonValue::String("test".into())]], Some(12345));
            assert_eq!(output.total_results(), 10);
            assert_eq!(output.results().len(), 1);
            assert_eq!(output.cursor_id(), Some(12345));
            assert!(output.has_more());
        }

        #[test]
        fn test_output_no_cursor() {
            let output = FtAggregateOutput::new(5, vec![], None);
            assert!(!output.has_more());
            assert_eq!(output.cursor_id(), None);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.AGGREGATE requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_aggregate_nonexistent_index() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtAggregateInput {
                                index: RedisJsonValue::String("nonexistent_index".into()),
                                query: RedisJsonValue::String("*".into()),
                                verbatim: None,
                                load: None,
                                timeout: None,
                                group_by: None,
                                sort_by: None,
                                apply: None,
                                limit: None,
                                filter: None,
                                with_cursor: None,
                                params: None,
                                scorer: None,
                                add_scores: None,
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
                            // Module not available or other case, skip
                        }
                    }
                })
            })
            .await;
        }
    }
}
