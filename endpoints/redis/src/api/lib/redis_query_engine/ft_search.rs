use crate::api::lib::redis_query_engine::{
    Fields, Filter, Geofilter, Highlight, Identifier, Infields, Inkeys, Limit, Parameters, Params, Return, SearchSortby, Sort, Summarize,
    Tags, Unit,
};
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
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, FtSearchInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FtSearch,
    "Searches the index with a textual query returning either documents or just ids",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `FT.SEARCH`
/// https://redis.io/docs/latest/commands/ft.search/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtSearchInput {
    index: RedisJsonValue,
    query: RedisJsonValue,
    no_content: Option<RedisJsonValue>,
    verbatim: Option<RedisJsonValue>,
    no_stop_words: Option<RedisJsonValue>,
    with_scores: Option<RedisJsonValue>,
    with_payloads: Option<RedisJsonValue>,
    with_sort_keys: Option<RedisJsonValue>,
    filters: Option<Vec<Filter>>,
    geo_filters: Option<Vec<Geofilter>>,
    in_keys: Option<Inkeys>,
    in_fields: Option<Infields>,
    r#return: Option<Return>,
    summarize: Option<Summarize>,
    highlight: Option<Highlight>,
    slop: Option<RedisJsonValue>,
    timeout: Option<RedisJsonValue>,
    in_order: Option<RedisJsonValue>,
    language: Option<RedisJsonValue>,
    expander: Option<RedisJsonValue>,
    scorer: Option<RedisJsonValue>,
    explain_score: Option<RedisJsonValue>,
    payload: Option<RedisJsonValue>,
    sort_by: Option<SearchSortby>,
    limit: Option<Limit>,
    params: Option<Params>,
    dialect: Option<RedisJsonValue>,
}

impl Serialize for FtSearchInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, index, query

        if self.no_content.is_some() {
            fields += 1;
        }
        if self.verbatim.is_some() {
            fields += 1;
        }
        if self.no_stop_words.is_some() {
            fields += 1;
        }
        if self.with_scores.is_some() {
            fields += 1;
        }
        if self.with_payloads.is_some() {
            fields += 1;
        }
        if self.with_sort_keys.is_some() {
            fields += 1;
        }
        if self.filters.is_some() {
            fields += 1;
        }
        if self.geo_filters.is_some() {
            fields += 1;
        }
        if self.in_keys.is_some() {
            fields += 1;
        }
        if self.in_fields.is_some() {
            fields += 1;
        }
        if self.r#return.is_some() {
            fields += 1;
        }
        if self.summarize.is_some() {
            fields += 1;
        }
        if self.highlight.is_some() {
            fields += 1;
        }
        if self.slop.is_some() {
            fields += 1;
        }
        if self.timeout.is_some() {
            fields += 1;
        }
        if self.in_order.is_some() {
            fields += 1;
        }
        if self.language.is_some() {
            fields += 1;
        }
        if self.expander.is_some() {
            fields += 1;
        }
        if self.scorer.is_some() {
            fields += 1;
        }
        if self.explain_score.is_some() {
            fields += 1;
        }
        if self.payload.is_some() {
            fields += 1;
        }
        if self.sort_by.is_some() {
            fields += 1;
        }
        if self.limit.is_some() {
            fields += 1;
        }
        if self.params.is_some() {
            fields += 1;
        }
        if self.dialect.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("FtSearchInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        state.serialize_field("query", &self.query)?;

        // Serialize all optional fields
        if let Some(no_content) = &self.no_content {
            state.serialize_field("no_content", no_content)?;
        }
        if let Some(verbatim) = &self.verbatim {
            state.serialize_field("verbatim", verbatim)?;
        }
        if let Some(no_stop_words) = &self.no_stop_words {
            state.serialize_field("no_stop_words", no_stop_words)?;
        }
        if let Some(with_scores) = &self.with_scores {
            state.serialize_field("with_scores", with_scores)?;
        }
        if let Some(with_payloads) = &self.with_payloads {
            state.serialize_field("with_payloads", with_payloads)?;
        }
        if let Some(with_sort_keys) = &self.with_sort_keys {
            state.serialize_field("with_sort_keys", with_sort_keys)?;
        }
        if let Some(filters) = &self.filters {
            state.serialize_field("filters", filters)?;
        }
        if let Some(geo_filters) = &self.geo_filters {
            state.serialize_field("geo_filters", geo_filters)?;
        }
        if let Some(in_keys) = &self.in_keys {
            state.serialize_field("in_keys", in_keys)?;
        }
        if let Some(in_fields) = &self.in_fields {
            state.serialize_field("in_fields", in_fields)?;
        }
        if let Some(r#return) = &self.r#return {
            state.serialize_field("return", r#return)?;
        }
        if let Some(summarize) = &self.summarize {
            state.serialize_field("summarize", summarize)?;
        }
        if let Some(highlight) = &self.highlight {
            state.serialize_field("highlight", highlight)?;
        }
        if let Some(slop) = &self.slop {
            state.serialize_field("slop", slop)?;
        }
        if let Some(timeout) = &self.timeout {
            state.serialize_field("timeout", timeout)?;
        }
        if let Some(in_order) = &self.in_order {
            state.serialize_field("in_order", in_order)?;
        }
        if let Some(language) = &self.language {
            state.serialize_field("language", language)?;
        }
        if let Some(expander) = &self.expander {
            state.serialize_field("expander", expander)?;
        }
        if let Some(scorer) = &self.scorer {
            state.serialize_field("scorer", scorer)?;
        }
        if let Some(explain_score) = &self.explain_score {
            state.serialize_field("explain_score", explain_score)?;
        }
        if let Some(payload) = &self.payload {
            state.serialize_field("payload", payload)?;
        }
        if let Some(sort_by) = &self.sort_by {
            state.serialize_field("sort_by", sort_by)?;
        }
        if let Some(limit) = &self.limit {
            state.serialize_field("limit", limit)?;
        }
        if let Some(params) = &self.params {
            state.serialize_field("params", &params)?;
        }
        if let Some(dialect) = &self.dialect {
            state.serialize_field("dialect", dialect)?;
        }

        state.end()
    }
}

impl_redis_operation!(
    FtSearchInput,
    API_INFO,
    {index, query, no_content, verbatim, no_stop_words, with_scores, with_payloads, with_sort_keys, filters, geo_filters, in_keys, in_fields, r#return, summarize, highlight, slop, timeout, in_order, language, expander, scorer, explain_score, payload, sort_by, limit, params, dialect});

impl RedisCommandInput for FtSearchInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index).arg(&self.query);

        if let Some(no_content) = &self.no_content
            && (matches!(no_content, RedisJsonValue::Bool(true) | RedisJsonValue::Integer(1))
                || matches!(no_content, RedisJsonValue::String(s) if s == "1" || s.to_uppercase() == "TRUE"))
        {
            command.arg("NOCONTENT");
        }

        if let Some(verbatim) = &self.verbatim
            && (matches!(verbatim, RedisJsonValue::Bool(true) | RedisJsonValue::Integer(1))
                || matches!(verbatim, RedisJsonValue::String(s) if s == "1" || s.to_uppercase() == "TRUE"))
        {
            command.arg("VERBATIM");
        }

        if let Some(no_stop_words) = &self.no_stop_words
            && (matches!(no_stop_words, RedisJsonValue::Bool(true) | RedisJsonValue::Integer(1))
                || matches!(no_stop_words, RedisJsonValue::String(s) if s == "1" || s.to_uppercase() == "TRUE"))
        {
            command.arg("NOSTOPWORDS");
        }

        if let Some(with_scores) = &self.with_scores
            && (matches!(with_scores, RedisJsonValue::Bool(true) | RedisJsonValue::Integer(1))
                || matches!(with_scores, RedisJsonValue::String(s) if s == "1" || s.to_uppercase() == "TRUE"))
        {
            command.arg("WITHSCORES");
        }

        if let Some(with_payloads) = &self.with_payloads
            && (matches!(with_payloads, RedisJsonValue::Bool(true) | RedisJsonValue::Integer(1))
                || matches!(with_payloads, RedisJsonValue::String(s) if s == "1" || s.to_uppercase() == "TRUE"))
        {
            command.arg("WITHPAYLOADS");
        }

        if let Some(with_sort_keys) = &self.with_sort_keys
            && (matches!(with_sort_keys, RedisJsonValue::Bool(true) | RedisJsonValue::Integer(1))
                || matches!(with_sort_keys, RedisJsonValue::String(s) if s == "1" || s.to_uppercase() == "TRUE"))
        {
            command.arg("WITHSORTKEYS");
        }

        if let Some(filters) = &self.filters {
            for filter in filters {
                filter.cmd(&mut command);
            }
        }

        if let Some(geo_filters) = &self.geo_filters {
            for geo_filter in geo_filters {
                geo_filter.cmd(&mut command);
            }
        }

        if let Some(in_keys) = &self.in_keys {
            in_keys.cmd(&mut command);
        }

        if let Some(in_fields) = &self.in_fields {
            in_fields.cmd(&mut command);
        }

        if let Some(r#return) = &self.r#return {
            r#return.cmd(&mut command);
        }

        if let Some(summarize) = &self.summarize {
            summarize.cmd(&mut command);
        }

        if let Some(highlight) = &self.highlight {
            highlight.cmd(&mut command);
        }

        if let Some(slop) = &self.slop {
            command.arg("SLOP").arg(slop);
        }

        if let Some(timeout) = &self.timeout {
            command.arg("TIMEOUT").arg(timeout);
        }

        if let Some(in_order) = &self.in_order
            && (matches!(in_order, RedisJsonValue::Bool(true) | RedisJsonValue::Integer(1))
                || matches!(in_order, RedisJsonValue::String(s) if s == "1" || s.to_uppercase() == "TRUE"))
        {
            command.arg("INORDER");
        }

        if let Some(language) = &self.language {
            command.arg("LANGUAGE").arg(language);
        }

        if let Some(expander) = &self.expander {
            command.arg("EXPANDER").arg(expander);
        }

        if let Some(scorer) = &self.scorer {
            command.arg("SCORER").arg(scorer);
        }

        if let Some(explain_score) = &self.explain_score
            && (matches!(explain_score, RedisJsonValue::Bool(true) | RedisJsonValue::Integer(1))
                || matches!(explain_score, RedisJsonValue::String(s) if s == "1" || s.to_uppercase() == "TRUE"))
        {
            command.arg("EXPLAINSCORE");
        }

        if let Some(payload) = &self.payload {
            command.arg("PAYLOAD").arg(payload);
        }

        if let Some(sort_by) = &self.sort_by {
            sort_by.cmd(&mut command);
        }

        if let Some(limit) = &self.limit {
            limit.cmd(&mut command);
        }

        if let Some(params) = &self.params {
            params.cmd(&mut command);
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
            return Err(EpError::request(format!("FT.SEARCH requires at least 2 arguments, given {}", args.len())));
        }

        let index = args[0].clone();
        let query = args[1].clone();

        // Initialize all optional fields
        let mut no_content = None;
        let mut verbatim = None;
        let mut no_stop_words = None;
        let mut with_scores = None;
        let mut with_payloads = None;
        let mut with_sort_keys = None;
        let mut filters: Option<Vec<Filter>> = None;
        let mut geo_filters: Option<Vec<Geofilter>> = None;
        let mut in_keys = None;
        let mut in_fields = None;
        let mut r#return = None;
        let mut summarize = None;
        let mut highlight = None;
        let mut slop = None;
        let mut timeout = None;
        let mut in_order = None;
        let mut language = None;
        let mut expander = None;
        let mut scorer = None;
        let mut explain_score = None;
        let mut payload = None;
        let mut sort_by = None;
        let mut limit = None;
        let mut params = None;
        let mut dialect = None;

        let mut i = 2;
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "NOCONTENT" => {
                        no_content = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "VERBATIM" => {
                        verbatim = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "NOSTOPWORDS" => {
                        no_stop_words = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "WITHSCORES" => {
                        with_scores = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "WITHPAYLOADS" => {
                        with_payloads = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "WITHSORTKEYS" => {
                        with_sort_keys = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "FILTER" if i + 3 < args.len() => {
                        let numeric_field = args[i + 1].clone();
                        let min = args[i + 2].clone();
                        let max = args[i + 3].clone();
                        let filter = Filter { numeric_field, min, max };
                        if let Some(ref mut existing_filters) = filters {
                            existing_filters.push(filter);
                        } else {
                            filters = Some(vec![filter]);
                        }
                        i += 4;
                    }
                    "GEOFILTER" if i + 5 < args.len() => {
                        let geo_filter = args[i + 1].clone();
                        let lon = args[i + 2].clone();
                        let lat = args[i + 3].clone();
                        let radius = args[i + 4].clone();
                        let unit_str = args[i + 5].clone();
                        let unit = match unit_str {
                            RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                                "M" => Unit::M,
                                "KM" => Unit::KM,
                                "MI" => Unit::MI,
                                "FT" => Unit::FT,
                                _ => return Err(EpError::parse("Invalid unit")),
                            },
                            _ => return Err(EpError::parse("Unit must be string")),
                        };
                        let geofilter = Geofilter { geo_filter, lon, lat, radius, unit };
                        if let Some(ref mut existing_geo_filters) = geo_filters {
                            existing_geo_filters.push(geofilter);
                        } else {
                            geo_filters = Some(vec![geofilter]);
                        }
                        i += 6;
                    }
                    "INKEYS" if i + 2 < args.len() => {
                        let count = args[i + 1].clone();
                        let key_count = match &count {
                            RedisJsonValue::Integer(n) => *n as usize,
                            _ => return Err(EpError::parse("INKEYS count must be integer")),
                        };
                        if i + 2 + key_count > args.len() {
                            return Err(EpError::parse("Insufficient keys for INKEYS"));
                        }

                        let mut keys = vec![];
                        for k in args[i + 2..i + 2 + key_count].iter() {
                            keys.push(k.clone().try_into()?);
                        }

                        in_keys = Some(Inkeys { count, keys });
                        i += 2 + key_count;
                    }
                    "INFIELDS" if i + 2 < args.len() => {
                        let count = args[i + 1].clone();
                        let field_count = match &count {
                            RedisJsonValue::Integer(n) => *n as usize,
                            _ => return Err(EpError::parse("INFIELDS count must be integer")),
                        };
                        if i + 2 + field_count > args.len() {
                            return Err(EpError::parse("Insufficient fields for INFIELDS"));
                        }
                        let fields = args[i + 2..i + 2 + field_count].to_vec();
                        in_fields = Some(Infields { count, fields });
                        i += 2 + field_count;
                    }
                    "RETURN" if i + 2 < args.len() => {
                        let count = args[i + 1].clone();
                        let id_count = match &count {
                            RedisJsonValue::Integer(n) => *n as usize,
                            _ => return Err(EpError::parse("RETURN count must be integer")),
                        };
                        if i + 2 + id_count > args.len() {
                            return Err(EpError::parse("Insufficient identifiers for RETURN"));
                        }

                        let mut identifiers = Vec::new();
                        let mut j = i + 2;
                        for _ in 0..id_count {
                            if j >= args.len() {
                                break;
                            }
                            let identifier = args[j].clone();
                            j += 1;

                            let mut r#as = None;
                            if j < args.len()
                                && let RedisJsonValue::String(as_cmd) = &args[j]
                                && as_cmd.to_uppercase() == "AS"
                                && j + 1 < args.len()
                            {
                                r#as = Some(args[j + 1].clone());
                                j += 2;
                            }

                            identifiers.push(Identifier { identifier, r#as });
                        }

                        r#return = Some(Return { count, identifiers });
                        i = j;
                    }
                    "SUMMARIZE" => {
                        let mut fields = None;
                        let mut frags = None;
                        let mut len = None;
                        let mut separator = None;

                        i += 1;
                        while i < args.len() {
                            if let RedisJsonValue::String(sub_cmd) = &args[i] {
                                match sub_cmd.to_uppercase().as_str() {
                                    "FIELDS" if i + 2 < args.len() => {
                                        let count = args[i + 1].clone();
                                        let field_count = match &count {
                                            RedisJsonValue::Integer(n) => *n as usize,
                                            _ => break,
                                        };
                                        if i + 2 + field_count <= args.len() {
                                            let field_list = args[i + 2..i + 2 + field_count].to_vec();
                                            fields = Some(Fields { count, fields: field_list });
                                            i += 2 + field_count;
                                        } else {
                                            break;
                                        }
                                    }
                                    "FRAGS" if i + 1 < args.len() => {
                                        frags = Some(args[i + 1].clone());
                                        i += 2;
                                    }
                                    "LEN" if i + 1 < args.len() => {
                                        len = Some(args[i + 1].clone());
                                        i += 2;
                                    }
                                    "SEPARATOR" if i + 1 < args.len() => {
                                        separator = Some(args[i + 1].clone());
                                        i += 2;
                                    }
                                    _ => break,
                                }
                            } else {
                                break;
                            }
                        }

                        summarize = Some(Summarize { fields, frags, len, separator });
                    }
                    "HIGHLIGHT" => {
                        let mut fields = None;
                        let mut tags = None;

                        i += 1;
                        while i < args.len() {
                            if let RedisJsonValue::String(sub_cmd) = &args[i] {
                                match sub_cmd.to_uppercase().as_str() {
                                    "FIELDS" if i + 2 < args.len() => {
                                        let count = args[i + 1].clone();
                                        let field_count = match &count {
                                            RedisJsonValue::Integer(n) => *n as usize,
                                            _ => break,
                                        };
                                        if i + 2 + field_count <= args.len() {
                                            let field_list = args[i + 2..i + 2 + field_count].to_vec();
                                            fields = Some(Fields { count, fields: field_list });
                                            i += 2 + field_count;
                                        } else {
                                            break;
                                        }
                                    }
                                    "TAGS" if i + 2 < args.len() => {
                                        let open = args[i + 1].clone();
                                        let close = args[i + 2].clone();
                                        tags = Some(Tags { open, close });
                                        i += 3;
                                    }
                                    _ => break,
                                }
                            } else {
                                break;
                            }
                        }

                        highlight = Some(Highlight { fields, tags });
                    }
                    "SLOP" if i + 1 < args.len() => {
                        slop = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "TIMEOUT" if i + 1 < args.len() => {
                        timeout = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "INORDER" => {
                        in_order = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "LANGUAGE" if i + 1 < args.len() => {
                        language = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "EXPANDER" if i + 1 < args.len() => {
                        expander = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "SCORER" if i + 1 < args.len() => {
                        scorer = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "EXPLAINSCORE" => {
                        explain_score = Some(RedisJsonValue::Bool(true));
                        i += 1;
                    }
                    "PAYLOAD" if i + 1 < args.len() => {
                        payload = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "SORTBY" if i + 1 < args.len() => {
                        let sort_by_field = args[i + 1].clone();
                        let mut sort = None;
                        let mut with_count = None;
                        i += 2;

                        // Check for optional ASC/DESC
                        if i < args.len()
                            && let RedisJsonValue::String(sort_cmd) = &args[i]
                        {
                            match sort_cmd.to_uppercase().as_str() {
                                "ASC" => {
                                    sort = Some(Sort::ASC);
                                    i += 1;
                                }
                                "DESC" => {
                                    sort = Some(Sort::DESC);
                                    i += 1;
                                }
                                _ => {}
                            }
                        }

                        // Check for WITHCOUNT
                        if i < args.len()
                            && let RedisJsonValue::String(count_cmd) = &args[i]
                            && count_cmd.to_uppercase() == "WITHCOUNT"
                        {
                            with_count = Some(RedisJsonValue::Bool(true));
                            i += 1;
                        }

                        sort_by = Some(SearchSortby { sort_by: sort_by_field, sort, with_count });
                    }
                    "LIMIT" if i + 2 < args.len() => {
                        let offset = args[i + 1].clone();
                        let num = args[i + 2].clone();
                        limit = Some(Limit { offset, num });
                        i += 3;
                    }
                    "PARAMS" if i + 2 < args.len() => {
                        let nargs = args[i + 1].clone();
                        let param_count = match &nargs {
                            RedisJsonValue::Integer(n) => *n as usize,
                            _ => return Err(EpError::parse("PARAMS nargs must be integer")),
                        };

                        if i + 2 + param_count > args.len() {
                            return Err(EpError::parse("Insufficient parameters for PARAMS"));
                        }

                        let mut parameters = Vec::new();
                        for j in (i + 2..i + 2 + param_count).step_by(2) {
                            if j + 1 < args.len() {
                                parameters.push(Parameters { name: args[j].clone(), value: args[j + 1].clone() });
                            }
                        }

                        params = Some(Params { nargs, parameters });
                        i += 2 + param_count;
                    }
                    "DIALECT" if i + 1 < args.len() => {
                        dialect = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => {
                        // Unknown option, skip
                        i += 1;
                    }
                }
            } else {
                i += 1;
            }
        }

        Ok(Self {
            index,
            query,
            no_content,
            verbatim,
            no_stop_words,
            with_scores,
            with_payloads,
            with_sort_keys,
            filters,
            geo_filters,
            in_keys,
            in_fields,
            r#return,
            summarize,
            highlight,
            slop,
            timeout,
            in_order,
            language,
            expander,
            scorer,
            explain_score,
            payload,
            sort_by,
            limit,
            params,
            dialect,
        })
    }
}

/// A single search result document
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SearchDocument {
    /// Document ID/key
    pub id: String,
    /// Document score (if WITHSCORES was specified)
    pub score: Option<f64>,
    /// Document payload (if WITHPAYLOADS was specified)
    pub payload: Option<String>,
    /// Sort key (if WITHSORTKEYS was specified)
    pub sort_key: Option<String>,
    /// Document fields as key-value pairs
    pub fields: Vec<(String, RedisJsonValue)>,
}

/// Output for Redis `FT.SEARCH` command.
///
/// Returns the total number of matching documents and an array of documents.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtSearchOutput {
    /// Total number of matching documents
    total_results: i64,
    /// List of documents matching the query
    documents: Vec<SearchDocument>,
}

impl Serialize for FtSearchOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtSearchOutput", 2)?;
        state.serialize_field("total_results", &self.total_results)?;
        state.serialize_field("documents", &self.documents)?;
        state.end()
    }
}

impl FtSearchOutput {
    pub fn new(total_results: i64, documents: Vec<SearchDocument>) -> Self {
        Self { total_results, documents }
    }

    /// Get the total number of matching documents
    pub fn total_results(&self) -> i64 {
        self.total_results
    }

    /// Get the documents
    pub fn documents(&self) -> &[SearchDocument] {
        &self.documents
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    /// Get the number of returned documents
    pub fn len(&self) -> usize {
        self.documents.len()
    }

    /// Decode the Redis protocol response into a FtSearchOutput
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
                    return Ok(Self { total_results: 0, documents: vec![] });
                }

                // First element is total count
                let total_results = match &arr[0] {
                    Resp2Frame::Integer(i) => *i,
                    Resp2Frame::BulkString(s) => String::from_utf8_lossy(s).parse::<i64>().unwrap_or(0),
                    _ => 0,
                };

                let mut documents = Vec::new();
                let mut i = 1;

                while i < arr.len() {
                    // Document ID
                    let id = match &arr[i] {
                        Resp2Frame::BulkString(s) => String::from_utf8(s.to_vec()).map_err(EpError::parse)?,
                        _ => {
                            i += 1;
                            continue;
                        }
                    };
                    i += 1;

                    let mut score = None;
                    let payload = None;
                    let sort_key = None;
                    let mut fields = Vec::new();

                    // Parse optional score, payload, sortkey, and fields
                    while i < arr.len() {
                        match &arr[i] {
                            Resp2Frame::BulkString(s) => {
                                // Check if this looks like a document ID (next document)
                                // or if it's a field array
                                if i + 1 < arr.len()
                                    && let Resp2Frame::Array(_) = &arr[i + 1]
                                {
                                    // This is the start of the next document
                                    break;
                                }
                                // Could be score, payload, or sortkey
                                if score.is_none()
                                    && let Ok(sc) = String::from_utf8_lossy(s).parse::<f64>()
                                {
                                    score = Some(sc);
                                    i += 1;
                                    continue;
                                }
                                break;
                            }
                            Resp2Frame::Array(field_arr) => {
                                // Parse fields
                                let mut j = 0;
                                while j + 1 < field_arr.len() {
                                    let key = match &field_arr[j] {
                                        Resp2Frame::BulkString(k) => k.clone(),
                                        _ => {
                                            j += 2;
                                            continue;
                                        }
                                    };
                                    let value = Self::frame_to_json_resp2(&field_arr[j + 1])?;
                                    fields.push((String::from_utf8(key).map_err(EpError::parse)?, value));
                                    j += 2;
                                }
                                i += 1;
                                break;
                            }
                            _ => {
                                i += 1;
                                break;
                            }
                        }
                    }

                    documents.push(SearchDocument { id, score, payload, sort_key, fields });
                }

                Ok(Self { total_results, documents })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected FT.SEARCH response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                if data.is_empty() {
                    return Ok(Self { total_results: 0, documents: vec![] });
                }

                // First element is total count
                let total_results = match &data[0] {
                    Resp3Frame::Number { data, .. } => *data,
                    Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                        String::from_utf8_lossy(data).parse::<i64>().unwrap_or(0)
                    }
                    _ => 0,
                };

                let mut documents = Vec::new();
                let mut i = 1;

                while i < data.len() {
                    // Document ID
                    let id = match &data[i] {
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                            String::from_utf8(data.to_vec()).map_err(EpError::parse)?
                        }
                        _ => {
                            i += 1;
                            continue;
                        }
                    };
                    i += 1;

                    let mut score = None;
                    let payload = None;
                    let sort_key = None;
                    let mut fields = Vec::new();

                    // Parse optional fields
                    while i < data.len() {
                        match &data[i] {
                            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                if score.is_none()
                                    && let Ok(sc) = String::from_utf8_lossy(data).parse::<f64>()
                                {
                                    score = Some(sc);
                                    i += 1;
                                    continue;
                                }
                                break;
                            }
                            Resp3Frame::Double { data, .. } => {
                                score = Some(*data);
                                i += 1;
                            }
                            Resp3Frame::Array { data, .. } => {
                                let mut j = 0;
                                while j + 1 < data.len() {
                                    let key = match &data[j] {
                                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => data,
                                        _ => {
                                            j += 2;
                                            continue;
                                        }
                                    };
                                    let value = Self::frame_to_json_resp3(&data[j + 1]);
                                    fields.push((String::from_utf8(key.to_vec()).map_err(EpError::parse)?, value?));
                                    j += 2;
                                }
                                i += 1;
                                break;
                            }
                            Resp3Frame::Map { data, .. } => {
                                for (k, v) in data {
                                    let key = match k {
                                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => data,
                                        _ => continue,
                                    };
                                    let value = Self::frame_to_json_resp3(v);
                                    fields.push((String::from_utf8(key.to_vec()).map_err(EpError::parse)?, value?));
                                }
                                i += 1;
                                break;
                            }
                            _ => {
                                i += 1;
                                break;
                            }
                        }
                    }

                    documents.push(SearchDocument { id, score, payload, sort_key, fields });
                }

                Ok(Self { total_results, documents })
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected FT.SEARCH response: {:?}", other))),
        }
    }

    fn frame_to_json_resp2(frame: &Resp2Frame) -> ResultEP<RedisJsonValue> {
        Ok(match frame {
            Resp2Frame::SimpleString(s) | Resp2Frame::BulkString(s) => {
                RedisJsonValue::String(String::from_utf8(s.to_vec()).map_err(EpError::parse)?)
            }
            Resp2Frame::Integer(i) => RedisJsonValue::Integer(*i),
            Resp2Frame::Array(arr) => {
                let mut items = Vec::with_capacity(arr.len());
                for item in arr {
                    items.push(Self::frame_to_json_resp2(item)?);
                }
                RedisJsonValue::Array(items)
            }
            Resp2Frame::Null => RedisJsonValue::Null,
            _ => RedisJsonValue::Null,
        })
    }

    fn frame_to_json_resp3(frame: &Resp3Frame) -> ResultEP<RedisJsonValue> {
        Ok(match frame {
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                RedisJsonValue::String(String::from_utf8(data.to_vec()).map_err(EpError::parse)?)
            }
            Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(*data),
            Resp3Frame::Double { data, .. } => RedisJsonValue::Float(*data),
            Resp3Frame::Boolean { data, .. } => RedisJsonValue::Bool(*data),
            Resp3Frame::Array { data, .. } => {
                let mut items = Vec::with_capacity(data.len());
                for item in data {
                    items.push(Self::frame_to_json_resp3(item)?);
                }
                RedisJsonValue::Array(items)
            }
            Resp3Frame::Null => RedisJsonValue::Null,
            _ => RedisJsonValue::Null,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = FtSearchInput {
                index: RedisJsonValue::String("my_index".into()),
                query: RedisJsonValue::String("@title:hello".into()),
                no_content: None,
                verbatim: None,
                no_stop_words: None,
                with_scores: None,
                with_payloads: None,
                with_sort_keys: None,
                filters: None,
                geo_filters: None,
                in_keys: None,
                in_fields: None,
                r#return: None,
                summarize: None,
                highlight: None,
                slop: None,
                timeout: None,
                in_order: None,
                language: None,
                expander: None,
                scorer: None,
                explain_score: None,
                payload: None,
                sort_by: None,
                limit: None,
                params: None,
                dialect: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.SEARCH"));
            assert!(cmd_str.contains("my_index"));
            assert!(cmd_str.contains("@title:hello"));
        }

        #[test]
        fn test_encode_command_with_options() {
            let input = FtSearchInput {
                index: RedisJsonValue::String("idx".into()),
                query: RedisJsonValue::String("*".into()),
                no_content: Some(RedisJsonValue::Bool(true)),
                verbatim: Some(RedisJsonValue::Bool(true)),
                no_stop_words: None,
                with_scores: Some(RedisJsonValue::Bool(true)),
                with_payloads: None,
                with_sort_keys: None,
                filters: None,
                geo_filters: None,
                in_keys: None,
                in_fields: None,
                r#return: None,
                summarize: None,
                highlight: None,
                slop: None,
                timeout: None,
                in_order: None,
                language: None,
                expander: None,
                scorer: None,
                explain_score: None,
                payload: None,
                sort_by: None,
                limit: Some(Limit {
                    offset: RedisJsonValue::Integer(0),
                    num: RedisJsonValue::Integer(10),
                }),
                params: None,
                dialect: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("NOCONTENT"));
            assert!(cmd_str.contains("VERBATIM"));
            assert!(cmd_str.contains("WITHSCORES"));
            assert!(cmd_str.contains("LIMIT"));
        }

        #[test]
        fn test_encode_command_with_sortby() {
            let input = FtSearchInput {
                index: RedisJsonValue::String("idx".into()),
                query: RedisJsonValue::String("*".into()),
                no_content: None,
                verbatim: None,
                no_stop_words: None,
                with_scores: None,
                with_payloads: None,
                with_sort_keys: None,
                filters: None,
                geo_filters: None,
                in_keys: None,
                in_fields: None,
                r#return: None,
                summarize: None,
                highlight: None,
                slop: None,
                timeout: None,
                in_order: None,
                language: None,
                expander: None,
                scorer: None,
                explain_score: None,
                payload: None,
                sort_by: Some(SearchSortby {
                    sort_by: RedisJsonValue::String("created_at".into()),
                    sort: Some(Sort::DESC),
                    with_count: None,
                }),
                limit: None,
                params: None,
                dialect: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SORTBY"));
            assert!(cmd_str.contains("created_at"));
            assert!(cmd_str.contains("DESC"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("idx".into()), RedisJsonValue::String("*".into())];
            let input = FtSearchInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("idx".into()));
            assert_eq!(input.query, RedisJsonValue::String("*".into()));
        }

        #[test]
        fn test_decode_input_with_nocontent() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("*".into()),
                RedisJsonValue::String("NOCONTENT".into()),
            ];
            let input = FtSearchInput::decode(args).unwrap();
            assert_eq!(input.no_content, Some(RedisJsonValue::Bool(true)));
        }

        #[test]
        fn test_decode_input_with_limit() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("*".into()),
                RedisJsonValue::String("LIMIT".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(10),
            ];
            let input = FtSearchInput::decode(args).unwrap();
            assert!(input.limit.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("idx".into())];
            let err = FtSearchInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = FtSearchOutput::decode(b"*1\r\n:0\r\n").unwrap();
            assert_eq!(output.total_results(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtSearchOutput::decode(b"-ERR unknown index\r\n").unwrap_err();
            assert!(err.to_string().contains("unknown index"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtSearchInput {
                index: RedisJsonValue::String("i".into()),
                query: RedisJsonValue::String("q".into()),
                no_content: None,
                verbatim: None,
                no_stop_words: None,
                with_scores: None,
                with_payloads: None,
                with_sort_keys: None,
                filters: None,
                geo_filters: None,
                in_keys: None,
                in_fields: None,
                r#return: None,
                summarize: None,
                highlight: None,
                slop: None,
                timeout: None,
                in_order: None,
                language: None,
                expander: None,
                scorer: None,
                explain_score: None,
                payload: None,
                sort_by: None,
                limit: None,
                params: None,
                dialect: None,
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_output_accessors() {
            let doc = SearchDocument {
                id: "doc1".into(),
                score: Some(1.5),
                payload: None,
                sort_key: None,
                fields: vec![("title".into(), RedisJsonValue::String("Hello".into()))],
            };
            let output = FtSearchOutput::new(10, vec![doc]);
            assert_eq!(output.total_results(), 10);
            assert_eq!(output.len(), 1);
            assert!(!output.is_empty());
            assert_eq!(output.documents()[0].id, "doc1");
        }

        #[test]
        fn test_serialize_input() {
            let input = FtSearchInput {
                index: RedisJsonValue::String("test_idx".into()),
                query: RedisJsonValue::String("test".into()),
                no_content: Some(RedisJsonValue::Bool(true)),
                verbatim: None,
                no_stop_words: None,
                with_scores: None,
                with_payloads: None,
                with_sort_keys: None,
                filters: None,
                geo_filters: None,
                in_keys: None,
                in_fields: None,
                r#return: None,
                summarize: None,
                highlight: None,
                slop: None,
                timeout: None,
                in_order: None,
                language: None,
                expander: None,
                scorer: None,
                explain_score: None,
                payload: None,
                sort_by: None,
                limit: None,
                params: None,
                dialect: None,
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_idx"));
            assert!(json.contains("no_content"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtSearchOutput::new(5, vec![]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("total_results"));
            assert!(json.contains("documents"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.SEARCH requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_search_nonexistent_index() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtSearchInput {
                                index: RedisJsonValue::String("nonexistent".into()),
                                query: RedisJsonValue::String("*".into()),
                                no_content: None,
                                verbatim: None,
                                no_stop_words: None,
                                with_scores: None,
                                with_payloads: None,
                                with_sort_keys: None,
                                filters: None,
                                geo_filters: None,
                                in_keys: None,
                                in_fields: None,
                                r#return: None,
                                summarize: None,
                                highlight: None,
                                slop: None,
                                timeout: None,
                                in_order: None,
                                language: None,
                                expander: None,
                                scorer: None,
                                explain_score: None,
                                payload: None,
                                sort_by: None,
                                limit: None,
                                params: None,
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
