use crate::api::key::RedisKey;
use crate::api::value::RedisJsonValue;
use borsh::{BorshDeserialize, BorshSerialize};
use derive_builder::Builder;
use error::EpError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

// Double underscore maps Redis FT._LIST command name.
#[allow(non_snake_case)]
mod ft__list;
mod ft_aggregate;
mod ft_aliasadd;
mod ft_aliasdel;
mod ft_aliasupdate;
mod ft_alter;
mod ft_config_get;
mod ft_config_set;
mod ft_create;
mod ft_cursor_del;
mod ft_cursor_read;
mod ft_dictadd;
mod ft_dictdel;
mod ft_dictdump;
mod ft_dropindex;
mod ft_explain;
mod ft_explaincli;
mod ft_info;
mod ft_profile;
mod ft_search;
mod ft_spellcheck;
mod ft_syndump;
mod ft_synupdate;
mod ft_tagvals;

pub use ft__list::*;
pub use ft_aggregate::*;
pub use ft_aliasadd::*;
pub use ft_aliasdel::*;
pub use ft_aliasupdate::*;
pub use ft_alter::*;
pub use ft_config_get::*;
pub use ft_config_set::*;
pub use ft_create::*;
pub use ft_cursor_del::*;
pub use ft_cursor_read::*;
pub use ft_dictadd::*;
pub use ft_dictdel::*;
pub use ft_dictdump::*;
pub use ft_dropindex::*;
pub use ft_explain::*;
pub use ft_explaincli::*;
pub use ft_info::*;
pub use ft_profile::*;
pub use ft_search::*;
pub use ft_spellcheck::*;
pub use ft_syndump::*;
pub use ft_synupdate::*;
pub use ft_tagvals::*;

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Params {
    pub(crate) nargs: RedisJsonValue,
    pub(crate) parameters: Vec<Parameters>,
}

impl Params {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("PARAMS").arg(&self.nargs);

        for parameter in &self.parameters {
            parameter.cmd(command);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Parameters {
    pub(crate) name: RedisJsonValue,
    pub(crate) value: RedisJsonValue,
}

impl Parameters {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg(&self.name).arg(&self.value);
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct WithCursor {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) count: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) maxidle: Option<RedisJsonValue>,
}

impl WithCursor {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("WITHCURSOR");

        if let Some(count) = &self.count {
            command.arg("COUNT").arg(count);
        }

        if let Some(maxidle) = &self.maxidle {
            command.arg("MAXIDLE").arg(maxidle);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Limit {
    pub(crate) offset: RedisJsonValue,
    pub(crate) num: RedisJsonValue,
}

impl Limit {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("LIMIT").arg(&self.offset).arg(&self.num);
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Apply {
    pub(crate) expression: RedisJsonValue,
    pub(crate) name: RedisJsonValue,
}

impl Apply {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("APPLY").arg(&self.expression).arg("AS").arg(&self.name);
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Sortby {
    pub(crate) nargs: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) properties: Option<Vec<Property>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) with_count: Option<RedisJsonValue>,
}

impl Sortby {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("SORTBY").arg(&self.nargs);

        if let Some(properties) = &self.properties {
            for property in properties {
                property.cmd(command);
            }
        }

        if let Some(max) = &self.max {
            command.arg("MAX").arg(max);
        }

        if let Some(with_count) = &self.with_count {
            match with_count {
                RedisJsonValue::Bool(true) => {
                    command.arg("WITHCOUNT");
                }
                RedisJsonValue::Integer(n) if *n != 0 => {
                    command.arg("WITHCOUNT");
                }
                RedisJsonValue::String(s) if !s.is_empty() && s != "0" && s.to_uppercase() != "FALSE" => {
                    command.arg("WITHCOUNT");
                }
                _ => {}
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Property {
    pub(crate) property: RedisJsonValue,
    pub(crate) sort: Sort,
}

impl Property {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg(&self.property);
        match self.sort {
            Sort::ASC => command.arg("ASC"),
            Sort::DESC => command.arg("DESC"),
        };
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum Sort {
    #[default]
    ASC,
    DESC,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Load {
    pub(crate) count: RedisJsonValue,
    pub(crate) field: Vec<RedisJsonValue>,
}

impl Load {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("LOAD").arg(&self.count).arg(&self.field);
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Groupby {
    pub(crate) nargs: RedisJsonValue,
    pub(crate) property: Vec<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) groups: Option<Vec<Groups>>,
}

impl Groupby {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("GROUPBY").arg(&self.nargs).arg(&self.property);

        if let Some(groups) = &self.groups {
            for group in groups {
                group.cmd(command);
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Groups {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reduce: Option<Reduce>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) r#as: Option<RedisJsonValue>,
}

impl Groups {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        if let Some(reduce) = &self.reduce {
            command.arg("REDUCE").arg(&reduce.function).arg(&reduce.nargs).arg(&reduce.args);
        }
        if let Some(name) = &self.r#as {
            command.arg("AS").arg(name);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Reduce {
    pub(crate) function: RedisJsonValue,
    pub(crate) nargs: RedisJsonValue,
    pub(crate) args: Vec<RedisJsonValue>,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Schema {
    pub(crate) fields: Vec<SchemaFields>,
}

impl TryFrom<RedisJsonValue> for On {
    type Error = EpError;

    fn try_from(value: RedisJsonValue) -> Result<Self, Self::Error> {
        match value {
            RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                "HASH" => Ok(Self::HASH),
                "JSON" => Ok(Self::JSON),
                _ => Err(EpError::parse("ON must be HASH or JSON")),
            },
            _ => Err(EpError::parse("ON must be string")),
        }
    }
}

impl TryFrom<RedisJsonValue> for AttributeType {
    type Error = EpError;

    fn try_from(value: RedisJsonValue) -> Result<Self, Self::Error> {
        match value {
            RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                "TEXT" => Ok(Self::TEXT),
                "TAG" => Ok(Self::TAG),
                "NUMERIC" => Ok(Self::NUMERIC),
                "GEO" => Ok(Self::GEO),
                "VECTOR" => Ok(Self::VECTOR),
                "GEOSHAPE" => Ok(Self::GEOSHAPE(None)),
                _ => Err(EpError::parse("Invalid attribute type")),
            },
            _ => Err(EpError::parse("Attribute type must be string")),
        }
    }
}

impl Schema {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("SCHEMA");
        for field in &self.fields {
            field.cmd(command);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct SchemaFields {
    pub(crate) field_name: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) r#as: Option<RedisJsonValue>,
    pub(crate) attribute_type: AttributeType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) no_index: Option<RedisJsonValue>,
}

impl SchemaFields {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg(&self.field_name);

        if let Some(alias) = &self.r#as {
            command.arg("AS").arg(alias);
        }

        match &self.attribute_type {
            AttributeType::TEXT => {
                command.arg("TEXT");
            }
            AttributeType::TAG => {
                command.arg("TAG");
            }
            AttributeType::NUMERIC => {
                command.arg("NUMERIC");
            }
            AttributeType::GEO => {
                command.arg("GEO");
            }
            AttributeType::VECTOR => {
                command.arg("VECTOR");
            }
            AttributeType::GEOSHAPE(sortable) => {
                command.arg("GEOSHAPE");
                if let Some(sortable) = sortable {
                    command.arg("SORTABLE");
                    if let Some(unf) = &sortable.unf {
                        match unf {
                            RedisJsonValue::Bool(true) => {
                                command.arg("UNF");
                            }
                            RedisJsonValue::Integer(n) if *n != 0 => {
                                command.arg("UNF");
                            }
                            RedisJsonValue::String(s) if !s.is_empty() && s != "0" && s.to_uppercase() != "FALSE" => {
                                command.arg("UNF");
                            }
                            _ => {}
                        }
                    }
                }
            }
        };

        if let Some(no_index) = &self.no_index {
            match no_index {
                RedisJsonValue::Bool(true) => {
                    command.arg("NOINDEX");
                }
                RedisJsonValue::Integer(n) if *n != 0 => {
                    command.arg("NOINDEX");
                }
                RedisJsonValue::String(s) if !s.is_empty() && s != "0" && s.to_uppercase() != "FALSE" => {
                    command.arg("NOINDEX");
                }
                _ => {}
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum AttributeType {
    #[default]
    TEXT,
    TAG,
    NUMERIC,
    GEO,
    VECTOR,
    GEOSHAPE(Option<Sortable>),
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, Builder, ToSchema, JsonSchema)]
pub struct Sortable {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unf: Option<RedisJsonValue>,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Prefix {
    pub(crate) count: RedisJsonValue,
    pub(crate) prefix: Vec<RedisJsonValue>,
}

impl Prefix {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("PREFIX").arg(&self.count).arg(&self.prefix);
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum On {
    #[default]
    HASH,
    JSON,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct StopWords {
    pub(crate) count: RedisJsonValue,
    pub(crate) stop_words: Vec<RedisJsonValue>,
}

impl StopWords {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("STOPWORDS").arg(&self.count).arg(&self.stop_words);
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum OnOn {
    #[default]
    HASH,
    JSON,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum Profile {
    #[default]
    SEARCH,
    AGGREGATE,
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct Parameter {
    pub(crate) name: RedisJsonValue,
    pub(crate) value: RedisJsonValue,
}

impl Parameter {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg(&self.name).arg(&self.value);
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct SearchSortby {
    pub(crate) sort_by: RedisJsonValue,
    pub(crate) sort: Option<Sort>,
    pub(crate) with_count: Option<RedisJsonValue>,
}

impl SearchSortby {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("SORTBY").arg(&self.sort_by);
        if let Some(sort) = &self.sort {
            match sort {
                Sort::ASC => command.arg("ASC"),
                Sort::DESC => command.arg("DESC"),
            };
        }
        if let Some(with_count) = &self.with_count
            && (matches!(with_count, RedisJsonValue::Bool(true) | RedisJsonValue::Integer(1))
                || matches!(with_count, RedisJsonValue::String(s) if s == "1" || s.to_uppercase() == "TRUE" || s.to_uppercase() == "WITHCOUNT"))
        {
            command.arg("WITHCOUNT");
        }
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct Highlight {
    pub(crate) fields: Option<Fields>,
    pub(crate) tags: Option<Tags>,
}

impl Highlight {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("HIGHLIGHT");
        if let Some(fields) = &self.fields {
            fields.cmd(command);
        }
        if let Some(tags) = &self.tags {
            tags.cmd(command);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct Tags {
    pub(crate) open: RedisJsonValue,
    pub(crate) close: RedisJsonValue,
}

impl Tags {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("TAGS").arg(&self.open).arg(&self.close);
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub struct Summarize {
    pub(crate) fields: Option<Fields>,
    pub(crate) frags: Option<RedisJsonValue>,
    pub(crate) len: Option<RedisJsonValue>,
    pub(crate) separator: Option<RedisJsonValue>,
}

impl Summarize {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("SUMMARIZE");
        if let Some(fields) = &self.fields {
            fields.cmd(command);
        }
        if let Some(frags) = &self.frags {
            command.arg("FRAGS").arg(frags);
        }
        if let Some(len) = &self.len {
            command.arg("LEN").arg(len);
        }
        if let Some(separator) = &self.separator {
            command.arg("SEPARATOR").arg(separator);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct Fields {
    pub(crate) count: RedisJsonValue,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Fields {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("FIELDS").arg(&self.count);
        for field in &self.fields {
            command.arg(field);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct Return {
    pub(crate) count: RedisJsonValue,
    pub(crate) identifiers: Vec<Identifier>,
}

impl Return {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("RETURN").arg(&self.count);
        for id in &self.identifiers {
            id.cmd(command);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct Identifier {
    pub(crate) identifier: RedisJsonValue,
    pub(crate) r#as: Option<RedisJsonValue>,
}

impl Identifier {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg(&self.identifier);
        if let Some(r#as) = &self.r#as {
            command.arg("AS").arg(r#as);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct Infields {
    pub(crate) count: RedisJsonValue,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Infields {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("INFIELDS").arg(&self.count);
        for field in &self.fields {
            command.arg(field);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct Inkeys {
    pub(crate) count: RedisJsonValue,
    pub(crate) keys: Vec<RedisKey>,
}

impl Inkeys {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("INKEYS").arg(&self.count);
        for key in &self.keys {
            command.arg(key);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct Geofilter {
    pub(crate) geo_filter: RedisJsonValue,
    pub(crate) lon: RedisJsonValue,
    pub(crate) lat: RedisJsonValue,
    pub(crate) radius: RedisJsonValue,
    pub(crate) unit: Unit,
}

impl Geofilter {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("GEOFILTER").arg(&self.geo_filter).arg(&self.lon).arg(&self.lat).arg(&self.radius);

        match &self.unit {
            Unit::M => command.arg("m"),
            Unit::MI => command.arg("mi"),
            Unit::FT => command.arg("ft"),
            Unit::KM => command.arg("km"),
        };
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum Unit {
    #[default]
    M,
    KM,
    MI,
    FT,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
struct Filter {
    pub(crate) numeric_field: RedisJsonValue,
    pub(crate) min: RedisJsonValue,
    pub(crate) max: RedisJsonValue,
}

impl Filter {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("FILTER").arg(&self.numeric_field).arg(&self.min).arg(&self.max);
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct Terms {
    pub(crate) term: Term,
    pub(crate) dictionary: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) terms: Option<Vec<RedisJsonValue>>,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum Term {
    #[default]
    INCLUDE,
    EXCLUDE,
}

impl Terms {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg("TERMS");
        match self.term {
            Term::INCLUDE => {
                command.arg("INCLUDE");
            }
            Term::EXCLUDE => {
                command.arg("EXCLUDE");
            }
        };
        command.arg(&self.dictionary);
        if let Some(terms) = &self.terms {
            for term in terms {
                command.arg(term);
            }
        }
    }
}

/// Policy for FUNCTION RESTORE command
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, PartialEq, Eq, ToSchema, JsonSchema)]
pub enum RestorePolicy {
    /// Append functions; fail if library exists (default)
    #[default]
    APPEND,
    /// Delete all libraries before restoring
    FLUSH,
    /// Replace existing libraries
    REPLACE,
}
