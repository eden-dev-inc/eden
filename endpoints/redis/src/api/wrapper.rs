use std::io::Read;
use std::num::NonZeroUsize;
use std::ops::Deref;

use crate::api::value::RedisJsonValue;
use borsh::{BorshDeserialize, BorshSerialize};
use error::EpError;
use redis::geo::{RadiusOptions, RadiusOrder, Unit};
use redis::streams::{
    StreamAddOptions, StreamAutoClaimOptions, StreamClaimOptions, StreamMaxlen, StreamReadOptions, StreamTrimOptions, StreamTrimStrategy,
    StreamTrimmingMode,
};
use redis::{Direction, ExistenceCheck, ExpireOption, Expiry, LposOptions, SetExpiry, SetOptions};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use utoipa::openapi::{KnownFormat, ObjectBuilder, RefOr, Schema, SchemaFormat, Type};
use utoipa::{PartialSchema, ToSchema};

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum RuleWrapper {
    #[default]
    On,
    Off,
    AddCommand(String),
    RemoveCommand(String),
    AddCategory(String),
    RemoveCategory(String),
    AllCommands,
    NoCommands,
    AddPass(String),
    RemovePass(String),
    AddHashedPass(String),
    RemoveHashedPass(String),
    NoPass,
    ResetPass,
    Pattern(String),
    AllKeys,
    ResetKeys,
    Reset,
    Other(String),
}

impl From<RuleWrapper> for redis::acl::Rule {
    fn from(rule: RuleWrapper) -> Self {
        match rule {
            RuleWrapper::On => redis::acl::Rule::On,
            RuleWrapper::Off => redis::acl::Rule::Off,
            RuleWrapper::AddCommand(c) => redis::acl::Rule::AddCommand(c),
            RuleWrapper::RemoveCommand(c) => redis::acl::Rule::RemoveCommand(c),
            RuleWrapper::AddCategory(c) => redis::acl::Rule::AddCategory(c),
            RuleWrapper::RemoveCategory(c) => redis::acl::Rule::RemoveCategory(c),
            RuleWrapper::AllCommands => redis::acl::Rule::AllCommands,
            RuleWrapper::NoCommands => redis::acl::Rule::NoCommands,
            RuleWrapper::AddPass(c) => redis::acl::Rule::AddPass(c),
            RuleWrapper::RemovePass(c) => redis::acl::Rule::RemovePass(c),
            RuleWrapper::AddHashedPass(c) => redis::acl::Rule::AddHashedPass(c),
            RuleWrapper::RemoveHashedPass(c) => redis::acl::Rule::RemoveHashedPass(c),
            RuleWrapper::NoPass => redis::acl::Rule::NoPass,
            RuleWrapper::ResetPass => redis::acl::Rule::ResetPass,
            RuleWrapper::Pattern(c) => redis::acl::Rule::Pattern(c),
            RuleWrapper::AllKeys => redis::acl::Rule::AllKeys,
            RuleWrapper::ResetKeys => redis::acl::Rule::ResetKeys,
            RuleWrapper::Reset => redis::acl::Rule::Reset,
            RuleWrapper::Other(c) => redis::acl::Rule::Other(c),
        }
    }
}

impl TryFrom<RedisJsonValue> for RuleWrapper {
    type Error = EpError;

    fn try_from(value: RedisJsonValue) -> Result<Self, Self::Error> {
        match value {
            RedisJsonValue::String(s) => {
                let rule = match s.as_str() {
                    "on" => RuleWrapper::On,
                    "off" => RuleWrapper::Off,
                    "allcommands" => RuleWrapper::AllCommands,
                    "nocommands" => RuleWrapper::NoCommands,
                    "nopass" => RuleWrapper::NoPass,
                    "resetpass" => RuleWrapper::ResetPass,
                    "allkeys" => RuleWrapper::AllKeys,
                    "resetkeys" => RuleWrapper::ResetKeys,
                    "reset" => RuleWrapper::Reset,
                    s if s.starts_with("+") => {
                        if let Some(stripped) = s.strip_prefix("+@") {
                            RuleWrapper::AddCategory(stripped.to_string())
                        } else {
                            RuleWrapper::AddCommand(s[1..].to_string())
                        }
                    }
                    s if s.starts_with("-") => {
                        if let Some(stripped) = s.strip_prefix("-@") {
                            RuleWrapper::RemoveCategory(stripped.to_string())
                        } else {
                            RuleWrapper::RemoveCommand(s[1..].to_string())
                        }
                    }
                    s if s.starts_with(">") => RuleWrapper::AddPass(s[1..].to_string()),
                    s if s.starts_with("<") => RuleWrapper::RemovePass(s[1..].to_string()),
                    s if s.starts_with("#") => RuleWrapper::AddHashedPass(s[1..].to_string()),
                    s if s.starts_with("!") => RuleWrapper::RemoveHashedPass(s[1..].to_string()),
                    s if s.starts_with("~") => RuleWrapper::Pattern(s[1..].to_string()),
                    _ => RuleWrapper::Other(s),
                };
                Ok(rule)
            }
            _ => Err(EpError::request("ACL rules must be strings")),
        }
    }
}

impl From<&RuleWrapper> for RedisJsonValue {
    fn from(value: &RuleWrapper) -> RedisJsonValue {
        match value {
            RuleWrapper::AddCategory(s) => RedisJsonValue::String(format!("+@{s}")),
            RuleWrapper::RemoveCategory(s) => RedisJsonValue::String(format!("-@{s}")),
            RuleWrapper::AddCommand(s) => RedisJsonValue::String(format!("+{s}")),
            RuleWrapper::RemoveCommand(s) => RedisJsonValue::String(format!("-{s}")),
            RuleWrapper::AddPass(s) => RedisJsonValue::String(format!(">{s}")),
            RuleWrapper::RemovePass(s) => RedisJsonValue::String(format!("<{s}")),
            RuleWrapper::AddHashedPass(s) => RedisJsonValue::String(format!("#{s}")),
            RuleWrapper::Pattern(s) => RedisJsonValue::String(format!("~{s}")),
            RuleWrapper::RemoveHashedPass(s) => RedisJsonValue::String(format!("!{s}")),
            RuleWrapper::Other(s) => RedisJsonValue::String(s.to_string()),
            RuleWrapper::On => RedisJsonValue::String("on".to_string()),
            RuleWrapper::Off => RedisJsonValue::String("off".to_string()),
            RuleWrapper::AllCommands => RedisJsonValue::String("allcommands".to_string()),
            RuleWrapper::NoCommands => RedisJsonValue::String("nocommands".to_string()),
            RuleWrapper::NoPass => RedisJsonValue::String("nopass".to_string()),
            RuleWrapper::ResetPass => RedisJsonValue::String("resetpass".to_string()),
            RuleWrapper::AllKeys => RedisJsonValue::String("allkeys".to_string()),
            RuleWrapper::ResetKeys => RedisJsonValue::String("resetkeys".to_string()),
            RuleWrapper::Reset => RedisJsonValue::String("reset".to_string()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, ToSchema)]
pub enum DirectionWrapper {
    #[default]
    Left,
    Right,
}

impl From<DirectionWrapper> for Direction {
    fn from(wrapper: DirectionWrapper) -> Self {
        match wrapper {
            DirectionWrapper::Left => Self::Left,
            DirectionWrapper::Right => Self::Right,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, ToSchema)]
pub enum UnitWrapper {
    #[default]
    Meters,
    Kilometers,
    Miles,
    Feet,
}

impl From<UnitWrapper> for Unit {
    fn from(wrapper: UnitWrapper) -> Self {
        match wrapper {
            UnitWrapper::Meters => Self::Meters,
            UnitWrapper::Kilometers => Self::Kilometers,
            UnitWrapper::Miles => Self::Miles,
            UnitWrapper::Feet => Self::Feet,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema)]
pub struct RadiusOptionsWrapper {
    with_coord: bool,
    with_dist: bool,
    count: Option<usize>,
    order: RadiusOrderWrapper,
    store: Option<Vec<Vec<u8>>>,
    store_dist: Option<Vec<Vec<u8>>>,
}

impl From<RadiusOptionsWrapper> for RadiusOptions {
    fn from(wrapper: RadiusOptionsWrapper) -> Self {
        let options = Self::default();

        if wrapper.with_coord && wrapper.with_dist {
            options
                .with_dist()
                .with_coord()
                .limit(wrapper.count.unwrap_or_default())
                .order(RadiusOrder::from(wrapper.order))
                .store(wrapper.store)
                .store_dist(wrapper.store_dist)
        } else if wrapper.with_coord {
            options
                .with_coord()
                .limit(wrapper.count.unwrap_or_default())
                .order(RadiusOrder::from(wrapper.order))
                .store(wrapper.store)
                .store_dist(wrapper.store_dist)
        } else if wrapper.with_dist {
            options
                .with_dist()
                .limit(wrapper.count.unwrap_or_default())
                .order(RadiusOrder::from(wrapper.order))
                .store(wrapper.store)
                .store_dist(wrapper.store_dist)
        } else {
            options
                .limit(wrapper.count.unwrap_or_default())
                .order(RadiusOrder::from(wrapper.order))
                .store(wrapper.store)
                .store_dist(wrapper.store_dist)
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, ToSchema)]
pub enum RadiusOrderWrapper {
    #[default]
    Unsorted,
    Asc,
    Desc,
}

impl From<RadiusOrderWrapper> for RadiusOrder {
    fn from(wrapper: RadiusOrderWrapper) -> Self {
        match wrapper {
            RadiusOrderWrapper::Unsorted => Self::Unsorted,
            RadiusOrderWrapper::Asc => Self::Asc,
            RadiusOrderWrapper::Desc => Self::Desc,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, ToSchema)]
pub enum ExpiryWrapper {
    EX(u64),
    PX(u64),
    EXAT(u64),
    PXAT(u64),
    #[default]
    PERSIST,
}

impl From<ExpiryWrapper> for Expiry {
    fn from(wrapper: ExpiryWrapper) -> Self {
        match wrapper {
            ExpiryWrapper::EX(u64) => Self::EX(u64),
            ExpiryWrapper::PX(u64) => Self::PX(u64),
            ExpiryWrapper::EXAT(u64) => Self::EXAT(u64),
            ExpiryWrapper::PXAT(u64) => Self::PX(u64),
            ExpiryWrapper::PERSIST => Self::PERSIST,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, ToSchema)]
pub enum ExpireOptionWrapper {
    #[default]
    NONE,
    NX,
    XX,
    GT,
    LT,
}

impl From<ExpireOptionWrapper> for ExpireOption {
    fn from(wrapper: ExpireOptionWrapper) -> Self {
        match wrapper {
            ExpireOptionWrapper::NONE => Self::NONE,
            ExpireOptionWrapper::NX => Self::NX,
            ExpireOptionWrapper::XX => Self::XX,
            ExpireOptionWrapper::GT => Self::GT,
            ExpireOptionWrapper::LT => Self::LT,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, ToSchema)]
pub struct LposOptionsWrapper {
    count: Option<usize>,
    maxlen: Option<usize>,
    rank: Option<isize>,
}

impl From<LposOptionsWrapper> for LposOptions {
    fn from(wrapper: LposOptionsWrapper) -> Self {
        Self::default()
            .count(wrapper.count.unwrap_or_default())
            .maxlen(wrapper.maxlen.unwrap_or_default())
            .rank(wrapper.rank.unwrap_or_default())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, ToSchema)]
pub struct SetOptionsWrapper {
    conditional_set: Option<ExistenceCheckWrapper>,
    get: bool,
    expiration: Option<SetExpiryWrapper>,
}

impl From<SetOptionsWrapper> for SetOptions {
    fn from(wrapper: SetOptionsWrapper) -> Self {
        let option = Self::default();

        let option = match wrapper.conditional_set {
            Some(existence_check) => option.conditional_set(existence_check.into()),
            None => option,
        };

        let option = option.get(wrapper.get);

        match wrapper.expiration {
            Some(set_expiry) => option.with_expiration(set_expiry.into()),
            None => option,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, ToSchema)]
pub enum ExistenceCheckWrapper {
    #[default]
    NX,
    XX,
}

impl From<ExistenceCheckWrapper> for ExistenceCheck {
    fn from(wrapper: ExistenceCheckWrapper) -> Self {
        match wrapper {
            ExistenceCheckWrapper::NX => Self::NX,
            ExistenceCheckWrapper::XX => Self::XX,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, ToSchema)]
pub enum SetExpiryWrapper {
    EX(u64),
    PX(u64),
    EXAT(u64),
    PXAT(u64),
    #[default]
    KEEPTTL,
}

impl From<SetExpiryWrapper> for SetExpiry {
    fn from(wrapper: SetExpiryWrapper) -> Self {
        match wrapper {
            SetExpiryWrapper::EX(u64) => Self::EX(u64),
            SetExpiryWrapper::PX(u64) => Self::PX(u64),
            SetExpiryWrapper::EXAT(u64) => Self::EXAT(u64),
            SetExpiryWrapper::PXAT(u64) => Self::PXAT(u64),
            SetExpiryWrapper::KEEPTTL => Self::KEEPTTL,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, ToSchema)]
pub enum StreamMaxlenWrapper {
    Equals(usize),
    Approx(usize),
}

impl Default for StreamMaxlenWrapper {
    fn default() -> Self {
        Self::Equals(1)
    }
}

impl From<StreamMaxlenWrapper> for StreamMaxlen {
    fn from(wrapper: StreamMaxlenWrapper) -> Self {
        match wrapper {
            StreamMaxlenWrapper::Equals(len) => Self::Equals(len),
            StreamMaxlenWrapper::Approx(len) => Self::Approx(len),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema)]
pub struct StreamAddOptionsWrapper {
    nomkstream: bool,
    trim: Option<StreamTrimStrategyWrapper>,
}

impl From<StreamAddOptionsWrapper> for StreamAddOptions {
    fn from(wrapper: StreamAddOptionsWrapper) -> Self {
        let options = match wrapper.trim {
            Some(trim) => Self::default().trim(trim.into()),
            None => Self::default(),
        };

        match wrapper.nomkstream {
            true => options.nomkstream(),
            false => options,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub enum StreamTrimStrategyWrapper {
    MaxLen(StreamTrimmingModeWrapper, usize, Option<usize>),
    MinId(StreamTrimmingModeWrapper, String, Option<usize>),
}

impl Default for StreamTrimStrategyWrapper {
    fn default() -> Self {
        Self::MaxLen(StreamTrimmingModeWrapper::default(), 1, None)
    }
}

impl From<StreamTrimStrategyWrapper> for StreamTrimStrategy {
    fn from(wrapper: StreamTrimStrategyWrapper) -> Self {
        match wrapper {
            StreamTrimStrategyWrapper::MaxLen(stream, size, option) => Self::MaxLen(stream.into(), size, option),
            StreamTrimStrategyWrapper::MinId(stream, string, option) => Self::MinId(stream.into(), string, option),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, ToSchema)]
pub enum StreamTrimmingModeWrapper {
    #[default]
    Exact,
    Approx,
}

impl From<StreamTrimmingModeWrapper> for StreamTrimmingMode {
    fn from(wrapper: StreamTrimmingModeWrapper) -> Self {
        match wrapper {
            StreamTrimmingModeWrapper::Exact => Self::Exact,
            StreamTrimmingModeWrapper::Approx => Self::Approx,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, ToSchema)]
pub struct StreamAutoClaimOptionsWrapper {
    count: Option<usize>,
    justid: bool,
}

impl From<StreamAutoClaimOptionsWrapper> for StreamAutoClaimOptions {
    fn from(wrapper: StreamAutoClaimOptionsWrapper) -> Self {
        let stream = Self::default().count(wrapper.count.unwrap_or_default());

        match wrapper.justid {
            true => stream.with_justid(),
            false => stream,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema)]
pub struct StreamClaimOptionsWrapper {
    idle: Option<usize>,
    time: Option<usize>,
    retry: Option<usize>,
    force: bool,
    justid: bool,
    lastid: Option<String>,
}

impl From<StreamClaimOptionsWrapper> for StreamClaimOptions {
    fn from(wrapper: StreamClaimOptionsWrapper) -> Self {
        let stream = Self::default();

        let stream = match wrapper.idle {
            Some(idle) => stream.idle(idle),
            None => stream,
        };

        let stream = match wrapper.time {
            Some(time) => stream.time(time),
            None => stream,
        };

        let stream = match wrapper.retry {
            Some(retry) => stream.retry(retry),
            None => stream,
        };

        let stream = match wrapper.force {
            true => stream.with_force(),
            false => stream,
        };

        let stream = match wrapper.justid {
            true => stream.with_justid(),
            false => stream,
        };

        match wrapper.lastid {
            Some(lastid) => stream.with_lastid(lastid),
            None => stream,
        }
    }
}

type StreamReadGroup = (Vec<Vec<u8>>, Vec<Vec<u8>>);

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct SRGroup(pub Option<StreamReadGroup>);
impl ToSchema for SRGroup {}
impl PartialSchema for SRGroup {
    fn schema() -> RefOr<Schema> {
        RefOr::T(
            Schema::Object(ObjectBuilder::new().property("0", <Vec<Vec<u8>>>::schema()).build()), // <Option<(Vec<Vec<u8>>, Vec<Vec<u8>>)>>::schema(),
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema)]
pub struct StreamReadOptionsWrapper {
    block: Option<usize>,
    count: Option<usize>,
    noack: Option<bool>,
    group: SRGroup,
}

impl From<StreamReadOptionsWrapper> for StreamReadOptions {
    fn from(wrapper: StreamReadOptionsWrapper) -> Self {
        let stream = Self::default();

        let stream = match wrapper.block {
            Some(block) => stream.block(block),
            None => stream,
        };

        let stream = match wrapper.count {
            Some(count) => stream.count(count),
            None => stream,
        };

        let stream = match wrapper.noack {
            Some(bool) => {
                if bool {
                    stream.noack()
                } else {
                    stream
                }
            }
            None => stream,
        };

        match wrapper.group.0 {
            Some(group) => stream.group(group.0, group.1),
            None => stream,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema)]
pub struct StreamTrimOptionsWrapper {
    strategy: StreamTrimStrategyWrapper,
}

impl From<StreamTrimOptionsWrapper> for StreamTrimOptions {
    fn from(wrapper: StreamTrimOptionsWrapper) -> Self {
        match wrapper.strategy {
            StreamTrimStrategyWrapper::MaxLen(stream, size, option) => {
                let stream = Self::maxlen(stream.into(), size);

                match option {
                    Some(limit) => stream.limit(limit),
                    None => stream,
                }
            }
            StreamTrimStrategyWrapper::MinId(stream, string, option) => {
                let stream = Self::minid(stream.into(), string);

                match option {
                    Some(limit) => stream.limit(limit),
                    None => stream,
                }
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema)]
pub struct StreamAutoClaimOptionWrapper {
    count: Option<usize>,
    justid: bool,
}

impl From<StreamAutoClaimOptionWrapper> for StreamAutoClaimOptions {
    fn from(wrapper: StreamAutoClaimOptionWrapper) -> Self {
        let stream = Self::default();

        let stream = match wrapper.count {
            Some(count) => stream.count(count),
            None => stream,
        };

        match wrapper.justid {
            true => stream.with_justid(),
            false => stream,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NonZeroUsizeWrapper(pub NonZeroUsize);
impl ToSchema for NonZeroUsizeWrapper {}
impl PartialSchema for NonZeroUsizeWrapper {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new().schema_type(Type::Integer).format(Some(SchemaFormat::KnownFormat(KnownFormat::Int64))).build(),
        ))
    }
}

impl Deref for NonZeroUsizeWrapper {
    type Target = NonZeroUsize;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for NonZeroUsizeWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Serialize::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for NonZeroUsizeWrapper {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let n = <NonZeroUsize as Deserialize>::deserialize(d)?;
        Ok(Self(n))
    }
}

impl BorshSerialize for NonZeroUsizeWrapper {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        BorshSerialize::serialize(&self.0, writer)
    }
}

impl BorshDeserialize for NonZeroUsizeWrapper {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, std::io::Error> {
        Self::deserialize_reader(&mut *buf)
    }

    fn deserialize_reader<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let n = <NonZeroUsize>::deserialize_reader(reader)?;
        Ok(Self(n))
    }
}
