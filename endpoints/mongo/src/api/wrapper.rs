use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{
    Array, Binary, Bson, DateTime, DbPointer, Decimal128, Document, JavaScriptCodeWithScope, RawArrayBuf, RawBson, RawDocumentBuf,
    RawJavaScriptCodeWithScope, Regex, Timestamp, doc,
};
use mongodb::change_stream::event::ResumeToken;
use mongodb::options::{
    Acknowledgment, AggregateOptions, ChangeStreamOptions, ChangeStreamPreAndPostImages, ClusteredIndex, Collation, CollationAlternate,
    CollationCaseFirst, CollationMaxVariable, CollationStrength, CollectionOptions, CommitQuorum, CountOptions, CreateCollectionOptions,
    CreateIndexOptions, CreateSearchIndexOptions, CursorType, DatabaseOptions, DeleteOptions, DistinctOptions, DropCollectionOptions,
    DropDatabaseOptions, DropIndexOptions, DropSearchIndexOptions, EstimatedDocumentCountOptions, FindOneAndDeleteOptions,
    FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOneOptions, FindOptions, FullDocumentBeforeChangeType, FullDocumentType,
    GridFsBucketOptions, GridFsFindOptions, HedgedReadOptions, Hint, IndexOptionDefaults, IndexOptions, IndexVersion, InsertManyOptions,
    InsertOneOptions, ListCollectionsOptions, ListDatabasesOptions, ListIndexesOptions, ListSearchIndexOptions, ReadConcern,
    ReadConcernLevel, ReadPreference, ReadPreferenceOptions, ReplaceOptions, ReturnDocument, RunCursorCommandOptions, SelectionCriteria,
    SessionOptions, Sphere2DIndexVersion, TextIndexVersion, TimeseriesGranularity, TimeseriesOptions, TransactionOptions,
    UpdateModifications, UpdateOptions, UpdateSearchIndexOptions, ValidationAction, ValidationLevel, WriteConcern,
};
use mongodb::{IndexModel, SearchIndexModel, bson};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use utoipa::{PartialSchema, ToSchema};

#[derive(Debug, Serialize, Deserialize, Default, ToSchema, Clone, JsonSchema)]
pub struct SessionOptionsWrapper {
    pub default_transaction_options: Option<TransactionOptionsWrapper>,
    pub causal_consistency: Option<bool>,
    pub snapshot: Option<bool>,
}

impl From<SessionOptionsWrapper> for SessionOptions {
    fn from(wrapper: SessionOptionsWrapper) -> Self {
        Self::builder()
            .default_transaction_options(wrapper.default_transaction_options.map(Into::into))
            .causal_consistency(wrapper.causal_consistency)
            .snapshot(wrapper.snapshot)
            .build()
    }
}

// #[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, ToSchema, JsonSchema)]
// pub struct DocumentWrapper {
//     pub inner: HashMap<String, BsonWrapper>,
// }
pub trait DocumentFunction {
    fn into_document(self) -> Document;
    fn from_document(doc: Document) -> Self;
    fn bson_to_wrapper(bson: Bson) -> BsonWrapper; // Fixed function name typo
}

#[derive(Debug, Clone, Default, ToSchema, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct DocumentWrapper(pub DocumentWrapperType);

pub type DocumentWrapperType = HashMap<String, BsonWrapper>;

impl From<Value> for DocumentWrapper {
    fn from(value: Value) -> Self {
        match value {
            Value::Object(map) => {
                let mut wrapper = DocumentWrapperType::default();
                for (key, val) in map {
                    wrapper.insert(key, BsonWrapper::from(val));
                }
                DocumentWrapper(wrapper)
            }
            _ => DocumentWrapper(DocumentWrapperType::default()), // Return empty HashMap for non-object values
        }
    }
}

impl From<Document> for DocumentWrapper {
    fn from(doc: Document) -> Self {
        Self(DocumentWrapperType::from_document(doc))
    }
}

impl From<DocumentWrapper> for Document {
    fn from(wrapper: DocumentWrapper) -> Self {
        wrapper.0.into_document()
    }
}

impl DocumentFunction for DocumentWrapperType {
    fn from_document(doc: bson::Document) -> Self {
        let mut wrapper = DocumentWrapperType::default();
        for (key, value) in doc {
            wrapper.insert(key, Self::bson_to_wrapper(value));
        }
        wrapper
    }

    fn into_document(self) -> Document {
        Document::from_iter(self.into_iter().map(|(k, v)| (k, v.into())))
    }

    // Helper function to convert Bson to BsonWrapper
    fn bson_to_wrapper(bson: Bson) -> BsonWrapper {
        match bson {
            Bson::Double(v) => BsonWrapper::Double(v),
            Bson::String(v) => BsonWrapper::String(v),
            Bson::Array(v) => BsonWrapper::Array(ArrayWrapper(
                v.into_iter().map(Self::bson_to_wrapper).collect(), // Use Self:: here
            )),
            Bson::Document(v) => BsonWrapper::Document(DocumentWrapper(DocumentWrapperType::from_document(v))), // Use Self:: here
            Bson::Boolean(v) => BsonWrapper::Boolean(v),
            Bson::Null => BsonWrapper::Null,
            Bson::RegularExpression(v) => BsonWrapper::RegularExpression(RegexWrapper { pattern: v.pattern, options: v.options }),
            Bson::JavaScriptCode(v) => BsonWrapper::JavaScriptCode(v),
            Bson::JavaScriptCodeWithScope(v) => {
                BsonWrapper::JavaScriptCodeWithScope(JavaScriptCodeWithScopeWrapper {
                    code: v.code,
                    scope: DocumentWrapper(DocumentWrapperType::from_document(v.scope)), // Use Self:: here
                })
            }
            Bson::Int32(v) => BsonWrapper::Int32(v),
            Bson::Int64(v) => BsonWrapper::Int64(v),
            Bson::Timestamp(v) => BsonWrapper::Timestamp(TimestampWrapper { time: v.time, increment: v.increment }),
            Bson::Binary(v) => {
                let subtype = match v.subtype {
                    BinarySubtype::Generic => BinarySubtypeWrapper::Generic,
                    BinarySubtype::Function => BinarySubtypeWrapper::Function,
                    BinarySubtype::BinaryOld => BinarySubtypeWrapper::BinaryOld,
                    BinarySubtype::UuidOld => BinarySubtypeWrapper::UuidOld,
                    BinarySubtype::Uuid => BinarySubtypeWrapper::Uuid,
                    BinarySubtype::Md5 => BinarySubtypeWrapper::Md5,
                    BinarySubtype::Encrypted => BinarySubtypeWrapper::Encrypted,
                    BinarySubtype::Column => BinarySubtypeWrapper::Column,
                    BinarySubtype::Sensitive => BinarySubtypeWrapper::Sensitive,
                    BinarySubtype::Vector => BinarySubtypeWrapper::Vector,
                    BinarySubtype::UserDefined(id) => BinarySubtypeWrapper::UserDefined(id),
                    BinarySubtype::Reserved(id) => BinarySubtypeWrapper::Reserved(id),
                    _ => BinarySubtypeWrapper::Generic, //TODO handle error
                };
                BsonWrapper::Binary(BinaryWrapper { subtype, bytes: v.bytes })
            }
            Bson::ObjectId(v) => BsonWrapper::ObjectId(ObjectIdWrapper { id: v.bytes() }),
            Bson::DateTime(v) => BsonWrapper::DateTime(DateTimeWrapper(v.timestamp_millis())),
            Bson::Decimal128(v) => BsonWrapper::Decimal128(Decimal128Wrapper { bytes: v.bytes() }),
            Bson::MaxKey => BsonWrapper::MaxKey,
            Bson::MinKey => BsonWrapper::MinKey,
            _ => BsonWrapper::Null, // Handle other types or defaults
        }
    }
}

#[derive(Debug, Clone, Default, ToSchema, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(untagged)]
pub enum BsonWrapper {
    Double(f64),
    String(String),
    Array(ArrayWrapper),
    Document(DocumentWrapper),
    Boolean(bool),
    #[default]
    Null,
    RegularExpression(RegexWrapper),
    JavaScriptCode(String),
    JavaScriptCodeWithScope(JavaScriptCodeWithScopeWrapper),
    Int32(i32),
    Int64(i64),
    Timestamp(TimestampWrapper),
    Binary(BinaryWrapper),
    ObjectId(ObjectIdWrapper),
    DateTime(DateTimeWrapper),
    Decimal128(Decimal128Wrapper),
    MaxKey,
    MinKey,
}

impl From<BsonWrapper> for Bson {
    fn from(wrapper: BsonWrapper) -> Self {
        match wrapper {
            BsonWrapper::Double(v) => Bson::Double(v),
            BsonWrapper::String(v) => Bson::String(v),
            BsonWrapper::Array(v) => Bson::Array(v.into()),
            BsonWrapper::Document(v) => Bson::Document(v.into()),
            BsonWrapper::Boolean(v) => Bson::Boolean(v),
            BsonWrapper::Null => Bson::Null,
            BsonWrapper::RegularExpression(v) => Bson::RegularExpression(v.into()),
            BsonWrapper::JavaScriptCode(v) => Bson::JavaScriptCode(v),
            BsonWrapper::JavaScriptCodeWithScope(v) => Bson::JavaScriptCodeWithScope(v.into()),
            BsonWrapper::Int32(v) => Bson::Int32(v),
            BsonWrapper::Int64(v) => Bson::Int64(v),
            BsonWrapper::Timestamp(v) => Bson::Timestamp(v.into()),
            BsonWrapper::Binary(v) => Bson::Binary(v.into()),
            BsonWrapper::ObjectId(v) => Bson::ObjectId(v.into()),
            BsonWrapper::DateTime(v) => Bson::DateTime(v.into()),
            BsonWrapper::Decimal128(v) => Bson::Decimal128(v.into()),
            BsonWrapper::MaxKey => Bson::MaxKey,
            BsonWrapper::MinKey => Bson::MinKey,
        }
    }
}

impl From<Value> for BsonWrapper {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => BsonWrapper::Null,
            Value::Bool(b) => BsonWrapper::Boolean(b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        BsonWrapper::Int32(i as i32)
                    } else {
                        BsonWrapper::Int64(i)
                    }
                } else if let Some(f) = n.as_f64() {
                    BsonWrapper::Double(f)
                } else {
                    BsonWrapper::Null
                }
            }
            Value::String(s) => BsonWrapper::String(s),
            Value::Array(arr) => {
                let bson_array: Vec<BsonWrapper> = arr.into_iter().map(BsonWrapper::from).collect();
                BsonWrapper::Array(ArrayWrapper(bson_array))
            }
            Value::Object(obj) => {
                // Recursively convert nested objects
                BsonWrapper::Document(DocumentWrapper::from(Value::Object(obj)))
            }
        }
    }
}

#[derive(Debug, Clone, Default, ToSchema, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct ArrayWrapper(Vec<BsonWrapper>);

impl From<ArrayWrapper> for Array {
    fn from(wrapper: ArrayWrapper) -> Self {
        wrapper.0.into_iter().map(Into::into).collect()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, ToSchema, Serialize, Deserialize, JsonSchema)]
pub struct RegexWrapper {
    pub pattern: String,
    pub options: String,
}

impl From<RegexWrapper> for Regex {
    fn from(wrapper: RegexWrapper) -> Self {
        Self { pattern: wrapper.pattern, options: wrapper.options }
    }
}

#[derive(Debug, Clone, ToSchema, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct JavaScriptCodeWithScopeWrapper {
    pub code: String,
    pub scope: DocumentWrapper,
}

impl From<JavaScriptCodeWithScopeWrapper> for JavaScriptCodeWithScope {
    fn from(wrapper: JavaScriptCodeWithScopeWrapper) -> Self {
        Self { code: wrapper.code, scope: wrapper.scope.into() }
    }
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Clone, Copy, Hash, ToSchema, Serialize, Deserialize, JsonSchema)]
pub struct TimestampWrapper {
    pub time: u32,
    pub increment: u32,
}

impl From<TimestampWrapper> for Timestamp {
    fn from(wrapper: TimestampWrapper) -> Self {
        Self { time: wrapper.time, increment: wrapper.increment }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, ToSchema, Serialize, Deserialize, JsonSchema)]
pub struct BinaryWrapper {
    pub subtype: BinarySubtypeWrapper,
    pub bytes: Vec<u8>,
}

impl From<BinaryWrapper> for Binary {
    fn from(wrapper: BinaryWrapper) -> Self {
        Self { subtype: wrapper.subtype.into(), bytes: wrapper.bytes }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, ToSchema, Serialize, Deserialize, JsonSchema)]
pub enum BinarySubtypeWrapper {
    Generic,
    Function,
    BinaryOld,
    UuidOld,
    Uuid,
    Md5,
    Encrypted,
    Column,
    Sensitive,
    Vector,
    UserDefined(u8),
    Reserved(u8),
}

impl From<BinarySubtypeWrapper> for BinarySubtype {
    fn from(wrapper: BinarySubtypeWrapper) -> Self {
        match wrapper {
            BinarySubtypeWrapper::Generic => BinarySubtype::Generic,
            BinarySubtypeWrapper::Function => BinarySubtype::Function,
            BinarySubtypeWrapper::BinaryOld => BinarySubtype::BinaryOld,
            BinarySubtypeWrapper::UuidOld => BinarySubtype::UuidOld,
            BinarySubtypeWrapper::Uuid => BinarySubtype::Uuid,
            BinarySubtypeWrapper::Md5 => BinarySubtype::Md5,
            BinarySubtypeWrapper::Encrypted => BinarySubtype::Encrypted,
            BinarySubtypeWrapper::Column => BinarySubtype::Column,
            BinarySubtypeWrapper::Sensitive => BinarySubtype::Sensitive,
            BinarySubtypeWrapper::Vector => BinarySubtype::Vector,
            BinarySubtypeWrapper::UserDefined(id) => BinarySubtype::UserDefined(id),
            BinarySubtypeWrapper::Reserved(id) => BinarySubtype::Reserved(id),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, ToSchema, Serialize, Deserialize, JsonSchema)]
pub struct ObjectIdWrapper {
    id: [u8; 12],
}

impl From<ObjectIdWrapper> for ObjectId {
    fn from(wrapper: ObjectIdWrapper) -> Self {
        Self::from_bytes(wrapper.id)
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Copy, Clone, ToSchema, Serialize, Deserialize, JsonSchema)]
pub struct DateTimeWrapper(i64);

impl From<DateTimeWrapper> for DateTime {
    fn from(wrapper: DateTimeWrapper) -> Self {
        Self::from_millis(wrapper.0)
    }
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, ToSchema, Serialize, Deserialize, JsonSchema)]
pub struct Decimal128Wrapper {
    /// BSON bytes containing the decimal128. Stored for round tripping.
    bytes: [u8; 16],
}

impl From<Decimal128Wrapper> for Decimal128 {
    fn from(wrapper: Decimal128Wrapper) -> Self {
        Self::from_bytes(wrapper.bytes)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema, DocumentInput, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AggregateOptionsWrapper {
    pub allow_disk_use: Option<bool>,
    #[serde(serialize_with = "serialize_u32_option_as_batch_size", rename(serialize = "cursor"))]
    pub batch_size: Option<u32>,
    pub bypass_document_validation: Option<bool>,
    pub collation: Option<CollationWrapper>,
    // TODO RUST-1364: Update this field to be of type Option<Bson>
    #[serde(skip_serializing)]
    pub comment: Option<String>,
    #[serde(rename(serialize = "comment"))]
    pub comment_bson: Option<BsonWrapper>,
    pub hint: Option<HintWrapper>,
    #[serde(skip_serializing, deserialize_with = "deserialize_duration_option_from_u64_millis", default)]
    pub max_await_time: Option<DurationWrapper>,
    #[serde(
        serialize_with = "serialize_duration_option_as_int_millis",
        rename = "maxTimeMS",
        deserialize_with = "deserialize_duration_option_from_u64_millis",
        default
    )]
    pub max_time: Option<DurationWrapper>,
    #[serde(skip_serializing)]
    pub read_concern: Option<ReadConcernWrapper>,
    #[serde(skip_serializing)]
    #[serde(rename = "readPreference")]
    pub selection_criteria: Option<SelectionCriteriaWrapper>,
    pub write_concern: Option<WriteConcernWrapper>,
    #[serde(rename = "let")]
    pub let_vars: Option<DocumentWrapper>,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) fn serialize_u32_option_as_batch_size<S: Serializer>(val: &Option<u32>, serializer: S) -> std::result::Result<S::Ok, S::Error> {
    match val {
        #[allow(clippy::cast_possible_wrap)]
        Some(val) if *val <= i32::MAX as u32 => (doc! {
            "batchSize": (*val as i32)
        })
        .serialize(serializer),
        None => Document::new().serialize(serializer),
        _ => Err(serde::ser::Error::custom("batch size must be able to fit into a signed 32-bit integer")),
    }
}

impl From<AggregateOptionsWrapper> for AggregateOptions {
    fn from(wrapper: AggregateOptionsWrapper) -> Self {
        Self::builder()
            .allow_disk_use(wrapper.allow_disk_use)
            .batch_size(wrapper.batch_size)
            .bypass_document_validation(wrapper.bypass_document_validation)
            .collation(wrapper.collation.map(Into::into))
            .comment(wrapper.comment)
            .comment_bson(wrapper.comment_bson.map(Into::into))
            .hint(wrapper.hint.map(Into::into))
            .max_await_time(wrapper.max_await_time.map(Into::into))
            .max_time(wrapper.max_time.map(Into::into))
            .read_concern(wrapper.read_concern.map(Into::into))
            .selection_criteria(wrapper.selection_criteria.map(Into::into))
            .write_concern(wrapper.write_concern.map(Into::into))
            .let_vars(wrapper.let_vars.map(Into::into))
            .build()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema, DocumentInput, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CollationWrapper {
    pub locale: String,
    pub strength: Option<CollationStrengthWrapper>,
    pub case_level: Option<bool>,
    pub case_first: Option<CollationCaseFirstWrapper>,
    pub numeric_ordering: Option<bool>,
    pub alternate: Option<CollationAlternateWrapper>,
    pub max_variable: Option<CollationMaxVariableWrapper>,
    pub normalization: Option<bool>,
    pub backwards: Option<bool>,
}

impl From<CollationWrapper> for Collation {
    fn from(wrapper: CollationWrapper) -> Self {
        Self::builder()
            .locale(wrapper.locale)
            .strength(wrapper.strength.map(Into::into))
            .case_level(wrapper.case_level)
            .case_first(wrapper.case_first.map(Into::into))
            .numeric_ordering(wrapper.numeric_ordering)
            .alternate(wrapper.alternate.map(Into::into))
            .max_variable(wrapper.max_variable.map(Into::into))
            .normalization(wrapper.normalization)
            .backwards(wrapper.backwards)
            .build()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, JsonSchema)]
pub enum CollationStrengthWrapper {
    Primary,
    Secondary,
    Tertiary,
    Quaternary,
    Identical,
}

impl From<CollationStrengthWrapper> for CollationStrength {
    fn from(wrapper: CollationStrengthWrapper) -> Self {
        match wrapper {
            CollationStrengthWrapper::Primary => Self::Primary,
            CollationStrengthWrapper::Secondary => Self::Secondary,
            CollationStrengthWrapper::Tertiary => Self::Tertiary,
            CollationStrengthWrapper::Quaternary => Self::Quaternary,
            CollationStrengthWrapper::Identical => Self::Identical,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum CollationCaseFirstWrapper {
    Upper,
    Lower,
    Off,
}

impl From<CollationCaseFirstWrapper> for CollationCaseFirst {
    fn from(wrapper: CollationCaseFirstWrapper) -> Self {
        match wrapper {
            CollationCaseFirstWrapper::Upper => Self::Upper,
            CollationCaseFirstWrapper::Lower => Self::Lower,
            CollationCaseFirstWrapper::Off => Self::Off,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum CollationAlternateWrapper {
    NonIgnorable,
    Shifted,
}

impl From<CollationAlternateWrapper> for CollationAlternate {
    fn from(wrapper: CollationAlternateWrapper) -> Self {
        match wrapper {
            CollationAlternateWrapper::NonIgnorable => Self::NonIgnorable,
            CollationAlternateWrapper::Shifted => Self::Shifted,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum CollationMaxVariableWrapper {
    Punct,
    Space,
}

impl From<CollationMaxVariableWrapper> for CollationMaxVariable {
    fn from(wrapper: CollationMaxVariableWrapper) -> Self {
        match wrapper {
            CollationMaxVariableWrapper::Punct => Self::Punct,
            CollationMaxVariableWrapper::Space => Self::Space,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema, JsonSchema)]
#[serde(untagged)]
#[non_exhaustive]
pub enum HintWrapper {
    Keys(DocumentWrapper),
    Name(String),
}

impl From<HintWrapper> for Hint {
    fn from(wrapper: HintWrapper) -> Self {
        match wrapper {
            HintWrapper::Keys(v) => Self::Keys(v.into()),
            HintWrapper::Name(v) => Self::Name(v),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema, JsonSchema)]
#[non_exhaustive]
pub struct WriteConcernWrapper {
    pub w: Option<AcknowledgmentWrapper>,
    #[serde(rename = "wtimeout", alias = "wtimeoutMS")]
    #[serde(serialize_with = "serialize_duration_option_as_int_millis")]
    #[serde(deserialize_with = "deserialize_duration_option_from_u64_millis")]
    #[serde(default)]
    pub w_timeout: Option<DurationWrapper>,
    #[serde(rename = "j", alias = "journal")]
    pub journal: Option<bool>,
}

impl From<WriteConcernWrapper> for WriteConcern {
    fn from(wrapper: WriteConcernWrapper) -> Self {
        Self::builder()
            .w(wrapper.w.map(Into::into))
            .w_timeout(wrapper.w_timeout.map(Into::into))
            .journal(wrapper.journal)
            .build()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema, JsonSchema)]
#[non_exhaustive]
pub enum AcknowledgmentWrapper {
    Nodes(u32),
    Majority,
    Custom(String),
}

impl From<AcknowledgmentWrapper> for Acknowledgment {
    fn from(wrapper: AcknowledgmentWrapper) -> Self {
        match wrapper {
            AcknowledgmentWrapper::Nodes(n) => Self::Nodes(n),
            AcknowledgmentWrapper::Majority => Self::Majority,
            AcknowledgmentWrapper::Custom(n) => Self::Custom(n),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct DurationWrapper {
    secs: u64,
    nanos: u32, // Always 0 <= nanos < NANOS_PER_SEC
}

impl DurationWrapper {
    pub const fn from_secs(secs: u64) -> Self {
        Self { secs, nanos: 0 }
    }
    pub const fn as_secs(&self) -> u64 {
        self.secs
    }
    pub const fn as_secs_f64(&self) -> f64 {
        (self.secs as f64) + (self.nanos as f64) / (NANOS_PER_SEC as f64)
    }
    pub const fn as_millis(&self) -> u128 {
        self.secs as u128 * MILLIS_PER_SEC as u128 + (self.nanos / NANOS_PER_MILLI) as u128
    }

    pub const fn from_millis(millis: u64) -> DurationWrapper {
        let secs = millis / MILLIS_PER_SEC;
        let subsec_millis = (millis % MILLIS_PER_SEC) as u32;
        // SAFETY: (x % 1_000) * 1_000_000 < 1_000_000_000
        //         => x % 1_000 < 1_000
        let subsec_nanos = subsec_millis * NANOS_PER_MILLI;

        DurationWrapper { secs, nanos: subsec_nanos }
    }
}

const MILLIS_PER_SEC: u64 = 1_000;
const NANOS_PER_MILLI: u32 = 1_000_000;
const NANOS_PER_SEC: u32 = 1_000_000_000;

impl From<DurationWrapper> for Duration {
    fn from(wrapper: DurationWrapper) -> Self {
        Self::new(wrapper.secs, wrapper.nanos)
    }
}

//TODO

impl SessionOptionsWrapper {
    pub fn as_session_options(&self) -> SessionOptions {
        SessionOptions::builder()
            .default_transaction_options(self.default_transaction_options.to_owned().map(Into::into))
            .causal_consistency(self.causal_consistency.to_owned())
            .snapshot(self.snapshot.to_owned())
            .build()
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, JsonSchema)]
pub struct TransactionOptionsWrapper {
    pub read_concern: Option<ReadConcernWrapper>,
    pub write_concern: Option<WriteConcernWrapper>,
    pub selection_criteria: Option<SelectionCriteriaWrapper>,
    pub max_commit_time: Option<DurationWrapper>,
}

impl From<TransactionOptionsWrapper> for TransactionOptions {
    fn from(wrapper: TransactionOptionsWrapper) -> Self {
        Self::builder()
            .read_concern(wrapper.read_concern.map(Into::into))
            .write_concern(wrapper.write_concern.map(Into::into))
            .selection_criteria(wrapper.selection_criteria.map(Into::into))
            .max_commit_time(wrapper.max_commit_time.map(Into::into))
            .build()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, ToSchema, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReadConcernWrapper {
    pub level: ReadConcernLevelWrapper,
}

impl From<ReadConcernWrapper> for ReadConcern {
    fn from(wrapper: ReadConcernWrapper) -> Self {
        match wrapper.level {
            ReadConcernLevelWrapper::Local => ReadConcern::local(),
            ReadConcernLevelWrapper::Majority => ReadConcern::majority(),
            ReadConcernLevelWrapper::Linearizable => ReadConcern::linearizable(),
            ReadConcernLevelWrapper::Available => ReadConcern::available(),
            ReadConcernLevelWrapper::Snapshot => ReadConcern::snapshot(),
            ReadConcernLevelWrapper::Custom(s) => ReadConcern::custom(s),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema, JsonSchema)]
pub enum ReadConcernLevelWrapper {
    Local,
    Majority,
    Linearizable,
    Available,
    Snapshot,
    Custom(String),
}

impl From<ReadConcernLevelWrapper> for ReadConcernLevel {
    fn from(wrapper: ReadConcernLevelWrapper) -> Self {
        match wrapper {
            ReadConcernLevelWrapper::Local => Self::Local,
            ReadConcernLevelWrapper::Majority => Self::Majority,
            ReadConcernLevelWrapper::Linearizable => Self::Linearizable,
            ReadConcernLevelWrapper::Available => Self::Available,
            ReadConcernLevelWrapper::Snapshot => Self::Snapshot,
            ReadConcernLevelWrapper::Custom(n) => Self::Custom(n),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum SelectionCriteriaWrapper {
    ReadPreference(ReadPreferenceWrapper),
    Predicate(bool),
}

impl From<SelectionCriteriaWrapper> for SelectionCriteria {
    fn from(wrapper: SelectionCriteriaWrapper) -> Self {
        match wrapper {
            SelectionCriteriaWrapper::ReadPreference(wrapper) => SelectionCriteria::ReadPreference(wrapper.into()),
            SelectionCriteriaWrapper::Predicate(_) => {
                SelectionCriteria::ReadPreference(ReadPreference::Primary) //TODO handle predicate
            }
        }
    }
}

// pub type PredicateWrapper = Arc<dyn Send + Sync + Fn(&ServerInfo) -> bool>;

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum ReadPreferenceWrapper {
    Primary,
    Secondary { options: ReadPreferenceOptionsWrapper },
    PrimaryPreferred { options: ReadPreferenceOptionsWrapper },
    SecondaryPreferred { options: ReadPreferenceOptionsWrapper },
    Nearest { options: ReadPreferenceOptionsWrapper },
}

impl From<ReadPreferenceWrapper> for ReadPreference {
    fn from(wrapper: ReadPreferenceWrapper) -> Self {
        match wrapper {
            ReadPreferenceWrapper::Primary => Self::Primary,
            ReadPreferenceWrapper::Secondary { options } => Self::Secondary { options: options.into() },
            ReadPreferenceWrapper::PrimaryPreferred { options } => Self::PrimaryPreferred { options: options.into() },
            ReadPreferenceWrapper::SecondaryPreferred { options } => Self::SecondaryPreferred { options: options.into() },
            ReadPreferenceWrapper::Nearest { options } => Self::Nearest { options: options.into() },
        }
    }
}

pub type TagSetWrapper = HashMap<String, String>;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, ToSchema, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReadPreferenceOptionsWrapper {
    pub tag_sets: Option<Vec<TagSetWrapper>>,
    #[serde(rename = "maxStalenessSeconds", default)]
    pub max_staleness: Option<DurationWrapper>,
    pub hedge: Option<HedgedReadOptionsWrapper>,
}

impl From<ReadPreferenceOptionsWrapper> for ReadPreferenceOptions {
    fn from(wrapper: ReadPreferenceOptionsWrapper) -> Self {
        Self::builder()
            .tag_sets(wrapper.tag_sets)
            .max_staleness(wrapper.max_staleness.map(Into::into))
            .hedge(wrapper.hedge.map(Into::into))
            .build()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema, JsonSchema)]
pub struct HedgedReadOptionsWrapper {
    pub enabled: bool,
}

impl From<HedgedReadOptionsWrapper> for HedgedReadOptions {
    fn from(wrapper: HedgedReadOptionsWrapper) -> Self {
        Self::builder().enabled(wrapper.enabled).build()
    }
}

pub(crate) mod duration_option_as_int_seconds {
    use super::*;

    #[allow(dead_code)]
    pub(crate) fn serialize<S: Serializer>(val: &Option<DurationWrapper>, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match val {
            Some(duration) if duration.as_secs() > i32::MAX as u64 => {
                serializer.serialize_i64(duration.as_secs().try_into().map_err(serde::ser::Error::custom)?)
            }
            #[allow(clippy::cast_possible_truncation)]
            Some(duration) => serializer.serialize_i32(duration.as_secs() as i32),
            None => serializer.serialize_none(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Option<DurationWrapper>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = Option::<u64>::deserialize(deserializer)?;
        Ok(millis.map(DurationWrapper::from_secs))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct DropDatabaseOptionsWrapper {
    pub write_concern: Option<WriteConcernWrapper>,
}

impl From<DropDatabaseOptionsWrapper> for DropDatabaseOptions {
    fn from(wrapper: DropDatabaseOptionsWrapper) -> Self {
        DropDatabaseOptions::builder().write_concern(wrapper.write_concern.to_owned().map(Into::into)).build()
    }
}

impl DropDatabaseOptionsWrapper {
    pub fn as_drop_database_options(&self) -> DropDatabaseOptions {
        DropDatabaseOptions::builder().write_concern(self.write_concern.to_owned().map(Into::into)).build()
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct GridFsBucketOptionsWrapper {
    pub bucket_name: Option<String>,
    pub chunk_size_bytes: Option<u32>,
    pub write_concern: Option<WriteConcernWrapper>,
    pub read_concern: Option<ReadConcernWrapper>,
    pub selection_criteria: Option<SelectionCriteriaWrapper>,
}

impl From<GridFsBucketOptionsWrapper> for GridFsBucketOptions {
    fn from(wrapper: GridFsBucketOptionsWrapper) -> Self {
        GridFsBucketOptions::builder()
            .bucket_name(wrapper.bucket_name)
            .chunk_size_bytes(wrapper.chunk_size_bytes)
            .write_concern(wrapper.write_concern.map(Into::into))
            .read_concern(wrapper.read_concern.map(Into::into))
            .selection_criteria(wrapper.selection_criteria.map(Into::into))
            .build()
    }
}

impl GridFsBucketOptionsWrapper {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub(crate) fn with_bucket_name(mut self, name: String) -> Self {
        self.bucket_name = Some(name);
        self
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct RunCursorCommandOptionsWrapper {
    pub selection_criteria: Option<SelectionCriteriaWrapper>,
    pub cursor_type: Option<CursorTypeWrapper>,
    pub batch_size: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_duration_option_from_u64_millis")]
    pub max_time: Option<DurationWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<RunCursorCommandOptionsWrapper> for RunCursorCommandOptions {
    fn from(wrapper: RunCursorCommandOptionsWrapper) -> Self {
        RunCursorCommandOptions::builder()
            .selection_criteria(wrapper.selection_criteria.map(Into::into))
            .cursor_type(wrapper.cursor_type.map(Into::into))
            .batch_size(wrapper.batch_size)
            .max_time(wrapper.max_time.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum CursorTypeWrapper {
    NonTailable,
    Tailable,
    TailableAwait,
}

impl From<CursorTypeWrapper> for CursorType {
    fn from(val: CursorTypeWrapper) -> CursorType {
        match val {
            CursorTypeWrapper::NonTailable => CursorType::NonTailable,
            CursorTypeWrapper::TailableAwait => CursorType::TailableAwait,
            CursorTypeWrapper::Tailable => CursorType::Tailable,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CountOptionsWrapper {
    pub hint: Option<HintWrapper>,
    pub limit: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_duration_option_from_u64_millis")]
    pub max_time: Option<DurationWrapper>,
    pub skip: Option<u64>,
    pub collation: Option<CollationWrapper>,
    pub selection_criteria: Option<SelectionCriteriaWrapper>,
    pub read_concern: Option<ReadConcernWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<CountOptionsWrapper> for CountOptions {
    fn from(wrapper: CountOptionsWrapper) -> Self {
        CountOptions::builder()
            .hint(wrapper.hint.map(Into::into))
            .limit(wrapper.limit)
            .max_time(wrapper.max_time.map(Into::into))
            .skip(wrapper.skip)
            .collation(wrapper.collation.map(Into::into))
            .selection_criteria(wrapper.selection_criteria.map(Into::into))
            .read_concern(wrapper.read_concern.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

fn deserialize_duration_option_from_u64_millis<'de, D>(deserializer: D) -> Result<Option<DurationWrapper>, D::Error>
where
    D: Deserializer<'de>,
{
    let millis: Option<u64> = serde::Deserialize::deserialize(deserializer).map(Some)?;
    Ok(millis.map(DurationWrapper::from_millis))
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CreateIndexOptionsWrapper {
    pub commit_quorum: Option<CommitQuorumWrapper>,
    #[serde(
        rename = "maxTimeMS",
        default,
        serialize_with = "serialize_duration_option_as_int_millis",
        deserialize_with = "deserialize_duration_option_from_u64_millis"
    )]
    pub max_time: Option<DurationWrapper>,
    pub write_concern: Option<WriteConcernWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<CreateIndexOptionsWrapper> for CreateIndexOptions {
    fn from(wrapper: CreateIndexOptionsWrapper) -> Self {
        CreateIndexOptions::builder()
            .commit_quorum(wrapper.commit_quorum.map(Into::into))
            .max_time(wrapper.max_time.map(Into::into))
            .write_concern(wrapper.write_concern.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

fn serialize_duration_option_as_int_millis<S: Serializer>(val: &Option<DurationWrapper>, serializer: S) -> Result<S::Ok, S::Error> {
    match val {
        Some(duration) if duration.as_millis() > i32::MAX as u128 => {
            serializer.serialize_i64(duration.as_millis().try_into().map_err(serde::ser::Error::custom)?)
        }
        #[allow(clippy::cast_possible_truncation)]
        Some(duration) => serializer.serialize_i32(duration.as_millis() as i32),
        None => serializer.serialize_none(),
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum CommitQuorumWrapper {
    Nodes(u32),
    VotingMembers,
    Majority,
    Custom(String),
}

impl From<CommitQuorumWrapper> for CommitQuorum {
    fn from(val: CommitQuorumWrapper) -> CommitQuorum {
        match val {
            CommitQuorumWrapper::Nodes(n) => CommitQuorum::Nodes(n),
            CommitQuorumWrapper::VotingMembers => CommitQuorum::VotingMembers,
            CommitQuorumWrapper::Majority => CommitQuorum::Majority,
            CommitQuorumWrapper::Custom(s) => CommitQuorum::Custom(s),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CreateSearchIndexOptionsWrapper {}

impl From<CreateSearchIndexOptionsWrapper> for CreateSearchIndexOptions {
    fn from(_: CreateSearchIndexOptionsWrapper) -> CreateSearchIndexOptions {
        CreateSearchIndexOptions::builder().build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct DropSearchIndexOptionsWrapper {}

impl From<DropSearchIndexOptionsWrapper> for DropSearchIndexOptions {
    fn from(_: DropSearchIndexOptionsWrapper) -> DropSearchIndexOptions {
        DropSearchIndexOptions::builder().build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FindOneOptionsWrapper {
    pub allow_partial_results: Option<bool>,
    pub collation: Option<CollationWrapper>,
    pub comment: Option<String>,
    pub comment_bson: Option<BsonWrapper>,
    pub hint: Option<HintWrapper>,
    pub max: Option<DocumentWrapper>,
    pub max_scan: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_duration_option_from_u64_millis")]
    pub max_time: Option<DurationWrapper>,
    pub min: Option<DocumentWrapper>,
    pub projection: Option<DocumentWrapper>,
    pub read_concern: Option<ReadConcernWrapper>,
    pub return_key: Option<bool>,
    pub selection_criteria: Option<SelectionCriteriaWrapper>,
    pub show_record_id: Option<bool>,
    pub skip: Option<u64>,
    pub sort: Option<DocumentWrapper>,
    #[serde(rename = "let")]
    pub let_vars: Option<DocumentWrapper>,
}

impl From<FindOneOptionsWrapper> for FindOneOptions {
    fn from(val: FindOneOptionsWrapper) -> FindOneOptions {
        FindOneOptions::builder()
            .allow_partial_results(val.allow_partial_results)
            .collation(val.collation.map(Into::into))
            .comment(val.comment)
            .comment_bson(val.comment_bson.map(Into::into))
            .hint(val.hint.map(Into::into))
            .max(val.max.map(Into::into))
            .max_time(val.max_time.map(Into::into))
            .max_scan(val.max_scan)
            .min(val.min.map(Into::into))
            .projection(val.projection.map(Into::into))
            .read_concern(val.read_concern.map(Into::into))
            .return_key(val.return_key)
            .selection_criteria(val.selection_criteria.map(Into::into))
            .show_record_id(val.show_record_id)
            .skip(val.skip)
            .sort(val.sort.map(Into::into))
            .let_vars(val.let_vars.map(Into::into))
            .build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FindOneAndDeleteOptionsWrapper {
    pub max_time: Option<DurationWrapper>,
    pub projection: Option<DocumentWrapper>,
    pub sort: Option<DocumentWrapper>,
    pub write_concern: Option<WriteConcernWrapper>,
    pub collation: Option<CollationWrapper>,
    pub hint: Option<HintWrapper>,
    #[serde(rename = "let")]
    pub let_vars: Option<DocumentWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<FindOneAndDeleteOptionsWrapper> for FindOneAndDeleteOptions {
    fn from(wrapper: FindOneAndDeleteOptionsWrapper) -> Self {
        Self::builder()
            .max_time(wrapper.max_time.map(Into::into))
            .projection(wrapper.projection.map(Into::into))
            .sort(wrapper.sort.map(Into::into))
            .write_concern(wrapper.write_concern.map(Into::into))
            .collation(wrapper.collation.map(Into::into))
            .hint(wrapper.hint.map(Into::into))
            .let_vars(wrapper.let_vars.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FindOneAndReplaceOptionsWrapper {
    pub bypass_document_validation: Option<bool>,
    pub max_time: Option<DurationWrapper>,
    pub projection: Option<DocumentWrapper>,
    pub return_document: Option<ReturnDocumentWrapper>,
    pub sort: Option<DocumentWrapper>,
    pub upsert: Option<bool>,
    pub write_concern: Option<WriteConcernWrapper>,
    pub collation: Option<CollationWrapper>,
    pub hint: Option<HintWrapper>,
    #[serde(rename = "let")]
    pub let_vars: Option<DocumentWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<FindOneAndReplaceOptionsWrapper> for FindOneAndReplaceOptions {
    fn from(val: FindOneAndReplaceOptionsWrapper) -> FindOneAndReplaceOptions {
        FindOneAndReplaceOptions::builder()
            .bypass_document_validation(val.bypass_document_validation)
            .max_time(val.max_time.map(Into::into))
            .projection(val.projection.map(Into::into))
            .return_document(val.return_document.map(Into::into))
            .sort(val.sort.map(Into::into))
            .upsert(val.upsert)
            .write_concern(val.write_concern.map(Into::into))
            .collation(val.collation.map(Into::into))
            .hint(val.hint.map(Into::into))
            .let_vars(val.let_vars.map(Into::into))
            .comment(val.comment.map(Into::into))
            .build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum ReturnDocumentWrapper {
    After,
    Before,
}

impl From<ReturnDocumentWrapper> for ReturnDocument {
    fn from(wrapper: ReturnDocumentWrapper) -> Self {
        match wrapper {
            ReturnDocumentWrapper::After => Self::After,
            ReturnDocumentWrapper::Before => Self::Before,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FindOneAndUpdateOptionsWrapper {
    pub array_filters: Option<Vec<DocumentWrapper>>,
    pub bypass_document_validation: Option<bool>,
    pub max_time: Option<DurationWrapper>,
    pub projection: Option<DocumentWrapper>,
    pub return_document: Option<ReturnDocumentWrapper>,
    pub sort: Option<DocumentWrapper>,
    pub upsert: Option<bool>,
    pub write_concern: Option<WriteConcernWrapper>,
    pub collation: Option<CollationWrapper>,
    pub hint: Option<HintWrapper>,
    #[serde(rename = "let")]
    pub let_vars: Option<DocumentWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<FindOneAndUpdateOptionsWrapper> for FindOneAndUpdateOptions {
    fn from(wrapper: FindOneAndUpdateOptionsWrapper) -> Self {
        Self::builder()
            .array_filters(wrapper.array_filters.map(|d| d.into_iter().map(Into::into).collect()))
            .bypass_document_validation(wrapper.bypass_document_validation)
            .max_time(wrapper.max_time.map(Into::into))
            .projection(wrapper.projection.map(Into::into))
            .return_document(wrapper.return_document.map(Into::into))
            .sort(wrapper.sort.map(Into::into))
            .upsert(wrapper.upsert)
            .write_concern(wrapper.write_concern.map(Into::into))
            .collation(wrapper.collation.map(Into::into))
            .hint(wrapper.hint.map(Into::into))
            .let_vars(wrapper.let_vars.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct InsertOneOptionsWrapper {
    pub bypass_document_validation: Option<bool>,
    pub write_concern: Option<WriteConcernWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<InsertOneOptionsWrapper> for InsertOneOptions {
    fn from(wrapper: InsertOneOptionsWrapper) -> Self {
        Self::builder()
            .bypass_document_validation(wrapper.bypass_document_validation)
            .write_concern(wrapper.write_concern.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema, JsonSchema)]
pub struct DatabaseOptionsWrapper {
    pub selection_criteria: Option<SelectionCriteriaWrapper>,
    pub read_concern: Option<ReadConcernWrapper>,
    pub write_concern: Option<WriteConcernWrapper>,
}

impl From<DatabaseOptionsWrapper> for DatabaseOptions {
    fn from(wrapper: DatabaseOptionsWrapper) -> Self {
        Self::builder()
            .selection_criteria(wrapper.selection_criteria.map(Into::into))
            .read_concern(wrapper.read_concern.map(Into::into))
            .write_concern(wrapper.write_concern.map(Into::into))
            .build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema, JsonSchema)]
pub struct CollectionOptionsWrapper {
    pub selection_criteria: Option<SelectionCriteriaWrapper>,
    pub read_concern: Option<ReadConcernWrapper>,
    pub write_concern: Option<WriteConcernWrapper>,
    pub human_readable_serialization: Option<bool>,
}

impl From<CollectionOptionsWrapper> for CollectionOptions {
    fn from(wrapper: CollectionOptionsWrapper) -> Self {
        CollectionOptions::builder()
            .selection_criteria(wrapper.selection_criteria.map(Into::into))
            .read_concern(wrapper.read_concern.map(Into::into))
            .write_concern(wrapper.write_concern.map(Into::into))
            .human_readable_serialization(wrapper.human_readable_serialization)
            .build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema, JsonSchema)]
pub struct GridFsFindOptionsWrapper {
    pub allow_disk_use: Option<bool>,
    pub batch_size: Option<u32>,
    pub limit: Option<i64>,
    pub max_time: Option<DurationWrapper>,
    pub skip: Option<u64>,
    pub sort: Option<DocumentWrapper>,
}

impl GridFsFindOptionsWrapper {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub fn with_sort(mut self, sort: DocumentWrapper) -> Self {
        self.sort = Some(sort);
        self
    }

    pub fn with_limit(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl From<GridFsFindOptionsWrapper> for GridFsFindOptions {
    fn from(wrapper: GridFsFindOptionsWrapper) -> Self {
        GridFsFindOptions::builder()
            .allow_disk_use(wrapper.allow_disk_use)
            .batch_size(wrapper.batch_size)
            .limit(wrapper.limit)
            .max_time(wrapper.max_time.map(Into::into))
            .skip(wrapper.skip)
            .sort(wrapper.sort.map(Into::into))
            .build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema, JsonSchema)]
pub struct ListSearchIndexOptionsWrapper {}

impl From<ListSearchIndexOptionsWrapper> for ListSearchIndexOptions {
    fn from(_: ListSearchIndexOptionsWrapper) -> Self {
        ListSearchIndexOptions::builder().build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema, JsonSchema)]
pub struct ReplaceOptionsWrapper {
    pub bypass_document_validation: Option<bool>,
    pub upsert: Option<bool>,
    pub collation: Option<CollationWrapper>,
    pub hint: Option<HintWrapper>,
    pub write_concern: Option<WriteConcernWrapper>,
    #[serde(rename = "let")]
    pub let_vars: Option<DocumentWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<ReplaceOptionsWrapper> for ReplaceOptions {
    fn from(wrapper: ReplaceOptionsWrapper) -> Self {
        ReplaceOptions::builder()
            .bypass_document_validation(wrapper.bypass_document_validation)
            .upsert(wrapper.upsert)
            .collation(wrapper.collation.map(Into::into))
            .hint(wrapper.hint.map(Into::into))
            .write_concern(wrapper.write_concern.map(Into::into))
            .let_vars(wrapper.let_vars.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema, JsonSchema)]
pub struct UpdateOptionsWrapper {
    pub array_filters: Option<Vec<DocumentWrapper>>,
    pub bypass_document_validation: Option<bool>,
    pub upsert: Option<bool>,
    pub collation: Option<CollationWrapper>,
    pub hint: Option<HintWrapper>,
    pub write_concern: Option<WriteConcernWrapper>,
    #[serde(rename = "let")]
    pub let_vars: Option<DocumentWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<UpdateOptionsWrapper> for UpdateOptions {
    fn from(wrapper: UpdateOptionsWrapper) -> Self {
        UpdateOptions::builder()
            .array_filters(wrapper.array_filters.map(|d| d.into_iter().map(Into::into).collect()))
            .bypass_document_validation(wrapper.bypass_document_validation)
            .upsert(wrapper.upsert)
            .collation(wrapper.collation.map(Into::into))
            .hint(wrapper.hint.map(Into::into))
            .write_concern(wrapper.write_concern.map(Into::into))
            .let_vars(wrapper.let_vars.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema, JsonSchema)]
pub struct UpdateSearchIndexOptionsWrapper {}

impl From<UpdateSearchIndexOptionsWrapper> for UpdateSearchIndexOptions {
    fn from(_: UpdateSearchIndexOptionsWrapper) -> Self {
        UpdateSearchIndexOptions::builder().build()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum UpdateModificationsWrapper {
    Document(DocumentWrapper),
    Pipeline(Vec<DocumentWrapper>),
}

impl Default for UpdateModificationsWrapper {
    fn default() -> Self {
        UpdateModificationsWrapper::Document(DocumentWrapper::default())
    }
}

impl From<UpdateModificationsWrapper> for UpdateModifications {
    fn from(wrapper: UpdateModificationsWrapper) -> Self {
        match wrapper {
            UpdateModificationsWrapper::Document(document) => Self::Document(document.into()),
            UpdateModificationsWrapper::Pipeline(pipeline) => Self::Pipeline(pipeline.into_iter().map(Into::into).collect()),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IndexModelWrapper {
    #[serde(rename = "key")]
    pub keys: DocumentWrapper,
    #[serde(flatten)]
    pub options: Option<IndexOptionsWrapper>,
}

impl From<IndexModelWrapper> for IndexModel {
    fn from(wrapper: IndexModelWrapper) -> Self {
        let doc: Document = wrapper.keys.into();
        Self::builder().keys(doc).options(wrapper.options.map(Into::into)).build()
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IndexOptionsWrapper {
    pub background: Option<bool>,
    #[serde(rename = "expireAfterSeconds", default)]
    pub expire_after: Option<DurationWrapper>,
    pub name: Option<String>,
    pub sparse: Option<bool>,
    pub storage_engine: Option<DocumentWrapper>,
    pub unique: Option<bool>,
    #[serde(rename = "v")]
    pub version: Option<IndexVersionWrapper>,
    #[serde(rename = "default_language")]
    pub default_language: Option<String>,
    #[serde(rename = "language_override")]
    pub language_override: Option<String>,
    pub text_index_version: Option<TextIndexVersionWrapper>,
    pub weights: Option<DocumentWrapper>,
    #[serde(rename = "2dsphereIndexVersion")]
    pub sphere_2d_index_version: Option<Sphere2DIndexVersionWrapper>,
    #[serde(serialize_with = "serialize_u32_option_as_i32")]
    pub bits: Option<u32>,
    pub max: Option<f64>,
    pub min: Option<f64>,
    #[serde(serialize_with = "serialize_u32_option_as_i32")]
    pub bucket_size: Option<u32>,
    pub partial_filter_expression: Option<DocumentWrapper>,
    pub collation: Option<CollationWrapper>,
    pub wildcard_projection: Option<DocumentWrapper>,
    pub hidden: Option<bool>,
    pub clustered: Option<bool>,
}

impl From<IndexOptionsWrapper> for IndexOptions {
    fn from(wrapper: IndexOptionsWrapper) -> Self {
        Self::builder()
            .background(wrapper.background)
            .expire_after(wrapper.expire_after.map(Into::into))
            .name(wrapper.name)
            .sparse(wrapper.sparse)
            .storage_engine(wrapper.storage_engine.map(Into::into))
            .unique(wrapper.unique)
            .version(wrapper.version.map(Into::into))
            .default_language(wrapper.default_language)
            .language_override(wrapper.language_override)
            .text_index_version(wrapper.text_index_version.map(Into::into))
            .weights(wrapper.weights.map(Into::into))
            .sphere_2d_index_version(wrapper.sphere_2d_index_version.map(Into::into))
            .bits(wrapper.bits)
            .max(wrapper.max)
            .min(wrapper.min)
            .bucket_size(wrapper.bucket_size)
            .partial_filter_expression(wrapper.partial_filter_expression.map(Into::into))
            .collation(wrapper.collation.map(Into::into))
            .wildcard_projection(wrapper.wildcard_projection.map(Into::into))
            .hidden(wrapper.hidden)
            .build()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, JsonSchema)]
#[non_exhaustive]
pub enum IndexVersionWrapper {
    V1,
    V2,
    Custom(u32),
}

impl From<IndexVersionWrapper> for IndexVersion {
    fn from(wrapper: IndexVersionWrapper) -> Self {
        match wrapper {
            IndexVersionWrapper::V1 => Self::V1,
            IndexVersionWrapper::V2 => Self::V2,
            IndexVersionWrapper::Custom(version) => Self::Custom(version),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, JsonSchema)]
pub enum TextIndexVersionWrapper {
    V1,
    V2,
    V3,
    Custom(u32),
}

impl From<TextIndexVersionWrapper> for TextIndexVersion {
    fn from(wrapper: TextIndexVersionWrapper) -> Self {
        match wrapper {
            TextIndexVersionWrapper::V1 => Self::V1,
            TextIndexVersionWrapper::V2 => Self::V2,
            TextIndexVersionWrapper::V3 => Self::V3,
            TextIndexVersionWrapper::Custom(version) => Self::Custom(version),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, JsonSchema)]
pub enum Sphere2DIndexVersionWrapper {
    V2,
    V3,
    Custom(u32),
}

impl From<Sphere2DIndexVersionWrapper> for Sphere2DIndexVersion {
    fn from(wrapper: Sphere2DIndexVersionWrapper) -> Self {
        match wrapper {
            Sphere2DIndexVersionWrapper::V2 => Self::V2,
            Sphere2DIndexVersionWrapper::V3 => Self::V3,
            Sphere2DIndexVersionWrapper::Custom(version) => Self::Custom(version),
        }
    }
}

pub(crate) fn serialize_u32_option_as_i32<S: Serializer>(val: &Option<u32>, serializer: S) -> std::result::Result<S::Ok, S::Error> {
    match val {
        Some(val) => bson::serde_helpers::serialize_u32_as_i32(val, serializer),
        None => serializer.serialize_none(),
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct SearchIndexModelWrapper {
    pub definition: DocumentWrapper,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl From<SearchIndexModelWrapper> for SearchIndexModel {
    fn from(wrapper: SearchIndexModelWrapper) -> Self {
        let doc: Document = wrapper.definition.into();
        Self::builder().definition(doc).name(wrapper.name).build()
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChangeStreamOptionsWrapper {
    pub full_document: Option<FullDocumentTypeWrapper>,
    pub full_document_before_change: Option<FullDocumentBeforeChangeTypeWrapper>,
    pub resume_after: Option<ResumeTokenWrapper>,
    pub start_at_operation_time: Option<TimestampWrapper>,
    pub start_after: Option<ResumeTokenWrapper>,
    pub(crate) all_changes_for_cluster: Option<bool>,
    #[serde(skip_serializing)]
    pub max_await_time: Option<DurationWrapper>,
    #[serde(skip_serializing)]
    pub batch_size: Option<u32>,
    #[serde(skip_serializing)]
    pub collation: Option<CollationWrapper>,
    #[serde(skip_serializing)]
    pub read_concern: Option<ReadConcernWrapper>,
    #[serde(skip_serializing)]
    pub selection_criteria: Option<SelectionCriteriaWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<ChangeStreamOptionsWrapper> for ChangeStreamOptions {
    fn from(wrapper: ChangeStreamOptionsWrapper) -> Self {
        Self::builder()
            .full_document(wrapper.full_document.map(Into::into))
            .full_document_before_change(wrapper.full_document_before_change.map(Into::into))
            .resume_after(wrapper.resume_after.map(Into::into))
            .start_at_operation_time(wrapper.start_at_operation_time.map(Into::into))
            .start_after(wrapper.start_after.map(Into::into))
            .max_await_time(wrapper.max_await_time.map(Into::into))
            .batch_size(wrapper.batch_size)
            .collation(wrapper.collation.map(Into::into))
            .read_concern(wrapper.read_concern.map(Into::into))
            .selection_criteria(wrapper.selection_criteria.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum FullDocumentTypeWrapper {
    UpdateLookup,
    WhenAvailable,
    Required,
    Other(String),
}

impl From<FullDocumentTypeWrapper> for FullDocumentType {
    fn from(wrapper: FullDocumentTypeWrapper) -> Self {
        match wrapper {
            FullDocumentTypeWrapper::UpdateLookup => Self::UpdateLookup,
            FullDocumentTypeWrapper::WhenAvailable => Self::WhenAvailable,
            FullDocumentTypeWrapper::Required => Self::Required,
            FullDocumentTypeWrapper::Other(other) => Self::Other(other),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum FullDocumentBeforeChangeTypeWrapper {
    WhenAvailable,
    Required,
    Off,
    Other(String),
}

impl From<FullDocumentBeforeChangeTypeWrapper> for FullDocumentBeforeChangeType {
    fn from(wrapper: FullDocumentBeforeChangeTypeWrapper) -> Self {
        match wrapper {
            FullDocumentBeforeChangeTypeWrapper::WhenAvailable => Self::WhenAvailable,
            FullDocumentBeforeChangeTypeWrapper::Required => Self::Required,
            FullDocumentBeforeChangeTypeWrapper::Off => Self::Off,
            FullDocumentBeforeChangeTypeWrapper::Other(other) => Self::Other(other),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, ToSchema, JsonSchema)]
pub struct ResumeTokenWrapper(pub(crate) RawBsonWrapper);

impl From<ResumeTokenWrapper> for ResumeToken {
    fn from(_wrapper: ResumeTokenWrapper) -> Self {
        todo!("")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema, JsonSchema)]
pub enum RawBsonWrapper {
    Double(f64),
    String(String),
    Array(RawArrayBufWrapper),
    Document(RawDocumentBufWrapper),
    Boolean(bool),
    Null,
    RegularExpression(RegexWrapper),
    JavaScriptCode(String),
    JavaScriptCodeWithScope(RawJavaScriptCodeWithScopeWrapper),
    Int32(i32),
    Int64(i64),
    Timestamp(TimestampWrapper),
    Binary(BinaryWrapper),
    ObjectId(ObjectIdWrapper),
    DateTime(DateTimeWrapper),
    Symbol(String),
    Decimal128(Decimal128Wrapper),
    Undefined,
    MaxKey,
    MinKey,
    DbPointer(DbPointerWrapper),
}

impl From<RawBsonWrapper> for RawBson {
    fn from(wrapper: RawBsonWrapper) -> Self {
        match wrapper {
            RawBsonWrapper::Double(d) => RawBson::Double(d),
            RawBsonWrapper::String(s) => RawBson::String(s),
            RawBsonWrapper::Array(a) => RawBson::Array(a.into()),
            RawBsonWrapper::Document(d) => RawBson::Document(d.into()),
            RawBsonWrapper::Boolean(b) => RawBson::Boolean(b),
            RawBsonWrapper::Null => RawBson::Null,
            RawBsonWrapper::RegularExpression(r) => RawBson::RegularExpression(r.into()),
            RawBsonWrapper::JavaScriptCode(s) => RawBson::JavaScriptCode(s),
            RawBsonWrapper::JavaScriptCodeWithScope(s) => RawBson::JavaScriptCodeWithScope(s.into()),
            RawBsonWrapper::Int32(i) => RawBson::Int32(i),
            RawBsonWrapper::Int64(i) => RawBson::Int64(i),
            RawBsonWrapper::Timestamp(t) => RawBson::Timestamp(t.into()),
            RawBsonWrapper::Binary(b) => RawBson::Binary(b.into()),
            RawBsonWrapper::ObjectId(o) => RawBson::ObjectId(o.into()),
            RawBsonWrapper::DateTime(d) => RawBson::DateTime(d.into()),
            RawBsonWrapper::Symbol(s) => RawBson::Symbol(s),
            RawBsonWrapper::Decimal128(d) => RawBson::Decimal128(d.into()),
            RawBsonWrapper::Undefined => RawBson::Undefined,
            RawBsonWrapper::MaxKey => RawBson::MaxKey,
            RawBsonWrapper::MinKey => RawBson::MinKey,
            RawBsonWrapper::DbPointer(d) => RawBson::DbPointer(d.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct RawArrayBufWrapper {
    inner: RawDocumentBufWrapper,
    len: usize,
}

impl From<RawArrayBufWrapper> for RawArrayBuf {
    fn from(_wrapper: RawArrayBufWrapper) -> Self {
        todo!("")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct RawDocumentBufWrapper {
    data: Vec<u8>,
}

impl From<RawDocumentBufWrapper> for RawDocumentBuf {
    fn from(wrapper: RawDocumentBufWrapper) -> Self {
        Self::from_bytes(wrapper.data).unwrap_or_default()
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct DbPointerWrapper {
    pub(crate) namespace: String,
    pub(crate) id: ObjectIdWrapper,
}

impl From<DbPointerWrapper> for DbPointer {
    fn from(_wrapper: DbPointerWrapper) -> Self {
        todo!("")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct RawJavaScriptCodeWithScopeWrapper {
    pub code: String,
    pub scope: RawDocumentBufWrapper,
}

impl From<RawJavaScriptCodeWithScopeWrapper> for RawJavaScriptCodeWithScope {
    fn from(_wrapper: RawJavaScriptCodeWithScopeWrapper) -> Self {
        todo!("")
    }
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListDatabasesOptionsWrapper {
    pub authorized_databases: Option<bool>,
    pub comment: Option<BsonWrapper>,
}

impl From<ListDatabasesOptionsWrapper> for ListDatabasesOptions {
    fn from(wrapper: ListDatabasesOptionsWrapper) -> Self {
        Self::builder().authorized_databases(wrapper.authorized_databases).comment(wrapper.comment.map(Into::into)).build()
    }
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListCollectionsOptionsWrapper {
    #[serde(serialize_with = "serialize_u32_option_as_batch_size", rename(serialize = "cursor"))]
    pub batch_size: Option<u32>,
    pub comment: Option<BsonWrapper>,
}

impl From<ListCollectionsOptionsWrapper> for ListCollectionsOptions {
    fn from(wrapper: ListCollectionsOptionsWrapper) -> Self {
        Self::builder().batch_size(wrapper.batch_size).comment(wrapper.comment.map(Into::into)).build()
    }
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateCollectionOptionsWrapper {
    pub capped: Option<bool>,
    #[serde(serialize_with = "serialize_u64_option_as_i64")]
    pub size: Option<u64>,
    #[serde(serialize_with = "serialize_u64_option_as_i64")]
    pub max: Option<u64>,
    pub storage_engine: Option<DocumentWrapper>,
    pub validator: Option<DocumentWrapper>,
    pub validation_level: Option<ValidationLevelWrapper>,
    pub validation_action: Option<ValidationActionWrapper>,
    pub view_on: Option<String>,
    pub pipeline: Option<Vec<DocumentWrapper>>,
    pub collation: Option<CollationWrapper>,
    pub write_concern: Option<WriteConcernWrapper>,
    pub index_option_defaults: Option<IndexOptionDefaultsWrapper>,
    pub timeseries: Option<TimeseriesOptionsWrapper>,
    pub expire_after_seconds: Option<DurationWrapper>,
    pub change_stream_pre_and_post_images: Option<ChangeStreamPreAndPostImagesWrapper>,
    pub clustered_index: Option<ClusteredIndexWrapper>,
    pub comment: Option<BsonWrapper>,
    // pub encrypted_fields: Option<DocumentWrapper>,
}

impl From<CreateCollectionOptionsWrapper> for CreateCollectionOptions {
    fn from(wrapper: CreateCollectionOptionsWrapper) -> Self {
        let mut collections_options = Self::default();

        collections_options.capped = wrapper.capped;
        collections_options.size = wrapper.size;
        collections_options.max = wrapper.max;
        collections_options.storage_engine = wrapper.storage_engine.map(Into::into);
        collections_options.validator = wrapper.validator.map(Into::into);
        collections_options.validation_level = wrapper.validation_level.map(Into::into);
        collections_options.validation_action = wrapper.validation_action.map(Into::into);
        collections_options.view_on = wrapper.view_on;
        collections_options.pipeline = wrapper.pipeline.map(|v| v.into_iter().map(Into::into).collect());
        collections_options.collation = wrapper.collation.map(Into::into);
        collections_options.write_concern = wrapper.write_concern.map(Into::into);
        collections_options.index_option_defaults = wrapper.index_option_defaults.map(Into::into);
        collections_options.timeseries = wrapper.timeseries.map(Into::into);
        collections_options.expire_after_seconds = wrapper.expire_after_seconds.map(Into::into);
        collections_options.change_stream_pre_and_post_images = wrapper.change_stream_pre_and_post_images.map(Into::into);
        collections_options.clustered_index = wrapper.clustered_index.map(Into::into);
        collections_options.comment = wrapper.comment.map(Into::into);

        collections_options
    }
}

pub(crate) fn serialize_u64_option_as_i64<S: Serializer>(val: &Option<u64>, serializer: S) -> std::result::Result<S::Ok, S::Error> {
    match val {
        Some(v) => bson::serde_helpers::serialize_u64_as_i64(v, serializer),
        None => serializer.serialize_none(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ValidationLevelWrapper {
    Off,
    Strict,
    Moderate,
}

impl From<ValidationLevelWrapper> for ValidationLevel {
    fn from(wrapper: ValidationLevelWrapper) -> Self {
        match wrapper {
            ValidationLevelWrapper::Off => ValidationLevel::Off,
            ValidationLevelWrapper::Strict => ValidationLevel::Strict,
            ValidationLevelWrapper::Moderate => ValidationLevel::Moderate,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ValidationActionWrapper {
    Error,
    Warn,
}

impl From<ValidationActionWrapper> for ValidationAction {
    fn from(wrapper: ValidationActionWrapper) -> Self {
        match wrapper {
            ValidationActionWrapper::Error => ValidationAction::Error,
            ValidationActionWrapper::Warn => ValidationAction::Warn,
        }
    }
}

#[derive(Clone, Debug, ToSchema, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IndexOptionDefaultsWrapper {
    pub storage_engine: DocumentWrapper,
}

impl From<IndexOptionDefaultsWrapper> for IndexOptionDefaults {
    fn from(wrapper: IndexOptionDefaultsWrapper) -> Self {
        Self::builder().storage_engine(wrapper.storage_engine.into()).build()
    }
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeleteOptionsWrapper {
    pub collation: Option<CollationWrapper>,
    pub write_concern: Option<WriteConcernWrapper>,
    pub hint: Option<HintWrapper>,
    #[serde(rename = "let")]
    pub let_vars: Option<DocumentWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<DeleteOptionsWrapper> for DeleteOptions {
    fn from(wrapper: DeleteOptionsWrapper) -> Self {
        Self::builder()
            .collation(wrapper.collation.map(Into::into))
            .write_concern(wrapper.write_concern.map(Into::into))
            .hint(wrapper.hint.map(Into::into))
            .let_vars(wrapper.let_vars.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

#[derive(Debug, Default, Deserialize, ToSchema, Serialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DistinctOptionsWrapper {
    #[serde(
        default,
        serialize_with = "serialize_duration_option_as_int_millis",
        rename = "maxTimeMS",
        deserialize_with = "deserialize_duration_option_from_u64_millis"
    )]
    pub max_time: Option<DurationWrapper>,
    #[serde(skip_serializing)]
    pub selection_criteria: Option<SelectionCriteriaWrapper>,
    #[serde(skip_serializing)]
    pub read_concern: Option<ReadConcernWrapper>,
    pub collation: Option<CollationWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<DistinctOptionsWrapper> for DistinctOptions {
    fn from(wrapper: DistinctOptionsWrapper) -> Self {
        Self::builder()
            .max_time(wrapper.max_time.map(Into::into))
            .selection_criteria(wrapper.selection_criteria.map(Into::into))
            .read_concern(wrapper.read_concern.map(Into::into))
            .collation(wrapper.collation.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TimeseriesOptionsWrapper {
    pub time_field: String,
    pub meta_field: Option<String>,
    pub granularity: Option<TimeseriesGranularityWrapper>,
    #[serde(default, rename = "bucketMaxSpanSeconds")]
    pub bucket_max_span: Option<DurationWrapper>,
    #[serde(default, rename = "bucketRoundingSeconds")]
    pub bucket_rounding: Option<DurationWrapper>,
}

impl From<TimeseriesOptionsWrapper> for TimeseriesOptions {
    fn from(wrapper: TimeseriesOptionsWrapper) -> Self {
        Self::builder()
            .time_field(wrapper.time_field)
            .meta_field(wrapper.meta_field)
            .granularity(wrapper.granularity.map(Into::into))
            .bucket_max_span(wrapper.bucket_max_span.map(Into::into))
            .bucket_rounding(wrapper.bucket_rounding.map(Into::into))
            .build()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum TimeseriesGranularityWrapper {
    Seconds,
    Minutes,
    Hours,
}

impl From<TimeseriesGranularityWrapper> for TimeseriesGranularity {
    fn from(wrapper: TimeseriesGranularityWrapper) -> Self {
        match wrapper {
            TimeseriesGranularityWrapper::Seconds => Self::Seconds,
            TimeseriesGranularityWrapper::Minutes => Self::Minutes,
            TimeseriesGranularityWrapper::Hours => Self::Hours,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ChangeStreamPreAndPostImagesWrapper {
    /// If `true`, change streams will be able to include pre- and post-images.
    ENABLED,
    #[default]
    DISABLED,
}

impl From<ChangeStreamPreAndPostImagesWrapper> for ChangeStreamPreAndPostImages {
    fn from(wrapper: ChangeStreamPreAndPostImagesWrapper) -> Self {
        match wrapper {
            ChangeStreamPreAndPostImagesWrapper::ENABLED => Self::builder().enabled(true).build(),
            ChangeStreamPreAndPostImagesWrapper::DISABLED => Self::builder().enabled(false).build(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusteredIndexWrapper {
    pub key: DocumentWrapper,
    pub unique: bool,
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub v: Option<i32>,
}

impl From<ClusteredIndexWrapper> for ClusteredIndex {
    fn from(_wrapper: ClusteredIndexWrapper) -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DropCollectionOptionsWrapper {
    pub write_concern: Option<WriteConcernWrapper>,
}

impl From<DropCollectionOptionsWrapper> for DropCollectionOptions {
    fn from(_wrapper: DropCollectionOptionsWrapper) -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListIndexesOptionsWrapper {
    #[serde(
        rename = "maxTimeMS",
        default,
        serialize_with = "serialize_duration_option_as_int_millis",
        deserialize_with = "deserialize_duration_option_from_u64_millis"
    )]
    pub max_time: Option<DurationWrapper>,
    #[serde(default, skip_serializing)]
    pub batch_size: Option<u32>,
    pub comment: Option<BsonWrapper>,
}

impl From<ListIndexesOptionsWrapper> for ListIndexesOptions {
    fn from(wrapper: ListIndexesOptionsWrapper) -> Self {
        Self::builder()
            .max_time(wrapper.max_time.map(Into::into))
            .batch_size(wrapper.batch_size)
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

#[derive(Clone, Debug, Default, ToSchema, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InsertManyOptionsWrapper {
    pub bypass_document_validation: Option<bool>,
    pub ordered: Option<bool>,
    #[serde(skip_deserializing)]
    pub write_concern: Option<WriteConcernWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<InsertManyOptionsWrapper> for InsertManyOptions {
    fn from(wrapper: InsertManyOptionsWrapper) -> Self {
        Self::builder()
            .bypass_document_validation(wrapper.bypass_document_validation)
            .ordered(wrapper.ordered)
            .write_concern(wrapper.write_concern.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

#[derive(Clone, Debug, Default, Builder, Deserialize, ToSchema, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[builder(setter(into, strip_option), default)]
pub struct FindOptionsWrapper {
    pub allow_disk_use: Option<bool>,
    pub allow_partial_results: Option<bool>,
    #[serde(serialize_with = "serialize_u32_option_as_i32")]
    pub batch_size: Option<u32>,
    #[serde(skip_serializing)]
    pub comment: Option<String>,
    #[serde(rename(serialize = "comment"))]
    pub comment_bson: Option<BsonWrapper>,
    #[serde(skip)]
    pub cursor_type: Option<CursorType>,
    pub hint: Option<HintWrapper>,
    pub limit: Option<i64>,
    pub max: Option<DocumentWrapper>,
    #[serde(skip)]
    pub max_await_time: Option<Duration>,
    #[serde(serialize_with = "serialize_u64_option_as_i64")]
    pub max_scan: Option<u64>,
    #[serde(rename = "maxTimeMS", serialize_with = "serialize_duration_option_as_int_millis")]
    pub max_time: Option<DurationWrapper>,
    pub min: Option<DocumentWrapper>,
    pub no_cursor_timeout: Option<bool>,
    pub projection: Option<DocumentWrapper>,
    #[serde(skip_serializing)]
    pub read_concern: Option<ReadConcernWrapper>,
    pub return_key: Option<bool>,
    #[serde(skip)]
    pub selection_criteria: Option<SelectionCriteria>,
    pub show_record_id: Option<bool>,
    #[serde(serialize_with = "serialize_u64_option_as_i64")]
    pub skip: Option<u64>,
    pub sort: Option<DocumentWrapper>,
    pub collation: Option<CollationWrapper>,
    #[serde(rename = "let")]
    pub let_vars: Option<DocumentWrapper>,
}

impl FindOptionsWrapper {
    /// Creates a new builder for FindOptionsWrapper
    pub fn builder() -> FindOptionsWrapperBuilder {
        FindOptionsWrapperBuilder::default()
    }

    /// Creates a new empty FindOptionsWrapper
    pub fn new() -> Self {
        Self::default()
    }

    /// Fluent builder methods for common operations
    pub fn with_limit(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_skip(mut self, skip: u64) -> Self {
        self.skip = Some(skip);
        self
    }

    pub fn with_sort(mut self, sort: DocumentWrapper) -> Self {
        self.sort = Some(sort);
        self
    }

    pub fn with_projection(mut self, projection: DocumentWrapper) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn with_allow_disk_use(mut self, allow: bool) -> Self {
        self.allow_disk_use = Some(allow);
        self
    }

    pub fn with_no_cursor_timeout(mut self, no_timeout: bool) -> Self {
        self.no_cursor_timeout = Some(no_timeout);
        self
    }

    pub fn with_hint(mut self, hint: HintWrapper) -> Self {
        self.hint = Some(hint);
        self
    }

    pub fn with_max_time(mut self, max_time: DurationWrapper) -> Self {
        self.max_time = Some(max_time);
        self
    }

    pub fn with_comment(mut self, comment: String) -> Self {
        self.comment = Some(comment);
        self
    }

    pub fn with_collation(mut self, collation: CollationWrapper) -> Self {
        self.collation = Some(collation);
        self
    }

    pub fn with_read_concern(mut self, read_concern: ReadConcernWrapper) -> Self {
        self.read_concern = Some(read_concern);
        self
    }
}

impl From<FindOptionsWrapper> for FindOptions {
    fn from(wrapper: FindOptionsWrapper) -> Self {
        Self::builder()
            .allow_disk_use(wrapper.allow_disk_use)
            .allow_partial_results(wrapper.allow_partial_results)
            .batch_size(wrapper.batch_size)
            .comment(wrapper.comment)
            .comment_bson(wrapper.comment_bson.map(Into::into))
            .cursor_type(wrapper.cursor_type)
            .hint(wrapper.hint.map(Into::into))
            .limit(wrapper.limit)
            .max(wrapper.max.map(Into::into))
            .max_await_time(wrapper.max_await_time)
            .max_scan(wrapper.max_scan)
            .max_time(wrapper.max_time.map(Into::into))
            .min(wrapper.min.map(Into::into))
            .no_cursor_timeout(wrapper.no_cursor_timeout)
            .projection(wrapper.projection.map(Into::into))
            .read_concern(wrapper.read_concern.map(Into::into))
            .return_key(wrapper.return_key)
            .selection_criteria(wrapper.selection_criteria)
            .show_record_id(wrapper.show_record_id)
            .skip(wrapper.skip)
            .sort(wrapper.sort.map(Into::into))
            .collation(wrapper.collation.map(Into::into))
            .let_vars(wrapper.let_vars.map(Into::into))
            .build()
    }
}

#[derive(Debug, Default, Deserialize, ToSchema, Serialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EstimatedDocumentCountOptionsWrapper {
    #[serde(
        default,
        serialize_with = "serialize_duration_option_as_int_millis",
        rename = "maxTimeMS",
        deserialize_with = "deserialize_duration_option_from_u64_millis"
    )]
    pub max_time: Option<DurationWrapper>,
    #[serde(skip_serializing)]
    pub selection_criteria: Option<SelectionCriteriaWrapper>,
    #[serde(skip_serializing)]
    pub read_concern: Option<ReadConcernWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<EstimatedDocumentCountOptionsWrapper> for EstimatedDocumentCountOptions {
    fn from(wrapper: EstimatedDocumentCountOptionsWrapper) -> Self {
        Self::builder()
            .max_time(wrapper.max_time.map(Into::into))
            .selection_criteria(wrapper.selection_criteria.map(Into::into))
            .read_concern(wrapper.read_concern.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DropIndexOptionsWrapper {
    #[serde(
        rename = "maxTimeMS",
        default,
        serialize_with = "serialize_duration_option_as_int_millis",
        deserialize_with = "deserialize_duration_option_from_u64_millis"
    )]
    pub max_time: Option<DurationWrapper>,
    pub write_concern: Option<WriteConcernWrapper>,
    pub comment: Option<BsonWrapper>,
}

impl From<DropIndexOptionsWrapper> for DropIndexOptions {
    fn from(wrapper: DropIndexOptionsWrapper) -> Self {
        Self::builder()
            .max_time(wrapper.max_time.map(Into::into))
            .write_concern(wrapper.write_concern.map(Into::into))
            .comment(wrapper.comment.map(Into::into))
            .build()
    }
}
