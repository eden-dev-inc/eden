use chrono;
use chrono::{Datelike, Timelike};
use format::timestamp::{DateTimeLocalWrapper, DateTimeWrapper, NaiveDateTimeWrapper, NaiveDateWrapper};
use oracle_client::sql_type::{IntervalDS, IntervalYM, OracleType, Timestamp, ToSql};
use oracle_client::{Connection, ShutdownMode, SqlValue, StartupMode};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema, JsonSchema)]
pub struct NamedParam {
    pub name: String,
    pub param: SqlParam,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum SqlParam {
    #[default]
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    UInt8(u64),
    Float4(f32),
    Float8(f64),
    Text(String),
    NText(String), // NVARCHAR2/NCHAR - Unicode text
    Char(String),  // Fixed-length CHAR
    NChar(String), // Fixed-length NCHAR
    Bytes(Vec<u8>),
    Raw(Vec<u8>), // RAW type specifically
    Json(serde_json::Value),
    Xml(String),      // XML type
    Long(String),     // LONG text
    LongRaw(Vec<u8>), // LONG RAW

    // Date and time types
    Date(NaiveDateWrapper),
    Timestamp(NaiveDateTimeWrapper),
    TimestampTz(DateTimeWrapper),
    TimestampLtz(DateTimeLocalWrapper),

    // Interval types (using duration-like representations)
    IntervalDayToSecond {
        days: i32,
        hours: i32,
        minutes: i32,
        seconds: i32,
        nanoseconds: i32,
    },
    IntervalYearToMonth {
        years: i32,
        months: i32,
    },

    // LOB types
    Clob(String),  // Character LOB
    NClob(String), // National Character LOB
    Blob(Vec<u8>), // Binary LOB
    BFile(String), // Binary file locator

    // Oracle-specific types
    Rowid(String),  // ROWID
    Number(String), // Oracle NUMBER as string for precision preservation
    Float(String),  // Oracle FLOAT as string

    // Object and cursor types (typically handled differently)
    RefCursor,      // REF CURSOR placeholder
    Object(String), // Object type placeholder (would need actual object handling)
}

impl ToSql for SqlParam {
    fn oratype(&self, _conn: &Connection) -> oracle_client::Result<OracleType> {
        match self {
            SqlParam::Null => Ok(OracleType::Varchar2(1)),
            SqlParam::Bool(_) => Ok(OracleType::Number(1, 0)), // Use NUMBER(1,0) for boolean
            SqlParam::Int2(_) => Ok(OracleType::Number(5, 0)),
            SqlParam::Int4(_) => Ok(OracleType::Number(10, 0)),
            SqlParam::Int8(_) => Ok(OracleType::Int64),
            SqlParam::UInt8(_) => Ok(OracleType::UInt64),
            SqlParam::Float4(_) => Ok(OracleType::BinaryFloat),
            SqlParam::Float8(_) => Ok(OracleType::BinaryDouble),

            SqlParam::Text(s) => {
                let byte_len = s.len() as u32;
                if byte_len <= 4000 {
                    Ok(OracleType::Varchar2(byte_len.max(1)))
                } else {
                    Ok(OracleType::CLOB)
                }
            }
            SqlParam::NText(s) => {
                let char_len = s.chars().count() as u32;
                if char_len <= 2000 {
                    Ok(OracleType::NVarchar2(char_len.max(1)))
                } else {
                    Ok(OracleType::NCLOB)
                }
            }
            SqlParam::Char(s) => {
                let byte_len = s.len() as u32;
                Ok(OracleType::Char(byte_len.clamp(1, 2000)))
            }
            SqlParam::NChar(s) => {
                let char_len = s.chars().count() as u32;
                Ok(OracleType::NChar(char_len.clamp(1, 1000)))
            }

            SqlParam::Bytes(b) | SqlParam::Raw(b) => {
                let byte_len = b.len() as u32;
                if byte_len <= 2000 {
                    Ok(OracleType::Raw(byte_len.max(1)))
                } else {
                    Ok(OracleType::BLOB)
                }
            }

            SqlParam::Json(_) => Ok(OracleType::Json),
            SqlParam::Xml(_) => Ok(OracleType::Xml),
            SqlParam::Long(_) => Ok(OracleType::Long),
            SqlParam::LongRaw(_) => Ok(OracleType::LongRaw),

            // Date/Time types
            SqlParam::Date(_) => Ok(OracleType::Date),
            SqlParam::Timestamp(_) => Ok(OracleType::Timestamp(6)),
            SqlParam::TimestampTz(_) => Ok(OracleType::TimestampTZ(6)),
            SqlParam::TimestampLtz(_) => Ok(OracleType::TimestampLTZ(6)),
            SqlParam::IntervalDayToSecond { .. } => Ok(OracleType::IntervalDS(2, 6)),
            SqlParam::IntervalYearToMonth { .. } => Ok(OracleType::IntervalYM(2)),

            // LOB types
            SqlParam::Clob(_) => Ok(OracleType::CLOB),
            SqlParam::NClob(_) => Ok(OracleType::NCLOB),
            SqlParam::Blob(_) => Ok(OracleType::BLOB),
            SqlParam::BFile(_) => Ok(OracleType::BFILE),

            // Oracle-specific types
            SqlParam::Rowid(_) => Ok(OracleType::Rowid),
            SqlParam::Number(_) => Ok(OracleType::Number(38, 127)), // Maximum precision
            SqlParam::Float(_) => Ok(OracleType::Float(126)),

            // Special types
            SqlParam::RefCursor => Ok(OracleType::RefCursor),
            SqlParam::Object(_) => {
                // This would need actual ObjectType handling in practice
                Err(oracle_client::Error::new(
                    oracle_client::ErrorKind::InvalidTypeConversion,
                    "Object types require specific ObjectType implementation",
                ))
            }
        }
    }

    fn to_sql(&self, val: &mut SqlValue) -> oracle_client::Result<()> {
        match self {
            SqlParam::Null => val.set_null(),

            // For bool, convert to i32 and use set() with the i32 value
            SqlParam::Bool(b) => {
                let bool_as_int = if *b { 1i32 } else { 0i32 };
                val.set(&bool_as_int)
            }

            // Primitive numeric types - use set() directly
            SqlParam::Int2(i) => val.set(i),
            SqlParam::Int4(i) => val.set(i),
            SqlParam::Int8(i) => val.set(i),
            SqlParam::UInt8(u) => val.set(u),
            SqlParam::Float4(f) => val.set(f),
            SqlParam::Float8(f) => val.set(f),

            // String types - use set() with string reference
            SqlParam::Text(s) | SqlParam::NText(s) | SqlParam::Char(s) | SqlParam::NChar(s) => val.set(&s.as_str()),

            // Binary types - use set() with slice reference
            SqlParam::Bytes(b) | SqlParam::Raw(b) => val.set(&b.as_slice()),

            SqlParam::Json(json) => {
                let json_str = serde_json::to_string(json).map_err(|e| {
                    oracle_client::Error::new(oracle_client::ErrorKind::InvalidTypeConversion, format!("Failed to serialize JSON: {}", e))
                })?;
                val.set(&json_str.as_str())
            }

            SqlParam::Xml(s) | SqlParam::Long(s) => val.set(&s.as_str()),
            SqlParam::LongRaw(b) => val.set(&b.as_slice()),

            // Date/Time types - convert to Oracle-specific types and use set()
            SqlParam::Date(date) => {
                let datetime = date.as_naive_date();
                let timestamp = Timestamp::new(datetime.year(), datetime.month(), datetime.day(), 0, 0, 0, 0)?;
                val.set(&timestamp)
            }

            SqlParam::Timestamp(dt) => {
                let timestamp = Timestamp::new(dt.year(), dt.month(), dt.day(), dt.hours(), dt.minutes(), dt.seconds(), dt.nanoseconds())?;
                val.set(&timestamp)
            }

            SqlParam::TimestampTz(dt) => {
                let dt = dt.as_datetime().naive_utc();
                let timestamp = Timestamp::new(dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(), dt.second(), dt.nanosecond())?;
                val.set(&timestamp)
            }

            SqlParam::TimestampLtz(dt) => {
                let naive = dt.as_datetime();
                let timestamp = Timestamp::new(
                    naive.year(),
                    naive.month(),
                    naive.day(),
                    naive.hour(),
                    naive.minute(),
                    naive.second(),
                    naive.nanosecond(),
                )?;
                val.set(&timestamp)
            }

            SqlParam::IntervalDayToSecond { days, hours, minutes, seconds, nanoseconds } => {
                let interval = IntervalDS::new(*days, *hours, *minutes, *seconds, *nanoseconds)?;
                val.set(&interval)
            }

            SqlParam::IntervalYearToMonth { years, months } => {
                let interval = IntervalYM::new(*years, *months)?;
                val.set(&interval)
            }

            // LOB types - use set() with string/slice reference
            SqlParam::Clob(s) | SqlParam::NClob(s) => val.set(&s.as_str()),
            SqlParam::Blob(b) => val.set(&b.as_slice()),
            SqlParam::BFile(s) => val.set(&s.as_str()),

            // Oracle-specific types - convert to string and use set()
            SqlParam::Rowid(s) => val.set(&s.as_str()),
            SqlParam::Number(s) | SqlParam::Float(s) => val.set(&s.as_str()),

            // Special types
            SqlParam::RefCursor => Err(oracle_client::Error::new(
                oracle_client::ErrorKind::InvalidTypeConversion,
                "RefCursor cannot be used as input parameter",
            )),
            SqlParam::Object(s) => Err(oracle_client::Error::new(
                oracle_client::ErrorKind::InvalidTypeConversion,
                format!("Object type not implemented: {}", s),
            )),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum ShutdownModeWrapper {
    /// Further connects are prohibited. Waits for users to disconnect from
    /// the database.
    #[default]
    Default,

    /// Further connects are prohibited and no new transactions are allowed.
    /// Waits for active transactions to complete.
    Transactional,

    /// Further connects are prohibited and no new transactions are allowed.
    /// Waits only for local transactions to complete.
    TransactionalLocal,

    /// Does not wait for current calls to complete or users to disconnect
    /// from the database. All uncommitted transactions are terminated and
    /// rolled back.
    Immediate,

    /// Does not wait for current calls to complete or users to disconnect
    /// from the database. All uncommitted transactions are terminated and
    /// are not rolled back. This is the fastest possible way to shut down
    /// the database, but the next database startup may require instance
    /// recovery. Therefore, this option should be used only in unusual
    /// circumstances; for example, if a background process terminates abnormally.
    Abort,

    /// Shuts down the database. Should be used only in the second call
    /// to [`Connection::shutdown_database`] after the database is closed and dismounted.
    Final,
}

impl From<ShutdownModeWrapper> for ShutdownMode {
    fn from(mode: ShutdownModeWrapper) -> Self {
        match mode {
            ShutdownModeWrapper::Default => ShutdownMode::Default,
            ShutdownModeWrapper::Transactional => ShutdownMode::Transactional,
            ShutdownModeWrapper::TransactionalLocal => ShutdownMode::TransactionalLocal,
            ShutdownModeWrapper::Immediate => ShutdownMode::Immediate,
            ShutdownModeWrapper::Abort => ShutdownMode::Abort,
            ShutdownModeWrapper::Final => ShutdownMode::Final,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum StartupModeWrapper {
    /// Shuts down a running instance (if there is any) using ABORT before
    /// starting a new one. This mode should be used only in unusual circumstances.
    #[default]
    Force,

    /// Allows database access only to users with both the CREATE SESSION
    /// and RESTRICTED SESSION privileges (normally, the DBA).
    Restrict,
}

impl From<StartupModeWrapper> for StartupMode {
    fn from(mode: StartupModeWrapper) -> Self {
        match mode {
            StartupModeWrapper::Restrict => StartupMode::Restrict,
            StartupModeWrapper::Force => StartupMode::Force,
        }
    }
}

//
// impl PartialSchema for SqlParam {
//     fn schema() -> RefOr<Schema> {
//         // Use OneOf to represent the enum variants
//         RefOr::T(Schema::OneOf(
//             OneOfBuilder::new()
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Null",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::AnyValue)
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Bool",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::Boolean))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Int2",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::Integer))
//                                     .format(Some(SchemaFormat::KnownFormat(
//                                         utoipa::openapi::schema::KnownFormat::Int32,
//                                     )))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Int4",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::Integer))
//                                     .format(Some(SchemaFormat::KnownFormat(
//                                         utoipa::openapi::schema::KnownFormat::Int32,
//                                     )))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Int8",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::Integer))
//                                     .format(Some(SchemaFormat::KnownFormat(
//                                         utoipa::openapi::schema::KnownFormat::Int64,
//                                     )))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "UInt8",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::Integer))
//                                     .format(Some(SchemaFormat::KnownFormat(
//                                         utoipa::openapi::schema::KnownFormat::Int64,
//                                     )))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Float4",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::Number))
//                                     .format(Some(SchemaFormat::KnownFormat(
//                                         utoipa::openapi::schema::KnownFormat::Float,
//                                     )))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Float8",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::Number))
//                                     .format(Some(SchemaFormat::KnownFormat(
//                                         utoipa::openapi::schema::KnownFormat::Double,
//                                     )))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Text",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "NText",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Char",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "NChar",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Bytes",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::Array))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Raw",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::Array))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Json",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::AnyValue)
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Xml",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Long",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "LongRaw",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::Array))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Date",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .format(Some(SchemaFormat::KnownFormat(
//                                         utoipa::openapi::schema::KnownFormat::Date,
//                                     )))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Timestamp",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .format(Some(SchemaFormat::KnownFormat(
//                                         utoipa::openapi::schema::KnownFormat::DateTime,
//                                     )))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "TimestampTz",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .format(Some(SchemaFormat::KnownFormat(
//                                         utoipa::openapi::schema::KnownFormat::DateTime,
//                                     )))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "TimestampLtz",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .format(Some(SchemaFormat::KnownFormat(
//                                         utoipa::openapi::schema::KnownFormat::DateTime,
//                                     )))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "IntervalDayToSecond",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::Object))
//                                     .property(
//                                         "days",
//                                         RefOr::T(Schema::Object(
//                                             ObjectBuilder::new()
//                                                 .schema_type(SchemaType::Type(Type::Integer))
//                                                 .format(Some(SchemaFormat::KnownFormat(
//                                                     utoipa::openapi::schema::KnownFormat::Int32,
//                                                 )))
//                                                 .build(),
//                                         )),
//                                     )
//                                     .property(
//                                         "hours",
//                                         RefOr::T(Schema::Object(
//                                             ObjectBuilder::new()
//                                                 .schema_type(SchemaType::Type(Type::Integer))
//                                                 .format(Some(SchemaFormat::KnownFormat(
//                                                     utoipa::openapi::schema::KnownFormat::Int32,
//                                                 )))
//                                                 .build(),
//                                         )),
//                                     )
//                                     .property(
//                                         "minutes",
//                                         RefOr::T(Schema::Object(
//                                             ObjectBuilder::new()
//                                                 .schema_type(SchemaType::Type(Type::Integer))
//                                                 .format(Some(SchemaFormat::KnownFormat(
//                                                     utoipa::openapi::schema::KnownFormat::Int32,
//                                                 )))
//                                                 .build(),
//                                         )),
//                                     )
//                                     .property(
//                                         "seconds",
//                                         RefOr::T(Schema::Object(
//                                             ObjectBuilder::new()
//                                                 .schema_type(SchemaType::Type(Type::Integer))
//                                                 .format(Some(SchemaFormat::KnownFormat(
//                                                     utoipa::openapi::schema::KnownFormat::Int32,
//                                                 )))
//                                                 .build(),
//                                         )),
//                                     )
//                                     .property(
//                                         "nanoseconds",
//                                         RefOr::T(Schema::Object(
//                                             ObjectBuilder::new()
//                                                 .schema_type(SchemaType::Type(Type::Integer))
//                                                 .format(Some(SchemaFormat::KnownFormat(
//                                                     utoipa::openapi::schema::KnownFormat::Int32,
//                                                 )))
//                                                 .build(),
//                                         )),
//                                     )
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "IntervalYearToMonth",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::Object))
//                                     .property(
//                                         "years",
//                                         RefOr::T(Schema::Object(
//                                             ObjectBuilder::new()
//                                                 .schema_type(SchemaType::Type(Type::Integer))
//                                                 .format(Some(SchemaFormat::KnownFormat(
//                                                     utoipa::openapi::schema::KnownFormat::Int32,
//                                                 )))
//                                                 .build(),
//                                         )),
//                                     )
//                                     .property(
//                                         "months",
//                                         RefOr::T(Schema::Object(
//                                             ObjectBuilder::new()
//                                                 .schema_type(SchemaType::Type(Type::Integer))
//                                                 .format(Some(SchemaFormat::KnownFormat(
//                                                     utoipa::openapi::schema::KnownFormat::Int32,
//                                                 )))
//                                                 .build(),
//                                         )),
//                                     )
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Clob",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "NClob",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Blob",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::Array))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "BFile",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Rowid",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Number",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Float",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "RefCursor",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::AnyValue)
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .item(
//                     ObjectBuilder::new()
//                         .schema_type(SchemaType::Type(Type::Object))
//                         .property(
//                             "Object",
//                             RefOr::T(Schema::Object(
//                                 ObjectBuilder::new()
//                                     .schema_type(SchemaType::Type(Type::String))
//                                     .build(),
//                             )),
//                         )
//                         .build(),
//                 )
//                 .build(),
//         ))
//     }
// }
//
// impl PartialSchema for ShutdownModeWrapper {
//     fn schema() -> RefOr<Schema> {
//         RefOr::T(Schema::Object(
//             ObjectBuilder::new()
//                 .schema_type(SchemaType::Type(Type::String))
//                 .enum_values(Some(vec![
//                     serde_json::Value::String("Default".to_string()),
//                     serde_json::Value::String("Transactional".to_string()),
//                     serde_json::Value::String("TransactionalLocal".to_string()),
//                     serde_json::Value::String("Immediate".to_string()),
//                     serde_json::Value::String("Abort".to_string()),
//                     serde_json::Value::String("Final".to_string()),
//                 ]))
//                 .default(Some(serde_json::Value::String("Default".to_string())))
//                 .description(Some("Oracle database shutdown mode".to_string()))
//                 .build(),
//         ))
//     }
// }
//
// impl PartialSchema for StartupModeWrapper {
//     fn schema() -> RefOr<Schema> {
//         RefOr::T(Schema::Object(
//             ObjectBuilder::new()
//                 .schema_type(SchemaType::Type(Type::String))
//                 .enum_values(Some(vec![
//                     serde_json::Value::String("Force".to_string()),
//                     serde_json::Value::String("Restrict".to_string()),
//                 ]))
//                 .default(Some(serde_json::Value::String("Force".to_string())))
//                 .description(Some("Oracle database startup mode".to_string()))
//                 .build(),
//         ))
//     }
// }
