//! Oracle data type descriptors.
//!
//! This module defines Oracle's internal data type identifiers used in TTI
//! protocol for describing column types, bind variables, and result sets.
//!
//! # Oracle Database Version Compatibility
//!
//! This module supports Oracle Database versions 11g through 23c:
//! - **11g/12c**: Core types (VARCHAR2, NUMBER, DATE, TIMESTAMP, LOB, etc.)
//! - **18c/19c**: Enhanced JSON support via CLOB/BLOB
//! - **21c+**: Native JSON type (code 266), extended BOOLEAN support
//! - **23c**: BOOLEAN as SQL type (not just PL/SQL)
//!
//! # Oracle Products Using TNS/TTI
//!
//! The TNS (Transparent Network Substrate) and TTI (Two-Task Interface)
//! protocols are used by:
//! - Oracle Database (all editions: Express, Standard, Enterprise)
//! - Oracle Autonomous Database (cloud)
//! - Oracle Exadata
//! - Oracle RAC (Real Application Clusters)
//!
//! Note: Oracle TimesTen uses ODBC/JDBC, Oracle NoSQL has its own protocol.

/// Oracle data type identifier.
///
/// These are the internal type codes Oracle uses to represent data types
/// in the wire protocol. Note that some types share the same code but differ
/// by character set form (e.g., VARCHAR2 vs NVARCHAR2, CLOB vs NCLOB).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OracleDataType {
    // ============================================================
    // Character Types
    // ============================================================
    /// VARCHAR2 (variable-length character string, max 4000/32767 bytes).
    /// Type code: 1. NVARCHAR2 uses same code with NCHAR_CS charset form.
    Varchar2,
    /// VARCHAR (ANSI fixed-length character string).
    /// Type code: 9. Legacy type, prefer VARCHAR2.
    Varchar,
    /// CHAR (fixed-length character, space-padded).
    /// Type code: 96 with CHAR_CS. Max 2000 bytes.
    Char,
    /// NCHAR (national character set fixed-length).
    /// Type code: 96 with NCHAR_CS.
    Nchar,
    /// STRING (null-terminated C-style string).
    /// Type code: 5. Internal/external type.
    String,
    /// CHARZ (null-terminated fixed-length char).
    /// Type code: 97. Primarily for external representation.
    Charz,
    /// LONG (deprecated variable-length character, max 2GB).
    /// Type code: 8. Use CLOB instead.
    Long,

    // ============================================================
    // Numeric Types
    // ============================================================
    /// NUMBER (Oracle's arbitrary precision numeric type).
    /// Type code: 2. Range: 1E-130 to 9.99...E125, 38 significant digits.
    Number,
    /// INTEGER (binary integer, stored as NUMBER).
    /// Type code: 3.
    Integer,
    /// FLOAT (ANSI floating point, stored as NUMBER).
    /// Type code: 4.
    Float,
    /// VARNUM (variable-length number format).
    /// Type code: 6. Internal representation.
    Varnum,
    /// DECIMAL (packed decimal).
    /// Type code: 7. ANSI compatibility type.
    Decimal,
    /// UNSIGNED INTEGER.
    /// Type code: 68.
    UnsignedInt,
    /// BINARY_FLOAT (IEEE 754 single precision, 4 bytes).
    /// Type code: 100. Introduced in Oracle 10g.
    BinaryFloat,
    /// BINARY_DOUBLE (IEEE 754 double precision, 8 bytes).
    /// Type code: 101. Introduced in Oracle 10g.
    BinaryDouble,

    // ============================================================
    // Date/Time Types
    // ============================================================
    /// DATE (date and time, 7 bytes).
    /// Type code: 12. Range: 4712 BC to 9999 AD, seconds precision.
    Date,
    /// TIMESTAMP (date/time with fractional seconds, 11 bytes).
    /// Type code: 180. Up to 9 digits of fractional seconds.
    Timestamp,
    /// TIMESTAMP WITH TIME ZONE (13 bytes).
    /// Type code: 181. Stores time zone offset or region name.
    TimestampTz,
    /// TIMESTAMP WITH LOCAL TIME ZONE (11 bytes).
    /// Type code: 231. Normalized to database timezone, displayed in session timezone.
    TimestampLtz,
    /// INTERVAL YEAR TO MONTH (5 bytes).
    /// Type code: 182. Stores year and month intervals.
    IntervalYm,
    /// INTERVAL DAY TO SECOND (11 bytes).
    /// Type code: 183. Stores day, hour, minute, second intervals.
    IntervalDs,

    // ============================================================
    // Binary/Raw Types
    // ============================================================
    /// RAW (fixed-length binary data, max 2000/32767 bytes).
    /// Type code: 23.
    Raw,
    /// VARRAW (variable-length raw).
    /// Type code: 15. Internal type.
    Varraw,
    /// LONG RAW (deprecated, max 2GB binary).
    /// Type code: 24. Use BLOB instead.
    LongRaw,

    // ============================================================
    // LOB Types (Large Objects)
    // ============================================================
    /// CLOB (Character Large Object, max 4GB * db_block_size).
    /// Type code: 112. NCLOB uses same code with NCHAR_CS charset form.
    Clob,
    /// BLOB (Binary Large Object, max 4GB * db_block_size).
    /// Type code: 113.
    Blob,
    /// BFILE (external binary file locator).
    /// Type code: 114. Read-only reference to OS file.
    Bfile,

    // ============================================================
    // Row Identifier Types
    // ============================================================
    /// ROWID (physical row address, deprecated format).
    /// Type code: 11. 10 bytes, restricted format.
    Rowid,
    /// UROWID (Universal ROWID, max 4000 bytes).
    /// Type code: 104. Supports logical rowids and index-organized tables.
    Urowid,

    // ============================================================
    // Object/Reference Types
    // ============================================================
    /// NAMED TYPE (user-defined object type, XMLTYPE, SDO_GEOMETRY, etc).
    /// Type code: 108. ADT (Abstract Data Type).
    NamedType,
    /// VARRAY (variable-size array collection).
    /// Type code: 109. Ordered collection with max size.
    Varray,
    /// NESTED TABLE (collection type).
    /// Type code: 109 (same as VARRAY, distinguished by metadata).
    NestedTable,
    /// REF (reference to object instance).
    /// Type code: 110. Pointer to row object.
    Ref,

    // ============================================================
    // Special Types
    // ============================================================
    /// CURSOR (REF CURSOR for result sets).
    /// Type code: 102.
    Cursor,
    /// BOOLEAN (PL/SQL boolean, SQL boolean in 23c+).
    /// Type code: 252.
    Boolean,

    // ============================================================
    // JSON Type (Oracle 21c+)
    // ============================================================
    /// JSON (native JSON data type).
    /// Type code: 266. Introduced in Oracle 21c.
    /// Binary format (OSON) for efficient storage and querying.
    Json,

    // ============================================================
    // XMLTYPE
    // ============================================================
    /// XMLTYPE (XML data type).
    /// Type code: 108 (NamedType), but has special handling.
    /// Can be stored as CLOB, binary XML, or object-relational.
    XmlType,

    // ============================================================
    // Internal/System Types
    // ============================================================
    /// ANYDATA (self-describing data container).
    /// Type code: 184. Can hold any Oracle type.
    AnyData,
    /// ANYTYPE (type descriptor).
    /// Type code: 178. Describes any Oracle type.
    AnyType,
    /// ANYDATASET (collection of ANYDATA).
    /// Type code: 179.
    AnyDataSet,

    /// Unknown data type (preserves original code).
    Unknown(u8),
}

impl OracleDataType {
    /// Create from raw type code.
    ///
    /// Note: Some type codes can represent multiple types distinguished by
    /// character set form. Use `from_u8_with_charset` for precise mapping.
    ///
    /// # Type Code Ambiguities
    ///
    /// - Code 96: CHAR (CHAR_CS) or NCHAR (NCHAR_CS)
    /// - Code 1: VARCHAR2 (CHAR_CS) or NVARCHAR2 (NCHAR_CS)
    /// - Code 108: NamedType, XMLTYPE, or user-defined ADT
    /// - Code 109: VARRAY or NESTED TABLE
    /// - Code 112: CLOB (CHAR_CS) or NCLOB (NCHAR_CS)
    pub fn from_u8(value: u8) -> Self {
        match value {
            // Character types
            1 => Self::Varchar2,
            5 => Self::String,
            8 => Self::Long,
            9 => Self::Varchar,
            96 => Self::Char, // Default to CHAR; use from_u8_with_charset for NCHAR
            97 => Self::Charz,

            // Numeric types
            2 => Self::Number,
            3 => Self::Integer,
            4 => Self::Float,
            6 => Self::Varnum,
            7 => Self::Decimal,
            68 => Self::UnsignedInt,
            100 => Self::BinaryFloat,
            101 => Self::BinaryDouble,

            // Date/time types
            12 => Self::Date,
            180 => Self::Timestamp,
            181 => Self::TimestampTz,
            182 => Self::IntervalYm,
            183 => Self::IntervalDs,
            231 => Self::TimestampLtz,

            // Binary types
            15 => Self::Varraw,
            23 => Self::Raw,
            24 => Self::LongRaw,

            // LOB types
            112 => Self::Clob, // Default to CLOB; use from_u8_with_charset for NCLOB
            113 => Self::Blob,
            114 => Self::Bfile,

            // Row ID types
            11 => Self::Rowid,
            104 => Self::Urowid,

            // Object types
            108 => Self::NamedType, // Could be XMLTYPE or user ADT
            109 => Self::Varray,    // Could be VARRAY or NESTED TABLE
            110 => Self::Ref,

            // Special types
            102 => Self::Cursor,
            252 => Self::Boolean,

            // Internal/system types
            178 => Self::AnyType,
            179 => Self::AnyDataSet,
            184 => Self::AnyData,

            // Unknown (preserves original code)
            other => Self::Unknown(other),
        }
    }

    /// Create from raw type code with charset form for disambiguation.
    ///
    /// Use this method when you need to distinguish between:
    /// - VARCHAR2 (charset_form=1) vs NVARCHAR2 (charset_form=2)
    /// - CHAR (charset_form=1) vs NCHAR (charset_form=2)
    /// - CLOB (charset_form=1) vs NCLOB (charset_form=2)
    pub fn from_u8_with_charset(value: u8, charset_form: u8) -> Self {
        match (value, charset_form) {
            (96, charset_form::NCHAR_CS) => Self::Nchar,
            (96, _) => Self::Char,
            _ => Self::from_u8(value),
        }
    }

    /// Convert to raw type code.
    #[inline]
    pub fn code(&self) -> u8 {
        self.as_u8()
    }

    /// Convert to raw type code.
    ///
    /// Note: Some types share codes and are distinguished by charset form:
    /// - CHAR and NCHAR both use code 96
    /// - VARRAY and NestedTable both use code 109
    /// - XmlType uses code 108 (same as NamedType)
    pub fn as_u8(&self) -> u8 {
        match self {
            // Character types
            Self::Varchar2 => 1,
            Self::String => 5,
            Self::Long => 8,
            Self::Varchar => 9,
            Self::Char => 96,
            Self::Nchar => 96, // Same code, different charset form
            Self::Charz => 97,

            // Numeric types
            Self::Number => 2,
            Self::Integer => 3,
            Self::Float => 4,
            Self::Varnum => 6,
            Self::Decimal => 7,
            Self::UnsignedInt => 68,
            Self::BinaryFloat => 100,
            Self::BinaryDouble => 101,

            // Date/time types
            Self::Date => 12,
            Self::Timestamp => 180,
            Self::TimestampTz => 181,
            Self::IntervalYm => 182,
            Self::IntervalDs => 183,
            Self::TimestampLtz => 231,

            // Binary types
            Self::Varraw => 15,
            Self::Raw => 23,
            Self::LongRaw => 24,

            // LOB types
            Self::Clob => 112,
            Self::Blob => 113,
            Self::Bfile => 114,

            // Row ID types
            Self::Rowid => 11,
            Self::Urowid => 104,

            // Object types
            Self::NamedType => 108,
            Self::XmlType => 108, // Same code as NamedType
            Self::Varray => 109,
            Self::NestedTable => 109, // Same code as Varray
            Self::Ref => 110,

            // Special types
            Self::Cursor => 102,
            Self::Boolean => 252,
            Self::Json => 119, // Oracle 21c+ native JSON (actual code varies by version)

            // Internal/system types
            Self::AnyType => 178,
            Self::AnyDataSet => 179,
            Self::AnyData => 184,

            Self::Unknown(v) => *v,
        }
    }

    /// Get the human-readable type name.
    pub fn name(&self) -> &'static str {
        match self {
            // Character types
            Self::Varchar2 => "VARCHAR2",
            Self::Varchar => "VARCHAR",
            Self::Char => "CHAR",
            Self::Nchar => "NCHAR",
            Self::String => "STRING",
            Self::Charz => "CHARZ",
            Self::Long => "LONG",

            // Numeric types
            Self::Number => "NUMBER",
            Self::Integer => "INTEGER",
            Self::Float => "FLOAT",
            Self::Varnum => "VARNUM",
            Self::Decimal => "DECIMAL",
            Self::UnsignedInt => "UNSIGNED INTEGER",
            Self::BinaryFloat => "BINARY_FLOAT",
            Self::BinaryDouble => "BINARY_DOUBLE",

            // Date/time types
            Self::Date => "DATE",
            Self::Timestamp => "TIMESTAMP",
            Self::TimestampTz => "TIMESTAMP WITH TIME ZONE",
            Self::TimestampLtz => "TIMESTAMP WITH LOCAL TIME ZONE",
            Self::IntervalYm => "INTERVAL YEAR TO MONTH",
            Self::IntervalDs => "INTERVAL DAY TO SECOND",

            // Binary types
            Self::Raw => "RAW",
            Self::Varraw => "VARRAW",
            Self::LongRaw => "LONG RAW",

            // LOB types
            Self::Clob => "CLOB",
            Self::Blob => "BLOB",
            Self::Bfile => "BFILE",

            // Row ID types
            Self::Rowid => "ROWID",
            Self::Urowid => "UROWID",

            // Object types
            Self::NamedType => "NAMED TYPE",
            Self::XmlType => "XMLTYPE",
            Self::Varray => "VARRAY",
            Self::NestedTable => "NESTED TABLE",
            Self::Ref => "REF",

            // Special types
            Self::Cursor => "CURSOR",
            Self::Boolean => "BOOLEAN",
            Self::Json => "JSON",

            // Internal types
            Self::AnyType => "ANYTYPE",
            Self::AnyDataSet => "ANYDATASET",
            Self::AnyData => "ANYDATA",

            Self::Unknown(_) => "UNKNOWN",
        }
    }

    /// Check if this is a LOB type.
    pub fn is_lob(&self) -> bool {
        matches!(self, Self::Clob | Self::Blob | Self::Bfile)
    }

    /// Check if this is a character/string type.
    pub fn is_character(&self) -> bool {
        matches!(
            self,
            Self::Varchar2 | Self::Varchar | Self::Char | Self::Nchar | Self::Charz | Self::String | Self::Long | Self::Clob
        )
    }

    /// Check if this is a numeric type.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Number
                | Self::Integer
                | Self::Float
                | Self::Varnum
                | Self::Decimal
                | Self::UnsignedInt
                | Self::BinaryFloat
                | Self::BinaryDouble
        )
    }

    /// Check if this is a date/time type.
    pub fn is_datetime(&self) -> bool {
        matches!(
            self,
            Self::Date | Self::Timestamp | Self::TimestampTz | Self::TimestampLtz | Self::IntervalYm | Self::IntervalDs
        )
    }

    /// Check if this is a binary/raw type.
    pub fn is_binary(&self) -> bool {
        matches!(self, Self::Raw | Self::LongRaw | Self::Varraw | Self::Blob)
    }

    /// Check if this is a collection type.
    pub fn is_collection(&self) -> bool {
        matches!(self, Self::Varray | Self::NestedTable)
    }

    /// Check if this is an object/reference type.
    pub fn is_object(&self) -> bool {
        matches!(self, Self::NamedType | Self::XmlType | Self::Ref | Self::Varray | Self::NestedTable)
    }

    /// Check if this is a JSON type.
    pub fn is_json(&self) -> bool {
        matches!(self, Self::Json)
    }

    /// Check if this is an XML type.
    pub fn is_xml(&self) -> bool {
        matches!(self, Self::XmlType)
    }

    /// Check if this type requires special handling (LOB, collection, object).
    pub fn requires_special_handling(&self) -> bool {
        self.is_lob() || self.is_collection() || self.is_object() || self.is_json()
    }

    /// Get the minimum Oracle version that supports this type.
    pub fn min_oracle_version(&self) -> &'static str {
        match self {
            Self::Json => "21c",
            Self::BinaryFloat | Self::BinaryDouble => "10g",
            Self::TimestampLtz | Self::Timestamp | Self::TimestampTz | Self::IntervalYm | Self::IntervalDs => "9i",
            Self::Varray | Self::NestedTable | Self::NamedType | Self::Ref => "8i",
            _ => "7", // Most types available since Oracle 7
        }
    }
}

impl From<u8> for OracleDataType {
    fn from(value: u8) -> Self {
        Self::from_u8(value)
    }
}

impl From<OracleDataType> for u8 {
    fn from(value: OracleDataType) -> Self {
        value.as_u8()
    }
}

/// Type descriptor with size and precision information.
///
/// Used to fully describe a column or parameter type including
/// its length, precision, and scale.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TypeDescriptor {
    /// The Oracle data type.
    pub data_type: OracleDataType,
    /// Maximum size in bytes (for character/binary types).
    pub max_size: u32,
    /// Numeric precision (for NUMBER types).
    pub precision: u8,
    /// Numeric scale (for NUMBER types).
    pub scale: i8,
    /// Whether NULL values are allowed.
    pub nullable: bool,
    /// Character set form (CHAR_CS = 1, NCHAR_CS = 2).
    pub charset_form: u8,
}

impl TypeDescriptor {
    /// Create a new type descriptor.
    pub fn new(data_type: OracleDataType) -> Self {
        Self {
            data_type,
            max_size: 0,
            precision: 0,
            scale: 0,
            nullable: true,
            charset_form: 1,
        }
    }

    /// Set the maximum size.
    pub fn with_size(mut self, size: u32) -> Self {
        self.max_size = size;
        self
    }

    /// Set precision and scale for numeric types.
    pub fn with_precision(mut self, precision: u8, scale: i8) -> Self {
        self.precision = precision;
        self.scale = scale;
        self
    }

    /// Set whether NULL is allowed.
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Set the character set form.
    pub fn with_charset_form(mut self, form: u8) -> Self {
        self.charset_form = form;
        self
    }

    /// Create a VARCHAR2 descriptor with specified max length.
    pub fn varchar2(max_len: u32) -> Self {
        Self::new(OracleDataType::Varchar2).with_size(max_len)
    }

    /// Create a NUMBER descriptor with precision and scale.
    pub fn number(precision: u8, scale: i8) -> Self {
        Self::new(OracleDataType::Number).with_precision(precision, scale)
    }

    /// Create a DATE descriptor.
    pub fn date() -> Self {
        Self::new(OracleDataType::Date)
    }

    /// Create a TIMESTAMP descriptor.
    pub fn timestamp() -> Self {
        Self::new(OracleDataType::Timestamp)
    }

    /// Create a BLOB descriptor.
    pub fn blob() -> Self {
        Self::new(OracleDataType::Blob)
    }

    /// Create a CLOB descriptor.
    pub fn clob() -> Self {
        Self::new(OracleDataType::Clob)
    }
}

/// Character set form constants.
pub mod charset_form {
    /// Database character set (CHAR, VARCHAR2, CLOB).
    pub const CHAR_CS: u8 = 1;
    /// National character set (NCHAR, NVARCHAR2, NCLOB).
    pub const NCHAR_CS: u8 = 2;
}

/// JSON type code constants for different Oracle versions.
///
/// Oracle has used different type codes for JSON across versions:
/// - Pre-21c: JSON stored as VARCHAR2/CLOB, no dedicated type code
/// - 21c+: Native JSON type with code 266 (OSON binary format)
/// - Some contexts use code 119 for JSON compatibility
pub mod json_type_codes {
    /// JSON type code used in some Oracle contexts (text-based JSON).
    /// This is the legacy/compatibility code.
    pub const JSON_COMPAT: u8 = 119;

    /// Native JSON type code introduced in Oracle 21c (OSON binary format).
    /// This is a 16-bit code that requires extended type handling.
    pub const JSON_NATIVE_21C: u16 = 266;

    /// Check if a type code represents JSON.
    pub fn is_json_code(code: u16) -> bool {
        code == JSON_COMPAT as u16 || code == JSON_NATIVE_21C
    }
}

/// Oracle database version for protocol compatibility.
///
/// Used to select appropriate type codes and protocol features
/// for different Oracle versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum OracleVersion {
    /// Oracle 11g (11.1, 11.2)
    Oracle11g,
    /// Oracle 12c (12.1, 12.2)
    Oracle12c,
    /// Oracle 18c
    Oracle18c,
    /// Oracle 19c
    #[default]
    Oracle19c,
    /// Oracle 21c (native JSON, enhanced BOOLEAN)
    Oracle21c,
    /// Oracle 23c (SQL BOOLEAN, many enhancements)
    Oracle23c,
}

impl OracleVersion {
    /// Check if this version supports native JSON type (21c+).
    pub fn supports_native_json(&self) -> bool {
        matches!(self, Self::Oracle21c | Self::Oracle23c)
    }

    /// Check if this version supports SQL BOOLEAN (23c+).
    pub fn supports_sql_boolean(&self) -> bool {
        matches!(self, Self::Oracle23c)
    }

    /// Check if this version supports implicit results (12c+).
    pub fn supports_implicit_results(&self) -> bool {
        !matches!(self, Self::Oracle11g)
    }

    /// Check if this version supports extended VARCHAR2 (12c+).
    /// When enabled, VARCHAR2 can be up to 32767 bytes instead of 4000.
    pub fn supports_extended_varchar2(&self) -> bool {
        !matches!(self, Self::Oracle11g)
    }

    /// Get the JSON type code appropriate for this version.
    pub fn json_type_code(&self) -> u16 {
        if self.supports_native_json() {
            json_type_codes::JSON_NATIVE_21C
        } else {
            json_type_codes::JSON_COMPAT as u16
        }
    }

    /// Parse version from major version number.
    pub fn from_major_version(major: u32) -> Self {
        match major {
            11 => Self::Oracle11g,
            12 => Self::Oracle12c,
            18 => Self::Oracle18c,
            19 => Self::Oracle19c,
            21 => Self::Oracle21c,
            23.. => Self::Oracle23c,
            _ => Self::Oracle11g, // Default to oldest supported
        }
    }
}

impl OracleDataType {
    /// Convert to type code for a specific Oracle version.
    ///
    /// This is useful when the type code differs between Oracle versions,
    /// such as JSON (119 vs 266).
    ///
    /// Returns a u16 to accommodate extended type codes like JSON 266.
    pub fn as_u16_for_version(&self, version: OracleVersion) -> u16 {
        match self {
            Self::Json => version.json_type_code(),
            other => other.as_u8() as u16,
        }
    }

    /// Create from extended type code (u16) with version awareness.
    ///
    /// This handles extended type codes like JSON 266 that don't fit in u8.
    pub fn from_u16_with_version(code: u16, _version: OracleVersion) -> Self {
        if json_type_codes::is_json_code(code) {
            return Self::Json;
        }

        // For codes that fit in u8, delegate to existing method
        if code <= 255 {
            Self::from_u8(code as u8)
        } else {
            // Extended codes we don't recognize
            Self::Unknown(255) // Mark as unknown; we lose precision here
        }
    }

    /// Check if this type requires extended (u16) type code for a version.
    pub fn requires_extended_type_code(&self, version: OracleVersion) -> bool {
        match self {
            Self::Json => version.supports_native_json(),
            _ => false,
        }
    }
}
