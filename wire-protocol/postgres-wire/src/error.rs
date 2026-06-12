//! Error types and protocol constants for the PostgreSQL wire protocol.

use std::fmt;

/// Error indicating an unexpected message type was received.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IncorrectMessageType {
    /// The message type that was encountered.
    pub encountered: u8,
    /// The message type that was expected.
    pub expected: u8,
}

impl fmt::Display for IncorrectMessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "incorrect message type: expected '{}' ({:#04X}), got '{}' ({:#04X})",
            self.expected as char, self.expected, self.encountered as char, self.encountered
        )
    }
}

impl std::error::Error for IncorrectMessageType {}

/// General PostgreSQL wire protocol error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum PgWireError {
    #[error("message too short: expected at least {expected} bytes, got {actual}")]
    MessageTooShort { expected: usize, actual: usize },

    #[error("message too large: {size} exceeds maximum {max}")]
    MessageTooLarge { size: usize, max: usize },

    #[error("unknown message type: '{0}' ({0:#04X})")]
    UnknownMessageType(u8),

    #[error("invalid protocol version: {major}.{minor}")]
    InvalidProtocolVersion { major: i16, minor: i16 },

    #[error("invalid startup message")]
    InvalidStartupMessage,

    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("server error: {severity} {code}: {message}")]
    ServerError { severity: String, code: String, message: String },

    #[error(transparent)]
    IncorrectMessageType(#[from] IncorrectMessageType),

    #[error("invalid string encoding: not valid UTF-8")]
    InvalidStringEncoding,

    #[error("invalid field code: '{0}' ({0:#04X})")]
    InvalidFieldCode(u8),

    #[error("missing required field: {0}")]
    MissingRequiredField(&'static str),

    #[error("invalid transaction status: '{0}' ({0:#04X})")]
    InvalidTransactionStatus(u8),

    #[error("invalid format code: {0}")]
    InvalidFormatCode(i16),

    #[error("negative length: {0}")]
    NegativeLength(i32),
}

/// Backend (server to client) message types.
pub mod backend {
    /// Authentication request/response ('R')
    pub const AUTHENTICATION: u8 = b'R';
    /// Backend key data for cancellation ('K')
    pub const BACKEND_KEY_DATA: u8 = b'K';
    /// Bind complete ('2')
    pub const BIND_COMPLETE: u8 = b'2';
    /// Close complete ('3')
    pub const CLOSE_COMPLETE: u8 = b'3';
    /// Command complete ('C')
    pub const COMMAND_COMPLETE: u8 = b'C';
    /// Copy data ('d')
    pub const COPY_DATA: u8 = b'd';
    /// Copy done ('c')
    pub const COPY_DONE: u8 = b'c';
    /// Copy in response ('G')
    pub const COPY_IN_RESPONSE: u8 = b'G';
    /// Copy out response ('H')
    pub const COPY_OUT_RESPONSE: u8 = b'H';
    /// Copy both response ('W') - used for streaming replication
    pub const COPY_BOTH_RESPONSE: u8 = b'W';
    /// Data row ('D')
    pub const DATA_ROW: u8 = b'D';
    /// Empty query response ('I')
    pub const EMPTY_QUERY_RESPONSE: u8 = b'I';
    /// Error response ('E')
    pub const ERROR_RESPONSE: u8 = b'E';
    /// Function call response ('V') - deprecated
    pub const FUNCTION_CALL_RESPONSE: u8 = b'V';
    /// Negotiate protocol version ('v')
    pub const NEGOTIATE_PROTOCOL_VERSION: u8 = b'v';
    /// No data ('n')
    pub const NO_DATA: u8 = b'n';
    /// Notice response ('N')
    pub const NOTICE_RESPONSE: u8 = b'N';
    /// Notification response ('A')
    pub const NOTIFICATION_RESPONSE: u8 = b'A';
    /// Parameter description ('t')
    pub const PARAMETER_DESCRIPTION: u8 = b't';
    /// Parameter status ('S')
    pub const PARAMETER_STATUS: u8 = b'S';
    /// Parse complete ('1')
    pub const PARSE_COMPLETE: u8 = b'1';
    /// Portal suspended ('s')
    pub const PORTAL_SUSPENDED: u8 = b's';
    /// Ready for query ('Z')
    pub const READY_FOR_QUERY: u8 = b'Z';
    /// Row description ('T')
    pub const ROW_DESCRIPTION: u8 = b'T';
}

/// Frontend (client to server) message types.
pub mod frontend {
    /// Bind ('B')
    pub const BIND: u8 = b'B';
    /// Close ('C')
    pub const CLOSE: u8 = b'C';
    /// Copy data ('d')
    pub const COPY_DATA: u8 = b'd';
    /// Copy done ('c')
    pub const COPY_DONE: u8 = b'c';
    /// Copy fail ('f')
    pub const COPY_FAIL: u8 = b'f';
    /// Describe ('D')
    pub const DESCRIBE: u8 = b'D';
    /// Execute ('E')
    pub const EXECUTE: u8 = b'E';
    /// Flush ('H')
    pub const FLUSH: u8 = b'H';
    /// Function call ('F') - deprecated
    pub const FUNCTION_CALL: u8 = b'F';
    /// Parse ('P')
    pub const PARSE: u8 = b'P';
    /// Password message ('p') - also used for SASL
    pub const PASSWORD_MESSAGE: u8 = b'p';
    /// Query ('Q')
    pub const QUERY: u8 = b'Q';
    /// Sync ('S')
    pub const SYNC: u8 = b'S';
    /// Terminate ('X')
    pub const TERMINATE: u8 = b'X';
}

/// Authentication method identifiers.
pub mod auth {
    /// Authentication successful
    pub const OK: i32 = 0;
    /// Kerberos V5 required (obsolete)
    pub const KERBEROS_V5: i32 = 2;
    /// Cleartext password required
    pub const CLEARTEXT_PASSWORD: i32 = 3;
    /// MD5 password required
    pub const MD5_PASSWORD: i32 = 5;
    /// SCM credential required (obsolete)
    pub const SCM_CREDENTIAL: i32 = 6;
    /// GSS authentication required
    pub const GSS: i32 = 7;
    /// GSS authentication continuation
    pub const GSS_CONTINUE: i32 = 8;
    /// SSPI authentication required
    pub const SSPI: i32 = 9;
    /// SASL authentication required
    pub const SASL: i32 = 10;
    /// SASL authentication continuation
    pub const SASL_CONTINUE: i32 = 11;
    /// SASL authentication final
    pub const SASL_FINAL: i32 = 12;
}

/// Error/Notice response field types.
pub mod error_field {
    /// Severity (localized)
    pub const SEVERITY_LOCALIZED: u8 = b'S';
    /// Severity (always English)
    pub const SEVERITY: u8 = b'V';
    /// SQLSTATE code
    pub const CODE: u8 = b'C';
    /// Message
    pub const MESSAGE: u8 = b'M';
    /// Detail
    pub const DETAIL: u8 = b'D';
    /// Hint
    pub const HINT: u8 = b'H';
    /// Position (character offset in query)
    pub const POSITION: u8 = b'P';
    /// Internal position
    pub const INTERNAL_POSITION: u8 = b'p';
    /// Internal query
    pub const INTERNAL_QUERY: u8 = b'q';
    /// Where (context)
    pub const WHERE: u8 = b'W';
    /// Schema name
    pub const SCHEMA: u8 = b's';
    /// Table name
    pub const TABLE: u8 = b't';
    /// Column name
    pub const COLUMN: u8 = b'c';
    /// Data type name
    pub const DATATYPE: u8 = b'd';
    /// Constraint name
    pub const CONSTRAINT: u8 = b'n';
    /// File name
    pub const FILE: u8 = b'F';
    /// Line number
    pub const LINE: u8 = b'L';
    /// Routine name
    pub const ROUTINE: u8 = b'R';
}

/// Common PostgreSQL type OIDs.
///
/// These are the Object Identifiers for built-in PostgreSQL types.
/// Extension types will have OIDs > 16384 (first user-defined OID).
pub mod type_oid {
    // Basic scalar types
    pub const BOOL: i32 = 16;
    pub const BYTEA: i32 = 17;
    pub const CHAR: i32 = 18;
    pub const NAME: i32 = 19;
    pub const INT8: i32 = 20;
    pub const INT2: i32 = 21;
    pub const INT2VECTOR: i32 = 22;
    pub const INT4: i32 = 23;
    pub const REGPROC: i32 = 24;
    pub const TEXT: i32 = 25;
    pub const OID: i32 = 26;
    pub const TID: i32 = 27;
    pub const XID: i32 = 28;
    pub const CID: i32 = 29;
    pub const OIDVECTOR: i32 = 30;
    pub const JSON: i32 = 114;
    pub const XML: i32 = 142;
    pub const POINT: i32 = 600;
    pub const LSEG: i32 = 601;
    pub const PATH: i32 = 602;
    pub const BOX: i32 = 603;
    pub const POLYGON: i32 = 604;
    pub const LINE: i32 = 628;
    pub const FLOAT4: i32 = 700;
    pub const FLOAT8: i32 = 701;
    pub const UNKNOWN: i32 = 705;
    pub const CIRCLE: i32 = 718;
    pub const MONEY: i32 = 790;
    pub const MACADDR: i32 = 829;
    pub const INET: i32 = 869;
    pub const CIDR: i32 = 650;
    pub const MACADDR8: i32 = 774;
    pub const BPCHAR: i32 = 1042;
    pub const VARCHAR: i32 = 1043;
    pub const DATE: i32 = 1082;
    pub const TIME: i32 = 1083;
    pub const TIMESTAMP: i32 = 1114;
    pub const TIMESTAMPTZ: i32 = 1184;
    pub const INTERVAL: i32 = 1186;
    pub const TIMETZ: i32 = 1266;
    pub const BIT: i32 = 1560;
    pub const VARBIT: i32 = 1562;
    pub const NUMERIC: i32 = 1700;
    pub const REFCURSOR: i32 = 1790;
    pub const REGPROCEDURE: i32 = 2202;
    pub const REGOPER: i32 = 2203;
    pub const REGOPERATOR: i32 = 2204;
    pub const REGCLASS: i32 = 2205;
    pub const REGTYPE: i32 = 2206;
    pub const UUID: i32 = 2950;
    pub const PG_LSN: i32 = 3220;
    pub const JSONB: i32 = 3802;
    pub const JSONPATH: i32 = 4072;

    // Range types (PostgreSQL 9.2+)
    pub const INT4RANGE: i32 = 3904;
    pub const NUMRANGE: i32 = 3906;
    pub const TSRANGE: i32 = 3908;
    pub const TSTZRANGE: i32 = 3910;
    pub const DATERANGE: i32 = 3912;
    pub const INT8RANGE: i32 = 3926;

    // Multirange types (PostgreSQL 14+)
    pub const INT4MULTIRANGE: i32 = 4451;
    pub const NUMMULTIRANGE: i32 = 4532;
    pub const TSMULTIRANGE: i32 = 4533;
    pub const TSTZMULTIRANGE: i32 = 4534;
    pub const DATEMULTIRANGE: i32 = 4535;
    pub const INT8MULTIRANGE: i32 = 4536;

    // Array types (common ones used with extensions)
    pub const BOOL_ARRAY: i32 = 1000;
    pub const BYTEA_ARRAY: i32 = 1001;
    pub const CHAR_ARRAY: i32 = 1002;
    pub const NAME_ARRAY: i32 = 1003;
    pub const INT2_ARRAY: i32 = 1005;
    pub const INT4_ARRAY: i32 = 1007;
    pub const TEXT_ARRAY: i32 = 1009;
    pub const OID_ARRAY: i32 = 1028;
    pub const FLOAT4_ARRAY: i32 = 1021;
    pub const FLOAT8_ARRAY: i32 = 1022;
    pub const INT8_ARRAY: i32 = 1016;
    pub const MONEY_ARRAY: i32 = 791;
    pub const VARCHAR_ARRAY: i32 = 1015;
    pub const DATE_ARRAY: i32 = 1182;
    pub const TIME_ARRAY: i32 = 1183;
    pub const TIMESTAMP_ARRAY: i32 = 1115;
    pub const TIMESTAMPTZ_ARRAY: i32 = 1185;
    pub const INTERVAL_ARRAY: i32 = 1187;
    pub const NUMERIC_ARRAY: i32 = 1231;
    pub const UUID_ARRAY: i32 = 2951;
    pub const JSON_ARRAY: i32 = 199;
    pub const JSONB_ARRAY: i32 = 3807;
    pub const INET_ARRAY: i32 = 1041;
    pub const CIDR_ARRAY: i32 = 651;
    pub const MACADDR_ARRAY: i32 = 1040;

    // Pseudo-types (useful for extension functions)
    pub const RECORD: i32 = 2249;
    pub const CSTRING: i32 = 2275;
    pub const ANY: i32 = 2276;
    pub const ANYARRAY: i32 = 2277;
    pub const VOID: i32 = 2278;
    pub const TRIGGER: i32 = 2279;
    pub const INTERNAL: i32 = 2281;
    pub const ANYELEMENT: i32 = 2283;
    pub const ANYNONARRAY: i32 = 2776;
    pub const ANYENUM: i32 = 3500;
    pub const ANYRANGE: i32 = 3831;
    pub const ANYMULTIRANGE: i32 = 4537;
    pub const ANYCOMPATIBLE: i32 = 5077;
    pub const ANYCOMPATIBLEARRAY: i32 = 5078;
    pub const ANYCOMPATIBLENONARRAY: i32 = 5079;
    pub const ANYCOMPATIBLERANGE: i32 = 5080;
    pub const ANYCOMPATIBLEMULTIRANGE: i32 = 4538;

    // Transaction ID types (useful for extensions dealing with MVCC)
    pub const XID8: i32 = 5069;

    /// First OID available for user-defined types (extensions use OIDs >= this).
    pub const FIRST_USER_OID: i32 = 16384;

    /// Check if an OID is a user-defined type (extension or user-created).
    pub const fn is_user_defined(oid: i32) -> bool {
        oid >= FIRST_USER_OID
    }

    /// Check if an OID is an array type (array OIDs have specific patterns).
    /// Note: This is a heuristic; some array types don't follow this pattern.
    pub const fn is_array_type(oid: i32) -> bool {
        // Array types are typically in the 1000-1299 range for built-in types,
        // but this isn't a reliable check for all arrays. Use pg_type for certainty.
        matches!(
            oid,
            1000..=1299  // Main array type range
                | 199    // JSON array
                | 629    // LINE array
                | 651    // CIDR array
                | 719    // CIRCLE array
                | 775    // MACADDR8 array
                | 791    // MONEY array
                | 1561   // BIT array
                | 1563   // VARBIT array
                | 2201   // REFCURSOR array
                | 2207..=2211 // REG* type arrays
                | 2949   // TXID_SNAPSHOT array
                | 2951   // UUID array
                | 3643   // TSVECTOR array
                | 3644   // GTSVECTOR array
                | 3645   // TSQUERY array
                | 3735   // REGCONFIG array
                | 3770   // REGDICTIONARY array
                | 3807   // JSONB array
                | 3905   // INT4RANGE array
                | 3907   // NUMRANGE array
                | 3909   // TSRANGE array
                | 3911   // TSTZRANGE array
                | 3913   // DATERANGE array
                | 3927   // INT8RANGE array
                | 4090   // REGNAMESPACE array
                | 4097   // REGROLE array
        )
    }

    /// Map an array type OID to its element type OID.
    /// Returns 0 (unknown) for unrecognized array types.
    pub const fn array_element_type(oid: i32) -> i32 {
        match oid {
            BOOL_ARRAY => BOOL,
            BYTEA_ARRAY => BYTEA,
            CHAR_ARRAY => CHAR,
            NAME_ARRAY => NAME,
            INT2_ARRAY => INT2,
            INT4_ARRAY => INT4,
            INT8_ARRAY => INT8,
            TEXT_ARRAY => TEXT,
            OID_ARRAY => OID,
            FLOAT4_ARRAY => FLOAT4,
            FLOAT8_ARRAY => FLOAT8,
            MONEY_ARRAY => MONEY,
            VARCHAR_ARRAY => VARCHAR,
            DATE_ARRAY => DATE,
            TIME_ARRAY => TIME,
            TIMESTAMP_ARRAY => TIMESTAMP,
            TIMESTAMPTZ_ARRAY => TIMESTAMPTZ,
            INTERVAL_ARRAY => INTERVAL,
            NUMERIC_ARRAY => NUMERIC,
            UUID_ARRAY => UUID,
            JSON_ARRAY => JSON,
            JSONB_ARRAY => JSONB,
            INET_ARRAY => INET,
            CIDR_ARRAY => CIDR,
            MACADDR_ARRAY => MACADDR,
            _ => 0, // unknown element type — treat as text
        }
    }
}

/// Protocol version constants.
pub mod protocol {
    /// Protocol version 3.0 (PostgreSQL 7.4 through 17)
    pub const VERSION_3_0: i32 = 196608; // (3 << 16) | 0

    /// Protocol version 3.2 (PostgreSQL 18+)
    /// Introduces variable-length cancel keys for improved security.
    pub const VERSION_3_2: i32 = 196610; // (3 << 16) | 2

    /// SSL request code
    pub const SSL_REQUEST: i32 = 80877103;

    /// Cancel request code (Protocol 3.0)
    pub const CANCEL_REQUEST: i32 = 80877102;

    /// Cancel request code (Protocol 3.2, PostgreSQL 18+)
    /// Uses variable-length cancel key instead of fixed 4-byte key.
    pub const CANCEL_REQUEST_V2: i32 = 80877105;

    /// GSS encryption request code
    pub const GSSENC_REQUEST: i32 = 80877104;

    /// Check if a protocol version supports variable-length cancel keys.
    pub const fn supports_variable_cancel_key(version: i32) -> bool {
        let minor = version & 0xFFFF;
        minor >= 2
    }
}
