//! Oracle TTI (Two-Task Interface) protocol.
//!
//! TTI is Oracle's application-layer protocol that runs on top of TNS Data packets.
//! It handles database operations like queries, DML, authentication, and session management.
//!
//! # Structure
//!
//! TTI messages are encapsulated in TNS Data packets. The first byte of the
//! Data packet payload indicates the TTI function code.
//!
//! # Function Categories
//!
//! - **Session Management**: Login, logout, authentication
//! - **Query Operations**: Execute, fetch, describe
//! - **Transaction Control**: Commit, rollback
//! - **LOB Operations**: LOB read, write, create
//! - **Cursor Management**: Open, close, parse
//!
//! # Example
//!
//! ```rust,ignore
//! use oracle_wire::types::tti::{TtiMessage, function_codes};
//!
//! // Parse a TTI message from a Data packet payload
//! let function_code = payload[0];
//! match function_code {
//!     function_codes::QUERY => { /* handle query */ }
//!     function_codes::EXECUTE => { /* handle execute */ }
//!     _ => { /* unknown function */ }
//! }
//! ```

pub mod auth;
pub mod bind;
pub mod charset;
pub mod collection;
pub mod column;
pub mod compression;
pub mod crypto;
pub mod data_types;
pub mod datetime;
pub mod error;
pub mod function_codes;
pub mod limits;
pub mod lob;
pub mod message;
pub mod number;
pub mod oson;
pub mod protocol;
pub mod row;
pub mod session;
pub mod statement;

pub use auth::{AuthChallenge, AuthComputeError, AuthFlags, AuthMethod, AuthParseError, AuthProtocol, AuthRequest, AuthResponse};
pub use bind::{
    ArrayBindDescriptor, ArrayBindSet, ArrayBindType, ArrayBindValue, BindDescriptor, BindDirection, BindError, BindSet, BindValue,
    MAX_ARRAY_SIZE,
};
pub use charset::{
    CharsetConfig, CharsetError, CharsetId, NCharsetId, decode_string, decode_string_lossy, encode_string, estimate_encoded_length,
    is_valid_utf8, max_char_bytes,
};
pub use collection::{CollectionElement, CollectionError, CollectionType, NestedTable, OracleCollection, Varray, parse_collection};
pub use column::{ColumnInfo, MetadataBuilder, ResultSetMetadata};
pub use compression::{
    CompressionError, CompressionStats, MAX_DECOMPRESSED_SIZE, MIN_COMPRESS_SIZE, compress, compress_if_beneficial, compress_raw,
    decompress, decompress_raw, is_zlib_compressed,
};
pub use crypto::{
    AES_BLOCK_SIZE, AES128_KEY_SIZE, AES256_KEY_SIZE, AuthVerifier, CryptoError, SessionCipher, aes128_cbc_decrypt, aes128_cbc_encrypt,
    aes256_cbc_decrypt, aes256_cbc_encrypt, combine_session_keys, compute_o8logon_verifier, compute_o9logon_verifier, decode_hex_auth_data,
    derive_session_encryption_key_sha1, derive_session_encryption_key_sha256, encode_hex_auth_data, pkcs7_pad, pkcs7_unpad,
    sha1_password_hash, sha256_password_hash, xor_bytes,
};
pub use data_types::{OracleDataType, OracleVersion, TypeDescriptor, charset_form, json_type_codes};
pub use datetime::{DateTimeParseError, OracleDate, OracleIntervalDs, OracleIntervalYm, OracleTimestamp, OracleTimestampTz};
pub use error::{
    ErrorResponseBuilder, ErrorSeverity, OracleError, OracleWarning, ParseErrorError, TtiResponse, codes as error_codes,
    parse_error_response, parse_warning_response,
};
pub use function_codes::FunctionCode;
pub use limits::{
    LimitError, MAX_BIND_VARIABLES, MAX_CHAR_BYTES, MAX_COLUMNS, MAX_FETCH_SIZE, MAX_IDENTIFIER_LENGTH, MAX_LOB_BYTES, MAX_LOB_CHUNK_SIZE,
    MAX_NANOSECONDS, MAX_NUMBER_PRECISION, MAX_NUMBER_SCALE, MAX_RAW_BYTES, MAX_SQL_LENGTH, MAX_VARCHAR2_BYTES, MAX_YEAR, MIN_NUMBER_SCALE,
    MIN_YEAR, days_in_month, is_leap_year, validate_bind_count, validate_char_size, validate_column_count, validate_date,
    validate_datetime, validate_fetch_size, validate_identifier, validate_lob_chunk_size, validate_lob_offset, validate_nanoseconds,
    validate_number_precision, validate_number_scale, validate_raw_size, validate_sql_length, validate_time, validate_timestamp,
    validate_timestamp_tz, validate_timezone, validate_varchar2_size,
};
pub use lob::{
    LobAppendRequest, LobCloseRequest, LobCopyRequest, LobCreateTempRequest, LobCreateTempResponse, LobEraseRequest, LobEraseResponse,
    LobError, LobFreeTempRequest, LobGetCharsetIdRequest, LobGetCharsetIdResponse, LobGetChunkSizeRequest, LobGetChunkSizeResponse,
    LobGetLengthRequest, LobGetLengthResponse, LobIsOpenRequest, LobIsOpenResponse, LobIsTempRequest, LobIsTempResponse,
    LobLoadFromFileRequest, LobOpenMode, LobOpenRequest, LobOperation, LobPiecewiseReadRequest, LobPiecewiseReadResponse,
    LobPiecewiseReader, LobPiecewiseState, LobPiecewiseWriteRequest, LobPiecewiseWriteResponse, LobPiecewiseWriter, LobReadRequest,
    LobReadResponse, LobStream, LobTrimRequest, LobType, LobWriteRequest, LobWriteResponse, PieceType, TempLobDuration, split_into_chunks,
};
pub use message::{TtiMessage, TtiMessageBuilder, TtiMessageError};
pub use number::{NumberParseError, OracleNumber};
pub use oson::{
    MAX_CONTAINER_SIZE, MAX_NESTING_DEPTH, OSON_MAGIC, OsonBuilder, OsonError, OsonHeader, OsonParser, OsonType, OsonValue, OsonVersion,
    is_oson, parse_oson,
};
pub use protocol::{
    DataTypeNegotiationRequest, DataTypeNegotiationResponse, NegotiatedProtocol, ProtocolError, ProtocolNegotiationRequest,
    ProtocolNegotiationResponse, VersionComponents, VersionRequest, VersionResponse, capabilities,
};
pub use row::{BfileLocator, ColumnValue, JsonValue, LobLocator, ObjectValue, ResultSet, Row, RowDecoder, RowParseError};
pub use session::{
    Capability, ConnectionInfo, DrcpPurity, DrcpState, DrpcReleaseMode, DrpcReleaseRequest, Feature, GetServerInfoRequest,
    GetServerInfoResponse, ProtocolVersion, ServerInfoCategory, ServerInfoError, ServerType, SessionAttribute, SessionCapabilities,
    SessionState, SessionStatePiggybackRequest, StateConsistency, TransactionState,
};
pub use statement::{
    BatchError, BatchExecuteOptions, BatchExecuteRequest, BatchExecuteResponse, BindDescriptor as StmtBindDescriptor, CloseRequest,
    ColumnDescriptor, CommitRequest, Cursor, CursorState, DescribeRequest, DescribeResponse, ExecuteAndFetchRequest,
    ExecuteAndFetchResponse, ExecuteOptions, ExecuteRequest, ExecuteResponse, ExecuteWithReturningOptions, ExecuteWithReturningResponse,
    FetchRequest, FetchResponse, GetStatementByTagRequest, GetStatementByTagResponse, ImplicitResultCursor, ImplicitResultsResponse,
    OpenCursorRequest, OpenCursorResponse, ParseRequest, ParseResponse, ReturningBindBuilder, ReturningClauseResult,
    ReturningColumnDescriptor, ReturningRow, ReturningValue, RollbackRequest, ScrollDirection, ScrollFetchRequest, ScrollFetchResponse,
    ScrollMode, ScrollableCursor, SetStatementTagRequest, StatementCacheEntry, StatementError, StatementType, TransactionBeginRequest,
    TransactionIsolation,
};
