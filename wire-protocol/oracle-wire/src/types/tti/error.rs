//! TTI error responses.
//!
//! This module handles Oracle error responses in the TTI protocol.
//! Oracle errors follow the ORA-XXXXX format where XXXXX is the error code.

use std::fmt;

/// Oracle error code with optional message.
///
/// Oracle errors are identified by a numeric code (e.g., ORA-01017, ORA-00942).
/// The error code determines the error category and specific issue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleError {
    /// The numeric error code (e.g., 1017 for "invalid username/password").
    pub code: u32,
    /// The error message from the server.
    pub message: String,
    /// SQL offset where the error occurred (for syntax errors).
    pub sql_offset: Option<u32>,
    /// Row offset for batch operations.
    pub row_offset: Option<u32>,
}

impl OracleError {
    /// Create a new Oracle error.
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            sql_offset: None,
            row_offset: None,
        }
    }

    /// Set the SQL offset.
    pub fn with_sql_offset(mut self, offset: u32) -> Self {
        self.sql_offset = Some(offset);
        self
    }

    /// Set the row offset.
    pub fn with_row_offset(mut self, offset: u32) -> Self {
        self.row_offset = Some(offset);
        self
    }

    /// Get the error code as an ORA-XXXXX string.
    pub fn ora_code(&self) -> String {
        format!("ORA-{:05}", self.code)
    }

    /// Check if this is a specific error code.
    pub fn is_code(&self, code: u32) -> bool {
        self.code == code
    }

    /// Check if this is a connection-related error.
    pub fn is_connection_error(&self) -> bool {
        matches!(
            self.code,
            codes::TNS_LISTENER_NOT_RUNNING
                | codes::TNS_NO_LISTENER
                | codes::TNS_CONNECT_TIMEOUT
                | codes::TNS_PACKET_WRITER_FAILURE
                | codes::CONNECTION_CLOSED
        )
    }

    /// Check if this is an authentication error.
    pub fn is_auth_error(&self) -> bool {
        matches!(
            self.code,
            codes::INVALID_USERNAME_PASSWORD | codes::ACCOUNT_LOCKED | codes::PASSWORD_EXPIRED | codes::PASSWORD_WILL_EXPIRE
        )
    }

    /// Check if this is a constraint violation.
    pub fn is_constraint_violation(&self) -> bool {
        matches!(
            self.code,
            codes::UNIQUE_CONSTRAINT_VIOLATED
                | codes::INTEGRITY_CONSTRAINT_VIOLATED
                | codes::CHECK_CONSTRAINT_VIOLATED
                | codes::NOT_NULL_VIOLATION
        )
    }

    /// Check if this error is recoverable (can retry).
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self.code,
            codes::DEADLOCK_DETECTED | codes::RESOURCE_BUSY | codes::SNAPSHOT_TOO_OLD | codes::FETCH_OUT_OF_SEQUENCE
        )
    }

    /// Check if this error is transient and should be retried immediately.
    ///
    /// These are errors that may resolve on their own without any action.
    pub fn is_transient(&self) -> bool {
        matches!(
            self.code,
            codes::DEADLOCK_DETECTED
                | codes::RESOURCE_BUSY
                | codes::SNAPSHOT_TOO_OLD
                | codes::CANNOT_SERIALIZE_ACCESS
                | codes::TIMEOUT_WAITING_FOR_RESOURCE
        )
    }

    /// Check if this error requires reconnection to resolve.
    ///
    /// After these errors, the connection should be discarded and a new one established.
    pub fn requires_reconnect(&self) -> bool {
        matches!(
            self.code,
            codes::CONNECTION_CLOSED // Also covers EOF_ON_CHANNEL (same code 3113)
                | codes::NOT_CONNECTED
                | codes::SESSION_KILLED
                | codes::TNS_PACKET_WRITER_FAILURE
                | codes::CONNECTION_RESET
                | codes::BROKEN_PIPE
        )
    }

    /// Check if this is a data-related error (invalid input).
    pub fn is_data_error(&self) -> bool {
        matches!(
            self.code,
            codes::INVALID_NUMBER
                | codes::VALUE_TOO_LARGE
                | codes::NUMERIC_OVERFLOW
                | codes::NOT_VALID_MONTH
                | codes::DIVISION_BY_ZERO
                | codes::INVALID_DATE
                | codes::INVALID_YEAR
        )
    }

    /// Check if this is a permission/authorization error.
    ///
    /// Note: TABLE_NOT_FOUND (ORA-00942) can also indicate a permissions issue
    /// when the object exists but the user lacks access.
    pub fn is_permission_error(&self) -> bool {
        matches!(
            self.code,
            codes::INSUFFICIENT_PRIVILEGES | codes::TABLE_NOT_FOUND // May be due to permissions (also covers NO_ACCESS_TO_OBJECT)
        )
    }

    /// Check if this is a resource limit error.
    pub fn is_resource_limit_error(&self) -> bool {
        matches!(
            self.code,
            codes::EXCEEDED_MAX_OPEN_CURSORS | codes::OUT_OF_MEMORY | codes::EXCEEDED_MAX_PROCESSES | codes::EXCEEDED_MAX_SESSIONS
        )
    }

    /// Check if this is a JSON-related error (Oracle 12c+).
    pub fn is_json_error(&self) -> bool {
        matches!(
            self.code,
            codes::JSON_SYNTAX_ERROR | codes::JSON_PATH_ERROR | codes::JSON_NOT_SCALAR | codes::JSON_TOO_LARGE
        )
    }

    /// Check if this is a protocol/network error.
    pub fn is_protocol_error(&self) -> bool {
        matches!(
            self.code,
            codes::PROTOCOL_ERROR | codes::INTERNAL_PROTOCOL_ERROR | codes::CHECKSUM_MISMATCH | codes::PACKET_INTEGRITY_FAILED
        )
    }

    /// Check if this is a cursor/statement cache error.
    pub fn is_cursor_error(&self) -> bool {
        matches!(
            self.code,
            codes::STATEMENT_NOT_IN_CACHE
                | codes::CURSOR_INVALIDATED
                | codes::EXCEEDED_MAX_OPEN_CURSORS
                | codes::IMPLICIT_CURSOR_NOT_FOUND
                | codes::TOO_MANY_IMPLICIT_RESULTS
        )
    }

    /// Check if the cursor was invalidated and statement needs to be re-parsed.
    pub fn cursor_needs_reparse(&self) -> bool {
        self.code == codes::CURSOR_INVALIDATED
    }

    /// Get the recommended retry delay for this error.
    ///
    /// Returns `None` if the error should not be retried.
    /// Returns `Some(Duration)` with the recommended wait time before retrying.
    pub fn retry_delay(&self) -> Option<std::time::Duration> {
        use std::time::Duration;

        match self.code {
            // Immediate retry for transient errors
            codes::DEADLOCK_DETECTED => Some(Duration::from_millis(10)),
            // Short wait for resource contention
            codes::RESOURCE_BUSY => Some(Duration::from_millis(100)),
            codes::TIMEOUT_WAITING_FOR_RESOURCE => Some(Duration::from_millis(500)),
            // Medium wait for serialization failures
            codes::CANNOT_SERIALIZE_ACCESS => Some(Duration::from_millis(250)),
            codes::SNAPSHOT_TOO_OLD => Some(Duration::from_millis(500)),
            // Longer wait for connection issues (exponential backoff should be applied)
            codes::TNS_CONNECT_TIMEOUT => Some(Duration::from_secs(1)),
            codes::TNS_NO_LISTENER => Some(Duration::from_secs(2)),
            codes::TNS_LISTENER_NOT_RUNNING => Some(Duration::from_secs(2)),
            // Not retryable
            _ => None,
        }
    }

    /// Get the error severity level.
    pub fn severity(&self) -> ErrorSeverity {
        if self.is_auth_error() {
            ErrorSeverity::Fatal
        } else if self.requires_reconnect() {
            ErrorSeverity::ConnectionLost
        } else if self.is_transient() {
            ErrorSeverity::Transient
        } else if self.is_constraint_violation() || self.is_data_error() {
            ErrorSeverity::UserError
        } else if self.is_resource_limit_error() {
            ErrorSeverity::ResourceLimit
        } else {
            ErrorSeverity::Unknown
        }
    }
}

/// Error severity levels for categorizing how to handle errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorSeverity {
    /// Transient error that may resolve on retry.
    Transient,
    /// Error caused by invalid user input.
    UserError,
    /// Connection has been lost and needs to be re-established.
    ConnectionLost,
    /// Resource limit has been reached.
    ResourceLimit,
    /// Fatal error that cannot be recovered from (e.g., auth failure).
    Fatal,
    /// Unknown error type.
    Unknown,
}

impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.ora_code(), self.message)
    }
}

impl std::error::Error for OracleError {}

/// Common Oracle error codes.
pub mod codes {
    // Connection errors (12xxx and 3xxx)
    /// TNS: listener does not currently know of service requested.
    pub const TNS_LISTENER_NOT_RUNNING: u32 = 12514;
    /// TNS: no listener.
    pub const TNS_NO_LISTENER: u32 = 12541;
    /// TNS: connect timeout occurred.
    pub const TNS_CONNECT_TIMEOUT: u32 = 12170;
    /// TNS: packet writer failure.
    pub const TNS_PACKET_WRITER_FAILURE: u32 = 12571;
    /// Connection closed / End of file on communication channel.
    pub const CONNECTION_CLOSED: u32 = 3113;
    /// TNS: connection reset.
    pub const CONNECTION_RESET: u32 = 12537;
    /// TNS: send timeout.
    pub const TNS_SEND_TIMEOUT: u32 = 12608;
    /// TNS: receive timeout.
    pub const TNS_RECEIVE_TIMEOUT: u32 = 12609;
    /// Broken pipe.
    pub const BROKEN_PIPE: u32 = 3135;

    // Authentication errors
    /// Invalid username/password; logon denied.
    pub const INVALID_USERNAME_PASSWORD: u32 = 1017;
    /// The account is locked.
    pub const ACCOUNT_LOCKED: u32 = 28000;
    /// Password has expired.
    pub const PASSWORD_EXPIRED: u32 = 28001;
    /// Password will expire soon.
    pub const PASSWORD_WILL_EXPIRE: u32 = 28002;

    // SQL errors
    /// Table or view does not exist.
    pub const TABLE_NOT_FOUND: u32 = 942;
    /// Invalid identifier.
    pub const INVALID_IDENTIFIER: u32 = 904;
    /// Column not allowed here.
    pub const COLUMN_NOT_ALLOWED: u32 = 984;
    /// Missing expression.
    pub const MISSING_EXPRESSION: u32 = 936;
    /// Invalid number.
    pub const INVALID_NUMBER: u32 = 1722;
    /// Not a valid month.
    pub const NOT_VALID_MONTH: u32 = 1843;

    // Date/time errors
    /// Invalid date format.
    pub const INVALID_DATE: u32 = 1858;
    /// Invalid year.
    pub const INVALID_YEAR: u32 = 1841;
    /// Day of month must be between 1 and last day of month.
    pub const INVALID_DAY_OF_MONTH: u32 = 1847;
    /// Hour must be between 0 and 23.
    pub const INVALID_HOUR: u32 = 1850;
    /// Minutes must be between 0 and 59.
    pub const INVALID_MINUTE: u32 = 1851;
    /// Seconds must be between 0 and 59.
    pub const INVALID_SECOND: u32 = 1852;

    // Constraint violations
    /// Unique constraint violated.
    pub const UNIQUE_CONSTRAINT_VIOLATED: u32 = 1;
    /// Integrity constraint violated - parent key not found.
    pub const INTEGRITY_CONSTRAINT_VIOLATED: u32 = 2291;
    /// Integrity constraint violated - child record found.
    pub const CHILD_RECORD_FOUND: u32 = 2292;
    /// Check constraint violated.
    pub const CHECK_CONSTRAINT_VIOLATED: u32 = 2290;
    /// Cannot insert NULL.
    pub const NOT_NULL_VIOLATION: u32 = 1400;

    // Transaction/Lock errors
    /// Deadlock detected.
    pub const DEADLOCK_DETECTED: u32 = 60;
    /// Resource busy and acquire with NOWAIT specified.
    pub const RESOURCE_BUSY: u32 = 54;
    /// Snapshot too old.
    pub const SNAPSHOT_TOO_OLD: u32 = 1555;
    /// Fetch out of sequence.
    pub const FETCH_OUT_OF_SEQUENCE: u32 = 1002;
    /// Cannot serialize access for this transaction.
    pub const CANNOT_SERIALIZE_ACCESS: u32 = 8177;
    /// Timeout waiting for resource.
    pub const TIMEOUT_WAITING_FOR_RESOURCE: u32 = 30006;

    // Data errors
    /// Value too large for column.
    pub const VALUE_TOO_LARGE: u32 = 12899;
    /// Numeric overflow.
    pub const NUMERIC_OVERFLOW: u32 = 1426;
    /// Division by zero.
    pub const DIVISION_BY_ZERO: u32 = 1476;
    /// Character string buffer too small.
    pub const STRING_BUFFER_TOO_SMALL: u32 = 6502;

    // LOB errors
    /// LOB locator is invalid.
    pub const INVALID_LOB_LOCATOR: u32 = 22275;
    /// Cannot perform LOB operation inside a query.
    pub const LOB_OP_IN_QUERY: u32 = 22289;
    /// LOB read/write failed.
    pub const LOB_READ_WRITE_ERROR: u32 = 22922;
    /// Specified buffer too small for LOB.
    pub const LOB_BUFFER_TOO_SMALL: u32 = 22993;

    // Session errors
    /// Session killed.
    pub const SESSION_KILLED: u32 = 28;
    /// Not connected to Oracle.
    pub const NOT_CONNECTED: u32 = 3114;
    /// End of file on communication channel (same as CONNECTION_CLOSED).
    /// Note: This is intentionally the same value as CONNECTION_CLOSED (3113).
    pub const EOF_ON_CHANNEL: u32 = CONNECTION_CLOSED;
    /// Not logged on.
    pub const NOT_LOGGED_ON: u32 = 1012;

    // Permission errors
    /// Insufficient privileges.
    pub const INSUFFICIENT_PRIVILEGES: u32 = 1031;
    /// Execute on procedure was not granted.
    pub const EXECUTE_NOT_GRANTED: u32 = 6550;

    // Resource limit errors
    /// Maximum open cursors exceeded.
    pub const EXCEEDED_MAX_OPEN_CURSORS: u32 = 1000;
    /// Out of memory.
    pub const OUT_OF_MEMORY: u32 = 4031;
    /// Maximum processes exceeded.
    pub const EXCEEDED_MAX_PROCESSES: u32 = 20;
    /// Maximum sessions exceeded.
    pub const EXCEEDED_MAX_SESSIONS: u32 = 18;
    /// Tablespace quota exceeded.
    pub const TABLESPACE_QUOTA_EXCEEDED: u32 = 1536;
    /// Cannot extend rollback segment.
    pub const CANNOT_EXTEND_ROLLBACK: u32 = 1562;

    // PL/SQL errors
    /// User-defined exception.
    pub const USER_DEFINED_EXCEPTION: u32 = 6510;
    /// PL/SQL numeric or value error.
    pub const PLSQL_VALUE_ERROR: u32 = 6502;
    /// No data found.
    pub const NO_DATA_FOUND: u32 = 1403;
    /// Too many rows.
    pub const TOO_MANY_ROWS: u32 = 1422;

    // JSON errors (Oracle 12c+)
    /// JSON syntax error.
    pub const JSON_SYNTAX_ERROR: u32 = 40441;
    /// JSON path expression syntax error.
    pub const JSON_PATH_ERROR: u32 = 40442;
    /// JSON value is not a scalar.
    pub const JSON_NOT_SCALAR: u32 = 40456;
    /// JSON data too large.
    pub const JSON_TOO_LARGE: u32 = 40478;

    // Extended type errors (Oracle 12c+)
    /// Extended VARCHAR2/NVARCHAR2/RAW length exceeded.
    pub const EXTENDED_TYPE_LENGTH_EXCEEDED: u32 = 910;
    /// Identity column sequence exhausted.
    pub const IDENTITY_EXHAUSTED: u32 = 30667;

    // Flashback errors
    /// Flashback query results in snapshot too old.
    pub const FLASHBACK_SNAPSHOT_TOO_OLD: u32 = 8181;
    /// Flashback operation failed.
    pub const FLASHBACK_FAILED: u32 = 55509;

    // Parallel execution errors
    /// Parallel execution server unavailable.
    pub const PARALLEL_SERVER_UNAVAIL: u32 = 12801;
    /// Parallel execution limit exceeded.
    pub const PARALLEL_LIMIT_EXCEEDED: u32 = 12827;

    // Cursor/Statement cache errors
    /// Statement ID not found in cache.
    pub const STATEMENT_NOT_IN_CACHE: u32 = 1006;
    /// Cursor invalid due to DDL.
    pub const CURSOR_INVALIDATED: u32 = 1007;
    /// Maximum statement length exceeded.
    pub const SQL_TOO_LONG: u32 = 1704;

    // Network/protocol errors (Oracle 12c+)
    /// Protocol error during execution.
    pub const PROTOCOL_ERROR: u32 = 3106;
    /// Internal protocol error.
    pub const INTERNAL_PROTOCOL_ERROR: u32 = 600;
    /// Checksum mismatch.
    pub const CHECKSUM_MISMATCH: u32 = 3150;
    /// Packet integrity check failed.
    pub const PACKET_INTEGRITY_FAILED: u32 = 12152;

    // Autonomous Database specific (Oracle 18c+)
    /// Auto-scaling limit reached.
    pub const AUTO_SCALING_LIMIT: u32 = 64000;
    /// Service unavailable (maintenance).
    pub const SERVICE_MAINTENANCE: u32 = 64001;
    /// CPU quota exceeded.
    pub const CPU_QUOTA_EXCEEDED: u32 = 64002;

    // Implicit results errors (Oracle 12c+)
    /// Implicit result cursor not found.
    pub const IMPLICIT_CURSOR_NOT_FOUND: u32 = 8108;
    /// Too many implicit results.
    pub const TOO_MANY_IMPLICIT_RESULTS: u32 = 8109;

    // BOOLEAN type errors (Oracle 23c+)
    /// Boolean value expected.
    pub const BOOLEAN_EXPECTED: u32 = 1830;
    /// Cannot convert to boolean.
    pub const BOOLEAN_CONVERSION_ERROR: u32 = 1831;
}

/// Parse an Oracle error from TTI response data.
///
/// TTI error responses typically contain:
/// - Error code (4 bytes, can be negative for internal errors)
/// - Message length (2 bytes)
/// - Error message (variable)
/// - Optional: SQL offset, row offset
pub fn parse_error_response(data: &[u8]) -> Result<OracleError, ParseErrorError> {
    if data.len() < 6 {
        return Err(ParseErrorError::TooShort);
    }

    // Error code is typically a signed i32, but we store as u32
    // Safety: we checked data.len() >= 6 above, so this slice is valid
    let code_bytes: [u8; 4] = data[0..4].try_into().expect("slice length checked above");
    let code = i32::from_be_bytes(code_bytes).unsigned_abs();

    let msg_len = u16::from_be_bytes([data[4], data[5]]) as usize;

    if data.len() < 6 + msg_len {
        return Err(ParseErrorError::MessageTruncated);
    }

    let message = String::from_utf8_lossy(&data[6..6 + msg_len]).into_owned();

    let mut error = OracleError::new(code, message);

    // Check for additional fields after message
    let remaining = &data[6 + msg_len..];
    if remaining.len() >= 4 {
        // Safety: we just checked remaining.len() >= 4
        let sql_offset = u32::from_be_bytes(remaining[0..4].try_into().expect("slice length checked above"));
        if sql_offset > 0 {
            error = error.with_sql_offset(sql_offset);
        }
    }

    Ok(error)
}

/// Error when parsing an error response.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ParseErrorError {
    #[error("error response too short")]
    TooShort,
    #[error("error message truncated")]
    MessageTruncated,
}

/// Oracle warning (similar structure to error but non-fatal).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleWarning {
    /// The warning code.
    pub code: u32,
    /// The warning message.
    pub message: String,
}

impl OracleWarning {
    /// Create a new warning.
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }

    /// Get the warning code as an ORA-XXXXX string.
    pub fn ora_code(&self) -> String {
        format!("ORA-{:05}", self.code)
    }
}

impl fmt::Display for OracleWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Warning {}: {}", self.ora_code(), self.message)
    }
}

/// Parse a warning response from TTI data.
pub fn parse_warning_response(data: &[u8]) -> Result<OracleWarning, ParseErrorError> {
    if data.len() < 6 {
        return Err(ParseErrorError::TooShort);
    }

    let code = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let msg_len = u16::from_be_bytes([data[4], data[5]]) as usize;

    if data.len() < 6 + msg_len {
        return Err(ParseErrorError::MessageTruncated);
    }

    let message = String::from_utf8_lossy(&data[6..6 + msg_len]).into_owned();

    Ok(OracleWarning::new(code, message))
}

/// TTI response types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TtiResponse {
    /// Success response (operation completed).
    Success,
    /// Error response.
    Error(OracleError),
    /// Warning response (operation completed with warning).
    Warning(OracleWarning),
    /// Row data follows.
    RowData,
    /// End of data (no more rows).
    EndOfData,
}

impl TtiResponse {
    /// Parse a TTI response from wire data.
    ///
    /// The first byte indicates the response type:
    /// - 0x00: Success
    /// - 0x04: Error (followed by error data)
    /// - 0x05: Warning (followed by warning data)
    /// - 0x06: Row data follows
    /// - 0x08: End of data
    pub fn parse(data: &[u8]) -> Result<Self, ParseErrorError> {
        if data.is_empty() {
            return Err(ParseErrorError::TooShort);
        }

        match data[0] {
            0x00 => Ok(Self::Success),
            0x04 => {
                let error = parse_error_response(&data[1..])?;
                Ok(Self::Error(error))
            }
            0x05 => {
                if data.len() > 1 {
                    let warning = parse_warning_response(&data[1..])?;
                    Ok(Self::Warning(warning))
                } else {
                    // Empty warning
                    Ok(Self::Warning(OracleWarning::new(0, "")))
                }
            }
            0x06 => Ok(Self::RowData),
            0x08 => Ok(Self::EndOfData),
            _ => {
                // Unknown response type - treat as error
                if data.len() >= 5 {
                    let error = parse_error_response(data)?;
                    Ok(Self::Error(error))
                } else {
                    Err(ParseErrorError::TooShort)
                }
            }
        }
    }

    /// Check if this is a success response.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }

    /// Check if this is an error response.
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }

    /// Check if this is a warning response.
    pub fn is_warning(&self) -> bool {
        matches!(self, Self::Warning(_))
    }

    /// Check if more data is expected.
    pub fn has_more_data(&self) -> bool {
        matches!(self, Self::RowData)
    }

    /// Get the error if this is an error response.
    pub fn error(&self) -> Option<&OracleError> {
        match self {
            Self::Error(e) => Some(e),
            _ => None,
        }
    }

    /// Get the warning if this is a warning response.
    pub fn warning(&self) -> Option<&OracleWarning> {
        match self {
            Self::Warning(w) => Some(w),
            _ => None,
        }
    }

    /// Convert to Result, returning error if this is an error response.
    pub fn into_result(self) -> Result<Self, OracleError> {
        match self {
            Self::Error(e) => Err(e),
            other => Ok(other),
        }
    }
}

/// Builder for creating error responses (for testing or server-side).
#[derive(Clone, Debug)]
pub struct ErrorResponseBuilder {
    code: u32,
    message: String,
    sql_offset: Option<u32>,
    row_offset: Option<u32>,
}

impl ErrorResponseBuilder {
    /// Create a new builder.
    pub fn new(code: u32) -> Self {
        Self {
            code,
            message: String::new(),
            sql_offset: None,
            row_offset: None,
        }
    }

    /// Set the message.
    pub fn message(mut self, msg: impl Into<String>) -> Self {
        self.message = msg.into();
        self
    }

    /// Set the SQL offset.
    pub fn sql_offset(mut self, offset: u32) -> Self {
        self.sql_offset = Some(offset);
        self
    }

    /// Set the row offset.
    pub fn row_offset(mut self, offset: u32) -> Self {
        self.row_offset = Some(offset);
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let msg_bytes = self.message.as_bytes();
        let mut buf = Vec::with_capacity(6 + msg_bytes.len() + 8);

        // Response type (error)
        buf.push(0x04);

        // Error code (signed i32, but we use u32)
        buf.extend_from_slice(&(self.code as i32).to_be_bytes());

        // Message length and message
        buf.extend_from_slice(&(msg_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(msg_bytes);

        // SQL offset (if present)
        if let Some(offset) = self.sql_offset {
            buf.extend_from_slice(&offset.to_be_bytes());
        }

        // Row offset (if present)
        if let Some(offset) = self.row_offset {
            buf.extend_from_slice(&offset.to_be_bytes());
        }

        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ora_code_formatting() {
        let err = OracleError::new(942, "table or view does not exist");
        assert_eq!(err.ora_code(), "ORA-00942");

        let err = OracleError::new(12514, "TNS:listener does not currently know");
        assert_eq!(err.ora_code(), "ORA-12514");
    }

    #[test]
    fn test_error_categories() {
        let auth_err = OracleError::new(codes::INVALID_USERNAME_PASSWORD, "invalid login");
        assert!(auth_err.is_auth_error());
        assert!(!auth_err.is_connection_error());

        let conn_err = OracleError::new(codes::TNS_NO_LISTENER, "no listener");
        assert!(conn_err.is_connection_error());
        assert!(!conn_err.is_auth_error());

        let constraint_err = OracleError::new(codes::UNIQUE_CONSTRAINT_VIOLATED, "unique key");
        assert!(constraint_err.is_constraint_violation());

        let deadlock = OracleError::new(codes::DEADLOCK_DETECTED, "deadlock");
        assert!(deadlock.is_recoverable());
    }

    #[test]
    fn test_display() {
        let err = OracleError::new(1017, "invalid username/password; logon denied");
        assert_eq!(err.to_string(), "ORA-01017: invalid username/password; logon denied");
    }

    #[test]
    fn test_parse_error_response() {
        // Build a test error response
        let mut data = Vec::new();
        data.extend_from_slice(&942i32.to_be_bytes()); // error code
        data.extend_from_slice(&13u16.to_be_bytes()); // message length
        data.extend_from_slice(b"table missing"); // message

        let error = parse_error_response(&data).unwrap();
        assert_eq!(error.code, 942);
        assert_eq!(error.message, "table missing");
    }

    #[test]
    fn test_parse_error_with_offset() {
        let mut data = Vec::new();
        data.extend_from_slice(&936i32.to_be_bytes()); // missing expression
        data.extend_from_slice(&7u16.to_be_bytes()); // message length
        data.extend_from_slice(b"missing"); // message
        data.extend_from_slice(&15u32.to_be_bytes()); // SQL offset

        let error = parse_error_response(&data).unwrap();
        assert_eq!(error.code, 936);
        assert_eq!(error.sql_offset, Some(15));
    }

    #[test]
    fn test_tti_response_success() {
        let data = [0x00]; // success
        let resp = TtiResponse::parse(&data).unwrap();
        assert!(resp.is_success());
        assert!(!resp.is_error());
    }

    #[test]
    fn test_tti_response_error() {
        let mut data = vec![0x04]; // error type
        data.extend_from_slice(&1017i32.to_be_bytes()); // error code
        data.extend_from_slice(&5u16.to_be_bytes()); // message length
        data.extend_from_slice(b"login"); // message

        let resp = TtiResponse::parse(&data).unwrap();
        assert!(resp.is_error());
        let err = resp.error().unwrap();
        assert_eq!(err.code, 1017);
    }

    #[test]
    fn test_tti_response_end_of_data() {
        let data = [0x08]; // end of data
        let resp = TtiResponse::parse(&data).unwrap();
        assert!(matches!(resp, TtiResponse::EndOfData));
        assert!(!resp.has_more_data());
    }

    #[test]
    fn test_tti_response_row_data() {
        let data = [0x06]; // row data
        let resp = TtiResponse::parse(&data).unwrap();
        assert!(resp.has_more_data());
    }

    #[test]
    fn test_error_response_builder() {
        let encoded = ErrorResponseBuilder::new(942).message("table not found").sql_offset(10).encode();

        // First byte should be 0x04 (error type)
        assert_eq!(encoded[0], 0x04);

        // Parse it back (skip the first byte since that's the type marker)
        let error = parse_error_response(&encoded[1..]).unwrap();
        assert_eq!(error.code, 942);
        assert_eq!(error.message, "table not found");
        assert_eq!(error.sql_offset, Some(10));
    }

    #[test]
    fn test_into_result() {
        let success = TtiResponse::Success;
        assert!(success.into_result().is_ok());

        let error = TtiResponse::Error(OracleError::new(1, "test"));
        assert!(error.into_result().is_err());
    }

    #[test]
    fn test_oracle_warning() {
        let warning = OracleWarning::new(28002, "password will expire");
        assert_eq!(warning.ora_code(), "ORA-28002");
        assert!(warning.to_string().contains("Warning"));
    }

    #[test]
    fn test_transient_errors() {
        let deadlock = OracleError::new(codes::DEADLOCK_DETECTED, "deadlock");
        assert!(deadlock.is_transient());
        assert!(deadlock.retry_delay().is_some());

        let resource_busy = OracleError::new(codes::RESOURCE_BUSY, "busy");
        assert!(resource_busy.is_transient());

        let serialize = OracleError::new(codes::CANNOT_SERIALIZE_ACCESS, "serialization");
        assert!(serialize.is_transient());
    }

    #[test]
    fn test_requires_reconnect() {
        let conn_closed = OracleError::new(codes::CONNECTION_CLOSED, "closed");
        assert!(conn_closed.requires_reconnect());

        let eof = OracleError::new(codes::EOF_ON_CHANNEL, "eof");
        assert!(eof.requires_reconnect());

        let killed = OracleError::new(codes::SESSION_KILLED, "killed");
        assert!(killed.requires_reconnect());

        let broken = OracleError::new(codes::BROKEN_PIPE, "broken pipe");
        assert!(broken.requires_reconnect());
    }

    #[test]
    fn test_data_errors() {
        let invalid_num = OracleError::new(codes::INVALID_NUMBER, "invalid number");
        assert!(invalid_num.is_data_error());

        let overflow = OracleError::new(codes::NUMERIC_OVERFLOW, "overflow");
        assert!(overflow.is_data_error());

        let invalid_date = OracleError::new(codes::INVALID_DATE, "invalid date");
        assert!(invalid_date.is_data_error());
    }

    #[test]
    fn test_resource_limit_errors() {
        let cursors = OracleError::new(codes::EXCEEDED_MAX_OPEN_CURSORS, "too many cursors");
        assert!(cursors.is_resource_limit_error());

        let oom = OracleError::new(codes::OUT_OF_MEMORY, "out of memory");
        assert!(oom.is_resource_limit_error());
    }

    #[test]
    fn test_error_severity() {
        // Transient
        let deadlock = OracleError::new(codes::DEADLOCK_DETECTED, "deadlock");
        assert_eq!(deadlock.severity(), ErrorSeverity::Transient);

        // ConnectionLost
        let conn_closed = OracleError::new(codes::CONNECTION_CLOSED, "closed");
        assert_eq!(conn_closed.severity(), ErrorSeverity::ConnectionLost);

        // Fatal (auth)
        let auth_err = OracleError::new(codes::INVALID_USERNAME_PASSWORD, "bad login");
        assert_eq!(auth_err.severity(), ErrorSeverity::Fatal);

        // UserError (constraint)
        let constraint = OracleError::new(codes::UNIQUE_CONSTRAINT_VIOLATED, "duplicate");
        assert_eq!(constraint.severity(), ErrorSeverity::UserError);

        // ResourceLimit
        let cursors = OracleError::new(codes::EXCEEDED_MAX_OPEN_CURSORS, "cursors");
        assert_eq!(cursors.severity(), ErrorSeverity::ResourceLimit);
    }

    #[test]
    fn test_retry_delay() {
        // Deadlock should have very short retry
        let deadlock = OracleError::new(codes::DEADLOCK_DETECTED, "deadlock");
        let delay = deadlock.retry_delay().unwrap();
        assert!(delay.as_millis() < 50);

        // Resource busy should have slightly longer retry
        let busy = OracleError::new(codes::RESOURCE_BUSY, "busy");
        let delay = busy.retry_delay().unwrap();
        assert!(delay.as_millis() >= 100);

        // Connection errors should have longer retry
        let timeout = OracleError::new(codes::TNS_CONNECT_TIMEOUT, "timeout");
        let delay = timeout.retry_delay().unwrap();
        assert!(delay.as_secs() >= 1);

        // Non-retryable should return None
        let auth = OracleError::new(codes::INVALID_USERNAME_PASSWORD, "bad login");
        assert!(auth.retry_delay().is_none());
    }
}
