//! Configurable size limits for Sybase TDS parsing to prevent DoS attacks.
//!
//! These limits prevent malicious inputs from causing excessive memory allocation
//! or CPU consumption. TDS has built-in limits, but additional application-level
//! limits are useful for defense in depth.

/// Maximum TDS packet size in bytes.
/// TDS default is 512 bytes, but can be negotiated up to 32767.
/// Sybase ASE supports up to 65535 bytes with TDS 5.0.
pub const MAX_PACKET_SIZE: usize = 65_535;

/// Default TDS packet size (512 bytes).
pub const DEFAULT_PACKET_SIZE: usize = 512;

/// Minimum TDS packet size (512 bytes).
pub const MIN_PACKET_SIZE: usize = 512;

/// TDS header size in bytes.
pub const HEADER_SIZE: usize = 8;

/// Maximum size for login packet data.
/// Default: 4KB
pub const MAX_LOGIN_DATA: usize = 4_096;

/// Maximum size for SQL statements.
/// Default: 1MB
pub const MAX_SQL_LENGTH: usize = 1_048_576;

/// Maximum number of columns in a result set.
/// Sybase ASE supports up to 4096 columns.
/// Default: 4096
pub const MAX_COLUMNS: usize = 4_096;

/// Maximum number of parameters in a stored procedure call.
/// Default: 2048
pub const MAX_PARAMETERS: usize = 2_048;

/// Maximum rows to fetch in a single request.
/// Default: 10000
pub const MAX_FETCH_ROWS: usize = 10_000;

/// Maximum size for a single column value in bytes.
/// Text/Image columns can be larger but should be streamed.
/// Default: 32KB
pub const MAX_COLUMN_VALUE: usize = 32_768;

/// Maximum size for text/image data per chunk.
/// Default: 1MB
pub const MAX_TEXT_CHUNK: usize = 1_048_576;

/// Maximum pre-allocation size for vectors.
/// We cap pre-allocation to avoid OOM from large declared sizes.
/// Default: 8KB elements/bytes
pub const MAX_PREALLOC: usize = 8_192;

/// Maximum length for identifiers (table names, column names, etc.).
/// Sybase ASE limit is 255 characters.
pub const MAX_IDENTIFIER_LENGTH: usize = 255;

/// Maximum length for server name in login.
pub const MAX_SERVER_NAME_LENGTH: usize = 30;

/// Maximum length for username in login.
pub const MAX_USERNAME_LENGTH: usize = 30;

/// Maximum length for password in login.
pub const MAX_PASSWORD_LENGTH: usize = 30;

/// Maximum length for application name in login.
pub const MAX_APP_NAME_LENGTH: usize = 30;

/// Maximum length for host name in login.
pub const MAX_HOST_NAME_LENGTH: usize = 30;

/// Maximum length for error/info messages.
/// Default: 8KB
pub const MAX_MESSAGE_LENGTH: usize = 8_192;

/// Check if a packet size is within limits.
#[inline]
pub fn check_packet_size(size: usize) -> Result<(), LimitExceeded> {
    if size > MAX_PACKET_SIZE {
        Err(LimitExceeded::PacketSize { size, max: MAX_PACKET_SIZE })
    } else if size < MIN_PACKET_SIZE {
        Err(LimitExceeded::PacketTooSmall { size, min: MIN_PACKET_SIZE })
    } else {
        Ok(())
    }
}

/// Check if a column count is within limits.
#[inline]
pub fn check_column_count(count: usize) -> Result<(), LimitExceeded> {
    if count > MAX_COLUMNS {
        Err(LimitExceeded::ColumnCount { count, max: MAX_COLUMNS })
    } else {
        Ok(())
    }
}

/// Check if a parameter count is within limits.
#[inline]
pub fn check_parameter_count(count: usize) -> Result<(), LimitExceeded> {
    if count > MAX_PARAMETERS {
        Err(LimitExceeded::ParameterCount { count, max: MAX_PARAMETERS })
    } else {
        Ok(())
    }
}

/// Check if a string length is within limits.
#[inline]
pub fn check_string_length(len: usize, max: usize) -> Result<(), LimitExceeded> {
    if len > max {
        Err(LimitExceeded::StringLength { len, max })
    } else {
        Ok(())
    }
}

/// Compute a safe pre-allocation size.
#[inline]
pub fn safe_prealloc(requested: usize) -> usize {
    requested.min(MAX_PREALLOC)
}

/// Error when a limit is exceeded.
#[derive(Clone, Debug, thiserror::Error)]
pub enum LimitExceeded {
    #[error("packet size {size} exceeds maximum {max}")]
    PacketSize { size: usize, max: usize },

    #[error("packet size {size} is below minimum {min}")]
    PacketTooSmall { size: usize, min: usize },

    #[error("column count {count} exceeds maximum {max}")]
    ColumnCount { count: usize, max: usize },

    #[error("parameter count {count} exceeds maximum {max}")]
    ParameterCount { count: usize, max: usize },

    #[error("string length {len} exceeds maximum {max}")]
    StringLength { len: usize, max: usize },

    #[error("row count {count} exceeds maximum {max}")]
    RowCount { count: usize, max: usize },

    #[error("text chunk size {size} exceeds maximum {max}")]
    TextChunkSize { size: usize, max: usize },
}

/// Builder for custom limits configuration.
#[derive(Clone, Debug)]
pub struct Limits {
    pub max_packet_size: usize,
    pub max_sql_length: usize,
    pub max_columns: usize,
    pub max_parameters: usize,
    pub max_fetch_rows: usize,
    pub max_column_value: usize,
    pub max_text_chunk: usize,
    pub max_message_length: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_packet_size: MAX_PACKET_SIZE,
            max_sql_length: MAX_SQL_LENGTH,
            max_columns: MAX_COLUMNS,
            max_parameters: MAX_PARAMETERS,
            max_fetch_rows: MAX_FETCH_ROWS,
            max_column_value: MAX_COLUMN_VALUE,
            max_text_chunk: MAX_TEXT_CHUNK,
            max_message_length: MAX_MESSAGE_LENGTH,
        }
    }
}

impl Limits {
    /// Create a new limits configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create strict limits for untrusted input.
    pub fn strict() -> Self {
        Self {
            max_packet_size: DEFAULT_PACKET_SIZE * 2, // 1KB
            max_sql_length: 65536,
            max_columns: 100,
            max_parameters: 100,
            max_fetch_rows: 1000,
            max_column_value: 4096,
            max_text_chunk: 65536,
            max_message_length: 4096,
        }
    }

    /// Create relaxed limits for trusted internal use.
    pub fn relaxed() -> Self {
        Self {
            max_packet_size: MAX_PACKET_SIZE,
            max_sql_length: 16 * 1024 * 1024, // 16MB
            max_columns: 8192,
            max_parameters: 8192,
            max_fetch_rows: 100000,
            max_column_value: 1024 * 1024,    // 1MB
            max_text_chunk: 16 * 1024 * 1024, // 16MB
            max_message_length: 1024 * 1024,  // 1MB
        }
    }

    /// Check packet size against this limit configuration.
    pub fn check_packet(&self, size: usize) -> Result<(), LimitExceeded> {
        if size > self.max_packet_size {
            Err(LimitExceeded::PacketSize { size, max: self.max_packet_size })
        } else {
            Ok(())
        }
    }

    /// Check column count against this limit configuration.
    pub fn check_columns(&self, count: usize) -> Result<(), LimitExceeded> {
        if count > self.max_columns {
            Err(LimitExceeded::ColumnCount { count, max: self.max_columns })
        } else {
            Ok(())
        }
    }

    /// Check parameter count against this limit configuration.
    pub fn check_parameters(&self, count: usize) -> Result<(), LimitExceeded> {
        if count > self.max_parameters {
            Err(LimitExceeded::ParameterCount { count, max: self.max_parameters })
        } else {
            Ok(())
        }
    }

    /// Check row count against this limit configuration.
    pub fn check_rows(&self, count: usize) -> Result<(), LimitExceeded> {
        if count > self.max_fetch_rows {
            Err(LimitExceeded::RowCount { count, max: self.max_fetch_rows })
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_packet_size() {
        assert!(check_packet_size(1000).is_ok());
        assert!(check_packet_size(MAX_PACKET_SIZE).is_ok());
        assert!(check_packet_size(MAX_PACKET_SIZE + 1).is_err());
        assert!(check_packet_size(100).is_err()); // Too small
    }

    #[test]
    fn test_check_column_count() {
        assert!(check_column_count(100).is_ok());
        assert!(check_column_count(MAX_COLUMNS).is_ok());
        assert!(check_column_count(MAX_COLUMNS + 1).is_err());
    }

    #[test]
    fn test_safe_prealloc() {
        assert_eq!(safe_prealloc(100), 100);
        assert_eq!(safe_prealloc(MAX_PREALLOC), MAX_PREALLOC);
        assert_eq!(safe_prealloc(MAX_PREALLOC + 1000), MAX_PREALLOC);
    }

    #[test]
    fn test_limits_default() {
        let limits = Limits::default();
        assert_eq!(limits.max_packet_size, MAX_PACKET_SIZE);
        assert_eq!(limits.max_columns, MAX_COLUMNS);
    }

    #[test]
    fn test_limits_strict() {
        let limits = Limits::strict();
        assert!(limits.max_columns < MAX_COLUMNS);
        assert!(limits.max_sql_length < MAX_SQL_LENGTH);
    }

    #[test]
    fn test_limits_check() {
        let limits = Limits::strict();
        assert!(limits.check_columns(50).is_ok());
        assert!(limits.check_columns(200).is_err());
    }
}
