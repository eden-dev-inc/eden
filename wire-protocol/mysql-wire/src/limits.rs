//! Configurable size limits for MySQL parsing to prevent DoS attacks.
//!
//! These limits prevent malicious inputs from causing excessive memory allocation
//! or CPU consumption. MySQL's protocol has built-in limits (max packet size is
//! 16MB - 1), but additional application-level limits are useful for defense
//! in depth.

/// Maximum MySQL packet payload size in bytes.
/// MySQL's protocol limit is 16MB - 1 (0xFFFFFF).
pub const MAX_PACKET_SIZE: usize = 16_777_215;

/// Maximum size for SQL queries.
/// Default: 16MB (matches max_allowed_packet default).
pub const MAX_QUERY_LENGTH: usize = 16_777_216;

/// Maximum number of columns in a result set.
/// MySQL's theoretical limit is 4096, this provides DoS protection.
/// Default: 4096
pub const MAX_COLUMNS: usize = 4_096;

/// Maximum number of rows to buffer.
/// Default: 100000
pub const MAX_ROWS: usize = 100_000;

/// Maximum size for a single column value in bytes.
/// Default: 1MB
pub const MAX_COLUMN_VALUE: usize = 1_048_576;

/// Maximum size for username.
/// MySQL limit is 32 characters (80 bytes in UTF-8 worst case).
pub const MAX_USERNAME_LENGTH: usize = 80;

/// Maximum size for database name.
/// MySQL limit is 64 characters.
pub const MAX_DATABASE_LENGTH: usize = 64;

/// Maximum size for authentication data.
/// Default: 2KB
pub const MAX_AUTH_DATA: usize = 2_048;

/// Maximum pre-allocation size for vectors.
/// We cap pre-allocation to avoid OOM from large declared sizes.
/// Default: 8KB elements/bytes
pub const MAX_PREALLOC: usize = 8_192;

/// Maximum nesting depth for packet parsing.
/// Default: 32
pub const MAX_DEPTH: usize = 32;

/// Maximum number of parameters in a prepared statement.
/// Default: 65535 (MySQL limit)
pub const MAX_PARAMS: usize = 65_535;

/// Maximum size for error messages.
/// Default: 8KB
pub const MAX_ERROR_MESSAGE: usize = 8_192;

/// Check if a packet size is within limits.
#[inline]
pub fn check_packet_size(size: usize) -> Result<(), LimitExceeded> {
    if size > MAX_PACKET_SIZE {
        Err(LimitExceeded::PacketSize { size, max: MAX_PACKET_SIZE })
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

/// Check if a query length is within limits.
#[inline]
pub fn check_query_length(len: usize) -> Result<(), LimitExceeded> {
    if len > MAX_QUERY_LENGTH {
        Err(LimitExceeded::QueryLength { len, max: MAX_QUERY_LENGTH })
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

    #[error("column count {count} exceeds maximum {max}")]
    ColumnCount { count: usize, max: usize },

    #[error("query length {len} exceeds maximum {max}")]
    QueryLength { len: usize, max: usize },

    #[error("string length {len} exceeds maximum {max}")]
    StringLength { len: usize, max: usize },

    #[error("row count {count} exceeds maximum {max}")]
    RowCount { count: usize, max: usize },

    #[error("parameter count {count} exceeds maximum {max}")]
    ParamCount { count: usize, max: usize },

    #[error("nesting depth {depth} exceeds maximum {max}")]
    NestingDepth { depth: usize, max: usize },
}

/// Builder for custom limits configuration.
#[derive(Clone, Debug)]
pub struct Limits {
    pub max_packet_size: usize,
    pub max_query_length: usize,
    pub max_columns: usize,
    pub max_rows: usize,
    pub max_column_value: usize,
    pub max_params: usize,
    pub max_depth: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_packet_size: MAX_PACKET_SIZE,
            max_query_length: MAX_QUERY_LENGTH,
            max_columns: MAX_COLUMNS,
            max_rows: MAX_ROWS,
            max_column_value: MAX_COLUMN_VALUE,
            max_params: MAX_PARAMS,
            max_depth: MAX_DEPTH,
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
            max_packet_size: MAX_PACKET_SIZE,
            max_query_length: 65_536,
            max_columns: 100,
            max_rows: 10_000,
            max_column_value: 32_768,
            max_params: 1_000,
            max_depth: 8,
        }
    }

    /// Create relaxed limits for trusted internal use.
    pub fn relaxed() -> Self {
        Self {
            max_packet_size: MAX_PACKET_SIZE,
            max_query_length: 64 * 1024 * 1024, // 64MB
            max_columns: 10_000,
            max_rows: 1_000_000,
            max_column_value: 16 * 1024 * 1024, // 16MB
            max_params: 65_535,
            max_depth: 64,
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

    /// Check row count against this limit configuration.
    pub fn check_rows(&self, count: usize) -> Result<(), LimitExceeded> {
        if count > self.max_rows {
            Err(LimitExceeded::RowCount { count, max: self.max_rows })
        } else {
            Ok(())
        }
    }

    /// Check parameter count against this limit configuration.
    pub fn check_params(&self, count: usize) -> Result<(), LimitExceeded> {
        if count > self.max_params {
            Err(LimitExceeded::ParamCount { count, max: self.max_params })
        } else {
            Ok(())
        }
    }

    /// Check nesting depth against this limit configuration.
    pub fn check_depth(&self, depth: usize) -> Result<(), LimitExceeded> {
        if depth > self.max_depth {
            Err(LimitExceeded::NestingDepth { depth, max: self.max_depth })
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
        assert!(limits.max_query_length < MAX_QUERY_LENGTH);
    }

    #[test]
    fn test_limits_check() {
        let limits = Limits::strict();
        assert!(limits.check_columns(50).is_ok());
        assert!(limits.check_columns(200).is_err());
    }
}
