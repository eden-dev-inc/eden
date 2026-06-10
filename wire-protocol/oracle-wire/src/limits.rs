//! Configurable size limits for Oracle TNS parsing to prevent DoS attacks.
//!
//! These limits prevent malicious inputs from causing excessive memory allocation
//! or CPU consumption. Oracle's TNS protocol has built-in limits (max packet size
//! is 32767 bytes), but additional application-level limits are useful for defense
//! in depth.

/// Maximum TNS packet size in bytes.
/// Oracle's protocol limit is 32767 bytes.
pub const MAX_PACKET_SIZE: usize = 32_767;

/// Maximum size for connect data string.
/// Connect strings can be long due to TNS descriptors, but shouldn't exceed this.
/// Default: 4KB
pub const MAX_CONNECT_DATA: usize = 4_096;

/// Maximum size for redirect address data.
/// Default: 2KB
pub const MAX_REDIRECT_DATA: usize = 2_048;

/// Maximum size for error messages.
/// Default: 8KB
pub const MAX_ERROR_MESSAGE: usize = 8_192;

/// Maximum size for SQL statements.
/// Default: 1MB
pub const MAX_SQL_LENGTH: usize = 1_048_576;

/// Maximum number of columns in a result set.
/// Oracle's theoretical limit is much higher, but this provides DoS protection.
/// Default: 1000
pub const MAX_COLUMNS: usize = 1_000;

/// Maximum number of bind variables in a statement.
/// Default: 32767 (Oracle's limit)
pub const MAX_BIND_VARIABLES: usize = 32_767;

/// Maximum rows to prefetch in a single request.
/// Default: 10000
pub const MAX_PREFETCH_ROWS: usize = 10_000;

/// Maximum size for a single column value in bytes.
/// LOBs can be larger but should be streamed.
/// Default: 32KB
pub const MAX_COLUMN_VALUE: usize = 32_768;

/// Maximum size for LOB data per chunk.
/// Default: 1MB
pub const MAX_LOB_CHUNK: usize = 1_048_576;

/// Maximum pre-allocation size for vectors.
/// We cap pre-allocation to avoid OOM from large declared sizes.
/// Default: 8KB elements/bytes
pub const MAX_PREALLOC: usize = 8_192;

/// Maximum nesting depth for Oracle object types.
/// Prevents stack overflow from deeply nested objects.
/// Default: 32
pub const MAX_OBJECT_DEPTH: usize = 32;

/// Maximum size for username in authentication.
/// Oracle's limit is 128 characters.
/// Default: 128
pub const MAX_USERNAME_LENGTH: usize = 128;

/// Maximum size for authentication data (keys, verifiers, etc.).
/// Default: 2KB
pub const MAX_AUTH_DATA: usize = 2_048;

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

    #[error("string length {len} exceeds maximum {max}")]
    StringLength { len: usize, max: usize },

    #[error("bind variable count {count} exceeds maximum {max}")]
    BindCount { count: usize, max: usize },

    #[error("object nesting depth {depth} exceeds maximum {max}")]
    NestingDepth { depth: usize, max: usize },

    #[error("LOB chunk size {size} exceeds maximum {max}")]
    LobChunkSize { size: usize, max: usize },
}

/// Builder for custom limits configuration.
#[derive(Clone, Debug)]
pub struct Limits {
    pub max_packet_size: usize,
    pub max_connect_data: usize,
    pub max_sql_length: usize,
    pub max_columns: usize,
    pub max_bind_variables: usize,
    pub max_prefetch_rows: usize,
    pub max_column_value: usize,
    pub max_lob_chunk: usize,
    pub max_object_depth: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_packet_size: MAX_PACKET_SIZE,
            max_connect_data: MAX_CONNECT_DATA,
            max_sql_length: MAX_SQL_LENGTH,
            max_columns: MAX_COLUMNS,
            max_bind_variables: MAX_BIND_VARIABLES,
            max_prefetch_rows: MAX_PREFETCH_ROWS,
            max_column_value: MAX_COLUMN_VALUE,
            max_lob_chunk: MAX_LOB_CHUNK,
            max_object_depth: MAX_OBJECT_DEPTH,
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
            max_connect_data: 1024,
            max_sql_length: 65536,
            max_columns: 100,
            max_bind_variables: 1000,
            max_prefetch_rows: 1000,
            max_column_value: 4096,
            max_lob_chunk: 65536,
            max_object_depth: 8,
        }
    }

    /// Create relaxed limits for trusted internal use.
    pub fn relaxed() -> Self {
        Self {
            max_packet_size: MAX_PACKET_SIZE,
            max_connect_data: 16384,
            max_sql_length: 16 * 1024 * 1024, // 16MB
            max_columns: 10000,
            max_bind_variables: 65535,
            max_prefetch_rows: 100000,
            max_column_value: 1024 * 1024,   // 1MB
            max_lob_chunk: 16 * 1024 * 1024, // 16MB
            max_object_depth: 64,
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

    /// Check bind variable count against this limit configuration.
    pub fn check_binds(&self, count: usize) -> Result<(), LimitExceeded> {
        if count > self.max_bind_variables {
            Err(LimitExceeded::BindCount { count, max: self.max_bind_variables })
        } else {
            Ok(())
        }
    }

    /// Check object nesting depth against this limit configuration.
    pub fn check_depth(&self, depth: usize) -> Result<(), LimitExceeded> {
        if depth > self.max_object_depth {
            Err(LimitExceeded::NestingDepth { depth, max: self.max_object_depth })
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
        assert!(limits.max_sql_length < MAX_SQL_LENGTH);
    }

    #[test]
    fn test_limits_check() {
        let limits = Limits::strict();
        assert!(limits.check_columns(50).is_ok());
        assert!(limits.check_columns(200).is_err());
    }
}
