//! Size limits and validation for PostgreSQL wire protocol.

use std::fmt;

/// Maximum PostgreSQL message size.
///
/// The protocol uses i32 for lengths, so the theoretical maximum is i32::MAX (2GB - 1).
/// However, we use a practical limit for defense in depth.
pub const MAX_MESSAGE_SIZE: usize = 1_073_741_823; // ~1GB

/// Practical maximum message size for most operations (256MB).
pub const MAX_MESSAGE_SIZE_PRACTICAL: usize = 268_435_456;

/// Maximum query length (16MB).
pub const MAX_QUERY_LENGTH: usize = 16_777_216;

/// Maximum number of columns in a result set.
///
/// PostgreSQL has a hard limit of 1664 columns per table.
pub const MAX_COLUMNS: u16 = 1664;

/// Maximum number of parameters in a prepared statement.
pub const MAX_PARAMS: u16 = 65535;

/// Maximum identifier length (names).
///
/// PostgreSQL truncates identifiers to 63 bytes by default (NAMEDATALEN - 1).
pub const MAX_IDENTIFIER_LENGTH: usize = 63;

/// Maximum pre-allocation size to prevent memory DoS.
pub const MAX_PREALLOC: usize = 8192;

/// Error indicating a limit was exceeded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LimitExceeded {
    /// The limit that was exceeded.
    pub limit_name: &'static str,
    /// The value that exceeded the limit.
    pub value: usize,
    /// The maximum allowed value.
    pub max: usize,
}

impl fmt::Display for LimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} exceeded: {} > {}", self.limit_name, self.value, self.max)
    }
}

impl std::error::Error for LimitExceeded {}

/// Configurable limits for parsing.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Maximum message size in bytes.
    pub max_message_size: usize,
    /// Maximum query length in bytes.
    pub max_query_length: usize,
    /// Maximum number of columns.
    pub max_columns: u16,
    /// Maximum number of parameters.
    pub max_params: u16,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_message_size: MAX_MESSAGE_SIZE_PRACTICAL,
            max_query_length: MAX_QUERY_LENGTH,
            max_columns: MAX_COLUMNS,
            max_params: MAX_PARAMS,
        }
    }
}

impl Limits {
    /// Create limits with relaxed values (for testing or special cases).
    pub fn relaxed() -> Self {
        Self {
            max_message_size: MAX_MESSAGE_SIZE,
            max_query_length: MAX_MESSAGE_SIZE,
            max_columns: MAX_COLUMNS,
            max_params: MAX_PARAMS,
        }
    }

    /// Check if a message size is within limits.
    pub fn check_message_size(&self, size: usize) -> Result<(), LimitExceeded> {
        if size > self.max_message_size {
            Err(LimitExceeded {
                limit_name: "message_size",
                value: size,
                max: self.max_message_size,
            })
        } else {
            Ok(())
        }
    }

    /// Check if a query length is within limits.
    pub fn check_query_length(&self, length: usize) -> Result<(), LimitExceeded> {
        if length > self.max_query_length {
            Err(LimitExceeded {
                limit_name: "query_length",
                value: length,
                max: self.max_query_length,
            })
        } else {
            Ok(())
        }
    }

    /// Check if a column count is within limits.
    pub fn check_column_count(&self, count: u16) -> Result<(), LimitExceeded> {
        if count > self.max_columns {
            Err(LimitExceeded {
                limit_name: "column_count",
                value: count as usize,
                max: self.max_columns as usize,
            })
        } else {
            Ok(())
        }
    }

    /// Check if a parameter count is within limits.
    pub fn check_param_count(&self, count: u16) -> Result<(), LimitExceeded> {
        if count > self.max_params {
            Err(LimitExceeded {
                limit_name: "param_count",
                value: count as usize,
                max: self.max_params as usize,
            })
        } else {
            Ok(())
        }
    }
}
