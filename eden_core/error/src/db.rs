use std::{error, fmt, result};

/// Result type alias for database manager operations.
pub type ResultDB<T> = result::Result<T, DBError>;

/// Database manager-level errors.
///
/// Lower-level errors used within the database manager implementation.
/// Most application code should use [`EpError::Database`](crate::EpError::Database) instead.
#[derive(Clone, Debug, PartialEq)]
pub enum DBError {
    /// Database command execution error.
    Command(String),
    /// Database connection error.
    Connect(String),
    /// Ignored error (intentionally silenced).
    Ignored,
}

impl fmt::Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DBError::Command(s) => write!(f, "DB command error: {s}"),
            DBError::Connect(s) => write!(f, "DB connection error: {s}"),
            DBError::Ignored => "ignored".fmt(f),
        }
    }
}

impl error::Error for DBError {}
