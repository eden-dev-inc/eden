//! Statement buckets used by the protocol-level PostgreSQL classifier.

/// Coarse SQL statement buckets for routing and safety decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatementBucket {
    Read,
    Write,
    DDL,
    TCL,
    DCL,
    Other,
}
