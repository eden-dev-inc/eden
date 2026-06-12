//! Lightweight PostgreSQL SQL classification for protocol routing.
//!
//! This module intentionally stays smaller than Eden's proprietary request
//! analyzer. It only exposes protocol-level facts needed by open proxy hot
//! paths: read/write routing, session/transaction detection, and conservative
//! multiplex safety.

mod bucket;
mod classify;
mod command;
mod safety;
mod write_mux;

pub(crate) fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    let haystack = haystack.as_bytes();
    let needle = needle.as_bytes();
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }

    haystack
        .windows(needle.len())
        .any(|window| window.iter().zip(needle.iter()).all(|(lhs, rhs)| lhs.eq_ignore_ascii_case(rhs)))
}

pub use bucket::StatementBucket;
pub use classify::{
    PgClassification, PgReqType, classify_sql, classify_sql_req_type, first_sql_keyword, has_single_statement, skip_sql_comments,
};
pub use command::{CommandCategory, CommandRiskLevel, PgCommand, pg_command_from_sql};
pub use safety::{PgMuxSafety, multiplex_safety};
pub use write_mux::{PgWriteMuxSafety, write_multiplex_safety};
