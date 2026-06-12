//! Conservative multiplex-safety rules for PostgreSQL SQL text.

use super::classify::{PgReqType, classify_sql, has_single_statement};
use super::command::PgCommand;
use super::contains_ascii_case_insensitive;

/// Whether a SQL statement can safely ride a shared pipelined backend wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgMuxSafety {
    SafeReadOnlyAutocommit,
    UnsafeSessionState,
    UnsafeTransaction,
    UnsafeWrite,
    UnsafeMultiStatement,
    UnsafeCopy,
    Unknown,
}

impl PgMuxSafety {
    pub const fn is_safe(self) -> bool {
        matches!(self, Self::SafeReadOnlyAutocommit)
    }
}

pub fn multiplex_safety(sql: &str) -> PgMuxSafety {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return PgMuxSafety::Unknown;
    }
    if !has_single_statement(trimmed) {
        return PgMuxSafety::UnsafeMultiStatement;
    }

    let classification = classify_sql(trimmed);
    if classification.is_transaction_control {
        return PgMuxSafety::UnsafeTransaction;
    }
    if classification.command == PgCommand::Copy {
        return PgMuxSafety::UnsafeCopy;
    }
    if classification.is_session_state {
        return PgMuxSafety::UnsafeSessionState;
    }
    if classification.req_type == PgReqType::Write {
        return PgMuxSafety::UnsafeWrite;
    }

    match classification.command {
        PgCommand::Select => {
            if select_has_mux_unsafe_read_side_effect(trimmed) {
                PgMuxSafety::UnsafeSessionState
            } else {
                PgMuxSafety::SafeReadOnlyAutocommit
            }
        }
        PgCommand::Show | PgCommand::Values | PgCommand::Table | PgCommand::Explain => PgMuxSafety::SafeReadOnlyAutocommit,
        // WITH can hide mutating CTEs and volatile function calls unless a
        // full AST proves safety, so keep it out of the shared pipeline.
        PgCommand::With => PgMuxSafety::Unknown,
        _ => PgMuxSafety::Unknown,
    }
}

fn select_has_mux_unsafe_read_side_effect(sql: &str) -> bool {
    contains_ascii_case_insensitive(sql, " FOR UPDATE")
        || contains_ascii_case_insensitive(sql, " FOR NO KEY UPDATE")
        || contains_ascii_case_insensitive(sql, " FOR SHARE")
        || contains_ascii_case_insensitive(sql, " FOR KEY SHARE")
        || contains_ascii_case_insensitive(sql, "NEXTVAL")
        || contains_ascii_case_insensitive(sql, "SETVAL")
        || contains_ascii_case_insensitive(sql, "SET_CONFIG")
        || contains_ascii_case_insensitive(sql, "PG_ADVISORY_LOCK")
        || contains_ascii_case_insensitive(sql, "PG_TRY_ADVISORY_LOCK")
        || contains_ascii_case_insensitive(sql, "PG_NOTIFY")
        || contains_ascii_case_insensitive(sql, "INTO TEMP")
        || contains_ascii_case_insensitive(sql, "INTO TEMPORARY")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_reads_are_safe() {
        assert_eq!(multiplex_safety("SELECT 1"), PgMuxSafety::SafeReadOnlyAutocommit);
        assert_eq!(multiplex_safety("SHOW server_version"), PgMuxSafety::SafeReadOnlyAutocommit);
    }

    #[test]
    fn writes_transactions_and_session_commands_are_unsafe() {
        assert_eq!(multiplex_safety("UPDATE t SET x = 1"), PgMuxSafety::UnsafeWrite);
        assert_eq!(multiplex_safety("BEGIN"), PgMuxSafety::UnsafeTransaction);
        assert_eq!(multiplex_safety("SET search_path TO public"), PgMuxSafety::UnsafeSessionState);
    }

    #[test]
    fn multi_statement_is_unsafe_even_with_trailing_comments() {
        assert_eq!(multiplex_safety("SELECT 1; SELECT 2"), PgMuxSafety::UnsafeMultiStatement);
        assert_eq!(multiplex_safety("SELECT ';';"), PgMuxSafety::SafeReadOnlyAutocommit);
    }

    #[test]
    fn select_locking_and_sequence_functions_are_unsafe() {
        assert_eq!(multiplex_safety("SELECT * FROM jobs FOR UPDATE"), PgMuxSafety::UnsafeSessionState);
        assert_eq!(multiplex_safety("SELECT nextval('ids')"), PgMuxSafety::UnsafeWrite);
        assert_eq!(multiplex_safety("SELECT set_config('search_path', 'private', false)"), PgMuxSafety::UnsafeWrite);
    }
}
