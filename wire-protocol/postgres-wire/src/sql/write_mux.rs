//! Conservative write-side multiplex safety for PostgreSQL SQL text.
//!
//! This is intentionally much smaller than Eden's proprietary analyzer. It only
//! recognizes single-statement autocommit DML whose primary target relation can
//! be identified from the leading SQL text.

use super::classify::{classify_sql, has_single_statement, skip_sql_comments};
use super::command::PgCommand;
use super::contains_ascii_case_insensitive;

/// Whether a SQL write can safely use a shared autocommit write backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgWriteMuxSafety<'a> {
    SafeAutocommitDml { table: &'a str },
    UnsafeMultiStatement,
    UnsafeTransaction,
    UnsafeSessionState,
    UnsafeNonDml,
    Unknown,
}

impl<'a> PgWriteMuxSafety<'a> {
    pub const fn is_safe(self) -> bool {
        matches!(self, Self::SafeAutocommitDml { .. })
    }

    pub const fn table(self) -> Option<&'a str> {
        match self {
            Self::SafeAutocommitDml { table } => Some(table),
            _ => None,
        }
    }
}

/// Return conservative autocommit-write multiplex safety for a SQL statement.
pub fn write_multiplex_safety(sql: &str) -> PgWriteMuxSafety<'_> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return PgWriteMuxSafety::Unknown;
    }
    if !has_single_statement(trimmed) {
        return PgWriteMuxSafety::UnsafeMultiStatement;
    }

    let effective = skip_sql_comments(trimmed);
    let classification = classify_sql(effective);
    if classification.is_transaction_control {
        return PgWriteMuxSafety::UnsafeTransaction;
    }
    if classification.is_session_state || write_has_mux_unsafe_session_side_effect(effective) {
        return PgWriteMuxSafety::UnsafeSessionState;
    }
    if write_has_complex_dml_clause(effective) {
        return PgWriteMuxSafety::Unknown;
    }

    let table = match classification.command {
        PgCommand::Insert => insert_target(effective),
        PgCommand::Update => {
            if contains_ascii_case_insensitive(effective, " FROM ") {
                return PgWriteMuxSafety::Unknown;
            }
            update_target(effective)
        }
        PgCommand::Delete => delete_target(effective),
        _ => return PgWriteMuxSafety::UnsafeNonDml,
    };

    table
        .filter(|target| !target.is_empty())
        .map(|table| PgWriteMuxSafety::SafeAutocommitDml { table })
        .unwrap_or(PgWriteMuxSafety::Unknown)
}

fn insert_target(sql: &str) -> Option<&str> {
    let rest = strip_leading_word(sql, "INSERT")?;
    let rest = strip_leading_word(rest, "INTO")?;
    parse_relation_name(rest)
}

fn update_target(sql: &str) -> Option<&str> {
    let rest = strip_leading_word(sql, "UPDATE")?;
    parse_relation_name(rest)
}

fn delete_target(sql: &str) -> Option<&str> {
    let rest = strip_leading_word(sql, "DELETE")?;
    let rest = strip_leading_word(rest, "FROM")?;
    parse_relation_name(rest)
}

fn parse_relation_name(sql: &str) -> Option<&str> {
    let mut i = skip_ws_and_comments(sql, 0);

    let bytes = sql.as_bytes();
    if i >= bytes.len() || bytes[i] == b'(' || bytes[i] == b'"' {
        return None;
    }

    let start = i;
    i = parse_identifier(sql, i)?;
    let next = skip_ws_and_comments(sql, i);
    if sql.as_bytes().get(next) == Some(&b'.') {
        return None;
    }

    Some(sql[start..i].trim())
}

fn strip_leading_word<'a>(sql: &'a str, word: &str) -> Option<&'a str> {
    let i = skip_ws_and_comments(sql, 0);
    let s = &sql[i..];
    if s.len() < word.len() {
        return None;
    }
    let (head, tail) = s.split_at(word.len());
    if !head.eq_ignore_ascii_case(word) {
        return None;
    }
    if tail.as_bytes().first().is_some_and(|byte| is_identifier_continue(*byte)) {
        return None;
    }
    Some(tail)
}

fn parse_identifier(sql: &str, mut i: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    if i >= bytes.len() {
        return None;
    }

    if bytes[i] == b'"' {
        i += 1;
        while i < bytes.len() {
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                } else {
                    return Some(i + 1);
                }
            } else {
                i += 1;
            }
        }
        return None;
    }

    if !is_identifier_start(bytes[i]) {
        return None;
    }
    i += 1;
    while i < bytes.len() && is_identifier_continue(bytes[i]) {
        i += 1;
    }
    Some(i)
}

fn skip_ws_and_comments(sql: &str, mut i: usize) -> usize {
    let bytes = sql.as_bytes();
    loop {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            i = skip_block_comment(bytes, i + 2);
            continue;
        }
        return i;
    }
}

fn skip_block_comment(bytes: &[u8], mut i: usize) -> usize {
    let mut depth = 1u32;
    while i + 1 < bytes.len() {
        if bytes[i] == b'/' && bytes[i + 1] == b'*' {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            depth = depth.saturating_sub(1);
            i += 2;
            if depth == 0 {
                return i;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_identifier_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
}

fn write_has_mux_unsafe_session_side_effect(sql: &str) -> bool {
    contains_ascii_case_insensitive(sql, "SET_CONFIG")
        || contains_ascii_case_insensitive(sql, "PG_ADVISORY_LOCK")
        || contains_ascii_case_insensitive(sql, "PG_TRY_ADVISORY_LOCK")
        || contains_ascii_case_insensitive(sql, "PG_NOTIFY")
        || contains_ascii_case_insensitive(sql, "CREATE TEMP")
        || contains_ascii_case_insensitive(sql, "CREATE TEMPORARY")
}

fn write_has_complex_dml_clause(sql: &str) -> bool {
    contains_ascii_case_insensitive(sql, " RETURNING")
        || contains_ascii_case_insensitive(sql, " ON CONFLICT")
        || contains_ascii_case_insensitive(sql, " OVERRIDING")
        || contains_ascii_case_insensitive(sql, " USING ")
        || contains_ascii_case_insensitive(sql, " AS ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_simple_dml_targets() {
        assert_eq!(write_multiplex_safety("INSERT INTO users(id) VALUES (1)").table(), Some("users"));
        assert_eq!(write_multiplex_safety("UPDATE users SET name = 'x'").table(), Some("users"));
        assert_eq!(write_multiplex_safety("DELETE FROM jobs WHERE id = 1").table(), Some("jobs"));
    }

    #[test]
    fn rejects_non_autocommit_or_ambiguous_writes() {
        assert!(!write_multiplex_safety("BEGIN").is_safe());
        assert!(!write_multiplex_safety("UPDATE t SET x = 1; UPDATE t SET x = 2").is_safe());
        assert!(!write_multiplex_safety("WITH updated AS (UPDATE t SET x = 1 RETURNING *) SELECT * FROM updated").is_safe());
        assert!(!write_multiplex_safety("CREATE TABLE t(id int)").is_safe());
        assert!(!write_multiplex_safety("COPY t FROM STDIN").is_safe());
        assert!(!write_multiplex_safety("UPDATE t SET x = set_config('a.b', 'c', false)").is_safe());
        assert!(!write_multiplex_safety("UPDATE t SET x = pg_notify('topic', 'body')").is_safe());
        assert!(!write_multiplex_safety("UPDATE public.t SET x = 1").is_safe());
        assert!(!write_multiplex_safety("UPDATE \"t\" SET x = 1").is_safe());
        assert!(!write_multiplex_safety("UPDATE t SET x = 1 FROM other WHERE t.id = other.id").is_safe());
        assert!(!write_multiplex_safety("INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING").is_safe());
        assert!(!write_multiplex_safety("INSERT INTO t VALUES (1) RETURNING id").is_safe());
        assert!(!write_multiplex_safety("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = s.x").is_safe());
    }
}
