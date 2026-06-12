//! Lightweight SQL classification for PostgreSQL protocol routing.

use super::bucket::StatementBucket;
use super::command::{CommandCategory, CommandRiskLevel, PgCommand, pg_command_from_sql};

/// Read/write routing decision independent of endpoint crates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgReqType {
    Read,
    Write,
}

impl PgReqType {
    pub const fn is_write(self) -> bool {
        matches!(self, Self::Write)
    }
}

/// Protocol-level SQL classification result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PgClassification {
    pub command: PgCommand,
    pub command_name: &'static str,
    pub req_type: PgReqType,
    pub category: CommandCategory,
    pub risk: CommandRiskLevel,
    pub bucket: StatementBucket,
    pub is_session_state: bool,
    pub is_transaction_control: bool,
}

/// Classify SQL into protocol routing facts.
pub fn classify_sql(sql: &str) -> PgClassification {
    let explicit_req_type = extract_eden_annotation(sql);
    let effective = skip_sql_comments(sql.trim());
    let mut command = pg_command_from_sql(effective);

    let req_type = if let Some(req_type) = explicit_req_type {
        req_type
    } else {
        match command {
            PgCommand::Explain => {
                let executes = classify_explain_executes(effective);
                command = if executes { PgCommand::ExplainAnalyze } else { PgCommand::Explain };
                if executes
                    && let Some(inner) = strip_explain_prefix(effective)
                    && classify_sql(inner).req_type.is_write()
                {
                    PgReqType::Write
                } else {
                    PgReqType::Read
                }
            }
            PgCommand::With => {
                if contains_write_in_with(effective) {
                    PgReqType::Write
                } else {
                    PgReqType::Read
                }
            }
            PgCommand::Copy => {
                if classify_copy_writes(effective) {
                    PgReqType::Write
                } else {
                    PgReqType::Read
                }
            }
            PgCommand::Select => {
                if select_writes(effective) {
                    PgReqType::Write
                } else {
                    PgReqType::Read
                }
            }
            PgCommand::Other => PgReqType::Write,
            _ => {
                if command.base_is_write() {
                    PgReqType::Write
                } else {
                    PgReqType::Read
                }
            }
        }
    };

    PgClassification {
        command,
        command_name: command.as_str(),
        req_type,
        category: command.category(),
        risk: command.risk(),
        bucket: command.bucket(),
        is_session_state: command.is_session_state(),
        is_transaction_control: command.is_transaction_control(),
    }
}

pub fn classify_sql_req_type(sql: &str) -> PgReqType {
    classify_sql(sql).req_type
}

/// Return the literal first SQL keyword after leading SQL comments.
pub fn first_sql_keyword(sql: &str) -> String {
    skip_sql_comments(sql.trim())
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_end_matches(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .to_ascii_uppercase()
}

/// Skip SQL line comments and nested block comments.
pub fn skip_sql_comments(sql: &str) -> &str {
    let mut s = sql;
    loop {
        s = s.trim_start();
        if s.starts_with("--") {
            s = s.find('\n').map_or("", |pos| &s[pos + 1..]);
        } else if s.starts_with("/*") {
            s = skip_nested_block_comment(s);
        } else {
            return s;
        }
    }
}

fn skip_nested_block_comment(s: &str) -> &str {
    let bytes = s.as_bytes();
    let mut depth = 0u32;
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'/' && bytes[i + 1] == b'*' {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            depth = depth.saturating_sub(1);
            i += 2;
            if depth == 0 {
                return &s[i..];
            }
        } else {
            i += 1;
        }
    }
    ""
}

fn extract_eden_annotation(sql: &str) -> Option<PgReqType> {
    let trimmed = sql.trim_start();
    if trimmed.starts_with("/* eden:write */") {
        Some(PgReqType::Write)
    } else if trimmed.starts_with("/* eden:read */") {
        Some(PgReqType::Read)
    } else {
        None
    }
}

/// Return true when SQL contains at most one statement.
///
/// Semicolons inside quoted strings, quoted identifiers, comments, and dollar
/// quoted bodies are ignored. Extra trailing semicolons/comments are allowed.
pub fn has_single_statement(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = skip_single_quoted(bytes, i + 1),
            b'"' => i = skip_double_quoted(bytes, i + 1),
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => i = skip_line_comment(bytes, i + 2),
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => i = skip_block_comment(bytes, i + 2),
            b'$' => {
                if let Some(next) = skip_dollar_quoted(bytes, i) {
                    i = next;
                } else {
                    i += 1;
                }
            }
            b';' => return !tail_has_statement(bytes, i + 1),
            _ => i += 1,
        }
    }

    true
}

fn tail_has_statement(bytes: &[u8], mut i: usize) -> bool {
    while i < bytes.len() {
        match bytes[i] {
            b if b.is_ascii_whitespace() || b == b';' => i += 1,
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => i = skip_line_comment(bytes, i + 2),
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => i = skip_block_comment(bytes, i + 2),
            _ => return true,
        }
    }
    false
}

fn skip_single_quoted(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() {
        match bytes[i] {
            b'\'' if i + 1 < bytes.len() && bytes[i + 1] == b'\'' => i += 2,
            b'\'' => return i + 1,
            b'\\' if i + 1 < bytes.len() => i += 2,
            _ => i += 1,
        }
    }
    i
}

fn skip_double_quoted(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() {
        match bytes[i] {
            b'"' if i + 1 < bytes.len() && bytes[i + 1] == b'"' => i += 2,
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    i
}

fn skip_line_comment(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }
    i
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

fn skip_dollar_quoted(bytes: &[u8], start: usize) -> Option<usize> {
    let mut end_tag = start + 1;
    while end_tag < bytes.len() && is_dollar_tag_byte(bytes[end_tag]) {
        end_tag += 1;
    }
    if end_tag >= bytes.len() || bytes[end_tag] != b'$' {
        return None;
    }

    let delimiter = &bytes[start..=end_tag];
    let mut i = end_tag + 1;
    while i + delimiter.len() <= bytes.len() {
        if &bytes[i..i + delimiter.len()] == delimiter {
            return Some(i + delimiter.len());
        }
        i += 1;
    }
    Some(bytes.len())
}

fn is_dollar_tag_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn contains_write_in_with(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    contains_word(&upper, "INSERT")
        || contains_word(&upper, "UPDATE")
        || contains_word(&upper, "DELETE")
        || contains_word(&upper, "MERGE")
        || contains_word(&upper, "COPY")
        || contains_word(&upper, "TRUNCATE")
}

fn select_writes(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    contains_word(&upper, "INTO")
        || upper.contains("NEXTVAL")
        || upper.contains("SET_CONFIG")
        || upper.contains("PG_ADVISORY_LOCK")
        || upper.contains("PG_TRY_ADVISORY_LOCK")
        || upper.contains("PG_NOTIFY")
}

fn classify_copy_writes(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    if upper.contains(" FROM STDIN") || upper.contains(" FROM PROGRAM") || upper.contains(" FROM '") {
        return true;
    }
    if upper.contains(" TO STDOUT") || upper.contains(" TO PROGRAM") || upper.contains(" TO '") {
        return false;
    }
    true
}

fn classify_explain_executes(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    if !upper.starts_with("EXPLAIN") {
        return false;
    }

    let after_explain = upper[7..].trim_start();
    if after_explain.starts_with("ANALYZE") || after_explain.starts_with("ANALYSE") {
        return true;
    }

    if let Some(open) = upper.find('(')
        && let Some(close) = upper[open + 1..].find(')')
    {
        let options = &upper[open + 1..open + 1 + close];
        for opt in options.split(',') {
            let trimmed = opt.trim();
            if trimmed.starts_with("ANALYZE") || trimmed.starts_with("ANALYSE") {
                let rest = trimmed.trim_start_matches("ANALYZE").trim_start_matches("ANALYSE").trim_start();
                return rest.is_empty() || rest == "TRUE" || rest == "ON" || rest == "1";
            }
        }
    }

    false
}

fn strip_explain_prefix(sql: &str) -> Option<&str> {
    let upper = sql.to_ascii_uppercase();
    if !upper.starts_with("EXPLAIN") {
        return None;
    }
    let rest = sql[7..].trim_start();
    if rest.starts_with('(') {
        let close = rest.find(')')?;
        let after = rest[close + 1..].trim_start();
        return (!after.is_empty()).then_some(after);
    }

    let mut remaining = rest;
    let upper_remaining = remaining.to_ascii_uppercase();
    if upper_remaining.starts_with("ANALYZE") || upper_remaining.starts_with("ANALYSE") {
        remaining = remaining[7..].trim_start();
    }
    let upper_remaining = remaining.to_ascii_uppercase();
    if upper_remaining.starts_with("VERBOSE") {
        remaining = remaining[7..].trim_start();
    }

    (!remaining.is_empty()).then_some(remaining)
}

fn contains_word(haystack_upper: &str, needle_upper: &str) -> bool {
    let bytes = haystack_upper.as_bytes();
    let needle = needle_upper.as_bytes();
    if needle.is_empty() || needle.len() > bytes.len() {
        return false;
    }
    let mut i = 0usize;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle
            && (i == 0 || !is_ident_byte(bytes[i - 1]))
            && (i + needle.len() == bytes.len() || !is_ident_byte(bytes[i + needle.len()]))
        {
            return true;
        }
        i += 1;
    }
    false
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_nested_comments() {
        assert_eq!(skip_sql_comments(" /* a /* b */ c */ SELECT 1"), "SELECT 1");
    }

    #[test]
    fn single_statement_ignores_quoted_semicolons() {
        assert!(has_single_statement("SELECT ';'"));
        assert!(has_single_statement("SELECT $$;$$;"));
        assert!(has_single_statement("SELECT 1; -- trailing\n"));
        assert!(!has_single_statement("SELECT 1; SELECT 2"));
    }

    #[test]
    fn classifies_explain_analyze_as_write_when_inner_writes() {
        let plain = classify_sql("EXPLAIN UPDATE users SET name = 'x'");
        assert_eq!(plain.req_type, PgReqType::Read);
        assert_eq!(plain.command, PgCommand::Explain);

        let analyze = classify_sql("EXPLAIN ANALYZE UPDATE users SET name = 'x'");
        assert_eq!(analyze.req_type, PgReqType::Write);
        assert_eq!(analyze.command, PgCommand::ExplainAnalyze);

        let disabled = classify_sql("EXPLAIN (ANALYZE FALSE) DELETE FROM users");
        assert_eq!(disabled.req_type, PgReqType::Read);
    }

    #[test]
    fn classifies_copy_direction() {
        assert_eq!(classify_sql("COPY users TO STDOUT").req_type, PgReqType::Read);
        assert_eq!(classify_sql("COPY users FROM STDIN").req_type, PgReqType::Write);
    }

    #[test]
    fn classifies_with_mutation_conservatively() {
        assert_eq!(classify_sql("WITH rows AS (SELECT 1) SELECT * FROM rows").req_type, PgReqType::Read);
        assert_eq!(
            classify_sql("WITH updated AS (UPDATE users SET name='x' RETURNING *) SELECT * FROM updated").req_type,
            PgReqType::Write
        );
    }
}
