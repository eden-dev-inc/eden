use eden_core::format::cache_uuid::EndpointCacheUuid;
use eden_logger_internal::LogContext;

cfg_if::cfg_if! {
    if #[cfg(embedded_db)] {
        #[path = "../processor_els_lookup_embedded_db.rs"]
        mod els_lookup;
    } else {
        #[path = "../processor_els_lookup.rs"]
        mod els_lookup;
    }
}

pub type ElsRedisPool = database::lib::RedisConn;

pub(crate) fn detect_service_name(startup_params: &[(String, String)], ctx: &LogContext) -> String {
    for (key, value) in startup_params {
        if key.eq_ignore_ascii_case("application_name") {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }

    const SERVICE_KEYS: [&str; 3] = ["x-eden-service", "eden-service", "service"];
    for key in SERVICE_KEYS {
        if let Some(value) = ctx.additional.as_ref().and_then(|m| m.get(key)) {
            let trimmed = value.as_str().trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }

    "unknown".to_string()
}

#[allow(dead_code)]
pub(crate) fn escape_sql_literal(s: &str) -> String {
    s.replace('\0', "").replace('\'', "''")
}

pub(crate) fn sql_has_els_override_attempt(sql: &str) -> bool {
    let normalized = normalize_sql_for_els_check(sql);
    normalized.contains("set app.") || normalized.contains("set local app.") || normalized.contains("reset app.")
}

#[allow(dead_code)]
pub(crate) fn escape_sql_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('\0', "").replace('"', "\"\""))
}

#[allow(dead_code)]
pub(crate) fn build_custom_els_prefix(variables: &std::collections::HashMap<String, String>) -> (String, usize) {
    let mut prefix = String::new();
    let mut count = 0;
    for (name, value) in variables {
        if name.is_empty() {
            continue;
        }
        let safe_name = escape_sql_identifier(name);
        prefix.push_str(&format!("SET {} = '{}'; ", safe_name, escape_sql_literal(value)));
        count += 1;
    }
    (prefix, count)
}

pub(crate) async fn resolve_els_prefix(
    rbac_redis: Option<&ElsRedisPool>,
    endpoint: &EndpointCacheUuid,
    user: Option<&str>,
    org_key_provider: Option<&dyn database::encryption::OrgKeyProvider>,
) -> Option<(String, usize)> {
    let pool = rbac_redis?;
    let user = user?;
    let policy = els_lookup::lookup_els_credentials(pool, endpoint, user, org_key_provider).await?;
    let auth = policy.resolve().ok()?;
    let pg_auth = auth.as_any().downcast_ref::<ep_core::ep_auth::PostgresAuth>()?;
    let (prefix, count) = pg_auth.sql_prefix();
    if count > 0 { Some((prefix, count)) } else { None }
}

fn normalize_sql_for_els_check(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < len {
                i += 2;
            }
            if !out.ends_with(' ') {
                out.push(' ');
            }
            continue;
        }

        if i + 1 < len && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            if !out.ends_with(' ') {
                out.push(' ');
            }
            continue;
        }

        if bytes[i].is_ascii_whitespace() {
            if !out.ends_with(' ') {
                out.push(' ');
            }
            i += 1;
            continue;
        }

        out.push(bytes[i] as char);
        i += 1;
    }

    out.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::wire::build_q_message;
    use endpoints::endpoint::postgres::protocol::PostgresBytes;

    #[test]
    fn escape_sql_literal_escapes_values() {
        assert_eq!(escape_sql_literal("alice"), "alice");
        assert_eq!(escape_sql_literal("O'Brien"), "O''Brien");
        assert_eq!(escape_sql_literal("it's a 'test'"), "it''s a ''test''");
        assert_eq!(escape_sql_literal(""), "");
        assert_eq!(escape_sql_literal("null\0byte"), "nullbyte");
    }

    #[test]
    fn build_q_message_with_els_prefix() {
        let mut vars = std::collections::HashMap::new();
        vars.insert("app.organization_uuid".to_string(), "t-123".to_string());
        let (prefix, _) = build_custom_els_prefix(&vars);
        let modified = format!("{}SELECT 1", prefix);
        let msg = build_q_message(&modified);
        let pg_bytes = PostgresBytes::from(msg);
        let extracted = pg_bytes.extract_sql().expect("extract_sql");
        assert!(extracted.starts_with("SET \"app.organization_uuid\""));
        assert!(extracted.ends_with("SELECT 1"));
    }

    #[test]
    fn sql_has_els_override_attempt_positive() {
        assert!(sql_has_els_override_attempt("SET app.eden_user = 'alice'; SELECT 1"));
        assert!(sql_has_els_override_attempt("SET APP.EDEN_USER = 'alice'; SELECT 1"));
        assert!(sql_has_els_override_attempt("SET app.eden_org = 'org-1'; SELECT 1"));
        assert!(sql_has_els_override_attempt("SET app.organization_uuid = 'x'; SELECT 1"));
        assert!(sql_has_els_override_attempt("SET LOCAL app.eden_user = 'alice'; SELECT 1"));
        assert!(sql_has_els_override_attempt("RESET app.eden_user; SELECT 1"));
    }

    #[test]
    fn sql_has_els_override_attempt_bypass_block_comment() {
        assert!(sql_has_els_override_attempt("SET/**/app.eden_user = 'evil'"));
        assert!(sql_has_els_override_attempt("SET /* sneaky */ app.eden_user = 'evil'"));
        assert!(sql_has_els_override_attempt("SET/*comment*/LOCAL/**/app.organization_uuid = 'x'"));
        assert!(sql_has_els_override_attempt("RESET/**/app.eden_user"));
    }

    #[test]
    fn sql_has_els_override_attempt_bypass_line_comment() {
        assert!(sql_has_els_override_attempt("SET -- comment\napp.eden_user = 'evil'"));
        assert!(sql_has_els_override_attempt("RESET -- bypass\napp.eden_user"));
    }

    #[test]
    fn sql_has_els_override_attempt_bypass_whitespace() {
        assert!(sql_has_els_override_attempt("SET\tapp.eden_user = 'evil'"));
        assert!(sql_has_els_override_attempt("SET\napp.eden_user = 'evil'"));
        assert!(sql_has_els_override_attempt("SET\r\napp.eden_user = 'evil'"));
        assert!(sql_has_els_override_attempt("SET  \t  app.eden_user = 'evil'"));
        assert!(sql_has_els_override_attempt("SET\tLOCAL\tapp.eden_user = 'evil'"));
    }

    #[test]
    fn sql_has_els_override_attempt_negative() {
        assert!(!sql_has_els_override_attempt("SELECT 1"));
        assert!(!sql_has_els_override_attempt("SET search_path = public"));
        assert!(!sql_has_els_override_attempt("INSERT INTO users VALUES (1)"));
        assert!(!sql_has_els_override_attempt("SELECT /* app. */ 1"));
        assert!(!sql_has_els_override_attempt("SELECT 1 -- set app.foo"));
    }

    #[test]
    fn escape_sql_identifier_quotes_names() {
        assert_eq!(escape_sql_identifier("app.organization_uuid"), "\"app.organization_uuid\"");
        assert_eq!(escape_sql_identifier("app.role"), "\"app.role\"");
        assert_eq!(escape_sql_identifier("normal_var"), "\"normal_var\"");
        assert_eq!(escape_sql_identifier("app.foo; DROP TABLE users--"), "\"app.foo; DROP TABLE users--\"");
        assert_eq!(escape_sql_identifier(""), "\"\"");
        assert_eq!(escape_sql_identifier("has\"quote"), "\"has\"\"quote\"");
        assert_eq!(escape_sql_identifier("null\0byte"), "\"nullbyte\"");
    }

    #[test]
    fn build_custom_els_prefix_escapes_names_and_values() {
        let mut vars = std::collections::HashMap::new();
        vars.insert("app.organization_uuid".to_string(), "t-123".to_string());
        let (prefix, count) = build_custom_els_prefix(&vars);
        assert_eq!(count, 1);
        assert!(prefix.contains("SET \"app.organization_uuid\" = 't-123'"));
        assert!(prefix.ends_with("; "));

        let msg = build_q_message(&format!("{}SELECT 1", prefix));
        let pg_bytes = PostgresBytes::from(msg);
        let extracted = pg_bytes.extract_sql().expect("extract_sql");
        assert!(extracted.contains("SET \"app.organization_uuid\""), "expected quoted identifier, got: {extracted}");
        assert!(extracted.ends_with("SELECT 1"));
    }

    #[test]
    fn build_custom_els_prefix_skips_empty_names() {
        let mut vars = std::collections::HashMap::new();
        vars.insert(String::new(), "value".to_string());
        vars.insert("app.valid".to_string(), "ok".to_string());
        let (prefix, count) = build_custom_els_prefix(&vars);
        assert_eq!(count, 1);
        assert!(prefix.contains("SET \"app.valid\" = 'ok'"));
        assert!(!prefix.contains("\"\""));
    }
}
