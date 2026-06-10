//! X-ClickHouse-* header parsing.
//!
//! ClickHouse uses custom HTTP headers for configuration and metadata.

use crate::error::ClickhouseWireError;

/// Standard ClickHouse HTTP header names.
pub mod names {
    /// Output format (e.g., "JSONEachRow", "TabSeparated").
    pub const FORMAT: &str = "X-ClickHouse-Format";
    /// Database name.
    pub const DATABASE: &str = "X-ClickHouse-Database";
    /// Username for authentication.
    pub const USER: &str = "X-ClickHouse-User";
    /// Password/key for authentication.
    pub const KEY: &str = "X-ClickHouse-Key";
    /// Quota key.
    pub const QUOTA_KEY: &str = "X-ClickHouse-Quota";
    /// Session ID.
    pub const SESSION_ID: &str = "X-ClickHouse-Session-Id";
    /// Session check flag.
    pub const SESSION_CHECK: &str = "X-ClickHouse-Session-Check";
    /// Session timeout.
    pub const SESSION_TIMEOUT: &str = "X-ClickHouse-Session-Timeout";
    /// Query progress (JSON).
    pub const PROGRESS: &str = "X-ClickHouse-Progress";
    /// Exception code.
    pub const EXCEPTION_CODE: &str = "X-ClickHouse-Exception-Code";
    /// Query ID.
    pub const QUERY_ID: &str = "X-ClickHouse-Query-Id";
    /// Query summary (JSON).
    pub const SUMMARY: &str = "X-ClickHouse-Summary";
    /// Server timezone.
    pub const TIMEZONE: &str = "X-ClickHouse-Timezone";
    /// Server display name.
    pub const SERVER_DISPLAY_NAME: &str = "X-ClickHouse-Server-Display-Name";
}

/// Parsed ClickHouse request headers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClickhouseRequestHeaders {
    /// Database name.
    pub database: Option<String>,
    /// Username.
    pub user: Option<String>,
    /// Password/key.
    pub password: Option<String>,
    /// Output format.
    pub format: Option<String>,
    /// Quota key.
    pub quota_key: Option<String>,
    /// Session ID.
    pub session_id: Option<String>,
    /// Session check flag.
    pub session_check: bool,
    /// Session timeout in seconds.
    pub session_timeout: Option<u64>,
    /// Query ID.
    pub query_id: Option<String>,
}

impl ClickhouseRequestHeaders {
    /// Create new empty headers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse headers from an iterator of (name, value) pairs.
    ///
    /// Header names are compared case-insensitively.
    pub fn parse<'a, I>(headers: I) -> Result<Self, ClickhouseWireError>
    where
        I: Iterator<Item = (&'a str, &'a str)>,
    {
        let mut result = Self::new();

        for (name, value) in headers {
            if name.eq_ignore_ascii_case(names::DATABASE) {
                result.database = Some(value.to_string());
            } else if name.eq_ignore_ascii_case(names::USER) {
                result.user = Some(value.to_string());
            } else if name.eq_ignore_ascii_case(names::KEY) {
                result.password = Some(value.to_string());
            } else if name.eq_ignore_ascii_case(names::FORMAT) {
                result.format = Some(value.to_string());
            } else if name.eq_ignore_ascii_case(names::QUOTA_KEY) {
                result.quota_key = Some(value.to_string());
            } else if name.eq_ignore_ascii_case(names::SESSION_ID) {
                result.session_id = Some(value.to_string());
            } else if name.eq_ignore_ascii_case(names::SESSION_CHECK) {
                result.session_check = value == "1";
            } else if name.eq_ignore_ascii_case(names::SESSION_TIMEOUT) {
                result.session_timeout = value.parse().ok();
            } else if name.eq_ignore_ascii_case(names::QUERY_ID) {
                result.query_id = Some(value.to_string());
            }
        }

        Ok(result)
    }

    /// Set database.
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Set user.
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Set password.
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set format.
    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }

    /// Set session ID.
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }
}

/// Parsed ClickHouse response headers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClickhouseResponseHeaders {
    /// Output format.
    pub format: Option<String>,
    /// Query ID.
    pub query_id: Option<String>,
    /// Exception code (if error).
    pub exception_code: Option<i32>,
    /// Query summary (JSON string).
    pub summary: Option<String>,
    /// Server timezone.
    pub timezone: Option<String>,
    /// Server display name.
    pub server_display_name: Option<String>,
}

impl ClickhouseResponseHeaders {
    /// Create new empty headers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse headers from an iterator of (name, value) pairs.
    pub fn parse<'a, I>(headers: I) -> Result<Self, ClickhouseWireError>
    where
        I: Iterator<Item = (&'a str, &'a str)>,
    {
        let mut result = Self::new();

        for (name, value) in headers {
            if name.eq_ignore_ascii_case(names::FORMAT) {
                result.format = Some(value.to_string());
            } else if name.eq_ignore_ascii_case(names::QUERY_ID) {
                result.query_id = Some(value.to_string());
            } else if name.eq_ignore_ascii_case(names::EXCEPTION_CODE) {
                result.exception_code = value.parse().ok();
            } else if name.eq_ignore_ascii_case(names::SUMMARY) {
                result.summary = Some(value.to_string());
            } else if name.eq_ignore_ascii_case(names::TIMEZONE) {
                result.timezone = Some(value.to_string());
            } else if name.eq_ignore_ascii_case(names::SERVER_DISPLAY_NAME) {
                result.server_display_name = Some(value.to_string());
            }
        }

        Ok(result)
    }

    /// Check if response indicates an error.
    pub fn is_error(&self) -> bool {
        self.exception_code.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_request_headers() {
        let headers = vec![
            ("X-ClickHouse-Database", "mydb"),
            ("X-ClickHouse-User", "admin"),
            ("X-ClickHouse-Key", "secret"),
            ("X-ClickHouse-Format", "JSONEachRow"),
            ("X-ClickHouse-Session-Id", "session-123"),
            ("X-ClickHouse-Session-Check", "1"),
            ("X-ClickHouse-Session-Timeout", "3600"),
        ];

        let parsed = ClickhouseRequestHeaders::parse(headers.into_iter()).unwrap();

        assert_eq!(parsed.database, Some("mydb".to_string()));
        assert_eq!(parsed.user, Some("admin".to_string()));
        assert_eq!(parsed.password, Some("secret".to_string()));
        assert_eq!(parsed.format, Some("JSONEachRow".to_string()));
        assert_eq!(parsed.session_id, Some("session-123".to_string()));
        assert!(parsed.session_check);
        assert_eq!(parsed.session_timeout, Some(3600));
    }

    #[test]
    fn test_parse_case_insensitive() {
        let headers = vec![("x-clickhouse-database", "db1"), ("X-CLICKHOUSE-USER", "user1")];

        let parsed = ClickhouseRequestHeaders::parse(headers.into_iter()).unwrap();

        assert_eq!(parsed.database, Some("db1".to_string()));
        assert_eq!(parsed.user, Some("user1".to_string()));
    }

    #[test]
    fn test_parse_response_headers() {
        let headers = vec![
            ("X-ClickHouse-Format", "JSON"),
            ("X-ClickHouse-Query-Id", "query-456"),
            ("X-ClickHouse-Timezone", "UTC"),
            ("X-ClickHouse-Server-Display-Name", "clickhouse-server"),
        ];

        let parsed = ClickhouseResponseHeaders::parse(headers.into_iter()).unwrap();

        assert_eq!(parsed.format, Some("JSON".to_string()));
        assert_eq!(parsed.query_id, Some("query-456".to_string()));
        assert_eq!(parsed.timezone, Some("UTC".to_string()));
        assert!(!parsed.is_error());
    }

    #[test]
    fn test_response_error() {
        let headers = vec![("X-ClickHouse-Exception-Code", "62")];

        let parsed = ClickhouseResponseHeaders::parse(headers.into_iter()).unwrap();

        assert!(parsed.is_error());
        assert_eq!(parsed.exception_code, Some(62));
    }

    #[test]
    fn test_builder_pattern() {
        let headers = ClickhouseRequestHeaders::new().with_database("mydb").with_user("admin").with_format("TSV");

        assert_eq!(headers.database, Some("mydb".to_string()));
        assert_eq!(headers.user, Some("admin".to_string()));
        assert_eq!(headers.format, Some("TSV".to_string()));
    }
}
