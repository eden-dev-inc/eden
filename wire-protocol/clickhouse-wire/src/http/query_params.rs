//! HTTP query parameter handling for ClickHouse.
//!
//! ClickHouse accepts various query parameters for configuration.

use crate::error::ClickhouseWireError;
use std::collections::HashMap;

/// Standard ClickHouse query parameters.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryParams {
    /// SQL query (if not in body).
    pub query: Option<String>,
    /// Database name.
    pub database: Option<String>,
    /// Username.
    pub user: Option<String>,
    /// Password.
    pub password: Option<String>,
    /// Output format.
    pub format: Option<String>,
    /// Default output format.
    pub default_format: Option<String>,
    /// Session ID for stateful queries.
    pub session_id: Option<String>,
    /// Session timeout in seconds.
    pub session_timeout: Option<u64>,
    /// Whether to check session before query.
    pub session_check: bool,
    /// Enable compression for response.
    pub compress: bool,
    /// Decompress request body.
    pub decompress: bool,
    /// Query ID.
    pub query_id: Option<String>,
    /// Quota key.
    pub quota_key: Option<String>,
    /// Wait for end of query (for buffering).
    pub wait_end_of_query: bool,
    /// Buffer size.
    pub buffer_size: Option<usize>,
    /// Max result rows.
    pub max_result_rows: Option<u64>,
    /// Max result bytes.
    pub max_result_bytes: Option<u64>,
    /// Additional settings (key=value).
    pub settings: HashMap<String, String>,
}

impl QueryParams {
    /// Create new empty query params.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with a query.
    pub fn with_query(query: impl Into<String>) -> Self {
        Self { query: Some(query.into()), ..Default::default() }
    }

    /// Parse query parameters from an iterator of (key, value) pairs.
    pub fn parse<'a, I>(params: I) -> Result<Self, ClickhouseWireError>
    where
        I: Iterator<Item = (&'a str, &'a str)>,
    {
        let mut result = Self::new();

        for (key, value) in params {
            match key {
                "query" => result.query = Some(value.to_string()),
                "database" => result.database = Some(value.to_string()),
                "user" => result.user = Some(value.to_string()),
                "password" => result.password = Some(value.to_string()),
                "format" => result.format = Some(value.to_string()),
                "default_format" => result.default_format = Some(value.to_string()),
                "session_id" => result.session_id = Some(value.to_string()),
                "session_timeout" => result.session_timeout = value.parse().ok(),
                "session_check" => result.session_check = value == "1",
                "compress" => result.compress = value == "1",
                "decompress" => result.decompress = value == "1",
                "query_id" => result.query_id = Some(value.to_string()),
                "quota_key" => result.quota_key = Some(value.to_string()),
                "wait_end_of_query" => result.wait_end_of_query = value == "1",
                "buffer_size" => result.buffer_size = value.parse().ok(),
                "max_result_rows" => result.max_result_rows = value.parse().ok(),
                "max_result_bytes" => result.max_result_bytes = value.parse().ok(),
                // Collect other params as settings
                _ => {
                    result.settings.insert(key.to_string(), value.to_string());
                }
            }
        }

        Ok(result)
    }

    /// Parse from a query string (e.g., "key1=value1&key2=value2").
    pub fn parse_query_string(query_string: &str) -> Result<Self, ClickhouseWireError> {
        let pairs = query_string.split('&').filter(|s| !s.is_empty()).filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let value = parts.next().unwrap_or("");
            Some((key, value))
        });

        Self::parse(pairs)
    }

    /// Set database.
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Set format.
    pub fn format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }

    /// Set user.
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Set password.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Enable compression.
    pub fn compressed(mut self) -> Self {
        self.compress = true;
        self
    }

    /// Add a setting.
    pub fn setting(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.insert(key.into(), value.into());
        self
    }

    /// Get the effective format (format or default_format).
    pub fn effective_format(&self) -> Option<&str> {
        self.format.as_deref().or(self.default_format.as_deref())
    }

    /// Build a query string from the params.
    pub fn to_query_string(&self) -> String {
        let mut parts = Vec::new();

        if let Some(ref q) = self.query {
            parts.push(format!("query={}", urlencoding_light(q)));
        }
        if let Some(ref db) = self.database {
            parts.push(format!("database={}", db));
        }
        if let Some(ref u) = self.user {
            parts.push(format!("user={}", u));
        }
        if let Some(ref p) = self.password {
            parts.push(format!("password={}", p));
        }
        if let Some(ref f) = self.format {
            parts.push(format!("format={}", f));
        }
        if let Some(ref s) = self.session_id {
            parts.push(format!("session_id={}", s));
        }
        if self.compress {
            parts.push("compress=1".to_string());
        }
        if self.decompress {
            parts.push("decompress=1".to_string());
        }
        if let Some(ref qid) = self.query_id {
            parts.push(format!("query_id={}", qid));
        }

        for (k, v) in &self.settings {
            parts.push(format!("{}={}", k, v));
        }

        parts.join("&")
    }
}

/// Simple URL encoding for query strings.
fn urlencoding_light(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 3);
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                result.push(c);
            }
            ' ' => result.push('+'),
            _ => {
                for b in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_params() {
        let params = vec![
            ("query", "SELECT 1"),
            ("database", "mydb"),
            ("user", "admin"),
            ("format", "JSON"),
            ("compress", "1"),
            ("max_threads", "4"),
        ];

        let parsed = QueryParams::parse(params.into_iter()).unwrap();

        assert_eq!(parsed.query, Some("SELECT 1".to_string()));
        assert_eq!(parsed.database, Some("mydb".to_string()));
        assert_eq!(parsed.user, Some("admin".to_string()));
        assert_eq!(parsed.format, Some("JSON".to_string()));
        assert!(parsed.compress);
        assert_eq!(parsed.settings.get("max_threads"), Some(&"4".to_string()));
    }

    #[test]
    fn test_parse_query_string() {
        let qs = "database=test&format=TSV&compress=1";
        let parsed = QueryParams::parse_query_string(qs).unwrap();

        assert_eq!(parsed.database, Some("test".to_string()));
        assert_eq!(parsed.format, Some("TSV".to_string()));
        assert!(parsed.compress);
    }

    #[test]
    fn test_builder_pattern() {
        let params = QueryParams::with_query("SELECT 1").database("mydb").format("JSON").compressed().setting("max_threads", "8");

        assert_eq!(params.query, Some("SELECT 1".to_string()));
        assert_eq!(params.database, Some("mydb".to_string()));
        assert!(params.compress);
        assert_eq!(params.settings.get("max_threads"), Some(&"8".to_string()));
    }

    #[test]
    fn test_effective_format() {
        let params1 = QueryParams {
            format: Some("JSON".to_string()),
            default_format: Some("TSV".to_string()),
            ..Default::default()
        };
        assert_eq!(params1.effective_format(), Some("JSON"));

        let params2 = QueryParams {
            default_format: Some("TSV".to_string()),
            ..Default::default()
        };
        assert_eq!(params2.effective_format(), Some("TSV"));

        let params3 = QueryParams::default();
        assert_eq!(params3.effective_format(), None);
    }

    #[test]
    fn test_to_query_string() {
        let params = QueryParams::with_query("SELECT 1").database("test").compressed();

        let qs = params.to_query_string();
        assert!(qs.contains("query=SELECT+1"));
        assert!(qs.contains("database=test"));
        assert!(qs.contains("compress=1"));
    }
}
