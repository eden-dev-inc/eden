//! HTTP query parameter handling for Weaviate.
//!
//! Weaviate accepts various query parameters for filtering, pagination,
//! and configuration.

use std::collections::HashMap;

/// Standard Weaviate query parameters.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryParams {
    /// Class name filter.
    pub class_name: Option<String>,
    /// Consistency level: ONE, QUORUM, ALL.
    pub consistency_level: Option<String>,
    /// Tenant name for multi-tenant operations.
    pub tenant: Option<String>,
    /// Limit for list operations.
    pub limit: Option<u64>,
    /// Offset for pagination.
    pub offset: Option<u64>,
    /// After cursor for cursor-based pagination.
    pub after: Option<String>,
    /// Include additional properties in response (e.g., "vector", "classification").
    pub include: Option<String>,
    /// Node name filter (for /v1/nodes).
    pub node_name: Option<String>,
    /// Output verbosity (for /v1/nodes): "minimal" or "verbose".
    pub output: Option<String>,
    /// Additional parameters (catch-all).
    pub additional: HashMap<String, String>,
}

impl QueryParams {
    /// Create new empty query params.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse query parameters from an iterator of (key, value) pairs.
    pub fn parse<'a, I>(params: I) -> Self
    where
        I: Iterator<Item = (&'a str, &'a str)>,
    {
        let mut result = Self::new();

        for (key, value) in params {
            match key {
                "class" | "className" => result.class_name = Some(value.to_string()),
                "consistency_level" => result.consistency_level = Some(value.to_string()),
                "tenant" => result.tenant = Some(value.to_string()),
                "limit" => result.limit = value.parse().ok(),
                "offset" => result.offset = value.parse().ok(),
                "after" => result.after = Some(value.to_string()),
                "include" => result.include = Some(value.to_string()),
                "node_name" => result.node_name = Some(value.to_string()),
                "output" => result.output = Some(value.to_string()),
                _ => {
                    result.additional.insert(key.to_string(), value.to_string());
                }
            }
        }

        result
    }

    /// Parse from a query string (e.g., "limit=10&offset=0&tenant=abc").
    pub fn parse_query_string(query_string: &str) -> Self {
        let pairs = query_string.split('&').filter(|s| !s.is_empty()).filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let value = parts.next().unwrap_or("");
            Some((key, value))
        });

        Self::parse(pairs)
    }

    /// Set class name.
    pub fn class_name(mut self, class_name: impl Into<String>) -> Self {
        self.class_name = Some(class_name.into());
        self
    }

    /// Set consistency level.
    pub fn consistency_level(mut self, level: impl Into<String>) -> Self {
        self.consistency_level = Some(level.into());
        self
    }

    /// Set tenant.
    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Set limit.
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set offset.
    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Set after cursor.
    pub fn after(mut self, after: impl Into<String>) -> Self {
        self.after = Some(after.into());
        self
    }

    /// Set include.
    pub fn include(mut self, include: impl Into<String>) -> Self {
        self.include = Some(include.into());
        self
    }

    /// Build a query string from the params.
    pub fn to_query_string(&self) -> String {
        let mut parts = Vec::new();

        if let Some(ref cn) = self.class_name {
            parts.push(format!("class={}", urlencoding_light(cn)));
        }
        if let Some(ref cl) = self.consistency_level {
            parts.push(format!("consistency_level={}", cl));
        }
        if let Some(ref t) = self.tenant {
            parts.push(format!("tenant={}", urlencoding_light(t)));
        }
        if let Some(l) = self.limit {
            parts.push(format!("limit={}", l));
        }
        if let Some(o) = self.offset {
            parts.push(format!("offset={}", o));
        }
        if let Some(ref a) = self.after {
            parts.push(format!("after={}", urlencoding_light(a)));
        }
        if let Some(ref i) = self.include {
            parts.push(format!("include={}", urlencoding_light(i)));
        }
        if let Some(ref nn) = self.node_name {
            parts.push(format!("node_name={}", urlencoding_light(nn)));
        }
        if let Some(ref o) = self.output {
            parts.push(format!("output={}", o));
        }

        for (k, v) in &self.additional {
            parts.push(format!("{}={}", k, urlencoding_light(v)));
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
            ("class", "Article"),
            ("limit", "10"),
            ("offset", "20"),
            ("tenant", "tenantA"),
            ("consistency_level", "QUORUM"),
            ("include", "vector"),
        ];

        let parsed = QueryParams::parse(params.into_iter());

        assert_eq!(parsed.class_name, Some("Article".to_string()));
        assert_eq!(parsed.limit, Some(10));
        assert_eq!(parsed.offset, Some(20));
        assert_eq!(parsed.tenant, Some("tenantA".to_string()));
        assert_eq!(parsed.consistency_level, Some("QUORUM".to_string()));
        assert_eq!(parsed.include, Some("vector".to_string()));
    }

    #[test]
    fn test_parse_query_string() {
        let qs = "limit=5&offset=0&tenant=myTenant";
        let parsed = QueryParams::parse_query_string(qs);

        assert_eq!(parsed.limit, Some(5));
        assert_eq!(parsed.offset, Some(0));
        assert_eq!(parsed.tenant, Some("myTenant".to_string()));
    }

    #[test]
    fn test_parse_class_name_alias() {
        let params = vec![("className", "Article")];
        let parsed = QueryParams::parse(params.into_iter());
        assert_eq!(parsed.class_name, Some("Article".to_string()));
    }

    #[test]
    fn test_additional_params() {
        let params = vec![("limit", "10"), ("custom_param", "value1")];
        let parsed = QueryParams::parse(params.into_iter());

        assert_eq!(parsed.limit, Some(10));
        assert_eq!(parsed.additional.get("custom_param"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_builder_pattern() {
        let params = QueryParams::new().class_name("Article").tenant("tenantA").limit(10).offset(0).include("vector");

        assert_eq!(params.class_name, Some("Article".to_string()));
        assert_eq!(params.tenant, Some("tenantA".to_string()));
        assert_eq!(params.limit, Some(10));
        assert_eq!(params.offset, Some(0));
        assert_eq!(params.include, Some("vector".to_string()));
    }

    #[test]
    fn test_to_query_string() {
        let params = QueryParams::new().class_name("Article").tenant("myTenant").limit(10);

        let qs = params.to_query_string();
        assert!(qs.contains("class=Article"));
        assert!(qs.contains("tenant=myTenant"));
        assert!(qs.contains("limit=10"));
    }

    #[test]
    fn test_empty_query_string() {
        let parsed = QueryParams::parse_query_string("");
        assert_eq!(parsed, QueryParams::new());
    }

    #[test]
    fn test_to_query_string_roundtrip() {
        let params = QueryParams::new().limit(25).offset(50).tenant("test");
        let qs = params.to_query_string();
        let parsed = QueryParams::parse_query_string(&qs);

        assert_eq!(parsed.limit, Some(25));
        assert_eq!(parsed.offset, Some(50));
        assert_eq!(parsed.tenant, Some("test".to_string()));
    }
}
