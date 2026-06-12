//! Weaviate HTTP header parsing.
//!
//! Weaviate uses standard HTTP headers for authentication and
//! module-specific API keys for vectorizer integrations.

use std::collections::HashMap;

/// Standard Weaviate HTTP header names.
pub mod names {
    /// Standard Authorization header (Bearer token).
    pub const AUTHORIZATION: &str = "Authorization";
    /// Content-Type header.
    pub const CONTENT_TYPE: &str = "Content-Type";
    /// Tenant header for multi-tenant requests.
    pub const X_WEAVIATE_TENANT: &str = "X-Weaviate-Tenant";
    /// OpenAI API key for text2vec-openai module.
    pub const X_OPENAI_API_KEY: &str = "X-OpenAI-Api-Key";
    /// Cohere API key for text2vec-cohere module.
    pub const X_COHERE_API_KEY: &str = "X-Cohere-Api-Key";
    /// Hugging Face API key for text2vec-huggingface module.
    pub const X_HUGGINGFACE_API_KEY: &str = "X-HuggingFace-Api-Key";
    /// Azure OpenAI API key.
    pub const X_AZURE_API_KEY: &str = "X-Azure-Api-Key";
    /// Palm (Google) API key.
    pub const X_PALM_API_KEY: &str = "X-Palm-Api-Key";
    /// Google API key (newer generative integrations).
    pub const X_GOOGLE_API_KEY: &str = "X-Google-Api-Key";
    /// Jinaai API key.
    pub const X_JINAAI_API_KEY: &str = "X-JinaAI-Api-Key";
    /// VoyageAI API key.
    pub const X_VOYAGEAI_API_KEY: &str = "X-VoyageAI-Api-Key";
    /// AWS Access Key (for AWS-based modules).
    pub const X_AWS_ACCESS_KEY: &str = "X-Aws-Access-Key";
    /// Deprecation warning header.
    pub const DEPRECATION: &str = "Deprecation";
}

/// Parsed Weaviate request headers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WeaviateRequestHeaders {
    /// Bearer token from Authorization header (token portion only, without "Bearer " prefix).
    pub auth_token: Option<String>,
    /// Content type.
    pub content_type: Option<String>,
    /// Tenant name from X-Weaviate-Tenant header.
    pub tenant: Option<String>,
    /// Module-specific API keys (key = lowercase header name, value = key value).
    pub module_api_keys: HashMap<String, String>,
}

impl WeaviateRequestHeaders {
    /// Create new empty headers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse headers from an iterator of (name, value) pairs.
    ///
    /// Header names are compared case-insensitively.
    pub fn parse<'a, I>(headers: I) -> Self
    where
        I: Iterator<Item = (&'a str, &'a str)>,
    {
        let mut result = Self::new();

        for (name, value) in headers {
            if name.eq_ignore_ascii_case(names::AUTHORIZATION) {
                // Extract bearer token, stripping "Bearer " prefix if present.
                result.auth_token =
                    Some(value.strip_prefix("Bearer ").or_else(|| value.strip_prefix("bearer ")).unwrap_or(value).to_string());
            } else if name.eq_ignore_ascii_case(names::CONTENT_TYPE) {
                result.content_type = Some(value.to_string());
            } else if name.eq_ignore_ascii_case(names::X_WEAVIATE_TENANT) {
                result.tenant = Some(value.to_string());
            } else if name.to_ascii_lowercase().contains("-api-key") || name.to_ascii_lowercase().contains("-access-key") {
                // Collect module-specific API keys by lowercase header name.
                result.module_api_keys.insert(name.to_ascii_lowercase(), value.to_string());
            }
        }

        result
    }

    /// Set auth token.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Set content type.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Set tenant.
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Add a module API key.
    pub fn with_module_api_key(mut self, header: impl Into<String>, key: impl Into<String>) -> Self {
        self.module_api_keys.insert(header.into(), key.into());
        self
    }
}

/// Parsed Weaviate response headers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WeaviateResponseHeaders {
    /// Content type of response.
    pub content_type: Option<String>,
    /// Deprecation warning.
    pub deprecation: Option<String>,
}

impl WeaviateResponseHeaders {
    /// Create new empty headers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse headers from an iterator of (name, value) pairs.
    pub fn parse<'a, I>(headers: I) -> Self
    where
        I: Iterator<Item = (&'a str, &'a str)>,
    {
        let mut result = Self::new();

        for (name, value) in headers {
            if name.eq_ignore_ascii_case(names::CONTENT_TYPE) {
                result.content_type = Some(value.to_string());
            } else if name.eq_ignore_ascii_case(names::DEPRECATION) {
                result.deprecation = Some(value.to_string());
            }
        }

        result
    }

    /// Check if response indicates an error.
    ///
    /// Weaviate signals errors via HTTP status codes, not custom headers.
    /// This always returns false; use HTTP status code for error detection.
    pub fn is_error(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_request_headers_bearer() {
        let headers = vec![
            ("Authorization", "Bearer my-api-key-123"),
            ("Content-Type", "application/json"),
            ("X-Weaviate-Tenant", "tenantA"),
        ];

        let parsed = WeaviateRequestHeaders::parse(headers.into_iter());

        assert_eq!(parsed.auth_token, Some("my-api-key-123".to_string()));
        assert_eq!(parsed.content_type, Some("application/json".to_string()));
        assert_eq!(parsed.tenant, Some("tenantA".to_string()));
    }

    #[test]
    fn test_parse_request_headers_bearer_lowercase() {
        let headers = vec![("authorization", "bearer my-token")];

        let parsed = WeaviateRequestHeaders::parse(headers.into_iter());

        assert_eq!(parsed.auth_token, Some("my-token".to_string()));
    }

    #[test]
    fn test_parse_request_headers_no_bearer_prefix() {
        let headers = vec![("Authorization", "raw-key-value")];

        let parsed = WeaviateRequestHeaders::parse(headers.into_iter());

        assert_eq!(parsed.auth_token, Some("raw-key-value".to_string()));
    }

    #[test]
    fn test_parse_module_api_keys() {
        let headers = vec![
            ("X-OpenAI-Api-Key", "sk-openai-123"),
            ("X-Cohere-Api-Key", "cohere-456"),
            ("X-Aws-Access-Key", "AKIA-789"),
        ];

        let parsed = WeaviateRequestHeaders::parse(headers.into_iter());

        assert_eq!(parsed.module_api_keys.len(), 3);
        assert_eq!(parsed.module_api_keys.get("x-openai-api-key"), Some(&"sk-openai-123".to_string()));
        assert_eq!(parsed.module_api_keys.get("x-cohere-api-key"), Some(&"cohere-456".to_string()));
        assert_eq!(parsed.module_api_keys.get("x-aws-access-key"), Some(&"AKIA-789".to_string()));
    }

    #[test]
    fn test_parse_case_insensitive() {
        let headers = vec![("x-weaviate-tenant", "myTenant"), ("CONTENT-TYPE", "application/json")];

        let parsed = WeaviateRequestHeaders::parse(headers.into_iter());

        assert_eq!(parsed.tenant, Some("myTenant".to_string()));
        assert_eq!(parsed.content_type, Some("application/json".to_string()));
    }

    #[test]
    fn test_parse_response_headers() {
        let headers = vec![
            ("Content-Type", "application/json"),
            ("Deprecation", "endpoint will be removed in v2"),
        ];

        let parsed = WeaviateResponseHeaders::parse(headers.into_iter());

        assert_eq!(parsed.content_type, Some("application/json".to_string()));
        assert_eq!(parsed.deprecation, Some("endpoint will be removed in v2".to_string()));
        assert!(!parsed.is_error());
    }

    #[test]
    fn test_builder_pattern() {
        let headers = WeaviateRequestHeaders::new()
            .with_auth_token("my-token")
            .with_content_type("application/json")
            .with_tenant("tenantA")
            .with_module_api_key("x-openai-api-key", "sk-123");

        assert_eq!(headers.auth_token, Some("my-token".to_string()));
        assert_eq!(headers.content_type, Some("application/json".to_string()));
        assert_eq!(headers.tenant, Some("tenantA".to_string()));
        assert_eq!(headers.module_api_keys.get("x-openai-api-key"), Some(&"sk-123".to_string()));
    }
}
