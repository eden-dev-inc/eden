//! Weaviate gRPC metadata parsing.
//!
//! Parses gRPC metadata (headers) for authentication and routing.

/// Parsed Weaviate gRPC metadata (request headers).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GrpcMetadata {
    /// Authorization token.
    pub auth_token: Option<String>,
    /// Tenant name.
    pub tenant: Option<String>,
}

impl GrpcMetadata {
    /// Create new empty metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse from iterator of (key, value) string pairs.
    ///
    /// gRPC metadata keys are lowercase by convention.
    pub fn parse<'a, I>(metadata: I) -> Self
    where
        I: Iterator<Item = (&'a str, &'a str)>,
    {
        let mut result = Self::new();

        for (key, value) in metadata {
            match key {
                "authorization" => {
                    result.auth_token =
                        Some(value.strip_prefix("Bearer ").or_else(|| value.strip_prefix("bearer ")).unwrap_or(value).to_string());
                }
                "x-weaviate-tenant" | "tenant" => {
                    result.tenant = Some(value.to_string());
                }
                _ => {}
            }
        }

        result
    }

    /// Set auth token.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Set tenant.
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_auth_bearer() {
        let metadata = vec![("authorization", "Bearer my-grpc-token")];

        let parsed = GrpcMetadata::parse(metadata.into_iter());

        assert_eq!(parsed.auth_token, Some("my-grpc-token".to_string()));
    }

    #[test]
    fn test_parse_auth_raw() {
        let metadata = vec![("authorization", "raw-key")];

        let parsed = GrpcMetadata::parse(metadata.into_iter());

        assert_eq!(parsed.auth_token, Some("raw-key".to_string()));
    }

    #[test]
    fn test_parse_tenant() {
        let metadata = vec![("x-weaviate-tenant", "tenantA")];

        let parsed = GrpcMetadata::parse(metadata.into_iter());

        assert_eq!(parsed.tenant, Some("tenantA".to_string()));
    }

    #[test]
    fn test_parse_tenant_shorthand() {
        let metadata = vec![("tenant", "tenantB")];

        let parsed = GrpcMetadata::parse(metadata.into_iter());

        assert_eq!(parsed.tenant, Some("tenantB".to_string()));
    }

    #[test]
    fn test_parse_combined() {
        let metadata = vec![("authorization", "Bearer token-123"), ("x-weaviate-tenant", "tenantC")];

        let parsed = GrpcMetadata::parse(metadata.into_iter());

        assert_eq!(parsed.auth_token, Some("token-123".to_string()));
        assert_eq!(parsed.tenant, Some("tenantC".to_string()));
    }

    #[test]
    fn test_parse_empty() {
        let metadata: Vec<(&str, &str)> = vec![];

        let parsed = GrpcMetadata::parse(metadata.into_iter());

        assert_eq!(parsed.auth_token, None);
        assert_eq!(parsed.tenant, None);
    }

    #[test]
    fn test_builder_pattern() {
        let metadata = GrpcMetadata::new().with_auth_token("token").with_tenant("tenant1");

        assert_eq!(metadata.auth_token, Some("token".to_string()));
        assert_eq!(metadata.tenant, Some("tenant1".to_string()));
    }
}
