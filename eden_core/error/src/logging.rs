//! Integration between EpError and structured logging (eden_logger_internal)
//!
//! This module provides methods to convert errors into structured log contexts
//! that can be used with the eden_logger_internal system.
use crate::ep::EpError;
use eden_logger_internal::{EdenLog, LogAudience, LogContext, LogLevel};

impl EpError {
    /// Convert this error into a LogContext with error code and category metadata.
    pub fn to_log_context(&self) -> LogContext {
        let (category, _message) = self.category_and_message();

        LogContext::new().with_error_code(self.error_hex()).with_error_category(category.to_string())
    }

    /// Merge this error's metadata (code, category) with an existing LogContext.
    pub fn merge_with_context(&self, ctx: LogContext) -> LogContext {
        let error_ctx = self.to_log_context();
        ctx.merge(error_ctx)
    }

    /// Create a client-facing error log that will be included in API responses.
    pub fn to_client_log(&self) -> EdenLog {
        EdenLog::new(LogLevel::Error, self.to_string(), &self.to_log_context(), LogAudience::Client)
    }

    /// Create an internal error log for operators only (not sent to clients).
    pub fn to_internal_log(&self) -> EdenLog {
        EdenLog::new(LogLevel::Error, self.to_string(), &self.to_log_context(), LogAudience::Internal)
    }

    /// Returns true for client-facing errors (auth, validation, not found), false for internal errors (database, cache, connection).
    pub fn should_expose_to_client(&self) -> bool {
        matches!(
            self,
            // Client-facing errors (4xx)
            EpError::Request(_)
                | EpError::Parse(_)
                | EpError::Serde(_)
                | EpError::Data(_)
                | EpError::Template(_)
                | EpError::Auth(_)
                | EpError::Rbac(_)
                // Specific database errors that are client-safe
                | EpError::Database(crate::types::DatabaseError::UserNotFound)
                | EpError::Database(crate::types::DatabaseError::OrganizationNotFound)
                | EpError::Database(crate::types::DatabaseError::EndpointNotFound)
                | EpError::Database(crate::types::DatabaseError::EndpointGroupNotFound)
                | EpError::Database(crate::types::DatabaseError::TemplateNotFound)
                | EpError::Database(crate::types::DatabaseError::WorkflowNotFound)
                | EpError::Database(crate::types::DatabaseError::DuplicateUser)
                | EpError::Database(crate::types::DatabaseError::DuplicateOrganization)
                | EpError::Database(crate::types::DatabaseError::DuplicateEndpoint)
                | EpError::Database(crate::types::DatabaseError::DuplicateEndpointGroup)
                | EpError::Database(crate::types::DatabaseError::DuplicateTemplate)
                | EpError::Database(crate::types::DatabaseError::DuplicateWorkflow)
                | EpError::Database(crate::types::DatabaseError::RobotNotFound)
                | EpError::Database(crate::types::DatabaseError::DuplicateRobot)
                | EpError::Database(crate::types::DatabaseError::PipelineNotFound)
                | EpError::Database(crate::types::DatabaseError::DuplicatePipeline)
                | EpError::Database(crate::types::DatabaseError::SnapshotNotFound)
                | EpError::Database(crate::types::DatabaseError::DuplicateSnapshot)
                | EpError::Cache(crate::types::CacheError::KeyNotFound)
                | EpError::Fs(crate::types::FsError::FileNotFound)
                | EpError::Timeout(_)
                | EpError::Api(crate::types::ApiError::RateLimitExceeded)
                | EpError::Api(crate::types::ApiError::InvalidRequest)
                | EpError::Api(crate::types::ApiError::InvalidInput)
        )
    }

    /// Get the appropriate LogAudience for this error
    pub fn log_audience(&self) -> LogAudience {
        if self.should_expose_to_client() {
            LogAudience::Client
        } else {
            LogAudience::Internal
        }
    }

    /// Helper to extract category and message from error
    fn category_and_message(&self) -> (&str, String) {
        match self {
            EpError::Api(err) => ("API", err.to_string()),
            EpError::Init(err) => ("Initialization", err.to_string()),
            EpError::Transaction(err) => ("Transaction", err.to_string()),
            EpError::Request(err) => ("Request", err.to_string()),
            EpError::Connect(err) => ("Connection", err.to_string()),
            EpError::Serde(err) => ("Serialization", err.to_string()),
            EpError::Cache(err) => ("Cache", err.to_string()),
            EpError::Auth(err) => ("Authentication", err.to_string()),
            EpError::Rbac(err) => ("Access Control", err.to_string()),
            EpError::Database(err) => ("Database", err.to_string()),
            EpError::Metadata(err) => ("Metadata", err.to_string()),
            EpError::Interlay(err) => ("Interlay", err.to_string()),
            EpError::Io(err) => ("IO", err.to_string()),
            EpError::Parse(err) => ("Parsing", err.to_string()),
            EpError::Lock(err) => ("Lock", err.to_string()),
            EpError::Protocol(err) => ("Protocol", err.to_string()),
            EpError::Fs(err) => ("File System", err.to_string()),
            EpError::Data(err) => ("Data", err.to_string()),
            EpError::Timeout(err) => ("Timeout", err.to_string()),
            EpError::Tools(err) => ("Tools", err.to_string()),
            EpError::Template(err) => ("Template", err.to_string()),
            EpError::Workflow(err) => ("Workflow", err.to_string()),
            EpError::Redis(err) => ("Redis", err.to_string()),
            EpError::Provider(err) => ("Provider", err.to_string()),
            EpError::Ignored => ("Ignored", String::from("Ignored")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eden_logger_internal::{LogAudience, ctx_with_trace};
    use function_name::named;

    #[test]
    fn test_error_to_log_context() {
        let error = EpError::database_user_not_found();
        let ctx = error.to_log_context();

        assert_eq!(ctx.error_code.as_deref(), Some("E0A06"));
        assert_eq!(ctx.error_category.as_deref(), Some("Database"));
    }

    #[test]
    #[named]
    fn test_error_merge_with_context() {
        let error = EpError::invalid_credentials();
        let base_ctx = ctx_with_trace!().with_feature("auth");

        let merged = error.merge_with_context(base_ctx);

        assert_eq!(merged.feature.as_deref(), Some("auth"));
        assert_eq!(merged.function.as_deref(), Some("test_error_merge_with_context"));
        assert_eq!(merged.error_code.as_deref(), Some("E0801"));
        assert_eq!(merged.error_category.as_deref(), Some("Authentication"));
    }

    #[test]
    fn test_client_log_creation() {
        let error = EpError::database_user_not_found();
        let log = error.to_client_log();

        assert_eq!(log.level, LogLevel::Error);
        assert_eq!(log.audience, LogAudience::Client);
        assert!(log.should_send_to_client());
        assert!(log.message.contains("User not found"));
    }

    #[test]
    fn test_internal_log_creation() {
        let error = EpError::database_connection_timeout();
        let log = error.to_internal_log();

        assert_eq!(log.level, LogLevel::Error);
        assert_eq!(log.audience, LogAudience::Internal);
        assert!(!log.should_send_to_client());
    }

    #[test]
    fn test_should_expose_to_client() {
        // Client-facing errors
        assert!(EpError::invalid_credentials().should_expose_to_client());
        assert!(EpError::database_user_not_found().should_expose_to_client());
        assert!(EpError::api_rate_limit_exceeded().should_expose_to_client());
        assert!(EpError::request_invalid_format().should_expose_to_client());

        // Internal errors
        assert!(!EpError::database_connection_timeout().should_expose_to_client());
        assert!(!EpError::cache_connection_lost().should_expose_to_client());
        assert!(!EpError::connection_timeout().should_expose_to_client());
    }

    #[test]
    fn test_log_audience() {
        // Client errors
        assert_eq!(EpError::invalid_credentials().log_audience(), LogAudience::Client);

        // Internal errors
        assert_eq!(EpError::database_connection_timeout().log_audience(), LogAudience::Internal);
    }
}
