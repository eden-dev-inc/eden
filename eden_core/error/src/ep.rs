use crate::common::{EntityType, RbacErrorType};
use crate::types::*;
use ::serde::{Deserialize, Serialize};
use borsh::{BorshDeserialize, BorshSerialize};
use eden_logger_internal::{ctx_with_trace, log_trace};
use function_name::named;
use std::any::Any;
use std::{
    error::Error,
    fmt::{self, Debug},
};

/// Result type alias for endpoint operations.
///
/// This is the primary Result type used throughout Eve for endpoint-related
/// operations. It wraps successful values in `Ok(T)` and errors in `Err(EpError)`.
///
/// # Examples
///
/// ```
/// use error::{ResultEP, EpError};
///
/// fn validate_endpoint(id: &str) -> ResultEP<String> {
///     if id.is_empty() {
///         return Err(EpError::request_invalid_format());
///     }
///     Ok(id.to_string())
/// }
/// ```
pub type ResultEP<T> = Result<T, EpError>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum EpError {
    Api(ApiError),                 // 0x01
    Init(InitError),               // 0x02
    Transaction(TransactionError), // 0x03
    Request(RequestError),         // 0x04
    Connect(ConnectError),         // 0x05
    Serde(SerdeError),             // 0x06
    Cache(CacheError),             // 0x07
    Auth(AuthError),               // 0x08
    Rbac(RbacError),               // 0x09
    Database(DatabaseError),       // 0x0A
    Metadata(MetadataError),       // 0x0B
    Parse(ParseError),             // 0x0C
    Lock(LockError),               // 0x0D
    Fs(FsError),                   // 0x0F
    Data(DataError),               // 0x10
    Timeout(TimeoutError),         // 0x11
    Tools(ToolsError),             // 0x12
    Template(TemplateError),       // 0x13
    Workflow(WorkflowError),       // 0x14
    Interlay(InterlayError),       // 0x15
    Io(IoError),                   // 0x16
    Protocol(ProtocolError),       // 0x17
    Redis(RedisError),             // 0x18
    Provider(LlmProviderError),    // 0x19
    Ignored,                       // 0xFF
}

impl EpError {
    pub fn error_code(&self) -> u16 {
        let main_code: u8 = match self {
            EpError::Api(_) => 0x01,
            EpError::Init(_) => 0x02,
            EpError::Transaction(_) => 0x03,
            EpError::Request(_) => 0x04,
            EpError::Connect(_) => 0x05,
            EpError::Serde(_) => 0x06,
            EpError::Cache(_) => 0x07,
            EpError::Auth(_) => 0x08,
            EpError::Rbac(_) => 0x09,
            EpError::Database(_) => 0x0A,
            EpError::Metadata(_) => 0x0B,
            EpError::Parse(_) => 0x0C,
            EpError::Lock(_) => 0x0D,
            EpError::Fs(_) => 0x0F,
            EpError::Data(_) => 0x10,
            EpError::Timeout(_) => 0x11,
            EpError::Tools(_) => 0x12,
            EpError::Template(_) => 0x13,
            EpError::Workflow(_) => 0x14,
            EpError::Interlay(_) => 0x15,
            EpError::Io(_) => 0x16,
            EpError::Protocol(_) => 0x17,
            EpError::Redis(_) => 0x18,
            EpError::Provider(_) => 0x19,
            EpError::Ignored => 0xFF,
        };

        let sub_code: u8 = match self {
            EpError::Api(err) => err.error_code(),
            EpError::Init(err) => err.error_code(),
            EpError::Transaction(err) => err.error_code(),
            EpError::Request(err) => err.error_code(),
            EpError::Connect(err) => err.error_code(),
            EpError::Serde(err) => err.error_code(),
            EpError::Cache(err) => err.error_code(),
            EpError::Auth(err) => err.error_code(),
            EpError::Rbac(err) => err.error_code(),
            EpError::Database(err) => err.error_code(),
            EpError::Metadata(err) => err.error_code(),
            EpError::Parse(err) => err.error_code(),
            EpError::Lock(err) => err.error_code(),
            EpError::Fs(err) => err.error_code(),
            EpError::Data(err) => err.error_code(),
            EpError::Timeout(err) => err.error_code(),
            EpError::Tools(err) => err.error_code(),
            EpError::Template(err) => err.error_code(),
            EpError::Workflow(err) => err.error_code(),
            EpError::Interlay(err) => err.error_code(),
            EpError::Io(err) => err.error_code(),
            EpError::Protocol(err) => err.error_code(),
            EpError::Redis(err) => err.error_code(),
            EpError::Provider(err) => err.error_code(),
            EpError::Ignored => 0x00,
        };

        (main_code as u16) << 8 | sub_code as u16
    }

    /// Get the error code as a hex string (e.g., "E0A06")
    pub fn error_hex(&self) -> String {
        format!("E{:04X}", self.error_code())
    }

    #[named]
    pub fn database_query_error<E>(error: E, entity_type: EntityType) -> Self
    where
        E: std::error::Error + 'static,
    {
        // Prevent double-wrapping: if error is already an EpError, return it as-is
        if let Some(ep_error) = (&error as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        // Build the full error message by traversing the error chain.
        // This is necessary because tokio-postgres Kind::Db returns "db error"
        // but the actual PostgreSQL message (e.g., "duplicate key value violates
        // unique constraint") is in the source chain.
        let error_msg = {
            let mut msg = error.to_string();
            let mut current: Option<&(dyn std::error::Error + 'static)> = error.source();
            while let Some(source) = current {
                msg.push_str(": ");
                msg.push_str(&source.to_string());
                current = source.source();
            }
            msg
        };

        let db_error = match () {
            _ if error_msg.contains("query returned an unexpected number of rows") => match entity_type {
                EntityType::Api => DatabaseError::ApiNotFound,
                EntityType::EdenNode => DatabaseError::EdenNodeNotFound,
                EntityType::Endpoint => DatabaseError::EndpointNotFound,
                EntityType::EndpointGroup => DatabaseError::EndpointGroupNotFound,
                EntityType::Interlay => DatabaseError::InterlayNotFound,
                EntityType::Migration => DatabaseError::MigrationNotFound,
                EntityType::Organization => DatabaseError::OrganizationNotFound,
                EntityType::Pipeline => DatabaseError::PipelineNotFound,
                EntityType::Robot => DatabaseError::RobotNotFound,
                EntityType::Snapshot => DatabaseError::SnapshotNotFound,
                EntityType::Template => DatabaseError::TemplateNotFound,
                EntityType::User => DatabaseError::UserNotFound,
                EntityType::Workflow => DatabaseError::WorkflowNotFound,
            },
            _ if error_msg.contains("duplicate key value violates unique constraint") => match entity_type {
                EntityType::Api => DatabaseError::DuplicateApi,
                EntityType::EdenNode => DatabaseError::DuplicateEdenNode,
                EntityType::Endpoint => DatabaseError::DuplicateEndpoint,
                EntityType::EndpointGroup => DatabaseError::DuplicateEndpointGroup,
                EntityType::Interlay => DatabaseError::DuplicateInterlay,
                EntityType::Migration => DatabaseError::DuplicateMigration,
                EntityType::Organization => DatabaseError::DuplicateOrganization,
                EntityType::Pipeline => DatabaseError::DuplicatePipeline,
                EntityType::Robot => DatabaseError::DuplicateRobot,
                EntityType::Snapshot => DatabaseError::DuplicateSnapshot,
                EntityType::Template => DatabaseError::DuplicateTemplate,
                EntityType::User => DatabaseError::DuplicateUser,
                EntityType::Workflow => DatabaseError::DuplicateWorkflow,
            },
            // Foreign key violations - detect which referenced entity is missing
            // PostgreSQL format: 'is not present in table "organizations"'
            _ if error_msg.contains("violates foreign key constraint") => {
                if error_msg.contains("\"apis\"") {
                    DatabaseError::ApiNotFound
                } else if error_msg.contains("\"eden_nodes\"") {
                    DatabaseError::EdenNodeNotFound
                } else if error_msg.contains("\"endpoints\"") {
                    DatabaseError::EndpointNotFound
                } else if error_msg.contains("\"endpoint_groups\"") {
                    DatabaseError::EndpointGroupNotFound
                } else if error_msg.contains("\"interlays\"") {
                    DatabaseError::InterlayNotFound
                } else if error_msg.contains("\"migrations\"") {
                    DatabaseError::MigrationNotFound
                } else if error_msg.contains("\"organizations\"") {
                    DatabaseError::OrganizationNotFound
                } else if error_msg.contains("\"robots\"") {
                    DatabaseError::RobotNotFound
                } else if error_msg.contains("\"templates\"") {
                    DatabaseError::TemplateNotFound
                } else if error_msg.contains("\"users\"") {
                    DatabaseError::UserNotFound
                } else if error_msg.contains("\"pipelines\"") {
                    DatabaseError::PipelineNotFound
                } else if error_msg.contains("\"snapshots\"") {
                    DatabaseError::SnapshotNotFound
                } else if error_msg.contains("\"workflows\"") {
                    DatabaseError::WorkflowNotFound
                } else {
                    DatabaseError::ConstraintViolation
                }
            }
            _ if error_msg.contains("connection") || error_msg.contains("timeout") => DatabaseError::ConnectionTimeout,
            _ if error_msg.contains("permission denied") || error_msg.contains("authentication failed") => {
                DatabaseError::AuthenticationFailed
            }
            _ if error_msg.contains("relation") && error_msg.contains("does not exist") => DatabaseError::SchemaError,
            _ => DatabaseError::QueryFailed,
        };

        let error = EpError::Database(db_error);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    /// Parses Redis errors from the redis crate into structured RedisError types.
    ///
    /// This function analyzes Redis error messages and maps them to specific RedisError
    /// variants based on error patterns. It enables proper error categorization, retry logic,
    /// and client-friendly error responses.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use error::EpError;
    ///
    /// // Parse a Redis authentication error
    /// let error = redis::RedisError::from((redis::ErrorKind::AuthenticationFailed, "NOAUTH"));
    /// let ep_error = EpError::parse_redis_error(error);
    /// // Returns EpError::Redis(RedisError::AuthRequired)
    /// ```
    #[named]
    pub fn parse_redis_error<E>(error: E) -> Self
    where
        E: std::error::Error + 'static,
    {
        if let Some(ep_error) = (&error as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error_msg = error.to_string();
        let error_lower = error_msg.to_lowercase();

        let redis_error = match () {
            // Authentication errors
            _ if error_lower.contains("noauth") => RedisError::AuthRequired,
            _ if error_lower.contains("wrongpass") || error_lower.contains("invalid password") => RedisError::InvalidPassword,
            _ if error_lower.contains("noperm") || error_lower.contains("permission denied") => RedisError::PermissionDenied,

            // Type errors
            _ if error_lower.contains("wrongtype") => RedisError::WrongType,
            _ if error_lower.contains("type") && error_lower.contains("conversion") => RedisError::TypeConversionFailed,

            // Cluster errors
            _ if error_lower.contains("moved") => RedisError::ClusterMoved,
            _ if error_lower.contains("ask") => RedisError::ClusterAsk,
            _ if error_lower.contains("clusterdown") => RedisError::ClusterDown,
            _ if error_lower.contains("crossslot") => RedisError::ClusterCrossSlot,

            // Transaction errors
            _ if error_lower.contains("execabort") => RedisError::TransactionAborted,
            _ if error_lower.contains("watch") => RedisError::WatchFailed,

            // Scripting errors
            _ if error_lower.contains("noscript") => RedisError::ScriptNotFound,
            _ if error_lower.contains("script") && error_lower.contains("error") => RedisError::ScriptError,

            // Server state errors
            _ if error_lower.contains("loading") || error_lower.contains("busy") => RedisError::ServerBusy,
            _ if error_lower.contains("readonly") => RedisError::ServerReadOnly,
            _ if error_lower.contains("oom") || error_lower.contains("out of memory") => RedisError::ServerOutOfMemory,
            _ if error_lower.contains("masterdown") || error_lower.contains("master_down") => RedisError::MasterDown,

            // Connection errors
            _ if error_lower.contains("connection refused") => RedisError::ConnectionRefused,
            _ if error_lower.contains("connection") && error_lower.contains("timeout") => RedisError::ConnectionTimeout,
            _ if error_lower.contains("broken pipe")
                || error_lower.contains("connection reset")
                || error_lower.contains("connection lost") =>
            {
                RedisError::ConnectionLost
            }
            _ if error_lower.contains("pool") && error_lower.contains("exhausted") => RedisError::PoolExhausted,

            // Command errors
            _ if error_lower.contains("unknown command") || error_lower.contains("command not found") => RedisError::CommandNotFound,
            _ if error_lower.contains("syntax") || error_lower.contains("wrong number of arguments") => RedisError::InvalidSyntax,
            _ if error_lower.contains("out of range") || error_lower.contains("index out of") => RedisError::OutOfRange,
            _ if error_lower.contains("invalid") && error_lower.contains("argument") => RedisError::InvalidArgument,

            // Protocol errors
            _ if error_lower.contains("protocol") || error_lower.contains("malformed") => RedisError::ProtocolError,
            _ if error_lower.contains("invalid response") || error_lower.contains("unexpected response") => RedisError::InvalidResponse,

            // Retry-able errors
            _ if error_lower.contains("tryagain") || error_lower.contains("try again") => RedisError::TryAgain,
            _ if error_lower.contains("timeout") && !error_lower.contains("connection") => RedisError::Timeout,

            // I/O errors
            _ if error_lower.contains("i/o error") || error_lower.contains("io error") => RedisError::IoError(error_msg.clone()),

            // Default to custom
            _ => RedisError::Custom(error_msg.clone()),
        };

        let error = EpError::Redis(redis_error);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn rbac_operation_error<T>(rbac_error_type: RbacErrorType, operation: T) -> Self
    where
        T: ToString,
    {
        let _op = operation.to_string(); // Keep for potential future use

        let rbac_error = match rbac_error_type {
            RbacErrorType::ConnectionFailure => RbacError::ConnectionFailure,
            RbacErrorType::InvalidPermissions => RbacError::InvalidPermissions,
            RbacErrorType::RuleNotFound => RbacError::RuleNotFound,
        };

        let error = EpError::Rbac(rbac_error);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }
}

impl fmt::Display for EpError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (category, message) = match self {
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
            EpError::Ignored => return write!(f, "Ignored"),
        };

        write!(f, "[{}] {} error: {}", self.error_hex(), category, message)
    }
}

#[cfg(feature = "actix")]
impl From<EpError> for actix_web::error::Error {
    fn from(error: EpError) -> Self {
        use actix_web::error::*;

        match &error {
            // 400 Bad Request - Client errors
            EpError::Request(_) => ErrorBadRequest(error),
            EpError::Parse(_) => ErrorBadRequest(error),
            EpError::Serde(_) => ErrorBadRequest(error),
            EpError::Data(_) => ErrorBadRequest(error),
            EpError::Template(_) => ErrorBadRequest(error),
            EpError::Api(ApiError::InvalidRequest) => ErrorBadRequest(error),
            EpError::Api(ApiError::InvalidInput) => ErrorBadRequest(error),
            EpError::Api(ApiError::Custom(_)) => ErrorBadRequest(error),

            // 401 Unauthorized - Authentication required
            EpError::Auth(AuthError::TokenMalformed) => ErrorUnauthorized(error),
            EpError::Auth(AuthError::TokenExpired) => ErrorUnauthorized(error),
            EpError::Auth(AuthError::InvalidCredentials) => ErrorUnauthorized(error),
            EpError::Auth(AuthError::InvalidApiKey) => ErrorUnauthorized(error),
            EpError::Auth(AuthError::SessionExpired) => ErrorUnauthorized(error),
            EpError::Auth(_) => ErrorUnauthorized(error),

            // 403 Forbidden - Permission denied
            EpError::Rbac(_) => ErrorForbidden(error),

            // 404 Not Found - Resource not found
            EpError::Database(DatabaseError::UserNotFound) => ErrorNotFound(error),
            EpError::Database(DatabaseError::OrganizationNotFound) => ErrorNotFound(error),
            EpError::Database(DatabaseError::EndpointNotFound) => ErrorNotFound(error),
            EpError::Database(DatabaseError::EndpointGroupNotFound) => ErrorNotFound(error),
            EpError::Database(DatabaseError::TemplateNotFound) => ErrorNotFound(error),
            EpError::Database(DatabaseError::WorkflowNotFound) => ErrorNotFound(error),
            EpError::Database(DatabaseError::ConversationNotFound) => ErrorNotFound(error),
            EpError::Database(DatabaseError::MigrationNotFound) => ErrorNotFound(error),
            EpError::Database(DatabaseError::InterlayNotFound) => ErrorNotFound(error),
            EpError::Database(DatabaseError::EdenNodeNotFound) => ErrorNotFound(error),
            EpError::Database(DatabaseError::ApiNotFound) => ErrorNotFound(error),
            EpError::Database(DatabaseError::RobotNotFound) => ErrorNotFound(error),
            EpError::Database(DatabaseError::PipelineNotFound) => ErrorNotFound(error),
            EpError::Database(DatabaseError::SnapshotNotFound) => ErrorNotFound(error),
            EpError::Fs(FsError::FileNotFound) => ErrorNotFound(error),
            EpError::Cache(CacheError::KeyNotFound) => ErrorNotFound(error),

            // 408 Request Timeout
            EpError::Timeout(_) => ErrorRequestTimeout(error),
            EpError::Metadata(MetadataError::QueryTimeout(_)) => ErrorRequestTimeout(error),

            // 409 Conflict - Duplicate resources
            EpError::Database(DatabaseError::DuplicateUser) => ErrorConflict(error),
            EpError::Database(DatabaseError::DuplicateOrganization) => ErrorConflict(error),
            EpError::Database(DatabaseError::DuplicateEndpoint) => ErrorConflict(error),
            EpError::Database(DatabaseError::DuplicateEndpointGroup) => ErrorConflict(error),
            EpError::Database(DatabaseError::DuplicateTemplate) => ErrorConflict(error),
            EpError::Database(DatabaseError::DuplicateWorkflow) => ErrorConflict(error),
            EpError::Database(DatabaseError::DuplicateMigration) => ErrorConflict(error),
            EpError::Database(DatabaseError::DuplicateInterlay) => ErrorConflict(error),
            EpError::Database(DatabaseError::DuplicateEdenNode) => ErrorConflict(error),
            EpError::Database(DatabaseError::DuplicateApi) => ErrorConflict(error),
            EpError::Database(DatabaseError::DuplicateSnapshot) => ErrorConflict(error),
            EpError::Database(DatabaseError::ConstraintViolation) => ErrorConflict(error),

            // 422 Unprocessable Entity - Validation errors
            EpError::Workflow(WorkflowError::InvalidDefinition) => ErrorUnprocessableEntity(error),
            EpError::Workflow(WorkflowError::CycleDetected) => ErrorUnprocessableEntity(error),

            // 429 Too Many Requests
            EpError::Api(ApiError::RateLimitExceeded) => ErrorTooManyRequests(error),

            // 500 Internal Server Error - Server/system errors
            EpError::Api(ApiError::InternalError) => ErrorInternalServerError(error),
            EpError::Init(_) => ErrorInternalServerError(error),
            EpError::Connect(_) => ErrorInternalServerError(error),
            EpError::Database(_) => ErrorInternalServerError(error),
            EpError::Transaction(_) => ErrorInternalServerError(error),
            EpError::Cache(_) => ErrorInternalServerError(error),
            EpError::Lock(_) => ErrorInternalServerError(error),
            EpError::Tools(_) => ErrorInternalServerError(error),
            EpError::Metadata(_) => ErrorInternalServerError(error),
            EpError::Fs(_) => ErrorInternalServerError(error),
            EpError::Workflow(_) => ErrorInternalServerError(error),
            EpError::Redis(_) => ErrorInternalServerError(error),

            // 503 Service Unavailable
            EpError::Api(ApiError::ServiceUnavailable) => ErrorServiceUnavailable(error),

            // Default to 500 for any unhandled cases
            _ => ErrorInternalServerError(error),
        }
    }
}

// Implementation for &str
impl From<&str> for EpError {
    fn from(_err: &str) -> Self {
        EpError::Request(RequestError::InvalidFormat)
    }
}

// Implementation for String
impl From<String> for EpError {
    fn from(_err: String) -> Self {
        EpError::Request(RequestError::InvalidFormat)
    }
}

impl Error for EpError {}

// From implementations for common external error types
impl From<serde_json::Error> for EpError {
    fn from(_err: serde_json::Error) -> Self {
        EpError::Serde(SerdeError::DeserializationFailed)
    }
}

impl From<std::io::Error> for EpError {
    fn from(_err: std::io::Error) -> Self {
        EpError::Fs(FsError::IoError)
    }
}

impl EpError {
    // === API Error Constructors ===
    #[named]
    pub fn api_invalid_request() -> Self {
        let error = EpError::Api(ApiError::InvalidRequest);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn api_rate_limit_exceeded() -> Self {
        let error = EpError::Api(ApiError::RateLimitExceeded);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn api_service_unavailable() -> Self {
        let error = EpError::Api(ApiError::ServiceUnavailable);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn api_invalid_input() -> Self {
        let error = EpError::Api(ApiError::InvalidInput);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn api_internal_error() -> Self {
        let error = EpError::Api(ApiError::InternalError);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    // === Authentication Error Constructors ===
    #[named]
    pub fn invalid_credentials() -> Self {
        let error = EpError::Auth(AuthError::InvalidCredentials);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn invalid_api_key() -> Self {
        let error = EpError::Auth(AuthError::InvalidApiKey);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn token_expired() -> Self {
        let error = EpError::Auth(AuthError::TokenExpired);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn token_malformed() -> Self {
        let error = EpError::Auth(AuthError::TokenMalformed);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn insufficient_permissions() -> Self {
        let error = EpError::Auth(AuthError::InsufficientPermissions);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn session_expired() -> Self {
        let error = EpError::Auth(AuthError::SessionExpired);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    // === Database Error Constructors ===
    #[named]
    pub fn database_connection_timeout() -> Self {
        let error = EpError::Database(DatabaseError::ConnectionTimeout);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database_authentication_failed() -> Self {
        let error = EpError::Database(DatabaseError::AuthenticationFailed);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database_schema_error() -> Self {
        let error = EpError::Database(DatabaseError::SchemaError);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database_user_not_found() -> Self {
        let error = EpError::Database(DatabaseError::UserNotFound);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database_organization_not_found() -> Self {
        let error = EpError::Database(DatabaseError::OrganizationNotFound);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database_endpoint_not_found() -> Self {
        let error = EpError::Database(DatabaseError::EndpointNotFound);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database_template_not_found() -> Self {
        let error = EpError::Database(DatabaseError::TemplateNotFound);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database_workflow_not_found() -> Self {
        let error = EpError::Database(DatabaseError::WorkflowNotFound);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database_conversation_not_found() -> Self {
        let error = EpError::Database(DatabaseError::ConversationNotFound);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database_duplicate_user() -> Self {
        let error = EpError::Database(DatabaseError::DuplicateUser);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database_duplicate_organization() -> Self {
        let error = EpError::Database(DatabaseError::DuplicateOrganization);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database_duplicate_endpoint() -> Self {
        let error = EpError::Database(DatabaseError::DuplicateEndpoint);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database_duplicate_template() -> Self {
        let error = EpError::Database(DatabaseError::DuplicateTemplate);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database_duplicate_workflow() -> Self {
        let error = EpError::Database(DatabaseError::DuplicateWorkflow);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    // === RBAC Error Constructors ===
    #[named]
    pub fn rbac_rule_not_found() -> Self {
        let error = EpError::Rbac(RbacError::RuleNotFound);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn rbac_invalid_permissions() -> Self {
        let error = EpError::Rbac(RbacError::InvalidPermissions);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn rbac_connection_failure() -> Self {
        let error = EpError::Rbac(RbacError::ConnectionFailure);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn rbac_unauthorized() -> Self {
        let error = EpError::Rbac(RbacError::Unauthorized);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    // === Connection Error Constructors ===
    #[named]
    pub fn connection_refused() -> Self {
        let error = EpError::Connect(ConnectError::ConnectionRefused);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn connection_timeout() -> Self {
        let error = EpError::Connect(ConnectError::TimeoutReached);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn network_unreachable() -> Self {
        let error = EpError::Connect(ConnectError::NetworkUnreachable);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    // === Cache Error Constructors ===
    #[named]
    pub fn cache_key_not_found() -> Self {
        let error = EpError::Cache(CacheError::KeyNotFound);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn cache_connection_lost() -> Self {
        let error = EpError::Cache(CacheError::ConnectionLost);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn cache_memory_exhausted() -> Self {
        let error = EpError::Cache(CacheError::MemoryExhausted);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    // === Request Error Constructors ===
    #[named]
    pub fn request_invalid_format() -> Self {
        let error = EpError::Request(RequestError::InvalidFormat);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn request_missing_parameters() -> Self {
        let error = EpError::Request(RequestError::MissingParameters);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn request_invalid_parameters() -> Self {
        let error = EpError::Request(RequestError::InvalidParameters);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn request_payload_too_large() -> Self {
        let error = EpError::Request(RequestError::PayloadTooLarge);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn request_read_only() -> Self {
        let error = EpError::Request(RequestError::ReadOnly);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    // === Generic constructors for backward compatibility and flexibility ===
    #[named]
    pub fn api<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Api(ApiError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn auth<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Auth(AuthError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn database<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Database(DatabaseError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn rbac<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Rbac(RbacError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn connect<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Connect(ConnectError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn cache<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Cache(CacheError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn request<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Request(RequestError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        // log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn serde<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Serde(SerdeError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    // pub fn parse(parse_error: ParseError) -> Self {
    //     let error = EpError::Parse(parse_error);
    //     log::error!("{}", error);
    //     error
    // }

    #[named]
    pub fn parse<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Parse(ParseError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn init<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Init(InitError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn transaction<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Transaction(TransactionError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn metadata<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Metadata(MetadataError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn lock<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Lock(LockError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn fs<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Fs(FsError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn data<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Data(DataError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn timeout<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Timeout(TimeoutError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn tools<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Tools(ToolsError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn template<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Template(TemplateError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn workflow<E: std::fmt::Display + 'static>(message: E) -> Self {
        // Prevent double-wrapping: if message is already an EpError, return it as-is
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Workflow(WorkflowError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!();
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn redis<E: std::fmt::Display + 'static>(message: E) -> Self {
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::Redis(RedisError::Custom(message.to_string()));
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn redis_auth_required() -> Self {
        let error = EpError::Redis(RedisError::AuthRequired);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn redis_wrong_type() -> Self {
        let error = EpError::Redis(RedisError::WrongType);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn redis_connection_timeout() -> Self {
        let error = EpError::Redis(RedisError::ConnectionTimeout);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn redis_connection_refused() -> Self {
        let error = EpError::Redis(RedisError::ConnectionRefused);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn redis_cluster_moved() -> Self {
        let error = EpError::Redis(RedisError::ClusterMoved);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    #[named]
    pub fn provider(err: LlmProviderError) -> Self {
        let error = EpError::Provider(err);
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(_ctx, error.to_string(), audience = error.log_audience());
        error
    }

    // === Legacy/Compatibility Error Converters ===
    // These are used for .map_err(EpError::request_error) patterns
    #[named]
    pub fn request_error<E: std::fmt::Display>(_err: E) -> Self {
        let _ctx = ctx_with_trace!().with_feature("error");
        log_trace!(
            _ctx,
            "Request error",
            audience = eden_logger_internal::LogAudience::Client,
            error = _err.to_string()
        );
        EpError::Request(RequestError::InvalidFormat)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Custom error type for testing database_query_error which requires std::error::Error
    #[derive(Debug)]
    struct CustomError {
        message: String,
    }

    impl CustomError {
        fn new(message: &str) -> Self {
            Self { message: message.to_string() }
        }
    }

    impl fmt::Display for CustomError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{}", self.message)
        }
    }

    impl Error for CustomError {}

    #[test]
    fn test_from_string_errors() {
        // Test using From trait - should create RequestError::InvalidFormat
        let error: EpError = "test error".into();
        assert!(error.to_string().contains("Request format is invalid"));
        assert_eq!(error.error_code(), 0x0401); // Request(0x04) + InvalidFormat(0x01)

        let error: EpError = String::from("test string error").into();
        assert!(error.to_string().contains("Request format is invalid"));
        assert_eq!(error.error_code(), 0x0401); // Request(0x04) + InvalidFormat(0x01)
    }

    #[test]
    fn test_structured_errors() {
        // Test using new structured error constructors
        let error = EpError::request_invalid_format();
        assert!(error.to_string().contains("Request format is invalid"));
        assert_eq!(error.error_code(), 0x0401);

        let error = EpError::database_user_not_found();
        assert!(error.to_string().contains("User not found"));
        assert_eq!(error.error_code(), 0x0A06); // Database(0x0A) + UserNotFound(0x06)

        let error = EpError::invalid_credentials();
        assert!(error.to_string().contains("Invalid username or password"));
        assert_eq!(error.error_code(), 0x0801); // Auth(0x08) + InvalidCredentials(0x01)
    }

    #[test]
    fn test_error_codes() {
        let api_error = EpError::api_rate_limit_exceeded();
        assert_eq!(api_error.error_code(), 0x0102); // Api(0x01) + RateLimitExceeded(0x02)
        assert_eq!(api_error.error_hex(), "E0102");

        let db_error = EpError::database_duplicate_user();
        assert_eq!(db_error.error_code(), 0x0A0B); // Database(0x0A) + DuplicateUser(0x0B)
        assert_eq!(db_error.error_hex(), "E0A0B");

        let rbac_error = EpError::rbac_rule_not_found();
        assert_eq!(rbac_error.error_code(), 0x0901); // Rbac(0x09) + RuleNotFound(0x01)
        assert_eq!(rbac_error.error_hex(), "E0901");
    }

    #[test]
    fn test_different_variants() {
        let init_error = EpError::Init(InitError::ConfigurationMissing);
        let request_error = EpError::Request(RequestError::InvalidParameters);
        let transaction_error = EpError::Transaction(TransactionError::CommitFailed);

        assert!(init_error.to_string().contains("configuration"));
        assert!(request_error.to_string().contains("parameters are invalid"));
        assert!(transaction_error.to_string().contains("commit"));

        // Check error codes
        assert_eq!(init_error.error_code(), 0x0201); // Init(0x02) + ConfigurationMissing(0x01)
        assert_eq!(request_error.error_code(), 0x0403); // Request(0x04) + InvalidParameters(0x03)
        assert_eq!(transaction_error.error_code(), 0x0302); // Transaction(0x03) + CommitFailed(0x02)
    }

    #[test]
    fn test_database_query_error_function() {
        let user_not_found =
            EpError::database_query_error(CustomError::new("query returned an unexpected number of rows"), EntityType::User);
        assert!(user_not_found.to_string().contains("User not found"));
        assert_eq!(user_not_found.error_code(), 0x0A06);

        let duplicate_org =
            EpError::database_query_error(CustomError::new("duplicate key value violates unique constraint"), EntityType::Organization);
        assert!(duplicate_org.to_string().contains("Organization already exists"));
        assert_eq!(duplicate_org.error_code(), 0x0A0C);

        let connection_timeout = EpError::database_query_error(CustomError::new("connection timeout"), EntityType::User);
        assert!(connection_timeout.to_string().contains("connection timeout"));
        assert_eq!(connection_timeout.error_code(), 0x0A01);
    }

    #[test]
    fn test_ignored_error() {
        let error = EpError::Ignored;
        assert_eq!(error.to_string(), "Ignored");
    }

    #[test]
    fn test_custom_error_variants() {
        // Test Database::Custom
        let db_custom = EpError::database("Custom database error message");
        assert!(db_custom.to_string().contains("Custom database error message"));
        assert_eq!(db_custom.error_code(), 0x0AFF); // Database(0x0A) + Custom(0xFF)
        assert_eq!(db_custom.error_hex(), "E0AFF");

        // Test Auth::Custom
        let auth_custom = EpError::auth("Custom auth error");
        assert!(auth_custom.to_string().contains("Custom auth error"));
        assert_eq!(auth_custom.error_code(), 0x08FF); // Auth(0x08) + Custom(0xFF)
        assert_eq!(auth_custom.error_hex(), "E08FF");

        // Test Request::Custom
        let request_custom = EpError::request("Custom request error");
        assert!(request_custom.to_string().contains("Custom request error"));
        assert_eq!(request_custom.error_code(), 0x04FF); // Request(0x04) + Custom(0xFF)
        assert_eq!(request_custom.error_hex(), "E04FF");

        // Test Rbac::Custom
        let rbac_custom = EpError::rbac("Custom RBAC error");
        assert!(rbac_custom.to_string().contains("Custom RBAC error"));
        assert_eq!(rbac_custom.error_code(), 0x09FF); // Rbac(0x09) + Custom(0xFF)
        assert_eq!(rbac_custom.error_hex(), "E09FF");

        // Test Cache::Custom
        let cache_custom = EpError::cache("Custom cache error");
        assert!(cache_custom.to_string().contains("Custom cache error"));
        assert_eq!(cache_custom.error_code(), 0x07FF); // Cache(0x07) + Custom(0xFF)
        assert_eq!(cache_custom.error_hex(), "E07FF");
    }

    #[test]
    fn test_database_query_error_patterns() {
        // Test all entity types with "query returned unexpected rows"
        let entities = vec![
            (EntityType::User, 0x0A06, "User not found"),
            (EntityType::Organization, 0x0A07, "Organization not found"),
            (EntityType::Endpoint, 0x0A08, "Endpoint not found"),
            (EntityType::Template, 0x0A09, "Template not found"),
            (EntityType::Workflow, 0x0A0A, "Workflow not found"),
            (EntityType::Interlay, 0x0A15, "Interlay not found"),
        ];

        for (entity_type, expected_code, expected_msg) in entities {
            let error = EpError::database_query_error(CustomError::new("query returned an unexpected number of rows"), entity_type);
            assert_eq!(error.error_code(), expected_code);
            assert!(error.to_string().contains(expected_msg));
        }

        // Test duplicate key violations
        let duplicate_entities = vec![
            (EntityType::User, 0x0A0B, "User already exists"),
            (EntityType::Organization, 0x0A0C, "Organization already exists"),
            (EntityType::Endpoint, 0x0A0D, "Endpoint already exists"),
            (EntityType::Template, 0x0A0E, "Template already exists"),
            (EntityType::Workflow, 0x0A0F, "Workflow already exists"),
        ];

        for (entity_type, expected_code, expected_msg) in duplicate_entities {
            let error = EpError::database_query_error(CustomError::new("duplicate key value violates unique constraint"), entity_type);
            assert_eq!(error.error_code(), expected_code);
            assert!(error.to_string().contains(expected_msg));
        }

        // Test connection errors
        let connection_error = EpError::database_query_error(CustomError::new("connection failed"), EntityType::User);
        assert_eq!(connection_error.error_code(), 0x0A01); // ConnectionTimeout
        assert!(connection_error.to_string().contains("connection timeout"));

        // Test auth errors
        let auth_error = EpError::database_query_error(CustomError::new("authentication failed"), EntityType::User);
        assert_eq!(auth_error.error_code(), 0x0A02); // AuthenticationFailed
        assert!(auth_error.to_string().contains("authentication failed"));

        // Test schema errors
        let schema_error = EpError::database_query_error(CustomError::new("relation does not exist"), EntityType::User);
        assert_eq!(schema_error.error_code(), 0x0A03); // SchemaError
        assert!(schema_error.to_string().contains("schema error"));

        // Test generic query failed
        let generic_error = EpError::database_query_error(CustomError::new("some random error"), EntityType::User);
        assert_eq!(generic_error.error_code(), 0x0A04); // QueryFailed
    }

    #[test]
    fn test_foreign_key_violation_detection() {
        // Test FK violation on organizations table
        // Simulates: inserting endpoint with invalid organization UUID
        let org_fk_error = EpError::database_query_error(
            CustomError::new(
                r#"db error: ERROR: insert or update on table "organization_endpoints" violates foreign key constraint "organization_endpoints_organization_uuid_fkey"
DETAIL: Key (organization_uuid)=(cb7d697c-066d-4de7-a9d1-1e4382a1223f) is not present in table "organizations"."#,
            ),
            EntityType::Endpoint, // Note: entity_type is Endpoint, but error should detect Organization
        );
        assert_eq!(org_fk_error.error_code(), 0x0A07); // OrganizationNotFound
        assert!(org_fk_error.to_string().contains("Organization not found"));

        // Test FK violation on eden_nodes table
        let eden_node_fk_error = EpError::database_query_error(
            CustomError::new(
                r#"db error: ERROR: insert or update on table "eden_node_endpoints" violates foreign key constraint "eden_node_endpoints_eden_node_uuid_fkey"
DETAIL: Key (eden_node_uuid)=(12345678-1234-1234-1234-123456789012) is not present in table "eden_nodes"."#,
            ),
            EntityType::Endpoint,
        );
        assert_eq!(eden_node_fk_error.error_code(), 0x0A16); // EdenNodeNotFound
        assert!(eden_node_fk_error.to_string().contains("Eden node not found"));

        // Test FK violation on endpoints table
        let endpoint_fk_error = EpError::database_query_error(
            CustomError::new(
                r#"db error: ERROR: insert or update on table "some_table" violates foreign key constraint "some_fkey"
DETAIL: Key (endpoint_uuid)=(12345678-1234-1234-1234-123456789012) is not present in table "endpoints"."#,
            ),
            EntityType::Workflow,
        );
        assert_eq!(endpoint_fk_error.error_code(), 0x0A08); // EndpointNotFound
        assert!(endpoint_fk_error.to_string().contains("Endpoint not found"));

        // Test FK violation on users table
        let user_fk_error = EpError::database_query_error(
            CustomError::new(
                r#"db error: ERROR: insert or update on table "organization_users" violates foreign key constraint "organization_users_user_uuid_fkey"
DETAIL: Key (user_uuid)=(12345678-1234-1234-1234-123456789012) is not present in table "users"."#,
            ),
            EntityType::Organization,
        );
        assert_eq!(user_fk_error.error_code(), 0x0A06); // UserNotFound
        assert!(user_fk_error.to_string().contains("User not found"));

        // Test FK violation falls back to ConstraintViolation for unknown table
        let unknown_fk_error = EpError::database_query_error(
            CustomError::new(
                r#"db error: ERROR: insert or update on table "some_table" violates foreign key constraint "some_fkey"
DETAIL: Key (some_uuid)=(12345678-1234-1234-1234-123456789012) is not present in table "unknown_table"."#,
            ),
            EntityType::User,
        );
        assert_eq!(unknown_fk_error.error_code(), 0x0A10); // ConstraintViolation
    }

    #[test]
    fn test_rbac_operation_error() {
        let rbac_errors = vec![
            (RbacErrorType::RuleNotFound, 0x0901, "rule not found"),
            (RbacErrorType::InvalidPermissions, 0x0902, "Invalid permission bits"),
            (RbacErrorType::ConnectionFailure, 0x0903, "Connection timeout"),
        ];

        for (rbac_error_type, expected_code, expected_msg) in rbac_errors {
            let error = EpError::rbac_operation_error(rbac_error_type, "test_operation");
            assert_eq!(error.error_code(), expected_code);
            assert!(error.to_string().to_lowercase().contains(&expected_msg.to_lowercase()));
        }
    }

    #[cfg(feature = "actix")]
    #[test]
    fn test_actix_web_error_conversion() {
        // Test 400 Bad Request
        let bad_request_errors = vec![
            EpError::request_invalid_format(),
            EpError::Request(RequestError::InvalidParameters),
            EpError::Parse(ParseError::InvalidSyntax),
            EpError::Api(ApiError::InvalidRequest),
            EpError::Api(ApiError::InvalidInput),
            EpError::Api(ApiError::Custom("test error".to_string())),
            EpError::api("custom validation error"),
        ];
        for error in bad_request_errors {
            let actix_error: actix_web::Error = error.clone().into();
            let response = actix_error.as_response_error().error_response();
            assert_eq!(
                response.status(),
                actix_web::http::StatusCode::BAD_REQUEST,
                "Error {:?} should map to 400 BAD_REQUEST",
                error
            );
        }

        // Test 401 Unauthorized
        let auth_error: actix_web::Error = EpError::invalid_credentials().into();
        let response = auth_error.as_response_error().error_response();
        assert_eq!(response.status(), actix_web::http::StatusCode::UNAUTHORIZED);

        // Test 403 Forbidden
        let rbac_error: actix_web::Error = EpError::rbac_unauthorized().into();
        let response = rbac_error.as_response_error().error_response();
        assert_eq!(response.status(), actix_web::http::StatusCode::FORBIDDEN);

        // Test 404 Not Found
        let not_found_error: actix_web::Error = EpError::database_user_not_found().into();
        let response = not_found_error.as_response_error().error_response();
        assert_eq!(response.status(), actix_web::http::StatusCode::NOT_FOUND);

        // Test 409 Conflict
        let conflict_error: actix_web::Error = EpError::database_duplicate_user().into();
        let response = conflict_error.as_response_error().error_response();
        assert_eq!(response.status(), actix_web::http::StatusCode::CONFLICT);

        // Test 429 Too Many Requests
        let rate_limit_error: actix_web::Error = EpError::api_rate_limit_exceeded().into();
        let response = rate_limit_error.as_response_error().error_response();
        assert_eq!(response.status(), actix_web::http::StatusCode::TOO_MANY_REQUESTS);

        // Test 500 Internal Server Error
        let internal_errors = vec![EpError::Api(ApiError::InternalError), EpError::api_internal_error()];
        for error in internal_errors {
            let actix_error: actix_web::Error = error.clone().into();
            let response = actix_error.as_response_error().error_response();
            assert_eq!(
                response.status(),
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Error {:?} should map to 500 INTERNAL_SERVER_ERROR",
                error
            );
        }
    }

    #[test]
    fn test_runtime_double_wrapping_prevention() {
        // Test that all helper functions prevent double-wrapping at runtime
        let original_db_error = EpError::database_user_not_found();

        // Test database() helper - should detect EpError and return it as-is
        let wrapped_db = EpError::database(original_db_error.clone());
        assert_eq!(wrapped_db.error_code(), 0x0A06); // Should still be UserNotFound, not Custom
        assert_eq!(wrapped_db.error_hex(), "E0A06");
        assert!(wrapped_db.to_string().contains("[E0A06] Database error: User not found"));

        // Test api() helper
        let original_api_error = EpError::api_rate_limit_exceeded();
        let wrapped_api = EpError::api(original_api_error.clone());
        assert_eq!(wrapped_api.error_code(), 0x0102); // Should still be RateLimitExceeded
        assert_eq!(wrapped_api.error_hex(), "E0102");

        // Test auth() helper
        let original_auth_error = EpError::invalid_credentials();
        let wrapped_auth = EpError::auth(original_auth_error.clone());
        assert_eq!(wrapped_auth.error_code(), 0x0801); // Should still be InvalidCredentials
        assert_eq!(wrapped_auth.error_hex(), "E0801");

        // Test rbac() helper
        let original_rbac_error = EpError::rbac_unauthorized();
        let wrapped_rbac = EpError::rbac(original_rbac_error.clone());
        assert_eq!(wrapped_rbac.error_code(), 0x0905); // Should still be Unauthorized
        assert_eq!(wrapped_rbac.error_hex(), "E0905");

        // Test cache() helper
        let original_cache_error = EpError::cache_key_not_found();
        let wrapped_cache = EpError::cache(original_cache_error.clone());
        assert_eq!(wrapped_cache.error_code(), 0x0701); // Should still be KeyNotFound
        assert_eq!(wrapped_cache.error_hex(), "E0701");

        // Test request() helper
        let original_request_error = EpError::request_invalid_format();
        let wrapped_request = EpError::request(original_request_error.clone());
        assert_eq!(wrapped_request.error_code(), 0x0401); // Should still be InvalidFormat
        assert_eq!(wrapped_request.error_hex(), "E0401");

        // Test connect() helper
        let original_connect_error = EpError::Connect(ConnectError::TimeoutReached);
        let wrapped_connect = EpError::connect(original_connect_error.clone());
        assert_eq!(wrapped_connect.error_code(), 0x0503); // Should still be ConnectionTimeout
        assert_eq!(wrapped_connect.error_hex(), "E0503");

        // Test serde() helper
        let original_serde_error = EpError::Serde(SerdeError::DeserializationFailed);
        let wrapped_serde = EpError::serde(original_serde_error.clone());
        assert_eq!(wrapped_serde.error_code(), 0x0602); // Should still be DeserializationFailed
        assert_eq!(wrapped_serde.error_hex(), "E0602");
    }

    #[test]
    fn test_helper_functions_with_strings() {
        // Verify that helper functions still work correctly with regular strings
        let db_error = EpError::database("Custom database message");
        assert_eq!(db_error.error_code(), 0x0AFF); // Should be Custom
        assert!(db_error.to_string().contains("Custom database message"));

        let api_error = EpError::api("Custom API message");
        assert_eq!(api_error.error_code(), 0x01FF); // Should be Custom
        assert!(api_error.to_string().contains("Custom API message"));

        let auth_error = EpError::auth("Custom auth message");
        assert_eq!(auth_error.error_code(), 0x08FF); // Should be Custom
        assert!(auth_error.to_string().contains("Custom auth message"));
    }
}

// Test error type for standalone tests
#[cfg(test)]
#[derive(Debug)]
struct TestDbError(String);

#[cfg(test)]
impl fmt::Display for TestDbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
impl Error for TestDbError {}

#[test]
fn test_database_query_error_no_double_wrapping() {
    // Test that database_query_error prevents double-wrapping
    let original_error = EpError::database_user_not_found();

    // Try to wrap it with database_query_error - should detect and return as-is
    let wrapped = EpError::database_query_error(original_error.clone(), EntityType::User);
    assert_eq!(wrapped.error_code(), 0x0A06); // Should still be UserNotFound
    assert_eq!(wrapped.error_hex(), "E0A06");
    assert!(wrapped.to_string().contains("[E0A06] Database error: User not found"));

    // Verify no double error codes in message
    let error_string = wrapped.to_string();
    assert_eq!(error_string.matches("[E0A06]").count(), 1);

    // Test with different entity types
    let org_error = EpError::database_organization_not_found();
    let wrapped_org = EpError::database_query_error(org_error.clone(), EntityType::Organization);
    assert_eq!(wrapped_org.error_code(), 0x0A07); // OrganizationNotFound

    let endpoint_error = EpError::database_endpoint_not_found();
    let wrapped_endpoint = EpError::database_query_error(endpoint_error.clone(), EntityType::Endpoint);
    assert_eq!(wrapped_endpoint.error_code(), 0x0A08); // EndpointNotFound

    let duplicate_user =
        EpError::database_query_error(TestDbError("duplicate key value violates unique constraint".to_string()), EntityType::User);
    let wrapped_duplicate = EpError::database(duplicate_user);
    assert_eq!(wrapped_duplicate.error_code(), 0x0A0B); // Duplicate
    assert!(wrapped_duplicate.to_string().contains("User already exists"));
}

#[test]
fn test_all_custom_helpers_prevent_double_wrapping() {
    // Test parse() helper
    let parse_error = EpError::Parse(ParseError::InvalidSyntax);
    let wrapped_parse = EpError::parse(parse_error.clone());
    assert_eq!(wrapped_parse.error_code(), 0x0C01);

    // Test init() helper
    let init_error = EpError::Init(InitError::ConfigurationMissing);
    let wrapped_init = EpError::init(init_error.clone());
    assert_eq!(wrapped_init.error_code(), 0x0201);

    // Test fs() helper
    let fs_error = EpError::Fs(FsError::FileNotFound);
    let wrapped_fs = EpError::fs(fs_error.clone());
    assert_eq!(wrapped_fs.error_code(), 0x0F01);
}
