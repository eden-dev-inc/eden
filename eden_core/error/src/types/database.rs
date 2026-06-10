use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Supported database types in Eve.
///
/// Each variant corresponds to a Cargo feature flag that must be enabled to
/// include support for that database type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum DatabaseType {
    Aws,
    Azure,
    Cassandra,
    Clickhouse,
    Databricks,
    Datadog,
    Elasticache,
    Eraser,
    Function,
    Gitlab,
    GoogleWorkspace,
    Http,
    Llm,
    Mongo,
    Mssql,
    Mysql,
    Oracle,
    Pinecone,
    Postgres,
    Rds,
    Redis,
    S3,
    Salesforce,
    Snowflake,
    Tavily,
    Weaviate,
}

impl fmt::Display for DatabaseType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let name = match self {
            DatabaseType::Aws => "AWS",
            DatabaseType::Azure => "Azure",
            DatabaseType::Cassandra => "Cassandra",
            DatabaseType::Clickhouse => "Clickhouse",
            DatabaseType::Databricks => "Databricks",
            DatabaseType::Datadog => "Datadog",
            DatabaseType::Elasticache => "Elasticache",
            DatabaseType::Eraser => "Eraser",
            DatabaseType::Function => "Function",
            DatabaseType::Gitlab => "GitLab",
            DatabaseType::GoogleWorkspace => "Google Workspace",
            DatabaseType::Http => "Http",
            DatabaseType::Llm => "LLM",
            DatabaseType::Mongo => "Mongo",
            DatabaseType::Mssql => "Mssql",
            DatabaseType::Mysql => "Mysql",
            DatabaseType::Oracle => "Oracle",
            DatabaseType::Pinecone => "Pinecone",
            DatabaseType::Postgres => "Postgres",
            DatabaseType::Rds => "RDS",
            DatabaseType::Redis => "Redis",
            DatabaseType::S3 => "S3",
            DatabaseType::Salesforce => "Salesforce",
            DatabaseType::Snowflake => "Snowflake",
            DatabaseType::Tavily => "Tavily",
            DatabaseType::Weaviate => "Weaviate",
        };
        write!(f, "{}", name)
    }
}

impl DatabaseType {
    /// Returns the Cargo feature flag name for this database type.
    pub fn feature_name(&self) -> &'static str {
        match self {
            DatabaseType::Aws => "aws",
            DatabaseType::Azure => "azure",
            DatabaseType::Cassandra => "cassandra",
            DatabaseType::Clickhouse => "clickhouse",
            DatabaseType::Databricks => "databricks",
            DatabaseType::Datadog => "datadog",
            DatabaseType::Elasticache => "elasticache",
            DatabaseType::Eraser => "eraser",
            DatabaseType::Function => "function",
            DatabaseType::Gitlab => "gitlab",
            DatabaseType::GoogleWorkspace => "google_workspace",
            DatabaseType::Http => "http",
            DatabaseType::Llm => "llm",
            DatabaseType::Mongo => "mongo",
            DatabaseType::Mssql => "mssql",
            DatabaseType::Mysql => "mysql",
            DatabaseType::Oracle => "oracle",
            DatabaseType::Pinecone => "pinecone",
            DatabaseType::Postgres => "postgres",
            DatabaseType::Rds => "rds",
            DatabaseType::Redis => "redis",
            DatabaseType::S3 => "s3",
            DatabaseType::Salesforce => "salesforce",
            DatabaseType::Snowflake => "snowflake",
            DatabaseType::Tavily => "tavily",
            DatabaseType::Weaviate => "weaviate",
        }
    }
}

/// PostgreSQL database operation errors (0x0AXX error codes).
///
/// Covers connection failures, query execution errors, entity not found errors,
/// and constraint violations in the primary Eden database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum DatabaseError {
    ConnectionTimeout,               // 0x01
    AuthenticationFailed,            // 0x02
    SchemaError,                     // 0x03
    QueryFailed,                     // 0x04
    TransactionFailed,               // 0x05
    UserNotFound,                    // 0x06
    OrganizationNotFound,            // 0x07
    EndpointNotFound,                // 0x08
    TemplateNotFound,                // 0x09
    WorkflowNotFound,                // 0x0A
    DuplicateUser,                   // 0x0B
    DuplicateOrganization,           // 0x0C
    DuplicateEndpoint,               // 0x0D
    DuplicateTemplate,               // 0x0E
    DuplicateWorkflow,               // 0x0F
    ConstraintViolation,             // 0x10
    IndexCorruption,                 // 0x11
    FeatureNotEnabled(DatabaseType), // 0x12 - "{Database} not supported, use '{feature}' feature flag" (77x)
    ConversationNotFound,            // 0x13
    MigrationNotFound,               // 0x14
    InterlayNotFound,                // 0x15
    EdenNodeNotFound,                // 0x16
    DuplicateMigration,              // 0x17
    DuplicateInterlay,               // 0x18
    DuplicateEdenNode,               // 0x19
    ApiNotFound,                     // 0x1A
    DuplicateApi,                    // 0x1B
    RobotNotFound,                   // 0x1C
    DuplicateRobot,                  // 0x1D
    SnapshotNotFound,                // 0x1E
    DuplicateSnapshot,               // 0x1F
    PipelineNotFound,                // 0x20
    DuplicatePipeline,               // 0x21
    EndpointGroupNotFound,           // 0x22
    DuplicateEndpointGroup,          // 0x23
    Custom(String),                  // 0xFF - For backward compatibility with string errors
}

impl DatabaseError {
    /// Returns the specific error code (0x01-0xFF) for this database error.
    pub fn error_code(&self) -> u8 {
        match self {
            DatabaseError::ConnectionTimeout => 0x01,
            DatabaseError::AuthenticationFailed => 0x02,
            DatabaseError::SchemaError => 0x03,
            DatabaseError::QueryFailed => 0x04,
            DatabaseError::TransactionFailed => 0x05,
            DatabaseError::UserNotFound => 0x06,
            DatabaseError::OrganizationNotFound => 0x07,
            DatabaseError::EndpointNotFound => 0x08,
            DatabaseError::TemplateNotFound => 0x09,
            DatabaseError::WorkflowNotFound => 0x0A,
            DatabaseError::DuplicateUser => 0x0B,
            DatabaseError::DuplicateOrganization => 0x0C,
            DatabaseError::DuplicateEndpoint => 0x0D,
            DatabaseError::DuplicateTemplate => 0x0E,
            DatabaseError::DuplicateWorkflow => 0x0F,
            DatabaseError::ConstraintViolation => 0x10,
            DatabaseError::IndexCorruption => 0x11,
            DatabaseError::FeatureNotEnabled(_) => 0x12,
            DatabaseError::ConversationNotFound => 0x13,
            DatabaseError::MigrationNotFound => 0x14,
            DatabaseError::InterlayNotFound => 0x15,
            DatabaseError::EdenNodeNotFound => 0x16,
            DatabaseError::DuplicateMigration => 0x17,
            DatabaseError::DuplicateInterlay => 0x18,
            DatabaseError::DuplicateEdenNode => 0x19,
            DatabaseError::ApiNotFound => 0x1A,
            DatabaseError::DuplicateApi => 0x1B,
            DatabaseError::RobotNotFound => 0x1C,
            DatabaseError::DuplicateRobot => 0x1D,
            DatabaseError::SnapshotNotFound => 0x1E,
            DatabaseError::DuplicateSnapshot => 0x1F,
            DatabaseError::PipelineNotFound => 0x20,
            DatabaseError::DuplicatePipeline => 0x21,
            DatabaseError::EndpointGroupNotFound => 0x22,
            DatabaseError::DuplicateEndpointGroup => 0x23,
            DatabaseError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            DatabaseError::ConnectionTimeout => "Database connection timeout. Please check network connectivity and try again",
            DatabaseError::AuthenticationFailed => "Database authentication failed. Please check your database credentials",
            DatabaseError::SchemaError => "Database schema error. Please ensure the database is properly initialized",
            DatabaseError::QueryFailed => "Database query execution failed",
            DatabaseError::TransactionFailed => "Database transaction failed",
            DatabaseError::UserNotFound => "User not found. Please verify the user ID is correct",
            DatabaseError::OrganizationNotFound => "Organization not found. Please verify the organization ID is correct",
            DatabaseError::EndpointNotFound => "Endpoint not found. Please verify the endpoint ID is correct",
            DatabaseError::TemplateNotFound => "Template not found. Please verify the template ID is correct",
            DatabaseError::WorkflowNotFound => "Workflow not found. Please verify the workflow ID is correct",
            DatabaseError::DuplicateUser => "User already exists. Please choose a different username",
            DatabaseError::DuplicateOrganization => "Organization already exists. Please choose a different name",
            DatabaseError::DuplicateEndpoint => "Endpoint already exists. Please choose a different identifier",
            DatabaseError::DuplicateTemplate => "Template already exists. Please choose a different identifier",
            DatabaseError::DuplicateWorkflow => "Workflow already exists. Please choose a different identifier",
            DatabaseError::ConstraintViolation => "Database constraint violation. Operation violates data integrity rules",
            DatabaseError::IndexCorruption => "Database index corruption detected. Please contact system administrator",
            DatabaseError::FeatureNotEnabled(db_type) => {
                return write!(f, "{} not supported, use '{}' feature flag in Cargo.toml", db_type, db_type.feature_name());
            }
            DatabaseError::ConversationNotFound => "Conversation not found. Please verify the conversation ID is correct",
            DatabaseError::MigrationNotFound => "Migration not found. Please verify the migration ID is correct",
            DatabaseError::InterlayNotFound => "Interlay not found. Please verify the interlay ID is correct",
            DatabaseError::EdenNodeNotFound => "Eden node not found. Please verify the node ID is correct",
            DatabaseError::DuplicateMigration => "Migration already exists. Please choose a different identifier",
            DatabaseError::DuplicateInterlay => "Interlay already exists. Please choose a different identifier",
            DatabaseError::DuplicateEdenNode => "Eden node already exists. Please choose a different identifier",
            DatabaseError::ApiNotFound => "Api not found. Please verify the api ID is correct",
            DatabaseError::DuplicateApi => "Api already exists. Please choose a different identifier",
            DatabaseError::RobotNotFound => "Robot not found. Please verify the robot ID is correct",
            DatabaseError::DuplicateRobot => "Robot already exists. Please choose a different identifier",
            DatabaseError::SnapshotNotFound => "Snapshot not found. Please verify the snapshot ID is correct",
            DatabaseError::DuplicateSnapshot => "Snapshot already exists. Please choose a different identifier",
            DatabaseError::PipelineNotFound => "Pipeline not found. Please verify the pipeline ID is correct",
            DatabaseError::DuplicatePipeline => "Pipeline already exists. Please choose a different identifier",
            DatabaseError::EndpointGroupNotFound => "Endpoint group not found. Please verify the endpoint group ID is correct",
            DatabaseError::DuplicateEndpointGroup => "Endpoint group already exists. Please choose a different identifier",
            DatabaseError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
