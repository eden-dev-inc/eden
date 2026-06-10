use borsh::{BorshDeserialize, BorshSerialize};
use error::EpError;
use postgres::types::{FromSql, Type};
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Hash, Debug, Clone, Copy, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "lowercase")]
/// Endpoint type/kind enum for different database and service types.
#[derive(Default)]
pub enum EpKind {
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
    Posthog,
    #[default]
    Postgres,
    Rds,
    Redis,
    S3,
    Salesforce,
    Snowflake,
    Tavily,
    Weaviate,
}

impl TryFrom<String> for EpKind {
    type Error = EpError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "aws" | "aws_cloud_control" | "aws_cc" | "cloudcontrol" => Ok(EpKind::Aws),
            "azure" | "azure_rm" | "azure_cloud" => Ok(EpKind::Azure),
            "cassandra" => Ok(EpKind::Cassandra),
            "clickhouse" => Ok(EpKind::Clickhouse),
            "databricks" => Ok(EpKind::Databricks),
            "datadog" => Ok(EpKind::Datadog),
            "elasticache" => Ok(EpKind::Elasticache),
            "eraser" => Ok(EpKind::Eraser),
            "function" | "lambda" | "aws_lambda" => Ok(EpKind::Function),
            "gitlab" => Ok(EpKind::Gitlab),
            "google_workspace" | "gworkspace" | "gsuite" | "googleworkspace" => Ok(EpKind::GoogleWorkspace),
            "http" => Ok(EpKind::Http),
            "llm" => Ok(EpKind::Llm),
            "mongo" | "mongodb" => Ok(EpKind::Mongo),
            "mssql" => Ok(EpKind::Mssql),
            "mysql" => Ok(EpKind::Mysql),
            "oracle" => Ok(EpKind::Oracle),
            "pinecone" => Ok(EpKind::Pinecone),
            "posthog" | "post_hog" => Ok(EpKind::Posthog),
            "postgres" | "postgresql" | "pg" => Ok(EpKind::Postgres),
            "rds" | "aws_rds" => Ok(EpKind::Rds),
            "redis" => Ok(EpKind::Redis),
            "s3" | "aws_s3" | "object_storage" => Ok(EpKind::S3),
            "salesforce" => Ok(EpKind::Salesforce),
            "snowflake" => Ok(EpKind::Snowflake),
            "tavily" => Ok(EpKind::Tavily),
            "weaviate" => Ok(EpKind::Weaviate),
            _ => Err(EpError::parse("failed to parse EpKind from string")),
        }
    }
}

impl FromSql<'_> for EpKind {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<EpKind, Box<dyn std::error::Error + Sync + Send>> {
        let result = std::str::from_utf8(raw)?;
        Ok(EpKind::try_from(result.to_string())?)
    }
    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl EpKind {
    /// Returns the kind as a static string slice (no allocation).
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Aws => "aws",
            Self::Azure => "azure",
            Self::Cassandra => "cassandra",
            Self::Clickhouse => "clickhouse",
            Self::Databricks => "databricks",
            Self::Datadog => "datadog",
            Self::Elasticache => "elasticache",
            Self::Eraser => "eraser",
            Self::Function => "function",
            Self::Gitlab => "gitlab",
            Self::GoogleWorkspace => "google_workspace",
            Self::Http => "http",
            Self::Llm => "llm",
            Self::Mongo => "mongo",
            Self::Mssql => "mssql",
            Self::Mysql => "mysql",
            Self::Oracle => "oracle",
            Self::Pinecone => "pinecone",
            Self::Posthog => "posthog",
            Self::Postgres => "postgres",
            Self::Rds => "rds",
            Self::Redis => "redis",
            Self::S3 => "s3",
            Self::Salesforce => "salesforce",
            Self::Snowflake => "snowflake",
            Self::Tavily => "tavily",
            Self::Weaviate => "weaviate",
        }
    }
}

impl fmt::Display for EpKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl EpKind {
    /// Pre-computed span name for `raw_bytes_with_req_type`. Zero allocation.
    pub const fn span_raw_bytes(&self) -> &'static str {
        match self {
            Self::Aws => "aws.raw_bytes_with_req_type",
            Self::Azure => "azure.raw_bytes_with_req_type",
            Self::Cassandra => "cassandra.raw_bytes_with_req_type",
            Self::Clickhouse => "clickhouse.raw_bytes_with_req_type",
            Self::Databricks => "databricks.raw_bytes_with_req_type",
            Self::Datadog => "datadog.raw_bytes_with_req_type",
            Self::Elasticache => "elasticache.raw_bytes_with_req_type",
            Self::Eraser => "eraser.raw_bytes_with_req_type",
            Self::Function => "function.raw_bytes_with_req_type",
            Self::Gitlab => "gitlab.raw_bytes_with_req_type",
            Self::GoogleWorkspace => "google_workspace.raw_bytes_with_req_type",
            Self::Http => "http.raw_bytes_with_req_type",
            Self::Llm => "llm.raw_bytes_with_req_type",
            Self::Mongo => "mongo.raw_bytes_with_req_type",
            Self::Mssql => "mssql.raw_bytes_with_req_type",
            Self::Mysql => "mysql.raw_bytes_with_req_type",
            Self::Oracle => "oracle.raw_bytes_with_req_type",
            Self::Pinecone => "pinecone.raw_bytes_with_req_type",
            Self::Posthog => "posthog.raw_bytes_with_req_type",
            Self::Postgres => "postgres.raw_bytes_with_req_type",
            Self::Rds => "rds.raw_bytes_with_req_type",
            Self::Redis => "redis.raw_bytes_with_req_type",
            Self::S3 => "s3.raw_bytes_with_req_type",
            Self::Salesforce => "salesforce.raw_bytes_with_req_type",
            Self::Snowflake => "snowflake.raw_bytes_with_req_type",
            Self::Tavily => "tavily.raw_bytes_with_req_type",
            Self::Weaviate => "weaviate.raw_bytes_with_req_type",
        }
    }

    /// Pre-computed span name for pool acquire. Zero allocation.
    pub const fn span_pool_acquire(&self) -> &'static str {
        match self {
            Self::Aws => "aws.pool_acquire",
            Self::Azure => "azure.pool_acquire",
            Self::Cassandra => "cassandra.pool_acquire",
            Self::Clickhouse => "clickhouse.pool_acquire",
            Self::Databricks => "databricks.pool_acquire",
            Self::Datadog => "datadog.pool_acquire",
            Self::Elasticache => "elasticache.pool_acquire",
            Self::Eraser => "eraser.pool_acquire",
            Self::Function => "function.pool_acquire",
            Self::Gitlab => "gitlab.pool_acquire",
            Self::GoogleWorkspace => "google_workspace.pool_acquire",
            Self::Http => "http.pool_acquire",
            Self::Llm => "llm.pool_acquire",
            Self::Mongo => "mongo.pool_acquire",
            Self::Mssql => "mssql.pool_acquire",
            Self::Mysql => "mysql.pool_acquire",
            Self::Oracle => "oracle.pool_acquire",
            Self::Pinecone => "pinecone.pool_acquire",
            Self::Posthog => "posthog.pool_acquire",
            Self::Postgres => "postgres.pool_acquire",
            Self::Rds => "rds.pool_acquire",
            Self::Redis => "redis.pool_acquire",
            Self::S3 => "s3.pool_acquire",
            Self::Salesforce => "salesforce.pool_acquire",
            Self::Snowflake => "snowflake.pool_acquire",
            Self::Tavily => "tavily.pool_acquire",
            Self::Weaviate => "weaviate.pool_acquire",
        }
    }

    /// Pre-computed span name for send_raw_bytes. Zero allocation.
    pub const fn span_send_raw_bytes(&self) -> &'static str {
        match self {
            Self::Aws => "aws.send_raw_bytes",
            Self::Azure => "azure.send_raw_bytes",
            Self::Cassandra => "cassandra.send_raw_bytes",
            Self::Clickhouse => "clickhouse.send_raw_bytes",
            Self::Databricks => "databricks.send_raw_bytes",
            Self::Datadog => "datadog.send_raw_bytes",
            Self::Elasticache => "elasticache.send_raw_bytes",
            Self::Eraser => "eraser.send_raw_bytes",
            Self::Function => "function.send_raw_bytes",
            Self::Gitlab => "gitlab.send_raw_bytes",
            Self::GoogleWorkspace => "google_workspace.send_raw_bytes",
            Self::Http => "http.send_raw_bytes",
            Self::Llm => "llm.send_raw_bytes",
            Self::Mongo => "mongo.send_raw_bytes",
            Self::Mssql => "mssql.send_raw_bytes",
            Self::Mysql => "mysql.send_raw_bytes",
            Self::Oracle => "oracle.send_raw_bytes",
            Self::Pinecone => "pinecone.send_raw_bytes",
            Self::Posthog => "posthog.send_raw_bytes",
            Self::Postgres => "postgres.send_raw_bytes",
            Self::Rds => "rds.send_raw_bytes",
            Self::Redis => "redis.send_raw_bytes",
            Self::S3 => "s3.send_raw_bytes",
            Self::Salesforce => "salesforce.send_raw_bytes",
            Self::Snowflake => "snowflake.send_raw_bytes",
            Self::Tavily => "tavily.send_raw_bytes",
            Self::Weaviate => "weaviate.send_raw_bytes",
        }
    }

    pub fn support_tx(&self) -> bool {
        match self {
            EpKind::Aws => false,
            EpKind::Azure => false,
            EpKind::Cassandra => true,
            EpKind::Clickhouse => false,
            EpKind::Databricks => false,
            EpKind::Datadog => false,
            EpKind::Elasticache => true,
            EpKind::Eraser => false,
            EpKind::Function => false,
            EpKind::Gitlab => false,
            EpKind::GoogleWorkspace => false,
            EpKind::Http => false,
            EpKind::Llm => false,
            EpKind::Mongo => true,
            EpKind::Mssql => true,
            EpKind::Mysql => true,
            EpKind::Oracle => true,
            EpKind::Pinecone => false,
            EpKind::Posthog => false,
            EpKind::Postgres => true,
            EpKind::Rds => true,
            EpKind::Redis => true,
            EpKind::S3 => false,
            EpKind::Salesforce => false,
            EpKind::Snowflake => false,
            EpKind::Tavily => false,
            EpKind::Weaviate => false,
        }
    }
}
