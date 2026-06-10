#[cfg(feature = "aws")]
use crate::aws_core::config::AwsConfig;
#[cfg(feature = "azure")]
use crate::azure_core::config::AzureConfig;
#[cfg(feature = "cassandra")]
use crate::cassandra_core::config::CassandraConfig;
#[cfg(feature = "clickhouse")]
use crate::clickhouse_core::config::ClickhouseConfig;
#[cfg(feature = "databricks")]
use crate::databricks_core::config::DatabricksConfig;
#[cfg(feature = "datadog")]
use crate::datadog_core::config::DatadogConfig;
use crate::ep_core::ep::EpConfig;
#[cfg(feature = "eraser")]
use crate::eraser_core::config::EraserConfig;
#[cfg(feature = "function")]
use crate::function_core::config::FunctionConfig;
#[cfg(feature = "gitlab")]
use crate::gitlab_core::config::GitlabConfig;
#[cfg(feature = "gworkspace")]
use crate::gworkspace_core::config::GoogleWorkspaceConfig;
#[cfg(feature = "http")]
use crate::http_core::config::HttpConfig;

#[cfg(feature = "llm")]
use crate::llm_core::config::LlmConfig;
#[cfg(feature = "mongo")]
use crate::mongo_core::config::MongoConfig;
#[cfg(feature = "mssql")]
use crate::mssql_core::config::MssqlConfig;
#[cfg(feature = "mysql")]
use crate::mysql_core::config::MysqlConfig;
#[cfg(feature = "oracle")]
use crate::oracle_core::config::OracleConfig;
#[cfg(feature = "pinecone")]
use crate::pinecone_core::config::PineconeConfig;
#[cfg(feature = "posthog")]
use crate::posthog_core::config::PosthogConfig;
#[cfg(any(feature = "postgres", feature = "rds"))]
use crate::postgres_core::PostgresConfig;
#[cfg(any(feature = "redis", feature = "elasticache"))]
use crate::redis_core::config::RedisConfig;

#[cfg(feature = "s3")]
use crate::s3_core::config::S3Config;
#[cfg(feature = "salesforce")]
use crate::salesforce_core::config::SalesforceConfig;
#[cfg(feature = "snowflake")]
use crate::snowflake_core::config::SnowflakeConfig;
#[cfg(feature = "tavily")]
use crate::tavily_core::config::TavilyConfig;
#[cfg(feature = "weaviate")]
use crate::weaviate_core::config::WeaviateConfig;
// use database::db::lib::{DatabaseManager, EdenPostgresConnection, EdenRedisConnection, EdenClickhouseConnection};
// use database::db::methods::update::{SqlQueries, UpdateMethod};
use crate::EndpointSchemaInput;
use borsh::{BorshDeserialize, BorshSerialize};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use error::{EpError, ResultEP};
use format::{EdenId, EndpointId, EndpointUuid, UserUuid};
use format::{
    cache_id::{CacheId, EndpointCacheId},
    cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid},
    endpoint::EpKind,
    timestamp::DateTimeWrapper,
};
use ep_core::database::schema::Row;
use ep_core::database::schema::routing::EndpointRouting;
use ep_core::database::schema::{FromRow, Table};
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use redis::{FromRedisValue, ToRedisArgs};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::{any::Any, fmt::Debug};
use utoipa::openapi::{ObjectBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

#[allow(dead_code)]
fn unsupported_endpoint_config(kind: EpKind) -> String {
    format!("{kind:?} not supported in this build")
}

#[allow(unused_macros)]
macro_rules! serialize_config_value {
    ($config:expr, $config_type:ty, $type_name:expr) => {{
        let config = $config
            .as_any()
            .downcast_ref::<$config_type>()
            .ok_or_else(|| format!("Failed to downcast {}", $type_name))?;
        serde_json::to_value(config).map_err(|e| format!("Failed to serialize {}: {}", $type_name, e))
    }};
}

#[allow(unused_macros)]
macro_rules! json_config_to_box {
    ($config:expr, $config_type:ty, $type_name:expr) => {{
        let config: $config_type =
            serde_json::from_value($config).map_err(|e| format!("Failed to deserialize {}: {}", $type_name, e))?;
        Ok(BoxEpConfig::new(Box::new(config)))
    }};
}

#[allow(unused_macros)]
macro_rules! borsh_config_from_slice {
    ($config_data:expr, $config_type:ty) => {{
        let config: $config_type =
            borsh::from_slice($config_data).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Box::new(config) as Box<dyn EpConfig>)
    }};
}

#[allow(unused_macros)]
macro_rules! borsh_config_from_reader {
    ($reader:expr, $config_type:ty) => {{
        let config = <$config_type>::deserialize_reader($reader)?;
        Ok(BoxEpConfig::new(Box::new(config)))
    }};
}

#[allow(unused_macros)]
macro_rules! borsh_serialize_config {
    ($config:expr, $writer:expr, $config_type:ty, $type_name:expr) => {
        BorshSerialize::serialize(
            &$config
                .as_any()
                .downcast_ref::<$config_type>()
                .ok_or(std::io::Error::other(format!("error casting to {}", $type_name)))?,
            $writer,
        )
    };
}

fn serialize_endpoint_config(_config: &dyn EpConfig, kind: EpKind) -> Result<Value, String> {
    match kind {
        #[cfg(feature = "aws")]
        EpKind::Aws => serialize_config_value!(_config, AwsConfig, "AwsConfig"),
        #[cfg(not(feature = "aws"))]
        EpKind::Aws => Err(unsupported_endpoint_config(EpKind::Aws)),
        #[cfg(feature = "azure")]
        EpKind::Azure => serialize_config_value!(_config, AzureConfig, "AzureConfig"),
        #[cfg(not(feature = "azure"))]
        EpKind::Azure => Err(unsupported_endpoint_config(EpKind::Azure)),
        #[cfg(feature = "cassandra")]
        EpKind::Cassandra => serialize_config_value!(_config, CassandraConfig, "CassandraConfig"),
        #[cfg(not(feature = "cassandra"))]
        EpKind::Cassandra => Err(unsupported_endpoint_config(EpKind::Cassandra)),
        #[cfg(feature = "clickhouse")]
        EpKind::Clickhouse => serialize_config_value!(_config, ClickhouseConfig, "ClickhouseConfig"),
        #[cfg(not(feature = "clickhouse"))]
        EpKind::Clickhouse => Err(unsupported_endpoint_config(EpKind::Clickhouse)),
        #[cfg(feature = "databricks")]
        EpKind::Databricks => serialize_config_value!(_config, DatabricksConfig, "DatabricksConfig"),
        #[cfg(not(feature = "databricks"))]
        EpKind::Databricks => Err(unsupported_endpoint_config(EpKind::Databricks)),
        #[cfg(feature = "datadog")]
        EpKind::Datadog => serialize_config_value!(_config, DatadogConfig, "DatadogConfig"),
        #[cfg(not(feature = "datadog"))]
        EpKind::Datadog => Err(unsupported_endpoint_config(EpKind::Datadog)),
        #[cfg(feature = "elasticache")]
        EpKind::Elasticache => serialize_config_value!(_config, RedisConfig, "RedisConfig"),
        #[cfg(not(feature = "elasticache"))]
        EpKind::Elasticache => Err(unsupported_endpoint_config(EpKind::Elasticache)),
        #[cfg(feature = "eraser")]
        EpKind::Eraser => serialize_config_value!(_config, EraserConfig, "EraserConfig"),
        #[cfg(not(feature = "eraser"))]
        EpKind::Eraser => Err(unsupported_endpoint_config(EpKind::Eraser)),
        #[cfg(feature = "function")]
        EpKind::Function => serialize_config_value!(_config, FunctionConfig, "FunctionConfig"),
        #[cfg(not(feature = "function"))]
        EpKind::Function => Err(unsupported_endpoint_config(EpKind::Function)),
        #[cfg(feature = "gitlab")]
        EpKind::Gitlab => serialize_config_value!(_config, GitlabConfig, "GitlabConfig"),
        #[cfg(not(feature = "gitlab"))]
        EpKind::Gitlab => Err(unsupported_endpoint_config(EpKind::Gitlab)),
        #[cfg(feature = "gworkspace")]
        EpKind::GoogleWorkspace => serialize_config_value!(_config, GoogleWorkspaceConfig, "GoogleWorkspaceConfig"),
        #[cfg(not(feature = "gworkspace"))]
        EpKind::GoogleWorkspace => Err(unsupported_endpoint_config(EpKind::GoogleWorkspace)),
        #[cfg(feature = "http")]
        EpKind::Http => serialize_config_value!(_config, HttpConfig, "HttpConfig"),
        #[cfg(not(feature = "http"))]
        EpKind::Http => Err(unsupported_endpoint_config(EpKind::Http)),
        #[cfg(feature = "llm")]
        EpKind::Llm => serialize_config_value!(_config, LlmConfig, "LlmConfig"),
        #[cfg(not(feature = "llm"))]
        EpKind::Llm => Err(unsupported_endpoint_config(EpKind::Llm)),
        #[cfg(feature = "mongo")]
        EpKind::Mongo => serialize_config_value!(_config, MongoConfig, "MongoConfig"),
        #[cfg(not(feature = "mongo"))]
        EpKind::Mongo => Err(unsupported_endpoint_config(EpKind::Mongo)),
        #[cfg(feature = "mssql")]
        EpKind::Mssql => serialize_config_value!(_config, MssqlConfig, "MssqlConfig"),
        #[cfg(not(feature = "mssql"))]
        EpKind::Mssql => Err(unsupported_endpoint_config(EpKind::Mssql)),
        #[cfg(feature = "mysql")]
        EpKind::Mysql => serialize_config_value!(_config, MysqlConfig, "MysqlConfig"),
        #[cfg(not(feature = "mysql"))]
        EpKind::Mysql => Err(unsupported_endpoint_config(EpKind::Mysql)),
        #[cfg(feature = "oracle")]
        EpKind::Oracle => serialize_config_value!(_config, OracleConfig, "OracleConfig"),
        #[cfg(not(feature = "oracle"))]
        EpKind::Oracle => Err(unsupported_endpoint_config(EpKind::Oracle)),
        #[cfg(feature = "pinecone")]
        EpKind::Pinecone => serialize_config_value!(_config, PineconeConfig, "PineconeConfig"),
        #[cfg(not(feature = "pinecone"))]
        EpKind::Pinecone => Err(unsupported_endpoint_config(EpKind::Pinecone)),
        #[cfg(feature = "posthog")]
        EpKind::Posthog => serialize_config_value!(_config, PosthogConfig, "PosthogConfig"),
        #[cfg(not(feature = "posthog"))]
        EpKind::Posthog => Err(unsupported_endpoint_config(EpKind::Posthog)),
        #[cfg(feature = "postgres")]
        EpKind::Postgres => serialize_config_value!(_config, PostgresConfig, "PostgresConfig"),
        #[cfg(not(feature = "postgres"))]
        EpKind::Postgres => Err(unsupported_endpoint_config(EpKind::Postgres)),
        #[cfg(feature = "rds")]
        EpKind::Rds => serialize_config_value!(_config, PostgresConfig, "PostgresConfig"),
        #[cfg(not(feature = "rds"))]
        EpKind::Rds => Err(unsupported_endpoint_config(EpKind::Rds)),
        #[cfg(feature = "redis")]
        EpKind::Redis => serialize_config_value!(_config, RedisConfig, "RedisConfig"),
        #[cfg(not(feature = "redis"))]
        EpKind::Redis => Err(unsupported_endpoint_config(EpKind::Redis)),
        #[cfg(feature = "s3")]
        EpKind::S3 => serialize_config_value!(_config, S3Config, "S3Config"),
        #[cfg(not(feature = "s3"))]
        EpKind::S3 => Err(unsupported_endpoint_config(EpKind::S3)),
        #[cfg(feature = "salesforce")]
        EpKind::Salesforce => serialize_config_value!(_config, SalesforceConfig, "SalesforceConfig"),
        #[cfg(not(feature = "salesforce"))]
        EpKind::Salesforce => Err(unsupported_endpoint_config(EpKind::Salesforce)),
        #[cfg(feature = "snowflake")]
        EpKind::Snowflake => serialize_config_value!(_config, SnowflakeConfig, "SnowflakeConfig"),
        #[cfg(not(feature = "snowflake"))]
        EpKind::Snowflake => Err(unsupported_endpoint_config(EpKind::Snowflake)),
        #[cfg(feature = "tavily")]
        EpKind::Tavily => serialize_config_value!(_config, TavilyConfig, "TavilyConfig"),
        #[cfg(not(feature = "tavily"))]
        EpKind::Tavily => Err(unsupported_endpoint_config(EpKind::Tavily)),
        #[cfg(feature = "weaviate")]
        EpKind::Weaviate => serialize_config_value!(_config, WeaviateConfig, "WeaviateConfig"),
        #[cfg(not(feature = "weaviate"))]
        EpKind::Weaviate => Err(unsupported_endpoint_config(EpKind::Weaviate)),
    }
}

fn json_config_to_box_ep_config(kind: EpKind, _config: Value) -> Result<BoxEpConfig, String> {
    match kind {
        #[cfg(feature = "aws")]
        EpKind::Aws => json_config_to_box!(_config, AwsConfig, "AwsConfig"),
        #[cfg(not(feature = "aws"))]
        EpKind::Aws => Err(unsupported_endpoint_config(EpKind::Aws)),
        #[cfg(feature = "azure")]
        EpKind::Azure => json_config_to_box!(_config, AzureConfig, "AzureConfig"),
        #[cfg(not(feature = "azure"))]
        EpKind::Azure => Err(unsupported_endpoint_config(EpKind::Azure)),
        #[cfg(feature = "cassandra")]
        EpKind::Cassandra => json_config_to_box!(_config, CassandraConfig, "CassandraConfig"),
        #[cfg(not(feature = "cassandra"))]
        EpKind::Cassandra => Err(unsupported_endpoint_config(EpKind::Cassandra)),
        #[cfg(feature = "clickhouse")]
        EpKind::Clickhouse => json_config_to_box!(_config, ClickhouseConfig, "ClickhouseConfig"),
        #[cfg(not(feature = "clickhouse"))]
        EpKind::Clickhouse => Err(unsupported_endpoint_config(EpKind::Clickhouse)),
        #[cfg(feature = "databricks")]
        EpKind::Databricks => json_config_to_box!(_config, DatabricksConfig, "DatabricksConfig"),
        #[cfg(not(feature = "databricks"))]
        EpKind::Databricks => Err(unsupported_endpoint_config(EpKind::Databricks)),
        #[cfg(feature = "datadog")]
        EpKind::Datadog => json_config_to_box!(_config, DatadogConfig, "DatadogConfig"),
        #[cfg(not(feature = "datadog"))]
        EpKind::Datadog => Err(unsupported_endpoint_config(EpKind::Datadog)),
        #[cfg(feature = "elasticache")]
        EpKind::Elasticache => json_config_to_box!(_config, RedisConfig, "RedisConfig"),
        #[cfg(not(feature = "elasticache"))]
        EpKind::Elasticache => Err(unsupported_endpoint_config(EpKind::Elasticache)),
        #[cfg(feature = "eraser")]
        EpKind::Eraser => json_config_to_box!(_config, EraserConfig, "EraserConfig"),
        #[cfg(not(feature = "eraser"))]
        EpKind::Eraser => Err(unsupported_endpoint_config(EpKind::Eraser)),
        #[cfg(feature = "function")]
        EpKind::Function => json_config_to_box!(_config, FunctionConfig, "FunctionConfig"),
        #[cfg(not(feature = "function"))]
        EpKind::Function => Err(unsupported_endpoint_config(EpKind::Function)),
        #[cfg(feature = "gitlab")]
        EpKind::Gitlab => json_config_to_box!(_config, GitlabConfig, "GitlabConfig"),
        #[cfg(not(feature = "gitlab"))]
        EpKind::Gitlab => Err(unsupported_endpoint_config(EpKind::Gitlab)),
        #[cfg(feature = "gworkspace")]
        EpKind::GoogleWorkspace => json_config_to_box!(_config, GoogleWorkspaceConfig, "GoogleWorkspaceConfig"),
        #[cfg(not(feature = "gworkspace"))]
        EpKind::GoogleWorkspace => Err(unsupported_endpoint_config(EpKind::GoogleWorkspace)),
        #[cfg(feature = "http")]
        EpKind::Http => json_config_to_box!(_config, HttpConfig, "HttpConfig"),
        #[cfg(not(feature = "http"))]
        EpKind::Http => Err(unsupported_endpoint_config(EpKind::Http)),
        #[cfg(feature = "llm")]
        EpKind::Llm => json_config_to_box!(_config, LlmConfig, "LlmConfig"),
        #[cfg(not(feature = "llm"))]
        EpKind::Llm => Err(unsupported_endpoint_config(EpKind::Llm)),
        #[cfg(feature = "mongo")]
        EpKind::Mongo => json_config_to_box!(_config, MongoConfig, "MongoConfig"),
        #[cfg(not(feature = "mongo"))]
        EpKind::Mongo => Err(unsupported_endpoint_config(EpKind::Mongo)),
        #[cfg(feature = "mssql")]
        EpKind::Mssql => json_config_to_box!(_config, MssqlConfig, "MssqlConfig"),
        #[cfg(not(feature = "mssql"))]
        EpKind::Mssql => Err(unsupported_endpoint_config(EpKind::Mssql)),
        #[cfg(feature = "mysql")]
        EpKind::Mysql => json_config_to_box!(_config, MysqlConfig, "MysqlConfig"),
        #[cfg(not(feature = "mysql"))]
        EpKind::Mysql => Err(unsupported_endpoint_config(EpKind::Mysql)),
        #[cfg(feature = "oracle")]
        EpKind::Oracle => json_config_to_box!(_config, OracleConfig, "OracleConfig"),
        #[cfg(not(feature = "oracle"))]
        EpKind::Oracle => Err(unsupported_endpoint_config(EpKind::Oracle)),
        #[cfg(feature = "pinecone")]
        EpKind::Pinecone => json_config_to_box!(_config, PineconeConfig, "PineconeConfig"),
        #[cfg(not(feature = "pinecone"))]
        EpKind::Pinecone => Err(unsupported_endpoint_config(EpKind::Pinecone)),
        #[cfg(feature = "posthog")]
        EpKind::Posthog => json_config_to_box!(_config, PosthogConfig, "PosthogConfig"),
        #[cfg(not(feature = "posthog"))]
        EpKind::Posthog => Err(unsupported_endpoint_config(EpKind::Posthog)),
        #[cfg(feature = "postgres")]
        EpKind::Postgres => json_config_to_box!(_config, PostgresConfig, "PostgresConfig"),
        #[cfg(not(feature = "postgres"))]
        EpKind::Postgres => Err(unsupported_endpoint_config(EpKind::Postgres)),
        #[cfg(feature = "rds")]
        EpKind::Rds => json_config_to_box!(_config, PostgresConfig, "PostgresConfig"),
        #[cfg(not(feature = "rds"))]
        EpKind::Rds => Err(unsupported_endpoint_config(EpKind::Rds)),
        #[cfg(feature = "redis")]
        EpKind::Redis => json_config_to_box!(_config, RedisConfig, "RedisConfig"),
        #[cfg(not(feature = "redis"))]
        EpKind::Redis => Err(unsupported_endpoint_config(EpKind::Redis)),
        #[cfg(feature = "s3")]
        EpKind::S3 => json_config_to_box!(_config, S3Config, "S3Config"),
        #[cfg(not(feature = "s3"))]
        EpKind::S3 => Err(unsupported_endpoint_config(EpKind::S3)),
        #[cfg(feature = "salesforce")]
        EpKind::Salesforce => json_config_to_box!(_config, SalesforceConfig, "SalesforceConfig"),
        #[cfg(not(feature = "salesforce"))]
        EpKind::Salesforce => Err(unsupported_endpoint_config(EpKind::Salesforce)),
        #[cfg(feature = "snowflake")]
        EpKind::Snowflake => json_config_to_box!(_config, SnowflakeConfig, "SnowflakeConfig"),
        #[cfg(not(feature = "snowflake"))]
        EpKind::Snowflake => Err(unsupported_endpoint_config(EpKind::Snowflake)),
        #[cfg(feature = "tavily")]
        EpKind::Tavily => json_config_to_box!(_config, TavilyConfig, "TavilyConfig"),
        #[cfg(not(feature = "tavily"))]
        EpKind::Tavily => Err(unsupported_endpoint_config(EpKind::Tavily)),
        #[cfg(feature = "weaviate")]
        EpKind::Weaviate => json_config_to_box!(_config, WeaviateConfig, "WeaviateConfig"),
        #[cfg(not(feature = "weaviate"))]
        EpKind::Weaviate => Err(unsupported_endpoint_config(EpKind::Weaviate)),
    }
}

fn borsh_config_from_kind_and_slice(kind: EpKind, _config_data: &[u8]) -> std::io::Result<Box<dyn EpConfig>> {
    match kind {
        #[cfg(feature = "aws")]
        EpKind::Aws => borsh_config_from_slice!(_config_data, AwsConfig),
        #[cfg(not(feature = "aws"))]
        EpKind::Aws => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Aws))),
        #[cfg(feature = "azure")]
        EpKind::Azure => borsh_config_from_slice!(_config_data, AzureConfig),
        #[cfg(not(feature = "azure"))]
        EpKind::Azure => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Azure))),
        #[cfg(feature = "cassandra")]
        EpKind::Cassandra => borsh_config_from_slice!(_config_data, CassandraConfig),
        #[cfg(not(feature = "cassandra"))]
        EpKind::Cassandra => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Cassandra))),
        #[cfg(feature = "clickhouse")]
        EpKind::Clickhouse => borsh_config_from_slice!(_config_data, ClickhouseConfig),
        #[cfg(not(feature = "clickhouse"))]
        EpKind::Clickhouse => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Clickhouse))),
        #[cfg(feature = "databricks")]
        EpKind::Databricks => borsh_config_from_slice!(_config_data, DatabricksConfig),
        #[cfg(not(feature = "databricks"))]
        EpKind::Databricks => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Databricks))),
        #[cfg(feature = "datadog")]
        EpKind::Datadog => borsh_config_from_slice!(_config_data, DatadogConfig),
        #[cfg(not(feature = "datadog"))]
        EpKind::Datadog => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Datadog))),
        #[cfg(feature = "elasticache")]
        EpKind::Elasticache => borsh_config_from_slice!(_config_data, RedisConfig),
        #[cfg(not(feature = "elasticache"))]
        EpKind::Elasticache => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Elasticache))),
        #[cfg(feature = "eraser")]
        EpKind::Eraser => borsh_config_from_slice!(_config_data, EraserConfig),
        #[cfg(not(feature = "eraser"))]
        EpKind::Eraser => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Eraser))),
        #[cfg(feature = "function")]
        EpKind::Function => borsh_config_from_slice!(_config_data, FunctionConfig),
        #[cfg(not(feature = "function"))]
        EpKind::Function => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Function))),
        #[cfg(feature = "gitlab")]
        EpKind::Gitlab => borsh_config_from_slice!(_config_data, GitlabConfig),
        #[cfg(not(feature = "gitlab"))]
        EpKind::Gitlab => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Gitlab))),
        #[cfg(feature = "gworkspace")]
        EpKind::GoogleWorkspace => borsh_config_from_slice!(_config_data, GoogleWorkspaceConfig),
        #[cfg(not(feature = "gworkspace"))]
        EpKind::GoogleWorkspace => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::GoogleWorkspace))),
        #[cfg(feature = "http")]
        EpKind::Http => borsh_config_from_slice!(_config_data, HttpConfig),
        #[cfg(not(feature = "http"))]
        EpKind::Http => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Http))),
        #[cfg(feature = "llm")]
        EpKind::Llm => borsh_config_from_slice!(_config_data, LlmConfig),
        #[cfg(not(feature = "llm"))]
        EpKind::Llm => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Llm))),
        #[cfg(feature = "mongo")]
        EpKind::Mongo => borsh_config_from_slice!(_config_data, MongoConfig),
        #[cfg(not(feature = "mongo"))]
        EpKind::Mongo => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Mongo))),
        #[cfg(feature = "mssql")]
        EpKind::Mssql => borsh_config_from_slice!(_config_data, MssqlConfig),
        #[cfg(not(feature = "mssql"))]
        EpKind::Mssql => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Mssql))),
        #[cfg(feature = "mysql")]
        EpKind::Mysql => borsh_config_from_slice!(_config_data, MysqlConfig),
        #[cfg(not(feature = "mysql"))]
        EpKind::Mysql => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Mysql))),
        #[cfg(feature = "oracle")]
        EpKind::Oracle => borsh_config_from_slice!(_config_data, OracleConfig),
        #[cfg(not(feature = "oracle"))]
        EpKind::Oracle => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Oracle))),
        #[cfg(feature = "pinecone")]
        EpKind::Pinecone => borsh_config_from_slice!(_config_data, PineconeConfig),
        #[cfg(not(feature = "pinecone"))]
        EpKind::Pinecone => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Pinecone))),
        #[cfg(feature = "posthog")]
        EpKind::Posthog => borsh_config_from_slice!(_config_data, PosthogConfig),
        #[cfg(not(feature = "posthog"))]
        EpKind::Posthog => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Posthog))),
        #[cfg(feature = "postgres")]
        EpKind::Postgres => borsh_config_from_slice!(_config_data, PostgresConfig),
        #[cfg(not(feature = "postgres"))]
        EpKind::Postgres => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Postgres))),
        #[cfg(feature = "rds")]
        EpKind::Rds => borsh_config_from_slice!(_config_data, PostgresConfig),
        #[cfg(not(feature = "rds"))]
        EpKind::Rds => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Rds))),
        #[cfg(feature = "redis")]
        EpKind::Redis => borsh_config_from_slice!(_config_data, RedisConfig),
        #[cfg(not(feature = "redis"))]
        EpKind::Redis => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Redis))),
        #[cfg(feature = "s3")]
        EpKind::S3 => borsh_config_from_slice!(_config_data, S3Config),
        #[cfg(not(feature = "s3"))]
        EpKind::S3 => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::S3))),
        #[cfg(feature = "salesforce")]
        EpKind::Salesforce => borsh_config_from_slice!(_config_data, SalesforceConfig),
        #[cfg(not(feature = "salesforce"))]
        EpKind::Salesforce => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Salesforce))),
        #[cfg(feature = "snowflake")]
        EpKind::Snowflake => borsh_config_from_slice!(_config_data, SnowflakeConfig),
        #[cfg(not(feature = "snowflake"))]
        EpKind::Snowflake => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Snowflake))),
        #[cfg(feature = "tavily")]
        EpKind::Tavily => borsh_config_from_slice!(_config_data, TavilyConfig),
        #[cfg(not(feature = "tavily"))]
        EpKind::Tavily => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Tavily))),
        #[cfg(feature = "weaviate")]
        EpKind::Weaviate => borsh_config_from_slice!(_config_data, WeaviateConfig),
        #[cfg(not(feature = "weaviate"))]
        EpKind::Weaviate => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Weaviate))),
    }
}

fn borsh_config_from_kind_and_reader<R: std::io::Read>(kind: EpKind, _reader: &mut R) -> std::io::Result<BoxEpConfig> {
    match kind {
        #[cfg(feature = "aws")]
        EpKind::Aws => borsh_config_from_reader!(_reader, AwsConfig),
        #[cfg(not(feature = "aws"))]
        EpKind::Aws => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Aws))),
        #[cfg(feature = "azure")]
        EpKind::Azure => borsh_config_from_reader!(_reader, AzureConfig),
        #[cfg(not(feature = "azure"))]
        EpKind::Azure => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Azure))),
        #[cfg(feature = "cassandra")]
        EpKind::Cassandra => borsh_config_from_reader!(_reader, CassandraConfig),
        #[cfg(not(feature = "cassandra"))]
        EpKind::Cassandra => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Cassandra))),
        #[cfg(feature = "clickhouse")]
        EpKind::Clickhouse => borsh_config_from_reader!(_reader, ClickhouseConfig),
        #[cfg(not(feature = "clickhouse"))]
        EpKind::Clickhouse => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Clickhouse))),
        #[cfg(feature = "databricks")]
        EpKind::Databricks => borsh_config_from_reader!(_reader, DatabricksConfig),
        #[cfg(not(feature = "databricks"))]
        EpKind::Databricks => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Databricks))),
        #[cfg(feature = "datadog")]
        EpKind::Datadog => borsh_config_from_reader!(_reader, DatadogConfig),
        #[cfg(not(feature = "datadog"))]
        EpKind::Datadog => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Datadog))),
        #[cfg(feature = "elasticache")]
        EpKind::Elasticache => borsh_config_from_reader!(_reader, RedisConfig),
        #[cfg(not(feature = "elasticache"))]
        EpKind::Elasticache => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Elasticache))),
        #[cfg(feature = "eraser")]
        EpKind::Eraser => borsh_config_from_reader!(_reader, EraserConfig),
        #[cfg(not(feature = "eraser"))]
        EpKind::Eraser => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Eraser))),
        #[cfg(feature = "function")]
        EpKind::Function => borsh_config_from_reader!(_reader, FunctionConfig),
        #[cfg(not(feature = "function"))]
        EpKind::Function => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Function))),
        #[cfg(feature = "gitlab")]
        EpKind::Gitlab => borsh_config_from_reader!(_reader, GitlabConfig),
        #[cfg(not(feature = "gitlab"))]
        EpKind::Gitlab => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Gitlab))),
        #[cfg(feature = "gworkspace")]
        EpKind::GoogleWorkspace => borsh_config_from_reader!(_reader, GoogleWorkspaceConfig),
        #[cfg(not(feature = "gworkspace"))]
        EpKind::GoogleWorkspace => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::GoogleWorkspace))),
        #[cfg(feature = "http")]
        EpKind::Http => borsh_config_from_reader!(_reader, HttpConfig),
        #[cfg(not(feature = "http"))]
        EpKind::Http => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Http))),
        #[cfg(feature = "llm")]
        EpKind::Llm => borsh_config_from_reader!(_reader, LlmConfig),
        #[cfg(not(feature = "llm"))]
        EpKind::Llm => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Llm))),
        #[cfg(feature = "mongo")]
        EpKind::Mongo => borsh_config_from_reader!(_reader, MongoConfig),
        #[cfg(not(feature = "mongo"))]
        EpKind::Mongo => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Mongo))),
        #[cfg(feature = "mssql")]
        EpKind::Mssql => borsh_config_from_reader!(_reader, MssqlConfig),
        #[cfg(not(feature = "mssql"))]
        EpKind::Mssql => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Mssql))),
        #[cfg(feature = "mysql")]
        EpKind::Mysql => borsh_config_from_reader!(_reader, MysqlConfig),
        #[cfg(not(feature = "mysql"))]
        EpKind::Mysql => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Mysql))),
        #[cfg(feature = "oracle")]
        EpKind::Oracle => borsh_config_from_reader!(_reader, OracleConfig),
        #[cfg(not(feature = "oracle"))]
        EpKind::Oracle => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Oracle))),
        #[cfg(feature = "pinecone")]
        EpKind::Pinecone => borsh_config_from_reader!(_reader, PineconeConfig),
        #[cfg(not(feature = "pinecone"))]
        EpKind::Pinecone => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Pinecone))),
        #[cfg(feature = "posthog")]
        EpKind::Posthog => borsh_config_from_reader!(_reader, PosthogConfig),
        #[cfg(not(feature = "posthog"))]
        EpKind::Posthog => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Posthog))),
        #[cfg(feature = "postgres")]
        EpKind::Postgres => borsh_config_from_reader!(_reader, PostgresConfig),
        #[cfg(not(feature = "postgres"))]
        EpKind::Postgres => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Postgres))),
        #[cfg(feature = "rds")]
        EpKind::Rds => borsh_config_from_reader!(_reader, PostgresConfig),
        #[cfg(not(feature = "rds"))]
        EpKind::Rds => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Rds))),
        #[cfg(feature = "redis")]
        EpKind::Redis => borsh_config_from_reader!(_reader, RedisConfig),
        #[cfg(not(feature = "redis"))]
        EpKind::Redis => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Redis))),
        #[cfg(feature = "s3")]
        EpKind::S3 => borsh_config_from_reader!(_reader, S3Config),
        #[cfg(not(feature = "s3"))]
        EpKind::S3 => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::S3))),
        #[cfg(feature = "salesforce")]
        EpKind::Salesforce => borsh_config_from_reader!(_reader, SalesforceConfig),
        #[cfg(not(feature = "salesforce"))]
        EpKind::Salesforce => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Salesforce))),
        #[cfg(feature = "snowflake")]
        EpKind::Snowflake => borsh_config_from_reader!(_reader, SnowflakeConfig),
        #[cfg(not(feature = "snowflake"))]
        EpKind::Snowflake => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Snowflake))),
        #[cfg(feature = "tavily")]
        EpKind::Tavily => borsh_config_from_reader!(_reader, TavilyConfig),
        #[cfg(not(feature = "tavily"))]
        EpKind::Tavily => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Tavily))),
        #[cfg(feature = "weaviate")]
        EpKind::Weaviate => borsh_config_from_reader!(_reader, WeaviateConfig),
        #[cfg(not(feature = "weaviate"))]
        EpKind::Weaviate => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Weaviate))),
    }
}

#[derive(Debug, Clone)]
pub struct EndpointSchema {
    id: EndpointId,
    uuid: EndpointUuid,
    kind: EpKind,
    config: BoxEpConfig,
    routing: Option<EndpointRouting>,
    description: Option<String>,
    created_by: UserUuid,
    updated_by: UserUuid,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl Serialize for EndpointSchema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(10))?;
        map.serialize_entry("id", &self.id)?;
        map.serialize_entry("uuid", &self.uuid)?;
        map.serialize_entry("kind", &self.kind)?;

        let config_value = serialize_endpoint_config(self.config.as_ref(), self.kind).map_err(serde::ser::Error::custom)?;

        map.serialize_entry("config", &config_value)?;
        map.serialize_entry("routing", &self.routing)?;
        map.serialize_entry("description", &self.description)?;
        map.serialize_entry("created_by", &self.created_by)?;
        map.serialize_entry("updated_by", &self.updated_by)?;
        map.serialize_entry("created_at", &self.created_at)?;
        map.serialize_entry("updated_at", &self.updated_at)?;
        map.end()
    }
}

#[derive(Deserialize)]
struct EndpointSchemaHelper {
    id: EndpointId,
    uuid: EndpointUuid,
    kind: EpKind,
    config: serde_json::Value,
    routing: Option<EndpointRouting>,
    description: Option<String>,
    created_by: UserUuid,
    updated_by: UserUuid,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl<'de> Deserialize<'de> for EndpointSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let helper = EndpointSchemaHelper::deserialize(deserializer)?;

        let config = json_config_to_box_ep_config(helper.kind, helper.config).map_err(serde::de::Error::custom)?;

        Ok(EndpointSchema {
            id: helper.id,
            uuid: helper.uuid,
            kind: helper.kind,
            config,
            routing: helper.routing,
            description: helper.description,
            created_by: helper.created_by,
            updated_by: helper.updated_by,
            created_at: helper.created_at,
            updated_at: helper.updated_at,
        })
    }
}

fn borsh_serialize_config<W: std::io::Write>(config: &dyn EpConfig, _writer: &mut W) -> std::io::Result<()> {
    match config.kind() {
        #[cfg(feature = "aws")]
        EpKind::Aws => borsh_serialize_config!(config, _writer, AwsConfig, "AwsConfig"),
        #[cfg(not(feature = "aws"))]
        EpKind::Aws => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Aws))),
        #[cfg(feature = "azure")]
        EpKind::Azure => borsh_serialize_config!(config, _writer, AzureConfig, "AzureConfig"),
        #[cfg(not(feature = "azure"))]
        EpKind::Azure => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Azure))),
        #[cfg(feature = "cassandra")]
        EpKind::Cassandra => borsh_serialize_config!(config, _writer, CassandraConfig, "CassandraConfig"),
        #[cfg(not(feature = "cassandra"))]
        EpKind::Cassandra => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Cassandra))),
        #[cfg(feature = "clickhouse")]
        EpKind::Clickhouse => borsh_serialize_config!(config, _writer, ClickhouseConfig, "ClickhouseConfig"),
        #[cfg(not(feature = "clickhouse"))]
        EpKind::Clickhouse => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Clickhouse))),
        #[cfg(feature = "databricks")]
        EpKind::Databricks => borsh_serialize_config!(config, _writer, DatabricksConfig, "DatabricksConfig"),
        #[cfg(not(feature = "databricks"))]
        EpKind::Databricks => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Databricks))),
        #[cfg(feature = "datadog")]
        EpKind::Datadog => borsh_serialize_config!(config, _writer, DatadogConfig, "DatadogConfig"),
        #[cfg(not(feature = "datadog"))]
        EpKind::Datadog => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Datadog))),
        #[cfg(feature = "elasticache")]
        EpKind::Elasticache => borsh_serialize_config!(config, _writer, RedisConfig, "RedisConfig"),
        #[cfg(not(feature = "elasticache"))]
        EpKind::Elasticache => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Elasticache))),
        #[cfg(feature = "eraser")]
        EpKind::Eraser => borsh_serialize_config!(config, _writer, EraserConfig, "EraserConfig"),
        #[cfg(not(feature = "eraser"))]
        EpKind::Eraser => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Eraser))),
        #[cfg(feature = "rds")]
        EpKind::Rds => borsh_serialize_config!(config, _writer, PostgresConfig, "PostgresConfig"),
        #[cfg(not(feature = "rds"))]
        EpKind::Rds => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Rds))),
        #[cfg(feature = "function")]
        EpKind::Function => borsh_serialize_config!(config, _writer, FunctionConfig, "FunctionConfig"),
        #[cfg(not(feature = "function"))]
        EpKind::Function => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Function))),
        #[cfg(feature = "gitlab")]
        EpKind::Gitlab => borsh_serialize_config!(config, _writer, GitlabConfig, "GitlabConfig"),
        #[cfg(not(feature = "gitlab"))]
        EpKind::Gitlab => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Gitlab))),
        #[cfg(feature = "gworkspace")]
        EpKind::GoogleWorkspace => borsh_serialize_config!(config, _writer, GoogleWorkspaceConfig, "GoogleWorkspaceConfig"),
        #[cfg(not(feature = "gworkspace"))]
        EpKind::GoogleWorkspace => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::GoogleWorkspace))),
        #[cfg(feature = "http")]
        EpKind::Http => borsh_serialize_config!(config, _writer, HttpConfig, "HttpConfig"),
        #[cfg(not(feature = "http"))]
        EpKind::Http => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Http))),
        #[cfg(feature = "llm")]
        EpKind::Llm => borsh_serialize_config!(config, _writer, LlmConfig, "LlmConfig"),
        #[cfg(not(feature = "llm"))]
        EpKind::Llm => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Llm))),
        #[cfg(feature = "mongo")]
        EpKind::Mongo => borsh_serialize_config!(config, _writer, MongoConfig, "MongoConfig"),
        #[cfg(not(feature = "mongo"))]
        EpKind::Mongo => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Mongo))),
        #[cfg(feature = "mssql")]
        EpKind::Mssql => borsh_serialize_config!(config, _writer, MssqlConfig, "MssqlConfig"),
        #[cfg(not(feature = "mssql"))]
        EpKind::Mssql => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Mssql))),
        #[cfg(feature = "mysql")]
        EpKind::Mysql => borsh_serialize_config!(config, _writer, MysqlConfig, "MysqlConfig"),
        #[cfg(not(feature = "mysql"))]
        EpKind::Mysql => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Mysql))),
        #[cfg(feature = "oracle")]
        EpKind::Oracle => borsh_serialize_config!(config, _writer, OracleConfig, "OracleConfig"),
        #[cfg(not(feature = "oracle"))]
        EpKind::Oracle => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Oracle))),
        #[cfg(feature = "pinecone")]
        EpKind::Pinecone => borsh_serialize_config!(config, _writer, PineconeConfig, "PineconeConfig"),
        #[cfg(not(feature = "pinecone"))]
        EpKind::Pinecone => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Pinecone))),
        #[cfg(feature = "posthog")]
        EpKind::Posthog => borsh_serialize_config!(config, _writer, PosthogConfig, "PosthogConfig"),
        #[cfg(not(feature = "posthog"))]
        EpKind::Posthog => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Posthog))),
        #[cfg(feature = "postgres")]
        EpKind::Postgres => borsh_serialize_config!(config, _writer, PostgresConfig, "PostgresConfig"),
        #[cfg(not(feature = "postgres"))]
        EpKind::Postgres => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Postgres))),
        #[cfg(feature = "redis")]
        EpKind::Redis => borsh_serialize_config!(config, _writer, RedisConfig, "RedisConfig"),
        #[cfg(not(feature = "redis"))]
        EpKind::Redis => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Redis))),
        #[cfg(feature = "s3")]
        EpKind::S3 => borsh_serialize_config!(config, _writer, S3Config, "S3Config"),
        #[cfg(not(feature = "s3"))]
        EpKind::S3 => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::S3))),
        #[cfg(feature = "salesforce")]
        EpKind::Salesforce => borsh_serialize_config!(config, _writer, SalesforceConfig, "SalesforceConfig"),
        #[cfg(not(feature = "salesforce"))]
        EpKind::Salesforce => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Salesforce))),
        #[cfg(feature = "snowflake")]
        EpKind::Snowflake => borsh_serialize_config!(config, _writer, SnowflakeConfig, "SnowflakeConfig"),
        #[cfg(not(feature = "snowflake"))]
        EpKind::Snowflake => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Snowflake))),
        #[cfg(feature = "tavily")]
        EpKind::Tavily => borsh_serialize_config!(config, _writer, TavilyConfig, "TavilyConfig"),
        #[cfg(not(feature = "tavily"))]
        EpKind::Tavily => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Tavily))),
        #[cfg(feature = "weaviate")]
        EpKind::Weaviate => borsh_serialize_config!(config, _writer, WeaviateConfig, "WeaviateConfig"),
        #[cfg(not(feature = "weaviate"))]
        EpKind::Weaviate => Err(std::io::Error::other(unsupported_endpoint_config(EpKind::Weaviate))),
    }
}

// fn borsh_pg_config_to_vec(config: &PgEpConfig) -> Result<Vec<u8>, std::io::Error> {
//     let mut result = Vec::with_capacity(1024);
//     borsh_serialize_config(config, &mut result)?;
//     Ok(result)
// }

fn borsh_config_to_vec(config: &dyn EpConfig) -> Result<Vec<u8>, std::io::Error> {
    let mut result = Vec::with_capacity(1024);
    borsh_serialize_config(config, &mut result)?;
    Ok(result)
}

impl BorshSerialize for EndpointSchema {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        BorshSerialize::serialize(&self.id, writer)?;
        BorshSerialize::serialize(&self.uuid, writer)?;
        BorshSerialize::serialize(&self.kind, writer)?;
        borsh_serialize_config(self.config.as_ref(), writer)?;
        // Routing is serialized as JSON bytes within the Borsh stream
        let routing_json: Option<Vec<u8>> = self
            .routing
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|e| std::io::Error::other(format!("Failed to serialize routing: {}", e)))?;
        BorshSerialize::serialize(&routing_json, writer)?;
        BorshSerialize::serialize(&self.description, writer)?;
        BorshSerialize::serialize(&self.created_by, writer)?;
        BorshSerialize::serialize(&self.updated_by, writer)?;
        BorshSerialize::serialize(&self.created_at, writer)?;
        BorshSerialize::serialize(&self.updated_at, writer)
    }
}

impl BorshDeserialize for EndpointSchema {
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let id = EndpointId::deserialize_reader(reader)?;
        let uuid = EndpointUuid::deserialize_reader(reader)?;
        let kind = EpKind::deserialize_reader(reader)?;

        let config = borsh_config_from_kind_and_reader(kind, reader)?;

        // Routing is deserialized from JSON bytes within the Borsh stream
        let routing_json = Option::<Vec<u8>>::deserialize_reader(reader)?;
        let routing = routing_json
            .map(|bytes| serde_json::from_slice::<EndpointRouting>(&bytes))
            .transpose()
            .map_err(|e| std::io::Error::other(format!("Failed to deserialize routing: {}", e)))?;

        let description = Option::<String>::deserialize_reader(reader)?;
        let created_by = UserUuid::deserialize_reader(reader)?;
        let updated_by = UserUuid::deserialize_reader(reader)?;
        let created_at = DateTimeWrapper::deserialize_reader(reader)?;
        let updated_at = DateTimeWrapper::deserialize_reader(reader)?;

        Ok(EndpointSchema {
            id,
            uuid,
            kind,
            config,
            routing,
            description,
            created_by,
            updated_by,
            created_at,
            updated_at,
        })
    }
}

impl PartialEq for EndpointSchema {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.uuid == other.uuid
            && self.kind == other.kind
            && borsh_config_to_vec(self.config.as_ref()).unwrap_or_else(|_| vec![])
                == borsh_config_to_vec(other.config.as_ref()).unwrap_or_else(|_| vec![])
            && self.routing == other.routing
            && self.description == other.description
            && self.created_by == other.created_by
            && self.updated_by == other.updated_by
            && self.created_at == other.created_at
            && self.updated_at == other.updated_at
    }
}

impl TryFrom<(EndpointSchemaInput, UserUuid)> for EndpointSchema {
    type Error = EpError;

    fn try_from((input, created_by): (EndpointSchemaInput, UserUuid)) -> ResultEP<Self> {
        let config = BoxEpConfig::new(input.config.clone_box());

        Ok(EndpointSchema {
            id: input.endpoint,
            uuid: EndpointUuid::new_uuid(),
            kind: input.kind,
            config,
            routing: None,
            description: input.description,
            updated_by: created_by.clone(),
            created_by,
            created_at: DateTimeWrapper::now(),
            updated_at: DateTimeWrapper::now(),
        })
    }
}

impl ToRedisArgs for EndpointSchema {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let serialized = borsh::to_vec(self).unwrap_or_default();
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for EndpointSchema {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            redis::Value::BulkString(bytes) => borsh::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize with Borsh", e.to_string()))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting EndpointSchema",
            ))),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
pub struct EndpointSchemaIds {
    id: EndpointId,
    uuid: EndpointUuid,
}

impl FromRow for EndpointSchemaIds {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
        })
    }
}

impl ToRedisArgs for EndpointSchemaIds {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        // Serialize the AuthSchema to JSON
        let serialized = serde_json::to_vec(self).unwrap_or_default();

        // Write the serialized bytes to the Redis output
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for EndpointSchemaIds {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            // redis::Value::Data
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting AuthSchema",
            ))),
        }
    }
}

// Newtype wrapper for PostgreSQL serialization
#[derive(Debug, Clone)]
pub struct BoxEpConfig(Box<dyn EpConfig>);

impl BoxEpConfig {
    pub fn new(config: Box<dyn EpConfig>) -> Self {
        Self(config)
    }
    pub fn into_inner(self) -> Box<dyn EpConfig> {
        self.0
    }

    // TODO: Consider implementing AsRef trait if lifetime constraints can be resolved
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &dyn EpConfig {
        &*self.0
    }

    fn from_borsh_bytes(bytes: &[u8]) -> Result<Self, std::io::Error> {
        let wrapper: EpConfigWrapper = borsh::from_slice(bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Failed to deserialize config: {e}")))?;
        let config = wrapper
            .into_config()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Failed to convert config: {e}")))?;

        Ok(Self::new(config))
    }
}

impl From<Box<dyn EpConfig>> for BoxEpConfig {
    fn from(config: Box<dyn EpConfig>) -> Self {
        Self::new(config)
    }
}

impl From<BoxEpConfig> for Box<dyn EpConfig> {
    fn from(config: BoxEpConfig) -> Self {
        config.into_inner()
    }
}

// Custom wrapper struct for internal serialization
#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct EpConfigWrapper {
    kind: EpKind,
    config_data: Vec<u8>,
}

impl EpConfigWrapper {
    pub fn new(config: &dyn EpConfig) -> Result<Self, std::io::Error> {
        let kind = config.kind();
        let config_data = borsh_config_to_vec(config)?;
        Ok(Self { kind, config_data })
    }

    pub fn into_config(self) -> Result<Box<dyn EpConfig>, std::io::Error> {
        borsh_config_from_kind_and_slice(self.kind, &self.config_data)
    }
}

// PostgreSQL ToSql/FromSql implementations for PgEpConfig
impl ToSql for BoxEpConfig {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match *ty {
            Type::BYTEA => {
                let wrapper = EpConfigWrapper::new(self.as_ref()).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?;

                let serialized = borsh::to_vec(&wrapper).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?;

                out.extend_from_slice(&serialized);
                Ok(IsNull::No)
            }
            Type::JSONB => {
                // For JSONB, we need to create a JSON object that includes both kind and config
                let config_value =
                    serialize_endpoint_config(self.as_ref(), self.0.kind()).map_err(Box::<dyn std::error::Error + Sync + Send>::from)?;
                let json_value = serde_json::json!({
                    "kind": self.0.kind(),
                    "config": config_value
                });

                json_value.to_sql(ty, out)
            }
            _ => Err("Unsupported PostgreSQL type for EpConfig".into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::BYTEA | Type::JSONB)
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for BoxEpConfig {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match *ty {
            Type::BYTEA => Self::from_borsh_bytes(raw).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>),
            Type::JSONB => {
                // Parse the JSON value
                let json_value: serde_json::Value =
                    serde_json::from_slice(raw).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?;

                // Extract kind and config from the JSON
                let kind: EpKind = serde_json::from_value(json_value.get("kind").ok_or("Missing 'kind' field in JSONB")?.clone())
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?;

                let config_value = json_value.get("config").ok_or("Missing 'config' field in JSONB")?;

                let config =
                    json_config_to_box_ep_config(kind, config_value.clone()).map_err(Box::<dyn std::error::Error + Sync + Send>::from)?;
                Ok(config)
            }
            _ => Err("Unsupported PostgreSQL type for EpConfig".into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::BYTEA | Type::JSONB)
    }
}

impl EndpointSchema {
    pub fn new(
        id: EndpointId,
        kind: EpKind,
        config: Box<dyn EpConfig>,
        routing: Option<EndpointRouting>,
        description: Option<String>,
        created_by: UserUuid,
    ) -> Self {
        let now = DateTimeWrapper::now();
        Self {
            id,
            uuid: EndpointUuid::new_uuid(),
            kind,
            config: BoxEpConfig::new(config),
            routing,
            description,
            updated_by: created_by.clone(),
            created_by,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    /// Returns the routing config for this endpoint.
    /// If no routing is explicitly set, returns `Direct` pointing to this endpoint's uuid.
    pub fn routing(&self) -> EndpointRouting {
        self.routing.clone().unwrap_or_else(|| EndpointRouting::direct(self.uuid.clone()))
    }

    /// Returns the raw routing option (for database storage).
    pub fn routing_raw(&self) -> &Option<EndpointRouting> {
        &self.routing
    }

    pub fn set_routing(&mut self, routing: Option<EndpointRouting>) {
        self.routing = routing;
        self.update_timestamp();
    }

    pub fn created_by(&self) -> &UserUuid {
        &self.created_by
    }
    pub fn updated_by(&self) -> &UserUuid {
        &self.updated_by
    }
    pub fn set_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }

    pub fn config(&self) -> Box<dyn EpConfig> {
        self.config.0.clone()
    }

    pub fn update_config(&mut self, config: Box<dyn EpConfig>) {
        self.config = BoxEpConfig::new(config);
        self.update_timestamp();
    }

    pub fn endpoint_uuid(&self) -> EndpointUuid {
        self.uuid.clone()
    }

    pub fn kind(&self) -> EpKind {
        self.kind
    }

    pub fn cache_key(&self, org_cache_key: OrganizationCacheUuid) -> EndpointCacheUuid {
        EndpointCacheUuid::new(Some(org_cache_key), self.uuid.to_owned())
    }

    pub fn cache_pointer(&self, org_cache_pointer: OrganizationCacheUuid) -> EndpointCacheId {
        EndpointCacheId::new(Some(org_cache_pointer), self.id.to_owned())
    }
}

impl Table for EndpointSchema {
    type I = EndpointId;
    type U = EndpointUuid;

    fn id(&self) -> EndpointId {
        self.id.clone()
    }

    fn update_id(&mut self, id: String) -> Option<String> {
        let out = self.id.update(id);
        self.update_timestamp();
        Some(out)
    }

    fn uuid(&self) -> EndpointUuid {
        self.uuid.clone()
    }

    fn description(&self) -> Option<String> {
        self.description.clone()
    }

    fn update_description(&mut self, description: String) -> Option<String> {
        let out = self.description.replace(description);
        self.update_timestamp();
        out
    }

    fn created_at(&self) -> DateTime<Utc> {
        self.created_at.as_datetime()
    }

    fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at.as_datetime()
    }

    fn update_timestamp(&mut self) {
        self.updated_at = DateTimeWrapper::now();
    }

    fn update_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FromRow for EndpointSchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        let routing_json: Option<Value> = row.try_get("routing").map_err(EpError::database)?;
        let routing = routing_json.map(serde_json::from_value::<EndpointRouting>).transpose().map_err(EpError::serde)?;
        let config_bytes: Vec<u8> = row.try_get("config").map_err(EpError::database)?;
        let config = BoxEpConfig::from_borsh_bytes(&config_bytes).map_err(EpError::database)?;

        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            kind: row.try_get("kind").map_err(EpError::database)?,
            config,
            routing,
            description: row.try_get("description").map_err(EpError::database)?,
            created_by: row.try_get("created_by").map_err(EpError::database)?,
            updated_by: row.try_get("updated_by").map_err(EpError::database)?,
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateEndpointSchema {
    pub id: Option<EndpointId>,
    pub config: Option<Value>,
    pub description: Option<String>,
}

impl UpdateEndpointSchema {
    pub fn new(
        id: Option<EndpointId>,
        config: Option<Value>, //Option<Box<dyn EpConfig>>,
        description: Option<String>,
    ) -> Self {
        Self { id, config, description }
    }

    pub fn id(&self) -> Option<&EndpointId> {
        self.id.as_ref()
    }

    pub fn config(&self) -> Option<&Value> {
        //Option<&Box<dyn EpConfig>> {
        self.config.as_ref()
    }

    pub fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }
}

impl PartialSchema for UpdateEndpointSchema {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("id", EndpointId::schema())
                .property("config", <Box<dyn EpConfig>>::schema())
                .property("description", String::schema())
                .build(),
        ))
    }
}

impl ToSchema for UpdateEndpointSchema {}

#[cfg(all(test, feature = "gitlab"))]
mod tests {
    use super::*;
    use crate::gitlab_core::config::GitlabConfig;
    use crate::gitlab_core::connection::GitlabConnection;

    #[test]
    fn gitlab_endpoint_schema_round_trips_json_and_borsh() {
        let endpoint = EndpointSchema::new(
            EndpointId::from("gitlab-read-only".to_string()),
            EpKind::Gitlab,
            Box::new(GitlabConfig {
                read_conn: Some(GitlabConnection {
                    token: "glpat-test-token".to_string(),
                    base_url: Some("https://gitlab.example.com".to_string()),
                }),
                write_conn: None,
                admin_conn: None,
                system_conn: None,
            }),
            None,
            Some("GitLab connection".to_string()),
            UserUuid::new_uuid(),
        );

        let json = serde_json::to_string(&endpoint).expect("serialize gitlab endpoint to json");
        let from_json: EndpointSchema = serde_json::from_str(&json).expect("deserialize gitlab endpoint from json");
        assert_eq!(from_json.kind(), EpKind::Gitlab);

        let json_config = from_json.config();
        let json_gitlab = json_config.as_any().downcast_ref::<GitlabConfig>().expect("gitlab config after json round-trip");
        assert_eq!(
            json_gitlab.read_conn.as_ref().and_then(|conn| conn.base_url.as_deref()),
            Some("https://gitlab.example.com")
        );

        let borsh = borsh::to_vec(&endpoint).expect("serialize gitlab endpoint to borsh");
        let from_borsh: EndpointSchema = borsh::from_slice(&borsh).expect("deserialize gitlab endpoint from borsh");
        assert_eq!(from_borsh.kind(), EpKind::Gitlab);

        let borsh_config = from_borsh.config();
        let borsh_gitlab = borsh_config.as_any().downcast_ref::<GitlabConfig>().expect("gitlab config after borsh round-trip");
        assert_eq!(borsh_gitlab.read_conn.as_ref().map(|conn| conn.token.as_str()), Some("glpat-test-token"));
    }
}
