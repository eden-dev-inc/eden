use std::fmt;

#[cfg(feature = "aws")]
use aws_core::config::AwsConfig;
#[cfg(feature = "azure")]
use azure_core::config::AzureConfig;
#[cfg(feature = "cassandra")]
use cassandra_core::config::CassandraConfig;
#[cfg(feature = "clickhouse")]
use clickhouse_core::config::ClickhouseConfig;
#[cfg(feature = "databricks")]
use databricks_core::config::DatabricksConfig;
#[cfg(feature = "datadog")]
use datadog_core::config::DatadogConfig;
#[cfg(any(feature = "redis", feature = "elasticache"))]
use eden_logger_internal::{ctx_with_trace, log_debug};
use ep_core::database::schema::routing::EndpointRoutingInput;
use ep_core::ep::EpConfig;
#[cfg(feature = "eraser")]
use eraser_core::config::EraserConfig;
use error::{EpError, ResultEP};
use format::EndpointId;
use format::endpoint::EpKind;
#[cfg(feature = "function")]
use function_core::config::FunctionConfig;
use function_name::named;
#[cfg(feature = "gitlab")]
use gitlab_core::config::GitlabConfig;
#[cfg(feature = "gworkspace")]
use gworkspace_core::config::GoogleWorkspaceConfig;
#[cfg(feature = "http")]
use http_core::config::HttpConfig;

#[cfg(feature = "llm")]
use llm_core::config::LlmConfig;
#[cfg(feature = "mongo")]
use mongo_core::config::MongoConfig;
#[cfg(feature = "mssql")]
use mssql_core::config::MssqlConfig;
#[cfg(feature = "mysql")]
use mysql_core::config::MysqlConfig;
#[cfg(feature = "oracle")]
use oracle_core::config::OracleConfig;
#[cfg(feature = "pinecone")]
use pinecone_core::config::PineconeConfig;
#[cfg(any(feature = "postgres", feature = "rds"))]
use postgres_core::config::PostgresConfig;
#[cfg(any(feature = "redis", feature = "elasticache"))]
use redis_core::config::RedisConfig;

#[cfg(feature = "s3")]
use s3_core::config::S3Config;
#[cfg(feature = "salesforce")]
use salesforce_core::config::SalesforceConfig;
use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::json;
#[cfg(feature = "snowflake")]
use snowflake_core::config::SnowflakeConfig;
#[cfg(feature = "tavily")]
use tavily_core::config::TavilyConfig;
use utoipa::openapi::ObjectBuilder;
use utoipa::{
    PartialSchema, ToSchema,
    openapi::{RefOr, Schema},
};
#[cfg(feature = "weaviate")]
use weaviate_core::config::WeaviateConfig;
#[cfg(feature = "posthog")]
use posthog_core::config::PosthogConfig;

#[derive(Debug, Clone)]
pub struct EndpointSchemaInput {
    pub endpoint: EndpointId,
    pub kind: EpKind,
    pub config: Box<dyn EpConfig>,
    pub routing: Option<EndpointRoutingInput>,
    pub description: Option<String>,
}

#[allow(unused_macros)]
macro_rules! deserialize_borsh_config {
    ($config:expr, $config_type:ty) => {{
        let config: $config_type = borsh::from_slice($config).map_err(EpError::serde)?;
        config.as_config()
    }};
}

#[allow(unused_macros)]
macro_rules! unsupported_kind {
    ($kind:expr) => {
        return Err(EpError::database(format!("{:?} not supported in this build", $kind)))
    };
}

#[allow(unreachable_code, unused_variables)]
pub fn deserialize_config(_config: &[u8], kind: EpKind) -> ResultEP<Box<dyn EpConfig>> {
    let config = match kind {
        #[cfg(feature = "aws")]
        EpKind::Aws => deserialize_borsh_config!(_config, AwsConfig),
        #[cfg(not(feature = "aws"))]
        EpKind::Aws => unsupported_kind!(EpKind::Aws),
        #[cfg(feature = "azure")]
        EpKind::Azure => deserialize_borsh_config!(_config, AzureConfig),
        #[cfg(not(feature = "azure"))]
        EpKind::Azure => unsupported_kind!(EpKind::Azure),
        #[cfg(feature = "cassandra")]
        EpKind::Cassandra => deserialize_borsh_config!(_config, CassandraConfig),
        #[cfg(not(feature = "cassandra"))]
        EpKind::Cassandra => unsupported_kind!(EpKind::Cassandra),
        #[cfg(feature = "clickhouse")]
        EpKind::Clickhouse => deserialize_borsh_config!(_config, ClickhouseConfig),
        #[cfg(not(feature = "clickhouse"))]
        EpKind::Clickhouse => unsupported_kind!(EpKind::Clickhouse),
        #[cfg(feature = "databricks")]
        EpKind::Databricks => deserialize_borsh_config!(_config, DatabricksConfig),
        #[cfg(not(feature = "databricks"))]
        EpKind::Databricks => unsupported_kind!(EpKind::Databricks),
        #[cfg(feature = "datadog")]
        EpKind::Datadog => deserialize_borsh_config!(_config, DatadogConfig),
        #[cfg(not(feature = "datadog"))]
        EpKind::Datadog => unsupported_kind!(EpKind::Datadog),
        #[cfg(feature = "elasticache")]
        EpKind::Elasticache => deserialize_borsh_config!(_config, RedisConfig),
        #[cfg(not(feature = "elasticache"))]
        EpKind::Elasticache => unsupported_kind!(EpKind::Elasticache),
        #[cfg(feature = "eraser")]
        EpKind::Eraser => deserialize_borsh_config!(_config, EraserConfig),
        #[cfg(not(feature = "eraser"))]
        EpKind::Eraser => unsupported_kind!(EpKind::Eraser),
        #[cfg(feature = "function")]
        EpKind::Function => deserialize_borsh_config!(_config, FunctionConfig),
        #[cfg(not(feature = "function"))]
        EpKind::Function => unsupported_kind!(EpKind::Function),
        #[cfg(feature = "gitlab")]
        EpKind::Gitlab => deserialize_borsh_config!(_config, GitlabConfig),
        #[cfg(not(feature = "gitlab"))]
        EpKind::Gitlab => unsupported_kind!(EpKind::Gitlab),
        #[cfg(feature = "gworkspace")]
        EpKind::GoogleWorkspace => deserialize_borsh_config!(_config, GoogleWorkspaceConfig),
        #[cfg(not(feature = "gworkspace"))]
        EpKind::GoogleWorkspace => unsupported_kind!(EpKind::GoogleWorkspace),
        #[cfg(feature = "http")]
        EpKind::Http => deserialize_borsh_config!(_config, HttpConfig),
        #[cfg(not(feature = "http"))]
        EpKind::Http => unsupported_kind!(EpKind::Http),
        #[cfg(feature = "llm")]
        EpKind::Llm => deserialize_borsh_config!(_config, LlmConfig),
        #[cfg(not(feature = "llm"))]
        EpKind::Llm => unsupported_kind!(EpKind::Llm),
        #[cfg(feature = "mongo")]
        EpKind::Mongo => deserialize_borsh_config!(_config, MongoConfig),
        #[cfg(not(feature = "mongo"))]
        EpKind::Mongo => unsupported_kind!(EpKind::Mongo),
        #[cfg(feature = "mssql")]
        EpKind::Mssql => deserialize_borsh_config!(_config, MssqlConfig),
        #[cfg(not(feature = "mssql"))]
        EpKind::Mssql => unsupported_kind!(EpKind::Mssql),
        #[cfg(feature = "mysql")]
        EpKind::Mysql => deserialize_borsh_config!(_config, MysqlConfig),
        #[cfg(not(feature = "mysql"))]
        EpKind::Mysql => unsupported_kind!(EpKind::Mysql),
        #[cfg(feature = "oracle")]
        EpKind::Oracle => deserialize_borsh_config!(_config, OracleConfig),
        #[cfg(not(feature = "oracle"))]
        EpKind::Oracle => unsupported_kind!(EpKind::Oracle),
        #[cfg(feature = "pinecone")]
        EpKind::Pinecone => deserialize_borsh_config!(_config, PineconeConfig),
        #[cfg(not(feature = "pinecone"))]
        EpKind::Pinecone => unsupported_kind!(EpKind::Pinecone),
        #[cfg(feature = "posthog")]
        EpKind::Posthog => deserialize_borsh_config!(_config, PosthogConfig),
        #[cfg(not(feature = "posthog"))]
        EpKind::Posthog => unsupported_kind!(EpKind::Posthog),
        #[cfg(feature = "postgres")]
        EpKind::Postgres => deserialize_borsh_config!(_config, PostgresConfig),
        #[cfg(not(feature = "postgres"))]
        EpKind::Postgres => unsupported_kind!(EpKind::Postgres),
        #[cfg(feature = "rds")]
        EpKind::Rds => deserialize_borsh_config!(_config, PostgresConfig),
        #[cfg(not(feature = "rds"))]
        EpKind::Rds => unsupported_kind!(EpKind::Rds),
        #[cfg(feature = "redis")]
        EpKind::Redis => deserialize_borsh_config!(_config, RedisConfig),
        #[cfg(not(feature = "redis"))]
        EpKind::Redis => unsupported_kind!(EpKind::Redis),
        #[cfg(feature = "s3")]
        EpKind::S3 => deserialize_borsh_config!(_config, S3Config),
        #[cfg(not(feature = "s3"))]
        EpKind::S3 => unsupported_kind!(EpKind::S3),
        #[cfg(feature = "salesforce")]
        EpKind::Salesforce => deserialize_borsh_config!(_config, SalesforceConfig),
        #[cfg(not(feature = "salesforce"))]
        EpKind::Salesforce => unsupported_kind!(EpKind::Salesforce),
        #[cfg(feature = "snowflake")]
        EpKind::Snowflake => deserialize_borsh_config!(_config, SnowflakeConfig),
        #[cfg(not(feature = "snowflake"))]
        EpKind::Snowflake => unsupported_kind!(EpKind::Snowflake),
        #[cfg(feature = "tavily")]
        EpKind::Tavily => deserialize_borsh_config!(_config, TavilyConfig),
        #[cfg(not(feature = "tavily"))]
        EpKind::Tavily => unsupported_kind!(EpKind::Tavily),
        #[cfg(feature = "weaviate")]
        EpKind::Weaviate => deserialize_borsh_config!(_config, WeaviateConfig),
        #[cfg(not(feature = "weaviate"))]
        EpKind::Weaviate => unsupported_kind!(EpKind::Weaviate),
    };
    Ok(config)
}

impl ToSchema for EndpointSchemaInput {}

impl PartialSchema for EndpointSchemaInput {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("endpoint", EndpointId::schema())
                .property("kind", EpKind::schema())
                .property("config", Box::<dyn EpConfig>::schema())
                .property("routing", EndpointRoutingInput::schema())
                .property("description", String::schema())
                .required("endpoint")
                .required("kind")
                .required("config")
                .examples([json!(
                            {
                                "endpoint": "mongo_test_local",
                                "kind": "Mongo",
                                "config": {
                                    "auth": "None",
                                    "read_conn": null,
                                    "write_conn": {
                                        "url": "mongodb://localhost:27017",
                                        "auth": "None"
                                    },
                                    "content": "JSON",
                                    "accept": "JSON",
                                    "api_key": ""
                                },
                                "description": "test description"
                            }
                )])
                .build(),
        ))
    }
}

#[allow(unused_macros)]
macro_rules! serialize_config {
    ($self:expr, $config_type:ty, $type_name:expr) => {{
        let config = $self
            .config
            .as_any()
            .downcast_ref::<$config_type>()
            .ok_or_else(|| serde::ser::Error::custom(format!("Failed to downcast {}", $type_name)))?;
        serde_json::to_value(config).map_err(|e| serde::ser::Error::custom(format!("Failed to serialize {}: {}", $type_name, e)))?
    }};
}

#[allow(unused_macros)]
macro_rules! unsupported_serialize_kind {
    ($kind:expr) => {
        return Err(serde::ser::Error::custom(format!("{} not supported in this build", $kind)))
    };
}

#[allow(unused_macros)]
macro_rules! unsupported_deserialize_kind {
    ($kind:expr) => {
        return Err(de::Error::custom(format!("{} not supported in this build", $kind)))
    };
}

impl Serialize for EndpointSchemaInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[allow(unreachable_code, unused_variables)]
        {
        let mut map = serializer.serialize_map(Some(7))?;
        map.serialize_entry("endpoint", &self.endpoint)?;
        map.serialize_entry("kind", &self.kind)?;

        let config_value: serde_json::Value = match self.kind {
            #[cfg(feature = "aws")]
            EpKind::Aws => serialize_config!(self, AwsConfig, "AwsConfig"),
            #[cfg(not(feature = "aws"))]
            EpKind::Aws => unsupported_serialize_kind!(EpKind::Aws),
            #[cfg(feature = "azure")]
            EpKind::Azure => serialize_config!(self, AzureConfig, "AzureConfig"),
            #[cfg(not(feature = "azure"))]
            EpKind::Azure => unsupported_serialize_kind!(EpKind::Azure),
            #[cfg(feature = "cassandra")]
            EpKind::Cassandra => serialize_config!(self, CassandraConfig, "CassandraConfig"),
            #[cfg(not(feature = "cassandra"))]
            EpKind::Cassandra => unsupported_serialize_kind!(EpKind::Cassandra),
            #[cfg(feature = "clickhouse")]
            EpKind::Clickhouse => serialize_config!(self, ClickhouseConfig, "ClickhouseConfig"),
            #[cfg(not(feature = "clickhouse"))]
            EpKind::Clickhouse => unsupported_serialize_kind!(EpKind::Clickhouse),
            #[cfg(feature = "databricks")]
            EpKind::Databricks => serialize_config!(self, DatabricksConfig, "DatabricksConfig"),
            #[cfg(not(feature = "databricks"))]
            EpKind::Databricks => unsupported_serialize_kind!(EpKind::Databricks),
            #[cfg(feature = "datadog")]
            EpKind::Datadog => serialize_config!(self, DatadogConfig, "DatadogConfig"),
            #[cfg(not(feature = "datadog"))]
            EpKind::Datadog => unsupported_serialize_kind!(EpKind::Datadog),
            #[cfg(feature = "elasticache")]
            EpKind::Elasticache => serialize_config!(self, RedisConfig, "RedisConfig"),
            #[cfg(not(feature = "elasticache"))]
            EpKind::Elasticache => unsupported_serialize_kind!(EpKind::Elasticache),
            #[cfg(feature = "eraser")]
            EpKind::Eraser => serialize_config!(self, EraserConfig, "EraserConfig"),
            #[cfg(not(feature = "eraser"))]
            EpKind::Eraser => unsupported_serialize_kind!(EpKind::Eraser),
            #[cfg(feature = "rds")]
            EpKind::Rds => serialize_config!(self, PostgresConfig, "PostgresConfig"),
            #[cfg(not(feature = "rds"))]
            EpKind::Rds => unsupported_serialize_kind!(EpKind::Rds),
            #[cfg(feature = "function")]
            EpKind::Function => serialize_config!(self, FunctionConfig, "FunctionConfig"),
            #[cfg(not(feature = "function"))]
            EpKind::Function => unsupported_serialize_kind!(EpKind::Function),
            #[cfg(feature = "gitlab")]
            EpKind::Gitlab => serialize_config!(self, GitlabConfig, "GitlabConfig"),
            #[cfg(not(feature = "gitlab"))]
            EpKind::Gitlab => unsupported_serialize_kind!(EpKind::Gitlab),
            #[cfg(feature = "gworkspace")]
            EpKind::GoogleWorkspace => serialize_config!(self, GoogleWorkspaceConfig, "GoogleWorkspaceConfig"),
            #[cfg(not(feature = "gworkspace"))]
            EpKind::GoogleWorkspace => unsupported_serialize_kind!(EpKind::GoogleWorkspace),
            #[cfg(feature = "http")]
            EpKind::Http => serialize_config!(self, HttpConfig, "HttpConfig"),
            #[cfg(not(feature = "http"))]
            EpKind::Http => unsupported_serialize_kind!(EpKind::Http),
            #[cfg(feature = "llm")]
            EpKind::Llm => serialize_config!(self, LlmConfig, "LlmConfig"),
            #[cfg(not(feature = "llm"))]
            EpKind::Llm => unsupported_serialize_kind!(EpKind::Llm),
            #[cfg(feature = "mongo")]
            EpKind::Mongo => serialize_config!(self, MongoConfig, "MongoConfig"),
            #[cfg(not(feature = "mongo"))]
            EpKind::Mongo => unsupported_serialize_kind!(EpKind::Mongo),
            #[cfg(feature = "mssql")]
            EpKind::Mssql => serialize_config!(self, MssqlConfig, "MssqlConfig"),
            #[cfg(not(feature = "mssql"))]
            EpKind::Mssql => unsupported_serialize_kind!(EpKind::Mssql),
            #[cfg(feature = "mysql")]
            EpKind::Mysql => serialize_config!(self, MysqlConfig, "MysqlConfig"),
            #[cfg(not(feature = "mysql"))]
            EpKind::Mysql => unsupported_serialize_kind!(EpKind::Mysql),
            #[cfg(feature = "oracle")]
            EpKind::Oracle => serialize_config!(self, OracleConfig, "OracleConfig"),
            #[cfg(not(feature = "oracle"))]
            EpKind::Oracle => unsupported_serialize_kind!(EpKind::Oracle),
            #[cfg(feature = "pinecone")]
            EpKind::Pinecone => serialize_config!(self, PineconeConfig, "PineconeConfig"),
            #[cfg(not(feature = "pinecone"))]
            EpKind::Pinecone => unsupported_serialize_kind!(EpKind::Pinecone),
            #[cfg(feature = "posthog")]
            EpKind::Posthog => serialize_config!(self, PosthogConfig, "PosthogConfig"),
            #[cfg(not(feature = "posthog"))]
            EpKind::Posthog => unsupported_serialize_kind!(EpKind::Posthog),
            #[cfg(feature = "postgres")]
            EpKind::Postgres => serialize_config!(self, PostgresConfig, "PostgresConfig"),
            #[cfg(not(feature = "postgres"))]
            EpKind::Postgres => unsupported_serialize_kind!(EpKind::Postgres),
            #[cfg(feature = "redis")]
            EpKind::Redis => serialize_config!(self, RedisConfig, "RedisConfig"),
            #[cfg(not(feature = "redis"))]
            EpKind::Redis => unsupported_serialize_kind!(EpKind::Redis),
            #[cfg(feature = "s3")]
            EpKind::S3 => serialize_config!(self, S3Config, "S3Config"),
            #[cfg(not(feature = "s3"))]
            EpKind::S3 => unsupported_serialize_kind!(EpKind::S3),
            #[cfg(feature = "salesforce")]
            EpKind::Salesforce => serialize_config!(self, SalesforceConfig, "SalesforceConfig"),
            #[cfg(not(feature = "salesforce"))]
            EpKind::Salesforce => unsupported_serialize_kind!(EpKind::Salesforce),
            #[cfg(feature = "snowflake")]
            EpKind::Snowflake => serialize_config!(self, SnowflakeConfig, "SnowflakeConfig"),
            #[cfg(not(feature = "snowflake"))]
            EpKind::Snowflake => unsupported_serialize_kind!(EpKind::Snowflake),
            #[cfg(feature = "tavily")]
            EpKind::Tavily => serialize_config!(self, TavilyConfig, "TavilyConfig"),
            #[cfg(not(feature = "tavily"))]
            EpKind::Tavily => unsupported_serialize_kind!(EpKind::Tavily),
            #[cfg(feature = "weaviate")]
            EpKind::Weaviate => serialize_config!(self, WeaviateConfig, "WeaviateConfig"),
            #[cfg(not(feature = "weaviate"))]
            EpKind::Weaviate => unsupported_serialize_kind!(EpKind::Weaviate),
        };

        map.serialize_entry("config", &config_value)?;
        map.serialize_entry("routing", &self.routing)?;
        map.serialize_entry("description", &self.description)?;
        map.end()
        }
    }
}

#[allow(unused_macros)]
macro_rules! deserialize_json_config {
    ($raw_config:expr, $config_type:ty) => {
        serde_json::from_value::<$config_type>($raw_config).map_err(|e| de::Error::custom(e.to_string()))?.as_config()
    };
}

impl<'de> Deserialize<'de> for EndpointSchemaInput {
    // TODO: revisit when telemetry is added to this function
    #[allow(unused_macros)]
    #[named]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Endpoint,
            Kind,
            Config,
            Routing,
            Description,
        }

        struct SchemaVisitor;

        impl<'de> Visitor<'de> for SchemaVisitor {
            type Value = EndpointSchemaInput;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct EndpointSchemaInput")
            }

            // TODO: revisit when telemetry is added to this function
            #[allow(unused_macros)]
            #[named]
            #[allow(unreachable_code, unused_variables)]
            fn visit_map<V>(self, mut map: V) -> Result<EndpointSchemaInput, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut kind = None;
                let mut endpoint = None;
                let mut description = None;
                let mut raw_config: Option<serde_json::Value> = None;
                let mut routing = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Endpoint => {
                            if endpoint.is_some() {
                                return Err(de::Error::duplicate_field("endpoint"));
                            }
                            endpoint = Some(map.next_value()?);
                        }
                        Field::Kind => {
                            if kind.is_some() {
                                return Err(de::Error::duplicate_field("kind"));
                            }
                            kind = Some(map.next_value()?);
                        }
                        Field::Description => {
                            if description.is_some() {
                                return Err(de::Error::duplicate_field("description"));
                            }
                            description = Some(map.next_value()?);
                        }
                        Field::Config => {
                            if raw_config.is_some() {
                                return Err(de::Error::duplicate_field("config"));
                            }
                            raw_config = Some(map.next_value()?);
                        }
                        Field::Routing => {
                            if routing.is_some() {
                                return Err(de::Error::duplicate_field("routing"));
                            }
                            routing = map.next_value()?;
                        }
                    }
                }
                let endpoint = endpoint.ok_or_else(|| de::Error::missing_field("endpoint"))?;
                let kind = kind.ok_or_else(|| de::Error::missing_field("kind"))?;
                let raw_config = raw_config.ok_or_else(|| de::Error::missing_field("config"))?;
                let config = match kind {
                    #[cfg(feature = "aws")]
                    EpKind::Aws => deserialize_json_config!(raw_config, AwsConfig),
                    #[cfg(not(feature = "aws"))]
                    EpKind::Aws => unsupported_deserialize_kind!(EpKind::Aws),
                    #[cfg(feature = "azure")]
                    EpKind::Azure => deserialize_json_config!(raw_config, AzureConfig),
                    #[cfg(not(feature = "azure"))]
                    EpKind::Azure => unsupported_deserialize_kind!(EpKind::Azure),
                    #[cfg(feature = "cassandra")]
                    EpKind::Cassandra => deserialize_json_config!(raw_config, CassandraConfig),
                    #[cfg(not(feature = "cassandra"))]
                    EpKind::Cassandra => unsupported_deserialize_kind!(EpKind::Cassandra),
                    #[cfg(feature = "clickhouse")]
                    EpKind::Clickhouse => deserialize_json_config!(raw_config, ClickhouseConfig),
                    #[cfg(not(feature = "clickhouse"))]
                    EpKind::Clickhouse => unsupported_deserialize_kind!(EpKind::Clickhouse),
                    #[cfg(feature = "databricks")]
                    EpKind::Databricks => deserialize_json_config!(raw_config, DatabricksConfig),
                    #[cfg(not(feature = "databricks"))]
                    EpKind::Databricks => unsupported_deserialize_kind!(EpKind::Databricks),
                    #[cfg(feature = "datadog")]
                    EpKind::Datadog => deserialize_json_config!(raw_config, DatadogConfig),
                    #[cfg(not(feature = "datadog"))]
                    EpKind::Datadog => unsupported_deserialize_kind!(EpKind::Datadog),
                    #[cfg(feature = "elasticache")]
                    EpKind::Elasticache => {
                        let _ctx = ctx_with_trace!().with_feature("endpoint-core");
                        log_debug!(
                            _ctx,
                            "Raw config for Elasticache",
                            audience = eden_logger_internal::LogAudience::Internal,
                            raw_config = format!("{:?}", raw_config)
                        );
                        deserialize_json_config!(raw_config, RedisConfig)
                    }
                    #[cfg(not(feature = "elasticache"))]
                    EpKind::Elasticache => unsupported_deserialize_kind!(EpKind::Elasticache),
                    #[cfg(feature = "eraser")]
                    EpKind::Eraser => deserialize_json_config!(raw_config, EraserConfig),
                    #[cfg(not(feature = "eraser"))]
                    EpKind::Eraser => unsupported_deserialize_kind!(EpKind::Eraser),
                    #[cfg(feature = "rds")]
                    EpKind::Rds => deserialize_json_config!(raw_config, PostgresConfig),
                    #[cfg(not(feature = "rds"))]
                    EpKind::Rds => unsupported_deserialize_kind!(EpKind::Rds),
                    #[cfg(feature = "function")]
                    EpKind::Function => deserialize_json_config!(raw_config, FunctionConfig),
                    #[cfg(not(feature = "function"))]
                    EpKind::Function => unsupported_deserialize_kind!(EpKind::Function),
                    #[cfg(feature = "gitlab")]
                    EpKind::Gitlab => deserialize_json_config!(raw_config, GitlabConfig),
                    #[cfg(not(feature = "gitlab"))]
                    EpKind::Gitlab => unsupported_deserialize_kind!(EpKind::Gitlab),
                    #[cfg(feature = "gworkspace")]
                    EpKind::GoogleWorkspace => deserialize_json_config!(raw_config, GoogleWorkspaceConfig),
                    #[cfg(not(feature = "gworkspace"))]
                    EpKind::GoogleWorkspace => unsupported_deserialize_kind!(EpKind::GoogleWorkspace),
                    #[cfg(feature = "http")]
                    EpKind::Http => deserialize_json_config!(raw_config, HttpConfig),
                    #[cfg(not(feature = "http"))]
                    EpKind::Http => unsupported_deserialize_kind!(EpKind::Http),
                    #[cfg(feature = "llm")]
                    EpKind::Llm => deserialize_json_config!(raw_config, LlmConfig),
                    #[cfg(not(feature = "llm"))]
                    EpKind::Llm => unsupported_deserialize_kind!(EpKind::Llm),
                    #[cfg(feature = "mongo")]
                    EpKind::Mongo => deserialize_json_config!(raw_config, MongoConfig),
                    #[cfg(not(feature = "mongo"))]
                    EpKind::Mongo => unsupported_deserialize_kind!(EpKind::Mongo),
                    #[cfg(feature = "mssql")]
                    EpKind::Mssql => deserialize_json_config!(raw_config, MssqlConfig),
                    #[cfg(not(feature = "mssql"))]
                    EpKind::Mssql => unsupported_deserialize_kind!(EpKind::Mssql),
                    #[cfg(feature = "mysql")]
                    EpKind::Mysql => deserialize_json_config!(raw_config, MysqlConfig),
                    #[cfg(not(feature = "mysql"))]
                    EpKind::Mysql => unsupported_deserialize_kind!(EpKind::Mysql),
                    #[cfg(feature = "oracle")]
                    EpKind::Oracle => deserialize_json_config!(raw_config, OracleConfig),
                    #[cfg(not(feature = "oracle"))]
                    EpKind::Oracle => unsupported_deserialize_kind!(EpKind::Oracle),
                    #[cfg(feature = "pinecone")]
                    EpKind::Pinecone => deserialize_json_config!(raw_config, PineconeConfig),
                    #[cfg(not(feature = "pinecone"))]
                    EpKind::Pinecone => unsupported_deserialize_kind!(EpKind::Pinecone),
                    #[cfg(feature = "posthog")]
                    EpKind::Posthog => deserialize_json_config!(raw_config, PosthogConfig),
                    #[cfg(not(feature = "posthog"))]
                    EpKind::Posthog => unsupported_deserialize_kind!(EpKind::Posthog),
                    #[cfg(feature = "postgres")]
                    EpKind::Postgres => deserialize_json_config!(raw_config, PostgresConfig),
                    #[cfg(not(feature = "postgres"))]
                    EpKind::Postgres => unsupported_deserialize_kind!(EpKind::Postgres),
                    #[cfg(feature = "redis")]
                    EpKind::Redis => {
                        let _ctx = ctx_with_trace!().with_feature("endpoint-core");
                        log_debug!(
                            _ctx,
                            "Raw config for Redis",
                            audience = eden_logger_internal::LogAudience::Internal,
                            raw_config = format!("{:?}", raw_config)
                        );
                        deserialize_json_config!(raw_config, RedisConfig)
                    }
                    #[cfg(not(feature = "redis"))]
                    EpKind::Redis => unsupported_deserialize_kind!(EpKind::Redis),
                    #[cfg(feature = "s3")]
                    EpKind::S3 => deserialize_json_config!(raw_config, S3Config),
                    #[cfg(not(feature = "s3"))]
                    EpKind::S3 => unsupported_deserialize_kind!(EpKind::S3),
                    #[cfg(feature = "salesforce")]
                    EpKind::Salesforce => deserialize_json_config!(raw_config, SalesforceConfig),
                    #[cfg(not(feature = "salesforce"))]
                    EpKind::Salesforce => unsupported_deserialize_kind!(EpKind::Salesforce),
                    #[cfg(feature = "snowflake")]
                    EpKind::Snowflake => deserialize_json_config!(raw_config, SnowflakeConfig),
                    #[cfg(not(feature = "snowflake"))]
                    EpKind::Snowflake => unsupported_deserialize_kind!(EpKind::Snowflake),
                    #[cfg(feature = "tavily")]
                    EpKind::Tavily => deserialize_json_config!(raw_config, TavilyConfig),
                    #[cfg(not(feature = "tavily"))]
                    EpKind::Tavily => unsupported_deserialize_kind!(EpKind::Tavily),
                    #[cfg(feature = "weaviate")]
                    EpKind::Weaviate => deserialize_json_config!(raw_config, WeaviateConfig),
                    #[cfg(not(feature = "weaviate"))]
                    EpKind::Weaviate => unsupported_deserialize_kind!(EpKind::Weaviate),
                };

                Ok(EndpointSchemaInput { endpoint, kind, config, routing, description })
            }
        }

        const FIELDS: &[&str] = &["endpoint", "kind", "config", "routing", "description"];
        deserializer.deserialize_struct("EndpointSchemaInput", FIELDS, SchemaVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    const ALL_ENDPOINT_KINDS: &[EpKind] = &[
        EpKind::Aws,
        EpKind::Azure,
        EpKind::Cassandra,
        EpKind::Clickhouse,
        EpKind::Databricks,
        EpKind::Datadog,
        EpKind::Elasticache,
        EpKind::Eraser,
        EpKind::Function,
        EpKind::Gitlab,
        EpKind::GoogleWorkspace,
        EpKind::Http,
        EpKind::Llm,
        EpKind::Mongo,
        EpKind::Mssql,
        EpKind::Mysql,
        EpKind::Oracle,
        EpKind::Pinecone,
        EpKind::Posthog,
        EpKind::Postgres,
        EpKind::Rds,
        EpKind::Redis,
        EpKind::S3,
        EpKind::Salesforce,
        EpKind::Snowflake,
        EpKind::Tavily,
        EpKind::Weaviate,
    ];

    #[allow(unused_mut)]
    fn enabled_endpoint_kinds() -> HashSet<EpKind> {
        let mut kinds = HashSet::new();

        #[cfg(feature = "aws")]
        kinds.insert(EpKind::Aws);
        #[cfg(feature = "azure")]
        kinds.insert(EpKind::Azure);
        #[cfg(feature = "cassandra")]
        kinds.insert(EpKind::Cassandra);
        #[cfg(feature = "clickhouse")]
        kinds.insert(EpKind::Clickhouse);
        #[cfg(feature = "databricks")]
        kinds.insert(EpKind::Databricks);
        #[cfg(feature = "datadog")]
        kinds.insert(EpKind::Datadog);
        #[cfg(feature = "elasticache")]
        kinds.insert(EpKind::Elasticache);
        #[cfg(feature = "eraser")]
        kinds.insert(EpKind::Eraser);
        #[cfg(feature = "function")]
        kinds.insert(EpKind::Function);
        #[cfg(feature = "gitlab")]
        kinds.insert(EpKind::Gitlab);
        #[cfg(feature = "gworkspace")]
        kinds.insert(EpKind::GoogleWorkspace);
        #[cfg(feature = "http")]
        kinds.insert(EpKind::Http);
        #[cfg(feature = "llm")]
        kinds.insert(EpKind::Llm);
        #[cfg(feature = "mongo")]
        kinds.insert(EpKind::Mongo);
        #[cfg(feature = "mssql")]
        kinds.insert(EpKind::Mssql);
        #[cfg(feature = "mysql")]
        kinds.insert(EpKind::Mysql);
        #[cfg(feature = "oracle")]
        kinds.insert(EpKind::Oracle);
        #[cfg(feature = "pinecone")]
        kinds.insert(EpKind::Pinecone);
        #[cfg(feature = "posthog")]
        kinds.insert(EpKind::Posthog);
        #[cfg(feature = "postgres")]
        kinds.insert(EpKind::Postgres);
        #[cfg(feature = "rds")]
        kinds.insert(EpKind::Rds);
        #[cfg(feature = "redis")]
        kinds.insert(EpKind::Redis);
        #[cfg(feature = "s3")]
        kinds.insert(EpKind::S3);
        #[cfg(feature = "salesforce")]
        kinds.insert(EpKind::Salesforce);
        #[cfg(feature = "snowflake")]
        kinds.insert(EpKind::Snowflake);
        #[cfg(feature = "tavily")]
        kinds.insert(EpKind::Tavily);
        #[cfg(feature = "weaviate")]
        kinds.insert(EpKind::Weaviate);

        kinds
    }

    fn schema_json(kind: &str, config: serde_json::Value) -> String {
        serde_json::json!({
            "endpoint": format!("{}_test", kind),
            "kind": kind,
            "config": config,
            "description": format!("{} test description", kind)
        })
        .to_string()
    }

    fn serde_kind_name(kind: EpKind) -> &'static str {
        match kind {
            EpKind::GoogleWorkspace => "googleworkspace",
            _ => kind.as_str(),
        }
    }

    #[test]
    fn all_endpoint_kinds_are_listed_for_feature_gating_tests() {
        assert_eq!(
            ALL_ENDPOINT_KINDS.len(),
            27,
            "update ALL_ENDPOINT_KINDS when adding or removing EpKind variants"
        );

        let unique_kinds = ALL_ENDPOINT_KINDS.iter().copied().collect::<HashSet<_>>();
        assert_eq!(
            unique_kinds.len(),
            ALL_ENDPOINT_KINDS.len(),
            "ALL_ENDPOINT_KINDS contains duplicate variants: {ALL_ENDPOINT_KINDS:?}"
        );
    }

    #[test]
    fn disabled_endpoint_kinds_return_explicit_borsh_errors() {
        let enabled_kinds = enabled_endpoint_kinds();

        for kind in ALL_ENDPOINT_KINDS {
            if enabled_kinds.contains(kind) {
                continue;
            }

            let err = deserialize_config(&[], *kind)
                .expect_err(&format!("expected {kind:?} to be rejected when its feature is disabled"));
            let message = err.to_string();
            assert!(
                message.contains(&format!("{kind:?}")) && message.contains("not supported in this build"),
                "disabled {kind:?} returned an unclear error: {message}"
            );
        }
    }

    #[test]
    fn disabled_endpoint_kinds_return_explicit_json_errors() {
        let enabled_kinds = enabled_endpoint_kinds();

        for kind in ALL_ENDPOINT_KINDS {
            if enabled_kinds.contains(kind) {
                continue;
            }

            let json = schema_json(serde_kind_name(*kind), serde_json::json!({}));
            let err = serde_json::from_str::<EndpointSchemaInput>(&json)
                .expect_err(&format!("expected {kind:?} JSON config to be rejected when its feature is disabled"));
            let message = err.to_string();
            assert!(
                message.contains(kind.as_str()) && message.contains("not supported in this build"),
                "disabled {kind:?} JSON config returned an unclear error: {message}"
            );
        }
    }

    #[cfg(feature = "redis")]
    #[test]
    fn redis_schema_json_deserializes_legacy_connection_fields() {
        let schema = serde_json::from_str::<EndpointSchemaInput>(&schema_json(
            "redis",
            serde_json::json!({
                "read_conn": null,
                "write_conn": {
                    "host": "localhost",
                    "port": 6378,
                    "tls": null,
                    "password": "password"
                }
            }),
        ))
        .expect("redis schema JSON with legacy connection fields should deserialize");

        assert_eq!(schema.endpoint, EndpointId::from("redis_test"));
        assert_eq!(schema.kind, EpKind::Redis);
        assert_eq!(schema.description.as_deref(), Some("redis test description"));
        assert!(schema.routing.is_none(), "routing should default to None when omitted");

        let redis = schema
            .config
            .as_any()
            .downcast_ref::<RedisConfig>()
            .expect("redis schema config should downcast to RedisConfig");
        assert_eq!(redis.kind(), EpKind::Redis);
        assert_eq!(redis.target.host, "localhost");
        assert_eq!(redis.target.port, Some(6378));
        assert_eq!(
            redis.write_credentials.as_ref().and_then(|credentials| credentials.password.as_deref()),
            Some("password")
        );
        assert!(
            redis.read_credentials.is_none(),
            "null legacy read_conn should not create read credentials"
        );
    }

    #[cfg(not(feature = "redis"))]
    #[test]
    fn redis_schema_json_returns_feature_error_when_redis_is_disabled() {
        let err = serde_json::from_str::<EndpointSchemaInput>(&schema_json(
            "redis",
            serde_json::json!({
                "write_conn": {
                    "host": "localhost",
                    "port": 6378
                }
            }),
        ))
        .expect_err("redis schema JSON should be rejected when the redis feature is disabled");
        let message = err.to_string();

        assert!(
            message.contains("redis") && message.contains("not supported in this build"),
            "redis without the redis feature returned an unclear error: {message}"
        );
    }
}
