use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use crate::methods::update::{SqlQueries, UpdateActor, UpdateMethod};
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
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::EndpointCacheUuid;
use eden_core::format::endpoint::EpKind;
use eden_core::format::{CacheObjectType, CacheUuid, EndpointId, EndpointUuid};
use eden_core::telemetry::TelemetryWrapper;
use endpoint_schema::endpoint::{EndpointSchema, UpdateEndpointSchema};
use ep_core::ep::EpConfig;
#[cfg(feature = "eraser")]
use eraser_core::config::EraserConfig;
#[cfg(feature = "function")]
use function_core::config::FunctionConfig;
#[cfg(feature = "gitlab")]
use gitlab_core::config::GitlabConfig;
#[cfg(feature = "gworkspace")]
use gworkspace_core::config::GoogleWorkspaceConfig;
#[cfg(feature = "http")]
use http_core::config::HttpConfig;

use function_name::named;
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
use postgres_core::PostgresConfig;
#[cfg(feature = "posthog")]
use posthog_core::config::PosthogConfig;
#[cfg(any(feature = "redis", feature = "elasticache"))]
use redis_core::config::RedisConfig;
#[cfg(feature = "s3")]
use s3_core::config::S3Config;
#[cfg(feature = "salesforce")]
use salesforce_core::config::SalesforceConfig;
#[cfg(feature = "snowflake")]
use snowflake_core::config::SnowflakeConfig;
#[cfg(feature = "tavily")]
use tavily_core::config::TavilyConfig;
#[cfg(feature = "weaviate")]
use weaviate_core::config::WeaviateConfig;

#[cfg(any(
    feature = "aws",
    feature = "azure",
    feature = "cassandra",
    feature = "clickhouse",
    feature = "databricks",
    feature = "datadog",
    feature = "elasticache",
    feature = "eraser",
    feature = "rds",
    feature = "function",
    feature = "gitlab",
    feature = "gworkspace",
    feature = "http",
    feature = "llm",
    feature = "mongo",
    feature = "mssql",
    feature = "mysql",
    feature = "oracle",
    feature = "pinecone",
    feature = "posthog",
    feature = "postgres",
    feature = "redis",
    feature = "s3",
    feature = "salesforce",
    feature = "snowflake",
    feature = "tavily",
    feature = "weaviate"
))]
macro_rules! deserialize_ep_config {
    ($config:expr, $config_type:ty) => {
        Box::new(serde_json::from_value::<$config_type>($config.clone()).map_err(EpError::request_error)?)
    };
}

pub fn deserialize_endpoint_config_for_kind(kind: EpKind, _config: &serde_json::Value) -> ResultEP<Box<dyn EpConfig>> {
    match kind {
        #[cfg(feature = "aws")]
        EpKind::Aws => Ok(deserialize_ep_config!(_config, AwsConfig)),
        #[cfg(not(feature = "aws"))]
        EpKind::Aws => unsupported_endpoint_config_for_kind(EpKind::Aws),
        #[cfg(feature = "azure")]
        EpKind::Azure => Ok(deserialize_ep_config!(_config, AzureConfig)),
        #[cfg(not(feature = "azure"))]
        EpKind::Azure => unsupported_endpoint_config_for_kind(EpKind::Azure),
        #[cfg(feature = "cassandra")]
        EpKind::Cassandra => Ok(deserialize_ep_config!(_config, CassandraConfig)),
        #[cfg(not(feature = "cassandra"))]
        EpKind::Cassandra => unsupported_endpoint_config_for_kind(EpKind::Cassandra),
        #[cfg(feature = "clickhouse")]
        EpKind::Clickhouse => Ok(deserialize_ep_config!(_config, ClickhouseConfig)),
        #[cfg(not(feature = "clickhouse"))]
        EpKind::Clickhouse => unsupported_endpoint_config_for_kind(EpKind::Clickhouse),
        #[cfg(feature = "databricks")]
        EpKind::Databricks => Ok(deserialize_ep_config!(_config, DatabricksConfig)),
        #[cfg(not(feature = "databricks"))]
        EpKind::Databricks => unsupported_endpoint_config_for_kind(EpKind::Databricks),
        #[cfg(feature = "datadog")]
        EpKind::Datadog => Ok(deserialize_ep_config!(_config, DatadogConfig)),
        #[cfg(not(feature = "datadog"))]
        EpKind::Datadog => unsupported_endpoint_config_for_kind(EpKind::Datadog),
        #[cfg(feature = "elasticache")]
        EpKind::Elasticache => Ok(deserialize_ep_config!(_config, RedisConfig)),
        #[cfg(not(feature = "elasticache"))]
        EpKind::Elasticache => unsupported_endpoint_config_for_kind(EpKind::Elasticache),
        #[cfg(feature = "eraser")]
        EpKind::Eraser => Ok(deserialize_ep_config!(_config, EraserConfig)),
        #[cfg(not(feature = "eraser"))]
        EpKind::Eraser => unsupported_endpoint_config_for_kind(EpKind::Eraser),
        #[cfg(feature = "rds")]
        EpKind::Rds => Ok(deserialize_ep_config!(_config, PostgresConfig)),
        #[cfg(not(feature = "rds"))]
        EpKind::Rds => unsupported_endpoint_config_for_kind(EpKind::Rds),
        #[cfg(feature = "function")]
        EpKind::Function => Ok(deserialize_ep_config!(_config, FunctionConfig)),
        #[cfg(not(feature = "function"))]
        EpKind::Function => unsupported_endpoint_config_for_kind(EpKind::Function),
        #[cfg(feature = "gitlab")]
        EpKind::Gitlab => Ok(deserialize_ep_config!(_config, GitlabConfig)),
        #[cfg(not(feature = "gitlab"))]
        EpKind::Gitlab => unsupported_endpoint_config_for_kind(EpKind::Gitlab),
        #[cfg(feature = "gworkspace")]
        EpKind::GoogleWorkspace => Ok(deserialize_ep_config!(_config, GoogleWorkspaceConfig)),
        #[cfg(not(feature = "gworkspace"))]
        EpKind::GoogleWorkspace => unsupported_endpoint_config_for_kind(EpKind::GoogleWorkspace),
        #[cfg(feature = "http")]
        EpKind::Http => Ok(deserialize_ep_config!(_config, HttpConfig)),
        #[cfg(not(feature = "http"))]
        EpKind::Http => unsupported_endpoint_config_for_kind(EpKind::Http),
        #[cfg(feature = "llm")]
        EpKind::Llm => Ok(deserialize_ep_config!(_config, LlmConfig)),
        #[cfg(not(feature = "llm"))]
        EpKind::Llm => unsupported_endpoint_config_for_kind(EpKind::Llm),
        #[cfg(feature = "mongo")]
        EpKind::Mongo => Ok(deserialize_ep_config!(_config, MongoConfig)),
        #[cfg(not(feature = "mongo"))]
        EpKind::Mongo => unsupported_endpoint_config_for_kind(EpKind::Mongo),
        #[cfg(feature = "mssql")]
        EpKind::Mssql => Ok(deserialize_ep_config!(_config, MssqlConfig)),
        #[cfg(not(feature = "mssql"))]
        EpKind::Mssql => unsupported_endpoint_config_for_kind(EpKind::Mssql),
        #[cfg(feature = "mysql")]
        EpKind::Mysql => Ok(deserialize_ep_config!(_config, MysqlConfig)),
        #[cfg(not(feature = "mysql"))]
        EpKind::Mysql => unsupported_endpoint_config_for_kind(EpKind::Mysql),
        #[cfg(feature = "oracle")]
        EpKind::Oracle => Ok(deserialize_ep_config!(_config, OracleConfig)),
        #[cfg(not(feature = "oracle"))]
        EpKind::Oracle => unsupported_endpoint_config_for_kind(EpKind::Oracle),
        #[cfg(feature = "pinecone")]
        EpKind::Pinecone => Ok(deserialize_ep_config!(_config, PineconeConfig)),
        #[cfg(not(feature = "pinecone"))]
        EpKind::Pinecone => unsupported_endpoint_config_for_kind(EpKind::Pinecone),
        #[cfg(feature = "posthog")]
        EpKind::Posthog => Ok(deserialize_ep_config!(_config, PosthogConfig)),
        #[cfg(not(feature = "posthog"))]
        EpKind::Posthog => unsupported_endpoint_config_for_kind(EpKind::Posthog),
        #[cfg(feature = "postgres")]
        EpKind::Postgres => Ok(deserialize_ep_config!(_config, PostgresConfig)),
        #[cfg(not(feature = "postgres"))]
        EpKind::Postgres => unsupported_endpoint_config_for_kind(EpKind::Postgres),
        #[cfg(feature = "redis")]
        EpKind::Redis => Ok(deserialize_ep_config!(_config, RedisConfig)),
        #[cfg(not(feature = "redis"))]
        EpKind::Redis => unsupported_endpoint_config_for_kind(EpKind::Redis),
        #[cfg(feature = "s3")]
        EpKind::S3 => Ok(deserialize_ep_config!(_config, S3Config)),
        #[cfg(not(feature = "s3"))]
        EpKind::S3 => unsupported_endpoint_config_for_kind(EpKind::S3),
        #[cfg(feature = "salesforce")]
        EpKind::Salesforce => Ok(deserialize_ep_config!(_config, SalesforceConfig)),
        #[cfg(not(feature = "salesforce"))]
        EpKind::Salesforce => unsupported_endpoint_config_for_kind(EpKind::Salesforce),
        #[cfg(feature = "snowflake")]
        EpKind::Snowflake => Ok(deserialize_ep_config!(_config, SnowflakeConfig)),
        #[cfg(not(feature = "snowflake"))]
        EpKind::Snowflake => unsupported_endpoint_config_for_kind(EpKind::Snowflake),
        #[cfg(feature = "tavily")]
        EpKind::Tavily => Ok(deserialize_ep_config!(_config, TavilyConfig)),
        #[cfg(not(feature = "tavily"))]
        EpKind::Tavily => unsupported_endpoint_config_for_kind(EpKind::Tavily),
        #[cfg(feature = "weaviate")]
        EpKind::Weaviate => Ok(deserialize_ep_config!(_config, WeaviateConfig)),
        #[cfg(not(feature = "weaviate"))]
        EpKind::Weaviate => unsupported_endpoint_config_for_kind(EpKind::Weaviate),
    }
}

#[cfg(any(
    not(feature = "aws"),
    not(feature = "azure"),
    not(feature = "cassandra"),
    not(feature = "clickhouse"),
    not(feature = "databricks"),
    not(feature = "datadog"),
    not(feature = "elasticache"),
    not(feature = "eraser"),
    not(feature = "rds"),
    not(feature = "function"),
    not(feature = "gitlab"),
    not(feature = "gworkspace"),
    not(feature = "http"),
    not(feature = "llm"),
    not(feature = "mongo"),
    not(feature = "mssql"),
    not(feature = "mysql"),
    not(feature = "oracle"),
    not(feature = "pinecone"),
    not(feature = "posthog"),
    not(feature = "postgres"),
    not(feature = "redis"),
    not(feature = "s3"),
    not(feature = "salesforce"),
    not(feature = "snowflake"),
    not(feature = "tavily"),
    not(feature = "weaviate")
))]
fn unsupported_endpoint_config_for_kind(kind: EpKind) -> ResultEP<Box<dyn EpConfig>> {
    Err(EpError::database(format!("{} endpoint config is not supported in this build", kind)))
}

impl DatabaseManager<RedisConn, PgConn, ClickhouseConn> {
    #[named]
    pub async fn update_endpoint_schema(
        &self,
        endpoint_schema: UpdateEndpointSchema,
        cache_object: &CacheObjectType<EndpointCacheUuid, EndpointCacheId>,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(format!("database.{}.endpoint", function_name!()));

        if let Some(id) = endpoint_schema.id() {
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::update_id(self, cache_object, SqlQueries::UpdateEndpointId, id.to_string(), updated_by, telemetry_wrapper)
            .await?;
        }

        if let Some(description) = endpoint_schema.description() {
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::update_description(
                self,
                cache_object,
                SqlQueries::UpdateEndpointDescription,
                description.to_string(),
                updated_by,
                telemetry_wrapper,
            )
            .await?;
        }

        if let Some(config) = endpoint_schema.config() {
            if let Some(endpoint_cache_uuid) = cache_object.uuid() {
                let existing_endpoint =
                    self.select_endpoint_uuid::<EndpointSchema>(&EndpointUuid::from(endpoint_cache_uuid.uuid()), telemetry_wrapper).await?;
                let ep_config = deserialize_endpoint_config_for_kind(existing_endpoint.kind(), config)?;
                <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
                    EndpointSchema,
                    EndpointCacheUuid,
                    EndpointUuid,
                    EndpointCacheId,
                    EndpointId,
                >>::update_endpoint_config(self, cache_object, ep_config, updated_by, telemetry_wrapper)
                .await?;
            } else {
                return Err(EpError::request("endpoint doesn't exist"));
            }
        }
        Ok(())
    }
}

#[cfg(all(test, any(feature = "http", feature = "redis")))]
mod tests {
    use super::*;
    #[cfg(feature = "http")]
    use http_core::config::HttpConfig;
    #[cfg(feature = "redis")]
    use redis_core::config::RedisConfig;
    use serde_json::json;

    #[cfg(feature = "redis")]
    #[test]
    fn deserialize_endpoint_config_for_kind_accepts_redis_config() {
        let config = json!({
            "target": {
                "host": "localhost",
                "port": 6379
            },
            "write_credentials": {
                "password": "secret"
            }
        });

        let parsed = deserialize_endpoint_config_for_kind(EpKind::Redis, &config).expect("valid redis config");

        assert!(parsed.as_any().downcast_ref::<RedisConfig>().is_some());
        assert_eq!(parsed.kind(), EpKind::Redis);
    }

    #[cfg(feature = "http")]
    #[test]
    fn deserialize_endpoint_config_for_kind_accepts_http_config() {
        let config = json!({
            "target": {
                "url": "http://127.0.0.1:8080"
            },
            "write_credentials": {}
        });

        let parsed = deserialize_endpoint_config_for_kind(EpKind::Http, &config).expect("valid http config");

        assert!(parsed.as_any().downcast_ref::<HttpConfig>().is_some());
        assert_eq!(parsed.kind(), EpKind::Http);
    }

    #[cfg(feature = "http")]
    #[test]
    fn deserialize_endpoint_config_for_kind_rejects_invalid_config() {
        let config = json!({
            "target": "not an http target"
        });

        let result = deserialize_endpoint_config_for_kind(EpKind::Http, &config);

        assert!(result.is_err());
    }
}
