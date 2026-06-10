#![cfg_attr(test, allow(clippy::unwrap_used))]

#[cfg(feature = "aws")]
use endpoint::aws::api::lib::AwsApi;
#[cfg(feature = "azure")]
use endpoint::azure::api::lib::AzureApi;
#[cfg(feature = "cassandra")]
use endpoint::cassandra::api::lib::CassandraApi;
#[cfg(feature = "clickhouse")]
use endpoint::clickhouse::api::lib::ClickhouseApi;
#[cfg(feature = "databricks")]
use endpoint::databricks::api::lib::DatabricksApi;
#[cfg(feature = "datadog")]
use endpoint::datadog::api::lib::DatadogApi;
#[cfg(feature = "elasticache")]
use endpoint::ep_elasticache::api::control_plane::ElasticacheApi;
#[cfg(feature = "rds")]
use endpoint::ep_rds::api::control_plane::RdsApi;
#[cfg(feature = "redis")]
use endpoint::ep_redis::api::lib::RedisApi;
#[cfg(feature = "eraser")]
use endpoint::eraser::api::lib::EraserApi;
#[cfg(feature = "function")]
use endpoint::function::api::lib::FunctionApi;
#[cfg(feature = "gitlab")]
use endpoint::gitlab::api::lib::GitlabApi;
#[cfg(feature = "gworkspace")]
use endpoint::gworkspace::api::lib::GoogleWorkspaceApi;
#[cfg(feature = "http")]
use endpoint::http::api::lib::HttpApi;
#[cfg(feature = "llm")]
use endpoint::llm::api::lib::LlmApi;
#[cfg(feature = "mongo")]
use endpoint::mongo::api::lib::MongoApi;
#[cfg(feature = "mssql")]
use endpoint::mssql::api::lib::MssqlApi;
#[cfg(feature = "mysql")]
use endpoint::mysql::api::lib::MysqlApi;
#[cfg(feature = "oracle")]
use endpoint::oracle::api::lib::OracleApi;
#[cfg(feature = "pinecone")]
use endpoint::pinecone::api::lib::PineconeApi;
#[cfg(feature = "postgres")]
use endpoint::postgres::api::lib::PostgresApi;
#[cfg(feature = "s3")]
use endpoint::s3::api::lib::S3Api;
#[cfg(feature = "salesforce")]
use endpoint::salesforce::api::lib::SalesforceApi;
#[cfg(feature = "snowflake")]
use endpoint::snowflake::api::lib::SnowflakeApi;
#[cfg(feature = "tavily")]
use endpoint::tavily::api::lib::TavilyApi;
#[cfg(feature = "weaviate")]
use endpoint::weaviate::api::lib::WeaviateApi;
use serde_json::json;
use utoipa::openapi::{OneOfBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

#[derive(Debug, Clone)]
pub struct EndpointAPIRequest {}

impl ToSchema for EndpointAPIRequest {}

impl PartialSchema for EndpointAPIRequest {
    fn schema() -> RefOr<Schema> {
        let api_request = OneOfBuilder::new();
        #[cfg(feature = "aws")]
        let api_request = api_request.item(AwsApi::schema());
        #[cfg(feature = "azure")]
        let api_request = api_request.item(AzureApi::schema());
        #[cfg(feature = "cassandra")]
        let api_request = api_request.item(CassandraApi::schema());
        #[cfg(feature = "clickhouse")]
        let api_request = api_request.item(ClickhouseApi::schema());
        #[cfg(feature = "databricks")]
        let api_request = api_request.item(DatabricksApi::schema());
        #[cfg(feature = "datadog")]
        let api_request = api_request.item(DatadogApi::schema());
        #[cfg(feature = "elasticache")]
        let api_request = api_request.item(ElasticacheApi::schema());
        #[cfg(feature = "eraser")]
        let api_request = api_request.item(EraserApi::schema());
        #[cfg(feature = "function")]
        let api_request = api_request.item(FunctionApi::schema());
        #[cfg(feature = "gitlab")]
        let api_request = api_request.item(GitlabApi::schema());
        #[cfg(feature = "gworkspace")]
        let api_request = api_request.item(GoogleWorkspaceApi::schema());
        #[cfg(feature = "http")]
        let api_request = api_request.item(HttpApi::schema());
        #[cfg(feature = "s3")]
        let api_request = api_request.item(S3Api::schema());
        #[cfg(feature = "llm")]
        let api_request = api_request.item(LlmApi::schema());
        #[cfg(feature = "mongo")]
        let api_request = api_request.item(MongoApi::schema());
        #[cfg(feature = "mssql")]
        let api_request = api_request.item(MssqlApi::schema());
        #[cfg(feature = "mysql")]
        let api_request = api_request.item(MysqlApi::schema());
        #[cfg(feature = "oracle")]
        let api_request = api_request.item(OracleApi::schema());
        #[cfg(feature = "pinecone")]
        let api_request = api_request.item(PineconeApi::schema());
        #[cfg(feature = "postgres")]
        let api_request = api_request.item(PostgresApi::schema());
        #[cfg(feature = "rds")]
        let api_request = api_request.item(RdsApi::schema());
        #[cfg(feature = "redis")]
        let api_request = api_request.item(RedisApi::schema());
        #[cfg(feature = "salesforce")]
        let api_request = api_request.item(SalesforceApi::schema());
        #[cfg(feature = "snowflake")]
        let api_request = api_request.item(SnowflakeApi::schema());
        #[cfg(feature = "tavily")]
        let api_request = api_request.item(TavilyApi::schema());
        #[cfg(feature = "weaviate")]
        let api_request = api_request.item(WeaviateApi::schema());
        RefOr::T(Schema::OneOf(
            api_request
                .examples([json!(
                    {
                        "kind": "Redis",
                        "type": "SET",
                        "key": "x",
                        "value": 5
                    }
                )])
                .build(),
        ))
    }
}
