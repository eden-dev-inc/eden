use crate::EdenDb;
pub mod delete;
pub mod els;
pub mod get;
pub mod google_workspace_oauth;
pub mod list;
pub mod metadata;
pub mod patch;
pub mod post;
pub mod read;
#[cfg(any())]
mod redis_els_integration_tests;
pub(crate) mod runtime_cleanup;
pub mod transaction;
pub mod write;

// use eden_core::comm::{request::RequestSerde, transaction::TransactionSerde};
use eden_core::error::{EpError, ResultEP};
use endpoints::endpoint::EpRequest;

use eden_core::format::OrganizationUuid;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_schema::endpoint::EndpointSchema;

#[cfg(feature = "aws")]
use endpoints::endpoint::aws::request::AwsRequest;
#[cfg(feature = "azure")]
use endpoints::endpoint::azure::request::AzureRequest;
#[cfg(feature = "cassandra")]
use endpoints::endpoint::cassandra::request::CassandraRequest;
#[cfg(feature = "clickhouse")]
use endpoints::endpoint::clickhouse::request::ClickhouseRequest;
#[cfg(feature = "databricks")]
use endpoints::endpoint::databricks::request::DatabricksRequest;
#[cfg(feature = "datadog")]
use endpoints::endpoint::datadog::request::DatadogRequest;
#[cfg(feature = "elasticache")]
use endpoints::endpoint::ep_elasticache::request::ElasticacheRequest;
#[cfg(feature = "rds")]
use endpoints::endpoint::ep_rds::request::RdsRequest;
#[cfg(feature = "redis")]
use endpoints::endpoint::ep_redis::request::RedisRequest;
#[cfg(feature = "eraser")]
use endpoints::endpoint::eraser::request::EraserRequest;
#[cfg(feature = "function")]
use endpoints::endpoint::function::request::FunctionRequest;
#[cfg(feature = "gitlab")]
use endpoints::endpoint::gitlab::request::GitlabRequest;
#[cfg(feature = "gworkspace")]
use endpoints::endpoint::gworkspace::request::GoogleWorkspaceRequest;
#[cfg(feature = "http")]
use endpoints::endpoint::http::request::HttpRequest;

#[cfg(feature = "llm")]
use endpoints::endpoint::llm::request::LlmRequest;
#[cfg(feature = "mongo")]
use endpoints::endpoint::mongo::request::MongoRequest;
#[cfg(feature = "mssql")]
use endpoints::endpoint::mssql::request::MssqlRequest;
#[cfg(feature = "mysql")]
use endpoints::endpoint::mysql::request::MysqlRequest;
#[cfg(feature = "oracle")]
use endpoints::endpoint::oracle::request::OracleRequest;
#[cfg(feature = "pinecone")]
use endpoints::endpoint::pinecone::request::PineconeRequest;
#[cfg(feature = "postgres")]
use endpoints::endpoint::postgres::request::PostgresRequest;
#[cfg(feature = "posthog")]
use endpoints::endpoint::posthog::request::PosthogRequest;

#[cfg(feature = "s3")]
use endpoints::endpoint::s3::request::S3Request;
#[cfg(feature = "salesforce")]
use endpoints::endpoint::salesforce::request::SalesforceRequest;
#[cfg(feature = "snowflake")]
use endpoints::endpoint::snowflake::request::SnowflakeRequest;
#[cfg(feature = "tavily")]
use endpoints::endpoint::tavily::request::TavilyRequest;
#[cfg(feature = "weaviate")]
use endpoints::endpoint::weaviate::request::WeaviateRequest;

/// Serialize endpoint request with Borsh
pub fn borsh_serialize_request(input: &dyn EpRequest) -> ResultEP<Vec<u8>> {
    #[cfg(any(
        feature = "aws",
        feature = "azure",
        feature = "cassandra",
        feature = "clickhouse",
        feature = "databricks",
        feature = "datadog",
        feature = "elasticache",
        feature = "function",
        feature = "gitlab",
        feature = "gworkspace",
        feature = "s3",
        feature = "http",
        feature = "llm",
        feature = "mongo",
        feature = "mssql",
        feature = "mysql",
        feature = "oracle",
        feature = "pinecone",
        feature = "posthog",
        feature = "postgres",
        feature = "rds",
        feature = "redis",
        feature = "snowflake",
        feature = "tavily",
        feature = "eraser",
        feature = "salesforce",
        feature = "weaviate"
    ))]
    use eden_core::error::SerdeError;
    #[cfg(any(
        feature = "aws",
        feature = "azure",
        feature = "cassandra",
        feature = "clickhouse",
        feature = "databricks",
        feature = "datadog",
        feature = "elasticache",
        feature = "function",
        feature = "gitlab",
        feature = "gworkspace",
        feature = "s3",
        feature = "http",
        feature = "llm",
        feature = "mongo",
        feature = "mssql",
        feature = "mysql",
        feature = "oracle",
        feature = "pinecone",
        feature = "posthog",
        feature = "postgres",
        feature = "rds",
        feature = "redis",
        feature = "snowflake",
        feature = "tavily",
        feature = "eraser",
        feature = "salesforce",
        feature = "weaviate"
    ))]
    use eden_core::format::endpoint::EpKind;
    //TODO, get kind as an input variable
    match input.kind() {
        #[cfg(feature = "aws")]
        EpKind::Aws => borsh::to_vec(input.as_any().downcast_ref::<AwsRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),
        #[cfg(feature = "azure")]
        EpKind::Azure => borsh::to_vec(input.as_any().downcast_ref::<AzureRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),
        #[cfg(feature = "cassandra")]
        EpKind::Cassandra => {
            borsh::to_vec(input.as_any().downcast_ref::<CassandraRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
                .map_err(EpError::serde)
        }
        #[cfg(feature = "clickhouse")]
        EpKind::Clickhouse => {
            borsh::to_vec(input.as_any().downcast_ref::<ClickhouseRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
                .map_err(EpError::serde)
        }
        #[cfg(feature = "databricks")]
        EpKind::Databricks => {
            borsh::to_vec(input.as_any().downcast_ref::<DatabricksRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
                .map_err(EpError::serde)
        }
        #[cfg(feature = "datadog")]
        EpKind::Datadog => {
            borsh::to_vec(input.as_any().downcast_ref::<DatadogRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
                .map_err(EpError::serde)
        }
        #[cfg(feature = "elasticache")]
        EpKind::Elasticache => {
            borsh::to_vec(input.as_any().downcast_ref::<ElasticacheRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
                .map_err(EpError::serde)
        }
        #[cfg(feature = "function")]
        EpKind::Function => {
            borsh::to_vec(input.as_any().downcast_ref::<FunctionRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
                .map_err(EpError::serde)
        }
        #[cfg(feature = "gitlab")]
        EpKind::Gitlab => borsh::to_vec(input.as_any().downcast_ref::<GitlabRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),
        #[cfg(feature = "gworkspace")]
        EpKind::GoogleWorkspace => {
            borsh::to_vec(input.as_any().downcast_ref::<GoogleWorkspaceRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
                .map_err(EpError::serde)
        }

        #[cfg(feature = "s3")]
        EpKind::S3 => borsh::to_vec(input.as_any().downcast_ref::<S3Request>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),
        #[cfg(feature = "http")]
        EpKind::Http => borsh::to_vec(input.as_any().downcast_ref::<HttpRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),

        #[cfg(feature = "llm")]
        EpKind::Llm => borsh::to_vec(input.as_any().downcast_ref::<LlmRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),
        #[cfg(feature = "mongo")]
        EpKind::Mongo => borsh::to_vec(input.as_any().downcast_ref::<MongoRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),

        #[cfg(feature = "mssql")]
        EpKind::Mssql => borsh::to_vec(input.as_any().downcast_ref::<MssqlRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),
        #[cfg(feature = "mysql")]
        EpKind::Mysql => borsh::to_vec(input.as_any().downcast_ref::<MysqlRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),
        #[cfg(feature = "oracle")]
        EpKind::Oracle => borsh::to_vec(input.as_any().downcast_ref::<OracleRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),

        #[cfg(feature = "pinecone")]
        EpKind::Pinecone => {
            borsh::to_vec(input.as_any().downcast_ref::<PineconeRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
                .map_err(EpError::serde)
        }

        #[cfg(feature = "posthog")]
        EpKind::Posthog => {
            borsh::to_vec(input.as_any().downcast_ref::<PosthogRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
                .map_err(EpError::serde)
        }

        #[cfg(feature = "postgres")]
        EpKind::Postgres => {
            borsh::to_vec(input.as_any().downcast_ref::<PostgresRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
                .map_err(EpError::serde)
        }

        #[cfg(feature = "rds")]
        EpKind::Rds => borsh::to_vec(input.as_any().downcast_ref::<RdsRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),
        #[cfg(feature = "redis")]
        EpKind::Redis => borsh::to_vec(input.as_any().downcast_ref::<RedisRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),

        #[cfg(feature = "snowflake")]
        EpKind::Snowflake => {
            borsh::to_vec(input.as_any().downcast_ref::<SnowflakeRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
                .map_err(EpError::serde)
        }

        #[cfg(feature = "tavily")]
        EpKind::Tavily => borsh::to_vec(input.as_any().downcast_ref::<TavilyRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),
        #[cfg(feature = "eraser")]
        EpKind::Eraser => borsh::to_vec(input.as_any().downcast_ref::<EraserRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
            .map_err(EpError::serde),
        #[cfg(feature = "salesforce")]
        EpKind::Salesforce => {
            borsh::to_vec(input.as_any().downcast_ref::<SalesforceRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
                .map_err(EpError::serde)
        }
        #[cfg(feature = "weaviate")]
        EpKind::Weaviate => {
            borsh::to_vec(input.as_any().downcast_ref::<WeaviateRequest>().ok_or(EpError::Serde(SerdeError::InvalidRequest))?)
                .map_err(EpError::serde)
        }
        #[allow(unreachable_patterns)]
        other => Err(EpError::database(format!("{other} not supported in this build"))),
    }
}

#[cfg(feature = "llm")]
use endpoint_core::llm_core::{
    LlmCredential,
    config::{DEFAULT_MAX_TOOL_PASSES, LlmConfig},
};

#[cfg(feature = "llm")]
use std::collections::HashSet;

#[cfg(feature = "llm")]
use uuid::Uuid;

#[cfg(feature = "llm")]
pub async fn hydrate_llm_endpoint_config(
    db_manager: &EdenDb,
    endpoint_schema: &mut EndpointSchema,
    organization_uuid: &OrganizationUuid,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<()> {
    if endpoint_schema.kind() != eden_core::format::endpoint::EpKind::Llm {
        return Ok(());
    }

    let config = endpoint_schema.config();
    let mut llm_config =
        config.as_any().downcast_ref::<LlmConfig>().ok_or_else(|| EpError::connect("failed to downcast LlmConfig"))?.clone();
    let configured_max_tool_passes = usize::try_from(eden_config::agents().max_tool_passes)
        .ok()
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_TOOL_PASSES);
    if llm_config.max_tool_passes == DEFAULT_MAX_TOOL_PASSES {
        llm_config.max_tool_passes = configured_max_tool_passes;
    }

    let mut credential_ids: HashSet<Uuid> = HashSet::new();
    if let Some(read_creds) = &llm_config.read_credentials
        && let Some(id) = read_creds.credential_id
    {
        credential_ids.insert(id);
    }
    if let Some(write_creds) = &llm_config.write_credentials
        && let Some(id) = write_creds.credential_id
    {
        credential_ids.insert(id);
    }

    if credential_ids.is_empty() {
        endpoint_schema.update_config(Box::new(llm_config));
        return Ok(());
    }

    let credential_id_list: Vec<Uuid> = credential_ids.into_iter().collect();
    let stored_credentials = db_manager.fetch_llm_credentials_by_ids(organization_uuid, &credential_id_list, telemetry_wrapper).await?;

    llm_config.credentials.clear();

    for stored in stored_credentials {
        let api_key = stored.api_key.trim();
        let api_key = if api_key.is_empty() { None } else { Some(api_key.to_string()) };

        let credential = LlmCredential {
            id: stored.id,
            provider: stored.provider,
            label: stored.label.clone(),
            description: stored.description.clone(),
            base_url: stored.base_url.clone(),
            api_key,
        };

        llm_config.register_credential(credential);
    }

    endpoint_schema.update_config(Box::new(llm_config));

    Ok(())
}

#[cfg(not(feature = "llm"))]
pub async fn hydrate_llm_endpoint_config(
    _db_manager: &EdenDb,
    _endpoint_schema: &mut EndpointSchema,
    _organization_uuid: &OrganizationUuid,
    _telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<()> {
    Ok(())
}

#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod tests {
    use crate::EdenDb;
    use crate::comm::endpoints::delete::delete_endpoint;
    use crate::comm::endpoints::get::get_endpoint;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::eden_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use database::lib::{ClickhouseConn, PgConn, RedisConn};
    use database::methods::delete::DeleteMethod;
    use database::methods::delete::endpoint::DeleteEndpoint;
    use database::methods::insert::InsertMethod;
    use database::methods::insert::endpoint::InsertEndpoint;
    use eden_core::format::cache_id::EndpointCacheId;
    use eden_core::format::cache_uuid::EndpointCacheUuid;
    use eden_core::format::endpoint::EpKind;
    use eden_core::format::{CacheObjectType, CacheUuid, EdenId, EndpointId, EndpointUuid, OrganizationCacheUuid, UserUuid};
    use endpoint_core::ep_core::database::schema::Table;
    use endpoint_core::ep_core::ep::EpConfig;
    use endpoint_core::mongo_core::config::MongoConfig;
    use endpoint_schema::endpoint::EndpointSchema;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn endpoint_crud_test() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let (_, eden_node_schema, org_schema) = initialize_organization(&db_manager, test_telemetry).await;

        let endpoint_schema = EndpointSchema::new(
            EndpointId::new("test_endpoint".to_string()),
            EpKind::Mongo,
            MongoConfig::default().as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );

        // Post endpoint
        assert!(
            <EdenDb as InsertMethod<EndpointSchema, EndpointCacheUuid, EndpointCacheId, InsertEndpoint>>::insert(
                &db_manager,
                InsertEndpoint::new(org_schema.uuid(), endpoint_schema.clone(), eden_node_schema.uuid()),
                test_telemetry,
            )
            .await
            .is_ok()
        );

        let cache_object = CacheObjectType::new(
            Some(EndpointCacheUuid::new(
                Some(OrganizationCacheUuid::new(None, org_schema.uuid())),
                endpoint_schema.uuid(),
            )),
            None,
        );

        // Get endpoint
        assert_eq!(
            get_endpoint(&db_manager, &cache_object, test_telemetry,).await.expect("Failed to get endpoint").uuid(),
            endpoint_schema.uuid()
        );

        let del_endpoint = &<DeleteEndpoint as DeleteMethod<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointUuid,
            EndpointCacheId,
            EndpointId,
            RedisConn,
            PgConn,
            ClickhouseConn,
        >>::new(cache_object);

        // Delete endpoint
        assert!(delete_endpoint(&db_manager, test_telemetry, del_endpoint).await.is_ok());
    }
}
