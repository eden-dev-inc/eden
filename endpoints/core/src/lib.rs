#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Endpoint Implementations
//!
//! Database-specific endpoint implementations providing unified access to 12+ database types.
//!
//! ## Overview
//!
//! This crate implements the concrete database endpoint logic that powers Eve's
//! multi-database support. Each endpoint wraps a specific database driver with connection
//! pooling, request routing, and transaction support.
//!
//! ## Supported Endpoints
//!
//! - **SQL**: [`postgres`], [`mysql`], [`oracle`], [`mssql`], [`clickhouse`]
//! - **NoSQL**: [`mongo`], [`cassandra`], [`ep_redis`]
//! - **Vector**: [`pinecone`]
//! - **Web/AI**: [`http`], [`llm`], [`function`]
//!
//! ## Architecture
//!
//! Each endpoint follows a consistent structure:
//!
//! ```text
//! ┌──────────────────────────────────────┐
//! │  PostgresEp (example endpoint)       │
//! ├──────────────────────────────────────┤
//! │  - ep.rs: EP trait implementation    │
//! │  - api/: Available operations        │
//! │  - request.rs: Request wrapper       │
//! │  - metadata.rs: Schema introspection │
//! └──────────────────────────────────────┘
//! ```
//!
//! ## Core Traits
//!
//! ### [`EP`] - Endpoint Interface
//!
//! All endpoints implement this trait providing:
//! - `connect_async`: Establish connection pool
//! - `read_async`: Execute read operations
//! - `write_async`: Execute write operations
//! - `transaction`: Multi-operation ACID transactions
//! - `metadata`: Schema introspection
//! - `health_check`: Connection validation
//!
//! ### [`EpRequest`] - Type-safe Requests
//!
//! Each endpoint defines a request type wrapping operations:
//! ```ignore
//! // Example: PostgresRequest wraps PostgresApi operations
//! let request = PostgresRequest::new(Box::new(QueryInput::new("SELECT 1")));
//! ```
//!
//! ### [`Operation`] - Executable Operations
//!
//! Operations are database-specific API calls (e.g., `Query`, `Execute`, `Insert`).
//!
//! ## Request Flow
//!
//! 1. **HTTP API** → Receives endpoint request
//! 2. **Communication Layer** → Routes to [`EndpointRouter`]
//! 3. **Endpoint Implementation** → Downcasts request to specific type
//! 4. **Connection Pool** → Retrieves read/write connection
//! 5. **Operation Execution** → Runs against database
//! 6. **Response** → Serialized back through layers
//!
//! ## Connection Pooling
//!
//! All endpoints use [`EpPool`] for connection management:
//! - Separate read/write connection pools
//! - Automatic reconnection on failure
//! - Per-endpoint UUID isolation
//! - Telemetry integration
//!
//! ## Example Endpoint Structure
//!
//! Taking PostgreSQL as example:
//!
//! ```text
//! endpoints/postgres/
//! ├── ep.rs              # EP trait impl, transaction logic
//! ├── api/
//! │   ├── lib.rs        # PostgresApi enum
//! │   ├── query.rs      # SELECT operations
//! │   ├── execute.rs    # INSERT/UPDATE/DELETE
//! │   └── ...           # Other operations
//! ├── request.rs        # PostgresRequest wrapper
//! ├── metadata.rs       # Schema introspection
//! └── README.md         # Endpoint-specific docs
//! ```
//!
//! ## Adding New Endpoints
//!
//! To add a new database endpoint:
//!
//! 1. Create `endpoint_core/{db}_core` with connection logic
//! 2. Create an `endpoints/{db}/` crate
//! 3. Implement [`EP`] trait
//! 4. Define API operations enum
//! 5. Register the endpoint crate in the workspace manifests
//! 6. Register in [`EndpointRouter`]
//!
//! ## Testing
//!
//! Integration tests use [`testcontainers`] for real database instances:
//! ```bash
//! cargo test --test postgres_tests
//! ```

use std::{collections::HashMap, fmt::Debug};

pub use crate::{request::EpRequest, transaction::EpTransaction};

pub use ep_core::tls::TlsData;

#[cfg(feature = "aws")]
pub use ep_aws as aws;
#[cfg(feature = "azure")]
pub use ep_azure as azure;
#[cfg(feature = "cassandra")]
pub use ep_cassandra as cassandra;
#[cfg(feature = "clickhouse")]
pub use ep_clickhouse as clickhouse;
#[cfg(feature = "databricks")]
pub use ep_databricks as databricks;
#[cfg(feature = "datadog")]
pub use ep_datadog as datadog;
#[cfg(feature = "elasticache")]
pub use ep_elasticache;
#[cfg(feature = "eraser")]
pub use ep_eraser as eraser;
#[cfg(feature = "function")]
pub use ep_function as function;
#[cfg(feature = "gitlab")]
pub use ep_gitlab as gitlab;
#[cfg(feature = "gworkspace")]
pub use ep_gworkspace as gworkspace;
#[cfg(feature = "http")]
pub use ep_http as http;
#[cfg(feature = "llm")]
pub use ep_llm as llm;
#[cfg(feature = "mongo")]
pub use ep_mongo as mongo;
#[cfg(feature = "mssql")]
pub use ep_mssql as mssql;
#[cfg(feature = "mysql")]
pub use ep_mysql as mysql;
#[cfg(feature = "oracle")]
pub use ep_oracle as oracle;
#[cfg(feature = "pinecone")]
pub use ep_pinecone as pinecone;
#[cfg(feature = "postgres")]
pub use ep_postgres as postgres;
#[cfg(feature = "posthog")]
pub use ep_posthog as posthog;
#[cfg(feature = "rds")]
pub use ep_rds;
#[cfg(feature = "redis")]
pub use ep_redis;
#[cfg(feature = "s3")]
pub use ep_s3 as s3;
#[cfg(feature = "salesforce")]
pub use ep_salesforce as salesforce;
#[cfg(feature = "snowflake")]
pub use ep_snowflake as snowflake;
#[cfg(feature = "tavily")]
pub use ep_tavily as tavily;
#[cfg(feature = "weaviate")]
pub use ep_weaviate as weaviate;

#[cfg(feature = "aws")]
use crate::aws::ep::AwsEp;
#[cfg(feature = "azure")]
use crate::azure::ep::AzureEp;
#[cfg(feature = "cassandra")]
use crate::cassandra::ep::CassandraEp;
#[cfg(feature = "clickhouse")]
use crate::clickhouse::ep::ClickhouseEp;
#[cfg(feature = "databricks")]
use crate::databricks::ep::DatabricksEp;
#[cfg(feature = "datadog")]
use crate::datadog::ep::DatadogEp;
#[cfg(feature = "elasticache")]
use crate::ep_elasticache::ep::ElasticacheEp;
#[cfg(feature = "rds")]
use crate::ep_rds::ep::RdsEp;
#[cfg(feature = "redis")]
use crate::ep_redis::ep::RedisEp;
#[cfg(feature = "eraser")]
use crate::eraser::ep::EraserEp;
#[cfg(feature = "function")]
use crate::function::ep::FunctionEp;
#[cfg(feature = "gitlab")]
use crate::gitlab::ep::GitlabEp;
#[cfg(feature = "gworkspace")]
use crate::gworkspace::ep::GoogleWorkspaceEp;
#[cfg(feature = "http")]
use crate::http::ep::HttpEp;
#[cfg(feature = "llm")]
use crate::llm::ep::LlmEp;
#[cfg(feature = "mongo")]
use crate::mongo::ep::MongoEp;
#[cfg(feature = "mssql")]
use crate::mssql::ep::MssqlEp;
#[cfg(feature = "mysql")]
use crate::mysql::ep::MysqlEp;
#[cfg(feature = "oracle")]
use crate::oracle::ep::OracleEp;
#[cfg(feature = "pinecone")]
use crate::pinecone::ep::PineconeEp;
#[cfg(feature = "postgres")]
use crate::postgres::ep::PostgresEp;
#[cfg(feature = "posthog")]
use crate::posthog::ep::PosthogEp;
#[cfg(feature = "s3")]
use crate::s3::ep::S3Ep;
#[cfg(feature = "salesforce")]
use crate::salesforce::ep::SalesforceEp;
#[cfg(feature = "snowflake")]
use crate::snowflake::ep::SnowflakeEp;
#[cfg(feature = "tavily")]
use crate::tavily::ep::TavilyEp;
#[cfg(feature = "weaviate")]
use crate::weaviate::ep::WeaviateEp;

pub fn router_for(kind: endpoint_types::EpKind) -> Box<dyn endpoint_types::EpLifecycleRouter> {
    match kind {
        #[cfg(feature = "aws")]
        endpoint_types::EpKind::Aws => Box::new(AwsEp::default()),
        #[cfg(not(feature = "aws"))]
        endpoint_types::EpKind::Aws => panic!("Aws endpoint support is not enabled in this build"),
        #[cfg(feature = "azure")]
        endpoint_types::EpKind::Azure => Box::new(AzureEp::default()),
        #[cfg(not(feature = "azure"))]
        endpoint_types::EpKind::Azure => panic!("Azure endpoint support is not enabled in this build"),
        #[cfg(feature = "cassandra")]
        endpoint_types::EpKind::Cassandra => Box::new(CassandraEp::default()),
        #[cfg(not(feature = "cassandra"))]
        endpoint_types::EpKind::Cassandra => panic!("Cassandra endpoint support is not enabled in this build"),
        #[cfg(feature = "clickhouse")]
        endpoint_types::EpKind::Clickhouse => Box::new(ClickhouseEp::default()),
        #[cfg(not(feature = "clickhouse"))]
        endpoint_types::EpKind::Clickhouse => panic!("Clickhouse endpoint support is not enabled in this build"),
        #[cfg(feature = "databricks")]
        endpoint_types::EpKind::Databricks => Box::new(DatabricksEp::default()),
        #[cfg(not(feature = "databricks"))]
        endpoint_types::EpKind::Databricks => panic!("Databricks endpoint support is not enabled in this build"),
        #[cfg(feature = "datadog")]
        endpoint_types::EpKind::Datadog => Box::new(DatadogEp::default()),
        #[cfg(not(feature = "datadog"))]
        endpoint_types::EpKind::Datadog => panic!("Datadog endpoint support is not enabled in this build"),
        #[cfg(feature = "elasticache")]
        endpoint_types::EpKind::Elasticache => Box::new(ElasticacheEp::default()),
        #[cfg(not(feature = "elasticache"))]
        endpoint_types::EpKind::Elasticache => panic!("Elasticache endpoint support is not enabled in this build"),
        #[cfg(feature = "eraser")]
        endpoint_types::EpKind::Eraser => Box::new(EraserEp::default()),
        #[cfg(not(feature = "eraser"))]
        endpoint_types::EpKind::Eraser => panic!("Eraser endpoint support is not enabled in this build"),
        #[cfg(feature = "salesforce")]
        endpoint_types::EpKind::Salesforce => Box::new(SalesforceEp::default()),
        #[cfg(not(feature = "salesforce"))]
        endpoint_types::EpKind::Salesforce => panic!("Salesforce endpoint support is not enabled in this build"),
        #[cfg(feature = "function")]
        endpoint_types::EpKind::Function => Box::new(FunctionEp::default()),
        #[cfg(not(feature = "function"))]
        endpoint_types::EpKind::Function => panic!("Function endpoint support is not enabled in this build"),
        #[cfg(feature = "gitlab")]
        endpoint_types::EpKind::Gitlab => Box::new(GitlabEp::default()),
        #[cfg(not(feature = "gitlab"))]
        endpoint_types::EpKind::Gitlab => panic!("Gitlab endpoint support is not enabled in this build"),
        #[cfg(feature = "gworkspace")]
        endpoint_types::EpKind::GoogleWorkspace => Box::new(GoogleWorkspaceEp::default()),
        #[cfg(not(feature = "gworkspace"))]
        endpoint_types::EpKind::GoogleWorkspace => panic!("GoogleWorkspace endpoint support is not enabled in this build"),
        #[cfg(feature = "s3")]
        endpoint_types::EpKind::S3 => Box::new(S3Ep::default()),
        #[cfg(not(feature = "s3"))]
        endpoint_types::EpKind::S3 => panic!("S3 endpoint support is not enabled in this build"),
        #[cfg(feature = "http")]
        endpoint_types::EpKind::Http => Box::new(HttpEp::default()),
        #[cfg(not(feature = "http"))]
        endpoint_types::EpKind::Http => panic!("Http endpoint support is not enabled in this build"),
        #[cfg(feature = "llm")]
        endpoint_types::EpKind::Llm => Box::new(LlmEp::default()),
        #[cfg(not(feature = "llm"))]
        endpoint_types::EpKind::Llm => panic!("Llm endpoint support is not enabled in this build"),
        #[cfg(feature = "mongo")]
        endpoint_types::EpKind::Mongo => Box::new(MongoEp::default()),
        #[cfg(not(feature = "mongo"))]
        endpoint_types::EpKind::Mongo => panic!("Mongo endpoint support is not enabled in this build"),
        #[cfg(feature = "mssql")]
        endpoint_types::EpKind::Mssql => Box::new(MssqlEp::default()),
        #[cfg(not(feature = "mssql"))]
        endpoint_types::EpKind::Mssql => panic!("Mssql endpoint support is not enabled in this build"),
        #[cfg(feature = "mysql")]
        endpoint_types::EpKind::Mysql => Box::new(MysqlEp::default()),
        #[cfg(not(feature = "mysql"))]
        endpoint_types::EpKind::Mysql => panic!("Mysql endpoint support is not enabled in this build"),
        #[cfg(feature = "oracle")]
        endpoint_types::EpKind::Oracle => Box::new(OracleEp::default()),
        #[cfg(not(feature = "oracle"))]
        endpoint_types::EpKind::Oracle => panic!("Oracle endpoint support is not enabled in this build"),
        #[cfg(feature = "pinecone")]
        endpoint_types::EpKind::Pinecone => Box::new(PineconeEp::default()),
        #[cfg(not(feature = "pinecone"))]
        endpoint_types::EpKind::Pinecone => panic!("Pinecone endpoint support is not enabled in this build"),
        #[cfg(feature = "posthog")]
        endpoint_types::EpKind::Posthog => Box::new(PosthogEp::default()),
        #[cfg(not(feature = "posthog"))]
        endpoint_types::EpKind::Posthog => panic!("Posthog endpoint support is not enabled in this build"),
        #[cfg(feature = "postgres")]
        endpoint_types::EpKind::Postgres => Box::new(PostgresEp::default()),
        #[cfg(not(feature = "postgres"))]
        endpoint_types::EpKind::Postgres => panic!("Postgres endpoint support is not enabled in this build"),
        #[cfg(feature = "rds")]
        endpoint_types::EpKind::Rds => Box::new(RdsEp::default()),
        #[cfg(not(feature = "rds"))]
        endpoint_types::EpKind::Rds => panic!("Rds endpoint support is not enabled in this build"),
        #[cfg(feature = "redis")]
        endpoint_types::EpKind::Redis => Box::new(RedisEp::default()),
        #[cfg(not(feature = "redis"))]
        endpoint_types::EpKind::Redis => panic!("Redis endpoint support is not enabled in this build"),
        #[cfg(feature = "snowflake")]
        endpoint_types::EpKind::Snowflake => Box::new(SnowflakeEp::default()),
        #[cfg(not(feature = "snowflake"))]
        endpoint_types::EpKind::Snowflake => panic!("Snowflake endpoint support is not enabled in this build"),
        #[cfg(feature = "tavily")]
        endpoint_types::EpKind::Tavily => Box::new(TavilyEp::default()),
        #[cfg(not(feature = "tavily"))]
        endpoint_types::EpKind::Tavily => panic!("Tavily endpoint support is not enabled in this build"),
        #[cfg(feature = "weaviate")]
        endpoint_types::EpKind::Weaviate => Box::new(WeaviateEp::default()),
        #[cfg(not(feature = "weaviate"))]
        endpoint_types::EpKind::Weaviate => panic!("Weaviate endpoint support is not enabled in this build"),
        #[allow(unreachable_patterns)]
        other => panic!("{other} endpoint support is not enabled in this build"),
    }
}

pub fn default_engine_router() -> HashMap<endpoint_types::EpKind, Box<dyn endpoint_types::EpLifecycleRouter>> {
    HashMap::from([
        #[cfg(feature = "cassandra")]
        (endpoint_types::EpKind::Cassandra, router_for(endpoint_types::EpKind::Cassandra)),
        #[cfg(feature = "datadog")]
        (endpoint_types::EpKind::Datadog, router_for(endpoint_types::EpKind::Datadog)),
        #[cfg(feature = "mongo")]
        (endpoint_types::EpKind::Mongo, router_for(endpoint_types::EpKind::Mongo)),
        #[cfg(feature = "function")]
        (endpoint_types::EpKind::Function, router_for(endpoint_types::EpKind::Function)),
        #[cfg(feature = "s3")]
        (endpoint_types::EpKind::S3, router_for(endpoint_types::EpKind::S3)),
        #[cfg(feature = "mssql")]
        (endpoint_types::EpKind::Mssql, router_for(endpoint_types::EpKind::Mssql)),
        #[cfg(feature = "mysql")]
        (endpoint_types::EpKind::Mysql, router_for(endpoint_types::EpKind::Mysql)),
        #[cfg(feature = "oracle")]
        (endpoint_types::EpKind::Oracle, router_for(endpoint_types::EpKind::Oracle)),
        #[cfg(feature = "pinecone")]
        (endpoint_types::EpKind::Pinecone, router_for(endpoint_types::EpKind::Pinecone)),
        #[cfg(feature = "postgres")]
        (endpoint_types::EpKind::Postgres, router_for(endpoint_types::EpKind::Postgres)),
        #[cfg(feature = "redis")]
        (endpoint_types::EpKind::Redis, router_for(endpoint_types::EpKind::Redis)),
        #[cfg(feature = "llm")]
        (endpoint_types::EpKind::Llm, router_for(endpoint_types::EpKind::Llm)),
        #[cfg(feature = "weaviate")]
        (endpoint_types::EpKind::Weaviate, router_for(endpoint_types::EpKind::Weaviate)),
    ])
}

#[derive(Debug)]
pub enum EndpointRouter {
    #[cfg(feature = "aws")]
    Aws(AwsEp),
    #[cfg(feature = "azure")]
    Azure(AzureEp),
    #[cfg(feature = "cassandra")]
    Cassandra(CassandraEp),
    #[cfg(feature = "clickhouse")]
    Clickhouse(ClickhouseEp),
    #[cfg(feature = "databricks")]
    Databricks(DatabricksEp),
    #[cfg(feature = "datadog")]
    Datadog(DatadogEp),
    #[cfg(feature = "elasticache")]
    Elasticache(ElasticacheEp),
    #[cfg(feature = "eraser")]
    Eraser(EraserEp),
    #[cfg(feature = "function")]
    Function(FunctionEp),
    #[cfg(feature = "gitlab")]
    Gitlab(GitlabEp),
    #[cfg(feature = "gworkspace")]
    GoogleWorkspace(GoogleWorkspaceEp),
    #[cfg(feature = "s3")]
    S3(S3Ep),
    #[cfg(feature = "http")]
    Http(HttpEp),
    #[cfg(feature = "llm")]
    Llm(LlmEp),
    #[cfg(feature = "mongo")]
    Mongo(MongoEp),
    #[cfg(feature = "mssql")]
    Mssql(MssqlEp),
    #[cfg(feature = "mysql")]
    Mysql(MysqlEp),
    #[cfg(feature = "oracle")]
    Oracle(OracleEp),
    #[cfg(feature = "pinecone")]
    Pinecone(PineconeEp),
    #[cfg(feature = "posthog")]
    Posthog(PosthogEp),
    #[cfg(feature = "postgres")]
    Postgres(PostgresEp),
    #[cfg(feature = "rds")]
    Rds(RdsEp),
    #[cfg(feature = "redis")]
    Redis(RedisEp),
    #[cfg(feature = "salesforce")]
    Salesforce(SalesforceEp),
    #[cfg(feature = "tavily")]
    Tavily(TavilyEp),
    #[cfg(feature = "weaviate")]
    Weaviate(WeaviateEp),
}

pub use endpoint_types::*;

#[cfg(feature = "aws")]
pub use aws::serde::AwsOperation;
#[cfg(feature = "azure")]
pub use azure::serde::AzureOperation;
#[cfg(feature = "clickhouse")]
pub use clickhouse::serde::ClickhouseOperation;
#[cfg(feature = "databricks")]
pub use databricks::serde::DatabricksOperation;
#[cfg(feature = "datadog")]
pub use datadog::serde::DatadogOperation;
#[cfg(feature = "elasticache")]
pub use ep_elasticache::serde::ElasticacheOperation;
#[cfg(feature = "rds")]
pub use ep_rds::serde::RdsOperation;
#[cfg(feature = "redis")]
pub use ep_redis::serde::RedisOperation;
#[cfg(feature = "eraser")]
pub use eraser::serde::EraserOperation;
#[cfg(feature = "function")]
pub use function::serde::FunctionOperation;
#[cfg(feature = "gitlab")]
pub use gitlab::serde::GitlabOperation;
#[cfg(feature = "gworkspace")]
pub use gworkspace::serde::GoogleWorkspaceOperation;
#[cfg(feature = "http")]
pub use http::serde::HttpOperation;
#[cfg(feature = "llm")]
pub use llm::serde::LlmOperation;
#[cfg(feature = "mongo")]
pub use mongo::serde::MongoOperation;
#[cfg(feature = "mssql")]
pub use mssql::serde::MssqlOperation;
#[cfg(feature = "mysql")]
pub use mysql::serde::MysqlOperation;
#[cfg(feature = "oracle")]
pub use oracle::serde::OracleOperation;
#[cfg(feature = "pinecone")]
pub use pinecone::serde::PineconeOperation;
#[cfg(feature = "postgres")]
pub use postgres::serde::PostgresOperation;
#[cfg(feature = "posthog")]
pub use posthog::serde::PosthogOperation;
#[cfg(feature = "s3")]
pub use s3::serde::S3Operation;
#[cfg(feature = "salesforce")]
pub use salesforce::serde::SalesforceOperation;
#[cfg(feature = "tavily")]
pub use tavily::serde::TavilyOperation;
#[cfg(feature = "weaviate")]
pub use weaviate::serde::WeaviateOperation;
