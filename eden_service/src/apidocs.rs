use actix_web::{HttpResponse, Responder};
use utoipa::OpenApi;

#[cfg(feature = "openapi")]
use utoipa::{
    Modify,
    openapi::{
        self,
        security::{HttpBuilder, SecurityScheme},
    },
};

// OpenAPI-specific imports for database schemas
#[cfg(feature = "openapi")]
use endpoint_core::cassandra_core::{
    NonZeroU32Wrapper,
    config::CassandraConfig,
    connection::{CassandraConnection, CompressionWrapper, ConsistencyWrapper, Keyspace, SslMode, User},
};

#[cfg(feature = "openapi")]
use endpoint_core::clickhouse_core::connection::ClickhouseConnection;

#[cfg(all(feature = "openapi", feature = "function"))]
use endpoint_core::function_core::connection::FunctionConnection;

#[cfg(feature = "openapi")]
use endpoint_core::http_core::connection::HttpConnection;

#[cfg(feature = "openapi")]
use endpoint_core::mongo_core::{MongoConnection, auth::MongoAuth};

#[cfg(feature = "openapi")]
use endpoint_core::mssql_core::connection::MssqlConnection;

#[cfg(feature = "openapi")]
use endpoint_core::mysql_core::connection::MysqlConnection;

#[cfg(feature = "openapi")]
use endpoint_core::oracle_core::connection::OracleConnection;

#[cfg(feature = "openapi")]
use endpoint_core::pinecone_core::connection::PineconeConnection;

#[cfg(feature = "openapi")]
use endpoint_core::postgres_core::connection::PostgresConnection;

#[cfg(feature = "openapi")]
use endpoint_core::redis_core::RedisConnection;

#[cfg(feature = "openapi")]
pub struct AuthDocs;

#[cfg(feature = "openapi")]
impl Modify for AuthDocs {
    fn modify(&self, openapi: &mut openapi::OpenApi) {
        // Add security schemes
        if let Some(schema) = openapi.components.as_mut() {
            schema.add_security_scheme(
                "basicAuth",
                SecurityScheme::Http(HttpBuilder::new().scheme(openapi::security::HttpAuthScheme::Basic).build()),
            );
            schema.add_security_scheme(
                "bearerToken",
                SecurityScheme::Http(HttpBuilder::new().scheme(openapi::security::HttpAuthScheme::Bearer).bearer_format("JWT").build()),
            );
        }

        // Add /api/v1 prefix to all paths
        let old_paths = std::mem::take(&mut openapi.paths.paths);
        for (path, item) in old_paths {
            let new_path = format!("/api/v1{}", path);
            openapi.paths.paths.insert(new_path, item);
        }

        // Add database schemas used by endpoint configuration docs
        if let Some(schema) = openapi.components.as_mut() {
            {
                schema.schemas.insert(
                    <CassandraConnection as utoipa::ToSchema>::name().to_string(),
                    <CassandraConnection as utoipa::PartialSchema>::schema(),
                );
                schema.schemas.insert(
                    <CassandraConfig as utoipa::ToSchema>::name().to_string(),
                    <CassandraConfig as utoipa::PartialSchema>::schema(),
                );
                schema.schemas.insert(
                    <CompressionWrapper as utoipa::ToSchema>::name().to_string(),
                    <CompressionWrapper as utoipa::PartialSchema>::schema(),
                );
                schema.schemas.insert(
                    <ConsistencyWrapper as utoipa::ToSchema>::name().to_string(),
                    <ConsistencyWrapper as utoipa::PartialSchema>::schema(),
                );
                schema.schemas.insert(<Keyspace as utoipa::ToSchema>::name().to_string(), <Keyspace as utoipa::PartialSchema>::schema());
                schema.schemas.insert(
                    <NonZeroU32Wrapper as utoipa::ToSchema>::name().to_string(),
                    <NonZeroU32Wrapper as utoipa::PartialSchema>::schema(),
                );
                schema.schemas.insert(<SslMode as utoipa::ToSchema>::name().to_string(), <SslMode as utoipa::PartialSchema>::schema());
                schema.schemas.insert(<User as utoipa::ToSchema>::name().to_string(), <User as utoipa::PartialSchema>::schema());
            }
            {
                schema.schemas.insert(
                    <ClickhouseConnection as utoipa::ToSchema>::name().to_string(),
                    <ClickhouseConnection as utoipa::PartialSchema>::schema(),
                );
            }
            #[cfg(feature = "function")]
            {
                schema.schemas.insert(
                    <FunctionConnection as utoipa::ToSchema>::name().to_string(),
                    <FunctionConnection as utoipa::PartialSchema>::schema(),
                );
            }
            {
                schema.schemas.insert(
                    <HttpConnection as utoipa::ToSchema>::name().to_string(),
                    <HttpConnection as utoipa::PartialSchema>::schema(),
                );
            }
            {
                schema.schemas.insert(
                    <MongoConnection as utoipa::ToSchema>::name().to_string(),
                    <MongoConnection as utoipa::PartialSchema>::schema(),
                );
                schema
                    .schemas
                    .insert(<MongoAuth as utoipa::ToSchema>::name().to_string(), <MongoAuth as utoipa::PartialSchema>::schema());
            }
            {
                schema.schemas.insert(
                    <MssqlConnection as utoipa::ToSchema>::name().to_string(),
                    <MssqlConnection as utoipa::PartialSchema>::schema(),
                );
            }
            {
                schema.schemas.insert(
                    <MysqlConnection as utoipa::ToSchema>::name().to_string(),
                    <MysqlConnection as utoipa::PartialSchema>::schema(),
                );
            }
            {
                schema.schemas.insert(
                    <OracleConnection as utoipa::ToSchema>::name().to_string(),
                    <OracleConnection as utoipa::PartialSchema>::schema(),
                );
            }
            {
                schema.schemas.insert(
                    <PineconeConnection as utoipa::ToSchema>::name().to_string(),
                    <PineconeConnection as utoipa::PartialSchema>::schema(),
                );
            }
            {
                schema.schemas.insert(
                    <PostgresConnection as utoipa::ToSchema>::name().to_string(),
                    <PostgresConnection as utoipa::PartialSchema>::schema(),
                );
            }
            {
                schema.schemas.insert(
                    <RedisConnection as utoipa::ToSchema>::name().to_string(),
                    <RedisConnection as utoipa::PartialSchema>::schema(),
                );
            }
        }
    }
}

#[cfg(feature = "openapi")]
#[cfg_attr(embedded_db, path = "apidocs/api_docs_impl_embedded_db.rs")]
mod api_docs_impl;
#[cfg(feature = "openapi")]
pub use api_docs_impl::ApiDocs;

#[cfg(not(feature = "openapi"))]
#[derive(OpenApi)]
pub struct ApiDocs;

pub async fn serve_openapi_json() -> impl Responder {
    let openapi = ApiDocs::openapi();
    HttpResponse::Ok().content_type("application/json").body(openapi.to_pretty_json().unwrap_or_default())
}
