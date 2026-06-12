pub mod api;
pub mod auth;
pub mod eden_node;
pub mod endpoint;
pub mod endpoint_group;
pub mod interlay;
pub mod organization;
pub mod pipeline;
pub mod rbac;
pub mod robot;
pub mod snapshot;
pub mod template;
pub mod user;
pub mod workflow;

use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use eden_core::error::EpError;
use eden_core::telemetry::TelemetryWrapper;
use eden_logger_internal::{ctx_with_trace, log_debug};
use function_name::named;
use std::future::Future;

pub trait InsertMethod<T, U, I, S> {
    fn insert(&self, insert_schema: S, telemetry_wrapper: &mut TelemetryWrapper) -> impl Future<Output = Result<(), EpError>>;
}

pub(crate) trait Insert<R, P, C> {
    fn insert_database(
        &self,
        db: &DatabaseManager<R, P, C>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
    fn insert_cache(
        &self,
        db: &DatabaseManager<R, P, C>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(), EpError>>;
}

impl<T, U, I, S, R, P, C> InsertMethod<T, U, I, S> for DatabaseManager<R, P, C>
where
    S: Insert<R, P, C>,
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// New endpoints are associated to a customer server, and are depended on an
    /// existing organization, eden_node, and other parameters. When a new endpoint
    /// is inserted to the database, the `endpoint` table, `organization` table, and
    /// `eden_nodes` table should all be updated. The `eden_nodes` table should
    /// reflect which `eden_node` is connected to the endpoint, while the
    /// `organization` table reflected the new endpoint is part of the organization.
    #[named]
    async fn insert(&self, schema: S, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let mut span = telemetry_wrapper.client_tracer("directory.insert");

        let _ctx = ctx_with_trace!().with_feature("database");

        span.add_simple_event("inserting into database");
        log_debug!(_ctx.clone(), "Inserting into database", audience = eden_logger_internal::LogAudience::Internal);
        schema.insert_database(self, telemetry_wrapper).await?;

        span.add_simple_event("inserting key into cache");
        log_debug!(_ctx, "Inserting key into cache", audience = eden_logger_internal::LogAudience::Internal);
        schema.insert_cache(self, telemetry_wrapper).await
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
mod tests {
    use crate::db::methods::insert::endpoint::InsertEndpoint;
    use eden_core::format::UserUuid;
    use eden_core::format::endpoint::EpKind;
    use eden_core::format::{EdenId, EdenNodeUuid, EndpointId, OrganizationUuid};
    use endpoint_schema::endpoint::EndpointSchema;
    use mongo_core::auth::{MongoAuth, MongoUap};
    use mongo_core::config::{AcceptType, ContentType, MongoConfig};
    use mongo_core::connection::MongoConnection;
    use serde_json;

    #[test]
    fn test_endpoint_id_serialization() {
        let endpoint_id = EndpointId::new("test_endpoint".to_string());

        // Test borsh serialization
        let borsh_serialized = borsh::to_vec(&endpoint_id).expect("borsh serialization failed");
        let borsh_deserialized: EndpointId = borsh::from_slice(&borsh_serialized).expect("borsh deserialization failed");
        assert_eq!(endpoint_id, borsh_deserialized);

        // Test serde serialization
        let serde_serialized = serde_json::to_string(&endpoint_id).expect("serde serialization failed");
        let serde_deserialized: EndpointId = serde_json::from_str(&serde_serialized).expect("serde deserialization failed");
        assert_eq!(endpoint_id, serde_deserialized);
    }

    #[test]
    fn test_mongo_auth_serialization() {
        let auth_none = MongoAuth::UAP(MongoUap::default());

        // Test borsh serialization for None variant
        let borsh_serialized = borsh::to_vec(&auth_none).expect("borsh serialization failed");
        let borsh_deserialized: MongoAuth = borsh::from_slice(&borsh_serialized).expect("borsh deserialization failed");
        assert_eq!(auth_none, borsh_deserialized);

        // Test serde serialization for None variant
        let serde_serialized = serde_json::to_string(&auth_none).expect("serde serialization failed");
        let serde_deserialized: MongoAuth = serde_json::from_str(&serde_serialized).expect("serde deserialization failed");
        assert_eq!(auth_none, serde_deserialized);
    }

    #[test]
    fn test_mongo_connection_serialization() {
        let connection = MongoConnection { url: "mongodb://localhost:27017".to_string(), auth: None };

        // Test borsh serialization
        let borsh_serialized = borsh::to_vec(&connection).expect("borsh serialization failed");
        let borsh_deserialized: MongoConnection = borsh::from_slice(&borsh_serialized).expect("borsh deserialization failed");
        assert_eq!(connection, borsh_deserialized);

        // Test serde serialization
        let serde_serialized = serde_json::to_string(&connection).expect("serde serialization failed");
        let serde_deserialized: MongoConnection = serde_json::from_str(&serde_serialized).expect("serde deserialization failed");
        assert_eq!(connection, serde_deserialized);
    }

    #[test]
    fn test_content_type_serialization() {
        let content_type = ContentType::JSON;

        // Test borsh serialization
        let borsh_serialized = borsh::to_vec(&content_type).expect("borsh serialization failed");
        let borsh_deserialized: ContentType = borsh::from_slice(&borsh_serialized).expect("borsh deserialization failed");
        assert_eq!(content_type, borsh_deserialized);

        // Test serde serialization
        let serde_serialized = serde_json::to_string(&content_type).expect("serde serialization failed");
        let serde_deserialized: ContentType = serde_json::from_str(&serde_serialized).expect("serde deserialization failed");
        assert_eq!(content_type, serde_deserialized);
    }

    #[test]
    fn test_accept_type_serialization() {
        let accept_type = AcceptType::JSON;

        // Test borsh serialization
        let borsh_serialized = borsh::to_vec(&accept_type).expect("borsh serialization failed");
        let borsh_deserialized: AcceptType = borsh::from_slice(&borsh_serialized).expect("borsh deserialization failed");
        assert_eq!(accept_type, borsh_deserialized);

        // Test serde serialization
        let serde_serialized = serde_json::to_string(&accept_type).expect("serde serialization failed");
        let serde_deserialized: AcceptType = serde_json::from_str(&serde_serialized).expect("serde deserialization failed");
        assert_eq!(accept_type, serde_deserialized);
    }

    #[test]
    fn test_mongo_config_serialization() {
        // Test with None for read_conn
        let config_none = MongoConfig {
            auth: None,
            target: Default::default(),
            read_credentials: None,
            write_credentials: None,
            admin_credentials: None,
            system_credentials: None,
            content: ContentType::JSON,
            accept: AcceptType::JSON,
            api_key: "test_key".to_string(),
        };

        // Test borsh serialization
        let borsh_serialized = borsh::to_vec(&config_none).expect("borsh serialization failed");
        let borsh_deserialized: MongoConfig = borsh::from_slice(&borsh_serialized).expect("borsh deserialization failed");
        assert_eq!(config_none, borsh_deserialized);

        // Test serde serialization
        let serde_serialized = serde_json::to_string(&config_none).expect("serde serialization failed");
        let serde_deserialized: MongoConfig = serde_json::from_str(&serde_serialized).expect("serde deserialization failed");
        assert_eq!(config_none, serde_deserialized);

        // Test with Some for write_conn
        let config_some = MongoConfig {
            auth: None,
            target: Default::default(),
            read_credentials: None,
            write_credentials: Some(Default::default()),
            admin_credentials: None,
            system_credentials: None,
            content: ContentType::JSON,
            accept: AcceptType::JSON,
            api_key: "test_key".to_string(),
        };

        // Test borsh serialization with Some
        let borsh_serialized = borsh::to_vec(&config_some).expect("borsh serialization failed");
        let borsh_deserialized: MongoConfig = borsh::from_slice(&borsh_serialized).expect("borsh deserialization failed");
        assert_eq!(config_some, borsh_deserialized);

        // Test serde serialization with Some
        let serde_serialized = serde_json::to_string(&config_some).expect("serde serialization failed");
        let serde_deserialized: MongoConfig = serde_json::from_str(&serde_serialized).expect("serde deserialization failed");
        assert_eq!(config_some, serde_deserialized);
    }

    #[test]
    fn test_endpoint_schema_serialization() {
        // Test with None for description
        let endpoint = EndpointId::new("test_endpoint".to_string());
        let config = Box::new(MongoConfig {
            auth: None,
            target: Default::default(),
            read_credentials: None,
            write_credentials: None,
            admin_credentials: None,
            system_credentials: None,
            content: ContentType::JSON,
            accept: AcceptType::JSON,
            api_key: "test_key".to_string(),
        });
        let schema_none = EndpointSchema::new(endpoint.clone(), EpKind::Mongo, config.clone(), None, None, UserUuid::new_uuid());

        // Test borsh serialization
        let borsh_serialized = borsh::to_vec(&schema_none).expect("borsh serialization failed");
        let _borsh_deserialized: EndpointSchema = borsh::from_slice(&borsh_serialized).expect("borsh deserialization failed");
        // assert_eq!(schema_none, borsh_deserialized);

        // Test serde serialization
        let serde_serialized = serde_json::to_string(&schema_none).expect("serde serialization failed");
        let _serde_deserialized: EndpointSchema = serde_json::from_str(&serde_serialized).expect("serde deserialization failed");
        // assert_eq!(schema_none, serde_deserialized);

        // Test with Some for description
        let schema_some =
            EndpointSchema::new(endpoint, EpKind::Mongo, config, None, Some("test description".to_string()), UserUuid::new_uuid());

        // Test serde serialization with Some
        let serde_serialized = serde_json::to_string(&schema_some).expect("serde serialization failed");
        let _serde_deserialized: EndpointSchema = serde_json::from_str(&serde_serialized).expect("serde deserialization failed");
        // assert_eq!(schema_some, serde_deserialized);

        // Test borsh serialization with Some
        let borsh_serialized = borsh::to_vec(&schema_some).expect("borsh serialization failed");
        let _borsh_deserialized: EndpointSchema = borsh::from_slice(&borsh_serialized).expect("borsh deserialization failed");
        // assert_eq!(schema_some, borsh_deserialized);
    }

    #[test]
    fn test_insert_endpoint_serialization() {
        let endpoint = EndpointId::new("test_endpoint".to_string());
        let config = Box::new(MongoConfig {
            auth: None,
            target: Default::default(),
            read_credentials: None,
            write_credentials: None,
            admin_credentials: None,
            system_credentials: None,
            content: ContentType::JSON,
            accept: AcceptType::JSON,
            api_key: "test_key".to_string(),
        });

        let insert_endpoint = InsertEndpoint::new(
            OrganizationUuid::new_uuid(),
            EndpointSchema::new(endpoint, EpKind::Mongo, config, None, None, UserUuid::new_uuid()),
            EdenNodeUuid::new_uuid(),
        );

        // Test serde serialization
        let serde_serialized = serde_json::to_string(&insert_endpoint).expect("serde serialization failed");
        let _serde_deserialized: InsertEndpoint = serde_json::from_str(&serde_serialized).expect("serde deserialization failed");
        // assert_eq!(insert_endpoint, serde_deserialized);

        // Test borsh serialization
        let borsh_serialized = borsh::to_vec(&insert_endpoint).expect("borsh serialization failed");
        let _borsh_deserialized: InsertEndpoint = borsh::from_slice(&borsh_serialized).expect("borsh deserialization failed");
        // assert_eq!(insert_endpoint, borsh_deserialized);
    }
}
