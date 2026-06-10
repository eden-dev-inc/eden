use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;
use tonic::{Request, Response, Status, metadata::MetadataValue};

use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use endpoint::{EpLifecycleRouter, default_engine_router};

use eden_core::format::endpoint::EpKind;
use opentelemetry::propagation::Extractor;

pub type TransactionResult<T> = Result<Response<T>, Status>;

pub struct MetaMap<'a>(&'a tonic::metadata::MetadataMap);

impl<'a> Extractor for MetaMap<'a> {
    /// Get a value for a key from the MetadataMap.  If the value can't be converted to &str, returns None
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
                tonic::metadata::KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
    }
}

#[derive(Clone)]
pub struct MyEngineService {
    pub router: Arc<RwLock<HashMap<EpKind, Box<dyn EpLifecycleRouter>>>>,
    pub database_manager: Option<Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>>,
}

impl MyEngineService {
    pub fn with_database_manager(database_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>) -> Self {
        Self {
            router: Arc::new(RwLock::new(Self::default_router())),
            database_manager: Some(database_manager),
        }
    }

    pub fn database_manager(&self) -> Option<Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>> {
        self.database_manager.clone()
    }

    fn default_router() -> HashMap<EpKind, Box<dyn EpLifecycleRouter>> {
        default_engine_router()
    }
}

impl Default for MyEngineService {
    fn default() -> Self {
        Self {
            router: Arc::new(RwLock::new(Self::default_router())),
            database_manager: None,
        }
    }
}

#[tracing::instrument]
#[allow(clippy::result_large_err)]
pub fn check_auth(req: Request<()>) -> Result<Request<()>, Status> {
    let token: MetadataValue<_> = "auth_token".parse().map_err(|_| Status::internal("Invalid auth token"))?;

    match req.metadata().get("authorization") {
        Some(t) if token == t => {
            tracing::debug!("Auth Passed");
            Ok(req)
        }
        _ => {
            tracing::warn!("No valid auth token");
            Err(Status::unauthenticated("No valid auth token"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(all(feature = "infra-tests", external_db))]
    use crate::test_utils::database_test_utils::initialize_engine_service;
    #[cfg(all(feature = "infra-tests", external_db))]
    use serial_test::serial;

    #[test]
    fn default_service_has_no_database_manager() {
        let service = MyEngineService::default();
        assert!(service.database_manager().is_none(), "default service should have no database_manager");
    }

    #[cfg(all(feature = "infra-tests", external_db))]
    #[tokio::test]
    #[serial]
    async fn with_database_manager_stores_and_returns_manager() {
        let (_redis_container, _pg_container, _clickhouse_container, db_manager, _) = initialize_engine_service().await;
        let db_manager = Arc::new(db_manager);

        let service = MyEngineService::with_database_manager(db_manager.clone());

        let retrieved = service.database_manager();
        assert!(retrieved.is_some(), "service should have database_manager after with_database_manager");
        assert!(
            Arc::ptr_eq(&db_manager, &retrieved.expect("service database manager")),
            "returned manager should be the same instance"
        );
    }
}
