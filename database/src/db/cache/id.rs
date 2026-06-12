// use std::backtrace::Backtrace;

use crate::db::cache::CacheIdFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection, ShardCache};
use bytes::{BufMut, Bytes, BytesMut};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::CacheId;
use eden_core::format::cache_uuid::CacheUuid;
use eden_core::format::{ApiId, AuthId, EndpointGroupId, InterlayId, RobotId, UserId};
use eden_core::format::{EdenNodeId, EndpointId, IdKind, OrganizationId, OrganizationUuid, TemplateId, WorkflowId};
use eden_core::telemetry::{CacheKind, MetricEvent, TelemetryWrapper};
#[cfg(not(embedded_db))]
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use ep_core::database::schema::{FromRow, Table};
use function_name::named;
use serde::Serialize;
use serde::de::DeserializeOwned;

const CACHE_ID_NAMESPACE: &[u8] = b"eden-cache-id";

fn cache_id_key<I: CacheId>(key: &I) -> Bytes {
    let id = key.id();
    let kind = I::kind().as_str().as_bytes();
    let org = key.org();
    let org_len = org.as_ref().map_or(0, |_| 16);
    let mut encoded = BytesMut::with_capacity(1 + org_len + 2 + kind.len() + 4 + id.len());

    if let Some(org) = org {
        encoded.put_u8(1);
        encoded.extend_from_slice(org.uuid().as_bytes());
    } else {
        encoded.put_u8(0);
    }

    encoded.put_u16(kind.len() as u16);
    encoded.extend_from_slice(kind);
    encoded.put_u32(id.len() as u32);
    encoded.extend_from_slice(id.as_bytes());
    encoded.freeze()
}

#[cfg(not(embedded_db))]
impl<T, I, R, P, C> CacheIdFunctions<T, I> for DatabaseManager<R, P, C>
where
    T: Table + FromRow + Serialize + DeserializeOwned,
    I: CacheId + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    #[named]
    async fn set(&self, key: I, value: T, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        self.internal_cache().json_kv_set(CACHE_ID_NAMESPACE, cache_id_key(&key), &value).await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn set_ex(&self, key: I, value: T, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        self.internal_cache().json_kv_set_ex(CACHE_ID_NAMESPACE, cache_id_key(&key), &value, self.cache_ttl()).await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn get_from_cache(&self, key: &I, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let org_uuid_str = key.org().map(|o| o.uuid().to_string()).unwrap_or_default();

        if let Some(output) = self.internal_cache().json_kv_get(CACHE_ID_NAMESPACE, cache_id_key(key)).await? {
            telemetry_wrapper.record_event(MetricEvent::CacheHit { org_uuid: &org_uuid_str, kind: CacheKind::Local });
            return Ok(output);
        }

        telemetry_wrapper.record_event(MetricEvent::CacheMiss { org_uuid: &org_uuid_str, kind: CacheKind::Local });
        let _ctx = ctx_with_trace!().with_feature("database");
        log_warn!(_ctx, "Cache miss", audience = LogAudience::Internal, cache_key = key.to_string());

        let result = <Self as CacheIdFunctions<T, I>>::get_from_database(self, key, telemetry_wrapper).await?;
        if let Err(error) = <Self as CacheIdFunctions<T, I>>::set_ex(self, key.clone(), result.clone(), telemetry_wrapper).await {
            log_warn!(
                ctx_with_trace!().with_feature("cache"),
                "Failed to update cache after database read",
                audience = LogAudience::Internal,
                error = error.to_string()
            );
        }
        Ok(result)
    }
    #[named]
    async fn get_from_database(&self, key: &I, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        // log::trace!("Get from database: {}: {key}", I::kind());
        // log::trace!(
        //     "Get from database backtrace: {}",
        //     Backtrace::force_capture()
        // );
        match I::kind() {
            IdKind::Api => self.select_api_id(&key.eden_id::<ApiId>(), telemetry_wrapper).await,
            IdKind::Auth => self.select_auth_id(&key.eden_id::<AuthId>(), telemetry_wrapper).await,
            IdKind::EdenNode => self.select_eden_node_id(&key.eden_id::<EdenNodeId>(), telemetry_wrapper).await,
            IdKind::Endpoint => self.select_endpoint_id(&key.eden_id::<EndpointId>(), telemetry_wrapper).await,
            IdKind::EndpointGroup => self.select_endpoint_group_id(&key.eden_id::<EndpointGroupId>(), telemetry_wrapper).await,
            IdKind::Interlay => self.select_interlay_id(&key.eden_id::<InterlayId>(), telemetry_wrapper).await,
            IdKind::ToolServer => Err(EpError::database("Lookup by ToolServer ID is not yet implemented")),
            IdKind::Organization => self.select_organization_id(&key.eden_id::<OrganizationId>(), telemetry_wrapper).await,
            IdKind::Project => Err(EpError::database("Lookup by ProjectId is not supported via generic cache")),
            IdKind::Template => self.select_template_id(&key.eden_id::<TemplateId>(), telemetry_wrapper).await,
            IdKind::User => {
                let org_uuid = key
                    .org()
                    .ok_or_else(|| EpError::database("User ID lookup requires organization context"))?
                    .eden_uuid::<OrganizationUuid>();
                self.select_user_id(&key.eden_id::<UserId>(), &org_uuid, telemetry_wrapper).await
            }
            IdKind::Robot => {
                let org_uuid = key
                    .org()
                    .ok_or_else(|| EpError::database("Robot ID lookup requires organization context"))?
                    .eden_uuid::<OrganizationUuid>();
                self.select_robot_id(&key.eden_id::<RobotId>(), &org_uuid, telemetry_wrapper).await
            }
            IdKind::Policy => Err(EpError::database("Lookup by Policy ID is not supported (use ELS-specific methods)")),
            IdKind::Workflow => self.select_workflow_id(&key.eden_id::<WorkflowId>(), telemetry_wrapper).await,
        }
    }
    #[named]
    async fn invalidate(&self, key: &I, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        self.internal_cache().json_kv_del(CACHE_ID_NAMESPACE, cache_id_key(key)).await?;

        Ok::<_, EpError>(())
    }
}

// ---------------------------------------------------------------------------
// Local-binary (ShardMap cache + Turso) cache implementation
// ---------------------------------------------------------------------------
#[cfg(embedded_db)]
impl<T, I, R, P, C> CacheIdFunctions<T, I> for DatabaseManager<R, P, C>
where
    T: Table + FromRow + Serialize + DeserializeOwned,
    I: CacheId + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    #[named]
    async fn set(&self, key: I, value: T, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        self.internal_cache().json_kv_set(CACHE_ID_NAMESPACE, cache_id_key(&key), &value).await?;
        Ok::<_, EpError>(())
    }
    #[named]
    async fn set_ex(&self, key: I, value: T, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        self.internal_cache().json_kv_set_ex(CACHE_ID_NAMESPACE, cache_id_key(&key), &value, self.cache_ttl()).await?;
        Ok::<_, EpError>(())
    }
    #[named]
    async fn get_from_cache(&self, key: &I, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let org_uuid_str = key.org().map(|o| o.uuid().to_string()).unwrap_or_default();
        if let Some(cached) = self.internal_cache().json_kv_get(CACHE_ID_NAMESPACE, cache_id_key(key)).await? {
            telemetry_wrapper.record_event(MetricEvent::CacheHit { org_uuid: &org_uuid_str, kind: CacheKind::Local });
            return Ok(cached);
        }
        // Fallback to database
        telemetry_wrapper.record_event(MetricEvent::CacheMiss { org_uuid: &org_uuid_str, kind: CacheKind::Local });
        let result = <Self as CacheIdFunctions<T, I>>::get_from_database(self, key, telemetry_wrapper).await?;
        let _ = <Self as CacheIdFunctions<T, I>>::set_ex(self, key.clone(), result.clone(), telemetry_wrapper).await;
        Ok(result)
    }
    #[named]
    async fn get_from_database(&self, key: &I, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        match I::kind() {
            IdKind::Api => self.select_api_id(&key.eden_id::<ApiId>(), telemetry_wrapper).await,
            IdKind::Auth => self.select_auth_id(&key.eden_id::<AuthId>(), telemetry_wrapper).await,
            IdKind::EdenNode => self.select_eden_node_id(&key.eden_id::<EdenNodeId>(), telemetry_wrapper).await,
            IdKind::Endpoint => self.select_endpoint_id(&key.eden_id::<EndpointId>(), telemetry_wrapper).await,
            IdKind::EndpointGroup => self.select_endpoint_group_id(&key.eden_id::<EndpointGroupId>(), telemetry_wrapper).await,
            IdKind::Interlay => self.select_interlay_id(&key.eden_id::<InterlayId>(), telemetry_wrapper).await,
            IdKind::ToolServer => Err(EpError::database("Lookup by ToolServer ID is not yet implemented")),
            IdKind::Organization => self.select_organization_id(&key.eden_id::<OrganizationId>(), telemetry_wrapper).await,
            IdKind::Project => Err(EpError::database("Lookup by ProjectId is not supported via generic cache")),
            IdKind::Template => self.select_template_id(&key.eden_id::<TemplateId>(), telemetry_wrapper).await,
            IdKind::User => {
                let org_uuid = key
                    .org()
                    .ok_or_else(|| EpError::database("User ID lookup requires organization context"))?
                    .eden_uuid::<OrganizationUuid>();
                self.select_user_id(&key.eden_id::<UserId>(), &org_uuid, telemetry_wrapper).await
            }
            IdKind::Robot => {
                let org_uuid = key
                    .org()
                    .ok_or_else(|| EpError::database("Robot ID lookup requires organization context"))?
                    .eden_uuid::<OrganizationUuid>();
                self.select_robot_id(&key.eden_id::<RobotId>(), &org_uuid, telemetry_wrapper).await
            }
            IdKind::Workflow => self.select_workflow_id(&key.eden_id::<WorkflowId>(), telemetry_wrapper).await,
            IdKind::Policy => Err(EpError::database("Lookup by Policy ID is not supported (use ELS-specific methods)")),
        }
    }
    #[named]
    async fn invalidate(&self, key: &I, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        self.internal_cache().json_kv_del(CACHE_ID_NAMESPACE, cache_id_key(key)).await?;
        Ok::<_, EpError>(())
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
mod tests {
    use crate::cache::CacheIdFunctions;
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::format::OrganizationUuid;
    use eden_core::format::UserUuid;
    use eden_core::format::cache_id::{CacheId, EndpointCacheId};
    use eden_core::format::cache_uuid::OrganizationCacheUuid;
    use eden_core::format::endpoint::EpKind;
    use eden_core::format::{CacheUuid, EdenId, EndpointId};
    use endpoint_schema::endpoint::EndpointSchema;
    use ep_core::database::schema::Table;
    use ep_core::ep::EpConfig;
    use postgres_core::config::PostgresConfig;

    #[tokio::test]
    async fn cache_id_set_get_invalidate() {
        // start containers
        let db_manager = create_database_manager().await;

        let organization = OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid());

        let test_telemetry = &mut test_telemetry();

        let endpoint_id = EndpointId::new("test_endpoint".to_string());
        let endpoint_cache_id = EndpointCacheId::new(Some(organization.clone()), endpoint_id.clone());

        let endpoint_schema = EndpointSchema::new(
            endpoint_id,
            EpKind::Postgres,
            PostgresConfig::default().as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );

        // set endpoint
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<EndpointSchema, EndpointCacheId>>::set(
                &db_manager,
                endpoint_cache_id.clone(),
                endpoint_schema.clone(),
                test_telemetry,
            )
            .await
            .is_ok()
        );

        // get endpoint
        assert_eq!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<EndpointSchema, EndpointCacheId>>::get_from_cache(
                &db_manager,
                &endpoint_cache_id,
                test_telemetry
            )
            .await
            .expect("Failed to get endpoint schema from cache")
            .endpoint_uuid(),
            endpoint_schema.uuid()
        );

        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<EndpointSchema, EndpointCacheId>>::invalidate(
                &db_manager,
                &endpoint_cache_id,
                test_telemetry
            )
            .await
            .is_ok()
        );

        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<EndpointSchema, EndpointCacheId>>::get_from_cache(
                &db_manager,
                &endpoint_cache_id,
                test_telemetry
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn cache_id_setex_get_invalidate() {
        // start containers
        let db_manager = create_database_manager().await;

        let organization = OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid());

        let test_telemetry = &mut test_telemetry();

        let endpoint_id = EndpointId::new("test_endpoint".to_string());
        let endpoint_cache_id = EndpointCacheId::new(Some(organization.clone()), endpoint_id.clone());

        let endpoint_schema = EndpointSchema::new(
            endpoint_id,
            EpKind::Postgres,
            PostgresConfig::default().as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );

        // set endpoint
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<EndpointSchema, EndpointCacheId>>::set_ex(
                &db_manager,
                endpoint_cache_id.clone(),
                endpoint_schema.clone(),
                test_telemetry,
            )
            .await
            .is_ok()
        );

        // get endpoint
        assert_eq!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<EndpointSchema, EndpointCacheId>>::get_from_cache(
                &db_manager,
                &endpoint_cache_id,
                test_telemetry
            )
            .await
            .expect("Failed to get endpoint schema from cache")
            .endpoint_uuid(),
            endpoint_schema.uuid()
        );

        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<EndpointSchema, EndpointCacheId>>::invalidate(
                &db_manager,
                &endpoint_cache_id,
                test_telemetry
            )
            .await
            .is_ok()
        );

        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheIdFunctions<EndpointSchema, EndpointCacheId>>::get_from_cache(
                &db_manager,
                &endpoint_cache_id,
                test_telemetry
            )
            .await
            .is_err()
        );
    }
}
