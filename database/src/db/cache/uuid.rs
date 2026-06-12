use crate::db::cache::CacheUuidFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection, ShardCache};
use bytes::{BufMut, Bytes, BytesMut};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_uuid::CacheUuid;
use eden_core::format::{
    ApiUuid, AuthUuid, EdenNodeUuid, EndpointGroupUuid, EndpointUuid, IdKind, InterlayUuid, OrganizationUuid, RobotUuid, TemplateUuid,
    UserUuid, WorkflowUuid,
};
use eden_core::telemetry::{CacheKind, MetricEvent, TelemetryWrapper};
#[cfg(not(embedded_db))]
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use ep_core::database::schema::{FromRow, Table};
use function_name::named;
use serde::Serialize;
use serde::de::DeserializeOwned;

const CACHE_UUID_NAMESPACE: &[u8] = b"eden-cache-uuid";

fn cache_uuid_key<U: CacheUuid>(key: &U) -> Bytes {
    let kind = U::kind().as_str().as_bytes();
    let org = key.org();
    let org_len = org.as_ref().map_or(0, |_| 16);
    let mut encoded = BytesMut::with_capacity(1 + org_len + 2 + kind.len() + 16);

    if let Some(org) = org {
        encoded.put_u8(1);
        encoded.extend_from_slice(org.uuid().as_bytes());
    } else {
        encoded.put_u8(0);
    }

    encoded.put_u16(kind.len() as u16);
    encoded.extend_from_slice(kind);
    encoded.extend_from_slice(key.uuid().as_bytes());
    encoded.freeze()
}

// ---------------------------------------------------------------------------
// Default (ShardMap cache + PostgreSQL) cache implementation
// ---------------------------------------------------------------------------
#[cfg(not(embedded_db))]
impl<T, U, R, P, C> CacheUuidFunctions<T, U> for DatabaseManager<R, P, C>
where
    T: Table + FromRow + Serialize + DeserializeOwned,
    U: CacheUuid + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    #[named]
    async fn set(&self, key: U, value: T, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        self.internal_cache().json_kv_set(CACHE_UUID_NAMESPACE, cache_uuid_key(&key), &value).await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn set_ex(&self, key: U, value: T, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        self.internal_cache().json_kv_set_ex(CACHE_UUID_NAMESPACE, cache_uuid_key(&key), &value, self.cache_ttl()).await?;

        Ok::<_, EpError>(())
    }
    #[named]
    async fn get_from_cache(&self, key: &U, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let org_uuid_str = key.org().map(|o| o.uuid().to_string()).unwrap_or_default();

        if let Some(output) = self.internal_cache().json_kv_get(CACHE_UUID_NAMESPACE, cache_uuid_key(key)).await? {
            telemetry_wrapper.record_event(MetricEvent::CacheHit { org_uuid: &org_uuid_str, kind: CacheKind::Local });
            return Ok(output);
        }

        telemetry_wrapper.record_event(MetricEvent::CacheMiss { org_uuid: &org_uuid_str, kind: CacheKind::Local });
        let _ctx = ctx_with_trace!().with_feature("database");
        log_warn!(_ctx, "Cache miss", audience = LogAudience::Internal, cache_key = key.to_string());

        let result = <Self as CacheUuidFunctions<T, U>>::get_from_database(self, key, telemetry_wrapper).await?;
        match <Self as CacheUuidFunctions<T, U>>::set_ex(self, key.clone(), result.clone(), telemetry_wrapper).await {
            Ok(_) => {}
            Err(e) => {
                log_warn!(
                    ctx_with_trace!().with_feature("cache"),
                    "Failed to update cache after database read",
                    audience = LogAudience::Internal,
                    error = e.to_string()
                );
            }
        }
        Ok(result)
    }
    #[named]
    async fn get_from_database(&self, key: &U, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        match U::kind() {
            IdKind::Api => self.select_api_uuid(&key.eden_uuid::<ApiUuid>(), telemetry_wrapper).await,
            IdKind::Auth => self.select_auth_uuid(&key.eden_uuid::<AuthUuid>(), telemetry_wrapper).await,
            IdKind::EdenNode => self.select_eden_node_uuid(&key.eden_uuid::<EdenNodeUuid>(), telemetry_wrapper).await,
            IdKind::Endpoint => self.select_endpoint_uuid(&key.eden_uuid::<EndpointUuid>(), telemetry_wrapper).await,
            IdKind::EndpointGroup => self.select_endpoint_group_uuid(&key.eden_uuid::<EndpointGroupUuid>(), telemetry_wrapper).await,
            IdKind::Interlay => self.select_interlay_uuid(&key.eden_uuid::<InterlayUuid>(), telemetry_wrapper).await,
            IdKind::ToolServer => Err(EpError::database("Lookup by ToolServer UUID is not yet implemented")),
            IdKind::Organization => self.select_organization_uuid(&key.eden_uuid::<OrganizationUuid>(), telemetry_wrapper).await,
            IdKind::Project => Err(EpError::database("Lookup by ProjectUuid is not supported via generic cache")),
            IdKind::Robot => self.select_robot_uuid(&key.eden_uuid::<RobotUuid>(), telemetry_wrapper).await,
            IdKind::Template => self.select_template_uuid(&key.eden_uuid::<TemplateUuid>(), telemetry_wrapper).await,
            IdKind::User => self.select_user_uuid(&key.eden_uuid::<UserUuid>(), telemetry_wrapper).await,
            IdKind::Policy => Err(EpError::database("Lookup by Policy UUID is not supported via cache (use ELS-specific methods)")),
            IdKind::Workflow => self.select_workflow_uuid(&key.eden_uuid::<WorkflowUuid>(), telemetry_wrapper).await,
        }
    }
    #[named]
    async fn invalidate(&self, key: &U, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        self.internal_cache().json_kv_del(CACHE_UUID_NAMESPACE, cache_uuid_key(key)).await?;

        Ok::<_, EpError>(())
    }
}

// ---------------------------------------------------------------------------
// Local-binary (ShardMap cache + Turso) cache implementation
// ---------------------------------------------------------------------------
#[cfg(embedded_db)]
impl<T, U, R, P, C> CacheUuidFunctions<T, U> for DatabaseManager<R, P, C>
where
    T: Table + FromRow + Serialize + DeserializeOwned,
    U: CacheUuid + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    #[named]
    async fn set(&self, key: U, value: T, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        self.internal_cache().json_kv_set(CACHE_UUID_NAMESPACE, cache_uuid_key(&key), &value).await?;
        Ok::<_, EpError>(())
    }
    #[named]
    async fn set_ex(&self, key: U, value: T, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        self.internal_cache().json_kv_set_ex(CACHE_UUID_NAMESPACE, cache_uuid_key(&key), &value, self.cache_ttl()).await?;
        Ok::<_, EpError>(())
    }
    #[named]
    async fn get_from_cache(&self, key: &U, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let org_uuid_str = key.org().map(|o| o.uuid().to_string()).unwrap_or_default();
        if let Some(cached) = self.internal_cache().json_kv_get(CACHE_UUID_NAMESPACE, cache_uuid_key(key)).await? {
            telemetry_wrapper.record_event(MetricEvent::CacheHit { org_uuid: &org_uuid_str, kind: CacheKind::Local });
            return Ok(cached);
        }
        // Fallback to database
        telemetry_wrapper.record_event(MetricEvent::CacheMiss { org_uuid: &org_uuid_str, kind: CacheKind::Local });
        let result = <Self as CacheUuidFunctions<T, U>>::get_from_database(self, key, telemetry_wrapper).await?;
        let _ = <Self as CacheUuidFunctions<T, U>>::set_ex(self, key.clone(), result.clone(), telemetry_wrapper).await;
        Ok(result)
    }
    #[named]
    async fn get_from_database(&self, key: &U, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        match U::kind() {
            IdKind::Api => self.select_api_uuid(&key.eden_uuid::<ApiUuid>(), telemetry_wrapper).await,
            IdKind::Auth => self.select_auth_uuid(&key.eden_uuid::<AuthUuid>(), telemetry_wrapper).await,
            IdKind::EdenNode => self.select_eden_node_uuid(&key.eden_uuid::<EdenNodeUuid>(), telemetry_wrapper).await,
            IdKind::Endpoint => self.select_endpoint_uuid(&key.eden_uuid::<EndpointUuid>(), telemetry_wrapper).await,
            IdKind::EndpointGroup => self.select_endpoint_group_uuid(&key.eden_uuid::<EndpointGroupUuid>(), telemetry_wrapper).await,
            IdKind::Interlay => self.select_interlay_uuid(&key.eden_uuid::<InterlayUuid>(), telemetry_wrapper).await,
            IdKind::ToolServer => Err(EpError::database("Lookup by ToolServer UUID is not yet implemented")),
            IdKind::Organization => self.select_organization_uuid(&key.eden_uuid::<OrganizationUuid>(), telemetry_wrapper).await,
            IdKind::Project => Err(EpError::database("Lookup by ProjectUuid is not supported via generic cache")),
            IdKind::Robot => self.select_robot_uuid(&key.eden_uuid::<RobotUuid>(), telemetry_wrapper).await,
            IdKind::Template => self.select_template_uuid(&key.eden_uuid::<TemplateUuid>(), telemetry_wrapper).await,
            IdKind::User => self.select_user_uuid(&key.eden_uuid::<UserUuid>(), telemetry_wrapper).await,
            IdKind::Workflow => self.select_workflow_uuid(&key.eden_uuid::<WorkflowUuid>(), telemetry_wrapper).await,
            IdKind::Policy => Err(EpError::database("Lookup by Policy UUID is not supported via cache (use ELS-specific methods)")),
        }
    }
    #[named]
    async fn invalidate(&self, key: &U, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        self.internal_cache().json_kv_del(CACHE_UUID_NAMESPACE, cache_uuid_key(key)).await?;
        Ok::<_, EpError>(())
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
mod tests {
    use crate::cache::{CacheIdFunctions, CacheUuidFunctions};
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::format::OrganizationUuid;
    use eden_core::format::UserUuid;
    use eden_core::format::cache_id::{CacheId, EndpointCacheId};
    use eden_core::format::cache_uuid::{EndpointCacheUuid, OrganizationCacheUuid};
    use eden_core::format::endpoint::EpKind;
    use eden_core::format::{CacheUuid, EdenId, EndpointId};
    use endpoint_schema::endpoint::EndpointSchema;
    use ep_core::database::schema::Table;
    use ep_core::ep::EpConfig;
    use postgres_core::config::PostgresConfig;

    #[tokio::test]
    async fn cache_uuid_set_get_invalidate() {
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
    async fn cache_uuid_setex_get_invalidate() {
        // start containers
        let db_manager = create_database_manager().await;

        let organization = OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid());

        let test_telemetry = &mut test_telemetry();

        let endpoint_schema = EndpointSchema::new(
            EndpointId::new("test_endpoint".to_string()),
            EpKind::Postgres,
            PostgresConfig::default().as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );

        let endpoint_cache_uuid = EndpointCacheUuid::new(Some(organization.clone()), endpoint_schema.uuid());

        // set endpoint
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheUuidFunctions<EndpointSchema, EndpointCacheUuid>>::set_ex(
                &db_manager,
                endpoint_cache_uuid.clone(),
                endpoint_schema.clone(),
                test_telemetry,
            )
            .await
            .is_ok()
        );

        // get endpoint
        assert_eq!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheUuidFunctions<EndpointSchema, EndpointCacheUuid>>::get_from_cache(
                &db_manager,
                &endpoint_cache_uuid,
                test_telemetry
            )
            .await
            .expect("Failed to get endpoint schema from cache")
            .endpoint_uuid(),
            endpoint_schema.uuid()
        );

        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheUuidFunctions<EndpointSchema, EndpointCacheUuid>>::invalidate(
                &db_manager,
                &endpoint_cache_uuid,
                test_telemetry
            )
            .await
            .is_ok()
        );

        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheUuidFunctions<EndpointSchema, EndpointCacheUuid>>::get_from_cache(
                &db_manager,
                &endpoint_cache_uuid,
                test_telemetry
            )
            .await
            .is_err()
        );
    }
}
