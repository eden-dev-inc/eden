use eden_core::error::{CacheError, EpError, ResultEP};
use eden_core::format::cache_id::CacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::{CacheObjectType, EdenId, EdenUuid};
use eden_core::telemetry::TelemetryWrapper;
use function_name::named;
use std::future::Future;

mod id;
mod lib;
mod uuid;

pub trait CacheUuidFunctions<T, U> {
    /// Set value to cache
    fn set(&self, key: U, value: T, telemetry_wrapper: &mut TelemetryWrapper) -> impl Future<Output = ResultEP<()>>;
    /// Set value to cache with expiration
    fn set_ex(&self, key: U, value: T, telemetry_wrapper: &mut TelemetryWrapper) -> impl Future<Output = ResultEP<()>>;
    fn get_from_cache(&self, key: &U, telemetry_wrapper: &mut TelemetryWrapper) -> impl Future<Output = ResultEP<T>>;
    fn get_from_database(&self, key: &U, telemetry_wrapper: &mut TelemetryWrapper) -> impl Future<Output = ResultEP<T>>;
    fn invalidate(&self, key: &U, telemetry_wrapper: &mut TelemetryWrapper) -> impl Future<Output = ResultEP<()>>;
}

pub trait CacheIdFunctions<T, I> {
    /// Set value to cache
    fn set(&self, key: I, value: T, telemetry_wrapper: &mut TelemetryWrapper) -> impl Future<Output = ResultEP<()>>;
    /// Set value to cache with expiration
    fn set_ex(&self, key: I, value: T, telemetry_wrapper: &mut TelemetryWrapper) -> impl Future<Output = ResultEP<()>>;
    fn get_from_cache(&self, key: &I, telemetry_wrapper: &mut TelemetryWrapper) -> impl Future<Output = ResultEP<T>>;
    fn get_from_database(&self, key: &I, telemetry_wrapper: &mut TelemetryWrapper) -> impl Future<Output = ResultEP<T>>;
    fn invalidate(&self, key: &I, telemetry_wrapper: &mut TelemetryWrapper) -> impl Future<Output = ResultEP<()>>;
}

pub trait CacheFunctions<T, U, EU, I, EI>
where
    U: CacheUuid,
    EU: EdenUuid,
    I: CacheId,
    EI: EdenId,
{
    /// Get data from the cache by Key or Pointer, and if the cache misses data is pulled from the database
    fn get_from_cache(
        &self,
        cache_object: &CacheObjectType<U, I>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<T, EpError>>;

    #[named]
    fn get_id(
        &self,
        object: &CacheObjectType<U, I>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<EI, EpError>> {
        async move {
            let _ = telemetry_wrapper.client_tracer(function_name!().to_string());

            Ok(EI::new(self.get_cache_id(object, telemetry_wrapper).await?.id()))
        }
    }
    #[named]
    fn get_uuid(
        &self,
        object: &CacheObjectType<U, I>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<EU, EpError>> {
        async move {
            let _ = telemetry_wrapper.client_tracer(function_name!().to_string());

            Ok(EU::new(self.get_cache_uuid(object, telemetry_wrapper).await?.uuid()))
        }
    }
    #[named]
    fn get_id_and_uuid(
        &self,
        object: &CacheObjectType<U, I>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<(EI, EU), EpError>> {
        async move {
            let _ = telemetry_wrapper.client_tracer(function_name!().to_string());

            Ok((
                EI::new(self.get_cache_id(object, telemetry_wrapper).await?.id()),
                EU::new(self.get_cache_uuid(object, telemetry_wrapper).await?.uuid()),
            ))
        }
    }
    #[named]
    fn get_cache_id(
        &self,
        object: &CacheObjectType<U, I>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<I, EpError>> {
        async move {
            let _ = telemetry_wrapper.client_tracer(function_name!().to_string());

            if let Some(id) = object.id() {
                Ok(id.to_owned())
            } else if let Some(uuid) = object.uuid() {
                <Self as CacheFunctions<T, U, EU, I, EI>>::get_id_from_uuid(self, uuid, telemetry_wrapper).await
            } else {
                Err(EpError::Cache(CacheError::NoKeyProvided))
            }
        }
    }

    #[named]
    fn get_cache_uuid(
        &self,
        object: &CacheObjectType<U, I>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<U, EpError>> {
        async move {
            let _ = telemetry_wrapper.client_tracer(function_name!().to_string());
            if let Some(uuid) = object.uuid() {
                Ok(uuid.to_owned())
            } else if let Some(id) = object.id() {
                <Self as CacheFunctions<T, U, EU, I, EI>>::get_uuid_from_id(self, id, telemetry_wrapper).await
            } else {
                Err(EpError::Cache(CacheError::NoKeyProvided))
            }
        }
    }
    //
    // fn get_key_from_cache_object(
    //     &self,
    //     object: &CacheObjectType<U, I>,
    //     telemetry_wrapper: &mut TelemetryWrapper,
    // ) -> impl Future<Output = anyhow::Result<U, EpError>>;
    //
    // fn get_pointer_from_cache_object(
    //     &self,
    //     object: &CacheObjectType<U, I>,
    //     telemetry_wrapper: &mut TelemetryWrapper,
    // ) -> impl Future<Output = anyhow::Result<P, EpError>>;

    /// Get key from pointer
    fn get_uuid_from_id(
        &self,
        pointer_object: &I,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = anyhow::Result<U, EpError>>;

    /// Get key from pointer
    fn get_id_from_uuid(
        &self,
        key_object: &U,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = anyhow::Result<I, EpError>>;

    /// Get from data, and update cache
    fn get_from_database(
        &self,
        cache_object: &CacheObjectType<U, I>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = anyhow::Result<T, EpError>>;

    /// Set data to cache with expiration
    fn set_ex_cache(
        &self,
        org: Option<OrganizationCacheUuid>,
        value: T,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = anyhow::Result<(), EpError>>;

    /// Remove data from cache
    fn invalidate(
        &self,
        cache_object: &CacheObjectType<U, I>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<(U, I)>>;

    /// Database and cache health check
    fn health_check(&self) -> impl Future<Output = anyhow::Result<bool, EpError>>;

    // /// Parse input into key or pointer
    // fn parse_key(
    //     &self,
    //     org_input: Option<String>,
    //     sub_input: String,
    // ) -> impl Future<Output = anyhow::Result<CacheObjectType<U, I>, EpError>>;
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
mod tests {
    use crate::cache::CacheFunctions;
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::format::UserUuid;
    use eden_core::format::cache_id::{CacheId, EndpointCacheId};
    use eden_core::format::cache_uuid::{EndpointCacheUuid, OrganizationCacheUuid};
    use eden_core::format::endpoint::EpKind;
    use eden_core::format::{CacheObjectType, CacheUuid, EdenId, EndpointId};
    use eden_core::format::{EndpointUuid, OrganizationUuid};
    use endpoint_schema::endpoint::EndpointSchema;
    use ep_core::database::schema::Table;
    use ep_core::ep::EpConfig;
    use mongo_core::config::MongoConfig;

    #[tokio::test]
    async fn cache_set_get_invalidate() {
        // start containers
        let db_manager = create_database_manager().await;

        let organization_cache = OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid());

        let test_telemetry = &mut test_telemetry();

        let endpoint_id = EndpointId::new("test_endpoint".to_string());

        let endpoint_schema = EndpointSchema::new(
            endpoint_id.clone(),
            EpKind::Mongo,
            MongoConfig::default().as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );

        let endpoint_cache_id = EndpointCacheId::new(Some(organization_cache.clone()), endpoint_id.clone());
        let endpoint_cache_uuid = EndpointCacheUuid::new(Some(organization_cache.clone()), endpoint_schema.uuid());

        // set endpoint
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::set_ex_cache(&db_manager, Some(organization_cache.clone()), endpoint_schema.clone(), test_telemetry,)
            .await
            .is_ok()
        );

        // get endpoint
        assert_eq!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::get_from_cache(&db_manager, &CacheObjectType::new(Some(endpoint_cache_uuid.clone()), None), test_telemetry)
            .await
            .expect("Failed to get endpoint schema from cache")
            .endpoint_uuid(),
            endpoint_schema.uuid()
        );
        assert_eq!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::get_from_cache(&db_manager, &CacheObjectType::new(None, Some(endpoint_cache_id)), test_telemetry)
            .await
            .expect("Failed to get endpoint schema from cache")
            .endpoint_uuid(),
            endpoint_schema.uuid()
        );

        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::invalidate(&db_manager, &CacheObjectType::new(Some(endpoint_cache_uuid), None), test_telemetry)
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn cache_setex_get_invalidate() {
        // start containers
        let db_manager = create_database_manager().await;

        let organization_cache = OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid());

        let test_telemetry = &mut test_telemetry();

        let endpoint_id = EndpointId::new("test_endpoint".to_string());

        let endpoint_schema = EndpointSchema::new(
            endpoint_id.clone(),
            EpKind::Mongo,
            MongoConfig::default().as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );

        let endpoint_cache_id = EndpointCacheId::new(Some(organization_cache.clone()), endpoint_id.clone());
        let endpoint_cache_uuid = EndpointCacheUuid::new(Some(organization_cache.clone()), endpoint_schema.uuid());

        // set endpoint
        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::set_ex_cache(&db_manager, Some(organization_cache.clone()), endpoint_schema.clone(), test_telemetry,)
            .await
            .is_ok()
        );

        // get endpoint
        assert_eq!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::get_from_cache(&db_manager, &CacheObjectType::new(Some(endpoint_cache_uuid.clone()), None), test_telemetry)
            .await
            .expect("Failed to get endpoint schema from cache")
            .endpoint_uuid(),
            endpoint_schema.uuid()
        );
        assert_eq!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::get_from_cache(&db_manager, &CacheObjectType::new(None, Some(endpoint_cache_id)), test_telemetry)
            .await
            .expect("Failed to get endpoint schema from cache")
            .endpoint_uuid(),
            endpoint_schema.uuid()
        );

        assert!(
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::invalidate(&db_manager, &CacheObjectType::new(Some(endpoint_cache_uuid), None), test_telemetry)
            .await
            .is_ok()
        );
    }
}
