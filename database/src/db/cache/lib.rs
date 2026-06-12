use crate::db::cache::{CacheFunctions, CacheIdFunctions, CacheUuidFunctions};
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use anyhow::Result;
use eden_core::error::{CacheError, EpError, ResultEP};
use eden_core::format::cache_id::CacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::{CacheObjectType, EdenId, EdenUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::*;
use function_name::named;
use serde::Serialize;
use serde::de::DeserializeOwned;

impl<T, U, EU, I, EI, R, P, C> CacheFunctions<T, U, EU, I, EI> for DatabaseManager<R, P, C>
where
    T: Table + FromRow + Serialize + DeserializeOwned,
    U: CacheUuid + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
    EU: EdenUuid,
    I: CacheId + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
    EI: EdenId,
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    #[named]
    async fn get_from_cache(&self, cache_object: &CacheObjectType<U, I>, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        if let Some(uuid) = cache_object.uuid() {
            <Self as CacheUuidFunctions<T, U>>::get_from_cache(self, uuid, telemetry_wrapper).await
        } else if let Some(id) = cache_object.id() {
            <Self as CacheIdFunctions<T, I>>::get_from_cache(self, id, telemetry_wrapper).await
        } else {
            Err(EpError::Cache(CacheError::NoKeyProvided))
        }
    }

    #[named]
    async fn get_uuid_from_id(&self, id: &I, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<U> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        <Self as CacheIdFunctions<T, I>>::get_from_cache(self, id, telemetry_wrapper)
            .await
            .map(|value| U::new(id.org(), value.uuid()))
    }
    #[named]
    async fn get_id_from_uuid(&self, uuid: &U, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<I> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as CacheUuidFunctions<T, U>>::get_from_cache(self, uuid, telemetry_wrapper)
            .await
            .map(|value| I::new(uuid.org(), value.id()))
    }
    #[named]
    async fn get_from_database(&self, cache_object: &CacheObjectType<U, I>, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<T> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        if let Some(uuid) = cache_object.uuid() {
            <Self as CacheUuidFunctions<T, U>>::get_from_database(self, uuid, telemetry_wrapper).await
        } else if let Some(id) = cache_object.id() {
            <Self as CacheIdFunctions<T, I>>::get_from_database(self, id, telemetry_wrapper).await
        } else {
            Err(EpError::Cache(CacheError::NoKeyProvided))
        }
    }
    #[named]
    async fn set_ex_cache(
        &self,
        org: Option<OrganizationCacheUuid>,
        value: T,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        <Self as CacheIdFunctions<T, I>>::set_ex(self, I::new(org.clone(), value.id()), value.clone(), telemetry_wrapper).await?;

        <Self as CacheUuidFunctions<T, U>>::set_ex(self, U::new(org, value.uuid()), value, telemetry_wrapper).await
    }
    #[named]
    async fn invalidate(&self, cache_object: &CacheObjectType<U, I>, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<(U, I)> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        if let Some(uuid) = cache_object.uuid() {
            let id = match cache_object.id() {
                Some(id) => id,
                None => &<Self as CacheFunctions<T, U, EU, I, EI>>::get_id_from_uuid(self, uuid, telemetry_wrapper).await?,
            };

            <Self as CacheIdFunctions<T, I>>::invalidate(self, id, telemetry_wrapper).await?;
            <Self as CacheUuidFunctions<T, U>>::invalidate(self, uuid, telemetry_wrapper).await?;

            Ok((uuid.to_owned(), id.to_owned()))
        } else if let Some(id) = cache_object.id() {
            let uuid = match cache_object.uuid() {
                Some(uuid) => uuid,
                None => &<Self as CacheFunctions<T, U, EU, I, EI>>::get_uuid_from_id(self, id, telemetry_wrapper).await?,
            };

            <Self as CacheIdFunctions<T, I>>::invalidate(self, id, telemetry_wrapper).await?;
            <Self as CacheUuidFunctions<T, U>>::invalidate(self, uuid, telemetry_wrapper).await?;

            Ok((uuid.to_owned(), id.to_owned()))
        } else {
            Err(EpError::cache("No identifier found"))
        }
    }
    #[cfg(not(embedded_db))]
    async fn health_check(&self) -> Result<bool, EpError> {
        let postgres_health = self.pg_connection().await.is_ok();
        let clickhouse_health = self.clickhouse_connection().await.is_ok();
        Ok(postgres_health && clickhouse_health)
    }

    #[cfg(embedded_db)]
    async fn health_check(&self) -> Result<bool, EpError> {
        let postgres_health = self.pg_connection().await.is_ok();
        let clickhouse_health = self.clickhouse_connection().await.is_ok();
        Ok(postgres_health && clickhouse_health)
    }
}
