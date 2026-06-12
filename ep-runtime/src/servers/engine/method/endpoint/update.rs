use crate::comp::MyEngineService;
use borsh::{BorshDeserialize, BorshSerialize};
use database::db::cache::CacheFunctions;
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use database::db::methods::update::{UpdateActor, UpdateEndpoint, UpdateMethod};
use database::endpoint_schema::endpoint::EndpointSchema;
use eden_core::error::{ConnectError, EpError, ResultEP};
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::EndpointCacheUuid;
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid};
use eden_core::telemetry::TelemetryWrapper;
use endpoint::metadata::{EpMetadata, SyncMetadata};
use endpoint::{EP, EpRequest, RunRequest};
use ep_core::EndpointType;
use ep_core::ep::{EpConfig, RWPool};
use function_name::named;
use utoipa::ToSchema;

impl MyEngineService {
    #[named]
    pub async fn update_connection<
        A: Clone + Send + Sync + 'static,
        E: EP<A, C, Req, M, K, Tx> + 'static,
        Req: EpRequest + EndpointType + RunRequest<A, K, Tx> + BorshDeserialize + 'static,
        M: EpMetadata + SyncMetadata<A> + Clone + 'static,
        C: EpConfig + BorshDeserialize + BorshSerialize + RWPool<A> + Clone + ToSchema + 'static,
        K: 'static,
        Tx: 'static,
    >(
        &self,
        database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        endpoint_cache_object: &CacheObjectType<EndpointCacheUuid, EndpointCacheId>,
        update_endpoint: UpdateEndpoint,
        updated_by: UpdateActor<'_>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<String> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());

        // get database kind (needed for collecting route)
        let kind = match update_endpoint.kind() {
            Some(kind) => kind,
            None => return Err(EpError::connect("no updated connection provided")),
        };

        let endpoint_cache_uuid = <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointUuid,
            EndpointCacheId,
            EndpointId,
        >>::get_cache_uuid(database, endpoint_cache_object, telemetry_wrapper)
        .await?;

        // get write lock to router
        let mut lock = self.router.write().await;
        let route = match lock.get_mut(&kind) {
            Some(route) => route,
            None => Err(EpError::connect("no route to update"))?,
        };

        // old endpoint
        let ep = match route.any_mut().downcast_mut::<E>() {
            Some(ep) => ep,
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastRouter)),
        };

        // get the configuration data from the database
        let config = match <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointUuid,
            EndpointCacheId,
            EndpointId,
        >>::get_from_cache(database, endpoint_cache_object, telemetry_wrapper)
        .await?
        .config()
        .as_mut_any()
        .downcast_mut::<C>()
        {
            Some(config) => {
                if let Some(read) = update_endpoint.read_conn() {
                    let read = Box::new(read.clone());
                    config.update_read_conn(read.as_connection())?;
                }
                if let Some(write) = update_endpoint.write_conn() {
                    let write = Box::new(write.clone());
                    config.update_write_conn(write.as_connection())?;
                }
                config.clone_box()
            }

            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };

        // disconnect current endpoint
        ep.disconnect_async(&endpoint_cache_uuid, telemetry_wrapper).await?;

        // update connections
        // create synchronous connection to endpoint
        ep.connect_async(&endpoint_cache_uuid, config.clone(), telemetry_wrapper).await?;

        span.add_simple_event("connected to new async connection");

        // Update database cache
        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as UpdateMethod<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointUuid,
            EndpointCacheId,
            EndpointId,
        >>::update_endpoint_config(database, endpoint_cache_object, config, updated_by, telemetry_wrapper)
        .await?;

        Ok("Success".to_string())
    }
}
