use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::insert::Insert;
use crate::db::{cache::CacheFunctions, lib::DatabaseManager};
#[cfg(not(embedded_db))]
use crate::sql_file;
use eden_core::format::cache_id::{InterlayCacheId, OrganizationCacheId};
use eden_core::format::cache_uuid::InterlayCacheUuid;
use eden_core::format::{CacheObjectType, EdenUuid, InterlayId, InterlayUuid, OrganizationId, OrganizationUuid};
use eden_core::telemetry::TelemetryWrapper;
use eden_core::{
    error::{EntityType, EpError},
    format::cache_uuid::{CacheUuid, OrganizationCacheUuid},
};
use ep_core::database::schema::Table;
use ep_core::database::schema::interlay::InterlaySchema;
use ep_core::database::schema::organization::OrganizationSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct InsertInterlay {
    org_uuid: OrganizationUuid,
    interlay_schema: InterlaySchema,
}

impl InsertInterlay {
    pub fn new(org_uuid: OrganizationUuid, interlay_schema: InterlaySchema) -> Self {
        Self { org_uuid, interlay_schema }
    }
}

impl<R, P, C> Insert<R, P, C> for InsertInterlay
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, _telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let advertise_host = self.interlay_schema.advertise_host().cloned();
        let listeners_json = serde_json::to_value(self.interlay_schema.listeners())
            .map_err(|e| EpError::database(format!("failed to serialize listeners: {e}")))?;
        let tls_json =
            serde_json::to_value(self.interlay_schema.tls()).map_err(|e| EpError::database(format!("failed to serialize tls: {e}")))?;
        let settings_json = serde_json::to_value(self.interlay_schema.settings())
            .map_err(|e| EpError::database(format!("failed to serialize settings: {e}")))?;
        let interlay_uuid = self.interlay_schema.uuid();
        let org_uuid = self.org_uuid.uuid();
        let conn = db.pg_connection().await?;

        #[cfg(embedded_db)]
        {
            const UPSERT_INTERLAY_SQLITE: &str = r#"
                INSERT INTO interlays (
                    id,
                    uuid,
                    "description",
                    endpoint,
                    port,
                    listeners,
                    advertise_host,
                    tls,
                    settings,
                    created_by,
                    updated_by,
                    created_at,
                    updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (id) DO UPDATE
                    SET endpoint = EXCLUDED.endpoint,
                        "description" = EXCLUDED.description,
                        port = EXCLUDED.port,
                        listeners = EXCLUDED.listeners,
                        advertise_host = EXCLUDED.advertise_host,
                        tls = EXCLUDED.tls,
                        settings = EXCLUDED.settings,
                        updated_by = EXCLUDED.updated_by,
                        updated_at = EXCLUDED.updated_at
            "#;

            conn.execute(
                UPSERT_INTERLAY_SQLITE,
                &[
                    &self.interlay_schema.id(),
                    &interlay_uuid,
                    &self.interlay_schema.description(),
                    &self.interlay_schema.endpoint(),
                    &(self.interlay_schema.port() as i32),
                    &listeners_json,
                    &advertise_host,
                    &tls_json,
                    &settings_json,
                    &self.interlay_schema.created_by(),
                    &self.interlay_schema.updated_by(),
                    &self.interlay_schema.created_at(),
                    &self.interlay_schema.updated_at(),
                ],
            )
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))?;

            conn.execute(
                "INSERT OR IGNORE INTO organization_interlays (organization_uuid, interlay_uuid) VALUES ($1, $2)",
                &[&org_uuid, &interlay_uuid],
            )
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))
        }

        #[cfg(not(embedded_db))]
        {
            conn.execute(
                sql_file!("insert", "interlay"),
                &[
                    &self.interlay_schema.id(),
                    &interlay_uuid,
                    &self.interlay_schema.description(),
                    &self.interlay_schema.endpoint(),
                    &(self.interlay_schema.port() as i32),
                    &listeners_json,
                    &advertise_host,
                    &tls_json,
                    &settings_json,
                    &self.interlay_schema.created_by(),
                    &self.interlay_schema.updated_by(),
                    &self.interlay_schema.created_at(),
                    &self.interlay_schema.updated_at(),
                    &org_uuid,
                ],
            )
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::Interlay))
        }
    }
    async fn insert_cache(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let org_cache_uuid = Some(OrganizationCacheUuid::new(None, self.org_uuid.clone()));

        <DatabaseManager<R, P, C> as CacheFunctions<
            InterlaySchema,
            InterlayCacheUuid,
            InterlayUuid,
            InterlayCacheId,
            InterlayId,
        >>::set_ex_cache(
            db,
            org_cache_uuid.clone(),
            self.interlay_schema.to_owned(),
            telemetry_wrapper,
        )
        .await?;

        <DatabaseManager<R, P, C> as CacheFunctions<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationUuid,
            OrganizationCacheId,
            OrganizationId,
        >>::set_ex_cache(
            db,
            org_cache_uuid.clone(),
            <DatabaseManager<R, P, C> as CacheFunctions<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
            >>::get_from_cache(db, &CacheObjectType::new(org_cache_uuid.clone(), None), telemetry_wrapper)
            .await
            .map(|mut schema| {
                schema.add_interlay(self.interlay_schema.id(), self.interlay_schema.uuid());
                schema
            })?,
            telemetry_wrapper,
        )
        .await
    }
}
