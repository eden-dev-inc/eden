use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::insert::Insert;
use crate::{
    db::{cache::CacheFunctions, lib::DatabaseManager},
    sql_file,
};
use eden_core::error::{EntityType, EpError};
use eden_core::format::cache_id::EndpointGroupCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointGroupCacheUuid, OrganizationCacheUuid};
use eden_core::format::{EdenUuid, EndpointGroupId, EndpointGroupUuid, OrganizationUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::Table;
use ep_core::database::schema::endpoint_group::EndpointGroupSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct InsertEndpointGroup {
    org_uuid: OrganizationUuid,
    endpoint_group_schema: EndpointGroupSchema,
}

impl InsertEndpointGroup {
    pub fn new(org_uuid: OrganizationUuid, endpoint_group_schema: EndpointGroupSchema) -> Self {
        Self { org_uuid, endpoint_group_schema }
    }
}

impl<R, P, C> Insert<R, P, C> for InsertEndpointGroup
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    async fn insert_database(&self, db: &DatabaseManager<R, P, C>, _telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let conn = db.pg_connection().await?;

        // Insert endpoint group and link to org
        conn.execute(
            sql_file!("insert", "endpoint_group"),
            &[
                &self.endpoint_group_schema.id(),
                &self.endpoint_group_schema.uuid(),
                &self.endpoint_group_schema.description(),
                &self.endpoint_group_schema.ep_kind().to_string(),
                &self.endpoint_group_schema.default_endpoint(),
                &self.endpoint_group_schema.created_by(),
                &self.endpoint_group_schema.updated_by(),
                &self.endpoint_group_schema.created_at(),
                &self.endpoint_group_schema.updated_at(),
                &self.org_uuid.uuid(),
            ],
        )
        .await
        .map(|_| ())
        .map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))?;

        // Insert members
        for member_uuid in self.endpoint_group_schema.members() {
            conn.execute(sql_file!("insert", "endpoint_group_member"), &[&self.endpoint_group_schema.uuid(), member_uuid])
                .await
                .map(|_| ())
                .map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))?;
        }

        Ok(())
    }

    async fn insert_cache(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let org_cache_uuid = Some(OrganizationCacheUuid::new(None, self.org_uuid.clone()));

        <DatabaseManager<R, P, C> as CacheFunctions<
            EndpointGroupSchema,
            EndpointGroupCacheUuid,
            EndpointGroupUuid,
            EndpointGroupCacheId,
            EndpointGroupId,
        >>::set_ex_cache(db, org_cache_uuid, self.endpoint_group_schema.to_owned(), telemetry_wrapper)
        .await?;

        Ok(())
    }
}
