use super::decode_schema_row;
use crate::db::lib::{EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::{db::lib::DatabaseManager, sql_file};
use chrono::Utc;
use eden_core::error::{EntityType, EpError, ResultEP};
use eden_core::format::{EndpointGroupId, EndpointGroupUuid, EndpointUuid, OrganizationUuid, UserUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::endpoint_group::{EndpointGroupSchema, EndpointGroupSchemaIds};
use ep_core::database::schema::{FromRow, Table};

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Select endpoint group by UUID
    pub async fn select_endpoint_group_uuid<T>(
        &self,
        endpoint_group_uuid: &EndpointGroupUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "endpoint_group/endpoint_group_uuid"), &[endpoint_group_uuid])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))?,
        )
        .map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))
    }

    /// Select endpoint group by ID
    pub async fn select_endpoint_group_id<T>(
        &self,
        endpoint_group_id: &EndpointGroupId,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<T>
    where
        T: FromRow,
    {
        let conn = self.pg_connection().await?;

        decode_schema_row(
            conn.query_one(sql_file!("select", "endpoint_group/endpoint_group_id"), &[endpoint_group_id])
                .await
                .map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))?,
        )
        .map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))
    }

    /// Select all endpoint groups for an organization (lightweight IDs view)
    pub async fn select_all_endpoint_groups_ids(
        &self,
        org_uuid: &OrganizationUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<EndpointGroupSchemaIds>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "endpoint_group/endpoint_groups"), &[org_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))?
        {
            schemas.push(decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))?);
        }

        Ok(schemas)
    }

    /// Select all endpoint groups for an organization (full schema)
    pub async fn select_all_endpoint_groups(
        &self,
        org_uuid: &OrganizationUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<EndpointGroupSchema>> {
        let conn = self.pg_connection().await?;

        let mut schemas = vec![];

        for row in conn
            .query(sql_file!("select", "endpoint_group/endpoint_groups"), &[org_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))?
        {
            let mut schema: EndpointGroupSchema =
                decode_schema_row(row).map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))?;
            // Load members separately
            let members = self.select_endpoint_group_members(&schema.uuid(), telemetry_wrapper).await?;
            schema.set_members(members);
            schemas.push(schema);
        }

        Ok(schemas)
    }

    /// Select endpoint group members (endpoint UUIDs) for a given group
    pub async fn select_endpoint_group_members(
        &self,
        endpoint_group_uuid: &EndpointGroupUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<EndpointUuid>> {
        let conn = self.pg_connection().await?;

        let mut members = vec![];

        for row in &conn
            .query(sql_file!("select", "endpoint_group/endpoint_group_members"), &[endpoint_group_uuid])
            .await
            .map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))?
        {
            members.push(row.try_get("endpoint_uuid").map_err(EpError::database)?);
        }

        Ok(members)
    }

    /// Update endpoint group metadata
    pub async fn update_endpoint_group(
        &self,
        endpoint_group_uuid: &EndpointGroupUuid,
        id: Option<&str>,
        description: Option<&str>,
        default_endpoint: Option<&EndpointUuid>,
        updated_by: &UserUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let now = Utc::now();
        self.pg_connection()
            .await?
            .execute(
                sql_file!("update", "endpoint_group"),
                &[endpoint_group_uuid, &id, &description, &default_endpoint, updated_by, &now],
            )
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))
    }

    /// Add a member endpoint to an endpoint group
    pub async fn insert_endpoint_group_member(
        &self,
        endpoint_group_uuid: &EndpointGroupUuid,
        endpoint_uuid: &EndpointUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        self.pg_connection()
            .await?
            .execute(sql_file!("insert", "endpoint_group_member"), &[endpoint_group_uuid, endpoint_uuid])
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))
    }

    /// Remove a member endpoint from an endpoint group
    pub async fn delete_endpoint_group_member(
        &self,
        endpoint_group_uuid: &EndpointGroupUuid,
        endpoint_uuid: &EndpointUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        self.pg_connection()
            .await?
            .execute(sql_file!("delete", "endpoint_group_member"), &[endpoint_group_uuid, endpoint_uuid])
            .await
            .map(|_| ())
            .map_err(|e| EpError::database_query_error(e, EntityType::EndpointGroup))
    }
}
