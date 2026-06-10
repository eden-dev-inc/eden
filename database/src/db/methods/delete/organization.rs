use crate::db::cache::CacheFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::delete::{DeleteMethod, UuidsToUpdate};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::{
    ApiCacheId, AuthCacheId, EndpointCacheId, InterlayCacheId, OrganizationCacheId, RobotCacheId, TemplateCacheId, UserCacheId,
    WorkflowCacheId,
};
use eden_core::format::cache_uuid::{
    ApiCacheUuid, AuthCacheUuid, CacheUuid, EndpointCacheUuid, InterlayCacheUuid, OrganizationCacheUuid, RobotCacheUuid, TemplateCacheUuid,
    UserCacheUuid, WorkflowCacheUuid,
};
use eden_core::format::{
    ApiId, ApiUuid, AuthId, AuthUuid, CacheObjectType, EndpointId, EndpointUuid, InterlayId, InterlayUuid, OrganizationId,
    OrganizationUuid, RobotId, RobotUuid, TemplateId, TemplateUuid, UserId, UserUuid, WorkflowId, WorkflowUuid,
};
use eden_core::telemetry::FastSpanStatus;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_schema::endpoint::EndpointSchema;
use ep_core::database::schema::api::ApiSchema;
use ep_core::database::schema::auth::AuthSchema;
use ep_core::database::schema::interlay::InterlaySchema;
use ep_core::database::schema::organization::OrganizationSchema;
use ep_core::database::schema::robot::RobotSchema;
use ep_core::database::schema::template::TemplateSchema;
use ep_core::database::schema::user::UserSchema;
use ep_core::database::schema::workflow::WorkflowSchema;
use function_name::named;
use std::borrow::Cow;

pub struct DeleteOrganization {
    object: CacheObjectType<OrganizationCacheUuid, OrganizationCacheId>,
}

impl<R, P, C> DeleteMethod<OrganizationSchema, OrganizationCacheUuid, OrganizationUuid, OrganizationCacheId, OrganizationId, R, P, C>
    for DeleteOrganization
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    fn new(object: CacheObjectType<OrganizationCacheUuid, OrganizationCacheId>) -> Self {
        Self { object }
    }
    async fn cache_uuid(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<OrganizationCacheUuid> {
        <DatabaseManager<R, P, C> as CacheFunctions<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationUuid,
            OrganizationCacheId,
            OrganizationId,
        >>::get_cache_uuid(
            db,
            <Self as DeleteMethod<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
                R,
                P,
                C,
            >>::primary_object(self),
            telemetry_wrapper,
        )
        .await
    }
    fn primary_object(&self) -> &CacheObjectType<OrganizationCacheUuid, OrganizationCacheId> {
        &self.object
    }
    #[named]
    async fn update_cache_relations(
        &self,
        db: &DatabaseManager<R, P, C>,
        deleted_uuid: OrganizationCacheUuid,
        uuids: &UuidsToUpdate,
        org_key: Option<OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        // remove api
        for api_uuid in uuids.api() {
            <DatabaseManager<R, P, C> as CacheFunctions<ApiSchema, ApiCacheUuid, ApiUuid, ApiCacheId, ApiId>>::invalidate(
                db,
                &CacheObjectType::new(Some(ApiCacheUuid::new(org_key.clone(), api_uuid)), None),
                telemetry_wrapper,
            )
            .await
            .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?;
        }

        // remove auth
        for auth_uuid in uuids.auth() {
            <DatabaseManager<R, P, C> as CacheFunctions<AuthSchema, AuthCacheUuid, AuthUuid, AuthCacheId, AuthId>>::invalidate(
                db,
                &CacheObjectType::new(Some(AuthCacheUuid::new(Some(deleted_uuid.clone()), auth_uuid)), None),
                telemetry_wrapper,
            )
            .await
            .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?;
        }

        // remove endpoint
        for endpoint_uuid in uuids.endpoint() {
            <DatabaseManager<R, P, C> as CacheFunctions<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::invalidate(
                db,
                &CacheObjectType::new(
                    Some(EndpointCacheUuid::new(org_key.clone(), endpoint_uuid)),
                    None,
                ),
                telemetry_wrapper,
            )
            .await
            .inspect_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()),
                })
            })?;
        }

        // remove template
        for template_uuid in uuids.template() {
            <DatabaseManager<R, P, C> as CacheFunctions<
                TemplateSchema,
                TemplateCacheUuid,
                TemplateUuid,
                TemplateCacheId,
                TemplateId,
            >>::invalidate(
                db,
                &CacheObjectType::new(
                    Some(TemplateCacheUuid::new(org_key.clone(), template_uuid)),
                    None,
                ),
                telemetry_wrapper,
            )
            .await
            .inspect_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()),
                })
            })?;
        }

        // remove interlay
        for interlay_uuid in uuids.interlay() {
            <DatabaseManager<R, P, C> as CacheFunctions<
                InterlaySchema,
                InterlayCacheUuid,
                InterlayUuid,
                InterlayCacheId,
                InterlayId,
            >>::invalidate(
                db,
                &CacheObjectType::new(
                    Some(InterlayCacheUuid::new(org_key.clone(), interlay_uuid)),
                    None,
                ),
                telemetry_wrapper,
            )
            .await
            .inspect_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()),
                })
            })?;
        }

        // remove robot
        for robot_uuid in uuids.robot() {
            <DatabaseManager<R, P, C> as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::invalidate(
                db,
                &CacheObjectType::new(Some(RobotCacheUuid::new(org_key.clone(), robot_uuid)), None),
                telemetry_wrapper,
            )
            .await
            .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?;
        }

        // remove user
        for user_uuid in uuids.user() {
            <DatabaseManager<R, P, C> as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::invalidate(
                db,
                &CacheObjectType::new(Some(UserCacheUuid::new(org_key.clone(), user_uuid)), None),
                telemetry_wrapper,
            )
            .await
            .inspect_err(|e| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))?;
        }

        // remove workflows
        for workflow_uuid in uuids.workflow() {
            <DatabaseManager<R, P, C> as CacheFunctions<
                WorkflowSchema,
                WorkflowCacheUuid,
                WorkflowUuid,
                WorkflowCacheId,
                WorkflowId,
            >>::invalidate(
                db,
                &CacheObjectType::new(
                    Some(WorkflowCacheUuid::new(org_key.clone(), workflow_uuid)),
                    None,
                ),
                telemetry_wrapper,
            )
            .await
            .inspect_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()),
                })
            })?;
        }

        Ok(())
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub mod tests {
    use crate::db::methods::insert::Insert;
    use crate::lib::{ClickhouseConn, PgConn, RedisConn};
    use crate::methods::delete::DeleteMethod;
    use crate::methods::delete::organization::DeleteOrganization;
    use crate::methods::insert::user::InsertUser;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::organization_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::auth::Password;
    use eden_core::format::cache_id::{CacheId, OrganizationCacheId, UserCacheId};
    use eden_core::format::cache_uuid::UserCacheUuid;
    use eden_core::format::{CacheObjectType, CacheUuid, OrganizationCacheUuid, OrganizationId, OrganizationUuid, UserId};
    use ep_core::database::schema::Table;
    use ep_core::database::schema::organization::OrganizationSchema;
    use ep_core::database::schema::user::UserSchema;

    #[tokio::test]
    async fn delete() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let (user_schema, _eden_node_schema, organization_schema) = initialize_organization(&db_manager, test_telemetry).await;

        // test user
        let test_user_schema = UserSchema::new(
            UserId::from("test_user"),
            Password::new("password".to_string()),
            organization_schema.uuid(),
            None,
            None,
            None,
        );

        let insert_user = InsertUser::new(test_user_schema.clone());
        insert_user.insert_database(&db_manager, test_telemetry).await.unwrap_or_default();

        // Delete organization and check if the user's UUID was returned
        let org_cache_uuid = Some(CacheUuid::new(
            Some(OrganizationCacheUuid::from(organization_schema.uuid())),
            organization_schema.uuid(),
        ));
        let org_cache_id = Some(CacheId::new(org_cache_uuid.clone(), organization_schema.id()));

        let object: CacheObjectType<OrganizationCacheUuid, OrganizationCacheId> =
            CacheObjectType::<OrganizationCacheUuid, OrganizationCacheId>::new(org_cache_uuid.clone(), org_cache_id);

        let delete_org = <DeleteOrganization as DeleteMethod<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationUuid,
            OrganizationCacheId,
            OrganizationId,
            RedisConn,
            PgConn,
            ClickhouseConn,
        >>::new(object);

        let user_cache_uuid = Some(UserCacheUuid::new(org_cache_uuid.clone(), test_user_schema.uuid()));
        let user_cache_id = Some(UserCacheId::new(org_cache_uuid, test_user_schema.id()));
        let _object: CacheObjectType<UserCacheUuid, UserCacheId> =
            CacheObjectType::<UserCacheUuid, UserCacheId>::new(user_cache_uuid, user_cache_id);
        let removed_uuids = DeleteMethod::<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationUuid,
            OrganizationCacheId,
            OrganizationId,
            RedisConn,
            PgConn,
            ClickhouseConn,
        >::delete_database(&delete_org, &db_manager, test_telemetry)
        .await
        .unwrap_or_default();
        assert!(removed_uuids.user().contains(&user_schema.uuid()));
        assert!(removed_uuids.user().contains(&test_user_schema.uuid()));

        //manually teardown containers
    }
}
