use crate::db::cache::CacheFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::delete::{DeleteMethod, UuidsToUpdate};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::{OrganizationCacheId, UserCacheId};
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, UserCacheUuid};
use eden_core::format::{CacheObjectType, OrganizationId, OrganizationUuid, UserId, UserUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::organization::OrganizationSchema;
use ep_core::database::schema::user::UserSchema;
use function_name::named;

pub struct DeleteUser {
    object: CacheObjectType<UserCacheUuid, UserCacheId>,
}

impl<R, P, C> DeleteMethod<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId, R, P, C> for DeleteUser
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    fn new(object: CacheObjectType<UserCacheUuid, UserCacheId>) -> Self {
        Self { object }
    }
    async fn cache_uuid(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<UserCacheUuid> {
        <DatabaseManager<R, P, C> as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_cache_uuid(
            db,
            <Self as DeleteMethod<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId, R, P, C>>::primary_object(self),
            telemetry_wrapper,
        )
        .await
    }
    fn primary_object(&self) -> &CacheObjectType<UserCacheUuid, UserCacheId> {
        &self.object
    }
    #[named]
    async fn update_cache_relations(
        &self,
        db: &DatabaseManager<R, P, C>,
        deleted_cache_uuid: UserCacheUuid,
        uuids: &UuidsToUpdate,
        _: Option<OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        // remove template from organization
        for org_uuid in uuids.organization() {
            let org_uuid_label = org_uuid.to_string();
            let org_key = Some(OrganizationCacheUuid::new(None, org_uuid));
            let org_cache_object = CacheObjectType::new(org_key.clone(), None);

            // get mutable role object
            let mut org_schema = <DatabaseManager<R, P, C> as CacheFunctions<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
            >>::get_from_cache(db, &org_cache_object, telemetry_wrapper)
            .await?;

            // remove endpoint from eden_node object
            org_schema.remove_user_by_uuid(&deleted_cache_uuid.eden_uuid::<UserUuid>());

            if org_schema.remove_super_admin_by_uuid(&deleted_cache_uuid.eden_uuid::<UserUuid>()) {
                telemetry_wrapper.record_event(eden_core::telemetry::MetricEvent::RoleRevoked {
                    org_uuid: &org_uuid_label,
                    perms: eden_core::format::rbac::ControlPerms::all(),
                    resource: None,
                    resource_id: None,
                });
            }

            <DatabaseManager<R, P, C> as CacheFunctions<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
            >>::set_ex_cache(db, org_key, org_schema, telemetry_wrapper)
            .await?
        }

        Ok::<_, EpError>(())
    }
}

#[cfg(all(test, feature = "infra-tests", not(embedded_db)))]
pub mod tests {
    use crate::db::methods::insert::Insert;
    use crate::lib::{ClickhouseConn, PgConn, RedisConn};
    use crate::methods::delete::DeleteMethod;
    use crate::methods::delete::user::DeleteUser;
    use crate::methods::insert::user::InsertUser;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::organization_test_utils::initialize_organization;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use eden_core::auth::Password;
    use eden_core::format::cache_id::{CacheId, UserCacheId};
    use eden_core::format::cache_uuid::UserCacheUuid;
    use eden_core::format::{CacheObjectType, CacheUuid, OrganizationCacheUuid, UserId, UserUuid};
    use ep_core::database::schema::Table;
    use ep_core::database::schema::user::UserSchema;

    #[tokio::test]
    async fn delete() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, _eden_node_schema, organization_schema) = initialize_organization(&db_manager, test_telemetry).await;

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

        let org_cache_uuid = Some(CacheUuid::new(
            Some(OrganizationCacheUuid::from(organization_schema.uuid())),
            test_user_schema.uuid(),
        ));
        let user_cache_uuid = Some(UserCacheUuid::new(org_cache_uuid.clone(), test_user_schema.uuid()));
        let user_cache_id = Some(UserCacheId::new(org_cache_uuid, test_user_schema.id()));
        let object: CacheObjectType<UserCacheUuid, UserCacheId> =
            CacheObjectType::<UserCacheUuid, UserCacheId>::new(user_cache_uuid, user_cache_id);
        let delete_user =
            <DeleteUser as DeleteMethod<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId, RedisConn, PgConn, ClickhouseConn>>::new(
                object,
            );
        let removed_uuids =
            DeleteMethod::<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId, RedisConn, PgConn, ClickhouseConn>::delete_database(
                &delete_user,
                &db_manager,
                test_telemetry,
            )
            .await
            .expect("Failed to delete user");
        assert!(removed_uuids.organization().contains(&organization_schema.uuid()));

        //manually teardown containers
    }
}
