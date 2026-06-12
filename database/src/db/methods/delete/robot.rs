use crate::db::cache::CacheFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::delete::{DeleteMethod, UuidsToUpdate};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::RobotCacheId;
use eden_core::format::cache_uuid::{OrganizationCacheUuid, RobotCacheUuid};
use eden_core::format::{CacheObjectType, RobotId, RobotUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::robot::RobotSchema;

pub struct DeleteRobot {
    object: CacheObjectType<RobotCacheUuid, RobotCacheId>,
}

impl<R, P, C> DeleteMethod<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId, R, P, C> for DeleteRobot
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    fn new(object: CacheObjectType<RobotCacheUuid, RobotCacheId>) -> Self {
        Self { object }
    }
    async fn cache_uuid(&self, db: &DatabaseManager<R, P, C>, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<RobotCacheUuid> {
        <DatabaseManager<R, P, C> as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::get_cache_uuid(
            db,
            <Self as DeleteMethod<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId, R, P, C>>::primary_object(self),
            telemetry_wrapper,
        )
        .await
    }
    fn primary_object(&self) -> &CacheObjectType<RobotCacheUuid, RobotCacheId> {
        &self.object
    }
    async fn update_cache_relations(
        &self,
        _db: &DatabaseManager<R, P, C>,
        _deleted_cache_uuid: RobotCacheUuid,
        _uuids: &UuidsToUpdate,
        _: Option<OrganizationCacheUuid>,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        // Robots don't have nested cache relations to update beyond what RBAC handles
        Ok(())
    }
}
