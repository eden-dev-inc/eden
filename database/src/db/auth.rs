use crate::db::cache::CacheFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use eden_core::auth::auth::{JwToken, ParsedJwt};
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::{RobotCacheId, UserCacheId};
use eden_core::format::cache_uuid::{RobotCacheUuid, UserCacheUuid};
use eden_core::format::{CacheObjectType, OrganizationId, OrganizationUuid, RobotId, RobotUuid, UserId, UserUuid};
use eden_core::telemetry::TelemetryWrapper;
use ep_core::database::schema::robot::RobotSchema;
use ep_core::database::schema::user::UserSchema;

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    ///Verify the User by Password
    // #[telemetry_with_error]
    pub async fn verify_auth(
        &self,
        user_cache_object: &CacheObjectType<UserCacheUuid, UserCacheId>,
        password: String,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<bool> {
        <DatabaseManager<R, P, C> as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_from_cache(
            self,
            user_cache_object,
            telemetry_wrapper,
        )
        .await
        .map(|schema| schema.verify_password(password))
    }

    pub fn create_token(
        &self,
        user_id: &UserId,
        user_uuid: &UserUuid,
        org_id: &OrganizationId,
        org_uuid: &OrganizationUuid,
    ) -> ResultEP<JwToken> {
        self.jwt
            .as_ref()
            .map(|j| j.create(user_id, user_uuid, org_id, org_uuid))
            .ok_or_else(|| EpError::auth("JWT signing is not configured"))
    }

    /// Create a JWT token and return both the token and the jti (JWT ID).
    /// The jti can be used to record the session for later revocation.
    pub fn create_token_with_jti(
        &self,
        user_id: &UserId,
        user_uuid: &UserUuid,
        org_id: &OrganizationId,
        org_uuid: &OrganizationUuid,
    ) -> ResultEP<(JwToken, String)> {
        self.jwt
            .as_ref()
            .map(|j| j.create_with_jti(user_id, user_uuid, org_id, org_uuid))
            .ok_or_else(|| EpError::auth("JWT signing is not configured"))
    }

    pub fn validate_token(&self, token_str: &JwToken) -> Result<ParsedJwt, EpError> {
        match self.jwt.as_ref() {
            Some(j) => j.validate(token_str),
            None => Err(EpError::auth("JWT validation was not initialized")),
        }
    }

    /// Verify a robot's API key against the cached/stored robot schema.
    pub async fn verify_robot_auth(
        &self,
        robot_cache_object: &CacheObjectType<RobotCacheUuid, RobotCacheId>,
        api_key: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<bool> {
        let schema: RobotSchema =
            <DatabaseManager<R, P, C> as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::get_from_cache(
                self,
                robot_cache_object,
                telemetry_wrapper,
            )
            .await?;

        if schema.is_expired() {
            return Err(EpError::auth("Robot API key has expired"));
        }

        Ok(schema.verify_api_key(api_key))
    }

    /// Create a JWT token for a robot.
    pub fn create_robot_token(
        &self,
        robot_id: &RobotId,
        robot_uuid: &RobotUuid,
        org_id: &OrganizationId,
        org_uuid: &OrganizationUuid,
    ) -> ResultEP<JwToken> {
        self.jwt
            .as_ref()
            .map(|j| j.create_robot(robot_id, robot_uuid, org_id, org_uuid))
            .ok_or_else(|| EpError::auth("JWT signing is not configured"))
    }

    /// Create a robot JWT token and return both the token and the jti (JWT ID).
    /// The jti can be used to record the session for later revocation.
    pub fn create_robot_token_with_jti(
        &self,
        robot_id: &RobotId,
        robot_uuid: &RobotUuid,
        org_id: &OrganizationId,
        org_uuid: &OrganizationUuid,
    ) -> ResultEP<(JwToken, String)> {
        self.jwt
            .as_ref()
            .map(|j| j.create_robot_with_jti(robot_id, robot_uuid, org_id, org_uuid))
            .ok_or_else(|| EpError::auth("JWT signing is not configured"))
    }
}

// Mock implementation for testing
cfg_if::cfg_if! {
    if #[cfg(all(test, feature = "infra-tests", embedded_db))] {
        pub mod mocks {}
    } else if #[cfg(all(test, feature = "infra-tests"))] {
        #[path = "auth_mocks.rs"]
        pub mod mocks;
    }
}
