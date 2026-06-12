use crate::EdenDb;
pub mod endpoints;
pub mod organizations;
pub mod subjects;
pub mod templates;
pub mod workflows;

use database::db::cache::CacheFunctions;
use eden_core::error::{CacheError, DatabaseError, EpError, ResultEP};
use eden_core::format::EdenUuid;
use eden_core::format::cache_id::{RobotCacheId, UserCacheId};
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, RobotCacheUuid, UserCacheUuid};
use eden_core::format::rbac::{ControlPerms, DataPerms, RbacData, RbacKey};
use eden_core::format::{CacheObjectType, IdKind, OrganizationUuid, RobotId, RobotUuid, UserId, UserUuid};
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::robot::RobotSchema;
use endpoint_core::ep_core::database::schema::user::UserSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::hash::Hash;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SubjectInput {
    pub subjects: Vec<(String, ControlPerms)>,
}

impl SubjectInput {
    pub fn new(subjects: Vec<(String, ControlPerms)>) -> Self {
        Self { subjects }
    }
    pub fn subjects_as_uuid<S, U>(&self) -> ResultEP<Vec<S>>
    where
        S: CacheUuid,
        U: EdenUuid + From<Uuid>,
    {
        let mut subjects = Vec::with_capacity(self.subjects.len());
        for (subject, _) in self.subjects.iter() {
            subjects.push(S::parse::<U>(subject)?)
        }
        Ok(subjects)
    }
    pub fn subjects<S>(&self) -> Vec<String> {
        self.subjects.iter().map(|(s, _)| s.clone()).collect()
    }
    pub fn to_vec(&self) -> Vec<(String, ControlPerms)> {
        self.subjects.clone()
    }
    pub fn relations(&self) -> Vec<ControlPerms> {
        self.subjects.iter().map(|(_, r)| *r).collect::<Vec<ControlPerms>>()
    }
    pub fn required_grant_perms(&self) -> ControlPerms {
        self.subjects.iter().fold(ControlPerms::GRANT, |required, (_, perms)| required | *perms)
    }

    pub fn rbac<E, S, U>(&self, entity: E) -> ResultEP<Vec<RbacData<E, S>>>
    where
        E: RbacKey + CacheUuid + Clone + Hash + Display,
        S: CacheUuid + Clone + Hash + Display,
        U: EdenUuid + From<Uuid>,
    {
        let mut strings = Vec::with_capacity(self.subjects.len());
        for (subject, relation) in self.subjects.iter() {
            strings.push(RbacData::<E, S>::new(entity.clone(), *relation, S::parse::<U>(subject)?))
        }
        Ok(strings)
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DataSubjectInput {
    pub subjects: Vec<(String, DataPerms)>,
}

impl DataSubjectInput {
    pub fn new(subjects: Vec<(String, DataPerms)>) -> Self {
        Self { subjects }
    }

    pub fn to_vec(&self) -> Vec<(String, DataPerms)> {
        self.subjects.clone()
    }

    pub fn required_grant_perms(&self) -> ControlPerms {
        let _ = &self.subjects;
        ControlPerms::GRANT
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ControlPermInput {
    pub perms: ControlPerms,
}

impl ControlPermInput {
    pub fn required_grant_perms(&self) -> ControlPerms {
        ControlPerms::GRANT | self.perms
    }

    pub fn validate_for_put(&self) -> ResultEP<()> {
        if self.perms.is_empty() {
            Err(EpError::request("control-plane perms must not be empty; use DELETE to revoke the subject grant"))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DataPermInput {
    pub perms: DataPerms,
}

impl DataPermInput {
    pub fn required_grant_perms(&self) -> ControlPerms {
        ControlPerms::GRANT
    }

    pub fn validate_for_put(&self) -> ResultEP<()> {
        if self.perms.is_empty() {
            Err(EpError::request("data-plane perms must not be empty; use DELETE to revoke the subject grant"))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedSubject {
    pub kind: IdKind,
    pub uuid: Uuid,
}

fn is_missing_subject_error(error: &EpError) -> bool {
    matches!(
        error,
        EpError::Database(DatabaseError::UserNotFound | DatabaseError::RobotNotFound) | EpError::Cache(CacheError::KeyNotFound)
    )
}

async fn try_resolve_user_cache_uuid_for_org(
    database: &EdenDb,
    org_cache: &OrganizationCacheUuid,
    subject: &str,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<Option<UserCacheUuid>> {
    match <EdenDb as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_cache_uuid(
        database,
        &CacheObjectType::from((Some(org_cache.clone()), subject.to_string())),
        telemetry_wrapper,
    )
    .await
    {
        Ok(cache_uuid) => Ok(Some(cache_uuid)),
        Err(error) if is_missing_subject_error(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

async fn try_resolve_robot_cache_uuid_for_org(
    database: &EdenDb,
    org_cache: &OrganizationCacheUuid,
    subject: &str,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<Option<RobotCacheUuid>> {
    match <EdenDb as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::get_cache_uuid(
        database,
        &CacheObjectType::from((Some(org_cache.clone()), subject.to_string())),
        telemetry_wrapper,
    )
    .await
    {
        Ok(cache_uuid) => Ok(Some(cache_uuid)),
        Err(error) if is_missing_subject_error(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) async fn resolve_user_cache_uuid_for_org(
    database: &EdenDb,
    org_cache: &OrganizationCacheUuid,
    org_uuid: &OrganizationUuid,
    subject: &str,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<UserCacheUuid> {
    let user_schema = <EdenDb as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_from_cache(
        database,
        &CacheObjectType::from((Some(org_cache.clone()), subject.to_string())),
        telemetry_wrapper,
    )
    .await?;

    if user_schema.organization_uuid() != org_uuid {
        return Err(eden_core::error::EpError::rbac(format!("Subject '{subject}' does not belong to this organization")));
    }

    Ok(UserCacheUuid::new(Some(org_cache.clone()), user_schema.uuid()))
}

pub(crate) async fn resolve_subject_for_org(
    database: &EdenDb,
    org_cache: &OrganizationCacheUuid,
    org_uuid: &OrganizationUuid,
    subject: &str,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<ResolvedSubject> {
    let user_cache = try_resolve_user_cache_uuid_for_org(database, org_cache, subject, telemetry_wrapper).await?;
    let robot_cache = try_resolve_robot_cache_uuid_for_org(database, org_cache, subject, telemetry_wrapper).await?;

    match (user_cache, robot_cache) {
        (Some(user_cache), None) => {
            let user_schema = <EdenDb as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_from_cache(
                database,
                &CacheObjectType::new(Some(user_cache.clone()), None),
                telemetry_wrapper,
            )
            .await?;

            if user_schema.organization_uuid() != org_uuid {
                return Err(EpError::rbac(format!("Subject '{subject}' does not belong to this organization")));
            }

            Ok(ResolvedSubject { kind: IdKind::User, uuid: user_cache.uuid() })
        }
        (None, Some(robot_cache)) => {
            let robot_schema = <EdenDb as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::get_from_cache(
                database,
                &CacheObjectType::new(Some(robot_cache.clone()), None),
                telemetry_wrapper,
            )
            .await?;

            if robot_schema.organization_uuid() != org_uuid {
                return Err(EpError::rbac(format!("Subject '{subject}' does not belong to this organization")));
            }

            Ok(ResolvedSubject { kind: IdKind::Robot, uuid: robot_cache.uuid() })
        }
        (Some(_), Some(_)) => Err(EpError::request(format!(
            "Subject '{subject}' is ambiguous in this organization; use a unique human or agent identifier",
        ))),
        (None, None) => Err(EpError::rbac(format!("Subject '{subject}' does not belong to this organization"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_grant_perms_includes_grant_and_subject_bits() {
        let input = SubjectInput::new(vec![("user1".to_string(), ControlPerms::READ | ControlPerms::CONFIGURE)]);
        assert_eq!(input.required_grant_perms(), ControlPerms::GRANT | ControlPerms::READ | ControlPerms::CONFIGURE);
    }

    #[test]
    fn required_grant_perms_unions_multiple_subjects() {
        let input = SubjectInput::new(vec![
            ("user1".to_string(), ControlPerms::READ),
            ("user2".to_string(), ControlPerms::DESTROY),
        ]);
        assert_eq!(input.required_grant_perms(), ControlPerms::GRANT | ControlPerms::READ | ControlPerms::DESTROY);
    }

    #[test]
    fn required_grant_perms_empty_subjects_returns_grant() {
        let input = SubjectInput::new(vec![]);
        assert_eq!(input.required_grant_perms(), ControlPerms::GRANT);
    }

    #[test]
    fn data_subject_input_required_grant_perms_returns_grant() {
        let input = DataSubjectInput::new(vec![("user1".to_string(), DataPerms::READ | DataPerms::WRITE)]);
        assert_eq!(input.required_grant_perms(), ControlPerms::GRANT);
    }

    #[test]
    fn data_subject_input_empty_subjects_returns_grant() {
        let input = DataSubjectInput::new(vec![]);
        assert_eq!(input.required_grant_perms(), ControlPerms::GRANT);
    }

    #[test]
    fn control_perm_input_validate_for_put_rejects_empty_perms() {
        let input = ControlPermInput { perms: ControlPerms::empty() };
        assert!(input.validate_for_put().is_err());
    }

    #[test]
    fn control_perm_input_validate_for_put_accepts_non_empty_perms() {
        let input = ControlPermInput { perms: ControlPerms::READ };
        assert!(input.validate_for_put().is_ok());
    }

    #[test]
    fn data_perm_input_validate_for_put_rejects_empty_perms() {
        let input = DataPermInput { perms: DataPerms::empty() };
        assert!(input.validate_for_put().is_err());
    }

    #[test]
    fn data_perm_input_validate_for_put_accepts_non_empty_perms() {
        let input = DataPermInput { perms: DataPerms::READ };
        assert!(input.validate_for_put().is_ok());
    }
}
