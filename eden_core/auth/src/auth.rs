use std::{collections::HashMap, fmt::Display, ops::Deref};

use borsh::{BorshDeserialize, BorshSerialize};
use error::{AuthError, EpError, RequestError};
use format::{EdenUuid, OrganizationId, OrganizationUuid, RobotId, RobotUuid, UserId, UserUuid, rbac::ControlPerms};

use opentelemetry::{global::BoxedSpan, trace::Span};
use serde::{Deserialize, Serialize};

/// Identifies whether a JWT subject is a human user or a machine account (robot).
#[derive(Debug, Serialize, Deserialize, BorshDeserialize, BorshSerialize, Clone, PartialEq, Eq, Default)]
pub enum SubjectType {
    #[default]
    User,
    Robot,
}

impl SubjectType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::User => "user",
            Self::Robot => "robot",
        }
    }

    pub fn from_claim(s: &str) -> Option<Self> {
        match s {
            "robot" => Some(Self::Robot),
            "user" => Some(Self::User),
            _ => None,
        }
    }
}

/// JWT token string wrapper.
#[derive(Clone, Debug, Default)]
pub struct JwToken(String);

impl From<&str> for JwToken {
    fn from(token: &str) -> Self {
        Self(token.to_owned())
    }
}

impl Deref for JwToken {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

/// Parsed JWT claims containing subject and organization identifiers.
///
/// When `subject_type` is `User`, `user_id`/`user_uuid` contain human user identifiers.
/// When `subject_type` is `Robot`, `user_id`/`user_uuid` carry the robot's id/uuid,
/// and `robot_id`/`robot_uuid` provide typed access via convenience methods.
#[derive(Debug, Serialize, Deserialize, BorshDeserialize, BorshSerialize, Clone)]
pub struct ParsedJwt {
    user_id: UserId,
    user_uuid: UserUuid,
    org_id: OrganizationId,
    org_uuid: OrganizationUuid,
    subject_type: SubjectType,
    /// JWT ID (jti) - unique token identifier for per-session revocation.
    /// Optional for backwards compatibility with tokens issued before this field was added.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    jti: Option<String>,
}

impl ParsedJwt {
    pub fn new(user_id: UserId, user_uuid: UserUuid, org_id: OrganizationId, org_uuid: OrganizationUuid) -> Self {
        Self {
            user_id,
            user_uuid,
            org_id,
            org_uuid,
            subject_type: SubjectType::User,
            jti: None,
        }
    }

    pub fn new_with_jti(
        user_id: UserId,
        user_uuid: UserUuid,
        org_id: OrganizationId,
        org_uuid: OrganizationUuid,
        jti: Option<String>,
    ) -> Self {
        Self {
            user_id,
            user_uuid,
            org_id,
            org_uuid,
            subject_type: SubjectType::User,
            jti,
        }
    }

    pub fn new_robot(robot_id: RobotId, robot_uuid: RobotUuid, org_id: OrganizationId, org_uuid: OrganizationUuid) -> Self {
        Self {
            user_id: UserId::from(robot_id.to_string().as_str()),
            user_uuid: UserUuid::from(robot_uuid.uuid()),
            org_id,
            org_uuid,
            subject_type: SubjectType::Robot,
            jti: None,
        }
    }

    pub fn new_robot_with_jti(
        robot_id: RobotId,
        robot_uuid: RobotUuid,
        org_id: OrganizationId,
        org_uuid: OrganizationUuid,
        jti: Option<String>,
    ) -> Self {
        Self {
            user_id: UserId::from(robot_id.to_string().as_str()),
            user_uuid: UserUuid::from(robot_uuid.uuid()),
            org_id,
            org_uuid,
            subject_type: SubjectType::Robot,
            jti,
        }
    }

    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    pub fn user_uuid(&self) -> &UserUuid {
        &self.user_uuid
    }

    pub fn org_id(&self) -> &OrganizationId {
        &self.org_id
    }

    pub fn org_uuid(&self) -> &OrganizationUuid {
        &self.org_uuid
    }

    pub fn subject_type(&self) -> &SubjectType {
        &self.subject_type
    }

    pub fn is_robot(&self) -> bool {
        self.subject_type == SubjectType::Robot
    }

    /// Returns the robot ID when the subject is a robot.
    pub fn robot_id(&self) -> Option<RobotId> {
        if self.is_robot() {
            Some(RobotId::from(self.user_id.to_string().as_str()))
        } else {
            None
        }
    }

    /// Returns the robot UUID when the subject is a robot.
    pub fn robot_uuid(&self) -> Option<RobotUuid> {
        if self.is_robot() {
            Some(RobotUuid::from(self.user_uuid.uuid()))
        } else {
            None
        }
    }

    /// Returns the JWT ID (jti) if present.
    /// Used for per-session token revocation.
    pub fn jti(&self) -> Option<&str> {
        self.jti.as_deref()
    }
}

impl ParsedJwt {
    pub fn encode(&self) -> Result<Vec<u8>, EpError> {
        borsh::to_vec(self).map_err(|_| EpError::Request(RequestError::FailedToEncodeRequest))
    }
}

/// User control-plane permission mapping for authorization.
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct AuthAccess {
    user: HashMap<UserId, ControlPerms>,
}

impl AuthAccess {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Display for AuthAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(&self).unwrap_or_default())
    }
}

/// Creates invalid credentials error with telemetry event.
pub fn incorrect_auth(s: &str, span: &mut BoxedSpan) -> EpError {
    span.add_event(format!("error: {s} does not have proper credentials"), vec![]);
    EpError::Auth(AuthError::InvalidCredentials)
}
