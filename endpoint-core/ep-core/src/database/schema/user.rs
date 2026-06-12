use super::Row;
use crate::database::schema::{FromRow, Table};
use auth::Password;
use chrono::{DateTime, Utc};
use error::EpError;
pub use format::rbac::ControlPerms;
use format::timestamp::DateTimeWrapper;
use format::{EdenId, OrganizationUuid, UserId, UserUuid};
#[cfg(not(embedded_db))]
use postgres_types::Json;
use redis::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use std::any::Any;
use utoipa::ToSchema;

/// Input for creating a new user with credentials.
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct UserInput {
    username: String,
    password: String,
    description: Option<String>,
    email: Option<String>,
    display_name: Option<String>,
    perms: Option<ControlPerms>,
}

impl UserInput {
    pub fn new(
        username: String,
        password: String,
        description: Option<String>,
        email: Option<String>,
        display_name: Option<String>,
        perms: ControlPerms,
    ) -> Self {
        Self {
            username,
            password,
            description,
            email,
            display_name,
            perms: Some(perms),
        }
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn description(&self) -> Option<String> {
        self.description.to_owned()
    }

    pub fn email(&self) -> Option<String> {
        self.email.to_owned()
    }

    pub fn display_name(&self) -> Option<String> {
        self.display_name.to_owned()
    }

    pub fn perms(&self) -> Option<ControlPerms> {
        self.perms
    }
}

impl From<(UserInput, OrganizationUuid)> for UserSchema {
    fn from((input, organization_uuid): (UserInput, OrganizationUuid)) -> Self {
        Self::new(
            UserId::from(input.username),
            Password::new(input.password),
            organization_uuid,
            input.description,
            input.email,
            input.display_name,
        )
    }
}

impl From<(UserInput, OrganizationUuid, UserUuid)> for UserSchema {
    fn from((input, organization_uuid, created_by): (UserInput, OrganizationUuid, UserUuid)) -> Self {
        Self::new_with_created_by(
            UserId::from(input.username),
            Password::new(input.password),
            organization_uuid,
            input.description,
            input.email,
            input.display_name,
            created_by,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct UserSchema {
    uuid: UserUuid,
    username: UserId, // Username instead of UserId
    password: Password,
    organization_uuid: OrganizationUuid,
    description: Option<String>,
    email: Option<String>,
    display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bio: Option<String>,
    created_by: UserUuid,
    updated_by: UserUuid,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl UserSchema {
    pub fn new(
        username: UserId, // usernames should be in the `Username` format
        password: Password,
        organization_uuid: OrganizationUuid,
        description: Option<String>,
        email: Option<String>,
        display_name: Option<String>,
    ) -> Self {
        Self::new_with_created_by(username, password, organization_uuid, description, email, display_name, UserUuid::new_uuid())
    }

    pub fn new_with_created_by(
        username: UserId, // usernames should be in the `Username` format
        password: Password,
        organization_uuid: OrganizationUuid,
        description: Option<String>,
        email: Option<String>,
        display_name: Option<String>,
        created_by: UserUuid,
    ) -> Self {
        let now = DateTimeWrapper::now();
        Self {
            uuid: UserUuid::new_uuid(),
            username,
            password,
            organization_uuid,
            description,
            email,
            display_name,
            bio: None,
            updated_by: created_by.clone(),
            created_by,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    pub fn created_by(&self) -> &UserUuid {
        &self.created_by
    }

    pub fn updated_by(&self) -> &UserUuid {
        &self.updated_by
    }

    pub fn set_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }

    pub fn username(&self) -> &UserId {
        &self.username
    }

    pub fn password(&self) -> &Password {
        &self.password
    }

    pub fn organization_uuid(&self) -> &OrganizationUuid {
        &self.organization_uuid
    }

    pub fn email(&self) -> Option<String> {
        self.email.clone()
    }

    pub fn display_name(&self) -> Option<String> {
        self.display_name.clone()
    }

    pub fn bio(&self) -> Option<String> {
        self.bio.clone()
    }

    pub fn update_bio(&mut self, bio: String) {
        self.bio = Some(bio);
        self.update_timestamp();
    }

    pub fn verify_password(&self, password: String) -> bool {
        self.password.verify(password)
    }

    pub fn update_username(&mut self, user_id: String) {
        self.username.update(user_id);
        self.update_timestamp();
    }

    pub fn update_password(&mut self, password: String) {
        self.password = Password::new(password);
        self.update_timestamp();
    }

    pub fn update_organization_uuid(&mut self, organization_uuid: OrganizationUuid) {
        self.organization_uuid = organization_uuid;
        self.update_timestamp();
    }

    pub fn update_email(&mut self, email: String) {
        self.email = Some(email);
        self.update_timestamp();
    }

    pub fn update_display_name(&mut self, display_name: String) {
        self.display_name = Some(display_name);
        self.update_timestamp();
    }
}

impl Table for UserSchema {
    type I = UserId;
    type U = UserUuid;

    fn id(&self) -> UserId {
        UserId::new(self.username.to_string())
    }
    fn update_id(&mut self, id: String) -> Option<String> {
        let out = self.username.update(id);
        self.update_timestamp();
        Some(out)
    }
    fn uuid(&self) -> UserUuid {
        self.uuid.to_owned()
    }
    fn description(&self) -> Option<String> {
        self.description.clone()
    }
    fn update_description(&mut self, description: String) -> Option<String> {
        let out = self.description.replace(description);
        self.update_timestamp();
        out
    }
    fn created_at(&self) -> DateTime<Utc> {
        self.created_at.as_datetime()
    }
    fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at.as_datetime()
    }
    fn update_timestamp(&mut self) {
        self.updated_at = DateTimeWrapper::now();
    }
    fn update_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FromRow for UserSchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        Ok(Self {
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            username: row.try_get("username").map_err(EpError::database)?,
            password: {
                #[cfg(not(embedded_db))]
                {
                    row.try_get::<&str, Json<Password>>("password").map_err(EpError::database)?.0
                }
                #[cfg(embedded_db)]
                {
                    row.try_get_json::<&str, Password>("password").map_err(EpError::database)?
                }
            },
            organization_uuid: row.try_get("organization_uuid").map_err(EpError::database)?,
            description: row.try_get("description").map_err(EpError::database)?,
            email: row.try_get("email").map_err(EpError::database)?,
            display_name: row.try_get("display_name").map_err(EpError::database)?,
            bio: row.try_get("bio").map_err(EpError::database)?,
            created_by: row.try_get("created_by").map_err(EpError::database)?,
            updated_by: row.try_get("updated_by").map_err(EpError::database)?,
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

impl ToRedisArgs for UserSchema {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        // Serialize the UserSchema to JSON
        let serialized = serde_json::to_vec(self).unwrap_or_default();

        // Write the serialized bytes to the Redis output
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for UserSchema {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            // redis::Value::Data
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting UserSchema",
            ))),
        }
    }
}
