pub mod api;
pub mod auth;
pub mod eden_node;
pub mod endpoint;
pub mod endpoint_group;
pub mod interlay;
pub mod interlay_tls;
pub mod organization;
pub mod pipeline;
pub mod robot;
pub mod routing;
pub mod snapshot;
pub mod template;
pub mod user;
pub mod workflow;

use bytes::BytesMut;
use chrono::{DateTime, Utc};
use error::{EpError, FsError};
use format::{EdenId, EdenUuid, TemplateUuid, UserUuid, WorkflowId, WorkflowUuid};
use postgres_types::{FromSql, IsNull, ToSql, Type};
use redis::{FromRedisValue, RedisWrite, ToRedisArgs};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use std::{any::Any, fmt::Debug};
#[cfg(not(embedded_db))]
pub use tokio_postgres::Row;
#[cfg(embedded_db)]
mod eden_row;
#[cfg(embedded_db)]
pub use eden_row::{FromTursoColumn, Row, RowError};
use uuid::Uuid;

// Core trait that all (non-junction) table schemas should implement
pub trait Table: Debug + Clone + Serialize + DeserializeOwned + Send + Sync + FromRow + Serialize + ToRedisArgs + FromRedisValue {
    type I: EdenId;
    type U: EdenUuid;

    fn id(&self) -> Self::I;
    fn update_id(&mut self, _id: String) -> Option<String> {
        None
    }
    fn uuid(&self) -> Self::U;
    fn description(&self) -> Option<String> {
        None
    }
    fn update_description(&mut self, _description: String) -> Option<String> {
        None
    }
    fn created_at(&self) -> DateTime<Utc>;
    fn updated_at(&self) -> DateTime<Utc>;
    fn update_timestamp(&mut self);
    fn update_updated_by(&mut self, _updated_by: UserUuid) {}
    fn as_any(&self) -> &dyn Any;
}

pub trait Timestamps {
    fn is_newer_than(&self, other: &DateTime<Utc>) -> bool;
    fn age(&self) -> chrono::Duration;
}

impl<T: Table> Timestamps for T {
    fn is_newer_than(&self, other: &DateTime<Utc>) -> bool {
        self.updated_at() > *other
    }

    fn age(&self) -> chrono::Duration {
        Utc::now() - self.updated_at()
    }
}

pub trait FromRow {
    fn from_row(row: &Row) -> Result<Self, EpError>
    where
        Self: Sized;
}

pub trait Validate {
    fn validate(&self) -> Result<(), EpError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowTemplateSchema {
    pub workflow_uuid: WorkflowUuid,
    pub template_uuid: TemplateUuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Table for WorkflowTemplateSchema {
    type I = WorkflowId;
    type U = WorkflowUuid;
    fn id(&self) -> Self::I {
        WorkflowId::new(String::default())
    }
    fn uuid(&self) -> Self::U {
        self.workflow_uuid.to_owned() // Using workflow_uuid as the primary identifier
    }
    fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
    fn update_timestamp(&mut self) {
        self.updated_at = Utc::now();
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FromRow for WorkflowTemplateSchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        Ok(Self {
            workflow_uuid: row.try_get("workflow_uuid").map_err(EpError::database)?,
            template_uuid: row.try_get("template_uuid").map_err(EpError::database)?,
            created_at: row.try_get("created_at").map_err(EpError::database)?,
            updated_at: row.try_get("updated_at").map_err(EpError::database)?,
        })
    }
}

impl ToRedisArgs for WorkflowTemplateSchema {
    #[allow(unconditional_recursion)]
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        (*self).write_redis_args(out)
    }

    // fn is_single_arg(&self) -> bool {
    //     (*self).is_single_arg()
    // }
}

impl FromRedisValue for WorkflowTemplateSchema {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            // redis::Value::Data
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            redis::Value::Nil => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Nil response when expecting WorkflowTemplateSchema",
            ))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting WorkflowTemplateSchema",
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuthType {
    Read,
    Write,
    Admin,
}

impl ToSql for AuthType {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if !matches!(ty.name(), "auth_type") {
            return Err("AuthType can only be serialized to auth_type enum".into());
        }

        // Convert enum to its string representation
        let s = match self {
            AuthType::Read => "Read",
            AuthType::Write => "Write",
            AuthType::Admin => "Admin",
        };

        // Write the string bytes to the output
        out.extend_from_slice(s.as_bytes());

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.name(), "auth_type")
    }

    fn to_sql_checked(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.to_sql(ty, out)
    }
}

impl<'a> FromSql<'a> for AuthType {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if !matches!(ty.name(), "auth_type") {
            return Err("Expected auth_type enum for AuthType".into());
        }

        // Convert bytes to string
        let s = std::str::from_utf8(raw)?;

        // Match string to enum variant
        match s {
            "Read" => Ok(AuthType::Read),
            "Write" => Ok(AuthType::Write),
            "Admin" => Ok(AuthType::Admin),
            _ => Err("Invalid auth_type value".into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.name(), "auth_type")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewOrganization {
    pub id: String,
    pub user_uuids: Vec<Uuid>,
    pub endpoint_uuids: Vec<Uuid>,
    pub team_uuids: Vec<Uuid>,
    pub role_uuids: Vec<Uuid>,
    pub admin_uuids: Vec<Uuid>,
    pub template_uuids: Vec<Uuid>,
    pub workflow_uuids: Vec<Uuid>,
    pub description: Option<String>,
}

impl Validate for NewOrganization {
    fn validate(&self) -> Result<(), EpError> {
        if self.id.is_empty() {
            return Err(EpError::Fs(FsError::OrganizationIdEmpty));
        }

        // Helper function to check for duplicates in a vector of UUIDs
        fn check_duplicates(uuids: &[Uuid], field_name: &str) -> Result<(), EpError> {
            let mut unique = uuids.to_vec();
            unique.sort();
            unique.dedup();
            if unique.len() != uuids.len() {
                return Err(EpError::fs(format!("Duplicate {}s are not allowed", field_name)));
            }
            Ok(())
        }

        // Check all UUID vectors for duplicates
        check_duplicates(&self.user_uuids, "user UUID")?;
        check_duplicates(&self.endpoint_uuids, "endpoint UUID")?;
        check_duplicates(&self.team_uuids, "team UUID")?;
        check_duplicates(&self.role_uuids, "role UUID")?;
        check_duplicates(&self.admin_uuids, "admin UUID")?;
        check_duplicates(&self.template_uuids, "template UUID")?;
        check_duplicates(&self.workflow_uuids, "workflow UUID")?;

        // Validate that admin_uuids is a subset of user_uuids
        if !self.admin_uuids.iter().all(|uuid| self.user_uuids.contains(uuid)) {
            return Err(EpError::fs("All administrators must be users of the organization"));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewEdenNode {
    pub id: String,
    pub endpoint_uuids: Vec<Uuid>,
    pub info: Value,
}

impl Validate for NewEdenNode {
    fn validate(&self) -> Result<(), EpError> {
        if self.id.is_empty() {
            return Err(EpError::Fs(FsError::NodeIdEmpty));
        }
        if self.endpoint_uuids.is_empty() {
            return Err(EpError::Fs(FsError::NodeMustHaveEndpoint));
        }
        // Check for duplicate endpoint UUIDs
        let mut unique_endpoints = self.endpoint_uuids.clone();
        unique_endpoints.sort();
        unique_endpoints.dedup();
        if unique_endpoints.len() != self.endpoint_uuids.len() {
            return Err(EpError::Fs(FsError::DuplicateEndpointUuids));
        }
        // Validate that info is an object
        if !self.info.is_object() {
            return Err(EpError::Fs(FsError::InfoMustBeJsonObject));
        }
        Ok(())
    }
}

pub struct UuidArrayOperations;

pub trait UuidArrayOps<U: EdenUuid> {
    /// Removes matching UUIDs from the provided array. Best for small arrays (< 10 elements)
    /// Uses simple iteration - O(n*m) where n is array length and m is uuids.len()
    fn remove_uuids_small(array: &mut Vec<U>, uuids: &[U]) -> bool;

    /// Removes matching UUIDs from the array. Best for medium arrays (10-100 elements)
    /// Uses sorting for better comparison - O(n log n + m log m + n + m)
    fn remove_uuids_medium(array: &mut Vec<U>, uuids: &[U]) -> bool;

    /// Removes matching UUIDs from the array. Best for large arrays (>100 elements)
    /// Uses sorting and merge-style algorithm - O(n log n + m log m)
    fn remove_uuids_large(array: &mut Vec<U>, uuids: &[U]) -> bool;

    /// Smart method that chooses the appropriate algorithm based on input size
    fn remove_uuids(array: &mut Vec<U>, uuids: &[U]) -> bool;

    /// Removes a single UUID from the array
    /// This is optimized for removing just one UUID - O(n)
    fn remove_uuid(array: &mut Vec<U>, uuid: U) -> bool;
}
impl<U> UuidArrayOps<U> for UuidArrayOperations
where
    U: EdenUuid,
{
    /// Removes matching UUIDs from the provided array. Best for small arrays (< 10 elements)
    /// Returns true if any UUIDs were removed, false otherwise
    fn remove_uuids_small(array: &mut Vec<U>, uuids: &[U]) -> bool {
        let initial_len = array.len();
        array.retain(|uuid| !uuids.contains(uuid));
        array.len() < initial_len
    }

    /// Removes matching UUIDs from the array. Best for medium arrays (10-100 elements)
    /// Returns true if any UUIDs were removed, false otherwise
    fn remove_uuids_medium(array: &mut Vec<U>, uuids: &[U]) -> bool {
        if uuids.is_empty() {
            return false;
        }

        let initial_len = array.len();

        // Create sorted copy of input array
        let mut sorted_remove = uuids.to_vec();
        sorted_remove.sort_unstable();

        // Filter using binary search
        array.retain(|uuid| sorted_remove.binary_search(uuid).is_err());

        array.len() < initial_len
    }

    /// Removes matching UUIDs from the array. Best for large arrays (>100 elements)
    /// Returns true if any UUIDs were removed, false otherwise
    fn remove_uuids_large(array: &mut Vec<U>, uuids: &[U]) -> bool {
        if uuids.is_empty() {
            return false;
        }

        let initial_len = array.len();

        // Sort both arrays
        let mut sorted_remove = uuids.to_vec();
        sorted_remove.sort_unstable();

        array.sort_unstable();

        // Use two-pointer approach to build result
        let mut result = Vec::with_capacity(array.len());
        let mut i = 0; // Index for array
        let mut j = 0; // Index for sorted_remove

        while i < array.len() {
            if j == sorted_remove.len() || array[i] < sorted_remove[j] {
                // Current UUID should be kept
                result.push(array[i].clone());
                i += 1;
            } else if array[i] > sorted_remove[j] {
                // Move forward in sorted_remove
                j += 1;
            } else {
                // Equal - skip this UUID
                i += 1;
            }
        }

        *array = result;
        array.len() < initial_len
    }

    /// Smart method that chooses the appropriate algorithm based on input size
    /// Returns true if any UUIDs were removed, false otherwise
    fn remove_uuids(array: &mut Vec<U>, uuids: &[U]) -> bool {
        match (array.len(), uuids.len()) {
            (n, m) if n < 10 || m < 10 => Self::remove_uuids_small(array, uuids),
            (n, m) if n < 100 && m < 100 => Self::remove_uuids_medium(array, uuids),
            _ => Self::remove_uuids_large(array, uuids),
        }
    }

    /// Removes a single UUID from the array
    /// Returns true if the UUID was found and removed, false otherwise
    /// This is optimized for removing just one UUID - O(n)
    fn remove_uuid(array: &mut Vec<U>, uuid: U) -> bool {
        if let Some(pos) = array.iter().position(|x| x == &uuid) {
            array.swap_remove(pos);
            true
        } else {
            false
        }
    }
}
