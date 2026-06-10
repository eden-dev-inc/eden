#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Format
//!
//! Type-safe identifiers, UUIDs, and formatting utilities for Eve.
//!
//! ## Overview
//!
//! This crate provides strongly-typed wrappers around strings and UUIDs to prevent
//! mixing up different identifier types at compile time. It also includes cache keys,
//! timestamps, and RBAC subject types.
//!
//! ## Core Types
//!
//! ### Entity IDs and UUIDs
//!
//! For each Eden entity type, there are two identifier types:
//!
//! - **ID types** (String-based): `UserId`, `OrganizationId`, `EndpointId`
//! - **UUID types** (UUID-based): `UserUuid`, `OrganizationUuid`, `EndpointUuid`
//!
//! ```ignore
//! use format::{UserId, UserUuid, EdenUuid};
//!
//! // String-based ID
//! let user_id = UserId::from("user_123");
//!
//! // UUID-based identifier
//! let user_uuid = UserUuid::new_uuid(); // Generates new UUID
//!
//! // Convert to string
//! println!("User: {}", user_id);  // "user_123"
//! println!("UUID: {}", user_uuid.uuid()); // "550e8400-e29b-41d4-a716-446655440000"
//! ```
//!
//! ### Available Entity Types
//!
//! Defined via [`IdKind`] enum:
//! - `Organization` / `OrganizationId` / `OrganizationUuid`
//! - `User` / `UserId` / `UserUuid`
//! - `Endpoint` / `EndpointId` / `EndpointUuid`
//! - `Workflow` / `WorkflowId` / `WorkflowUuid`
//! - `Template` / `TemplateId` / `TemplateUuid`
//! - `Project` / `ProjectId` / `ProjectUuid`
//! - `EdenNode` / `EdenNodeId` / `EdenNodeUuid`
//! - `Api` / `ApiId` / `ApiUuid`
//! - `Auth` / `AuthId` / `AuthUuid`
//! - `Migration` / `MigrationId` / `MigrationUuid`
//! - `ToolServer` / `ToolServerId` / `ToolServerUuid`
//!
//! ### Cache Keys ([`cache_id`], [`cache_uuid`])
//!
//! Type-safe Redis cache keys with automatic prefixing:
//!
//! ```ignore
//! use format::{CacheId, UserCacheUuid};
//!
//! // String-based cache key
//! let cache_key = CacheId::user("user_123");
//! // Redis key: "user:user_123"
//!
//! // UUID-based cache key
//! let cache_uuid_key = UserCacheUuid::from(user_uuid);
//! // Redis key: "user:{uuid}"
//! ```
//!
//! ### RBAC Types ([`rbac`])
//!
//! Subject and resource types for role-based access control:
//!
//! - [`ControlPerms`](rbac::ControlPerms) - control-plane permission bits
//! - [`DataPerms`](rbac::DataPerms) - data-plane permission bits
//! - [`RbacData`](rbac::RbacData) - Permission relationship (entity + subject + level)
//! - [`RbacKey`](rbac::RbacKey) - Trait for RBAC-compatible types
//!
//! ### Timestamps ([`timestamp`])
//!
//! Wrapper types for time-based data:
//!
//! - `Timestamp` - Chrono DateTime with serialization
//! - `DateTimeWrapper` - Custom datetime formatting
//! - `DurationWrapper` - Duration with serialization
//!
//! ### Endpoint Types ([`endpoint`])
//!
//! Database endpoint kind enumeration:
//!
//! ```ignore
//! use format::endpoint::EpKind;
//!
//! let db_type = EpKind::Postgres;
//! let nosql = EpKind::MongoDB;
//! let cache = EpKind::Redis;
//! ```
//!
//! ## Type Safety Benefits
//!
//! Prevents mixing incompatible ID types:
//!
//! ```compile_fail
//! let user_id: UserId = UserId::from("user_123");
//! let endpoint_id: EndpointId = user_id; // Compile error!
//! ```
//!
//! ## Serialization
//!
//! All types implement:
//! - `Serialize` / `Deserialize` (JSON via serde)
//! - `ToSql` / `FromSql` (PostgreSQL)
//! - `BorshSerialize` / `BorshDeserialize` (Binary)
//! - `ToSchema` (OpenAPI via utoipa)
//!
//! ## Usage in API
//!
//! ```ignore
//! use format::{OrganizationId, UserId, EndpointUuid};
//!
//! #[derive(Serialize)]
//! struct CreateUserRequest {
//!     organization_id: OrganizationId,
//!     username: String,
//! }
//!
//! #[derive(Serialize)]
//! struct UserResponse {
//!     user_id: UserId,
//!     user_uuid: UserUuid,
//! }
//! ```

pub mod cache_id;
pub mod cache_uuid;
pub mod endpoint;
pub mod hashtype;
pub mod id;
pub mod nonce;
pub mod rbac;
pub mod timestamp;

pub use crate::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use borsh::{BorshDeserialize, BorshSerialize};
use cache_id::CacheId;
use error::{EpError, ResultEP};
use postgres::types::private::BytesMut;
use postgres::types::{FromSql, ToSql, Type};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::ops::Deref;
use std::{fmt, fmt::Debug, str::FromStr};
use utoipa::ToSchema;
use uuid::Uuid;

/// Enum of internal data structures
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// ID type discriminator (string ID vs UUID).
pub enum IdKind {
    Api,
    Auth,
    EdenNode,
    Endpoint,
    EndpointGroup,
    Interlay,
    ToolServer,
    Organization,
    Project,
    Policy,
    Robot,
    Template,
    User,
    Workflow,
}

// Implement ID and UUID types for IdKind enum
impl_eden_types!(
    Api,
    Auth,
    EdenNode,
    Endpoint,
    EndpointGroup,
    Interlay,
    ToolServer,
    Organization,
    Project,
    Policy,
    Robot,
    Template,
    User,
    Workflow
);

impl IdKind {
    /// Return the canonical string representation used in RBAC tables and APIs.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Api => API,
            Self::Auth => AUTH,
            Self::EdenNode => EDEN_NODE,
            Self::Endpoint => ENDPOINT,
            Self::EndpointGroup => ENDPOINT_GROUP,
            Self::Interlay => INTERLAY,
            Self::ToolServer => TOOL_SERVER,
            Self::Organization => ORG,
            Self::Project => PROJECT,
            Self::Policy => POLICY,
            Self::Robot => ROBOT,
            Self::Template => TEMPLATE,
            Self::User => USER,
            Self::Workflow => WORKFLOW,
        }
    }

    pub fn format_uuid(&self, root: &str, sub: Option<&str>) -> FormattedUuid {
        match self {
            Self::Organization | Self::EdenNode => FormattedUuid {
                root_kind: self.to_owned(),
                root: root.to_string(),
                sub_kind: None,
                sub: None,
            },
            _ => FormattedUuid {
                root_kind: Self::Organization,
                root: root.to_string(),
                sub_kind: Some(*self),
                sub: sub.map(String::from),
            },
        }
    }

    pub fn format_id(&self, org_id: &Uuid, id: &str) -> String {
        match self {
            Self::Organization => format!("{}:{}", self, id),
            Self::EdenNode => format!("{}:{}", self, id),
            _ => format!("{}:{}::{}:{}", Self::Organization, org_id, self, id),
        }
    }
}

const API: &str = "api";
const AUTH: &str = "auth";
const EDEN_NODE: &str = "eden_node";
const ENDPOINT: &str = "endpoint";
const ENDPOINT_GROUP: &str = "endpoint_group";
const INTERLAY: &str = "interlay";
const TOOL_SERVER: &str = "tool-server";
const ORG: &str = "org";
const PROJECT: &str = "project";
const POLICY: &str = "policy";
const ROBOT: &str = "robot";
const TEMPLATE: &str = "template";
const USER: &str = "user";
const WORKFLOW: &str = "workflow";

impl Display for IdKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IdKind::Api => write!(f, "{}", API),
            IdKind::Auth => write!(f, "{}", AUTH),
            IdKind::EdenNode => write!(f, "{}", EDEN_NODE),
            IdKind::Endpoint => write!(f, "{}", ENDPOINT),
            IdKind::EndpointGroup => write!(f, "{}", ENDPOINT_GROUP),
            IdKind::Interlay => write!(f, "{}", INTERLAY),
            IdKind::ToolServer => write!(f, "{}", TOOL_SERVER),
            IdKind::Organization => write!(f, "{}", ORG),
            IdKind::Project => write!(f, "{}", PROJECT),
            IdKind::Policy => write!(f, "{}", POLICY),
            IdKind::Robot => write!(f, "{}", ROBOT),
            IdKind::Template => write!(f, "{}", TEMPLATE),
            IdKind::User => write!(f, "{}", USER),
            IdKind::Workflow => write!(f, "{}", WORKFLOW),
        }
    }
}

impl FromStr for IdKind {
    type Err = EpError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            API => Self::Api,
            AUTH => Self::Auth,
            EDEN_NODE => Self::EdenNode,
            ENDPOINT => Self::Endpoint,
            ENDPOINT_GROUP => Self::EndpointGroup,
            INTERLAY => Self::Interlay,
            TOOL_SERVER => Self::ToolServer,
            ORG => Self::Organization,
            PROJECT => Self::Project,
            POLICY => Self::Policy,
            ROBOT => Self::Robot,
            TEMPLATE => Self::Template,
            USER => Self::User,
            WORKFLOW => Self::Workflow,
            _ => return Err(EpError::parse("failed to parse idkind")),
        })
    }
}

#[derive(Debug, Clone)]
/// Cache object type wrapper with UUID and ID types.
pub struct CacheObjectType<U, I> {
    uuid: Option<U>,
    id: Option<I>,
}

impl<U, I> From<(Option<OrganizationCacheUuid>, String)> for CacheObjectType<U, I>
where
    U: CacheUuid,
    I: CacheId,
{
    fn from((org, string): (Option<OrganizationCacheUuid>, String)) -> Self {
        match Uuid::parse_str(&string) {
            Ok(uuid) => Self { uuid: Some(U::from_raw_uuid(org, uuid)), id: None },
            Err(_) => Self { uuid: None, id: Some(I::from_raw_string(org, string)) },
        }
    }
}

impl<U: CacheUuid, I: CacheId> CacheObjectType<U, I> {
    pub fn new(uuid: Option<U>, id: Option<I>) -> Self {
        Self { uuid, id }
    }
    pub fn uuid(&self) -> Option<&U> {
        self.uuid.as_ref()
    }
    pub fn id(&self) -> Option<&I> {
        self.id.as_ref()
    }
    pub fn has_uuid(&self) -> bool {
        self.uuid.is_some()
    }
    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }
    pub fn org(&self) -> Option<OrganizationCacheUuid> {
        match self.uuid() {
            Some(uuid) => uuid.org(),
            None => self.id()?.org(),
        }
    }
    pub fn kind(&self) -> IdKind {
        U::kind()
    }
}

pub trait EdenId: Clone + PartialOrd + Ord + PartialEq + Eq + Send + Sync + Debug + ToSql {
    fn new(id: String) -> Self;
    fn to_bytes(&self) -> Vec<u8>;
    fn kind() -> IdKind;
    fn id(&self) -> String;
    fn update(&mut self, new_id: String) -> String;
    fn format_id(&self, org_uuid: Option<Uuid>) -> String;
}

pub trait EdenUuid: Clone + PartialOrd + Ord + PartialEq + Eq + Send + Sync + Debug + ToSql {
    fn new(uuid: Uuid) -> Self;
    fn to_bytes(&self) -> Vec<u8>;
    fn kind() -> IdKind;
    fn uuid(&self) -> Uuid;
    fn format_uuid(&self, org_uuid: Option<Uuid>) -> String;
}

#[allow(dead_code)]
pub struct FormattedId {
    kind: IdKind,
    org_id: &'static str,
    id: &'static str,
}

pub struct FormattedUuid {
    root_kind: IdKind,
    root: String,
    sub_kind: Option<IdKind>,
    sub: Option<String>,
}

impl FormattedUuid {
    pub fn new(root_kind: IdKind, root: String, sub_kind: Option<IdKind>, sub: Option<String>) -> Self {
        Self { root_kind, root, sub_kind, sub }
    }
}

impl Display for FormattedUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = if let Some(sub) = &self.sub {
            if let Some(sub_kind) = &self.sub_kind {
                format!("{}:{}::{}:{}", self.root_kind, self.root, sub_kind, sub)
            } else {
                format!("{}:{}::{}", self.root_kind, self.root, sub)
            }
        } else {
            format!("{}:{}", self.root_kind, self.root)
        };
        write!(f, "{}", str)
    }
}

// parse_kind_uuid parses a string with tagged Uuids,
// e.g. org:53abddae-e305-4959-92c7-7dc1ba0eb2ab::endpoint:8ef8ebf1-a3e0-4a23-86c8-0afd711eecbd
// and returns a result with UUID of the kind that was called with
// ed parse_kind_uuid::<OrganizationUuid>(s) => OrganizationUuid(53abddae-e305-4959-92c7-7dc1ba0eb2ab)
pub fn parse_kind_uuid<T: EdenUuid>(s: &str) -> ResultEP<T> {
    if let Ok(uuid) = Uuid::parse_str(s) {
        Ok(T::new(uuid))
    } else {
        for chunk in s.split("::") {
            let mut chunk_found = false;
            for parsed_chunk in chunk.split(":") {
                if chunk_found {
                    return Ok(T::new(
                        Uuid::parse_str(parsed_chunk).map_err(|e| EpError::parse(format!("{parsed_chunk} is not a valid UUID: {e}")))?,
                    ));
                }
                if parsed_chunk == T::kind().to_string().as_str() {
                    chunk_found = true;
                }
            }
        }
        Err(EpError::parse(format!("{} not found in string {}", T::kind(), s)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_uuid() {
        let root_id = "test_root";
        let sub_id = Some("test_sub");

        // Test Organization format
        assert_eq!(IdKind::Organization.format_uuid(root_id, sub_id).to_string(), "org:test_root");

        // Test EdenNode format
        assert_eq!(IdKind::EdenNode.format_uuid(root_id, sub_id).to_string(), "eden_node:test_root");

        // Test other types that include organization prefix
        assert_eq!(IdKind::User.format_uuid(root_id, sub_id).to_string(), "org:test_root::user:test_sub");
        assert_eq!(IdKind::Auth.format_uuid(root_id, sub_id).to_string(), "org:test_root::auth:test_sub");
        assert_eq!(IdKind::Endpoint.format_uuid(root_id, sub_id).to_string(), "org:test_root::endpoint:test_sub");
        assert_eq!(IdKind::Template.format_uuid(root_id, sub_id).to_string(), "org:test_root::template:test_sub");
        assert_eq!(IdKind::Workflow.format_uuid(root_id, sub_id).to_string(), "org:test_root::workflow:test_sub");
    }

    #[test]
    fn test_display() {
        assert_eq!(IdKind::Auth.to_string(), "auth");
        assert_eq!(IdKind::EdenNode.to_string(), "eden_node");
        assert_eq!(IdKind::Endpoint.to_string(), "endpoint");
        assert_eq!(IdKind::ToolServer.to_string(), "tool-server");
        assert_eq!(IdKind::Organization.to_string(), "org");
        assert_eq!(IdKind::Template.to_string(), "template");
        assert_eq!(IdKind::User.to_string(), "user");
        assert_eq!(IdKind::Workflow.to_string(), "workflow");
    }

    #[test]
    fn test_from_str() {
        assert!(matches!(IdKind::from_str("auth"), Ok(IdKind::Auth)));
        assert!(matches!(IdKind::from_str("eden_node"), Ok(IdKind::EdenNode)));
        assert!(matches!(IdKind::from_str("endpoint"), Ok(IdKind::Endpoint)));
        assert!(matches!(IdKind::from_str("org"), Ok(IdKind::Organization)));
        assert!(matches!(IdKind::from_str("template"), Ok(IdKind::Template)));
        assert!(matches!(IdKind::from_str("user"), Ok(IdKind::User)));
        assert!(matches!(IdKind::from_str("workflow"), Ok(IdKind::Workflow)));
    }

    #[test]
    fn test_from_str_invalid() {
        assert!(matches!(IdKind::from_str("invalid"), Err(EpError::Parse(_))));
    }

    #[test]
    fn test_parse_kind_uuid() {
        const TEST_STR: &str = "org:53abddae-e305-4959-92c7-7dc1ba0eb2ab::endpoint:8ef8ebf1-a3e0-4a23-86c8-0afd711eecbd";
        const TEST_EP_STR: &str = "endpoint:8ef8ebf1-a3e0-4a23-86c8-0afd711eecbd";

        assert_eq!(
            parse_kind_uuid::<OrganizationUuid>(TEST_STR).unwrap_or_default(),
            OrganizationUuid(Uuid::parse_str("53abddae-e305-4959-92c7-7dc1ba0eb2ab").unwrap_or_default())
        );
        assert_eq!(
            parse_kind_uuid::<EndpointUuid>(TEST_STR).unwrap_or_default(),
            EndpointUuid(Uuid::parse_str("8ef8ebf1-a3e0-4a23-86c8-0afd711eecbd").unwrap_or_default())
        );
        assert_eq!(
            parse_kind_uuid::<EndpointUuid>("8ef8ebf1-a3e0-4a23-86c8-0afd711eecbd").unwrap_or_default(),
            EndpointUuid(Uuid::parse_str("8ef8ebf1-a3e0-4a23-86c8-0afd711eecbd").unwrap_or_default())
        );
        assert!(parse_kind_uuid::<EndpointUuid>("invalid_uuid").is_err(),);
        assert_eq!(
            parse_kind_uuid::<EndpointUuid>(TEST_EP_STR).unwrap_or_default(),
            EndpointUuid(Uuid::parse_str("8ef8ebf1-a3e0-4a23-86c8-0afd711eecbd").unwrap_or_default())
        );
        assert!(parse_kind_uuid::<OrganizationUuid>(TEST_EP_STR).is_err(),);
    }
}
