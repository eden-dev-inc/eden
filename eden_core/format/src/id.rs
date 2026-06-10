use crate::nonce::Nonce;
use crate::{EdenId, EdenUuid, EndpointId, OrganizationUuid, UserId};
use borsh::{BorshDeserialize, BorshSerialize};
use error::{EpError, ParseError};
use postgres::types::{FromSql, Type};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;
use utoipa::ToSchema;
use uuid::Uuid;

/// Macro to implement all Eden types based on IdKind
#[macro_export]
macro_rules! impl_eden_types {
    ($($variant:ident),*) => {
        paste::paste! {
            $(
                // Generate UUID struct and implementations
                #[derive(
                    Serialize,
                    Deserialize,
                    BorshSerialize,
                    BorshDeserialize,
                    Debug,
                    Clone,
                    Default,
                    Hash,
                    Eq,
                    PartialEq,
                    PartialOrd,
                    Ord,
                    ToSchema,
                )]
                pub struct [<$variant Uuid>](Uuid);

                impl FromSql<'_> for [<$variant Uuid>] {
                    fn from_sql(
                        _: &Type,
                        raw: &[u8],
                    ) -> Result<[<$variant Uuid>], Box<dyn std::error::Error + Sync + Send>> {
                        // if raw is 16 bytes, put them into Uuid directly
                        match <[u8;16]>::try_from(raw) {
                            Ok(b128) => Ok(Self::new(Uuid::from_slice(&b128)?)),
                            Err(_) => {
                                // if we didn't get raw 16 bytes (128 bits), try to parse the string
                                let result = std::str::from_utf8(raw)?;
                                Ok(Self::from_sql_str(result)?)
                            }
                        }
                    }

                    fn accepts(_ty: &Type) -> bool {
                        true
                    }
                }

                impl ToSql for [<$variant Uuid>] {
                    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                        // Directly use the internal Uuid's to_sql implementation
                        self.0.to_sql(ty, out)
                    }

                    fn accepts(ty: &Type) -> bool {
                        <Uuid as ToSql>::accepts(ty)
                    }

                    fn to_sql_checked(&self, ty: &Type, out: &mut BytesMut) -> Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                        self.0.to_sql_checked(ty, out)
                    }
                }

                impl Deref for [<$variant Uuid>] {
                    type Target = Uuid;

                    fn deref(&self) -> &Self::Target {
                        &self.0
                    }
                }

                impl fmt::Display for [<$variant Uuid>] {
                    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                        write!(f, "{}:{}", [<$variant Uuid>]::kind(), self.0)
                    }
                }

                impl [<$variant Uuid>] {
                    pub fn new_uuid() -> Self {
                        Self(Uuid::new_v4())
                    }

                    fn from_sql_str(s: &str) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                        // Check if string contains a colon (formatted as "kind:uuid")
                        if let Some((_kind, uuid_str)) = s.split_once(':') {
                            Ok(Self::new(Uuid::from_str(uuid_str)?))
                        } else {
                            // If no colon, try parsing directly as UUID
                            Ok(Self::new(Uuid::from_str(s)?))
                        }
                    }
                }

                impl From<Uuid> for [<$variant Uuid>] {
                    fn from(uuid: Uuid) -> Self {
                        Self(uuid)
                    }
                }

                impl From<[<$variant Uuid>]> for Uuid {
                    fn from(uuid: [<$variant Uuid>]) -> Uuid {
                        uuid.0
                    }
                }

                impl EdenUuid for [<$variant Uuid>] {
                    fn new(uuid: Uuid) -> Self {
                        Self(uuid)
                    }

                    fn to_bytes(&self) -> Vec<u8> {
                        self.0.as_bytes().to_vec()
                    }

                    fn kind() -> IdKind {
                        IdKind::$variant
                    }

                    fn uuid(&self) -> Uuid {
                        self.0.clone()
                    }

                    fn format_uuid(&self, _org_uuid: Option<Uuid>) -> String {
                        format!("{}:{}", [<$variant Uuid>]::kind(), self.0)
                    }
                }

                // Generate ID struct and implementations
                #[derive(
                    Serialize,
                    Deserialize,
                    BorshSerialize,
                    BorshDeserialize,
                    Debug,
                    Clone,
                    Default,
                    Hash,
                    Eq,
                    PartialEq,
                    PartialOrd,
                    Ord,
                    ToSchema,
                )]
                pub struct [<$variant Id>](String);

                impl FromSql<'_> for [<$variant Id>] {
                    fn from_sql(
                        _: &Type,
                        raw: &[u8],
                    ) -> Result<[<$variant Id>], Box<dyn std::error::Error + Sync + Send>> {
                        let result = std::str::from_utf8(raw)?;
                        Self::from_sql_str(result)
                    }

                    fn accepts(_ty: &Type) -> bool {
                        true
                    }
                }

                impl ToSql for [<$variant Id>] {
                    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                        // Use the internal String directly
                        self.0.to_sql(ty, out)
                    }

                    fn accepts(ty: &Type) -> bool {
                        <String as ToSql>::accepts(ty)
                    }

                    fn to_sql_checked(&self, ty: &Type, out: &mut BytesMut) -> Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                        self.0.to_sql_checked(ty, out)
                    }
                }

                impl Deref for [<$variant Id>] {
                    type Target = String;

                    fn deref(&self) -> &Self::Target {
                        &self.0
                    }
                }

                impl fmt::Display for [<$variant Id>] {
                    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                        write!(f, "{}", self.0)
                    }
                }

                impl [<$variant Id>] {
                    fn from_sql_str(s: &str) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                        Ok(Self(s.to_owned()))
                    }
                }

                impl From<&str> for [<$variant Id>] {
                    fn from(id: &str) -> Self {
                        Self(id.to_owned())
                    }
                }

                impl From<String> for [<$variant Id>] {
                    fn from(id: String) -> Self {
                        Self(id)
                    }
                }

                impl EdenId for [<$variant Id>] {
                    fn new(id: String) -> Self {
                        Self(id)
                    }

                    fn to_bytes(&self) -> Vec<u8> {
                        self.0.as_bytes().to_vec()
                    }

                    fn kind() -> IdKind {
                        IdKind::$variant
                    }

                    fn update(&mut self, new_id: String) -> String {
                        let old = self.0.clone();
                        self.0 = new_id;
                        old
                    }

                    fn id(&self) -> String {
                        self.0.clone()
                    }

                    fn format_id(&self, org_uuid: Option<Uuid>) -> String {
                        format!(
                            "{}:{}::{}:{}",
                            IdKind::Organization,
                            org_uuid.unwrap_or_default(),
                            Self::kind(),
                            self.0
                        )
                    }
                }
            )*
        }
    };
}

// Generate ID struct and implementations
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default, Hash, Eq, PartialEq, PartialOrd, ToSchema)]
/// Username wrapper with validation.
pub struct Username {
    org_uuid: OrganizationUuid,
    user_id: UserId,
}

impl Username {
    pub fn new(org_uuid: OrganizationUuid, user_id: UserId) -> Self {
        Self { org_uuid, user_id }
    }
    pub fn update(&mut self, user_id: UserId) {
        self.user_id = user_id;
    }
    pub fn parts(&self) -> (OrganizationUuid, UserId) {
        (self.org_uuid.clone(), self.user_id.clone())
    }
    fn from_sql_str(s: &str) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self::try_from(s)?)
    }
}

impl FromSql<'_> for Username {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Username, Box<dyn std::error::Error + Sync + Send>> {
        let result = std::str::from_utf8(raw)?;
        Self::from_sql_str(result)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl TryFrom<String> for Username {
    type Error = EpError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if let Some((org_uuid, username)) = value.split_once('#') {
            Ok(Self::new(
                OrganizationUuid::from(Uuid::from_str(org_uuid).map_err(EpError::parse)?),
                UserId::from(username),
            ))
        } else {
            Err(EpError::Parse(ParseError::InvalidDatabaseUsername))
        }
    }
}

impl TryFrom<&str> for Username {
    type Error = EpError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if let Some((org_uuid, username)) = value.split_once('#') {
            Ok(Self::new(
                OrganizationUuid::from(Uuid::from_str(org_uuid).map_err(EpError::parse)?),
                UserId::from(username),
            ))
        } else {
            Err(EpError::Parse(ParseError::InvalidDatabaseUsername))
        }
    }
}

impl Display for Username {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.org_uuid.uuid(), self.user_id.id())
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Default, Hash, Eq, PartialEq)]
/// Endpoint URL wrapper for HTTP/external endpoints.
pub struct EndpointUrl(String);

impl Deref for EndpointUrl {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for EndpointUrl {
    fn from(name: &str) -> Self {
        EndpointUrl(name.to_owned())
    }
}

impl EndpointUrl {
    /// new nonce of zero
    pub fn new() -> Self {
        EndpointUrl::default()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }
}

impl Display for EndpointUrl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
/// Endpoint query response containing ID and UUID.
pub struct EndpointResponse {
    pub name: EndpointId,
    pub url: EndpointUrl,
    pub db_type: String,
    pub read_only: bool,
    pub connected: bool,
    pub last_nonce: Nonce,
}
