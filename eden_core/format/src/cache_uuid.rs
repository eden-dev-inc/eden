use crate::{EdenUuid, IdKind, OrganizationUuid};
use borsh::{BorshDeserialize, BorshSerialize};
use error::EpError;
use redis::ToRedisArgs;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    str::FromStr,
};
use uuid::Uuid;

use utoipa::ToSchema;

// Implement simple cache keys
crate::impl_simple_cache_uuid!(Organization);
crate::impl_simple_cache_uuid!(EdenNode);

// Implement all org-based cache keys
crate::impl_org_cache_uuid!(Api);
crate::impl_org_cache_uuid!(Auth);
crate::impl_org_cache_uuid!(Endpoint);
crate::impl_org_cache_uuid!(EndpointGroup);
crate::impl_org_cache_uuid!(Interlay);
crate::impl_org_cache_uuid!(Robot);
crate::impl_org_cache_uuid!(Template);
crate::impl_org_cache_uuid!(User);
crate::impl_org_cache_uuid!(Workflow);

/// Macro to implement a simple cache key without an org key
#[macro_export]
macro_rules! impl_simple_cache_uuid {
    ($kind:expr) => {
        paste::paste! {
            #[derive(Clone, Debug, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Ord, PartialOrd, Eq, PartialEq, Hash, ToSchema)]
            pub struct [<$kind CacheUuid>](Uuid);

            impl CacheUuid for [<$kind CacheUuid>] {
                fn new<E: EdenUuid>(_org_key: Option<OrganizationCacheUuid>, uuid: E) -> Self {
                    Self(uuid.uuid())
                }
                fn kind() -> IdKind {
                    IdKind::$kind
                }
                fn uuid(&self) -> Uuid {
                    self.0.clone()
                }
                fn eden_uuid<E: EdenUuid>(&self) -> E {
                    E::new(self.0.clone())
                }
                fn org(&self) -> Option<OrganizationCacheUuid> {
                    None
                }
                fn from_raw_uuid(org_key: Option<OrganizationCacheUuid>, uuid: Uuid) -> Self {
                    // Import the specific UUID type from the crate
                    use $crate::[<$kind Uuid>];
                    let typed_uuid = [<$kind Uuid>]::from(uuid);
                    Self::new(org_key, typed_uuid)
                }
                // Override the default implementation for simple cache keys
                fn parse<U: EdenUuid + From<Uuid>>(input: &str) -> Result<Self, EpError> {
                    let (key, uuid_str) = match input.split_once(":") {
                        Some(i) => i,
                        None => {
                            return Err(EpError::parse(format!(
                                "failed to parse {} cache key",
                                Self::kind()
                            )))
                        }
                    };

                    if IdKind::from_str(key)? != Self::kind() {
                        return Err(EpError::parse(format!(
                            "failed to parse {} cache key",
                            Self::kind()
                        )));
                    }

                    let uuid = Uuid::parse_str(uuid_str).map_err(EpError::parse)?;
                    let typed_uuid = U::from(uuid);

                    Ok(Self::new(None, typed_uuid))
                }
            }

            impl Display for [<$kind CacheUuid>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}:{}", Self::kind(), self.uuid())
                }
            }

            impl ToRedisArgs for [<$kind CacheUuid>] {
                fn write_redis_args<W>(&self, out: &mut W)
                where
                    W: ?Sized + redis::RedisWrite,
                {
                    let key_string = self.to_string();
                    key_string.write_redis_args(out);
                }
            }
        }
    };
}

/// Macro to implement a cache key with an org key
#[macro_export]
macro_rules! impl_org_cache_uuid {
    ($kind:expr) => {
        paste::paste! {
            #[derive(Clone, Debug, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Ord, PartialOrd, Eq, PartialEq, Hash, ToSchema)]
            pub struct [<$kind CacheUuid>] {
                org_key: OrganizationCacheUuid,
                uuid: Uuid,
            }

            impl CacheUuid for [<$kind CacheUuid>] {
                fn new<U: EdenUuid>(org_key: Option<OrganizationCacheUuid>, uuid: U) -> Self {
                    Self {
                        org_key: org_key.unwrap_or_else(|| OrganizationCacheUuid::from_raw_uuid(None, Uuid::nil())),
                        uuid: uuid.uuid(),
                    }
                }
                fn kind() -> IdKind {
                    IdKind::$kind
                }
                fn uuid(&self) -> Uuid {
                    self.uuid.clone()
                }
                fn eden_uuid<E: EdenUuid>(&self) -> E {
                    E::new(self.uuid.clone())
                }
                fn org(&self) -> Option<OrganizationCacheUuid> {
                    Some(self.org_key.clone())
                }
                fn from_raw_uuid(org_key: Option<OrganizationCacheUuid>, uuid: Uuid) -> Self {
                    // Import the specific UUID type from the crate
                    use $crate::[<$kind Uuid>];
                    let typed_uuid = [<$kind Uuid>]::from(uuid);
                    Self::new(org_key, typed_uuid)
                }
            }



            impl Display for [<$kind CacheUuid>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(
                        f,
                        "{}::{}:{}",
                        self.org_key.to_string(),
                        Self::kind(),
                        self.uuid()
                    )
                }
            }

            impl ToRedisArgs for [<$kind CacheUuid>] {
                fn write_redis_args<W>(&self, out: &mut W)
                where
                    W: ?Sized + redis::RedisWrite,
                {
                    let key_string = self.to_string();
                    key_string.write_redis_args(out);
                }
            }
        }
    };
}

pub trait CacheUuid: Display + Clone + Debug + Serialize + DeserializeOwned + ToRedisArgs + Sync + Send + 'static {
    fn new<E: EdenUuid>(org_key: Option<OrganizationCacheUuid>, uuid: E) -> Self;
    fn uuid(&self) -> Uuid;
    fn eden_uuid<E: EdenUuid>(&self) -> E;
    fn kind() -> IdKind;
    fn org(&self) -> Option<OrganizationCacheUuid>;
    fn from_raw_uuid(org_key: Option<OrganizationCacheUuid>, uuid: Uuid) -> Self;
    fn parse<U: EdenUuid + From<Uuid>>(input: &str) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let (org, endpoint) = match input.split_once("::") {
            Some(i) => i,
            None => {
                return Err(EpError::parse(format!("failed to parse {} cache key", Self::kind())));
            }
        };

        Ok(Self::new(
            Some(OrganizationCacheUuid::from(generic_parse::<OrganizationCacheUuid, OrganizationUuid>(org)?)),
            generic_parse::<Self, U>(endpoint)?,
        ))
    }
}

/// Generic parsing formula
pub fn generic_parse<C: CacheUuid, U: EdenUuid + From<Uuid>>(input: &str) -> Result<U, EpError> {
    let (key, uuid) = match input.split_once(":") {
        Some(i) => i,
        None => {
            return Err(EpError::parse(format!("failed to parse {} cache key", C::kind())));
        }
    };

    if IdKind::from_str(key)? != C::kind() {
        return Err(EpError::parse(format!("failed to parse {} cache key", C::kind())));
    }

    let uuid = Uuid::parse_str(uuid).map_err(EpError::parse)?;
    Ok(U::from(uuid))
}

// Implementation specific to OrganizationCacheUuid
impl From<OrganizationUuid> for OrganizationCacheUuid {
    fn from(value: OrganizationUuid) -> Self {
        Self::new(None, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthUuid, CacheUuid, EdenNodeUuid, EndpointUuid, TemplateUuid, WorkflowUuid};

    #[test]
    fn test_all_key_types() {
        let org_uuid = OrganizationUuid::new_uuid();
        let org_key = OrganizationCacheUuid::new(None, org_uuid);

        // Test EdenNode (simple key)
        let eden_uuid = EdenNodeUuid::new_uuid();
        let eden_key = EdenNodeCacheUuid::new(None, eden_uuid);
        assert!(eden_key.org().is_none());

        let _auth_key = AuthCacheUuid::new(Some(org_key.clone()), AuthUuid::new_uuid());
        // assert_eq!(auth_key.kind(), IdKind::Auth);

        let _endpoint_key = EndpointCacheUuid::new(Some(org_key.clone()), EndpointUuid::new_uuid());
        // assert_eq!(endpoint_key.kind(), IdKind::Endpoint);

        let _template_key = TemplateCacheUuid::new(Some(org_key.clone()), TemplateUuid::new_uuid());
        // assert_eq!(template_key.kind(), IdKind::Template);

        let _workflow_key = WorkflowCacheUuid::new(Some(org_key.clone()), WorkflowUuid::new_uuid());
        // assert_eq!(workflow_key.kind(), IdKind::Workflow);
    }
}

#[cfg(test)]
mod parsing_tests {
    use super::*;
    use crate::{AuthUuid, EdenNodeUuid, EndpointUuid, OrganizationUuid, TemplateUuid, UserUuid, WorkflowUuid};

    #[test]
    fn test_simple_cache_uuid_parsing() {
        // Test Organization (simple key)
        let org_uuid = OrganizationUuid::new_uuid();
        let org_key = OrganizationCacheUuid::new(None, org_uuid);
        let org_key_str = org_key.to_string();

        let parsed_org_key = OrganizationCacheUuid::parse::<OrganizationUuid>(&org_key_str).unwrap_or_default();
        assert_eq!(org_key, parsed_org_key);

        // Test EdenNode (simple key)
        let eden_uuid = EdenNodeUuid::new_uuid();
        let eden_key = EdenNodeCacheUuid::new(None, eden_uuid);
        let eden_key_str = eden_key.to_string();

        let parsed_eden_key = EdenNodeCacheUuid::parse::<EdenNodeUuid>(&eden_key_str).unwrap_or_default();
        assert_eq!(eden_key, parsed_eden_key);
    }

    #[test]
    fn test_org_cache_uuid_parsing() {
        let org_uuid = OrganizationUuid::new_uuid();
        let org_key = OrganizationCacheUuid::new(None, org_uuid);

        // Test Auth (org-based key)
        let auth_uuid = AuthUuid::new_uuid();
        let auth_key = AuthCacheUuid::new(Some(org_key.clone()), auth_uuid);
        let auth_key_str = auth_key.to_string();

        let parsed_auth_key = AuthCacheUuid::parse::<AuthUuid>(&auth_key_str).unwrap_or_default();
        assert_eq!(auth_key, parsed_auth_key);

        // Test Endpoint (org-based key)
        let endpoint_uuid = EndpointUuid::new_uuid();
        let endpoint_key = EndpointCacheUuid::new(Some(org_key.clone()), endpoint_uuid);
        let endpoint_key_str = endpoint_key.to_string();

        let parsed_endpoint_key = EndpointCacheUuid::parse::<EndpointUuid>(&endpoint_key_str).unwrap_or_default();
        assert_eq!(endpoint_key, parsed_endpoint_key);

        // Test Template (org-based key)
        let template_uuid = TemplateUuid::new_uuid();
        let template_key = TemplateCacheUuid::new(Some(org_key.clone()), template_uuid);
        let template_key_str = template_key.to_string();

        let parsed_template_key = TemplateCacheUuid::parse::<TemplateUuid>(&template_key_str).unwrap_or_default();
        assert_eq!(template_key, parsed_template_key);

        // Test User (org-based key)
        let user_uuid = UserUuid::new_uuid();
        let user_key = UserCacheUuid::new(Some(org_key.clone()), user_uuid);
        let user_key_str = user_key.to_string();

        let parsed_user_key = UserCacheUuid::parse::<UserUuid>(&user_key_str).unwrap_or_default();
        assert_eq!(user_key, parsed_user_key);

        // Test Workflow (org-based key)
        let workflow_uuid = WorkflowUuid::new_uuid();
        let workflow_key = WorkflowCacheUuid::new(Some(org_key.clone()), workflow_uuid);
        let workflow_key_str = workflow_key.to_string();

        let parsed_workflow_key = WorkflowCacheUuid::parse::<WorkflowUuid>(&workflow_key_str).unwrap_or_default();
        assert_eq!(workflow_key, parsed_workflow_key);
    }

    #[test]
    fn test_malformed_strings() {
        // Test invalid organization key format
        let result = OrganizationCacheUuid::parse::<OrganizationUuid>("Invalid:123");
        assert!(result.is_err());

        // Test missing separator
        let result = OrganizationCacheUuid::parse::<OrganizationUuid>("Organization123");
        assert!(result.is_err());

        // Test invalid UUID
        let result = OrganizationCacheUuid::parse::<OrganizationUuid>("Organization:not-a-uuid");
        assert!(result.is_err());

        // Test wrong kind for org-based key
        let org_uuid = OrganizationUuid::new_uuid();
        let org_key = OrganizationCacheUuid::new(None, org_uuid);
        let wrong_kind = format!("{}::Role:{}", org_key, Uuid::new_v4());

        let result = AuthCacheUuid::parse::<AuthUuid>(&wrong_kind);
        assert!(result.is_err());
    }

    #[test]
    fn test_explicit_string_formats() {
        // Create a specific UUID for testing
        let test_uuid = Uuid::parse_str("12345678-1234-5678-1234-567812345678").unwrap_or_default();

        // Test simple key format - should be "Kind:uuid"
        let org_uuid = OrganizationUuid::from(test_uuid);
        let org_key = OrganizationCacheUuid::new(None, org_uuid);
        assert_eq!(org_key.to_string(), "org:12345678-1234-5678-1234-567812345678");

        // Test org-based key format - should be "OrgKey::Kind:uuid"
        let auth_uuid = AuthUuid::from(test_uuid);
        let auth_key = AuthCacheUuid::new(Some(org_key.clone()), auth_uuid);
        assert_eq!(
            auth_key.to_string(),
            "org:12345678-1234-5678-1234-567812345678::auth:12345678-1234-5678-1234-567812345678"
        );

        // Parse back to ensure the round trip works
        let parsed_org_key =
            OrganizationCacheUuid::parse::<OrganizationUuid>("org:12345678-1234-5678-1234-567812345678").unwrap_or_default();
        assert_eq!(org_key, parsed_org_key);

        let parsed_auth_key =
            AuthCacheUuid::parse::<AuthUuid>("org:12345678-1234-5678-1234-567812345678::auth:12345678-1234-5678-1234-567812345678")
                .unwrap_or_default();
        assert_eq!(auth_key, parsed_auth_key);
    }
}
