use super::{CacheUuid, EdenId, OrganizationUuid};
use crate::IdKind;
use crate::cache_uuid::OrganizationCacheUuid;
use error::EpError;
use redis::ToRedisArgs;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fmt::{Debug, Display},
    str::FromStr,
};
use utoipa::ToSchema;
use uuid::Uuid;

// Implementation for the actual types
crate::impl_simple_cache_id!(Organization);
crate::impl_simple_cache_id!(EdenNode);

crate::impl_org_cache_id!(Api);
crate::impl_org_cache_id!(Auth);
crate::impl_org_cache_id!(Endpoint);
crate::impl_org_cache_id!(EndpointGroup);
crate::impl_org_cache_id!(Interlay);
crate::impl_org_cache_id!(Robot);
crate::impl_org_cache_id!(Template);
crate::impl_org_cache_id!(User);
crate::impl_org_cache_id!(Workflow);

#[macro_export]
macro_rules! impl_simple_cache_id {
    ($kind:ident) => {
        paste::paste! {
            #[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
            pub struct [<$kind CacheId>](String);

            impl CacheId for [<$kind CacheId>] {
                fn new<I: EdenId>(_org_id: Option<OrganizationCacheUuid>, id: I) -> Self {
                    Self(id.id())
                }
                fn kind() -> IdKind {
                    IdKind::$kind
                }
                fn id(&self) -> String {
                    self.0.clone()
                }
                fn eden_id<E: EdenId>(&self) -> E {
                    E::new(self.0.clone())
                }
                fn org(&self) -> Option<OrganizationCacheUuid> {
                    None
                }
                fn from_raw_string(org_key: Option<OrganizationCacheUuid>, id: String) -> Self {
                    use $crate::[<$kind Id>];
                    let typed_uuid = [<$kind Id>]::from(id);
                    Self::new(org_key, typed_uuid)
                }
                fn parse<I: EdenId>(input: &str) -> Result<Self, EpError> {
                    let (key, id_str) = match input.split_once(":") {
                        Some(i) => i,
                        None => {
                            return Err(EpError::parse(format!(
                                "failed to parse {} cache pointer",
                                Self::kind()
                            )))
                        }
                    };

                    if IdKind::from_str(key)? != Self::kind() {
                        return Err(EpError::parse(format!(
                            "failed to parse {} cache pointer",
                            Self::kind()
                        )));
                    }

                    Ok(Self::new(None, I::new(id_str.to_string())))
                }
            }

            impl Display for [<$kind CacheId>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}:{}", [<$kind CacheId>]::kind(), self.id())
                }
            }

            impl ToRedisArgs for [<$kind CacheId>] {
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

#[macro_export]
macro_rules! impl_org_cache_id {
    ($kind:ident) => {
        paste::paste! {
            #[derive(Clone, Debug, Serialize, Deserialize)]
            pub struct [<$kind CacheId>] {
                org_id: OrganizationCacheUuid,
                id: String,
            }

            impl CacheId for [<$kind CacheId>] {
                fn new<I: EdenId>(org_id: Option<OrganizationCacheUuid>, id: I) -> Self {
                    Self {
                        org_id: org_id.unwrap_or_else(|| OrganizationCacheUuid::from_raw_uuid(None, Uuid::nil())),
                        id: id.id(),
                    }
                }
                fn kind() -> IdKind {
                    IdKind::$kind
                }
                fn id(&self) -> String {
                    self.id.clone()
                }
                fn eden_id<E: EdenId>(&self) -> E {
                    E::new(self.id.clone())
                }
                fn org(&self) -> Option<OrganizationCacheUuid> {
                    Some(self.org_id.clone())
                }
                fn from_raw_string(org_key: Option<OrganizationCacheUuid>, id: String) -> Self {
                    use $crate::[<$kind Id>];
                    let typed_uuid = [<$kind Id>]::from(id);
                    Self::new(org_key, typed_uuid)
                }
                fn parse<I: EdenId>(input: &str) -> Result<Self, EpError> {
                    let (org, id_part) = match input.split_once("::") {
                        Some(i) => i,
                        None => {
                            return Err(EpError::parse(format!(
                                "failed to parse {} cache pointer",
                                Self::kind()
                            )))
                        }
                    };

                    let (key, id_str) = match id_part.split_once(":") {
                        Some(i) => i,
                        None => {
                            return Err(EpError::parse(format!(
                                "failed to parse {} cache pointer",
                                Self::kind()
                            )))
                        }
                    };

                    if IdKind::from_str(key)? != Self::kind() {
                        return Err(EpError::parse(format!(
                            "failed to parse {} cache pointer",
                            Self::kind()
                        )));
                    }

                    Ok(Self::new(
                        Some(OrganizationCacheUuid::parse::<OrganizationUuid>(org)?),
                        I::new(id_str.to_string())
                    ))
                }
            }

            impl Display for [<$kind CacheId>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(
                        f,
                        "{}::{}:{}",
                        self.org_id.to_string(),
                        Self::kind(),
                        self.id()
                    )
                }
            }

            impl ToRedisArgs for [<$kind CacheId>] {
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

pub trait CacheId: Display + Clone + Debug + Serialize + DeserializeOwned + ToRedisArgs + Sync + Send + 'static {
    fn new<I: EdenId>(org_id: Option<OrganizationCacheUuid>, id: I) -> Self;
    fn id(&self) -> String;
    fn eden_id<E: EdenId>(&self) -> E;
    fn kind() -> IdKind;
    fn org(&self) -> Option<OrganizationCacheUuid>;
    fn from_raw_string(org_key: Option<OrganizationCacheUuid>, id: String) -> Self;
    fn parse<I: EdenId>(input: &str) -> Result<Self, EpError>
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
            Some(OrganizationCacheUuid::from(super::cache_uuid::generic_parse::<
                OrganizationCacheUuid,
                OrganizationUuid,
            >(org)?)),
            generic_parse::<Self, I>(endpoint)?,
        ))
    }
}

/// Generic parsing formula
pub fn generic_parse<C: CacheId, I: EdenId>(input: &str) -> Result<I, EpError> {
    let (key, id) = match input.split_once(":") {
        Some(i) => i,
        None => {
            return Err(EpError::parse(format!("failed to parse {} cache pointer", C::kind())));
        }
    };

    if IdKind::from_str(key)? != C::kind() {
        return Err(EpError::parse(format!("failed to parse {} cache pointer", C::kind())));
    }

    Ok(I::new(id.to_string()))
}
