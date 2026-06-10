use crate::EdenUuid;
use crate::cache_uuid::{
    ApiCacheUuid, CacheUuid, EndpointCacheUuid, OrganizationCacheUuid, RobotCacheUuid, TemplateCacheUuid, UserCacheUuid, WorkflowCacheUuid,
};
use error::{EpError, ResultEP};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use utoipa::ToSchema;
use uuid::Uuid;

const RBAC_PREFIX: &str = "rbac::";

// ---------------------------------------------------------------------------
// Permission Bits (Linux-style composable permissions)
// ---------------------------------------------------------------------------

bitflags::bitflags! {
    /// Control plane permission bits — who can configure Eden itself.
    ///
    /// - **R (READ)**: View config, metadata, policy names (not credential secrets).
    /// - **C (CONFIGURE)**: Mutate control plane state — edit config, draft ELS
    ///   policies, create workflows/templates. Does NOT activate changes.
    /// - **P (PROMOTE)**: Activate/rollback versioned changes (ELS versions, config
    ///   deployments). The save-is-not-publishing gate.
    /// - **G (GRANT)**: Manage other users' permissions on this scope. Also required
    ///   to view credential secret values.
    /// - **D (DESTROY)**: Irreversible operations — delete org/endpoint, transfer
    ///   ownership.
    /// - **A (AUDIT)**: View decision log, version history, authorization decision
    ///   records.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct ControlPerms: u8 {
        const READ      = 0b0000_0001; // R
        const CONFIGURE = 0b0000_0010; // C
        const PROMOTE   = 0b0000_0100; // P
        const GRANT     = 0b0000_1000; // G
        const DESTROY   = 0b0001_0000; // D
        const AUDIT     = 0b0010_0000; // A
    }
}

bitflags::bitflags! {
    /// Data plane permission bits — what operations a user can perform at request
    /// time through an endpoint.
    ///
    /// - **r (READ)**: SELECT queries.
    /// - **w (WRITE)**: INSERT, UPDATE, DELETE.
    /// - **x (EXECUTE)**: DDL and administrative operations (CREATE TABLE, VACUUM,
    ///   GRANT, etc.).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct DataPerms: u8 {
        const READ    = 0b001; // r
        const WRITE   = 0b010; // w
        const EXECUTE = 0b100; // x
    }
}

/// Canonical character order for [`ControlPerms`] serialization (LSB → MSB).
const CP_CHARS: [(ControlPerms, char); 6] = [
    (ControlPerms::READ, 'R'),
    (ControlPerms::CONFIGURE, 'C'),
    (ControlPerms::PROMOTE, 'P'),
    (ControlPerms::GRANT, 'G'),
    (ControlPerms::DESTROY, 'D'),
    (ControlPerms::AUDIT, 'A'),
];

/// Canonical character order for [`DataPerms`] serialization (LSB → MSB).
const DP_CHARS: [(DataPerms, char); 3] = [(DataPerms::READ, 'r'), (DataPerms::WRITE, 'w'), (DataPerms::EXECUTE, 'x')];

impl ControlPerms {
    /// Serialize to canonical string form. Output is always in `RCPGDA` order
    /// with missing bits omitted. Examples: `"RCPA"`, `"RG"`, `""`.
    pub fn to_perm_string(&self) -> String {
        let mut s = String::with_capacity(6);
        for &(flag, ch) in &CP_CHARS {
            if self.contains(flag) {
                s.push(ch);
            }
        }
        s
    }

    /// Parse from a permission string. Accepts any order; unknown characters
    /// are rejected.
    pub fn from_perm_str(s: &str) -> ResultEP<Self> {
        let mut perms = Self::empty();
        for ch in s.chars() {
            match ch {
                'R' => perms |= Self::READ,
                'C' => perms |= Self::CONFIGURE,
                'P' => perms |= Self::PROMOTE,
                'G' => perms |= Self::GRANT,
                'D' => perms |= Self::DESTROY,
                'A' => perms |= Self::AUDIT,
                _ => return Err(EpError::parse(format!("unknown control permission character '{ch}'"))),
            }
        }
        Ok(perms)
    }
}

impl Display for ControlPerms {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_perm_string())
    }
}

impl Serialize for ControlPerms {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_perm_string())
    }
}

impl<'de> Deserialize<'de> for ControlPerms {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_perm_str(&s).map_err(serde::de::Error::custom)
    }
}

impl utoipa::PartialSchema for ControlPerms {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(
            utoipa::openapi::ObjectBuilder::new()
                .schema_type(utoipa::openapi::schema::SchemaType::Type(utoipa::openapi::schema::Type::String))
                .description(Some("Control plane permission string (e.g. \"RCPGDA\")"))
                .build(),
        ))
    }
}

impl ToSchema for ControlPerms {}

impl DataPerms {
    /// Serialize to canonical string form. Output is always in `rwx` order with
    /// missing bits omitted. Examples: `"rw"`, `"r"`, `""`.
    pub fn to_perm_string(&self) -> String {
        let mut s = String::with_capacity(3);
        for &(flag, ch) in &DP_CHARS {
            if self.contains(flag) {
                s.push(ch);
            }
        }
        s
    }

    /// Parse from a permission string. Accepts any order; unknown characters
    /// are rejected.
    pub fn from_perm_str(s: &str) -> ResultEP<Self> {
        let mut perms = Self::empty();
        for ch in s.chars() {
            match ch {
                'r' => perms |= Self::READ,
                'w' => perms |= Self::WRITE,
                'x' => perms |= Self::EXECUTE,
                _ => return Err(EpError::parse(format!("unknown data permission character '{ch}'"))),
            }
        }
        Ok(perms)
    }
}

impl Display for DataPerms {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_perm_string())
    }
}

impl Serialize for DataPerms {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_perm_string())
    }
}

impl<'de> Deserialize<'de> for DataPerms {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_perm_str(&s).map_err(serde::de::Error::custom)
    }
}

impl utoipa::PartialSchema for DataPerms {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(
            utoipa::openapi::ObjectBuilder::new()
                .schema_type(utoipa::openapi::schema::SchemaType::Type(utoipa::openapi::schema::Type::String))
                .description(Some("Data plane permission string (e.g. \"rwx\")"))
                .build(),
        ))
    }
}

impl ToSchema for DataPerms {}

// ---------------------------------------------------------------------------
// RBAC data structs
// ---------------------------------------------------------------------------

/// Control plane RBAC entry — who can configure Eden resources.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ControlPlaneRbacData {
    pub org_uuid: Uuid,
    pub entity_kind: String,
    pub entity_uuid: Uuid,
    pub subject_kind: String,
    pub subject_uuid: Uuid,
    pub perms: ControlPerms,
}

/// Data plane RBAC entry — what operations a user can run through an endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct DataPlaneRbacData {
    pub org_uuid: Uuid,
    pub endpoint_uuid: Uuid,
    pub subject_kind: String,
    pub subject_uuid: Uuid,
    pub perms: DataPerms,
}

/// RBAC Entity includes any target (Org, Endpoint, etc)
// pub trait RbacFromString:
//     CacheUuid + Clone + Ord + Eq + Hash + Debug + Display + Send + Sync + 'static
// {
//     fn try_from_str(str: &str) -> ResultEP<Self>;
// }

#[test]
fn from_str() {
    use crate::OrganizationUuid;

    let _org = OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid());
}

impl RbacKey for OrganizationCacheUuid {}

impl RbacKey for ApiCacheUuid {}

impl RbacKey for EndpointCacheUuid {}

impl RbacKey for TemplateCacheUuid {}

impl RbacKey for WorkflowCacheUuid {}

impl RbacKey for RobotCacheUuid {}

impl RbacKey for UserCacheUuid {}

#[derive(Clone, Serialize, Deserialize)]
/// RBAC permission data linking an entity, exact control-plane permission
/// bits, and a subject.
pub struct RbacData<E: RbacKey, S> {
    entity: E,
    perms: ControlPerms,
    subject: S,
}

pub trait RbacKey: Display + Clone {
    fn as_rbac_key(&self) -> String {
        format!("{RBAC_PREFIX}{self}")
    }
    fn try_from_rbac_key<C: CacheUuid, E: EdenUuid + From<Uuid>>(key: String) -> ResultEP<C> {
        if let Some(key) = key.strip_prefix(RBAC_PREFIX) {
            C::parse::<E>(key)
        } else {
            Err(EpError::parse("failed to parse RBAC key"))
        }
    }
}

/// `RbacString` is used to format Role-Based-Access rules that are stored
/// locally in redis.
///
/// user:user_one#admin@org:test_org
/// RBAC string identifier wrapper for serialization.
#[allow(dead_code)]
pub struct RbacString(String);

impl<E, S> RbacData<E, S>
where
    E: CacheUuid + Clone + Display + RbacKey,
    S: CacheUuid + Clone + Display,
{
    pub fn new(entity: E, perms: ControlPerms, subject: S) -> Self {
        RbacData { entity, perms, subject }
    }

    pub fn to_rbac_string(&self) -> RbacString {
        RbacString(self.to_string())
    }

    pub fn entity(&self) -> &E {
        &self.entity
    }

    pub fn perms(&self) -> &ControlPerms {
        &self.perms
    }

    pub fn subject(&self) -> &S {
        &self.subject
    }

    pub fn update(&mut self, entity: Option<E>, perms: Option<ControlPerms>, subject: Option<S>) {
        if let Some(entity) = entity {
            self.entity = entity;
        };
        if let Some(perms) = perms {
            self.perms = perms;
        };
        if let Some(subject) = subject {
            self.subject = subject;
        };
    }
}

impl<E, S> Display for RbacData<E, S>
where
    E: Display + CacheUuid + RbacKey,
    S: Display + CacheUuid,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "rbac::{}#{}@{}", self.subject, self.perms, self.entity)
    }
}

#[cfg(test)]
mod tests {
    use crate::{OrganizationUuid, UserUuid};

    use super::*;

    #[test]
    fn test_rbac_string_creation() {
        let organization_uuid = OrganizationUuid::new_uuid(); // Assuming EdenUuid provides a new() method

        let entity = OrganizationCacheUuid::new(None, organization_uuid);
        let subject = UserCacheUuid::new(Some(entity.clone()), UserUuid::new_uuid());

        let relation = ControlPerms::READ | ControlPerms::GRANT;

        let rbac = RbacData::new(entity.clone(), relation, subject.clone());

        assert_eq!(rbac.entity(), &entity);
        assert_eq!(rbac.subject(), &subject);
        assert_eq!(rbac.perms(), &relation);

        println!("{}", rbac);
    }

    #[test]
    fn test_rbac_string_display() {
        let organization_uuid = OrganizationUuid::new_uuid(); // Assuming EdenUuid provides a new() method

        let entity = OrganizationCacheUuid::new(None, organization_uuid);
        let subject = UserCacheUuid::new(Some(entity.clone()), UserUuid::new_uuid());

        let rbac = RbacData::new(entity.clone(), ControlPerms::READ | ControlPerms::GRANT, subject.clone());

        // The actual string will depend on the Display implementation of OrganizationUuid and UserUuid
        let formatted = format!("{}", rbac);
        assert!(formatted.starts_with("rbac::"));
        assert!(formatted.contains("#RG@"));
    }

    #[test]
    fn test_rbac_string_update() {
        let organization_uuid = OrganizationUuid::new_uuid(); // Assuming EdenUuid provides a new() method

        let entity = OrganizationCacheUuid::new(None, organization_uuid);
        let subject = UserCacheUuid::new(Some(entity.clone()), UserUuid::new_uuid());

        let mut rbac = RbacData::new(entity.clone(), ControlPerms::READ | ControlPerms::GRANT, subject.clone());

        rbac.update(Some(entity.clone()), Some(ControlPerms::READ | ControlPerms::CONFIGURE), None);

        assert_eq!(rbac.entity(), &entity);
        assert_eq!(rbac.perms(), &(ControlPerms::READ | ControlPerms::CONFIGURE));
        assert_eq!(rbac.subject(), &subject);
    }

    // -------------------------------------------------------------------
    // Permission Bits tests
    // -------------------------------------------------------------------

    #[test]
    fn test_control_perms_canonical_roundtrip() {
        // Every combination should survive a string round-trip.
        for bits in 0u8..=0b0011_1111 {
            let perms = ControlPerms::from_bits_truncate(bits);
            let s = perms.to_perm_string();
            let parsed = ControlPerms::from_perm_str(&s).expect("round-trip parse");
            assert_eq!(perms, parsed, "failed for bits={bits:#010b} string=\"{s}\"");
        }
    }

    #[test]
    fn test_data_perms_canonical_roundtrip() {
        for bits in 0u8..=0b111 {
            let perms = DataPerms::from_bits_truncate(bits);
            let s = perms.to_perm_string();
            let parsed = DataPerms::from_perm_str(&s).expect("round-trip parse");
            assert_eq!(perms, parsed, "failed for bits={bits:#05b} string=\"{s}\"");
        }
    }

    #[test]
    fn test_control_perms_canonical_order() {
        let perms = ControlPerms::all();
        assert_eq!(perms.to_perm_string(), "RCPGDA");
    }

    #[test]
    fn test_control_perms_order_independent_parse() {
        let a = ControlPerms::from_perm_str("DAGP").expect("parse DAGP");
        let b = ControlPerms::from_perm_str("PGDA").expect("parse PGDA");
        assert_eq!(a, b);
        // Both should produce canonical output
        assert_eq!(a.to_perm_string(), "PGDA");
    }

    #[test]
    fn test_control_perms_contains() {
        let perms = ControlPerms::READ | ControlPerms::CONFIGURE | ControlPerms::PROMOTE;
        assert!(perms.contains(ControlPerms::READ | ControlPerms::PROMOTE));
        assert!(!perms.contains(ControlPerms::GRANT));
        assert!(!perms.contains(ControlPerms::READ | ControlPerms::GRANT));
    }

    #[test]
    fn test_data_perms_contains() {
        let perms = DataPerms::READ | DataPerms::WRITE;
        assert!(perms.contains(DataPerms::READ));
        assert!(perms.contains(DataPerms::WRITE));
        assert!(!perms.contains(DataPerms::EXECUTE));
        assert!(perms.contains(DataPerms::READ | DataPerms::WRITE));
    }

    #[test]
    fn test_empty_perms() {
        assert_eq!(ControlPerms::empty().to_perm_string(), "");
        assert_eq!(DataPerms::empty().to_perm_string(), "");
        assert_eq!(ControlPerms::from_perm_str("").expect("empty"), ControlPerms::empty());
        assert_eq!(DataPerms::from_perm_str("").expect("empty"), DataPerms::empty());
    }

    #[test]
    fn test_control_perms_unknown_char_rejected() {
        assert!(ControlPerms::from_perm_str("RCZ").is_err());
        assert!(ControlPerms::from_perm_str("r").is_err()); // lowercase not valid for control
    }

    #[test]
    fn test_data_perms_unknown_char_rejected() {
        assert!(DataPerms::from_perm_str("rz").is_err());
        assert!(DataPerms::from_perm_str("R").is_err()); // uppercase not valid for data
    }

    #[test]
    fn test_rbac_data_uses_exact_control_perms() {
        let perms = ControlPerms::READ | ControlPerms::CONFIGURE | ControlPerms::AUDIT;
        let entity = OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid());
        let subject = UserCacheUuid::new(Some(entity.clone()), UserUuid::new_uuid());

        let rbac = RbacData::new(entity, perms, subject);
        assert_eq!(rbac.perms(), &perms);
    }

    #[test]
    fn test_control_perms_serde_json_roundtrip() {
        let perms = ControlPerms::READ | ControlPerms::PROMOTE | ControlPerms::AUDIT;
        let json = serde_json::to_string(&perms).expect("serialize");
        assert_eq!(json, "\"RPA\"");
        let parsed: ControlPerms = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(perms, parsed);
    }

    #[test]
    fn test_data_perms_serde_json_roundtrip() {
        let perms = DataPerms::READ | DataPerms::WRITE;
        let json = serde_json::to_string(&perms).expect("serialize");
        assert_eq!(json, "\"rw\"");
        let parsed: DataPerms = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(perms, parsed);
    }

    #[test]
    fn test_cp_rbac_data_serde() {
        let data = ControlPlaneRbacData {
            org_uuid: Uuid::nil(),
            entity_kind: "endpoint".to_string(),
            entity_uuid: Uuid::nil(),
            subject_kind: "user".to_string(),
            subject_uuid: Uuid::nil(),
            perms: ControlPerms::READ | ControlPerms::CONFIGURE,
        };
        let json = serde_json::to_string(&data).expect("serialize");
        let parsed: ControlPlaneRbacData = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(data, parsed);
    }

    #[test]
    fn test_dp_rbac_data_serde() {
        let data = DataPlaneRbacData {
            org_uuid: Uuid::nil(),
            endpoint_uuid: Uuid::nil(),
            subject_kind: "user".to_string(),
            subject_uuid: Uuid::nil(),
            perms: DataPerms::READ | DataPerms::WRITE,
        };
        let json = serde_json::to_string(&data).expect("serialize");
        let parsed: DataPlaneRbacData = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(data, parsed);
    }

    #[test]
    fn test_default_is_no_perms() {
        assert_eq!(ControlPerms::default(), ControlPerms::empty());
        assert_eq!(DataPerms::default(), DataPerms::empty());
    }
}
