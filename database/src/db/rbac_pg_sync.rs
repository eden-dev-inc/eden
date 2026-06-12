//! Versioned RBAC event helpers retained for replay tests and compatibility.
//!
//! Runtime RBAC writes are Postgres-authoritative now; the service-side Redis
//! stream consumer was removed with the internal ShardMap cache migration.

use eden_core::error::{EpError, ResultEP};
use eden_core::format::IdKind;
use eden_core::format::cache_uuid::CacheUuid;
use eden_core::format::rbac::{DataPerms, RbacData, RbacKey};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[allow(dead_code)]
pub const RBAC_EVENTS_GROUP: &str = "rbac:pg-sync";

#[allow(dead_code)]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RbacPgSyncLag {
    pub stream: String,
    pub group: String,
    pub lag: usize,
}

#[allow(dead_code)]
pub(crate) fn org_uuid_for_cache_key<K: CacheUuid>(key: &K) -> ResultEP<Uuid> {
    match key.org() {
        Some(org) => Ok(org.uuid()),
        None if K::kind() == IdKind::Organization => Ok(key.uuid()),
        None => Err(EpError::parse(format!("missing org scope for kind '{}'", K::kind()))),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RbacPgSyncOp {
    Upsert,
    DeleteRow,
    DeleteSubject,
    DeleteEntity,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct RbacPgSyncEvent {
    pub(crate) op: RbacPgSyncOp,
    pub(crate) org_uuid: Uuid,
    #[serde(default)]
    pub(crate) entity_kind: Option<String>,
    #[serde(default)]
    pub(crate) entity_uuid: Option<Uuid>,
    #[serde(default)]
    pub(crate) subject_kind: Option<String>,
    #[serde(default)]
    pub(crate) subject_uuid: Option<Uuid>,
    #[serde(default)]
    pub(crate) control_perms: Option<String>,
    #[serde(default)]
    pub(crate) data_perms: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) version: Option<(i64, i64)>,
}

#[allow(dead_code)]
impl RbacPgSyncEvent {
    pub(crate) fn serialize(&self) -> ResultEP<Vec<u8>> {
        serde_json::to_vec(self).map_err(EpError::parse)
    }

    pub(crate) fn entity_parts(&self) -> ResultEP<(&str, &Uuid)> {
        let kind = self
            .entity_kind
            .as_deref()
            .ok_or_else(|| EpError::parse(format!("missing `entity_kind` for RBAC sync op `{:?}`", self.op)))?;

        let uuid = self
            .entity_uuid
            .as_ref()
            .ok_or_else(|| EpError::parse(format!("missing `entity_uuid` for RBAC sync op `{:?}`", self.op)))?;

        Ok((kind, uuid))
    }

    pub(crate) fn subject_parts(&self) -> ResultEP<(&str, &Uuid)> {
        let kind = self
            .subject_kind
            .as_deref()
            .ok_or_else(|| EpError::parse(format!("missing `subject_kind` for RBAC sync op `{:?}`", self.op)))?;

        let uuid = self
            .subject_uuid
            .as_ref()
            .ok_or_else(|| EpError::parse(format!("missing `subject_uuid` for RBAC sync op `{:?}`", self.op)))?;

        Ok((kind, uuid))
    }

    pub(crate) fn control_perms(&self) -> ResultEP<&str> {
        self.control_perms
            .as_deref()
            .ok_or_else(|| EpError::parse(format!("missing `control_perms` for RBAC sync op `{:?}`", self.op)))
    }

    pub(crate) fn data_perms(&self) -> Option<&str> {
        self.data_perms.as_deref()
    }

    pub(crate) fn version(&self) -> ResultEP<(i64, i64)> {
        self.version.ok_or_else(|| EpError::parse(format!("missing `version` for RBAC sync op `{:?}`", self.op)))
    }

    pub(crate) fn set_version_from_stream_id(&mut self, stream_id: &str) -> ResultEP<()> {
        let (ms_raw, seq_raw) = stream_id.split_once('-').ok_or_else(|| EpError::parse(format!("invalid RBAC event ID '{stream_id}'")))?;

        let ms = ms_raw.parse::<i64>().map_err(EpError::parse)?;
        let seq = seq_raw.parse::<i64>().map_err(EpError::parse)?;
        self.version = Some((ms, seq));

        Ok(())
    }

    pub(crate) fn delete_row<E, S>(entity: &E, subject: &S) -> ResultEP<Self>
    where
        E: CacheUuid,
        S: CacheUuid,
    {
        Ok(Self {
            op: RbacPgSyncOp::DeleteRow,
            org_uuid: org_uuid_for_cache_key(entity)?,
            entity_kind: Some(E::kind().to_string()),
            entity_uuid: Some(entity.uuid()),
            subject_kind: Some(S::kind().to_string()),
            subject_uuid: Some(subject.uuid()),
            control_perms: None,
            data_perms: None,
            version: None,
        })
    }

    pub(crate) fn delete_subject<S>(subject: &S) -> ResultEP<Self>
    where
        S: CacheUuid,
    {
        Ok(Self {
            op: RbacPgSyncOp::DeleteSubject,
            org_uuid: org_uuid_for_cache_key(subject)?,
            entity_kind: None,
            entity_uuid: None,
            subject_kind: Some(S::kind().to_string()),
            subject_uuid: Some(subject.uuid()),
            control_perms: None,
            data_perms: None,
            version: None,
        })
    }

    pub(crate) fn delete_entity<E>(entity: &E) -> ResultEP<Self>
    where
        E: CacheUuid,
    {
        Ok(Self {
            op: RbacPgSyncOp::DeleteEntity,
            org_uuid: org_uuid_for_cache_key(entity)?,
            entity_kind: Some(E::kind().to_string()),
            entity_uuid: Some(entity.uuid()),
            subject_kind: None,
            subject_uuid: None,
            control_perms: None,
            data_perms: None,
            version: None,
        })
    }

    pub(crate) fn upsert_row<E, S>(rbac: &RbacData<E, S>) -> ResultEP<Self>
    where
        E: CacheUuid + RbacKey,
        S: CacheUuid,
    {
        let org_uuid = org_uuid_for_cache_key(rbac.entity())?;
        Ok(Self {
            op: RbacPgSyncOp::Upsert,
            org_uuid,
            entity_kind: Some(E::kind().to_string()),
            entity_uuid: Some(rbac.entity().uuid()),
            subject_kind: Some(S::kind().to_string()),
            subject_uuid: Some(rbac.subject().uuid()),
            control_perms: Some(rbac.perms().to_string()),
            data_perms: if E::kind() == IdKind::Endpoint {
                Some(DataPerms::empty().to_string())
            } else {
                None
            },
            version: None,
        })
    }
}
