use bytes::Bytes;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::rbac::DataPerms;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use shardmap::config::EvictionPolicy;
use shardmap::{
    CacheOptions, CodecError, CodecKey, CodecKeyDecode, CodecShardMap, CodecValue, CodecValueEncode, EncodedBytes, SharedCache,
};
use std::collections::HashMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub const INTERNAL_CACHE_SHARDS: usize = 64;
const DEFAULT_INTERNAL_CACHE_MEMORY_BYTES: usize = 128 * 1024 * 1024;
const INTERNAL_CACHE_MEMORY_BYTES_ENV: &str = "EDEN_INTERNAL_CACHE_MEMORY_BYTES";
const KV_NAMESPACE: &[u8] = b"eden-internal-kv";
const ELS_NAMESPACE: &[u8] = b"eden-els-policy";
const RATE_BUCKET_NAMESPACE: &[u8] = b"eden-rate-bucket";
const RBAC_DATA_NAMESPACE: &[u8] = b"eden-rbac-data";
const RBAC_ORG_MEMBERSHIP_NAMESPACE: &[u8] = b"eden-rbac-org-membership";

type StringMap = CodecShardMap<Bytes, String, INTERNAL_CACHE_SHARDS>;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct CompositeCacheKey {
    group: Bytes,
    item: Bytes,
}

impl CompositeCacheKey {
    pub fn new(group: impl AsRef<[u8]>, item: impl AsRef<[u8]>) -> Self {
        Self {
            group: Bytes::copy_from_slice(group.as_ref()),
            item: Bytes::copy_from_slice(item.as_ref()),
        }
    }

    pub fn group(&self) -> &[u8] {
        self.group.as_ref()
    }

    pub fn item(&self) -> &[u8] {
        self.item.as_ref()
    }
}

impl CodecKey for CompositeCacheKey {
    fn encode_key(&self) -> EncodedBytes<'_> {
        let group = self.group.as_ref();
        let item = self.item.as_ref();
        let mut encoded = Vec::with_capacity(4 + group.len() + item.len());
        encoded.extend_from_slice(&(group.len() as u32).to_be_bytes());
        encoded.extend_from_slice(group);
        encoded.extend_from_slice(item);
        EncodedBytes::Owned(encoded.into())
    }
}

impl CodecKeyDecode for CompositeCacheKey {
    fn decode_key(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() < 4 {
            return Err(CodecError::InvalidLength { expected: 4, actual: bytes.len() });
        }
        let mut len_raw = [0u8; 4];
        len_raw.copy_from_slice(&bytes[..4]);
        let group_len = u32::from_be_bytes(len_raw) as usize;
        let rest = &bytes[4..];
        if rest.len() < group_len {
            return Err(CodecError::InvalidLength { expected: 4 + group_len, actual: bytes.len() });
        }
        let (group, item) = rest.split_at(group_len);
        Ok(Self {
            group: Bytes::copy_from_slice(group),
            item: Bytes::copy_from_slice(item),
        })
    }
}

#[derive(Debug)]
struct JsonEncodedValue {
    bytes: Bytes,
}

impl JsonEncodedValue {
    fn new<T>(value: &T) -> ResultEP<Self>
    where
        T: Serialize + ?Sized,
    {
        Ok(Self {
            bytes: Bytes::from(serde_json::to_vec(value).map_err(EpError::serde)?),
        })
    }
}

impl CodecValueEncode for JsonEncodedValue {
    fn encode_value(&self) -> EncodedBytes<'_> {
        EncodedBytes::Owned(self.bytes.clone())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
struct RbacDataCacheKey {
    endpoint_uuid: Uuid,
    subject_uuid: Uuid,
}

impl RbacDataCacheKey {
    fn new(endpoint_uuid: Uuid, subject_uuid: Uuid) -> Self {
        Self { endpoint_uuid, subject_uuid }
    }
}

impl CodecKey for RbacDataCacheKey {
    fn encode_key(&self) -> EncodedBytes<'_> {
        let mut encoded = Vec::with_capacity(32);
        encoded.extend_from_slice(self.endpoint_uuid.as_bytes());
        encoded.extend_from_slice(self.subject_uuid.as_bytes());
        EncodedBytes::Owned(encoded.into())
    }
}

impl CodecKeyDecode for RbacDataCacheKey {
    fn decode_key(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() != 32 {
            return Err(CodecError::InvalidLength { expected: 32, actual: bytes.len() });
        }
        Ok(Self {
            endpoint_uuid: decode_uuid(&bytes[..16])?,
            subject_uuid: decode_uuid(&bytes[16..])?,
        })
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct CachedDataPerms(DataPerms);

impl CachedDataPerms {
    fn into_inner(self) -> DataPerms {
        self.0
    }
}

impl CodecValueEncode for CachedDataPerms {
    fn encode_value(&self) -> EncodedBytes<'_> {
        EncodedBytes::Owned(vec![self.0.bits()].into())
    }
}

impl CodecValue for CachedDataPerms {
    type Borrowed<'a> = DataPerms;

    fn decode_owned(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() != 1 {
            return Err(CodecError::InvalidLength { expected: 1, actual: bytes.len() });
        }
        DataPerms::from_bits(bytes[0])
            .map(Self)
            .ok_or_else(|| CodecError::custom(format!("invalid data-plane permission bits: {}", bytes[0])))
    }

    fn decode_borrowed(bytes: &[u8]) -> Result<Self::Borrowed<'_>, CodecError> {
        Self::decode_owned(bytes).map(Self::into_inner)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct OrgMembershipCacheKey {
    org_uuid: Uuid,
    subject_kind: String,
    subject_uuid: Uuid,
}

impl OrgMembershipCacheKey {
    fn new(org_uuid: Uuid, subject_kind: impl Into<String>, subject_uuid: Uuid) -> Self {
        Self { org_uuid, subject_kind: subject_kind.into(), subject_uuid }
    }
}

impl CodecKey for OrgMembershipCacheKey {
    fn encode_key(&self) -> EncodedBytes<'_> {
        let subject_kind = self.subject_kind.as_bytes();
        let mut encoded = Vec::with_capacity(36 + subject_kind.len());
        encoded.extend_from_slice(self.org_uuid.as_bytes());
        encoded.extend_from_slice(&(subject_kind.len() as u32).to_be_bytes());
        encoded.extend_from_slice(subject_kind);
        encoded.extend_from_slice(self.subject_uuid.as_bytes());
        EncodedBytes::Owned(encoded.into())
    }
}

impl CodecKeyDecode for OrgMembershipCacheKey {
    fn decode_key(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() < 36 {
            return Err(CodecError::InvalidLength { expected: 36, actual: bytes.len() });
        }
        let org_uuid = decode_uuid(&bytes[..16])?;
        let mut subject_kind_len_raw = [0u8; 4];
        subject_kind_len_raw.copy_from_slice(&bytes[16..20]);
        let subject_kind_len = u32::from_be_bytes(subject_kind_len_raw) as usize;
        let subject_uuid_offset = 20 + subject_kind_len;
        if bytes.len() != subject_uuid_offset + 16 {
            return Err(CodecError::InvalidLength { expected: subject_uuid_offset + 16, actual: bytes.len() });
        }
        Ok(Self {
            org_uuid,
            subject_kind: String::from_utf8(bytes[20..subject_uuid_offset].to_vec())?,
            subject_uuid: decode_uuid(&bytes[subject_uuid_offset..])?,
        })
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct CachedOrgMembership {
    pub version_ms: i64,
    pub version_seq: i64,
    pub is_member: bool,
}

impl CodecValueEncode for CachedOrgMembership {
    fn encode_value(&self) -> EncodedBytes<'_> {
        let mut encoded = Vec::with_capacity(17);
        encoded.extend_from_slice(&self.version_ms.to_be_bytes());
        encoded.extend_from_slice(&self.version_seq.to_be_bytes());
        encoded.push(u8::from(self.is_member));
        EncodedBytes::Owned(encoded.into())
    }
}

impl CodecValue for CachedOrgMembership {
    type Borrowed<'a> = Self;

    fn decode_owned(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() != 17 {
            return Err(CodecError::InvalidLength { expected: 17, actual: bytes.len() });
        }
        let mut version_ms = [0u8; 8];
        version_ms.copy_from_slice(&bytes[..8]);
        let mut version_seq = [0u8; 8];
        version_seq.copy_from_slice(&bytes[8..16]);
        let is_member = match bytes[16] {
            0 => false,
            1 => true,
            other => return Err(CodecError::InvalidBool(other)),
        };
        Ok(Self {
            version_ms: i64::from_be_bytes(version_ms),
            version_seq: i64::from_be_bytes(version_seq),
            is_member,
        })
    }

    fn decode_borrowed(bytes: &[u8]) -> Result<Self::Borrowed<'_>, CodecError> {
        Self::decode_owned(bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RateBucketState {
    pub tokens: f64,
    pub last: i64,
    pub consumed: u64,
}

impl CodecValueEncode for RateBucketState {
    fn encode_value(&self) -> EncodedBytes<'_> {
        let mut encoded = Vec::with_capacity(24);
        encoded.extend_from_slice(&self.tokens.to_be_bytes());
        encoded.extend_from_slice(&self.last.to_be_bytes());
        encoded.extend_from_slice(&self.consumed.to_be_bytes());
        EncodedBytes::Owned(encoded.into())
    }
}

impl CodecValue for RateBucketState {
    type Borrowed<'a> = Self;

    fn decode_owned(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() != 24 {
            return Err(CodecError::InvalidLength { expected: 24, actual: bytes.len() });
        }
        let mut tokens = [0u8; 8];
        tokens.copy_from_slice(&bytes[..8]);
        let mut last = [0u8; 8];
        last.copy_from_slice(&bytes[8..16]);
        let mut consumed = [0u8; 8];
        consumed.copy_from_slice(&bytes[16..24]);
        Ok(Self {
            tokens: f64::from_be_bytes(tokens),
            last: i64::from_be_bytes(last),
            consumed: u64::from_be_bytes(consumed),
        })
    }

    fn decode_borrowed(bytes: &[u8]) -> Result<Self::Borrowed<'_>, CodecError> {
        Self::decode_owned(bytes)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct InternalCacheSnapshot {
    #[serde(default)]
    pub raw_entries: Vec<InternalCacheRawEntry>,
    /// Legacy compatibility field for older snapshots.
    #[serde(default)]
    pub kv: HashMap<String, String>,
    /// Legacy compatibility field for older Redis-hash snapshots. New
    /// ShardMap snapshots store raw namespaced entries instead.
    #[serde(default)]
    pub hashes: HashMap<String, HashMap<String, String>>,
}

impl InternalCacheSnapshot {
    pub fn entry_count(&self) -> usize {
        if self.raw_entries.is_empty() {
            self.kv.len()
        } else {
            self.raw_entries.len()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InternalCacheRawEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expires_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct InternalCache {
    engine: SharedCache<INTERNAL_CACHE_SHARDS>,
}

impl Default for InternalCache {
    fn default() -> Self {
        Self::new()
    }
}

impl InternalCache {
    pub fn new() -> Self {
        let memory_bytes = env::var(INTERNAL_CACHE_MEMORY_BYTES_ENV)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(DEFAULT_INTERNAL_CACHE_MEMORY_BYTES);

        let engine = SharedCache::<INTERNAL_CACHE_SHARDS>::with_options(CacheOptions {
            total_memory_bytes: Some(memory_bytes),
            eviction_policy: EvictionPolicy::Lru,
            ..CacheOptions::default()
        });

        Self { engine }
    }

    pub fn shared_engine(&self) -> SharedCache<INTERNAL_CACHE_SHARDS> {
        self.engine.clone()
    }

    fn kv(&self) -> ResultEP<StringMap> {
        CodecShardMap::from_shared_engine(namespace_bytes(KV_NAMESPACE), self.engine.clone()).map_err(cache_codec_error)
    }

    pub fn typed_kv<V>(&self, namespace: &'static [u8]) -> ResultEP<CodecShardMap<Bytes, V, INTERNAL_CACHE_SHARDS>> {
        CodecShardMap::from_shared_engine(namespace_bytes(namespace), self.engine.clone()).map_err(cache_codec_error)
    }

    pub fn typed_composite<V>(&self, namespace: &'static [u8]) -> ResultEP<CodecShardMap<CompositeCacheKey, V, INTERNAL_CACHE_SHARDS>> {
        CodecShardMap::from_shared_engine(namespace_bytes(namespace), self.engine.clone()).map_err(cache_codec_error)
    }

    pub async fn json_kv_set<T>(&self, namespace: &'static [u8], key: Bytes, value: &T) -> ResultEP<()>
    where
        T: Serialize + ?Sized,
    {
        let encoded = JsonEncodedValue::new(value)?;
        self.typed_kv::<Bytes>(namespace)?.insert_ref(&key, &encoded);
        Ok(())
    }

    pub async fn json_kv_set_ex<T>(&self, namespace: &'static [u8], key: Bytes, value: &T, ttl_secs: u64) -> ResultEP<()>
    where
        T: Serialize + ?Sized,
    {
        let encoded = JsonEncodedValue::new(value)?;
        self.typed_kv::<Bytes>(namespace)?.insert_ref_with_ttl(&key, &encoded, Some(ttl_ms(ttl_secs)?));
        Ok(())
    }

    pub async fn json_kv_get<T>(&self, namespace: &'static [u8], key: Bytes) -> ResultEP<Option<T>>
    where
        T: DeserializeOwned,
    {
        self.typed_kv::<Bytes>(namespace)?
            .get(&key)
            .map_err(cache_codec_error)?
            .map(|bytes| serde_json::from_slice(bytes.as_ref()).map_err(EpError::serde))
            .transpose()
    }

    pub async fn json_kv_del(&self, namespace: &'static [u8], key: Bytes) -> ResultEP<()> {
        self.typed_kv::<Bytes>(namespace)?.delete(&key);
        Ok(())
    }

    pub async fn json_kv_expire(&self, namespace: &'static [u8], key: Bytes, ttl_secs: u64) -> ResultEP<()> {
        let kv = self.typed_kv::<Bytes>(namespace)?;
        if let Some(value) = kv.get(&key).map_err(cache_codec_error)? {
            kv.insert_ref_with_ttl(&key, &value, Some(ttl_ms(ttl_secs)?));
        }
        Ok(())
    }

    pub async fn json_kv_del_with_prefix(&self, namespace: &'static [u8], prefix: Bytes) -> ResultEP<usize> {
        let kv = self.typed_kv::<Bytes>(namespace)?;
        let mut keys = Vec::new();
        kv.visit_keys(|key| {
            if key.starts_with(prefix.as_ref()) {
                keys.push(key);
            }
            true
        })
        .map_err(cache_codec_error)?;
        let deleted = keys.len();
        for key in keys {
            kv.delete(&key);
        }
        Ok(deleted)
    }

    fn rbac_data(&self) -> ResultEP<CodecShardMap<RbacDataCacheKey, CachedDataPerms, INTERNAL_CACHE_SHARDS>> {
        CodecShardMap::from_shared_engine(namespace_bytes(RBAC_DATA_NAMESPACE), self.engine.clone()).map_err(cache_codec_error)
    }

    fn rbac_org_membership(&self) -> ResultEP<CodecShardMap<OrgMembershipCacheKey, CachedOrgMembership, INTERNAL_CACHE_SHARDS>> {
        CodecShardMap::from_shared_engine(namespace_bytes(RBAC_ORG_MEMBERSHIP_NAMESPACE), self.engine.clone()).map_err(cache_codec_error)
    }

    pub async fn rbac_data_set(&self, endpoint_uuid: Uuid, subject_uuid: Uuid, perms: DataPerms) -> ResultEP<()> {
        self.rbac_data()?.insert(RbacDataCacheKey::new(endpoint_uuid, subject_uuid), CachedDataPerms(perms));
        Ok(())
    }

    pub async fn rbac_data_get(&self, endpoint_uuid: Uuid, subject_uuid: Uuid) -> ResultEP<Option<DataPerms>> {
        self.rbac_data()?
            .get(&RbacDataCacheKey::new(endpoint_uuid, subject_uuid))
            .map_err(cache_codec_error)
            .map(|value| value.map(CachedDataPerms::into_inner))
    }

    pub async fn rbac_data_del(&self, endpoint_uuid: Uuid, subject_uuid: Uuid) -> ResultEP<()> {
        self.rbac_data()?.delete(&RbacDataCacheKey::new(endpoint_uuid, subject_uuid));
        Ok(())
    }

    pub async fn rbac_data_clear_endpoint(&self, endpoint_uuid: Uuid) -> ResultEP<()> {
        let map = self.rbac_data()?;
        let mut keys = Vec::new();
        map.visit_keys(|key| {
            if key.endpoint_uuid == endpoint_uuid {
                keys.push(key);
            }
            true
        })
        .map_err(cache_codec_error)?;
        for key in keys {
            map.delete(&key);
        }
        Ok(())
    }

    pub async fn rbac_org_membership_set(
        &self,
        org_uuid: Uuid,
        subject_kind: &str,
        subject_uuid: Uuid,
        is_member: bool,
        version_ms: i64,
        version_seq: i64,
    ) -> ResultEP<()> {
        let map = self.rbac_org_membership()?;
        let key = OrgMembershipCacheKey::new(org_uuid, subject_kind, subject_uuid);
        map.insert(key, CachedOrgMembership { version_ms, version_seq, is_member });
        Ok(())
    }

    pub async fn rbac_org_membership_get(
        &self,
        org_uuid: Uuid,
        subject_kind: &str,
        subject_uuid: Uuid,
    ) -> ResultEP<Option<CachedOrgMembership>> {
        self.rbac_org_membership()?
            .get(&OrgMembershipCacheKey::new(org_uuid, subject_kind, subject_uuid))
            .map_err(cache_codec_error)
    }

    pub async fn rbac_org_membership_clear_org(&self, org_uuid: Uuid) -> ResultEP<()> {
        let map = self.rbac_org_membership()?;
        let mut keys = Vec::new();
        map.visit_keys(|key| {
            if key.org_uuid == org_uuid {
                keys.push(key);
            }
            true
        })
        .map_err(cache_codec_error)?;
        for key in keys {
            map.delete(&key);
        }
        Ok(())
    }

    fn els_policies(&self) -> ResultEP<CodecShardMap<CompositeCacheKey, Bytes, INTERNAL_CACHE_SHARDS>> {
        self.typed_composite(ELS_NAMESPACE)
    }

    pub async fn els_policy_set_raw(&self, endpoint_key: &str, user_key: &str, value: &str) -> ResultEP<()> {
        self.els_policies()?.insert(CompositeCacheKey::new(endpoint_key, user_key), Bytes::copy_from_slice(value.as_bytes()));
        Ok(())
    }

    pub async fn els_policy_get_raw(&self, endpoint_key: &str, user_key: &str) -> ResultEP<Option<String>> {
        self.els_policies()?
            .get(&CompositeCacheKey::new(endpoint_key, user_key))
            .map_err(cache_codec_error)?
            .map(|bytes| String::from_utf8(bytes.to_vec()).map_err(EpError::parse))
            .transpose()
    }

    pub async fn els_policy_exists(&self, endpoint_key: &str, user_key: &str) -> ResultEP<bool> {
        Ok(self.els_policies()?.contains_key(&CompositeCacheKey::new(endpoint_key, user_key)))
    }

    pub async fn els_policy_del(&self, endpoint_key: &str, user_key: &str) -> ResultEP<()> {
        self.els_policies()?.delete(&CompositeCacheKey::new(endpoint_key, user_key));
        Ok(())
    }

    pub async fn els_clear_endpoint(&self, endpoint_key: &str) -> ResultEP<()> {
        let policies = self.els_policies()?;
        let mut keys = Vec::new();
        policies
            .visit_keys(|key| {
                if key.group() == endpoint_key.as_bytes() {
                    keys.push(key);
                }
                true
            })
            .map_err(cache_codec_error)?;
        for key in keys {
            policies.delete(&key);
        }
        Ok(())
    }

    pub async fn els_clear_all(&self) -> ResultEP<()> {
        let policies = self.els_policies()?;
        let mut keys = Vec::new();
        policies
            .visit_keys(|key| {
                keys.push(key);
                true
            })
            .map_err(cache_codec_error)?;
        for key in keys {
            policies.delete(&key);
        }
        Ok(())
    }

    fn rate_buckets(&self) -> ResultEP<CodecShardMap<Bytes, RateBucketState, INTERNAL_CACHE_SHARDS>> {
        self.typed_kv(RATE_BUCKET_NAMESPACE)
    }

    pub async fn rate_bucket_get(&self, key: &str) -> ResultEP<Option<RateBucketState>> {
        self.rate_buckets()?.get(&Bytes::copy_from_slice(key.as_bytes())).map_err(cache_codec_error)
    }

    pub async fn rate_bucket_set(&self, key: &str, state: RateBucketState) -> ResultEP<()> {
        self.rate_buckets()?.insert_ref(&Bytes::copy_from_slice(key.as_bytes()), &state);
        Ok(())
    }

    pub async fn rate_bucket_set_ex(&self, key: &str, state: RateBucketState, ttl_secs: u64) -> ResultEP<()> {
        self.rate_buckets()?.insert_ref_with_ttl(&Bytes::copy_from_slice(key.as_bytes()), &state, Some(ttl_ms(ttl_secs)?));
        Ok(())
    }

    pub async fn rate_bucket_del(&self, key: &str) -> ResultEP<()> {
        self.rate_buckets()?.delete(&Bytes::copy_from_slice(key.as_bytes()));
        Ok(())
    }

    pub async fn kv_set(&self, key: String, value: String) -> ResultEP<()> {
        self.kv()?.insert(Bytes::from(key), value);
        Ok(())
    }

    pub async fn kv_set_ex(&self, key: String, value: String, ttl_secs: u64) -> ResultEP<()> {
        self.kv()?.insert_with_ttl(Bytes::from(key), value, Some(ttl_ms(ttl_secs)?));
        Ok(())
    }

    pub async fn kv_get(&self, key: &str) -> ResultEP<Option<String>> {
        self.kv()?.get(&Bytes::copy_from_slice(key.as_bytes())).map_err(cache_codec_error)
    }

    pub async fn kv_del(&self, key: &str) -> ResultEP<()> {
        self.kv()?.delete(&Bytes::copy_from_slice(key.as_bytes()));
        Ok(())
    }

    pub async fn kv_get_del(&self, key: &str) -> ResultEP<Option<String>> {
        self.kv()?.remove(&Bytes::copy_from_slice(key.as_bytes())).map_err(cache_codec_error)
    }

    pub async fn kv_expire(&self, key: &str, ttl_secs: u64) -> ResultEP<()> {
        let kv = self.kv()?;
        let key = Bytes::copy_from_slice(key.as_bytes());
        if let Some(value) = kv.get(&key).map_err(cache_codec_error)? {
            kv.insert_ref_with_ttl(&key, &value, Some(ttl_ms(ttl_secs)?));
        }
        Ok(())
    }

    pub async fn clear_all(&self) -> ResultEP<()> {
        let mut keys = Vec::new();
        self.engine.visit_keys(|key| {
            keys.push(key.to_vec());
            true
        });
        for key in keys {
            self.engine.remove(&key);
        }
        Ok(())
    }

    pub async fn snapshot(&self) -> ResultEP<InternalCacheSnapshot> {
        let mut snapshot = InternalCacheSnapshot::default();
        self.engine.visit_entries(|key, value, expires_at_ms| {
            snapshot.raw_entries.push(InternalCacheRawEntry { key: key.to_vec(), value: value.to_vec(), expires_at_ms });
            true
        });
        Ok(snapshot)
    }

    pub async fn snapshot_with_key_prefix(&self, prefix: &str) -> ResultEP<InternalCacheSnapshot> {
        self.snapshot_with_key_filter(|key| key.starts_with(prefix)).await
    }

    async fn snapshot_with_key_filter<F>(&self, include_key: F) -> ResultEP<InternalCacheSnapshot>
    where
        F: Fn(&str) -> bool,
    {
        let mut snapshot = InternalCacheSnapshot::default();

        for (key, value) in self.kv()?.entries().map_err(cache_codec_error)? {
            let key = String::from_utf8(key.to_vec()).map_err(EpError::parse)?;
            if include_key(&key) {
                snapshot.kv.insert(key, value);
            }
        }

        Ok(snapshot)
    }

    pub async fn restore_snapshot(&self, snapshot: &InternalCacheSnapshot) -> ResultEP<usize> {
        if !snapshot.raw_entries.is_empty() {
            let now_ms = unix_time_ms()?;
            let mut restored = 0usize;
            for entry in &snapshot.raw_entries {
                let ttl_ms = match entry.expires_at_ms {
                    Some(expires_at_ms) if expires_at_ms <= now_ms => continue,
                    Some(expires_at_ms) => Some(expires_at_ms - now_ms),
                    None => None,
                };
                self.engine.insert_slice_with_ttl(&entry.key, &entry.value, ttl_ms);
                restored += 1;
            }
            return Ok(restored);
        }

        let mut restored = 0usize;
        for (key, value) in &snapshot.kv {
            self.kv_set(key.clone(), value.clone()).await?;
            restored += 1;
        }

        Ok(restored)
    }

    pub async fn restore_snapshot_with_key_prefix(&self, snapshot: &InternalCacheSnapshot, expected_prefix: &str) -> ResultEP<usize> {
        let filtered = InternalCacheSnapshot {
            raw_entries: Vec::new(),
            kv: snapshot
                .kv
                .iter()
                .filter(|(key, _)| key.starts_with(expected_prefix))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            hashes: snapshot
                .hashes
                .iter()
                .filter(|(key, _)| key.starts_with(expected_prefix))
                .map(|(key, fields)| (key.clone(), fields.clone()))
                .collect(),
        };
        self.restore_snapshot(&filtered).await
    }
}

fn ttl_ms(ttl_secs: u64) -> ResultEP<u64> {
    ttl_secs.checked_mul(1_000).ok_or_else(|| EpError::cache("cache TTL overflow"))
}

fn unix_time_ms() -> ResultEP<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| EpError::cache(format!("system clock is before UNIX epoch: {error}")))?;
    u64::try_from(duration.as_millis()).map_err(|error| EpError::cache(format!("system time overflow: {error}")))
}

fn namespace_bytes(namespace: &'static [u8]) -> Bytes {
    Bytes::from_static(namespace)
}

fn cache_codec_error(error: CodecError) -> EpError {
    EpError::cache(format!("internal cache codec error: {error}"))
}

fn decode_uuid(bytes: &[u8]) -> Result<Uuid, CodecError> {
    Uuid::from_slice(bytes).map_err(|error| CodecError::custom(format!("invalid UUID bytes: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn snapshot_restores_raw_typed_namespaces() -> ResultEP<()> {
        let cache = InternalCache::new();
        let endpoint_uuid = Uuid::new_v4();
        let subject_uuid = Uuid::new_v4();

        cache.rbac_data_set(endpoint_uuid, subject_uuid, DataPerms::READ | DataPerms::WRITE).await?;
        cache.els_policy_set_raw("endpoint", "user", "policy").await?;

        let snapshot = cache.snapshot().await?;
        assert!(snapshot.raw_entries.len() >= 2);
        assert_eq!(snapshot.entry_count(), snapshot.raw_entries.len());

        let restored = InternalCache::new();
        let restored_count = restored.restore_snapshot(&snapshot).await?;

        assert_eq!(restored_count, snapshot.raw_entries.len());
        assert_eq!(restored.rbac_data_get(endpoint_uuid, subject_uuid).await?, Some(DataPerms::READ | DataPerms::WRITE));
        assert_eq!(restored.els_policy_get_raw("endpoint", "user").await?, Some("policy".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn prefix_snapshot_uses_legacy_kv_only() -> ResultEP<()> {
        let cache = InternalCache::new();
        cache.kv_set("org:one".to_string(), "keep".to_string()).await?;
        cache.kv_set("other:two".to_string(), "skip".to_string()).await?;
        cache.els_policy_set_raw("org:one", "user", "derived").await?;

        let snapshot = cache.snapshot_with_key_prefix("org:").await?;

        assert!(snapshot.raw_entries.is_empty());
        assert_eq!(snapshot.entry_count(), 1);
        assert_eq!(snapshot.kv.get("org:one").map(String::as_str), Some("keep"));
        assert!(!snapshot.kv.contains_key("other:two"));

        Ok(())
    }

    #[tokio::test]
    async fn restore_snapshot_skips_expired_raw_entries() -> ResultEP<()> {
        let now_ms = unix_time_ms()?;
        let snapshot = InternalCacheSnapshot {
            raw_entries: vec![
                InternalCacheRawEntry {
                    key: b"expired".to_vec(),
                    value: b"old".to_vec(),
                    expires_at_ms: Some(now_ms.saturating_sub(1)),
                },
                InternalCacheRawEntry {
                    key: b"live".to_vec(),
                    value: b"new".to_vec(),
                    expires_at_ms: Some(now_ms.saturating_add(60_000)),
                },
            ],
            kv: HashMap::new(),
            hashes: HashMap::new(),
        };
        let restored = InternalCache::new();

        let restored_count = restored.restore_snapshot(&snapshot).await?;

        assert_eq!(restored_count, 1);
        assert!(restored.engine.get_owned(b"expired").is_none());
        assert_eq!(restored.engine.get_owned(b"live").map(|value| value.to_vec()), Some(b"new".to_vec()));
        Ok(())
    }

    #[tokio::test]
    async fn restore_snapshot_preserves_live_ttl() -> ResultEP<()> {
        let cache = InternalCache::new();
        cache.kv_set_ex("ttl-key".to_string(), "ttl-value".to_string(), 60).await?;

        let snapshot = cache.snapshot().await?;
        assert_eq!(snapshot.raw_entries.len(), 1);
        let original_expires_at = snapshot.raw_entries[0].expires_at_ms.expect("snapshot entry has TTL");

        let restored = InternalCache::new();
        assert_eq!(restored.restore_snapshot(&snapshot).await?, 1);
        let restored_snapshot = restored.snapshot().await?;
        assert_eq!(restored_snapshot.raw_entries.len(), 1);
        let restored_expires_at = restored_snapshot.raw_entries[0].expires_at_ms.expect("restored entry has TTL");

        assert!(restored_expires_at <= original_expires_at);
        assert!(restored_expires_at > unix_time_ms()?);
        assert_eq!(restored.kv_get("ttl-key").await?, Some("ttl-value".to_string()));
        Ok(())
    }

    #[test]
    fn ttl_ms_rejects_overflow() {
        assert!(ttl_ms(u64::MAX).is_err());
    }

    #[test]
    fn codec_decode_rejects_malformed_keys_and_values() {
        assert!(CompositeCacheKey::decode_key(&[0, 0, 0]).is_err());
        assert!(CompositeCacheKey::decode_key(&[0, 0, 0, 2, b'a']).is_err());

        assert!(RbacDataCacheKey::decode_key(&[0; 31]).is_err());

        let mut bad_membership_key = Vec::new();
        bad_membership_key.extend_from_slice(Uuid::nil().as_bytes());
        bad_membership_key.extend_from_slice(&1u32.to_be_bytes());
        bad_membership_key.push(0xff);
        bad_membership_key.extend_from_slice(Uuid::nil().as_bytes());
        assert!(OrgMembershipCacheKey::decode_key(&bad_membership_key).is_err());
        assert!(OrgMembershipCacheKey::decode_key(&[0; 35]).is_err());

        assert!(CachedDataPerms::decode_owned(&[]).is_err());
        assert!(CachedDataPerms::decode_owned(&[0b1000]).is_err());

        assert!(CachedOrgMembership::decode_owned(&[0; 16]).is_err());
        let mut bad_membership_value = vec![0; 17];
        bad_membership_value[16] = 2;
        assert!(CachedOrgMembership::decode_owned(&bad_membership_value).is_err());

        assert!(RateBucketState::decode_owned(&[0; 23]).is_err());
    }

    #[tokio::test]
    async fn org_membership_set_round_trips_committed_state() -> ResultEP<()> {
        let cache = InternalCache::new();
        let org_uuid = Uuid::new_v4();
        let subject_uuid = Uuid::new_v4();

        cache.rbac_org_membership_set(org_uuid, "user", subject_uuid, true, 20, 2).await?;
        let cached = cache.rbac_org_membership_get(org_uuid, "user", subject_uuid).await?.expect("membership cache entry");
        assert!(cached.is_member);
        assert_eq!(cached.version_ms, 20);
        assert_eq!(cached.version_seq, 2);

        cache.rbac_org_membership_set(org_uuid, "user", subject_uuid, false, 21, 0).await?;
        let cached = cache.rbac_org_membership_get(org_uuid, "user", subject_uuid).await?.expect("membership cache entry");
        assert!(!cached.is_member);
        assert_eq!(cached.version_ms, 21);
        assert_eq!(cached.version_seq, 0);
        Ok(())
    }

    #[tokio::test]
    async fn rate_bucket_round_trip_preserves_compact_state() -> ResultEP<()> {
        let cache = InternalCache::new();
        let state = RateBucketState { tokens: 42.5, last: 1234, consumed: 99 };

        cache.rate_bucket_set_ex("bucket", state, 60).await?;

        assert_eq!(cache.rate_bucket_get("bucket").await?, Some(state));
        Ok(())
    }

    #[tokio::test]
    async fn clear_all_removes_every_namespace() -> ResultEP<()> {
        let cache = InternalCache::new();
        let endpoint_uuid = Uuid::new_v4();
        let subject_uuid = Uuid::new_v4();

        cache.kv_set("plain".to_string(), "value".to_string()).await?;
        cache.json_kv_set(b"test-json", Bytes::from_static(b"json"), &vec!["value"]).await?;
        cache.rbac_data_set(endpoint_uuid, subject_uuid, DataPerms::READ).await?;
        cache.rbac_org_membership_set(Uuid::new_v4(), "user", subject_uuid, true, 1, 0).await?;
        cache.els_policy_set_raw("endpoint", "user", "policy").await?;
        cache.rate_bucket_set("bucket", RateBucketState { tokens: 1.0, last: 2, consumed: 3 }).await?;

        assert!(cache.snapshot().await?.entry_count() >= 6);
        cache.clear_all().await?;

        assert_eq!(cache.snapshot().await?.entry_count(), 0);
        assert_eq!(cache.kv_get("plain").await?, None);
        assert_eq!(cache.rbac_data_get(endpoint_uuid, subject_uuid).await?, None);
        assert_eq!(cache.els_policy_get_raw("endpoint", "user").await?, None);
        assert_eq!(cache.rate_bucket_get("bucket").await?, None);
        Ok(())
    }
}
