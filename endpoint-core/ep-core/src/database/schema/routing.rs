use error::EpError;
use format::EndpointUuid;
use format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use postgres_types::{FromSql, ToSql, Type};
use serde::{Deserialize, Serialize};
use std::error::Error;
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
pub enum EndpointRouting {
    Direct {
        endpoint: EndpointUuid,
    },
    ReadReplica {
        primary: EndpointUuid,
        replicas: Vec<EndpointUuid>,
        strategy: ReplicaStrategy,
    },
    Sharded {
        shards: Vec<ShardEndpoint>,
        rule: ShardingRule,
    },
    ShardedWithReplicas {
        shards: Vec<ShardGroup>,
        rule: ShardingRule,
    },
}

impl Default for EndpointRouting {
    fn default() -> Self {
        Self::Direct { endpoint: EndpointUuid::default() }
    }
}

impl EndpointRouting {
    /// Convenience constructor for direct single-endpoint routing.
    pub fn direct(endpoint: EndpointUuid) -> Self {
        Self::Direct { endpoint }
    }

    /// Returns true if this is a Direct routing variant.
    pub fn is_direct(&self) -> bool {
        matches!(self, Self::Direct { .. })
    }

    /// Returns the primary endpoint for any routing variant.
    /// For Direct: the single endpoint.
    /// For ReadReplica: the primary.
    /// For Sharded/ShardedWithReplicas: the first shard's primary, or `None` if
    /// the shards list is empty.
    pub fn primary_endpoint(&self) -> Option<&EndpointUuid> {
        match self {
            Self::Direct { endpoint } => Some(endpoint),
            Self::ReadReplica { primary, .. } => Some(primary),
            Self::Sharded { shards, .. } => shards.first().map(|s| &s.endpoint),
            Self::ShardedWithReplicas { shards, .. } => shards.first().map(|s| &s.primary),
        }
    }

    /// Returns all endpoint UUIDs referenced by this routing configuration.
    pub fn all_endpoints(&self) -> Vec<&EndpointUuid> {
        match self {
            Self::Direct { endpoint } => vec![endpoint],
            Self::ReadReplica { primary, replicas, .. } => {
                let mut eps = vec![primary];
                eps.extend(replicas.iter());
                eps
            }
            Self::Sharded { shards, .. } => shards.iter().map(|s| &s.endpoint).collect(),
            Self::ShardedWithReplicas { shards, .. } => {
                let mut eps = Vec::new();
                for group in shards {
                    eps.push(&group.primary);
                    eps.extend(group.replicas.iter());
                }
                eps
            }
        }
    }

    /// Validate that the routing configuration is structurally sound.
    /// Returns a descriptive error if any invariants are violated.
    pub fn validate(&self) -> Result<(), EpError> {
        match self {
            Self::Direct { .. } => Ok(()),
            Self::ReadReplica { replicas, strategy, .. } => {
                if replicas.is_empty() {
                    return Err(EpError::parse("ReadReplica routing requires at least one replica"));
                }
                Self::validate_strategy(strategy, replicas)?;
                Ok(())
            }
            Self::Sharded { shards, rule } => {
                if shards.is_empty() {
                    return Err(EpError::parse("Sharded routing requires at least one shard"));
                }
                rule.validate()?;
                let ranges: Vec<Option<&ShardRange>> = shards.iter().map(|s| s.range.as_ref()).collect();
                if matches!(rule, ShardingRule::ConsistentHash { .. }) && ranges.iter().any(|r| r.is_some()) {
                    return Err(EpError::parse(
                        "ConsistentHash sharding uses a hash ring and does not support explicit shard ranges",
                    ));
                }
                Self::validate_shard_ranges(&ranges)?;
                Ok(())
            }
            Self::ShardedWithReplicas { shards, rule } => {
                if shards.is_empty() {
                    return Err(EpError::parse("ShardedWithReplicas routing requires at least one shard"));
                }
                for (i, group) in shards.iter().enumerate() {
                    if group.replicas.is_empty() {
                        return Err(EpError::parse(format!("ShardedWithReplicas shard {i} has no replicas; use Sharded instead")));
                    }
                    Self::validate_strategy(&group.replica_strategy, &group.replicas)?;
                }
                rule.validate()?;
                let ranges: Vec<Option<&ShardRange>> = shards.iter().map(|s| s.range.as_ref()).collect();
                if matches!(rule, ShardingRule::ConsistentHash { .. }) && ranges.iter().any(|r| r.is_some()) {
                    return Err(EpError::parse(
                        "ConsistentHash sharding uses a hash ring and does not support explicit shard ranges",
                    ));
                }
                Self::validate_shard_ranges(&ranges)?;
                Ok(())
            }
        }
    }

    /// Validate that a replica strategy's endpoint references exist in the replicas list.
    fn validate_strategy(strategy: &ReplicaStrategy, replicas: &[EndpointUuid]) -> Result<(), EpError> {
        match strategy {
            ReplicaStrategy::Weighted { weights } => {
                for (ep, weight) in weights {
                    if !replicas.contains(ep) {
                        return Err(EpError::parse(format!(
                            "Weighted strategy references endpoint {ep:?} which is not in the replicas list"
                        )));
                    }
                    if *weight < 0.0 {
                        return Err(EpError::parse(format!("Weighted strategy has negative weight ({weight}) for endpoint {ep:?}")));
                    }
                }
                let total: f64 = weights.iter().map(|(_, w)| w).sum();
                if total <= 0.0 {
                    return Err(EpError::parse("Weighted strategy total weight must be positive"));
                }
                Ok(())
            }
            ReplicaStrategy::Performance { memory, compute } => {
                for ep in memory.iter().chain(compute.iter()) {
                    if !replicas.contains(ep) {
                        return Err(EpError::parse(format!(
                            "Performance strategy references endpoint {ep:?} which is not in the replicas list"
                        )));
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Validate shard ranges: each range must have start <= end, ranges must not
    /// overlap, and if any shard defines a range then all shards must.
    fn validate_shard_ranges(ranges: &[Option<&ShardRange>]) -> Result<(), EpError> {
        let defined_count = ranges.iter().filter(|r| r.is_some()).count();
        if defined_count == 0 {
            return Ok(()); // Positional sharding — no ranges to validate.
        }
        if defined_count != ranges.len() {
            return Err(EpError::parse("Shard ranges must be defined on all shards or none; mixing is not supported"));
        }

        // Collect, validate start <= end, sort by start.
        let mut sorted: Vec<(usize, &ShardRange)> = ranges.iter().enumerate().map(|(i, r)| (i, r.expect("checked above"))).collect();

        for &(i, r) in &sorted {
            if r.start > r.end {
                return Err(EpError::parse(format!("Shard {i} range start ({}) is greater than end ({})", r.start, r.end)));
            }
        }

        sorted.sort_by_key(|(_, r)| r.start);

        // Check for overlaps.
        for pair in sorted.windows(2) {
            let (i_a, a) = pair[0];
            let (i_b, b) = pair[1];
            if a.end >= b.start {
                return Err(EpError::parse(format!(
                    "Shard ranges overlap: shard {i_a} ({}-{}) and shard {i_b} ({}-{})",
                    a.start, a.end, b.start, b.end
                )));
            }
        }

        Ok(())
    }
}

impl<'a> FromSql<'a> for EndpointRouting {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match *ty {
            Type::JSONB => {
                // JSONB wire format has a 1-byte version prefix (\x01) before the JSON payload.
                let json_bytes = if raw.first() == Some(&1) { &raw[1..] } else { raw };
                let routing: EndpointRouting = serde_json::from_slice(json_bytes)?;
                Ok(routing)
            }
            Type::JSON => {
                let json_str = std::str::from_utf8(raw)?;
                let routing: EndpointRouting = serde_json::from_str(json_str)?;
                Ok(routing)
            }
            Type::TEXT | Type::VARCHAR => {
                let json_str = std::str::from_utf8(raw)?;
                let routing: EndpointRouting = serde_json::from_str(json_str)?;
                Ok(routing)
            }
            _ => Err(format!("cannot convert from SQL type {} to EndpointRouting", ty).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::JSON | Type::JSONB | Type::TEXT | Type::VARCHAR)
    }
}

impl ToSql for EndpointRouting {
    fn to_sql(&self, ty: &Type, out: &mut bytes::BytesMut) -> Result<postgres_types::IsNull, Box<dyn Error + Sync + Send>> {
        match *ty {
            Type::JSON | Type::JSONB => {
                let json_string = serde_json::to_string(self)?;
                json_string.to_sql(ty, out)
            }
            Type::TEXT | Type::VARCHAR => {
                let json_string = serde_json::to_string(self)?;
                json_string.to_sql(ty, out)
            }
            _ => Err(format!("cannot convert EndpointRouting to SQL type {}", ty).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::JSON | Type::JSONB | Type::TEXT | Type::VARCHAR)
    }

    postgres_types::to_sql_checked!();
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ShardEndpoint {
    pub endpoint: EndpointUuid,
    pub range: Option<ShardRange>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ShardGroup {
    pub primary: EndpointUuid,
    pub replicas: Vec<EndpointUuid>,
    pub range: Option<ShardRange>,
    pub replica_strategy: ReplicaStrategy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ShardRange {
    pub start: u32,
    pub end: u32,
}

/// Configurable delimiter pair for extracting the hashable portion of a key.
/// When set, only the content between `open` and `close` is hashed. If no
/// delimiters are found in the key (or the content between them is empty), the
/// whole key is hashed.
///
/// Example: `{ open: '{', close: '}' }` — given key `user:{123}:profile`,
/// only `123` is hashed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct HashTagDelimiter {
    pub open: char,
    pub close: char,
}

/// Hash algorithm used for shard key distribution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema, Default)]
pub enum HashAlgorithm {
    /// CRC16-CCITT — 16-bit, fast, well-distributed for short keys.
    #[default]
    Crc16,
    /// FNV-1a 32-bit — simple, fast, good general-purpose distribution.
    Fnv1a,
}

/// Hashing configuration shared by hash-based sharding rules.
/// Groups the algorithm choice and optional hash-tag delimiter so they
/// don't have to be repeated on every `ShardingRule` variant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema, Default)]
pub struct HashConfig {
    /// Hash algorithm to use for key distribution.
    #[serde(default)]
    pub algorithm: HashAlgorithm,
    /// Optional delimiter for extracting the hashable portion of a key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash_tag: Option<HashTagDelimiter>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
pub enum ShardingRule {
    ConsistentHash {
        #[serde(default)]
        hash: HashConfig,
    },
    HashSlot {
        slots: u32,
        #[serde(default)]
        hash: HashConfig,
    },
    KeyPrefix {
        pattern: String,
    },
    Modulo {
        divisor: u32,
        #[serde(default)]
        hash: HashConfig,
    },
}

impl HashConfig {
    /// Extract the hashable portion of a key, applying the hash-tag delimiter
    /// if configured. Returns the full key when no delimiter is set or no
    /// match is found.
    fn extract_hash_input<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        if let Some(ref tag) = self.hash_tag {
            let open_byte = tag.open as u8;
            let close_byte = tag.close as u8;
            if let Some(start) = key.iter().position(|&b| b == open_byte)
                && let Some(end) = key[start + 1..].iter().position(|&b| b == close_byte)
                && end > 0
            {
                return &key[start + 1..start + 1 + end];
            }
        }
        key
    }

    /// Compute a 32-bit hash of the given key using the configured algorithm
    /// and hash-tag extraction.
    pub fn hash_key(&self, key: &[u8]) -> u32 {
        let input = self.extract_hash_input(key);
        match self.algorithm {
            HashAlgorithm::Crc16 => crc16(input) as u32,
            HashAlgorithm::Fnv1a => fnv1a_32(input),
        }
    }
}

impl ShardingRule {
    /// Validate that the sharding rule parameters are sound.
    pub fn validate(&self) -> Result<(), EpError> {
        match self {
            ShardingRule::HashSlot { slots, .. } => {
                if *slots == 0 {
                    return Err(EpError::parse("HashSlot slots must be greater than 0"));
                }
            }
            ShardingRule::Modulo { divisor, .. } => {
                if *divisor == 0 {
                    return Err(EpError::parse("Modulo divisor must be greater than 0"));
                }
            }
            ShardingRule::KeyPrefix { pattern } => {
                if !pattern.contains('*') {
                    return Err(EpError::parse("KeyPrefix pattern must contain a '*' wildcard"));
                }
            }
            ShardingRule::ConsistentHash { .. } => {}
        }
        Ok(())
    }

    /// Resolve which shard index a key maps to.
    ///
    /// For range-based sharding, the computed hash/slot is matched against each
    /// shard's `ShardRange`. For positional sharding (no ranges), the result is
    /// modulo shard count.
    pub fn resolve_shard(&self, key: &[u8], ranges: &[Option<ShardRange>], shard_count: usize) -> usize {
        if shard_count == 0 {
            return 0;
        }
        match self {
            ShardingRule::ConsistentHash { hash } => {
                let h = hash.hash_key(key);
                Self::match_range_or_modulo(h, ranges, shard_count)
            }
            ShardingRule::HashSlot { slots, hash } => {
                let slot = hash.hash_key(key) % (*slots);
                Self::match_range_or_modulo(slot, ranges, shard_count)
            }
            ShardingRule::KeyPrefix { pattern } => {
                // The pattern uses '*' as a wildcard for the shard identifier.
                // e.g., pattern "shard-*:" with key "shard-2:mykey" extracts "2"
                if let Ok(key_str) = std::str::from_utf8(key)
                    && let Some(star_pos) = pattern.find('*')
                {
                    let prefix = &pattern[..star_pos];
                    let suffix = &pattern[star_pos + 1..];
                    if let Some(stripped) = key_str.strip_prefix(prefix)
                        && let Some(end) = if suffix.is_empty() {
                            Some(stripped.len())
                        } else {
                            stripped.find(suffix)
                        }
                    {
                        let shard_id = &stripped[..end];
                        if let Ok(idx) = shard_id.parse::<usize>() {
                            return idx.min(shard_count - 1);
                        }
                        // Fall back to hashing the shard identifier
                        let h = HashConfig::default().hash_key(shard_id.as_bytes());
                        return (h as usize) % shard_count;
                    }
                }
                0
            }
            ShardingRule::Modulo { divisor, hash } => {
                let slot = hash.hash_key(key) % (*divisor);
                Self::match_range_or_modulo(slot, ranges, shard_count)
            }
        }
    }

    /// If shards have ranges defined, find the shard whose range contains the value.
    /// Otherwise, fall back to positional modulo (`value % shard_count`).
    ///
    /// When ranges are defined but the value falls into a gap between ranges, this
    /// falls back to positional modulo, which may not be the intended behaviour.
    /// Use `EndpointRouting::validate()` at config time to catch gaps.
    fn match_range_or_modulo(value: u32, ranges: &[Option<ShardRange>], shard_count: usize) -> usize {
        let has_ranges = ranges.iter().any(|r| r.is_some());
        if has_ranges {
            for (i, range) in ranges.iter().enumerate() {
                if let Some(r) = range
                    && value >= r.start
                    && value <= r.end
                {
                    return i;
                }
            }
            // Value fell into a gap between defined ranges — fall back to modulo.
        }
        (value as usize) % shard_count
    }
}

// ---------------------------------------------------------------------------
// Hash algorithm implementations
// ---------------------------------------------------------------------------

/// CRC16-CCITT over the input bytes.
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc = ((crc << 8) & 0xFF00) ^ CRC16_TAB[((crc >> 8) as u8 ^ byte) as usize];
    }
    crc
}

/// FNV-1a 32-bit hash.
fn fnv1a_32(data: &[u8]) -> u32 {
    const FNV_OFFSET: u32 = 2_166_136_261;
    const FNV_PRIME: u32 = 16_777_619;
    let mut hash = FNV_OFFSET;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[rustfmt::skip]
static CRC16_TAB: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50A5, 0x6086, 0x70C7,
    0x8108, 0x9129, 0xA14A, 0xB16B, 0xC18C, 0xD1AD, 0xE1CE, 0xF1EF,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52B5, 0x4294, 0x72F7, 0x62D6,
    0x9339, 0x8318, 0xB37B, 0xA35A, 0xD3BD, 0xC39C, 0xF3FF, 0xE3DE,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64E6, 0x74C7, 0x44A4, 0x5485,
    0xA56A, 0xB54B, 0x8528, 0x9509, 0xE5EE, 0xF5CF, 0xC5AC, 0xD58D,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76D7, 0x66F6, 0x5695, 0x46B4,
    0xB75B, 0xA77A, 0x9719, 0x8738, 0xF7DF, 0xE7FE, 0xD79D, 0xC7BC,
    0x4864, 0x5845, 0x6826, 0x7807, 0x08E0, 0x18C1, 0x28A2, 0x38A3,
    0xC94C, 0xD96D, 0xE90E, 0xF92F, 0x89C8, 0x99E9, 0xA98A, 0xB9AB,
    0x5A75, 0x4A54, 0x7A37, 0x6A16, 0x1AF1, 0x0AD0, 0x3AB3, 0x2A92,
    0xDB7D, 0xCB5C, 0xFB3F, 0xEB1E, 0x9BF9, 0x8BD8, 0xBBBB, 0xAB9A,
    0x6CA6, 0x7C87, 0x4CE4, 0x5CC5, 0x2C22, 0x3C03, 0x0C60, 0x1C41,
    0xEDAE, 0xFD8F, 0xCDEC, 0xDDCD, 0xAD2A, 0xBD0B, 0x8D68, 0x9D49,
    0x7E97, 0x6EB6, 0x5ED5, 0x4EF4, 0x3E13, 0x2E32, 0x1E51, 0x0E70,
    0xFF9F, 0xEFBE, 0xDFDD, 0xCFFC, 0xBF1B, 0xAF3A, 0x9F59, 0x8F78,
    0x9188, 0x81A9, 0xB1CA, 0xA1EB, 0xD10C, 0xC12D, 0xF14E, 0xE16F,
    0x1080, 0x00A1, 0x30C2, 0x20E3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83B9, 0x9398, 0xA3FB, 0xB3DA, 0xC33D, 0xD31C, 0xE37F, 0xF35E,
    0x02B1, 0x1290, 0x22F3, 0x32D2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xB5EA, 0xA5CB, 0x95A8, 0x85A9, 0xF56E, 0xE54F, 0xD52C, 0xC50D,
    0x34E2, 0x24C3, 0x14A0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xA7DB, 0xB7FA, 0x8799, 0x9798, 0xE77F, 0xF75E, 0xC73D, 0xD71C,
    0x26D3, 0x36F2, 0x0691, 0x16B0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xD94C, 0xC96D, 0xF90E, 0xE92F, 0x99C8, 0x89E9, 0xB98A, 0xA9AB,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18C0, 0x08E1, 0x3882, 0x28A3,
    0xCB7D, 0xDB5C, 0xEB3F, 0xFB1E, 0x8BF9, 0x9BD8, 0xABBB, 0xBB9A,
    0x4A55, 0x5A74, 0x6A17, 0x7A36, 0x0AD1, 0x1AF0, 0x2A93, 0x3AB2,
    0xFD2E, 0xED0F, 0xDD6C, 0xCD4D, 0xBDAA, 0xAD8B, 0x9DE8, 0x8DC9,
    0x7C26, 0x6C07, 0x5C64, 0x4C45, 0x3CA2, 0x2C83, 0x1CE0, 0x0CC1,
    0xEF1F, 0xFF3E, 0xCF5D, 0xDF7C, 0xAF9B, 0xBF3A, 0x8F59, 0x9F78,
    0x6E97, 0x7EB6, 0x4ED5, 0x5EF4, 0x2E13, 0x3E32, 0x0E51, 0x1E70,
];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
pub enum ReplicaStrategy {
    RoundRobin,
    Random,
    LeastConnections,
    LeastLatency,
    Performance {
        memory: Vec<EndpointUuid>,
        compute: Vec<EndpointUuid>,
    },
    Weighted {
        weights: Vec<(EndpointUuid, f64)>,
    },
}

/// Consistent hash ring for stable key-to-shard mapping.
///
/// Uses virtual nodes (replicas on the ring) to distribute shard ownership evenly.
/// When shards are added or removed, only ~1/N of keys are remapped (where N is the
/// total number of shards), unlike simple modulo hashing which remaps nearly all keys.
#[derive(Clone, Debug)]
pub struct ConsistentHashRing {
    /// Sorted ring of (position, shard_index) pairs.
    ring: Vec<(u32, usize)>,
    /// Hash config used for key hashing.
    hash: HashConfig,
}

impl ConsistentHashRing {
    /// Number of virtual nodes per shard for even distribution.
    const VIRTUAL_NODES: usize = 150;

    /// Build a consistent hash ring for the given number of shards.
    pub fn new(shard_count: usize, hash: &HashConfig) -> Self {
        let mut ring = Vec::with_capacity(shard_count * Self::VIRTUAL_NODES);
        for shard_idx in 0..shard_count {
            for vnode in 0..Self::VIRTUAL_NODES {
                let vnode_key = format!("{shard_idx}-{vnode}");
                let position = hash.hash_key(vnode_key.as_bytes());
                ring.push((position, shard_idx));
            }
        }
        ring.sort_by_key(|(pos, _)| *pos);
        Self { ring, hash: hash.clone() }
    }

    /// Find which shard a key maps to on the ring.
    /// Returns 0 if the ring is empty.
    pub fn resolve(&self, key: &[u8]) -> usize {
        if self.ring.is_empty() {
            return 0;
        }
        let h = self.hash.hash_key(key);
        // Binary search for the first ring position >= h (clockwise walk).
        match self.ring.binary_search_by_key(&h, |(pos, _)| *pos) {
            Ok(idx) => self.ring[idx].1,
            Err(idx) => {
                // Wrap around to the first node if past the end of the ring.
                if idx >= self.ring.len() { self.ring[0].1 } else { self.ring[idx].1 }
            }
        }
    }
}

/// Pre-resolved shard routing info for a single shard group.
#[derive(Clone)]
pub struct ResolvedShard {
    /// Primary endpoint for this shard.
    pub primary: EndpointCacheUuid,
    /// Replica endpoints for this shard (empty for `Sharded` without replicas).
    pub replicas: Vec<EndpointCacheUuid>,
    /// Strategy for selecting among replicas.
    pub replica_strategy: ReplicaStrategy,
    /// Atomic counter for round-robin within this shard.
    pub replica_counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

/// Database-agnostic routing resolver that maps keys and read/write intent to
/// concrete `EndpointCacheUuid` targets. Protocol processors (Redis, SQL, etc.)
/// extract keys in their own wire-format-specific way and then delegate the
/// routing decision to this resolver.
#[derive(Clone)]
pub struct RoutingResolver {
    /// Primary endpoint (Direct single endpoint, ReadReplica primary, or first shard primary).
    primary: EndpointCacheUuid,
    /// Full routing configuration (Arc-wrapped — the endpoint/shard data is already
    /// pre-resolved into the fields above, so this is only kept for variant matching
    /// and equality comparison during routing updates).
    routing: std::sync::Arc<EndpointRouting>,
    /// Pre-resolved top-level replicas for `ReadReplica` routing.
    replicas: Vec<EndpointCacheUuid>,
    /// Atomic counter for round-robin replica selection at top level.
    replica_counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Pre-resolved shard groups for `Sharded` / `ShardedWithReplicas` routing.
    shards: Vec<ResolvedShard>,
    /// Shard ranges (parallel to `shards`), used by `ShardingRule::resolve_shard`.
    shard_ranges: Vec<Option<ShardRange>>,
    /// Pre-built consistent hash ring (populated only for `ConsistentHash` sharding).
    hash_ring: Option<ConsistentHashRing>,
}

impl RoutingResolver {
    /// Build a resolver from a routing configuration and an optional organization
    /// context used to construct `EndpointCacheUuid` values.
    ///
    /// Returns an error if the routing configuration is invalid (e.g. empty shards).
    pub fn new(routing: &EndpointRouting, org: Option<&OrganizationCacheUuid>) -> Result<Self, EpError> {
        routing.validate()?;

        // Safe to unwrap: validate() ensures shards are non-empty.
        let primary_uuid = routing.primary_endpoint().ok_or_else(|| EpError::parse("routing has no primary endpoint"))?;
        let primary = EndpointCacheUuid::new(org.cloned(), primary_uuid.clone());

        let replicas = match routing {
            EndpointRouting::ReadReplica { replicas, .. } => {
                replicas.iter().map(|r| EndpointCacheUuid::new(org.cloned(), r.clone())).collect()
            }
            _ => Vec::new(),
        };

        let (shards, shard_ranges) = match routing {
            EndpointRouting::Sharded { shards, .. } => {
                let resolved: Vec<ResolvedShard> = shards
                    .iter()
                    .map(|s| ResolvedShard {
                        primary: EndpointCacheUuid::new(org.cloned(), s.endpoint.clone()),
                        replicas: Vec::new(),
                        replica_strategy: ReplicaStrategy::RoundRobin,
                        replica_counter: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                    })
                    .collect();
                let ranges: Vec<Option<ShardRange>> = shards.iter().map(|s| s.range.clone()).collect();
                (resolved, ranges)
            }
            EndpointRouting::ShardedWithReplicas { shards, .. } => {
                let resolved: Vec<ResolvedShard> = shards
                    .iter()
                    .map(|s| ResolvedShard {
                        primary: EndpointCacheUuid::new(org.cloned(), s.primary.clone()),
                        replicas: s.replicas.iter().map(|r| EndpointCacheUuid::new(org.cloned(), r.clone())).collect(),
                        replica_strategy: s.replica_strategy.clone(),
                        replica_counter: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                    })
                    .collect();
                let ranges: Vec<Option<ShardRange>> = shards.iter().map(|s| s.range.clone()).collect();
                (resolved, ranges)
            }
            _ => (Vec::new(), Vec::new()),
        };

        // Build consistent hash ring if the sharding rule is ConsistentHash.
        let hash_ring = match routing {
            EndpointRouting::Sharded { rule: ShardingRule::ConsistentHash { hash }, shards } => {
                Some(ConsistentHashRing::new(shards.len(), hash))
            }
            EndpointRouting::ShardedWithReplicas { rule: ShardingRule::ConsistentHash { hash }, shards } => {
                Some(ConsistentHashRing::new(shards.len(), hash))
            }
            _ => None,
        };

        Ok(Self {
            primary,
            routing: std::sync::Arc::new(routing.clone()),
            replicas,
            replica_counter: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            shards,
            shard_ranges,
            hash_ring,
        })
    }

    /// Returns the primary endpoint.
    pub fn primary(&self) -> &EndpointCacheUuid {
        &self.primary
    }

    /// Returns the routing configuration.
    pub fn routing(&self) -> &EndpointRouting {
        &self.routing
    }

    /// Resolve the shard index for a key, using the pre-built consistent hash ring
    /// for `ConsistentHash` rules or delegating to `ShardingRule::resolve_shard` otherwise.
    fn resolve_shard_idx(&self, rule: &ShardingRule, key: &[u8]) -> usize {
        if let Some(ref ring) = self.hash_ring {
            ring.resolve(key)
        } else {
            rule.resolve_shard(key, &self.shard_ranges, self.shards.len())
        }
    }

    /// Select the endpoint for a command based on routing strategy, optional key,
    /// and read/write intent.
    ///
    /// - **Direct**: always returns primary.
    /// - **ReadReplica**: reads go to a replica (via strategy), writes to primary.
    /// - **Sharded**: resolves shard from key, returns shard primary.
    /// - **ShardedWithReplicas**: resolves shard, reads go to shard replica, writes to shard primary.
    pub fn select_endpoint(&self, key: Option<&[u8]>, is_read: bool) -> &EndpointCacheUuid {
        match self.routing.as_ref() {
            EndpointRouting::Direct { .. } => &self.primary,

            EndpointRouting::ReadReplica { .. } => {
                if is_read && !self.replicas.is_empty() {
                    Self::pick_replica(&self.replicas, &self.routing, &self.replica_counter)
                } else {
                    &self.primary
                }
            }

            EndpointRouting::Sharded { rule, .. } => {
                if self.shards.is_empty() {
                    return &self.primary;
                }
                let shard_idx = match key {
                    Some(k) => self.resolve_shard_idx(rule, k),
                    None => return &self.primary,
                };
                &self.shards[shard_idx].primary
            }

            EndpointRouting::ShardedWithReplicas { rule, .. } => {
                if self.shards.is_empty() {
                    return &self.primary;
                }
                let shard_idx = match key {
                    Some(k) => self.resolve_shard_idx(rule, k),
                    None => return &self.primary,
                };
                let shard = &self.shards[shard_idx];
                if is_read && !shard.replicas.is_empty() {
                    Self::pick_replica_from_shard(shard)
                } else {
                    &shard.primary
                }
            }
        }
    }

    /// Pick a replica from the top-level replicas list using the routing strategy.
    fn pick_replica<'a>(
        replicas: &'a [EndpointCacheUuid],
        routing: &EndpointRouting,
        counter: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> &'a EndpointCacheUuid {
        let strategy = match routing {
            EndpointRouting::ReadReplica { strategy, .. } => strategy,
            _ => return &replicas[0],
        };
        Self::pick_by_strategy(replicas, strategy, counter)
    }

    /// Pick a replica from a shard's replica list using the shard's strategy.
    fn pick_replica_from_shard(shard: &ResolvedShard) -> &EndpointCacheUuid {
        Self::pick_by_strategy(&shard.replicas, &shard.replica_strategy, &shard.replica_counter)
    }

    /// Select a replica endpoint based on the given strategy.
    fn pick_by_strategy<'a>(
        replicas: &'a [EndpointCacheUuid],
        strategy: &ReplicaStrategy,
        counter: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> &'a EndpointCacheUuid {
        match strategy {
            ReplicaStrategy::RoundRobin => {
                let idx = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % replicas.len();
                &replicas[idx]
            }
            ReplicaStrategy::Random => {
                use rand::Rng;
                let idx = rand::rng().random_range(0..replicas.len());
                &replicas[idx]
            }
            ReplicaStrategy::Weighted { weights } => {
                use rand::Rng;
                let total: f64 = weights.iter().map(|(_, w)| w).sum();
                if total <= 0.0 {
                    return &replicas[0];
                }
                let mut r = rand::rng().random::<f64>() * total;
                for (endpoint_uuid, w) in weights.iter() {
                    r -= w;
                    if r <= 0.0 {
                        // Match weight's endpoint UUID to a replica by UUID
                        let target_uuid: uuid::Uuid = endpoint_uuid.clone().into();
                        if let Some(replica) = replicas.iter().find(|r| r.uuid() == target_uuid) {
                            return replica;
                        }
                    }
                }
                // Fallback: last replica if no UUID match found
                &replicas[replicas.len() - 1]
            }
            // LeastConnections / LeastLatency / Performance require runtime metrics;
            // fall back to round-robin until per-endpoint metric tracking is implemented.
            _ => {
                let idx = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % replicas.len();
                &replicas[idx]
            }
        }
    }
}

/// Routing configuration input using string identifiers (resolved to UUIDs during creation).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
pub enum EndpointRoutingInput {
    Direct {
        endpoint: String,
    },
    ReadReplica {
        primary: String,
        replicas: Vec<String>,
        strategy: ReplicaStrategy,
    },
    Sharded {
        shards: Vec<ShardEndpointInput>,
        rule: ShardingRule,
    },
    ShardedWithReplicas {
        shards: Vec<ShardGroupInput>,
        rule: ShardingRule,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ShardEndpointInput {
    pub endpoint: String,
    pub range: Option<ShardRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ShardGroupInput {
    pub primary: String,
    pub replicas: Vec<String>,
    pub range: Option<ShardRange>,
    pub replica_strategy: ReplicaStrategy,
}

impl EndpointRoutingInput {
    /// Returns all endpoint string identifiers referenced by this routing input.
    pub fn all_endpoint_ids(&self) -> Vec<&str> {
        match self {
            Self::Direct { endpoint } => vec![endpoint.as_str()],
            Self::ReadReplica { primary, replicas, .. } => {
                let mut ids = vec![primary.as_str()];
                ids.extend(replicas.iter().map(|r| r.as_str()));
                ids
            }
            Self::Sharded { shards, .. } => shards.iter().map(|s| s.endpoint.as_str()).collect(),
            Self::ShardedWithReplicas { shards, .. } => {
                let mut ids = Vec::new();
                for group in shards {
                    ids.push(group.primary.as_str());
                    ids.extend(group.replicas.iter().map(|r| r.as_str()));
                }
                ids
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    enum KeylessLoadProfile {
        Consistent,
        Variable,
        Malicious,
    }

    impl KeylessLoadProfile {
        fn label(self) -> &'static str {
            match self {
                Self::Consistent => "consistent",
                Self::Variable => "variable",
                Self::Malicious => "malicious",
            }
        }
    }

    fn keyless_profiles() -> [KeylessLoadProfile; 3] {
        [
            KeylessLoadProfile::Consistent,
            KeylessLoadProfile::Variable,
            KeylessLoadProfile::Malicious,
        ]
    }

    fn keyless_request_mix(profile: KeylessLoadProfile) -> Vec<bool> {
        match profile {
            KeylessLoadProfile::Consistent => vec![true; 32],
            KeylessLoadProfile::Variable => vec![true, false, true, false, true, false, true, false],
            KeylessLoadProfile::Malicious => (0..128).map(|idx| idx % 3 == 0).collect(),
        }
    }

    fn sharded_resolver() -> RoutingResolver {
        let routing = EndpointRouting::Sharded {
            shards: vec![
                ShardEndpoint { endpoint: EndpointUuid::new_uuid(), range: None },
                ShardEndpoint { endpoint: EndpointUuid::new_uuid(), range: None },
                ShardEndpoint { endpoint: EndpointUuid::new_uuid(), range: None },
            ],
            rule: ShardingRule::Modulo { divisor: 3, hash: HashConfig::default() },
        };
        RoutingResolver::new(&routing, None).expect("build sharded resolver")
    }

    fn sharded_with_replicas_resolver() -> RoutingResolver {
        let routing = EndpointRouting::ShardedWithReplicas {
            shards: vec![
                ShardGroup {
                    primary: EndpointUuid::new_uuid(),
                    replicas: vec![EndpointUuid::new_uuid()],
                    range: None,
                    replica_strategy: ReplicaStrategy::RoundRobin,
                },
                ShardGroup {
                    primary: EndpointUuid::new_uuid(),
                    replicas: vec![EndpointUuid::new_uuid()],
                    range: None,
                    replica_strategy: ReplicaStrategy::RoundRobin,
                },
                ShardGroup {
                    primary: EndpointUuid::new_uuid(),
                    replicas: vec![EndpointUuid::new_uuid()],
                    range: None,
                    replica_strategy: ReplicaStrategy::RoundRobin,
                },
            ],
            rule: ShardingRule::Modulo { divisor: 3, hash: HashConfig::default() },
        };
        RoutingResolver::new(&routing, None).expect("build sharded-with-replicas resolver")
    }

    #[test]
    fn keyless_sharded_requests_all_route_to_shard_zero_primary() {
        let resolver = sharded_resolver();
        let shard_zero = resolver.primary().clone();

        for profile in keyless_profiles() {
            let selected: Vec<_> =
                keyless_request_mix(profile).into_iter().map(|is_read| resolver.select_endpoint(None, is_read).clone()).collect();

            assert!(
                selected.iter().all(|endpoint| endpoint == &shard_zero),
                "all keyless {} requests currently route to shard zero's primary endpoint",
                profile.label()
            );
        }
    }

    #[test]
    fn keyless_sharded_with_replicas_requests_still_stick_to_shard_zero_group() {
        let resolver = sharded_with_replicas_resolver();
        let shard_zero_primary = resolver.primary().clone();
        let shard_zero_read = resolver.select_endpoint(None, true).clone();

        for profile in keyless_profiles() {
            for is_read in keyless_request_mix(profile) {
                let selected = resolver.select_endpoint(None, is_read).clone();
                if is_read {
                    assert_eq!(
                        selected,
                        shard_zero_read,
                        "keyless {} reads currently stay on shard zero's replica group",
                        profile.label()
                    );
                } else {
                    assert_eq!(
                        selected,
                        shard_zero_primary,
                        "keyless {} writes currently stay on shard zero's primary",
                        profile.label()
                    );
                }
            }
        }
    }
}
