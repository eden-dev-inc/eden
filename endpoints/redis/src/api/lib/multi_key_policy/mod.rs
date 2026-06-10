//! Runtime command policy for multi-key Redis commands.
//!
//! Some Redis providers do not implement the full set of multi-key commands
//! that single-node Redis offers. To support those providers, Eden can be
//! configured with [`redis_core::MultiKeyExecution::Deconstruct`],
//! which intercepts multi-key commands and executes equivalent single-key
//! commands on the user's behalf.
//!
//! Deconstruction mode is a compatibility fallback, not a Redis semantic
//! upgrade or performance optimization. The supported v1 commands are chosen
//! because their response shapes can be reconstructed from independent
//! single-key replies, but splitting a command still loses the single-command
//! atomicity Redis would provide under concurrent writes.
//!
//! This module is the policy layer those execution paths consult. Given a
//! parsed RESP command (`RedisCommandArgs`), it answers four questions:
//!
//! 1. **Is this a multi-key command at all?** Single-key commands and
//!    same-key multi-arg commands (HMGET, HDEL, SMISMEMBER, ZMSCORE,
//!    BF.MEXISTS, …) are
//!    always classified as [`CommandClassification::Passthrough`] — Eden
//!    forwards them to Redis unchanged.
//! 2. **If multi-key, can it be safely deconstructed?** Commands whose
//!    multi-key form is observably equivalent to N independent single-key
//!    calls (MGET, JSON.MGET, DEL, EXISTS, TOUCH, UNLINK, WATCH) classify
//!    as [`CommandClassification::SupportedMultiKey`] with a pre-built
//!    [`SupportedSplit`].
//! 3. **If multi-key but unsafe, why?** Commands whose multi-key semantics
//!    cannot be preserved by per-key fan-out (aggregating reads, atomic
//!    multi-key writes, two-key moves, blocking first-non-empty reads,
//!    multi-key scripts, HyperLogLog merges) classify as
//!    [`CommandClassification::UnsupportedMultiKey`] with a structured
//!    [`RejectReason`]. [`rejection_error_bytes`] maps that classification
//!    to the Redis-style `-ERR …` reply used by deconstruction mode.
//! 4. **How are the per-key responses recombined?** Each
//!    [`SupportedSplit`] carries a [`ResponseCombiner`] that maps N RESP
//!    replies back to the original multi-key shape (sum of integers, array
//!    preserving nils, all-OK, …).
//!
//! # Exclusion list
//!
//! Multi-key commands that **cannot** be deconstructed safely:
//!
//! * **Aggregating reads** — `SUNION`, `SINTER`, `SDIFF`, `SINTERCARD`,
//!   `ZUNION`, `ZINTER`, `ZDIFF`, `ZINTERCARD`, `BITOP`, multi-key
//!   `PFCOUNT`, distinct-key `LCS`. Computed over multiple keys; not equal
//!   to per-key fan-out.
//! * **Aggregating writes / `*STORE`** — `SUNIONSTORE`, `SINTERSTORE`,
//!   `SDIFFSTORE`, `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE`,
//!   `ZRANGESTORE`, `GEOSEARCHSTORE`, `GEORADIUS … STORE|STOREDIST`,
//!   `GEORADIUSBYMEMBER … STORE|STOREDIST`, `CMS.MERGE`, `TDIGEST.MERGE`.
//!   Same as above plus a destination write.
//! * **HyperLogLog merge** — `PFMERGE` (atomic merge of HLL registers).
//! * **Atomic multi-key writes** — `MSET`, `MSETNX`, `JSON.MSET`,
//!   distinct-key `TS.MADD`. The all-or-nothing semantics are lost when
//!   split into per-key writes.
//! * **Two-key atomic moves / renames** — `RENAME`, `RENAMENX`, `COPY`,
//!   `LMOVE`, `BLMOVE`, `RPOPLPUSH`, `BRPOPLPUSH`, `SMOVE`,
//!   `MIGRATE … KEYS`, `SORT … STORE`, `TS.CREATERULE`, `TS.DELETERULE`.
//! * **Blocking first-non-empty across keys** — `BLPOP`, `BRPOP`, `LMPOP`,
//!   `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `ZMPOP`, `BZMPOP`, `XREAD`,
//!   `XREADGROUP`. Return from the first key with data and atomically
//!   consume.
//! * **Scripts/functions with `numkeys > 1`** — `EVAL`, `EVALSHA`,
//!   `EVAL_RO`, `EVALSHA_RO`, `FCALL`, `FCALL_RO`. Scripts execute
//!   atomically across all referenced keys.
//!
//! # Out of scope (today)
//!
//! * **`MULTI`/`EXEC` transactions.** Inside an open transaction the user
//!   has opted into atomicity guarantees the deconstructor cannot preserve.
//!   Execution suppresses deconstruction inside `MULTI` and rejects split or
//!   unsupported multi-key commands so they never reach Redis. The proxy treats
//!   such local rejections as transaction queue errors and returns `EXECABORT`
//!   on the following `EXEC`. The legacy pinned raw API returns only bytes and
//!   does not own queue-error lifecycle; proxy callers use the reporting
//!   variant when they need to update transaction state.
//! * **Sharded migration.** The Redis proxy can fan out deconstructed
//!   commands across shards on its no-migration path. Migration routing still
//!   targets a single old/new endpoint pair and does not add cross-shard
//!   deconstruction semantics.
//!
//! # Read/write classification
//!
//! Independent of this module. [`crate::api::RedisApi::request_type`]
//! continues to classify each command as a read or a write; deconstruction
//! never flips that classification.

pub use self::classify::classify;
pub use self::combine::{RespWireVersion, ResponseCombiner};
pub use self::execute::{FrameAction, plan_frame, plan_pipeline};
pub use self::key_layout::{KeyLayout, key_positions};

mod classify;
mod combine;
mod execute;
mod key_layout;
mod split;

#[cfg(test)]
mod integration_tests;

use bytes::Bytes;

/// The result of classifying a parsed RESP command for deconstruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandClassification {
    /// Forward the command to Redis unchanged. This covers:
    /// * commands not relevant to multi-key handling (PING, AUTH, …),
    /// * single-key commands (GET, SET, …),
    /// * same-key multi-arg commands (HMGET, HDEL, SMISMEMBER, ZMSCORE,
    ///   BF.MEXISTS, …)
    ///   that look multi-arg but operate on a single Redis key.
    Passthrough,

    /// A multi-key command that can be deconstructed into N single-key
    /// commands. The split is pre-encoded; the combiner knows how to
    /// recompose the responses.
    SupportedMultiKey(SupportedSplit),

    /// A multi-key command that cannot be safely deconstructed.
    ///
    /// Execution paths should use [`rejection_error_bytes`] to format a
    /// Redis-style `-ERR …` reply and reject before sending anything to
    /// Redis.
    UnsupportedMultiKey {
        /// Why the command was rejected. See [`RejectReason`].
        reason: RejectReason,
    },
}

/// A pre-encoded plan for executing a deconstructed multi-key command.
///
/// `parts` contains one RESP-encoded single-key command per key, in the
/// same order as the original command's keys. `combiner` describes how the
/// per-key replies are merged back into the multi-key reply shape.
/// `constraint` tells the execution layer whether the parts can
/// be pipelined across the pool or must run on the same pinned connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportedSplit {
    /// Pre-encoded RESP for each per-key sub-command, in original key
    /// order. Length equals the original command's key count.
    pub parts: Vec<Bytes>,

    /// How to combine N per-key replies into one multi-key reply.
    pub combiner: ResponseCombiner,

    /// Routing requirement for the per-key sub-commands.
    pub constraint: ExecutionConstraint,
}

/// Routing requirement for a deconstructed multi-key command's parts.
///
/// Execution paths read this to decide whether to pipeline the parts in
/// parallel or serialize them on a single connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionConstraint {
    /// Parts may be sent on any connection from the pool, in any order
    /// (subject to combiner-ordering invariants on the response).
    AnyConnection,

    /// All parts must run on the same connection. Used by `WATCH`, whose
    /// effect is per-connection state.
    SameConnection,
}

/// The reason a multi-key command was rejected as not deconstructible.
///
/// Execution paths surface unsupported commands to the client as the fixed
/// Redis-style error returned by [`rejection_error_bytes`]. The variant keeps
/// policy tests and future telemetry reviewable in one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// Aggregating read across multiple keys.
    /// `SINTER`, `SUNION`, `SDIFF`, `SINTERCARD`, `ZINTER`, `ZUNION`,
    /// `ZDIFF`, `ZINTERCARD`, `BITOP`, multi-key `PFCOUNT`, distinct-key
    /// `LCS`.
    AggregatingRead,

    /// Aggregating write that targets a destination key.
    /// `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE`, `ZUNIONSTORE`,
    /// `ZINTERSTORE`, `ZDIFFSTORE`, `ZRANGESTORE`, `GEOSEARCHSTORE`,
    /// `GEORADIUS … STORE|STOREDIST`,
    /// `GEORADIUSBYMEMBER … STORE|STOREDIST`, `CMS.MERGE`, `TDIGEST.MERGE`.
    AggregatingWriteStore,

    /// Atomic write across multiple keys.
    /// `MSET`, `MSETNX`, `JSON.MSET`, distinct-key `TS.MADD`.
    AtomicMultiKeyWrite,

    /// Atomic move/copy/rename between two keys.
    /// `RENAME`, `RENAMENX`, `COPY`, `LMOVE`, `BLMOVE`, `RPOPLPUSH`,
    /// `BRPOPLPUSH`, `SMOVE`, `MIGRATE … KEYS`, `SORT … STORE`,
    /// `TS.CREATERULE`, `TS.DELETERULE`.
    TwoKeyMove,

    /// Blocking read that returns from the first non-empty key.
    /// `BLPOP`, `BRPOP`, `LMPOP`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`,
    /// `ZMPOP`, `BZMPOP`, `XREAD`, `XREADGROUP`.
    BlockingFirstNonEmpty,

    /// HyperLogLog merge of register state across multiple keys.
    /// `PFMERGE`.
    HllSemantics,

    /// Script or function call with `numkeys > 1`.
    /// `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `FCALL`, `FCALL_RO`.
    ScriptMultiKey,
}

/// RESP error returned for unsupported multi-key commands in deconstruction
/// mode.
pub const UNSUPPORTED_MULTI_KEY_ERROR_BYTES: &[u8] = b"-ERR multi-key command is not supported by this endpoint configuration\r\n";

/// Returns the Redis-style rejection response for unsupported multi-key
/// classifications.
pub fn rejection_error_bytes(classification: &CommandClassification) -> Option<Bytes> {
    matches!(classification, CommandClassification::UnsupportedMultiKey { .. })
        .then_some(Bytes::from_static(UNSUPPORTED_MULTI_KEY_ERROR_BYTES))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejection_error_bytes_only_applies_to_unsupported_multikey() {
        assert_eq!(
            rejection_error_bytes(&CommandClassification::UnsupportedMultiKey { reason: RejectReason::AggregatingRead }),
            Some(Bytes::from_static(b"-ERR multi-key command is not supported by this endpoint configuration\r\n")),
        );
        assert_eq!(rejection_error_bytes(&CommandClassification::Passthrough), None);
    }
}
