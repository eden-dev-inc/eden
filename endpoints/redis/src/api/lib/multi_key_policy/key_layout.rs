//! Per-`RedisApi` description of where keys live in a command's argument
//! list.
//!
//! [`key_positions`] returns a [`KeyLayout`] for each known command. The
//! layout is consumed by [`super::classify`] to compute the effective key
//! count and to drive deconstruction; it is also the canonical place to
//! extend if new multi-key commands are added.
//!
//! Same-key multi-arg commands like `HMGET`, `HDEL`, `SMISMEMBER`,
//! `ZMSCORE`, and `BF.MEXISTS` map to [`KeyLayout::SingleAtPos`] — they
//! look multi-arg in the wire format but target a single Redis key, so
//! deconstruction does not apply.

use crate::api::RedisApi;
use crate::protocol::decoder::RedisCommandArgs;

/// Where the keys live in a parsed command's argument list.
///
/// The layout is used both to compute the effective key count for
/// classification and to drive per-key splitting for supported commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyLayout {
    /// The command does not take a key (`PING`, `AUTH`, `CLUSTER INFO`,
    /// …) or its key is not relevant to multi-key handling.
    None,

    /// The command targets a single Redis key at the given argument
    /// position. Used both for genuinely single-key commands (`GET`,
    /// `SET`, `INCR`, `JSON.GET`) and for same-key multi-arg commands
    /// (`HMGET`, `HDEL`, `SMISMEMBER`, `ZMSCORE`, `BF.MEXISTS`, …).
    SingleAtPos(usize),

    /// Every argument is a key. Used by `MGET`, `DEL`, `EXISTS`, `TOUCH`,
    /// `UNLINK`, `WATCH`, and the aggregating-read family (`SDIFF`,
    /// `SINTER`, `SUNION`, multi-key `PFCOUNT`, `PFMERGE`).
    AllArgs,

    /// All arguments except the last are keys; the last argument is a
    /// shared trailing parameter. Used by `JSON.MGET key... path`.
    AllArgsExceptTrailing,

    /// Keys and values alternate from `start` with the given `stride`.
    /// `MSET` / `MSETNX` use `start: 0, stride: 2` — key, value, key,
    /// value, …
    AlternatingFromPos { start: usize, stride: usize },

    /// The first argument is a destination key; the remaining arguments
    /// are source keys. Used by `*STORE` aggregating writes and `BITOP`.
    FirstThenRest,

    /// Two keys at fixed positions; everything else is a value or
    /// modifier. Used by `RENAME`, `RENAMENX`, `COPY`, `SMOVE`,
    /// `LMOVE`/`BLMOVE`, `RPOPLPUSH`/`BRPOPLPUSH`.
    TwoFixed { src: usize, dst: usize },

    /// `numkeys` is at the given argument position; the next `numkeys`
    /// arguments are keys. Used by `EVAL`, `EVALSHA`, `EVAL_RO`,
    /// `EVALSHA_RO`, `FCALL`, `FCALL_RO`, and the explicit-keys family
    /// `LMPOP`, `BLMPOP`, `ZMPOP`, `BZMPOP`, `SINTERCARD`,
    /// `ZINTERCARD`, `ZINTER`, `ZUNION`, `ZDIFF`.
    NumKeysAt(usize),

    /// Layout that needs runtime parsing of the args (e.g. `MIGRATE …
    /// KEYS k1 k2`, `XREAD STREAMS k1 k2 id1 id2`, `GEORADIUS … STORE
    /// dst`, `GEOSEARCHSTORE dst src …`, `ZRANGESTORE dst src …`,
    /// `BLPOP/BRPOP key... timeout`, `SORT key … STORE dst`,
    /// `TS.MADD key timestamp value ...`). Classification handles each
    /// command explicitly.
    Custom,
}

/// Returns the layout for `args.command()`. Unknown commands return
/// [`KeyLayout::None`].
pub fn key_positions(args: &RedisCommandArgs) -> KeyLayout {
    use RedisApi::*;
    match args.command() {
        // ------------------------------------------------------------------
        // Supported deconstructible multi-key commands.
        // ------------------------------------------------------------------
        Mget | Del | Exists | Touch | Unlink | Watch => KeyLayout::AllArgs,
        JsonMget => KeyLayout::AllArgsExceptTrailing,

        // ------------------------------------------------------------------
        // Atomic multi-key writes (rejected).
        // ------------------------------------------------------------------
        Mset | Msetnx => KeyLayout::AlternatingFromPos { start: 0, stride: 2 },
        JsonMset => KeyLayout::AlternatingFromPos { start: 0, stride: 3 },
        TsMadd => KeyLayout::Custom,

        // ------------------------------------------------------------------
        // Aggregating reads (rejected when multi-key).
        // ------------------------------------------------------------------
        Sdiff | Sinter | Sunion | Pfcount | Pfmerge => KeyLayout::AllArgs,
        Sintercard | Zinter | Zunion | Zdiff | Zintercard => KeyLayout::NumKeysAt(0),
        Lcs => KeyLayout::TwoFixed { src: 0, dst: 1 },

        // ------------------------------------------------------------------
        // Aggregating writes / *STORE family (rejected).
        // ------------------------------------------------------------------
        Sdiffstore | Sinterstore | Sunionstore => KeyLayout::FirstThenRest,
        Bitop => KeyLayout::Custom,                                  // BITOP <op> <dest> <src...>
        Zdiffstore | Zinterstore | Zunionstore => KeyLayout::Custom, // dst, numkeys, key...
        Zrangestore | Geosearchstore => KeyLayout::TwoFixed { src: 1, dst: 0 },
        Georadius | Georadiusbymember => KeyLayout::Custom, // key ... [STORE|STOREDIST dst]
        CmsMerge | TdigestMerge => KeyLayout::Custom,       // dst, numkeys, key...

        // ------------------------------------------------------------------
        // Two-key moves / renames (rejected when distinct keys).
        // ------------------------------------------------------------------
        Rename | Renamenx | Copy | Lmove | Blmove | Rpoplpush | Brpoplpush | Smove => KeyLayout::TwoFixed { src: 0, dst: 1 },
        Migrate => KeyLayout::Custom, // MIGRATE host port key|"" ... [KEYS key ...]
        Sort => KeyLayout::Custom,    // SORT key ... [STORE dest]
        TsCreaterule | TsDeleterule => KeyLayout::TwoFixed { src: 0, dst: 1 },

        // ------------------------------------------------------------------
        // Blocking first-non-empty (rejected when multi-key).
        // ------------------------------------------------------------------
        Blpop | Brpop | Bzpopmin | Bzpopmax => KeyLayout::Custom, // key... timeout
        Lmpop | Blmpop | Zmpop | Bzmpop => KeyLayout::Custom,     // [timeout] numkeys key... ...
        Xread | Xreadgroup => KeyLayout::Custom,                  // ... STREAMS key... id...

        // ------------------------------------------------------------------
        // Scripts / functions (rejected when numkeys > 1).
        // ------------------------------------------------------------------
        Eval | EvalRo | Evalsha | EvalshaRo => KeyLayout::NumKeysAt(1),
        Fcall | FcallRo => KeyLayout::NumKeysAt(1),

        // ------------------------------------------------------------------
        // Same-key multi-arg commands — always passthrough.
        //
        // These look multi-arg on the wire but target one Redis key.
        // ------------------------------------------------------------------
        Hmget | Hdel | Smismember | Zmscore => KeyLayout::SingleAtPos(0),
        BfMexists | CfMexists => KeyLayout::SingleAtPos(0),

        // ------------------------------------------------------------------
        // Anything else: not relevant to multi-key handling. Single-key
        // commands fall here too — classification treats `None` and
        // `SingleAtPos` identically (Passthrough), so we don't enumerate
        // the entire single-key inventory.
        // ------------------------------------------------------------------
        _ => KeyLayout::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::value::RedisJsonValue;

    fn args_with(cmd: RedisApi, n: usize) -> RedisCommandArgs {
        let args: Vec<RedisJsonValue> = (0..n).map(|i| RedisJsonValue::String(format!("k{i}"))).collect();
        RedisCommandArgs::new(cmd, args)
    }

    #[test]
    fn supported_multikey_layouts() {
        assert_eq!(key_positions(&args_with(RedisApi::Mget, 3)), KeyLayout::AllArgs);
        assert_eq!(key_positions(&args_with(RedisApi::Del, 3)), KeyLayout::AllArgs);
        assert_eq!(key_positions(&args_with(RedisApi::Exists, 3)), KeyLayout::AllArgs);
        assert_eq!(key_positions(&args_with(RedisApi::Touch, 3)), KeyLayout::AllArgs);
        assert_eq!(key_positions(&args_with(RedisApi::Unlink, 3)), KeyLayout::AllArgs);
        assert_eq!(key_positions(&args_with(RedisApi::Watch, 3)), KeyLayout::AllArgs);
        assert_eq!(key_positions(&args_with(RedisApi::JsonMget, 3)), KeyLayout::AllArgsExceptTrailing);
    }

    #[test]
    fn atomic_multikey_write_layouts() {
        assert_eq!(key_positions(&args_with(RedisApi::Mset, 4)), KeyLayout::AlternatingFromPos { start: 0, stride: 2 });
        assert_eq!(
            key_positions(&args_with(RedisApi::Msetnx, 4)),
            KeyLayout::AlternatingFromPos { start: 0, stride: 2 }
        );
        assert_eq!(
            key_positions(&args_with(RedisApi::JsonMset, 6)),
            KeyLayout::AlternatingFromPos { start: 0, stride: 3 }
        );
        assert_eq!(key_positions(&args_with(RedisApi::TsMadd, 6)), KeyLayout::Custom);
    }

    #[test]
    fn aggregating_read_layouts() {
        assert_eq!(key_positions(&args_with(RedisApi::Sinter, 3)), KeyLayout::AllArgs);
        assert_eq!(key_positions(&args_with(RedisApi::Sunion, 3)), KeyLayout::AllArgs);
        assert_eq!(key_positions(&args_with(RedisApi::Sdiff, 3)), KeyLayout::AllArgs);
        assert_eq!(key_positions(&args_with(RedisApi::Pfcount, 3)), KeyLayout::AllArgs);
        assert_eq!(key_positions(&args_with(RedisApi::Pfmerge, 3)), KeyLayout::AllArgs);
        assert_eq!(key_positions(&args_with(RedisApi::Sintercard, 3)), KeyLayout::NumKeysAt(0));
        assert_eq!(key_positions(&args_with(RedisApi::Zinter, 3)), KeyLayout::NumKeysAt(0));
        assert_eq!(key_positions(&args_with(RedisApi::Zunion, 3)), KeyLayout::NumKeysAt(0));
        assert_eq!(key_positions(&args_with(RedisApi::Zdiff, 3)), KeyLayout::NumKeysAt(0));
        assert_eq!(key_positions(&args_with(RedisApi::Lcs, 2)), KeyLayout::TwoFixed { src: 0, dst: 1 });
    }

    #[test]
    fn store_family_layouts() {
        assert_eq!(key_positions(&args_with(RedisApi::Sinterstore, 3)), KeyLayout::FirstThenRest);
        assert_eq!(key_positions(&args_with(RedisApi::Sunionstore, 3)), KeyLayout::FirstThenRest);
        assert_eq!(key_positions(&args_with(RedisApi::Sdiffstore, 3)), KeyLayout::FirstThenRest);
        assert_eq!(key_positions(&args_with(RedisApi::Bitop, 4)), KeyLayout::Custom);
        assert_eq!(key_positions(&args_with(RedisApi::Zinterstore, 4)), KeyLayout::Custom);
        assert_eq!(key_positions(&args_with(RedisApi::Zrangestore, 4)), KeyLayout::TwoFixed { src: 1, dst: 0 });
        assert_eq!(key_positions(&args_with(RedisApi::Geosearchstore, 4)), KeyLayout::TwoFixed { src: 1, dst: 0 });
        assert_eq!(key_positions(&args_with(RedisApi::Georadius, 7)), KeyLayout::Custom);
        assert_eq!(key_positions(&args_with(RedisApi::Georadiusbymember, 6)), KeyLayout::Custom);
    }

    #[test]
    fn two_key_move_layouts() {
        assert_eq!(key_positions(&args_with(RedisApi::Rename, 2)), KeyLayout::TwoFixed { src: 0, dst: 1 });
        assert_eq!(key_positions(&args_with(RedisApi::Renamenx, 2)), KeyLayout::TwoFixed { src: 0, dst: 1 });
        assert_eq!(key_positions(&args_with(RedisApi::Copy, 2)), KeyLayout::TwoFixed { src: 0, dst: 1 });
        assert_eq!(key_positions(&args_with(RedisApi::Lmove, 4)), KeyLayout::TwoFixed { src: 0, dst: 1 });
        assert_eq!(key_positions(&args_with(RedisApi::Smove, 3)), KeyLayout::TwoFixed { src: 0, dst: 1 });
    }

    #[test]
    fn blocking_first_non_empty_layouts() {
        assert_eq!(key_positions(&args_with(RedisApi::Blpop, 3)), KeyLayout::Custom);
        assert_eq!(key_positions(&args_with(RedisApi::Brpop, 3)), KeyLayout::Custom);
        assert_eq!(key_positions(&args_with(RedisApi::Lmpop, 4)), KeyLayout::Custom);
        assert_eq!(key_positions(&args_with(RedisApi::Xread, 4)), KeyLayout::Custom);
    }

    #[test]
    fn script_layouts() {
        assert_eq!(key_positions(&args_with(RedisApi::Eval, 4)), KeyLayout::NumKeysAt(1));
        assert_eq!(key_positions(&args_with(RedisApi::Evalsha, 4)), KeyLayout::NumKeysAt(1));
        assert_eq!(key_positions(&args_with(RedisApi::Fcall, 4)), KeyLayout::NumKeysAt(1));
    }

    #[test]
    fn same_key_multiarg_layouts() {
        assert_eq!(key_positions(&args_with(RedisApi::Hmget, 3)), KeyLayout::SingleAtPos(0));
        assert_eq!(key_positions(&args_with(RedisApi::Hdel, 3)), KeyLayout::SingleAtPos(0));
        assert_eq!(key_positions(&args_with(RedisApi::Smismember, 3)), KeyLayout::SingleAtPos(0));
        assert_eq!(key_positions(&args_with(RedisApi::Zmscore, 3)), KeyLayout::SingleAtPos(0));
        assert_eq!(key_positions(&args_with(RedisApi::BfMexists, 3)), KeyLayout::SingleAtPos(0));
        assert_eq!(key_positions(&args_with(RedisApi::CfMexists, 3)), KeyLayout::SingleAtPos(0));
    }

    #[test]
    fn unrelated_commands_have_no_layout() {
        assert_eq!(key_positions(&args_with(RedisApi::Get, 1)), KeyLayout::None);
        assert_eq!(key_positions(&args_with(RedisApi::Set, 2)), KeyLayout::None);
        assert_eq!(key_positions(&args_with(RedisApi::Ping, 0)), KeyLayout::None);
    }
}
