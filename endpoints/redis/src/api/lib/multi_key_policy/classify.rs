use super::key_layout::{KeyLayout, key_positions};
use super::split::split_supported;
use super::{CommandClassification, RejectReason};
use crate::api::{RedisApi, RedisJsonValue};
use crate::protocol::decoder::RedisCommandArgs;

pub fn classify(args: &RedisCommandArgs) -> CommandClassification {
    if is_supported_command(args.command()) {
        return classify_supported(args);
    }

    if let Some(reason) = reject_reason(args) {
        return CommandClassification::UnsupportedMultiKey { reason };
    }

    CommandClassification::Passthrough
}

fn classify_supported(args: &RedisCommandArgs) -> CommandClassification {
    if effective_key_count(args) < Some(2) {
        return CommandClassification::Passthrough;
    }

    match split_supported(args) {
        Some(split) => CommandClassification::SupportedMultiKey(split),
        None => CommandClassification::Passthrough,
    }
}

fn is_supported_command(command: &RedisApi) -> bool {
    use RedisApi::*;
    matches!(command, Mget | JsonMget | Del | Exists | Touch | Unlink | Watch)
}

fn reject_reason(args: &RedisCommandArgs) -> Option<RejectReason> {
    use RedisApi::*;

    match args.command() {
        Mset | Msetnx | JsonMset | TsMadd => atomic_write_is_multikey(args).then_some(RejectReason::AtomicMultiKeyWrite),
        Sdiff | Sinter | Sunion | Sintercard | Zdiff | Zinter | Zunion | Zintercard => {
            multi_key(args).then_some(RejectReason::AggregatingRead)
        }
        Pfcount => multi_key(args).then_some(RejectReason::AggregatingRead),
        Bitop => bitop_is_multikey(args).then_some(RejectReason::AggregatingRead),
        Lcs => two_fixed_keys_are_distinct(args).then_some(RejectReason::AggregatingRead),
        Sdiffstore | Sinterstore | Sunionstore | Zdiffstore | Zinterstore | Zunionstore | Zrangestore | Geosearchstore | CmsMerge
        | TdigestMerge => aggregating_store_is_multikey(args).then_some(RejectReason::AggregatingWriteStore),
        Georadius | Georadiusbymember => geospatial_store_is_multikey(args).then_some(RejectReason::AggregatingWriteStore),
        Pfmerge => multi_key(args).then_some(RejectReason::HllSemantics),
        Rename | Renamenx | Copy | Lmove | Blmove | Rpoplpush | Brpoplpush | Smove | TsCreaterule | TsDeleterule => {
            two_fixed_keys_are_distinct(args).then_some(RejectReason::TwoKeyMove)
        }
        Migrate => migrate_has_keys(args).then_some(RejectReason::TwoKeyMove),
        Sort => sort_has_store(args).then_some(RejectReason::TwoKeyMove),
        Blpop | Brpop | Bzpopmin | Bzpopmax | Lmpop | Blmpop | Zmpop | Bzmpop | Xread | Xreadgroup => {
            blocking_command_is_multikey(args).then_some(RejectReason::BlockingFirstNonEmpty)
        }
        Eval | EvalRo | Evalsha | EvalshaRo | Fcall | FcallRo => script_is_multikey(args).then_some(RejectReason::ScriptMultiKey),
        _ => None,
    }
}

fn multi_key(args: &RedisCommandArgs) -> bool {
    matches!(effective_key_count(args), Some(count) if count > 1)
}

fn effective_key_count(args: &RedisCommandArgs) -> Option<usize> {
    let values = args.args();
    match key_positions(args) {
        KeyLayout::None => Some(0),
        KeyLayout::SingleAtPos(pos) => Some(usize::from(values.len() > pos)),
        KeyLayout::AllArgs => Some(values.len()),
        KeyLayout::AllArgsExceptTrailing => values.len().checked_sub(1),
        KeyLayout::AlternatingFromPos { start, stride } => alternating_count(values, start, stride),
        KeyLayout::FirstThenRest => Some(values.len()),
        KeyLayout::TwoFixed { src, dst } => two_fixed_count(values, src, dst),
        KeyLayout::NumKeysAt(pos) => numkeys_count(values, pos),
        KeyLayout::Custom => custom_key_count(args),
    }
}

fn alternating_count(values: &[RedisJsonValue], start: usize, stride: usize) -> Option<usize> {
    if values.len() <= start || stride == 0 {
        return None;
    }

    let remaining = values.len() - start;
    if !remaining.is_multiple_of(stride) {
        return None;
    }

    Some(remaining / stride)
}

fn two_fixed_count(values: &[RedisJsonValue], src: usize, dst: usize) -> Option<usize> {
    if values.len() <= src || values.len() <= dst {
        return None;
    }

    if values[src] == values[dst] {
        return Some(1);
    }

    Some(2)
}

fn numkeys_count(values: &[RedisJsonValue], pos: usize) -> Option<usize> {
    let count = parse_nonnegative_usize(values.get(pos)?)?;
    let first_key = pos + 1;
    if values.len() < first_key + count {
        return None;
    }

    Some(count)
}

fn custom_key_count(args: &RedisCommandArgs) -> Option<usize> {
    use RedisApi::*;
    match args.command() {
        Bitop => bitop_key_count(args.args()),
        Zdiffstore | Zinterstore | Zunionstore | CmsMerge | TdigestMerge => store_with_numkeys_count(args.args()),
        Migrate => migrate_key_count(args.args()),
        Sort => sort_key_count(args.args()),
        Georadius => geospatial_store_key_count(args.args(), 5),
        Georadiusbymember => geospatial_store_key_count(args.args(), 4),
        TsMadd => ts_madd_key_count(args.args()),
        Blpop | Brpop | Bzpopmin | Bzpopmax => trailing_timeout_key_count(args.args()),
        Lmpop | Zmpop => numkeys_count(args.args(), 0),
        Blmpop | Bzmpop => numkeys_count(args.args(), 1),
        Xread | Xreadgroup => streams_key_count(args.args()),
        _ => None,
    }
}

fn parse_nonnegative_usize(value: &RedisJsonValue) -> Option<usize> {
    match value {
        RedisJsonValue::Integer(value) => usize::try_from(*value).ok(),
        RedisJsonValue::String(value) => value.parse::<usize>().ok(),
        RedisJsonValue::Bytes(value) => std::str::from_utf8(value).ok()?.parse::<usize>().ok(),
        _ => None,
    }
}

fn value_eq_ignore_ascii_case(value: &RedisJsonValue, expected: &str) -> bool {
    match value {
        RedisJsonValue::String(value) => value.eq_ignore_ascii_case(expected),
        RedisJsonValue::Bytes(value) => std::str::from_utf8(value).is_ok_and(|value| value.eq_ignore_ascii_case(expected)),
        _ => false,
    }
}

fn atomic_write_is_multikey(args: &RedisCommandArgs) -> bool {
    use RedisApi::*;
    match args.command() {
        Mset | Msetnx => matches!(alternating_count(args.args(), 0, 2), Some(count) if count > 1),
        JsonMset => matches!(alternating_count(args.args(), 0, 3), Some(count) if count > 1),
        TsMadd => matches!(ts_madd_key_count(args.args()), Some(count) if count > 1),
        _ => false,
    }
}

fn two_fixed_keys_are_distinct(args: &RedisCommandArgs) -> bool {
    matches!(effective_key_count(args), Some(count) if count > 1)
}

fn bitop_key_count(values: &[RedisJsonValue]) -> Option<usize> {
    if values.len() < 3 {
        return None;
    }

    Some(values.len() - 1)
}

fn bitop_is_multikey(args: &RedisCommandArgs) -> bool {
    matches!(bitop_key_count(args.args()), Some(count) if count > 1)
}

fn aggregating_store_is_multikey(args: &RedisCommandArgs) -> bool {
    matches!(effective_key_count(args), Some(count) if count > 1)
}

fn store_with_numkeys_count(values: &[RedisJsonValue]) -> Option<usize> {
    if values.len() < 3 {
        return None;
    }

    let source_count = parse_nonnegative_usize(values.get(1)?)?;
    if values.len() < 2 + source_count {
        return None;
    }

    Some(source_count + 1)
}

fn migrate_key_count(values: &[RedisJsonValue]) -> Option<usize> {
    let keys_pos = migrate_keys_position(values)?;
    let key_count = values.len().checked_sub(keys_pos + 1)?;
    (key_count > 0).then_some(key_count)
}

fn migrate_keys_position(values: &[RedisJsonValue]) -> Option<usize> {
    if values.len() < 5 {
        return None;
    }

    let mut pos = 5;
    while pos < values.len() {
        if value_eq_ignore_ascii_case(&values[pos], "KEYS") {
            return Some(pos);
        }

        if value_eq_ignore_ascii_case(&values[pos], "COPY") || value_eq_ignore_ascii_case(&values[pos], "REPLACE") {
            pos += 1;
            continue;
        }

        if value_eq_ignore_ascii_case(&values[pos], "AUTH") {
            pos += 2;
            continue;
        }

        if value_eq_ignore_ascii_case(&values[pos], "AUTH2") {
            pos += 3;
            continue;
        }

        return None;
    }

    None
}

fn migrate_has_keys(args: &RedisCommandArgs) -> bool {
    matches!(migrate_key_count(args.args()), Some(count) if count > 0)
}

fn sort_key_count(values: &[RedisJsonValue]) -> Option<usize> {
    if values.is_empty() {
        return None;
    }

    let store_pos = sort_store_position(values)?;
    if values.len() <= store_pos + 1 {
        return None;
    }

    Some(2)
}

fn sort_store_position(values: &[RedisJsonValue]) -> Option<usize> {
    if values.is_empty() {
        return None;
    }

    let mut pos = 1;
    while pos < values.len() {
        if value_eq_ignore_ascii_case(&values[pos], "STORE") {
            return Some(pos);
        }

        if value_eq_ignore_ascii_case(&values[pos], "BY") || value_eq_ignore_ascii_case(&values[pos], "GET") {
            pos += 2;
            continue;
        }

        if value_eq_ignore_ascii_case(&values[pos], "LIMIT") {
            pos += 3;
            continue;
        }

        if value_eq_ignore_ascii_case(&values[pos], "ASC")
            || value_eq_ignore_ascii_case(&values[pos], "DESC")
            || value_eq_ignore_ascii_case(&values[pos], "ALPHA")
        {
            pos += 1;
            continue;
        }

        return None;
    }

    None
}

fn sort_has_store(args: &RedisCommandArgs) -> bool {
    matches!(sort_key_count(args.args()), Some(count) if count > 1)
}

fn geospatial_store_key_count(values: &[RedisJsonValue], option_start: usize) -> Option<usize> {
    let store_pos = geospatial_store_position(values, option_start)?;
    if values.len() <= store_pos + 1 {
        return None;
    }

    Some(2)
}

fn geospatial_store_position(values: &[RedisJsonValue], option_start: usize) -> Option<usize> {
    if values.len() <= option_start {
        return None;
    }

    let mut pos = option_start;
    while pos < values.len() {
        if value_eq_ignore_ascii_case(&values[pos], "STORE") || value_eq_ignore_ascii_case(&values[pos], "STOREDIST") {
            return Some(pos);
        }

        if value_eq_ignore_ascii_case(&values[pos], "COUNT") {
            pos += 2;
            if pos < values.len() && value_eq_ignore_ascii_case(&values[pos], "ANY") {
                pos += 1;
            }
            continue;
        }

        if value_eq_ignore_ascii_case(&values[pos], "WITHCOORD")
            || value_eq_ignore_ascii_case(&values[pos], "WITHCORD")
            || value_eq_ignore_ascii_case(&values[pos], "WITHDIST")
            || value_eq_ignore_ascii_case(&values[pos], "WITHHASH")
            || value_eq_ignore_ascii_case(&values[pos], "ASC")
            || value_eq_ignore_ascii_case(&values[pos], "DESC")
        {
            pos += 1;
            continue;
        }

        return None;
    }

    None
}

fn geospatial_store_is_multikey(args: &RedisCommandArgs) -> bool {
    matches!(effective_key_count(args), Some(count) if count > 1)
}

fn ts_madd_key_count(values: &[RedisJsonValue]) -> Option<usize> {
    if values.is_empty() || !values.len().is_multiple_of(3) {
        return None;
    }

    let mut keys: Vec<&RedisJsonValue> = Vec::new();
    for key in values.iter().step_by(3) {
        if !keys.contains(&key) {
            keys.push(key);
        }
    }

    Some(keys.len())
}

fn trailing_timeout_key_count(values: &[RedisJsonValue]) -> Option<usize> {
    values.len().checked_sub(1)
}

fn blocking_command_is_multikey(args: &RedisCommandArgs) -> bool {
    matches!(effective_key_count(args), Some(count) if count > 1)
}

fn streams_key_count(values: &[RedisJsonValue]) -> Option<usize> {
    let streams_pos = streams_position(values)?;
    let remaining = values.len().checked_sub(streams_pos + 1)?;
    if remaining == 0 || remaining % 2 != 0 {
        return None;
    }

    Some(remaining / 2)
}

fn streams_position(values: &[RedisJsonValue]) -> Option<usize> {
    let mut pos = 0;
    if values.first().is_some_and(|value| value_eq_ignore_ascii_case(value, "GROUP")) {
        if values.len() < 3 {
            return None;
        }
        pos = 3;
    }

    while pos < values.len() {
        if value_eq_ignore_ascii_case(&values[pos], "STREAMS") {
            return Some(pos);
        }

        if value_eq_ignore_ascii_case(&values[pos], "COUNT") || value_eq_ignore_ascii_case(&values[pos], "BLOCK") {
            pos += 2;
            continue;
        }

        if value_eq_ignore_ascii_case(&values[pos], "NOACK") {
            pos += 1;
            continue;
        }

        return None;
    }

    None
}

fn script_is_multikey(args: &RedisCommandArgs) -> bool {
    matches!(effective_key_count(args), Some(count) if count > 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::RedisApi;
    use crate::api::value::RedisJsonValue;

    fn args(command: RedisApi, values: &[&str]) -> RedisCommandArgs {
        RedisCommandArgs::new(command, values.iter().map(|value| RedisJsonValue::String((*value).to_string())).collect())
    }

    fn int_args(command: RedisApi, values: Vec<RedisJsonValue>) -> RedisCommandArgs {
        RedisCommandArgs::new(command, values)
    }

    fn assert_supported(command: RedisApi, values: &[&str]) {
        assert!(matches!(classify(&args(command, values)), CommandClassification::SupportedMultiKey(_)));
    }

    fn assert_passthrough(command: RedisApi, values: &[&str]) {
        assert_eq!(classify(&args(command, values)), CommandClassification::Passthrough);
    }

    fn assert_rejected(command: RedisApi, values: &[&str], reason: RejectReason) {
        assert_eq!(classify(&args(command, values)), CommandClassification::UnsupportedMultiKey { reason });
    }

    #[test]
    fn local_multikey_inventory_is_explicitly_classified() {
        let supported: &[(RedisApi, &[&str])] = &[
            (RedisApi::Mget, &["k1", "k2"]),
            (RedisApi::JsonMget, &["k1", "k2", "$"]),
            (RedisApi::Del, &["k1", "k2"]),
            (RedisApi::Exists, &["k1", "k2"]),
            (RedisApi::Touch, &["k1", "k2"]),
            (RedisApi::Unlink, &["k1", "k2"]),
            (RedisApi::Watch, &["k1", "k2"]),
        ];
        for (command, values) in supported {
            assert_supported(command.clone(), values);
        }

        let same_key_passthrough: &[(RedisApi, &[&str])] = &[
            (RedisApi::Hmget, &["hash", "f1", "f2"]),
            (RedisApi::Hdel, &["hash", "f1", "f2"]),
            (RedisApi::Smismember, &["set", "m1", "m2"]),
            (RedisApi::Zmscore, &["zset", "m1", "m2"]),
            (RedisApi::BfMexists, &["filter", "a", "b"]),
            (RedisApi::CfMexists, &["filter", "a", "b"]),
        ];
        for (command, values) in same_key_passthrough {
            assert_passthrough(command.clone(), values);
        }

        let rejected: &[(RedisApi, &[&str], RejectReason)] = &[
            (RedisApi::Mset, &["k1", "v1", "k2", "v2"], RejectReason::AtomicMultiKeyWrite),
            (RedisApi::Msetnx, &["k1", "v1", "k2", "v2"], RejectReason::AtomicMultiKeyWrite),
            (RedisApi::JsonMset, &["k1", "$", "{}", "k2", "$", "{}"], RejectReason::AtomicMultiKeyWrite),
            (RedisApi::TsMadd, &["k1", "1000", "1.0", "k2", "1001", "2.0"], RejectReason::AtomicMultiKeyWrite),
            (RedisApi::Sdiff, &["k1", "k2"], RejectReason::AggregatingRead),
            (RedisApi::Sinter, &["k1", "k2"], RejectReason::AggregatingRead),
            (RedisApi::Sunion, &["k1", "k2"], RejectReason::AggregatingRead),
            (RedisApi::Sintercard, &["2", "k1", "k2"], RejectReason::AggregatingRead),
            (RedisApi::Zdiff, &["2", "k1", "k2"], RejectReason::AggregatingRead),
            (RedisApi::Zinter, &["2", "k1", "k2"], RejectReason::AggregatingRead),
            (RedisApi::Zunion, &["2", "k1", "k2"], RejectReason::AggregatingRead),
            (RedisApi::Zintercard, &["2", "k1", "k2"], RejectReason::AggregatingRead),
            (RedisApi::Pfcount, &["k1", "k2"], RejectReason::AggregatingRead),
            (RedisApi::Bitop, &["AND", "dst", "k1", "k2"], RejectReason::AggregatingRead),
            (RedisApi::Lcs, &["k1", "k2"], RejectReason::AggregatingRead),
            (RedisApi::Sdiffstore, &["dst", "k1"], RejectReason::AggregatingWriteStore),
            (RedisApi::Sinterstore, &["dst", "k1"], RejectReason::AggregatingWriteStore),
            (RedisApi::Sunionstore, &["dst", "k1"], RejectReason::AggregatingWriteStore),
            (RedisApi::Zdiffstore, &["dst", "2", "k1", "k2"], RejectReason::AggregatingWriteStore),
            (RedisApi::Zinterstore, &["dst", "2", "k1", "k2"], RejectReason::AggregatingWriteStore),
            (RedisApi::Zunionstore, &["dst", "2", "k1", "k2"], RejectReason::AggregatingWriteStore),
            (RedisApi::Zrangestore, &["dst", "src", "0", "-1"], RejectReason::AggregatingWriteStore),
            (
                RedisApi::Geosearchstore,
                &["dst", "src", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "m"],
                RejectReason::AggregatingWriteStore,
            ),
            (
                RedisApi::Georadius,
                &["src", "0", "0", "1", "m", "STORE", "dst"],
                RejectReason::AggregatingWriteStore,
            ),
            (
                RedisApi::Georadiusbymember,
                &["src", "member", "1", "m", "STORE", "dst"],
                RejectReason::AggregatingWriteStore,
            ),
            (RedisApi::CmsMerge, &["dst", "2", "k1", "k2"], RejectReason::AggregatingWriteStore),
            (RedisApi::TdigestMerge, &["dst", "2", "k1", "k2"], RejectReason::AggregatingWriteStore),
            (RedisApi::Pfmerge, &["dst", "k1"], RejectReason::HllSemantics),
            (RedisApi::Rename, &["src", "dst"], RejectReason::TwoKeyMove),
            (RedisApi::Renamenx, &["src", "dst"], RejectReason::TwoKeyMove),
            (RedisApi::Copy, &["src", "dst"], RejectReason::TwoKeyMove),
            (RedisApi::Lmove, &["src", "dst", "LEFT", "RIGHT"], RejectReason::TwoKeyMove),
            (RedisApi::Blmove, &["src", "dst", "LEFT", "RIGHT", "1"], RejectReason::TwoKeyMove),
            (RedisApi::Rpoplpush, &["src", "dst"], RejectReason::TwoKeyMove),
            (RedisApi::Brpoplpush, &["src", "dst", "1"], RejectReason::TwoKeyMove),
            (RedisApi::Smove, &["src", "dst", "member"], RejectReason::TwoKeyMove),
            (RedisApi::Migrate, &["host", "6379", "", "0", "5000", "KEYS", "k1", "k2"], RejectReason::TwoKeyMove),
            (RedisApi::Sort, &["src", "STORE", "dst"], RejectReason::TwoKeyMove),
            (RedisApi::TsCreaterule, &["src", "dst", "AGGREGATION", "avg", "60000"], RejectReason::TwoKeyMove),
            (RedisApi::TsDeleterule, &["src", "dst"], RejectReason::TwoKeyMove),
            (RedisApi::Blpop, &["k1", "k2", "1"], RejectReason::BlockingFirstNonEmpty),
            (RedisApi::Brpop, &["k1", "k2", "1"], RejectReason::BlockingFirstNonEmpty),
            (RedisApi::Bzpopmin, &["k1", "k2", "1"], RejectReason::BlockingFirstNonEmpty),
            (RedisApi::Bzpopmax, &["k1", "k2", "1"], RejectReason::BlockingFirstNonEmpty),
            (RedisApi::Lmpop, &["2", "k1", "k2", "LEFT"], RejectReason::BlockingFirstNonEmpty),
            (RedisApi::Blmpop, &["1", "2", "k1", "k2", "LEFT"], RejectReason::BlockingFirstNonEmpty),
            (RedisApi::Zmpop, &["2", "k1", "k2", "MIN"], RejectReason::BlockingFirstNonEmpty),
            (RedisApi::Bzmpop, &["1", "2", "k1", "k2", "MIN"], RejectReason::BlockingFirstNonEmpty),
            (
                RedisApi::Xread,
                &["COUNT", "1", "STREAMS", "k1", "k2", "0", "0"],
                RejectReason::BlockingFirstNonEmpty,
            ),
            (
                RedisApi::Xreadgroup,
                &["GROUP", "g", "c", "STREAMS", "k1", "k2", ">", ">"],
                RejectReason::BlockingFirstNonEmpty,
            ),
            (RedisApi::Eval, &["return 1", "2", "k1", "k2"], RejectReason::ScriptMultiKey),
            (RedisApi::EvalRo, &["return 1", "2", "k1", "k2"], RejectReason::ScriptMultiKey),
            (RedisApi::Evalsha, &["sha", "2", "k1", "k2"], RejectReason::ScriptMultiKey),
            (RedisApi::EvalshaRo, &["sha", "2", "k1", "k2"], RejectReason::ScriptMultiKey),
            (RedisApi::Fcall, &["function", "2", "k1", "k2"], RejectReason::ScriptMultiKey),
            (RedisApi::FcallRo, &["function", "2", "k1", "k2"], RejectReason::ScriptMultiKey),
        ];
        for (command, values, reason) in rejected {
            assert_rejected(command.clone(), values, *reason);
        }
    }

    #[test]
    fn supported_multikey_commands_classify_with_splits() {
        assert_supported(RedisApi::Mget, &["k1", "k2"]);
        assert_supported(RedisApi::JsonMget, &["k1", "k2", "$"]);
        assert_supported(RedisApi::Del, &["k1", "k2"]);
        assert_supported(RedisApi::Exists, &["k1", "k2"]);
        assert_supported(RedisApi::Touch, &["k1", "k2"]);
        assert_supported(RedisApi::Unlink, &["k1", "k2"]);
        assert_supported(RedisApi::Watch, &["k1", "k2"]);
    }

    #[test]
    fn supported_single_key_forms_passthrough() {
        assert_eq!(classify(&args(RedisApi::Mget, &["k1"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::JsonMget, &["k1", "$"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::Watch, &["k1"])), CommandClassification::Passthrough);
    }

    #[test]
    fn same_key_multiarg_commands_passthrough() {
        assert_eq!(classify(&args(RedisApi::Hmget, &["hash", "f1", "f2"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::Hdel, &["hash", "f1", "f2"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::Smismember, &["set", "m1", "m2"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::Zmscore, &["zset", "m1", "m2"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::BfMexists, &["filter", "a", "b"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::CfMexists, &["filter", "a", "b"])), CommandClassification::Passthrough);
    }

    #[test]
    fn atomic_write_forms_reject_only_when_multikey() {
        assert_eq!(classify(&args(RedisApi::Mset, &["k1", "v1"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::Msetnx, &["k1", "v1"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::JsonMset, &["k1", "$", "{}"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::TsMadd, &["k1", "1000", "1.0"])), CommandClassification::Passthrough);
        assert_eq!(
            classify(&args(RedisApi::TsMadd, &["k1", "1000", "1.0", "k1", "1001", "2.0"])),
            CommandClassification::Passthrough
        );

        assert_rejected(RedisApi::Mset, &["k1", "v1", "k2", "v2"], RejectReason::AtomicMultiKeyWrite);
        assert_rejected(RedisApi::Msetnx, &["k1", "v1", "k2", "v2"], RejectReason::AtomicMultiKeyWrite);
        assert_rejected(RedisApi::JsonMset, &["k1", "$", "{}", "k2", "$", "{}"], RejectReason::AtomicMultiKeyWrite);
        assert_rejected(RedisApi::TsMadd, &["k1", "1000", "1.0", "k2", "1001", "2.0"], RejectReason::AtomicMultiKeyWrite);
    }

    #[test]
    fn rejects_aggregating_read_inventory() {
        assert_rejected(RedisApi::Sdiff, &["k1", "k2"], RejectReason::AggregatingRead);
        assert_rejected(RedisApi::Sinter, &["k1", "k2"], RejectReason::AggregatingRead);
        assert_rejected(RedisApi::Sunion, &["k1", "k2"], RejectReason::AggregatingRead);
        assert_rejected(RedisApi::Sintercard, &["2", "k1", "k2"], RejectReason::AggregatingRead);
        assert_rejected(RedisApi::Zdiff, &["2", "k1", "k2"], RejectReason::AggregatingRead);
        assert_rejected(RedisApi::Zinter, &["2", "k1", "k2"], RejectReason::AggregatingRead);
        assert_rejected(RedisApi::Zunion, &["2", "k1", "k2"], RejectReason::AggregatingRead);
        assert_rejected(RedisApi::Zintercard, &["2", "k1", "k2"], RejectReason::AggregatingRead);
        assert_rejected(RedisApi::Pfcount, &["k1", "k2"], RejectReason::AggregatingRead);
        assert_rejected(RedisApi::Bitop, &["AND", "dst", "k1", "k2"], RejectReason::AggregatingRead);
        assert_rejected(RedisApi::Lcs, &["k1", "k2"], RejectReason::AggregatingRead);
    }

    #[test]
    fn rejects_aggregating_store_inventory() {
        assert_rejected(RedisApi::Sdiffstore, &["dst", "k1"], RejectReason::AggregatingWriteStore);
        assert_rejected(RedisApi::Sinterstore, &["dst", "k1"], RejectReason::AggregatingWriteStore);
        assert_rejected(RedisApi::Sunionstore, &["dst", "k1"], RejectReason::AggregatingWriteStore);
        assert_rejected(RedisApi::Zdiffstore, &["dst", "2", "k1", "k2"], RejectReason::AggregatingWriteStore);
        assert_rejected(RedisApi::Zinterstore, &["dst", "2", "k1", "k2"], RejectReason::AggregatingWriteStore);
        assert_rejected(RedisApi::Zunionstore, &["dst", "2", "k1", "k2"], RejectReason::AggregatingWriteStore);
        assert_rejected(RedisApi::Zrangestore, &["dst", "src", "0", "-1"], RejectReason::AggregatingWriteStore);
        assert_rejected(
            RedisApi::Geosearchstore,
            &["dst", "src", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "m"],
            RejectReason::AggregatingWriteStore,
        );
        assert_rejected(
            RedisApi::Georadius,
            &["src", "0", "0", "1", "m", "STORE", "dst"],
            RejectReason::AggregatingWriteStore,
        );
        assert_rejected(
            RedisApi::Georadius,
            &["src", "0", "0", "1", "m", "STOREDIST", "dst"],
            RejectReason::AggregatingWriteStore,
        );
        assert_rejected(
            RedisApi::Georadiusbymember,
            &["src", "member", "1", "m", "STORE", "dst"],
            RejectReason::AggregatingWriteStore,
        );
        assert_rejected(
            RedisApi::Georadiusbymember,
            &["src", "member", "1", "m", "STOREDIST", "dst"],
            RejectReason::AggregatingWriteStore,
        );
        assert_rejected(RedisApi::CmsMerge, &["dst", "2", "k1", "k2"], RejectReason::AggregatingWriteStore);
        assert_rejected(RedisApi::TdigestMerge, &["dst", "2", "k1", "k2"], RejectReason::AggregatingWriteStore);
    }

    #[test]
    fn rejects_hyperloglog_multikey_inventory() {
        assert_rejected(RedisApi::Pfmerge, &["dst", "k1"], RejectReason::HllSemantics);
    }

    #[test]
    fn rejects_two_key_move_inventory() {
        assert_rejected(RedisApi::Rename, &["src", "dst"], RejectReason::TwoKeyMove);
        assert_rejected(RedisApi::Renamenx, &["src", "dst"], RejectReason::TwoKeyMove);
        assert_rejected(RedisApi::Copy, &["src", "dst"], RejectReason::TwoKeyMove);
        assert_rejected(RedisApi::Lmove, &["src", "dst", "LEFT", "RIGHT"], RejectReason::TwoKeyMove);
        assert_rejected(RedisApi::Blmove, &["src", "dst", "LEFT", "RIGHT", "1"], RejectReason::TwoKeyMove);
        assert_rejected(RedisApi::Rpoplpush, &["src", "dst"], RejectReason::TwoKeyMove);
        assert_rejected(RedisApi::Brpoplpush, &["src", "dst", "1"], RejectReason::TwoKeyMove);
        assert_rejected(RedisApi::Smove, &["src", "dst", "member"], RejectReason::TwoKeyMove);
        assert_rejected(RedisApi::Migrate, &["host", "6379", "", "0", "5000", "KEYS", "k1", "k2"], RejectReason::TwoKeyMove);
        assert_rejected(RedisApi::Sort, &["src", "BY", "weight_*", "STORE", "dst"], RejectReason::TwoKeyMove);
        assert_rejected(RedisApi::TsCreaterule, &["src", "dst", "AGGREGATION", "avg", "60000"], RejectReason::TwoKeyMove);
        assert_rejected(RedisApi::TsDeleterule, &["src", "dst"], RejectReason::TwoKeyMove);
    }

    #[test]
    fn rejects_blocking_first_non_empty_inventory() {
        assert_rejected(RedisApi::Blpop, &["k1", "k2", "1"], RejectReason::BlockingFirstNonEmpty);
        assert_rejected(RedisApi::Brpop, &["k1", "k2", "1"], RejectReason::BlockingFirstNonEmpty);
        assert_rejected(RedisApi::Lmpop, &["2", "k1", "k2", "LEFT"], RejectReason::BlockingFirstNonEmpty);
        assert_rejected(RedisApi::Blmpop, &["1", "2", "k1", "k2", "LEFT"], RejectReason::BlockingFirstNonEmpty);
        assert_rejected(RedisApi::Bzpopmin, &["k1", "k2", "1"], RejectReason::BlockingFirstNonEmpty);
        assert_rejected(RedisApi::Bzpopmax, &["k1", "k2", "1"], RejectReason::BlockingFirstNonEmpty);
        assert_rejected(RedisApi::Zmpop, &["2", "k1", "k2", "MIN"], RejectReason::BlockingFirstNonEmpty);
        assert_rejected(RedisApi::Bzmpop, &["1", "2", "k1", "k2", "MIN"], RejectReason::BlockingFirstNonEmpty);
        assert_rejected(
            RedisApi::Xread,
            &["COUNT", "1", "STREAMS", "k1", "k2", "0", "0"],
            RejectReason::BlockingFirstNonEmpty,
        );
        assert_rejected(
            RedisApi::Xreadgroup,
            &["GROUP", "g", "c", "STREAMS", "k1", "k2", ">", ">"],
            RejectReason::BlockingFirstNonEmpty,
        );
    }

    #[test]
    fn rejects_scripts_and_functions_only_when_multikey() {
        assert_eq!(
            classify(&int_args(
                RedisApi::Eval,
                vec![
                    RedisJsonValue::String("return 1".into()),
                    RedisJsonValue::Integer(1),
                    RedisJsonValue::String("k1".into()),
                ],
            )),
            CommandClassification::Passthrough
        );
        assert_rejected(RedisApi::Eval, &["return 1", "2", "k1", "k2"], RejectReason::ScriptMultiKey);
        assert_rejected(RedisApi::Evalsha, &["sha", "2", "k1", "k2"], RejectReason::ScriptMultiKey);
        assert_rejected(RedisApi::EvalRo, &["return 1", "2", "k1", "k2"], RejectReason::ScriptMultiKey);
        assert_rejected(RedisApi::EvalshaRo, &["sha", "2", "k1", "k2"], RejectReason::ScriptMultiKey);
        assert_rejected(RedisApi::Fcall, &["function", "2", "k1", "k2"], RejectReason::ScriptMultiKey);
        assert_rejected(RedisApi::FcallRo, &["function", "2", "k1", "k2"], RejectReason::ScriptMultiKey);
    }

    #[test]
    fn malformed_or_single_key_unsafe_shapes_passthrough() {
        assert_eq!(classify(&args(RedisApi::Sinter, &["k1"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::Rename, &["same", "same"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::Lcs, &["same", "same"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::Bitop, &["AND", "dst"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::Xread, &["STREAMS", "k1", "0"])), CommandClassification::Passthrough);
    }

    #[test]
    fn keyword_like_fixed_args_and_option_values_do_not_trigger_custom_parsers() {
        assert_eq!(
            classify(&args(RedisApi::Migrate, &["host", "6379", "KEYS", "0", "5000"])),
            CommandClassification::Passthrough
        );
        assert_eq!(
            classify(&args(RedisApi::Migrate, &["host", "6379", "", "0", "5000", "AUTH", "KEYS"])),
            CommandClassification::Passthrough
        );
        assert_eq!(classify(&args(RedisApi::Sort, &["STORE", "ALPHA"])), CommandClassification::Passthrough);
        assert_eq!(classify(&args(RedisApi::Sort, &["src", "BY", "STORE"])), CommandClassification::Passthrough);
        assert_eq!(
            classify(&args(RedisApi::Georadius, &["STORE", "0", "0", "1", "m"])),
            CommandClassification::Passthrough
        );
        assert_eq!(
            classify(&args(RedisApi::Georadius, &["src", "0", "0", "1", "m", "COUNT", "STORE"])),
            CommandClassification::Passthrough
        );
        assert_eq!(
            classify(&args(RedisApi::Georadiusbymember, &["src", "STORE", "1", "m"])),
            CommandClassification::Passthrough
        );
        assert_eq!(
            classify(&args(RedisApi::Xread, &["COUNT", "STREAMS", "STREAMS", "k1", "0"])),
            CommandClassification::Passthrough
        );
        assert_eq!(
            classify(&args(RedisApi::Xreadgroup, &["GROUP", "STREAMS", "consumer", "STREAMS", "k1", ">"])),
            CommandClassification::Passthrough
        );
    }
}
