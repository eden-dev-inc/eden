use super::{ExecutionConstraint, ResponseCombiner, SupportedSplit};
use crate::api::{RedisApi, RedisJsonValue};
use crate::protocol::decoder::RedisCommandArgs;
use bytes::Bytes;

pub(super) fn split_supported(args: &RedisCommandArgs) -> Option<SupportedSplit> {
    use RedisApi::*;

    let split = match args.command() {
        Mget => split_all_keys(
            "GET",
            args.args(),
            ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true },
            ExecutionConstraint::AnyConnection,
        ),
        JsonMget => split_json_mget(args.args()),
        Del => split_all_keys("DEL", args.args(), ResponseCombiner::SumIntegers, ExecutionConstraint::AnyConnection),
        Exists => split_all_keys("EXISTS", args.args(), ResponseCombiner::SumIntegers, ExecutionConstraint::AnyConnection),
        Touch => split_all_keys("TOUCH", args.args(), ResponseCombiner::SumIntegers, ExecutionConstraint::AnyConnection),
        Unlink => split_all_keys("UNLINK", args.args(), ResponseCombiner::SumIntegers, ExecutionConstraint::AnyConnection),
        Watch => split_all_keys("WATCH", args.args(), ResponseCombiner::AllOk, ExecutionConstraint::SameConnection),
        _ => None,
    }?;

    (!split.parts.is_empty()).then_some(split)
}

fn split_all_keys(
    subcommand: &str,
    keys: &[RedisJsonValue],
    combiner: ResponseCombiner,
    constraint: ExecutionConstraint,
) -> Option<SupportedSplit> {
    let parts = keys.iter().map(|key| encode_subcommand(subcommand, &[key])).collect();
    Some(SupportedSplit { parts, combiner, constraint })
}

fn split_json_mget(args: &[RedisJsonValue]) -> Option<SupportedSplit> {
    if args.len() < 2 {
        return None;
    }

    let (path, keys) = args.split_last()?;
    let parts = keys.iter().map(|key| encode_subcommand("JSON.GET", &[key, path])).collect();
    Some(SupportedSplit {
        parts,
        combiner: ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true },
        constraint: ExecutionConstraint::AnyConnection,
    })
}

fn encode_subcommand(command_name: &str, args: &[&RedisJsonValue]) -> Bytes {
    let mut command = crate::command::cmd(command_name);
    for arg in args {
        command.arg(*arg);
    }
    command.get_packed_command()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::RedisApi;
    use crate::api::key::RedisKey;
    use crate::api::lib::{
        DelInput, ExistsInput, GetInput, JsonMgetInput, MgetInput, MultiCommand, RedisCommandInput, TouchInput, UnlinkInput, WatchInput,
    };
    use crate::api::value::RedisJsonValue;
    use crate::protocol::decoder::RedisCommandArgs;

    fn key(name: &str) -> RedisJsonValue {
        RedisJsonValue::String(name.to_string())
    }

    fn redis_key(name: &str) -> RedisKey {
        RedisKey::String(name.to_string())
    }

    fn parts_for(command: RedisApi, values: Vec<RedisJsonValue>) -> Vec<Bytes> {
        let args = RedisCommandArgs::new(command, values);
        split_supported(&args).expect("supported command should split").parts
    }

    fn split_for(command: RedisApi, values: Vec<RedisJsonValue>) -> SupportedSplit {
        let args = RedisCommandArgs::new(command, values);
        split_supported(&args).expect("supported command should split")
    }

    #[test]
    fn supported_split_inventory_carries_expected_combiners_and_constraints() {
        let cases = [
            (
                RedisApi::Mget,
                vec![key("k1"), key("k2")],
                ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true },
                ExecutionConstraint::AnyConnection,
            ),
            (
                RedisApi::JsonMget,
                vec![key("k1"), key("k2"), RedisJsonValue::String("$".to_string())],
                ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true },
                ExecutionConstraint::AnyConnection,
            ),
            (
                RedisApi::Del,
                vec![key("k1"), key("k2")],
                ResponseCombiner::SumIntegers,
                ExecutionConstraint::AnyConnection,
            ),
            (
                RedisApi::Exists,
                vec![key("k1"), key("k2")],
                ResponseCombiner::SumIntegers,
                ExecutionConstraint::AnyConnection,
            ),
            (
                RedisApi::Touch,
                vec![key("k1"), key("k2")],
                ResponseCombiner::SumIntegers,
                ExecutionConstraint::AnyConnection,
            ),
            (
                RedisApi::Unlink,
                vec![key("k1"), key("k2")],
                ResponseCombiner::SumIntegers,
                ExecutionConstraint::AnyConnection,
            ),
            (
                RedisApi::Watch,
                vec![key("k1"), key("k2")],
                ResponseCombiner::AllOk,
                ExecutionConstraint::SameConnection,
            ),
        ];

        for (command, values, combiner, constraint) in cases {
            let split = split_for(command, values);
            assert_eq!(split.parts.len(), 2);
            assert_eq!(split.combiner, combiner);
            assert_eq!(split.constraint, constraint);
        }
    }

    #[test]
    fn mget_split_matches_typed_deconstruct() {
        let typed = MgetInput { keys: vec![redis_key("k1"), redis_key("k2")] };
        let expected: Vec<Bytes> = typed.deconstruct().iter().map(RedisCommandInput::command).collect();
        assert_eq!(parts_for(RedisApi::Mget, vec![key("k1"), key("k2")]), expected);
    }

    #[test]
    fn mget_split_preserves_duplicate_keys() {
        let parts = parts_for(RedisApi::Mget, vec![key("k1"), key("k1"), key("k2")]);
        let expected: Vec<Bytes> = ["k1", "k1", "k2"].into_iter().map(|name| GetInput { key: redis_key(name) }.command()).collect();

        assert_eq!(parts, expected);
    }

    #[test]
    fn json_mget_split_matches_typed_deconstruct() {
        let values = vec![key("k1"), key("k2"), RedisJsonValue::String("$".to_string())];
        let typed = JsonMgetInput::decode(values.clone()).expect("valid JSON.MGET args");
        let expected: Vec<Bytes> = typed.deconstruct().iter().map(RedisCommandInput::command).collect();
        assert_eq!(parts_for(RedisApi::JsonMget, values), expected);
    }

    #[test]
    fn json_mget_split_preserves_duplicate_keys_and_shared_path() {
        let values = vec![key("k1"), key("k1"), key("k2"), RedisJsonValue::String("$.path".to_string())];
        let typed = JsonMgetInput::decode(values.clone()).expect("valid JSON.MGET args");
        let expected: Vec<Bytes> = typed.deconstruct().iter().map(RedisCommandInput::command).collect();

        assert_eq!(parts_for(RedisApi::JsonMget, values), expected);
    }

    #[test]
    fn integer_sum_splits_match_typed_deconstruct() {
        let keys = vec![redis_key("k1"), redis_key("k2")];
        let values = vec![key("k1"), key("k2")];

        let del = DelInput { keys: keys.clone() };
        let exists = ExistsInput { keys: keys.clone() };
        let touch = TouchInput { keys: keys.clone() };
        let unlink = UnlinkInput { keys };

        let del_expected: Vec<Bytes> = del.deconstruct().iter().map(RedisCommandInput::command).collect();
        let exists_expected: Vec<Bytes> = exists.deconstruct().iter().map(RedisCommandInput::command).collect();
        let touch_expected: Vec<Bytes> = touch.deconstruct().iter().map(RedisCommandInput::command).collect();
        let unlink_expected: Vec<Bytes> = unlink.deconstruct().iter().map(RedisCommandInput::command).collect();

        assert_eq!(parts_for(RedisApi::Del, values.clone()), del_expected);
        assert_eq!(parts_for(RedisApi::Exists, values.clone()), exists_expected);
        assert_eq!(parts_for(RedisApi::Touch, values.clone()), touch_expected);
        assert_eq!(parts_for(RedisApi::Unlink, values), unlink_expected);
    }

    #[test]
    fn integer_sum_splits_preserve_duplicate_keys() {
        let values = vec![key("k1"), key("k1"), key("k2")];
        let expected: Vec<Bytes> =
            ["k1", "k1", "k2"].into_iter().map(|name| ExistsInput { keys: vec![redis_key(name)] }.command()).collect();

        assert_eq!(parts_for(RedisApi::Exists, values), expected);
    }

    #[test]
    fn watch_split_matches_typed_deconstruct_and_requires_same_connection() {
        let typed = WatchInput { keys: vec![redis_key("k1"), redis_key("k2")] };
        let expected: Vec<Bytes> = typed.deconstruct().iter().map(RedisCommandInput::command).collect();
        let split = split_supported(&RedisCommandArgs::new(RedisApi::Watch, vec![key("k1"), key("k2")])).expect("WATCH should split");

        assert_eq!(split.parts, expected);
        assert_eq!(split.constraint, ExecutionConstraint::SameConnection);
    }
}
