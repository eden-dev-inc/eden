//! Runtime executor for the multi-key policy.
//!
//! The policy module ([`super`]) decides what to do with a parsed command:
//! pass it through, reject it, or split it into per-key sub-commands. This
//! module turns those decisions into concrete actions execution paths can
//! consume without owning a Redis connection themselves. Callers
//! ([`crate::protocol::RedisBytes::send_raw_bytes`], the pinned-conn
//! helpers, and [`crate::api::lib::RedisCommandInput::run_async_generic`])
//! drive the I/O; the executor only plans.
//!
//! # Two entry points
//!
//! * [`plan_frame`] — for callers that already hold exactly one RESP frame
//!   (e.g. the typed API, where `self.command()` returns one command's
//!   bytes). If the input contains pipelined frames it is forwarded
//!   verbatim — call [`plan_pipeline`] to handle that case.
//! * [`plan_pipeline`] — for callers that may receive a pipeline of frames
//!   (the wire path). Returns one [`FrameAction`] per frame, in order.
//!
//! Both short-circuit on [`MultiKeyExecution::Native`] without parsing.
//!
//! # Frame-count parity
//!
//! Each input frame must produce exactly one response RESP frame in the
//! caller's output stream — a forwarded raw response, a pre-built `-ERR …`
//! reject, or a combiner-merged response. RESP clients and the proxy rely
//! on this 1:1 ordering.

use super::{CommandClassification, ExecutionConstraint, ResponseCombiner, SupportedSplit, UNSUPPORTED_MULTI_KEY_ERROR_BYTES, classify};
use crate::protocol::RedisProtocol;
use bytes::Bytes;
use endpoint_types::protocol::EpProtocol;
use error::ResultEP;
use redis_core::config::MultiKeyExecution;

/// Action the caller should take for a single inbound RESP frame.
#[derive(Debug)]
pub enum FrameAction {
    /// Forward the original frame bytes to Redis unchanged. Covers
    /// [`MultiKeyExecution::Native`], single-key commands, same-key
    /// multi-arg commands, and any frame the policy did not recognise.
    Forward(Bytes),
    /// Return the embedded RESP error to the client without contacting
    /// Redis. Always carries [`UNSUPPORTED_MULTI_KEY_ERROR_BYTES`].
    Reject(Bytes),
    /// Send each `parts[i]` as a single-key sub-command, then run the
    /// per-key replies through `combiner` to produce one response RESP
    /// frame.
    Split {
        /// Original inbound frame. Transaction-aware callers use this when
        /// deconstruction is suppressed inside `MULTI`.
        original: Bytes,
        parts: Vec<Bytes>,
        combiner: ResponseCombiner,
        constraint: ExecutionConstraint,
    },
}

/// Decide how to handle one RESP frame.
///
/// `mode == Native` short-circuits to [`FrameAction::Forward`] without
/// parsing. `mode == Deconstruct` parses the frame, classifies it, and
/// returns the matching action. Frames the policy can't parse, or that
/// contain trailing bytes (pipelines), degrade to [`FrameAction::Forward`]
/// so the wire stays intact — those callers should use [`plan_pipeline`].
pub fn plan_frame(frame: Bytes, mode: MultiKeyExecution) -> ResultEP<FrameAction> {
    if matches!(mode, MultiKeyExecution::Native) {
        return Ok(FrameAction::Forward(frame));
    }

    let parsed = RedisProtocol::parse_buffer(&frame).ok().flatten();
    let Some((args, consumed)) = parsed else {
        return Ok(FrameAction::Forward(frame));
    };
    if consumed != frame.len() {
        return Ok(FrameAction::Forward(frame));
    }

    Ok(action_from_classification(classify(&args), frame))
}

/// Plan a (possibly pipelined) buffer one frame at a time, preserving
/// frame order.
///
/// `mode == Native` returns a single [`FrameAction::Forward`] for the whole
/// buffer with zero parsing overhead — callers that need per-frame
/// inspection (e.g. the in-MULTI rejection check) should pass
/// [`MultiKeyExecution::Deconstruct`] explicitly.
pub fn plan_pipeline(buffer: &[u8], mode: MultiKeyExecution) -> ResultEP<Vec<FrameAction>> {
    if matches!(mode, MultiKeyExecution::Native) {
        return Ok(vec![FrameAction::Forward(Bytes::copy_from_slice(buffer))]);
    }

    let mut offset = 0usize;
    let mut actions: Vec<FrameAction> = Vec::new();

    while offset < buffer.len() {
        let remaining = &buffer[offset..];

        let parsed = RedisProtocol::parse_buffer(remaining).ok().flatten();
        let Some((args, consumed)) = parsed else {
            // Couldn't parse a frame at this offset: forward the rest as
            // one chunk so we don't drop bytes the caller may have
            // intended for Redis.
            actions.push(FrameAction::Forward(Bytes::copy_from_slice(remaining)));
            break;
        };

        let frame_bytes = Bytes::copy_from_slice(&remaining[..consumed]);
        actions.push(action_from_classification(classify(&args), frame_bytes));
        offset += consumed;
    }

    Ok(actions)
}

fn action_from_classification(classification: CommandClassification, frame: Bytes) -> FrameAction {
    match classification {
        CommandClassification::Passthrough => FrameAction::Forward(frame),
        CommandClassification::UnsupportedMultiKey { .. } => FrameAction::Reject(Bytes::from_static(UNSUPPORTED_MULTI_KEY_ERROR_BYTES)),
        CommandClassification::SupportedMultiKey(SupportedSplit { parts, combiner, constraint }) => {
            FrameAction::Split { original: frame, parts, combiner, constraint }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::lib::{DelInput, GetInput, MgetInput, MultiCommand, RedisCommandInput};
    use crate::api::value::RedisJsonValue;
    use crate::command::cmd;

    fn build_command(name: &str, args: &[&str]) -> Bytes {
        let mut command = cmd(name);
        for arg in args {
            command.arg(RedisJsonValue::String((*arg).to_string()));
        }
        command.get_packed_command()
    }

    #[test]
    fn native_mode_short_circuits_plan_frame() {
        let frame = build_command("MGET", &["a", "b"]);
        let action = plan_frame(frame.clone(), MultiKeyExecution::Native).expect("plan_frame");
        assert!(matches!(action, FrameAction::Forward(bytes) if bytes == frame));
    }

    #[test]
    fn deconstruct_mode_splits_mget() {
        let frame = build_command("MGET", &["a", "b"]);
        let action = plan_frame(frame, MultiKeyExecution::Deconstruct).expect("plan_frame");
        let FrameAction::Split { original, parts, combiner, constraint } = action else {
            panic!("expected Split for multi-key MGET");
        };
        assert_eq!(original, build_command("MGET", &["a", "b"]));
        assert_eq!(parts.len(), 2);
        let expected: Vec<Bytes> = ["a", "b"].into_iter().map(|name| GetInput { key: name.into() }.command()).collect();
        assert_eq!(parts, expected);
        assert!(matches!(combiner, ResponseCombiner::ConcatArrayPreservingNils { wrongtype_as_nil: true }));
        assert_eq!(constraint, ExecutionConstraint::AnyConnection);
    }

    #[test]
    fn deconstruct_rejects_unsupported_mset() {
        let frame = build_command("MSET", &["a", "1", "b", "2"]);
        let action = plan_frame(frame, MultiKeyExecution::Deconstruct).expect("plan_frame");
        let FrameAction::Reject(bytes) = action else {
            panic!("expected Reject for multi-key MSET");
        };
        assert_eq!(bytes.as_ref(), UNSUPPORTED_MULTI_KEY_ERROR_BYTES);
    }

    #[test]
    fn deconstruct_passes_single_key_mget_through() {
        let frame = build_command("MGET", &["only"]);
        let action = plan_frame(frame.clone(), MultiKeyExecution::Deconstruct).expect("plan_frame");
        assert!(matches!(action, FrameAction::Forward(bytes) if bytes == frame));
    }

    #[test]
    fn plan_pipeline_partitions_forwards_and_splits() {
        let mut pipeline = Vec::new();
        pipeline.extend_from_slice(&build_command("MGET", &["a", "b"]));
        pipeline.extend_from_slice(&build_command("SET", &["k", "v"]));
        pipeline.extend_from_slice(&build_command("DEL", &["a", "b"]));

        let actions = plan_pipeline(&pipeline, MultiKeyExecution::Deconstruct).expect("plan_pipeline");
        assert_eq!(actions.len(), 3);

        let mget_parts: Vec<Bytes> =
            MgetInput { keys: vec!["a".into(), "b".into()] }.deconstruct().iter().map(RedisCommandInput::command).collect();
        let del_parts: Vec<Bytes> =
            DelInput { keys: vec!["a".into(), "b".into()] }.deconstruct().iter().map(RedisCommandInput::command).collect();

        match &actions[0] {
            FrameAction::Split { parts, combiner, .. } => {
                assert_eq!(parts, &mget_parts);
                assert!(matches!(combiner, ResponseCombiner::ConcatArrayPreservingNils { .. }));
            }
            other => panic!("expected Split for MGET, got {other:?}"),
        }

        match &actions[1] {
            FrameAction::Forward(bytes) => assert_eq!(bytes, &build_command("SET", &["k", "v"])),
            other => panic!("expected Forward for SET, got {other:?}"),
        }

        match &actions[2] {
            FrameAction::Split { parts, combiner, .. } => {
                assert_eq!(parts, &del_parts);
                assert!(matches!(combiner, ResponseCombiner::SumIntegers));
            }
            other => panic!("expected Split for DEL, got {other:?}"),
        }
    }

    #[test]
    fn plan_pipeline_native_returns_single_forward() {
        let mut pipeline = Vec::new();
        pipeline.extend_from_slice(&build_command("MGET", &["a", "b"]));
        pipeline.extend_from_slice(&build_command("SET", &["k", "v"]));

        let actions = plan_pipeline(&pipeline, MultiKeyExecution::Native).expect("plan_pipeline");
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            FrameAction::Forward(bytes) => assert_eq!(bytes.as_ref(), pipeline.as_slice()),
            other => panic!("expected single Forward in Native mode, got {other:?}"),
        }
    }
}
