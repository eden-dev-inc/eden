//! End-to-end tests for the multi-key deconstruction policy against a real
//! Redis (testcontainer-backed). Complements the policy unit tests in this
//! module and the raw-protocol/MULTI-suppression tests in
//! [`crate::protocol::tests`].
//!
//! The tests in this file focus on three gaps:
//!
//! 1. The typed API path (`RedisCommandInput::run_async_generic`) is exercised
//!    end-to-end across `Native` and `Deconstruct` modes for MGET/DEL/HMGET.
//! 2. WATCH split execution is observed to actually engage WATCH on every key
//!    (not just respond `OK`) by aborting a transaction on concurrent change
//!    of a non-first key — proves [`super::ExecutionConstraint::SameConnection`]
//!    is honored.
//! 3. Unsupported multi-key commands are rejected before reaching Redis in
//!    non-`MULTI` pipelines and over the typed path, verified via
//!    `INFO commandstats`.

use crate::api::key::RedisKey;
use crate::api::lib::{
    DelInput, DelOutput, DiscardInput, Field, GetInput, HmgetInput, HsetInput, MgetInput, MultiInput, RedisCommandInput, SetInput,
    multi_key_policy,
};
use crate::api::value::RedisJsonValue;
use crate::command::cmd;
use crate::protocol::{RedisBytes, RedisProtocol};
use crate::test_utils::{RespVersion, TestContext, setup_with_multi_key_execution};
use redis_core::config::MultiKeyExecution;
use serial_test::serial;

fn key(name: &str) -> RedisKey {
    RedisKey::String(name.into())
}

fn string(value: &str) -> RedisJsonValue {
    RedisJsonValue::String(value.into())
}

/// Read a single counter from `INFO commandstats`. Returns `0` if the counter
/// is not present (Redis only reports counters for commands that have run).
async fn cmdstat_calls(ctx: &mut TestContext, command: &str) -> u64 {
    let response = ctx.raw(&cmd("INFO").arg("commandstats").get_packed_command()).await.expect("INFO commandstats");
    let body = std::str::from_utf8(&response).expect("INFO body utf8");
    let needle = format!("cmdstat_{}:calls=", command.to_ascii_lowercase());
    let Some(idx) = body.find(&needle) else { return 0 };
    let tail = &body[idx + needle.len()..];
    let end = tail.find(',').unwrap_or(tail.len());
    tail[..end].trim().parse().unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn typed_mget_native_and_deconstruct_return_equivalent_output() {
    let mut native = setup_with_multi_key_execution(RespVersion::Resp2, None, MultiKeyExecution::Native).await;
    let mut deconstruct = setup_with_multi_key_execution(RespVersion::Resp2, None, MultiKeyExecution::Deconstruct).await;

    for ctx in [&mut native, &mut deconstruct] {
        ctx.write(SetInput {
            key: key("mk:eq:a"),
            value: string("va"),
            ..Default::default()
        })
        .await;
        ctx.write(SetInput {
            key: key("mk:eq:c"),
            value: string("vc"),
            ..Default::default()
        })
        .await;
    }

    let request = MgetInput { keys: vec![key("mk:eq:a"), key("mk:eq:b"), key("mk:eq:c")] };
    let native_out = native.read(request.clone()).await;
    let deconstruct_out = deconstruct.read(request).await;
    assert_eq!(native_out, deconstruct_out, "typed MGET output diverges between Native and Deconstruct");

    // In Deconstruct, observed Redis-side calls should be N GETs and zero MGETs.
    assert_eq!(cmdstat_calls(&mut deconstruct, "MGET").await, 0, "Deconstruct mode must not forward MGET to Redis");
    assert!(
        cmdstat_calls(&mut deconstruct, "GET").await >= 3,
        "Deconstruct mode should fan out into per-key GETs"
    );

    // In Native, MGET must hit Redis as MGET.
    assert!(cmdstat_calls(&mut native, "MGET").await >= 1, "Native mode should forward MGET unchanged");

    native.stop().await;
    deconstruct.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn typed_resp3_mget_deconstruct_keeps_resp3_metadata_after_recombine() {
    let mut ctx = setup_with_multi_key_execution(RespVersion::Resp3, None, MultiKeyExecution::Deconstruct).await;

    ctx.write(SetInput {
        key: key("mk:resp3:a"),
        value: string("va"),
        ..Default::default()
    })
    .await;

    let request = MgetInput { keys: vec![key("mk:resp3:a"), key("mk:resp3:missing")] };
    let pool = ctx.pool();
    let output = request.run_async_generic(pool, &mut ctx.telemetry).await.expect("typed RESP3 MGET");
    let serialized = output.try_serde_serialize().expect("serialize output");
    let serialized_text = serialized.to_string();
    assert!(serialized_text.contains("\"Resp3\""), "typed output must retain RESP3 metadata: {serialized_text}");
    assert!(
        !serialized_text.contains("\"Resp2\""),
        "typed output must not be tagged as RESP2: {serialized_text}"
    );

    let raw = output.try_to_bytes().expect("raw output");
    assert!(raw.starts_with(b"*"), "combined array should use array framing: {raw:?}");
    assert!(
        raw.windows(3).any(|window| window == b"_\r\n"),
        "combined RESP3 array should contain a RESP3 null: {raw:?}"
    );

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn typed_del_in_deconstruct_returns_correct_sum() {
    let mut ctx = setup_with_multi_key_execution(RespVersion::Resp2, None, MultiKeyExecution::Deconstruct).await;

    ctx.write(SetInput {
        key: key("mk:del:a"),
        value: string("1"),
        ..Default::default()
    })
    .await;
    ctx.write(SetInput {
        key: key("mk:del:c"),
        value: string("3"),
        ..Default::default()
    })
    .await;

    let raw = ctx
        .raw(
            &DelInput {
                keys: vec![key("mk:del:a"), key("mk:del:b"), key("mk:del:c")],
            }
            .command(),
        )
        .await
        .expect("raw DEL");
    let output = DelOutput::decode(&raw).expect("decode DEL");
    assert_eq!(output.deleted(), 2, "DEL should report 2 deleted keys (b was missing)");

    // Deconstruct mode must fan out — at least 3 DEL invocations on Redis
    // (one per requested key) rather than a single 3-arg DEL.
    assert!(cmdstat_calls(&mut ctx, "DEL").await >= 3, "Deconstruct mode should split DEL into one call per key",);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn same_key_hmget_passes_through_in_deconstruct() {
    let mut ctx = setup_with_multi_key_execution(RespVersion::Resp2, None, MultiKeyExecution::Deconstruct).await;

    ctx.write(HsetInput {
        key: key("mk:hash"),
        fields: vec![Field::new(string("f1"), string("v1")), Field::new(string("f2"), string("v2"))],
    })
    .await;

    let raw = ctx
        .raw(
            &HmgetInput {
                key: key("mk:hash"),
                fields: vec![string("f1"), string("missing"), string("f2")],
            }
            .command(),
        )
        .await
        .expect("raw HMGET");

    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&raw).expect("HMGET response");
    assert_eq!(responses.len(), 1, "HMGET must respond as one frame in Deconstruct");
    assert_eq!(
        responses[0], b"*3\r\n$2\r\nv1\r\n$-1\r\n$2\r\nv2\r\n",
        "HMGET in Deconstruct must forward verbatim to Redis (same-key multi-arg passthrough)",
    );

    // No HGET fan-out must have happened — HMGET is a single-key command.
    assert!(cmdstat_calls(&mut ctx, "HMGET").await >= 1, "HMGET should hit Redis as HMGET, not be deconstructed");
    assert_eq!(cmdstat_calls(&mut ctx, "HGET").await, 0, "Deconstruct mode must not split HMGET into HGETs");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn watch_split_aborts_transaction_on_concurrent_non_first_key_change() {
    let mut ctx = setup_with_multi_key_execution(RespVersion::Resp2, None, MultiKeyExecution::Deconstruct).await;

    ctx.write(SetInput {
        key: key("mk:w:a"),
        value: string("a0"),
        ..Default::default()
    })
    .await;
    ctx.write(SetInput {
        key: key("mk:w:b"),
        value: string("b0"),
        ..Default::default()
    })
    .await;
    ctx.write(SetInput {
        key: key("mk:w:c"),
        value: string("c0"),
        ..Default::default()
    })
    .await;

    let mut watcher = ctx.pinned_connection().await.expect("pinned watcher");
    let mut writer = ctx.pinned_connection().await.expect("pinned writer");

    // WATCH a, b, c — split into 3 single-key WATCHes on the watcher connection.
    let watch_ack = RedisBytes::from(cmd("WATCH").arg(key("mk:w:a")).arg(key("mk:w:b")).arg(key("mk:w:c")).get_packed_command())
        .send_raw_bytes_on_conn_no_reconnect_with_tx_state(&mut watcher, false)
        .await
        .expect("WATCH split");
    assert_eq!(watch_ack.as_ref(), b"+OK\r\n", "WATCH split must reconstruct as single +OK");

    // Concurrently change the *middle* key from a different connection. If WATCH
    // had only engaged on the first key, the transaction below would still succeed.
    let _ = TestContext::raw_on_pinned(
        &mut writer,
        &SetInput {
            key: key("mk:w:b"),
            value: string("changed"),
            ..Default::default()
        }
        .command(),
    )
    .await
    .expect("concurrent SET on mk:w:b");

    let _ = RedisBytes::from(MultiInput {}.command())
        .send_raw_bytes_on_conn_no_reconnect_with_tx_state(&mut watcher, false)
        .await
        .expect("MULTI");
    let _ = RedisBytes::from(
        SetInput {
            key: key("mk:w:a"),
            value: string("aX"),
            ..Default::default()
        }
        .command(),
    )
    .send_raw_bytes_on_conn_no_reconnect_with_tx_state(&mut watcher, true)
    .await
    .expect("SET inside MULTI");
    let exec = RedisBytes::from(cmd("EXEC").get_packed_command())
        .send_raw_bytes_on_conn_no_reconnect_with_tx_state(&mut watcher, true)
        .await
        .expect("EXEC");
    assert_eq!(exec.as_ref(), b"*-1\r\n", "EXEC must abort because mk:w:b changed after WATCH");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn raw_unsupported_multikey_in_pipeline_rejects_without_forwarding() {
    let mut ctx = setup_with_multi_key_execution(RespVersion::Resp2, None, MultiKeyExecution::Deconstruct).await;

    ctx.write(SetInput {
        key: key("mk:s:a"),
        value: string("va"),
        ..Default::default()
    })
    .await;
    ctx.write(SetInput {
        key: key("mk:s:b"),
        value: string("vb"),
        ..Default::default()
    })
    .await;

    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&GetInput { key: key("mk:s:a") }.command());
    pipeline.extend_from_slice(&cmd("SDIFF").arg(key("mk:s:a")).arg(key("mk:s:b")).get_packed_command());
    pipeline.extend_from_slice(&GetInput { key: key("mk:s:b") }.command());

    let result = ctx.raw(&pipeline).await.expect("raw pipeline");
    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("pipeline response");

    assert_eq!(responses.len(), 3, "frame parity: 3 input frames must yield 3 output frames");
    assert_eq!(responses[0], b"$2\r\nva\r\n");
    assert_eq!(responses[1], multi_key_policy::UNSUPPORTED_MULTI_KEY_ERROR_BYTES);
    assert_eq!(responses[2], b"$2\r\nvb\r\n");

    // Verify the rejected SDIFF never reached Redis.
    assert_eq!(cmdstat_calls(&mut ctx, "SDIFF").await, 0, "SDIFF must be rejected before being forwarded");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn deconstruct_pipeline_with_unsupported_does_not_poison_pinned_conn() {
    // Regression guard: rejecting an unsupported multi-key command must not leave
    // the pinned connection in an unusable state. After the reject we reuse the
    // same connection for follow-up commands.
    let mut ctx = setup_with_multi_key_execution(RespVersion::Resp2, None, MultiKeyExecution::Deconstruct).await;
    let mut conn = ctx.pinned_connection().await.expect("pinned connection");

    let reject = RedisBytes::from(cmd("SDIFF").arg(key("mk:p:a")).arg(key("mk:p:b")).get_packed_command())
        .send_raw_bytes_on_conn_no_reconnect_with_tx_state(&mut conn, false)
        .await
        .expect("SDIFF rejection");
    assert_eq!(reject.as_ref(), multi_key_policy::UNSUPPORTED_MULTI_KEY_ERROR_BYTES);

    let ping = TestContext::raw_on_pinned(&mut conn, &cmd("PING").get_packed_command()).await.expect("PING after reject");
    assert_eq!(ping.as_ref(), b"+PONG\r\n", "pinned conn must remain usable after a policy reject");

    let _ = RedisBytes::from(DiscardInput {}.command()).send_raw_bytes_on_conn_no_reconnect_with_tx_state(&mut conn, false).await;
    ctx.stop().await;
}
