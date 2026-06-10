#![allow(clippy::unwrap_used)]
//! Comparison benchmarks: our pipeline vs redis-protocol crate.
//!
//! Run with: cargo bench --bench pipeline_comparison

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use redis_protocol::resp2::decode::decode_range;
use resp_wire::PipelineExt;
use wire_stream::SliceStream;

fn pipeline_simple_strings(n: usize) -> Vec<u8> {
    let mut v = Vec::new();
    for _ in 0..n {
        v.extend_from_slice(b"+OK\r\n");
    }
    v
}

fn pipeline_integers(n: usize) -> Vec<u8> {
    let mut v = Vec::new();
    for i in 0..n {
        v.extend_from_slice(format!(":{}\r\n", i).as_bytes());
    }
    v
}

fn pipeline_bulk_strings(n: usize, size: usize) -> Vec<u8> {
    let mut v = Vec::new();
    let data = "x".repeat(size);
    for _ in 0..n {
        v.extend_from_slice(format!("${}\r\n{}\r\n", size, data).as_bytes());
    }
    v
}

fn pipeline_mixed(n: usize) -> Vec<u8> {
    let mut v = Vec::new();
    for i in 0..n {
        match i % 4 {
            0 => v.extend_from_slice(b"+OK\r\n"),
            1 => v.extend_from_slice(format!(":{}\r\n", i).as_bytes()),
            2 => v.extend_from_slice(b"$5\r\nhello\r\n"),
            3 => v.extend_from_slice(b"-ERR error\r\n"),
            _ => unreachable!(),
        }
    }
    v
}

fn pipeline_arrays(n: usize, elements: usize) -> Vec<u8> {
    let mut v = Vec::new();
    for _ in 0..n {
        v.extend_from_slice(format!("*{}\r\n", elements).as_bytes());
        for j in 0..elements {
            v.extend_from_slice(format!(":{}\r\n", j).as_bytes());
        }
    }
    v
}

fn pipeline_nested(depth: usize, width: usize) -> Vec<u8> {
    fn build_nested(v: &mut Vec<u8>, depth: usize, width: usize) {
        if depth == 0 {
            v.extend_from_slice(b":42\r\n");
        } else {
            v.extend_from_slice(format!("*{}\r\n", width).as_bytes());
            for _ in 0..width {
                build_nested(v, depth - 1, width);
            }
        }
    }
    let mut v = Vec::new();
    build_nested(&mut v, depth, width);
    v
}

/// Decode all frames using redis-protocol's range decoder, returning slices
fn redis_protocol_decode_all(mut buf: &[u8]) -> usize {
    let mut count = 0;
    while !buf.is_empty() {
        match decode_range(buf) {
            Ok(Some((frame, len))) => {
                black_box(frame);
                buf = &buf[len..];
                count += 1;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    count
}

/// Skip all frames using redis-protocol (just advance, don't return frame)
fn redis_protocol_skip_all(mut buf: &[u8]) -> usize {
    let mut count = 0;
    while !buf.is_empty() {
        match decode_range(buf) {
            Ok(Some((_, len))) => {
                buf = &buf[len..];
                count += 1;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    count
}

/// Decode using our pipeline (zero-copy slices)
fn eden_pipeline_decode_all(buf: &[u8]) -> usize {
    let stream = SliceStream::new(buf);
    let mut pipeline = stream.pipeline();
    let mut count = 0;
    while let Ok(Some(raw)) = pipeline.next_raw() {
        black_box(raw);
        count += 1;
    }
    count
}

/// Decode using our pipeline skip (fastest - no slice return)
fn eden_pipeline_skip_all(buf: &[u8]) -> usize {
    let stream = SliceStream::new(buf);
    let mut pipeline = stream.pipeline();
    let mut count = 0;
    while let Ok(true) = pipeline.skip() {
        count += 1;
    }
    count
}

/// Decode using our pipeline with collect_into (zero-copy, no heap)
fn eden_pipeline_collect<const N: usize>(buf: &[u8]) -> usize {
    let stream = SliceStream::new(buf);
    let mut pipeline = stream.pipeline();
    let mut out: [&[u8]; N] = [&[]; N];
    let mut count = 0;
    while count < N {
        match pipeline.next_raw() {
            Ok(Some(raw)) => {
                out[count] = raw;
                count += 1;
            }
            _ => break,
        }
    }
    count
}

fn bench_simple_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/simple_strings");

    for n in [10, 100, 1000] {
        let data: &'static [u8] = pipeline_simple_strings(n).leak();
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("eden-pipeline", n), &data, |b, data| {
            b.iter(|| eden_pipeline_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("eden-skip", n), &data, |b, data| {
            b.iter(|| eden_pipeline_skip_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-pipeline", n), &data, |b, data| {
            b.iter(|| redis_protocol_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-skip", n), &data, |b, data| {
            b.iter(|| redis_protocol_skip_all(black_box(data)))
        });
    }

    group.finish();
}

fn bench_integers(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/integers");

    for n in [10, 100, 1000] {
        let data: &'static [u8] = pipeline_integers(n).leak();
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("eden-pipeline", n), &data, |b, data| {
            b.iter(|| eden_pipeline_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("eden-skip", n), &data, |b, data| {
            b.iter(|| eden_pipeline_skip_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-pipeline", n), &data, |b, data| {
            b.iter(|| redis_protocol_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-skip", n), &data, |b, data| {
            b.iter(|| redis_protocol_skip_all(black_box(data)))
        });
    }

    group.finish();
}

fn bench_bulk_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/bulk_strings");

    for (n, size) in [(100, 100), (100, 1000), (10, 65536)] {
        let data: &'static [u8] = pipeline_bulk_strings(n, size).leak();
        let label = format!("{}x{}b", n, size);
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-pipeline", &label), &data, |b, data| {
            b.iter(|| eden_pipeline_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("eden-skip", &label), &data, |b, data| {
            b.iter(|| eden_pipeline_skip_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-pipeline", &label), &data, |b, data| {
            b.iter(|| redis_protocol_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-skip", &label), &data, |b, data| {
            b.iter(|| redis_protocol_skip_all(black_box(data)))
        });
    }

    group.finish();
}

fn bench_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/mixed_types");

    for n in [10, 100, 1000] {
        let data: &'static [u8] = pipeline_mixed(n).leak();
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("eden-pipeline", n), &data, |b, data| {
            b.iter(|| eden_pipeline_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("eden-skip", n), &data, |b, data| {
            b.iter(|| eden_pipeline_skip_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-pipeline", n), &data, |b, data| {
            b.iter(|| redis_protocol_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-skip", n), &data, |b, data| {
            b.iter(|| redis_protocol_skip_all(black_box(data)))
        });
    }

    group.finish();
}

fn bench_arrays(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/arrays");

    for (n, elements) in [(10, 10), (10, 100), (100, 10)] {
        let data: &'static [u8] = pipeline_arrays(n, elements).leak();
        let label = format!("{}x{}elem", n, elements);
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("eden-pipeline", &label), &data, |b, data| {
            b.iter(|| eden_pipeline_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("eden-skip", &label), &data, |b, data| {
            b.iter(|| eden_pipeline_skip_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-pipeline", &label), &data, |b, data| {
            b.iter(|| redis_protocol_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-skip", &label), &data, |b, data| {
            b.iter(|| redis_protocol_skip_all(black_box(data)))
        });
    }

    group.finish();
}

fn bench_nested(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/nested_arrays");

    for (depth, width) in [(3, 3), (4, 2), (2, 5)] {
        let data: &'static [u8] = pipeline_nested(depth, width).leak();
        let label = format!("d{}w{}", depth, width);

        group.bench_with_input(BenchmarkId::new("eden-pipeline", &label), &data, |b, data| {
            b.iter(|| eden_pipeline_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("eden-skip", &label), &data, |b, data| {
            b.iter(|| eden_pipeline_skip_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-pipeline", &label), &data, |b, data| {
            b.iter(|| redis_protocol_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-skip", &label), &data, |b, data| {
            b.iter(|| redis_protocol_skip_all(black_box(data)))
        });
    }

    group.finish();
}

fn bench_collect_fixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/collect_fixed");

    let data_10: &'static [u8] = pipeline_integers(10).leak();
    let data_100: &'static [u8] = pipeline_integers(100).leak();

    group.throughput(Throughput::Elements(10));
    group.bench_function("eden-collect/10", |b| b.iter(|| eden_pipeline_collect::<16>(black_box(data_10))));
    group.bench_function("redis-protocol-pipeline/10", |b| b.iter(|| redis_protocol_decode_all(black_box(data_10))));

    group.throughput(Throughput::Elements(100));
    group.bench_function("eden-collect/100", |b| b.iter(|| eden_pipeline_collect::<128>(black_box(data_100))));
    group.bench_function("redis-protocol-pipeline/100", |b| b.iter(|| redis_protocol_decode_all(black_box(data_100))));

    group.finish();
}

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/throughput");

    // Large bulk strings for throughput testing
    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let data: &'static [u8] = pipeline_bulk_strings(10, size).leak();
        let label = format!("10x{}B", size);
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-pipeline", &label), &data, |b, data| {
            b.iter(|| eden_pipeline_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("eden-skip", &label), &data, |b, data| {
            b.iter(|| eden_pipeline_skip_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-pipeline", &label), &data, |b, data| {
            b.iter(|| redis_protocol_decode_all(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-skip", &label), &data, |b, data| {
            b.iter(|| redis_protocol_skip_all(black_box(data)))
        });
    }

    group.finish();
}

/// Get nth element using eden pipeline (returns slice via next_raw) and process it
fn eden_get_nth(buf: &[u8], n: usize) -> Option<u64> {
    let stream = SliceStream::new(buf);
    let mut pipeline = stream.pipeline();

    // Skip n elements
    for _ in 0..n {
        if !pipeline.skip().ok()? {
            return None;
        }
    }

    // Get the target element using next_raw and process it
    if let Some(raw) = pipeline.next_raw().ok()? {
        // Sum all bytes to force processing
        let sum: u64 = raw.iter().map(|&b| b as u64).sum();
        Some(sum)
    } else {
        None
    }
}

/// Get nth element using eden pipeline skip (just get offset/length, then process)
fn eden_skip_nth(buf: &[u8], n: usize) -> Option<u64> {
    let stream = SliceStream::new(buf);
    let mut pipeline = stream.pipeline();

    // Skip n elements
    for _ in 0..n {
        if !pipeline.skip().ok()? {
            return None;
        }
    }

    // Skip the target element and get its bounds
    let start = stream.consumed();
    if !pipeline.skip().ok()? {
        return None;
    }
    let end = stream.consumed();

    // Process the bytes directly from original buffer
    let raw = &buf[start..end];
    let sum: u64 = raw.iter().map(|&b| b as u64).sum();
    Some(sum)
}

/// Get nth element using redis-protocol pipeline (returns frame) and process it
fn redis_protocol_pipeline_nth(buf: &[u8], n: usize) -> Option<u64> {
    let mut offset = 0;

    // Skip n elements
    for _ in 0..n {
        match decode_range(&buf[offset..]) {
            Ok(Some((_, len))) => offset += len,
            _ => return None,
        }
    }

    // Decode the target element (returns frame) and process bytes
    match decode_range(&buf[offset..]) {
        Ok(Some((frame, len))) => {
            black_box(&frame);
            // Process the raw bytes
            let raw = &buf[offset..offset + len];
            let sum: u64 = raw.iter().map(|&b| b as u64).sum();
            Some(sum)
        }
        _ => None,
    }
}

/// Get nth element using redis-protocol skip (no frame inspection) and process it
fn redis_protocol_skip_nth(buf: &[u8], n: usize) -> Option<u64> {
    let mut offset = 0;

    // Skip n elements
    for _ in 0..n {
        match decode_range(&buf[offset..]) {
            Ok(Some((_, len))) => offset += len,
            _ => return None,
        }
    }

    // Get length of target element (ignore frame)
    match decode_range(&buf[offset..]) {
        Ok(Some((_, len))) => {
            // Process the raw bytes
            let raw = &buf[offset..offset + len];
            let sum: u64 = raw.iter().map(|&b| b as u64).sum();
            Some(sum)
        }
        _ => None,
    }
}

fn bench_random_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/random_access");

    // 100 simple integers
    let integers_100: &'static [u8] = pipeline_integers(100).leak();

    // 100 bulk strings (100 bytes each)
    let bulk_100: &'static [u8] = pipeline_bulk_strings(100, 100).leak();

    // 100 arrays (10 elements each)
    let arrays_100: &'static [u8] = pipeline_arrays(100, 10).leak();

    // Test positions: first, early, middle, late, last
    let positions = [
        ("first", 0usize),
        ("5th", 4),
        ("25th", 24),
        ("50th", 49),
        ("75th", 74),
        ("last", 99),
    ];

    // Integers
    for (pos_name, pos) in &positions {
        group.bench_function(format!("eden-pipeline/integers/{}", pos_name), |b| {
            b.iter(|| eden_get_nth(black_box(integers_100), *pos))
        });
        group.bench_function(format!("eden-skip/integers/{}", pos_name), |b| {
            b.iter(|| eden_skip_nth(black_box(integers_100), *pos))
        });
        group.bench_function(format!("redis-protocol-pipeline/integers/{}", pos_name), |b| {
            b.iter(|| redis_protocol_pipeline_nth(black_box(integers_100), *pos))
        });
        group.bench_function(format!("redis-protocol-skip/integers/{}", pos_name), |b| {
            b.iter(|| redis_protocol_skip_nth(black_box(integers_100), *pos))
        });
    }

    // Bulk strings
    for (pos_name, pos) in &positions {
        group.bench_function(format!("eden-pipeline/bulk_100B/{}", pos_name), |b| {
            b.iter(|| eden_get_nth(black_box(bulk_100), *pos))
        });
        group.bench_function(format!("eden-skip/bulk_100B/{}", pos_name), |b| b.iter(|| eden_skip_nth(black_box(bulk_100), *pos)));
        group.bench_function(format!("redis-protocol-pipeline/bulk_100B/{}", pos_name), |b| {
            b.iter(|| redis_protocol_pipeline_nth(black_box(bulk_100), *pos))
        });
        group.bench_function(format!("redis-protocol-skip/bulk_100B/{}", pos_name), |b| {
            b.iter(|| redis_protocol_skip_nth(black_box(bulk_100), *pos))
        });
    }

    // Arrays
    for (pos_name, pos) in &positions {
        group.bench_function(format!("eden-pipeline/arrays_10elem/{}", pos_name), |b| {
            b.iter(|| eden_get_nth(black_box(arrays_100), *pos))
        });
        group.bench_function(format!("eden-skip/arrays_10elem/{}", pos_name), |b| {
            b.iter(|| eden_skip_nth(black_box(arrays_100), *pos))
        });
        group.bench_function(format!("redis-protocol-pipeline/arrays_10elem/{}", pos_name), |b| {
            b.iter(|| redis_protocol_pipeline_nth(black_box(arrays_100), *pos))
        });
        group.bench_function(format!("redis-protocol-skip/arrays_10elem/{}", pos_name), |b| {
            b.iter(|| redis_protocol_skip_nth(black_box(arrays_100), *pos))
        });
    }

    group.finish();
}

fn bench_random_access_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/random_access_large");

    // 1000 elements for more pronounced differences
    let integers_1000: &'static [u8] = pipeline_integers(1000).leak();
    let bulk_1000: &'static [u8] = pipeline_bulk_strings(1000, 100).leak();

    let positions = [("first", 0usize), ("100th", 99), ("500th", 499), ("last", 999)];

    // Integers - 1000 elements
    for (pos_name, pos) in &positions {
        group.bench_function(format!("eden-pipeline/integers_1000/{}", pos_name), |b| {
            b.iter(|| eden_get_nth(black_box(integers_1000), *pos))
        });
        group.bench_function(format!("eden-skip/integers_1000/{}", pos_name), |b| {
            b.iter(|| eden_skip_nth(black_box(integers_1000), *pos))
        });
        group.bench_function(format!("redis-protocol-pipeline/integers_1000/{}", pos_name), |b| {
            b.iter(|| redis_protocol_pipeline_nth(black_box(integers_1000), *pos))
        });
        group.bench_function(format!("redis-protocol-skip/integers_1000/{}", pos_name), |b| {
            b.iter(|| redis_protocol_skip_nth(black_box(integers_1000), *pos))
        });
    }

    // Bulk strings - 1000 elements
    for (pos_name, pos) in &positions {
        group.bench_function(format!("eden-pipeline/bulk_1000/{}", pos_name), |b| {
            b.iter(|| eden_get_nth(black_box(bulk_1000), *pos))
        });
        group.bench_function(format!("eden-skip/bulk_1000/{}", pos_name), |b| {
            b.iter(|| eden_skip_nth(black_box(bulk_1000), *pos))
        });
        group.bench_function(format!("redis-protocol-pipeline/bulk_1000/{}", pos_name), |b| {
            b.iter(|| redis_protocol_pipeline_nth(black_box(bulk_1000), *pos))
        });
        group.bench_function(format!("redis-protocol-skip/bulk_1000/{}", pos_name), |b| {
            b.iter(|| redis_protocol_skip_nth(black_box(bulk_1000), *pos))
        });
    }

    group.finish();
}

/// Build a realistic Redis command: *N\r\n$cmd_len\r\ncmd\r\n$key_len\r\nkey\r\n...args...
fn build_command(cmd: &str, key: &str, args: &[&str]) -> Vec<u8> {
    let mut v = Vec::new();
    let argc = 2 + args.len();
    v.extend_from_slice(format!("*{}\r\n", argc).as_bytes());

    // Command name
    v.extend_from_slice(format!("${}\r\n{}\r\n", cmd.len(), cmd).as_bytes());

    // Key
    v.extend_from_slice(format!("${}\r\n{}\r\n", key.len(), key).as_bytes());

    // Additional args
    for arg in args {
        v.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
    }
    v
}

/// Build a pipeline of various Redis commands
#[allow(dead_code)]
fn build_command_pipeline() -> Vec<u8> {
    let mut v = Vec::new();

    // Mix of different commands with varying key/value sizes
    v.extend(build_command("GET", "user:12345", &[]));
    v.extend(build_command("SET", "session:abc123", &["value_data_here"]));
    v.extend(build_command("HGET", "hash:users:profile", &["email"]));
    v.extend(build_command("LPUSH", "queue:notifications", &["msg1", "msg2", "msg3"]));
    v.extend(build_command("ZADD", "leaderboard:game1", &["100", "player1", "200", "player2"]));
    v.extend(build_command("SET", "cache:api:response:endpoint1", &[&"x".repeat(1000)]));
    v.extend(build_command("GET", "k", &[])); // tiny key
    v.extend(build_command("MGET", "key1", &["key2", "key3", "key4", "key5"]));
    v.extend(build_command("SETEX", "temp:data", &["3600", &"y".repeat(500)]));
    v.extend(build_command("HSET", "user:settings:99999", &["theme", "dark", "lang", "en"]));

    v
}

/// Extract command name and key using Eden - manual parsing, stops at key
fn eden_extract_cmd_and_key(cmd_buf: &[u8]) -> Option<(&[u8], &[u8])> {
    // Parse *N\r\n prefix
    if cmd_buf.first()? != &b'*' {
        return None;
    }
    let crlf = cmd_buf.iter().position(|&b| b == b'\r')?;
    let mut pos = crlf + 2; // skip *N\r\n

    // Parse first bulk string (command)
    if cmd_buf.get(pos)? != &b'$' {
        return None;
    }
    pos += 1;
    let crlf = cmd_buf[pos..].iter().position(|&b| b == b'\r')?;
    let cmd_len: usize = std::str::from_utf8(&cmd_buf[pos..pos + crlf]).ok()?.parse().ok()?;
    pos += crlf + 2;
    let cmd = &cmd_buf[pos..pos + cmd_len];
    pos += cmd_len + 2;

    // Parse second bulk string (key)
    if cmd_buf.get(pos)? != &b'$' {
        return None;
    }
    pos += 1;
    let crlf = cmd_buf[pos..].iter().position(|&b| b == b'\r')?;
    let key_len: usize = std::str::from_utf8(&cmd_buf[pos..pos + crlf]).ok()?.parse().ok()?;
    pos += crlf + 2;
    let key = &cmd_buf[pos..pos + key_len];

    Some((cmd, key))
}

/// Extract command name and key using redis-protocol
fn redis_protocol_extract_cmd_and_key(cmd_buf: &[u8]) -> Option<((usize, usize), (usize, usize))> {
    use redis_protocol::resp2::types::RangeFrame;

    match decode_range(cmd_buf) {
        Ok(Some((RangeFrame::Array(data), _))) => {
            let cmd_range = match data.first()? {
                RangeFrame::BulkString(range) => *range,
                _ => return None,
            };
            let key_range = match data.get(1)? {
                RangeFrame::BulkString(range) => *range,
                _ => return None,
            };
            Some((cmd_range, key_range))
        }
        _ => None,
    }
}

fn bench_command_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("command_parsing");

    // Single commands of varying sizes
    let get_small: &'static [u8] = build_command("GET", "k", &[]).leak();
    let get_medium: &'static [u8] = build_command("GET", "user:profile:12345", &[]).leak();
    let set_large: &'static [u8] = build_command("SET", "cache:key", &[&"x".repeat(10000)]).leak();
    let hset_multi: &'static [u8] = build_command("HSET", "user:1", &["field1", "value1", "field2", "value2", "field3", "value3"]).leak();

    // Extract command name only
    group.bench_function("eden/cmd_name/GET_small", |b| {
        b.iter(|| eden_extract_cmd_and_key(black_box(get_small)).map(|(cmd, _)| cmd))
    });
    group.bench_function("redis-protocol/cmd_name/GET_small", |b| {
        b.iter(|| redis_protocol_extract_cmd_and_key(black_box(get_small)).map(|(cmd, _)| cmd))
    });

    group.bench_function("eden/cmd_name/GET_medium", |b| {
        b.iter(|| eden_extract_cmd_and_key(black_box(get_medium)).map(|(cmd, _)| cmd))
    });
    group.bench_function("redis-protocol/cmd_name/GET_medium", |b| {
        b.iter(|| redis_protocol_extract_cmd_and_key(black_box(get_medium)).map(|(cmd, _)| cmd))
    });

    group.bench_function("eden/cmd_name/SET_10KB", |b| {
        b.iter(|| eden_extract_cmd_and_key(black_box(set_large)).map(|(cmd, _)| cmd))
    });
    group.bench_function("redis-protocol/cmd_name/SET_10KB", |b| {
        b.iter(|| redis_protocol_extract_cmd_and_key(black_box(set_large)).map(|(cmd, _)| cmd))
    });

    group.bench_function("eden/cmd_name/HSET_multi", |b| {
        b.iter(|| eden_extract_cmd_and_key(black_box(hset_multi)).map(|(cmd, _)| cmd))
    });
    group.bench_function("redis-protocol/cmd_name/HSET_multi", |b| {
        b.iter(|| redis_protocol_extract_cmd_and_key(black_box(hset_multi)).map(|(cmd, _)| cmd))
    });

    // Extract command + key
    group.bench_function("eden/cmd_and_key/GET_small", |b| b.iter(|| eden_extract_cmd_and_key(black_box(get_small))));
    group.bench_function("redis-protocol/cmd_and_key/GET_small", |b| {
        b.iter(|| redis_protocol_extract_cmd_and_key(black_box(get_small)))
    });

    group.bench_function("eden/cmd_and_key/SET_10KB", |b| b.iter(|| eden_extract_cmd_and_key(black_box(set_large))));
    group.bench_function("redis-protocol/cmd_and_key/SET_10KB", |b| {
        b.iter(|| redis_protocol_extract_cmd_and_key(black_box(set_large)))
    });

    group.finish();
}

fn bench_pipeline_command_routing(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_command_routing");

    // Build a pipeline with 100 commands
    let mut pipeline_data = Vec::new();
    for i in 0..100 {
        match i % 5 {
            0 => pipeline_data.extend(build_command("GET", &format!("key:{}", i), &[])),
            1 => pipeline_data.extend(build_command("SET", &format!("key:{}", i), &["value"])),
            2 => pipeline_data.extend(build_command("HGET", &format!("hash:{}", i), &["field"])),
            3 => pipeline_data.extend(build_command("LPUSH", &format!("list:{}", i), &["item"])),
            4 => pipeline_data.extend(build_command("ZADD", &format!("zset:{}", i), &["1", "member"])),
            _ => unreachable!(),
        }
    }
    let pipeline_100: &'static [u8] = pipeline_data.leak();

    // Count commands by type (simulates routing) - Eden version using manual offset tracking
    group.bench_function("eden/route_100_commands", |b| {
        b.iter(|| {
            let mut counts = [0u32; 5]; // GET, SET, HGET, LPUSH, ZADD
            let buf = black_box(pipeline_100);
            let stream = SliceStream::new(buf);
            let mut pipeline = stream.pipeline();

            while pipeline.skip().unwrap_or(false) {
                // After skip, we know the bounds - get the command from the buffer
                let _consumed = stream.consumed();
                // Find start of this command by scanning back (or track it)
            }

            // Actually, let's use a simpler approach - just iterate the buffer directly
            let mut offset = 0;
            while offset < buf.len() {
                if let Some((cmd, _)) = eden_extract_cmd_and_key(&buf[offset..]) {
                    match cmd {
                        b"GET" => counts[0] += 1,
                        b"SET" => counts[1] += 1,
                        b"HGET" => counts[2] += 1,
                        b"LPUSH" => counts[3] += 1,
                        b"ZADD" => counts[4] += 1,
                        _ => {}
                    }
                    // Find end of this command by parsing array header
                    let stream = SliceStream::new(&buf[offset..]);
                    let mut p = stream.pipeline();
                    if p.skip().unwrap_or(false) {
                        offset += stream.consumed();
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            counts
        })
    });

    group.bench_function("redis-protocol/route_100_commands", |b| {
        b.iter(|| {
            use redis_protocol::resp2::types::RangeFrame;

            let mut counts = [0u32; 5];
            let mut buf = black_box(pipeline_100);

            while !buf.is_empty() {
                match decode_range(buf) {
                    Ok(Some((frame, len))) => {
                        if let RangeFrame::Array(data) = &frame
                            && let Some(RangeFrame::BulkString((start, end))) = data.first()
                        {
                            let cmd = &buf[*start..*end];
                            match cmd {
                                b"GET" => counts[0] += 1,
                                b"SET" => counts[1] += 1,
                                b"HGET" => counts[2] += 1,
                                b"LPUSH" => counts[3] += 1,
                                b"ZADD" => counts[4] += 1,
                                _ => {}
                            }
                        }
                        buf = &buf[len..];
                    }
                    _ => break,
                }
            }
            counts
        })
    });

    // Extract all keys from pipeline
    group.bench_function("eden/extract_100_keys", |b| {
        b.iter(|| {
            let mut key_lengths = 0usize;
            let buf = black_box(pipeline_100);
            let mut offset = 0;

            while offset < buf.len() {
                if let Some((_, key)) = eden_extract_cmd_and_key(&buf[offset..]) {
                    key_lengths += key.len();
                    // Advance offset
                    let stream = SliceStream::new(&buf[offset..]);
                    let mut p = stream.pipeline();
                    if p.skip().unwrap_or(false) {
                        offset += stream.consumed();
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            key_lengths
        })
    });

    group.bench_function("redis-protocol/extract_100_keys", |b| {
        b.iter(|| {
            use redis_protocol::resp2::types::RangeFrame;

            let mut key_lengths = 0usize;
            let mut buf = black_box(pipeline_100);

            while !buf.is_empty() {
                match decode_range(buf) {
                    Ok(Some((frame, len))) => {
                        if let RangeFrame::Array(data) = &frame
                            && let Some(RangeFrame::BulkString((start, end))) = data.get(1)
                        {
                            key_lengths += end - start;
                        }
                        buf = &buf[len..];
                    }
                    _ => break,
                }
            }
            key_lengths
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_strings,
    bench_integers,
    bench_bulk_strings,
    bench_mixed,
    bench_arrays,
    bench_nested,
    bench_collect_fixed,
    bench_throughput,
    bench_random_access,
    bench_random_access_large,
    bench_command_parsing,
    bench_pipeline_command_routing,
);

criterion_main!(benches);
