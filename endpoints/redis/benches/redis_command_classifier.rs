use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use endpoint_types::request::EpWireRequest;
use ep_redis::api::RedisApi;
use ep_redis::protocol::{RedisBytes, extract_resp_command_bytes, extract_resp_command_str};

const HOT_COMMANDS: &[&[u8]] = &[b"GET", b"set", b"PiNg", b"MGET", b"HSET", b"EXPIRE", b"LRANGE"];
const MIXED_COMMANDS: &[&[u8]] = &[
    b"GET",
    b"set",
    b"COMMAND",
    b"FT.SEARCH",
    b"JSON.GET",
    b"XREAD",
    b"ZREMRANGEBYSCORE",
    b"BF.ADD",
];
const WORD_COMMANDS: &[(&[u8], Option<&[u8]>)] = &[
    (b"GET", Some(b"bench:key")),
    (b"SET", Some(b"bench:key")),
    (b"CLIENT", Some(b"SETINFO")),
    (b"COMMAND", Some(b"DOCS")),
    (b"CLUSTER", Some(b"SLOTS")),
    (b"FT.CONFIG", Some(b"GET")),
    (b"XGROUP", Some(b"CREATE")),
];

fn bench_direct_classification(c: &mut Criterion) {
    let mut group = c.benchmark_group("redis_command_classifier");
    group.throughput(Throughput::Elements(HOT_COMMANDS.len() as u64));

    group.bench_function("bytes_hot", |b| {
        b.iter(|| {
            let mut reads = 0usize;
            for command in black_box(HOT_COMMANDS) {
                let api = RedisApi::try_from_case_insensitive_bytes(command).expect("classify command bytes");
                reads += usize::from(api.request_type().is_read());
            }
            black_box(reads)
        })
    });

    group.bench_function("str_hot", |b| {
        b.iter(|| {
            let mut reads = 0usize;
            for command in black_box(HOT_COMMANDS) {
                let command = std::str::from_utf8(command).expect("command is utf-8");
                let api = RedisApi::try_from_case_insensitive(command).expect("classify command str");
                reads += usize::from(api.request_type().is_read());
            }
            black_box(reads)
        })
    });

    group.bench_function("old_uppercase_baseline_hot", |b| {
        b.iter(|| {
            let mut reads = 0usize;
            for command in black_box(HOT_COMMANDS) {
                let uppercase = std::str::from_utf8(command).expect("command is utf-8").to_ascii_uppercase();
                let api = RedisApi::try_from_case_insensitive(&uppercase).expect("classify uppercase command str");
                reads += usize::from(api.request_type().is_read());
            }
            black_box(reads)
        })
    });

    group.throughput(Throughput::Elements(WORD_COMMANDS.len() as u64));
    group.bench_function("command_words_bytes", |b| {
        b.iter(|| {
            let mut consumed = 0usize;
            for (command, subcommand) in black_box(WORD_COMMANDS) {
                let (_, words) = RedisApi::try_from_command_words_bytes(command, *subcommand).expect("classify command words");
                consumed += words;
            }
            black_box(consumed)
        })
    });

    group.throughput(Throughput::Elements(MIXED_COMMANDS.len() as u64));
    group.bench_function("bytes_mixed", |b| {
        b.iter(|| {
            let mut writes = 0usize;
            for command in black_box(MIXED_COMMANDS) {
                let api = RedisApi::try_from_case_insensitive_bytes(command).expect("classify mixed command bytes");
                writes += usize::from(!api.request_type().is_read());
            }
            black_box(writes)
        })
    });

    group.finish();
}

fn bench_resp_extraction(c: &mut Criterion) {
    let requests = [
        Bytes::from_static(b"*2\r\n$3\r\nGET\r\n$9\r\nbench:key\r\n"),
        Bytes::from_static(b"*3\r\n$3\r\nset\r\n$9\r\nbench:key\r\n$5\r\nvalue\r\n"),
        Bytes::from_static(b"*3\r\n$4\r\nHSET\r\n$9\r\nbench:key\r\n$5\r\nfield\r\n"),
        Bytes::from_static(b"*2\r\n$4\r\nMGET\r\n$9\r\nbench:key\r\n"),
    ];
    let redis_bytes: Vec<_> = requests.iter().cloned().map(RedisBytes::from).collect();

    let mut group = c.benchmark_group("redis_resp_command_extraction");
    group.throughput(Throughput::Elements(requests.len() as u64));

    group.bench_function(BenchmarkId::new("extract_and_classify", "bytes"), |b| {
        b.iter(|| {
            let mut reads = 0usize;
            for request in black_box(&requests) {
                let command = extract_resp_command_bytes(request).expect("extract command bytes");
                let api = RedisApi::try_from_case_insensitive_bytes(command).expect("classify command bytes");
                reads += usize::from(api.request_type().is_read());
            }
            black_box(reads)
        })
    });

    group.bench_function(BenchmarkId::new("extract_and_classify", "str"), |b| {
        b.iter(|| {
            let mut reads = 0usize;
            for request in black_box(&requests) {
                let command = extract_resp_command_str(request).expect("extract command str");
                let api = RedisApi::try_from_case_insensitive(command).expect("classify command str");
                reads += usize::from(api.request_type().is_read());
            }
            black_box(reads)
        })
    });

    group.bench_function("redis_bytes_request_type", |b| {
        b.iter(|| {
            let mut reads = 0usize;
            for request in black_box(&redis_bytes) {
                let req_type = request.request_type().expect("classify request type");
                reads += usize::from(req_type.is_read());
            }
            black_box(reads)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_direct_classification, bench_resp_extraction);
criterion_main!(benches);
