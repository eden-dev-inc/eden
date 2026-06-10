#![allow(clippy::unwrap_used)]
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use redis_protocol::resp2::decode::decode_range;
use resp_wire::SliceStream;
use resp_wire::types::array::Array;
use resp_wire::types::bignum::BigNumber;
use resp_wire::types::boolean::Boolean;
use resp_wire::types::bulk_error::BulkError;
use resp_wire::types::bulk_string::{BulkString, BulkStringValue};
use resp_wire::types::double::Double;
use resp_wire::types::dynamic::Dynamic;
use resp_wire::types::integer::Integer;
use resp_wire::types::null::Null;
use resp_wire::types::set::Set;
use resp_wire::types::simple_error::SimpleError;
use resp_wire::types::simple_string::SimpleString;
use resp_wire::types::verbatim_string::VerbatimString;
use resp_wire::{RespParse, RespParseSync};

fn redis_protocol_zerocopy(data: &[u8]) -> Option<usize> {
    match decode_range(data) {
        Ok(Some((frame, consumed))) => {
            black_box(frame);
            Some(consumed)
        }
        _ => None,
    }
}

fn bench_simple_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_string");

    let short = b"+OK\r\n";
    let medium: &'static [u8] = format!("+{}\r\n", "x".repeat(100)).into_bytes().leak();
    let long: &'static [u8] = format!("+{}\r\n", "x".repeat(1000)).into_bytes().leak();

    for (name, data) in [("short", &short[..]), ("100B", medium), ("1KB", long)] {
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", name), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                let mut reader = SimpleString::parse_sync(&stream).expect("");
                while let Some(chunk) = reader.next_sync().expect("") {
                    black_box(&chunk);
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", name), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    let mut reader = SimpleString::parse(&stream).await.expect("");
                    while let Some(chunk) = reader.next().await.expect("") {
                        black_box(&chunk);
                    }
                })
            });
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-zerocopy", name), &data, |b, data| {
            b.iter(|| redis_protocol_zerocopy(data))
        });
    }

    group.finish();
}

fn bench_simple_error(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_error");

    let short = b"-ERR unknown\r\n";
    let medium: &'static [u8] = format!("-ERR {}\r\n", "x".repeat(100)).into_bytes().leak();

    for (name, data) in [("short", &short[..]), ("100B", medium)] {
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", name), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                let mut reader = SimpleError::parse_sync(&stream).expect("");
                while let Some(chunk) = reader.next_sync().expect("") {
                    black_box(&chunk);
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", name), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    let mut reader = SimpleError::parse(&stream).await.expect("");
                    while let Some(chunk) = reader.next().await.expect("") {
                        black_box(&chunk);
                    }
                })
            });
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-zerocopy", name), &data, |b, data| {
            b.iter(|| redis_protocol_zerocopy(data))
        });
    }

    group.finish();
}

fn bench_integer(c: &mut Criterion) {
    let mut group = c.benchmark_group("integer");

    let small = b":42\r\n";
    let large = b":9223372036854775807\r\n"; // i64::MAX

    for (name, data) in [("small", &small[..]), ("large", &large[..])] {
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", name), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                let value = Integer::parse_sync(&stream).expect("");
                black_box(value);
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", name), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    let value = Integer::parse(&stream).await.expect("");
                    black_box(value);
                })
            });
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-zerocopy", name), &data, |b, data| {
            b.iter(|| redis_protocol_zerocopy(data))
        });
    }

    group.finish();
}

fn bench_bulk_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_string");

    let short = b"$5\r\nhello\r\n";
    let null = b"$-1\r\n";
    let kb: &'static [u8] = format!("${}\r\n{}\r\n", 1000, "x".repeat(1000)).into_bytes().leak();
    let mb: &'static [u8] = format!("${}\r\n{}\r\n", 1_000_000, "x".repeat(1_000_000)).into_bytes().leak();

    for (name, data) in [("short", &short[..]), ("null", &null[..]), ("1KB", kb), ("1MB", mb)] {
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", name), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                match BulkString::parse_sync(&stream).expect("") {
                    BulkStringValue::Null => black_box(()),
                    BulkStringValue::String(mut reader) => {
                        if let Some(chunk) = reader.consume_all().expect("") {
                            black_box(&chunk);
                        }
                    }
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", name), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    match BulkString::parse(&stream).await.expect("") {
                        BulkStringValue::Null => black_box(()),
                        BulkStringValue::String(mut reader) => {
                            while let Some(chunk) = reader.next().await.expect("") {
                                black_box(&chunk);
                            }
                        }
                    }
                })
            });
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-zerocopy", name), &data, |b, data| {
            b.iter(|| redis_protocol_zerocopy(data))
        });
    }

    group.finish();
}

fn bench_array(c: &mut Criterion) {
    let mut group = c.benchmark_group("array");

    let empty = b"*0\r\n";
    let small = b"*3\r\n:1\r\n:2\r\n:3\r\n";

    // Build a larger array
    let mut large_arr = String::from("*100\r\n");
    for i in 0..100 {
        large_arr.push_str(&format!(":{}\r\n", i));
    }
    let large: &'static [u8] = large_arr.into_bytes().leak();

    // Integer-only arrays (can use Integer parser)
    for (name, data) in [("empty", &empty[..]), ("3-elem", &small[..]), ("100-elem", large)] {
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", name), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                let mut reader = Array::parse_sync(&stream).expect("");
                while let Some(elem) = reader.next_sync().expect("") {
                    let val = elem.parse_sync::<Integer>().expect("");
                    black_box(val);
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", name), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    let mut reader = Array::parse(&stream).await.expect("");
                    while let Some(elem) = reader.next().await.expect("") {
                        let val = elem.parse::<Integer>().await.expect("");
                        black_box(val);
                    }
                })
            });
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-zerocopy", name), &data, |b, data| {
            b.iter(|| redis_protocol_zerocopy(data))
        });
    }

    // Nested array - use Dynamic parser
    let nested = b"*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n";
    group.throughput(Throughput::Bytes(nested.len() as u64));

    group.bench_with_input(BenchmarkId::new("eden-sync", "nested"), &nested, |b, data| {
        b.iter(|| {
            let stream = SliceStream::new(&data[..]);
            let value = Dynamic::parse_sync(&stream).expect("");
            black_box(value);
        });
    });

    group.bench_with_input(BenchmarkId::new("eden-async", "nested"), &nested, |b, data| {
        b.iter(|| {
            pollster::block_on(async {
                let stream = SliceStream::new(&data[..]);
                let value = Dynamic::parse(&stream).await.expect("");
                black_box(value);
            })
        });
    });

    group.bench_with_input(BenchmarkId::new("redis-protocol-zerocopy", "nested"), &nested, |b, data| {
        b.iter(|| redis_protocol_zerocopy(&data[..]))
    });

    group.finish();
}

fn bench_null(c: &mut Criterion) {
    let mut group = c.benchmark_group("null");

    let data = b"_\r\n";
    group.throughput(Throughput::Bytes(data.len() as u64));

    group.bench_function("eden-sync", |b| {
        b.iter(|| {
            let stream = SliceStream::new(&data[..]);
            Null::parse_sync(&stream).expect("");
        });
    });

    group.bench_function("eden-async", |b| {
        b.iter(|| {
            pollster::block_on(async {
                let stream = SliceStream::new(&data[..]);
                Null::parse(&stream).await.expect("");
            })
        });
    });

    group.finish();
}

fn bench_boolean(c: &mut Criterion) {
    let mut group = c.benchmark_group("boolean");

    for (name, data) in [("true", &b"#t\r\n"[..]), ("false", &b"#f\r\n"[..])] {
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", name), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                let value = Boolean::parse_sync(&stream).expect("");
                black_box(value);
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", name), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    let value = Boolean::parse(&stream).await.expect("");
                    black_box(value);
                })
            });
        });
    }

    group.finish();
}

fn bench_double(c: &mut Criterion) {
    let mut group = c.benchmark_group("double");

    let simple = b",3.14\r\n";
    let scientific = b",1.23456789e10\r\n";
    let inf = b",inf\r\n";

    for (name, data) in [("simple", &simple[..]), ("scientific", &scientific[..]), ("inf", &inf[..])] {
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", name), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                let value = Double::parse_sync(&stream).expect("");
                black_box(value);
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", name), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    let value = Double::parse(&stream).await.expect("");
                    black_box(value);
                })
            });
        });
    }

    group.finish();
}

fn bench_bignum(c: &mut Criterion) {
    let mut group = c.benchmark_group("bignum");

    let small = b"(12345\r\n";
    let large: &'static [u8] = format!("({}\r\n", "9".repeat(100)).into_bytes().leak();

    for (name, data) in [("small", &small[..]), ("100-digit", large)] {
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", name), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                let value = BigNumber::parse_sync(&stream).expect("");
                black_box(&*value);
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", name), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    let value = BigNumber::parse(&stream).await.expect("");
                    black_box(&*value);
                })
            });
        });
    }

    group.finish();
}

fn bench_bulk_error(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_error");

    let short = b"!10\r\nERR failed\r\n";
    let long: &'static [u8] = format!("!{}\r\n{}\r\n", 1000, "x".repeat(1000)).into_bytes().leak();

    for (name, data) in [("short", &short[..]), ("1KB", long)] {
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", name), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                let mut reader = BulkError::parse_sync(&stream).expect("");
                while let Some(chunk) = reader.next_sync().expect("") {
                    black_box(&chunk);
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", name), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    let mut reader = BulkError::parse(&stream).await.expect("");
                    while let Some(chunk) = reader.next().await.expect("") {
                        black_box(&chunk);
                    }
                })
            });
        });
    }

    group.finish();
}

fn bench_verbatim_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("verbatim_string");

    let short = b"=9\r\ntxt:hello\r\n";
    let long: &'static [u8] = format!("={}\r\ntxt:{}\r\n", 1004, "x".repeat(1000)).into_bytes().leak();

    for (name, data) in [("short", &short[..]), ("1KB", long)] {
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", name), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                let mut reader = VerbatimString::parse_sync(&stream).expect("");
                black_box(reader.encoding());
                while let Some(chunk) = reader.next_sync().expect("") {
                    black_box(&chunk);
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", name), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    let mut reader = VerbatimString::parse(&stream).await.expect("");
                    black_box(reader.encoding());
                    while let Some(chunk) = reader.next().await.expect("") {
                        black_box(&chunk);
                    }
                })
            });
        });
    }

    group.finish();
}

fn bench_map(c: &mut Criterion) {
    let mut group = c.benchmark_group("map");

    let empty = b"%0\r\n";
    let small = b"%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n";

    // Build larger map
    let mut large_map = String::from("%50\r\n");
    for i in 0..50 {
        large_map.push_str(&format!("+key{}\r\n:{}\r\n", i, i));
    }
    let large: &'static [u8] = large_map.into_bytes().leak();

    for (name, data) in [("empty", &empty[..]), ("2-entry", &small[..]), ("50-entry", large)] {
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", name), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                let value = Dynamic::parse_sync(&stream).expect("");
                black_box(value);
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", name), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    let value = Dynamic::parse(&stream).await.expect("");
                    black_box(value);
                })
            });
        });
    }

    group.finish();
}

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("set");

    let empty = b"~0\r\n";
    let small = b"~3\r\n:1\r\n:2\r\n:3\r\n";

    let mut large_set = String::from("~100\r\n");
    for i in 0..100 {
        large_set.push_str(&format!(":{}\r\n", i));
    }
    let large: &'static [u8] = large_set.into_bytes().leak();

    for (name, data) in [("empty", &empty[..]), ("3-elem", &small[..]), ("100-elem", large)] {
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", name), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                let mut reader = Set::parse_sync(&stream).expect("");
                while let Some(elem) = reader.next_sync().expect("") {
                    let val = elem.parse_sync::<Integer>().expect("");
                    black_box(val);
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", name), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    let mut reader = Set::parse(&stream).await.expect("");
                    while let Some(elem) = reader.next().await.expect("") {
                        let val = elem.parse::<Integer>().await.expect("");
                        black_box(val);
                    }
                })
            });
        });
    }

    group.finish();
}

// =============================================================================
// Dynamic Parsing (any type)
// =============================================================================

fn bench_dynamic(c: &mut Criterion) {
    let mut group = c.benchmark_group("dynamic");

    let integer = b":42\r\n";
    let string = b"+OK\r\n";
    let bulk = b"$5\r\nhello\r\n";
    let array = b"*3\r\n:1\r\n:2\r\n:3\r\n";
    let map = b"%2\r\n+a\r\n:1\r\n+b\r\n:2\r\n";

    for (name, data) in [
        ("integer", &integer[..]),
        ("simple_string", &string[..]),
        ("bulk_string", &bulk[..]),
        ("array", &array[..]),
        ("map", &map[..]),
    ] {
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", name), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                let value = Dynamic::parse_sync(&stream).expect("");
                black_box(value);
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", name), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    let value = Dynamic::parse(&stream).await.expect("");
                    black_box(value);
                })
            });
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-zerocopy", name), &data, |b, data| {
            b.iter(|| redis_protocol_zerocopy(data))
        });
    }

    group.finish();
}

// =============================================================================
// Throughput comparison at scale
// =============================================================================

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");

    // Different payload sizes for bulk strings
    for size in [100, 1_000, 10_000, 100_000, 1_000_000] {
        let content = "x".repeat(size);
        let data: &'static [u8] = format!("${}\r\n{}\r\n", size, content).into_bytes().leak();

        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(BenchmarkId::new("eden-sync", format!("{}B", size)), &data, |b, data| {
            b.iter(|| {
                let stream = SliceStream::new(data);
                match BulkString::parse_sync(&stream).expect("") {
                    BulkStringValue::Null => {}
                    BulkStringValue::String(mut reader) => {
                        // Use consume_all for complete buffers - faster than iterating
                        if let Some(chunk) = reader.consume_all().expect("") {
                            black_box(&chunk);
                        }
                    }
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("eden-async", format!("{}B", size)), &data, |b, data| {
            b.iter(|| {
                pollster::block_on(async {
                    let stream = SliceStream::new(data);
                    match BulkString::parse(&stream).await.expect("") {
                        BulkStringValue::Null => {}
                        BulkStringValue::String(mut reader) => {
                            while let Some(chunk) = reader.next().await.expect("") {
                                black_box(&chunk);
                            }
                        }
                    }
                })
            });
        });

        group.bench_with_input(BenchmarkId::new("redis-protocol-zerocopy", format!("{}B", size)), &data, |b, data| {
            b.iter(|| redis_protocol_zerocopy(data))
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    // RESP2 types
    bench_simple_string,
    bench_simple_error,
    bench_integer,
    bench_bulk_string,
    bench_array,
    // RESP3 types
    bench_null,
    bench_boolean,
    bench_double,
    bench_bignum,
    bench_bulk_error,
    bench_verbatim_string,
    bench_map,
    bench_set,
    // Dynamic and throughput
    bench_dynamic,
    bench_throughput,
);

criterion_main!(benches);
