#![allow(clippy::unwrap_used)]

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use mongo_wire::{MessageHeader, MongoReadSyncExt, OpCode, OpCompressed, OpMsg, OpQuery, OpReply, SliceStream};

// Test data builders

fn build_bson_document(target_size: usize) -> Vec<u8> {
    let overhead = 4 + 1 + 2 + 4 + 1 + 1;
    let content_size = target_size.saturating_sub(overhead);

    let mut doc = Vec::with_capacity(target_size);
    doc.extend_from_slice(&[0, 0, 0, 0]);
    doc.push(0x02);
    doc.push(b'x');
    doc.push(0);
    let str_len = (content_size + 1) as i32;
    doc.extend_from_slice(&str_len.to_le_bytes());
    doc.extend(std::iter::repeat_n(b'y', content_size));
    doc.push(0);
    doc.push(0);

    let len = doc.len() as i32;
    doc[0..4].copy_from_slice(&len.to_le_bytes());
    doc
}

fn build_op_msg(doc: &[u8], with_checksum: bool) -> Vec<u8> {
    let flags: u32 = if with_checksum { 1 } else { 0 };
    let body_len = 4 + 1 + doc.len() + if with_checksum { 4 } else { 0 };
    let message_len = 16 + body_len;

    let mut msg = Vec::with_capacity(message_len);
    msg.extend_from_slice(&(message_len as i32).to_le_bytes());
    msg.extend_from_slice(&1i32.to_le_bytes());
    msg.extend_from_slice(&0i32.to_le_bytes());
    msg.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    msg.extend_from_slice(&flags.to_le_bytes());
    msg.push(0);
    msg.extend_from_slice(doc);

    if with_checksum {
        let checksum = crc32c::crc32c(&msg);
        msg.extend_from_slice(&checksum.to_le_bytes());
    }
    msg
}

fn build_op_msg_multi_section(body_doc: &[u8], seq_docs: &[&[u8]], identifier: &str) -> Vec<u8> {
    let seq_docs_len: usize = seq_docs.iter().map(|d| d.len()).sum();
    let section1_size = 4 + identifier.len() + 1 + seq_docs_len;
    let body_len = 4 + 1 + body_doc.len() + 1 + section1_size;
    let message_len = 16 + body_len;

    let mut msg = Vec::with_capacity(message_len);
    msg.extend_from_slice(&(message_len as i32).to_le_bytes());
    msg.extend_from_slice(&1i32.to_le_bytes());
    msg.extend_from_slice(&0i32.to_le_bytes());
    msg.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    msg.extend_from_slice(&0u32.to_le_bytes());
    msg.push(0);
    msg.extend_from_slice(body_doc);
    msg.push(1);
    msg.extend_from_slice(&(section1_size as i32).to_le_bytes());
    msg.extend_from_slice(identifier.as_bytes());
    msg.push(0);
    for doc in seq_docs {
        msg.extend_from_slice(doc);
    }
    msg
}

fn build_op_compressed(inner_msg: &[u8], compressor: u8) -> Vec<u8> {
    let uncompressed_size = inner_msg.len() as i32;
    let body_len = 4 + 4 + 1 + inner_msg.len();
    let message_len = 16 + body_len;

    let mut msg = Vec::with_capacity(message_len);
    msg.extend_from_slice(&(message_len as i32).to_le_bytes());
    msg.extend_from_slice(&1i32.to_le_bytes());
    msg.extend_from_slice(&0i32.to_le_bytes());
    msg.extend_from_slice(&(OpCode::Compressed as i32).to_le_bytes());
    msg.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    msg.extend_from_slice(&uncompressed_size.to_le_bytes());
    msg.push(compressor);
    msg.extend_from_slice(inner_msg);
    msg
}

fn build_op_query(collection: &str, query_doc: &[u8]) -> Vec<u8> {
    let body_len = 4 + collection.len() + 1 + 4 + 4 + query_doc.len();
    let message_len = 16 + body_len;

    let mut msg = Vec::with_capacity(message_len);
    msg.extend_from_slice(&(message_len as i32).to_le_bytes());
    msg.extend_from_slice(&1i32.to_le_bytes());
    msg.extend_from_slice(&0i32.to_le_bytes());
    msg.extend_from_slice(&(OpCode::Query as i32).to_le_bytes());
    msg.extend_from_slice(&0u32.to_le_bytes());
    msg.extend_from_slice(collection.as_bytes());
    msg.push(0);
    msg.extend_from_slice(&0i32.to_le_bytes());
    msg.extend_from_slice(&100i32.to_le_bytes());
    msg.extend_from_slice(query_doc);
    msg
}

fn build_op_reply(documents: &[&[u8]]) -> Vec<u8> {
    let docs_len: usize = documents.iter().map(|d| d.len()).sum();
    let body_len = 4 + 8 + 4 + 4 + docs_len;
    let message_len = 16 + body_len;

    let mut msg = Vec::with_capacity(message_len);
    msg.extend_from_slice(&(message_len as i32).to_le_bytes());
    msg.extend_from_slice(&1i32.to_le_bytes());
    msg.extend_from_slice(&0i32.to_le_bytes());
    msg.extend_from_slice(&(OpCode::Reply as i32).to_le_bytes());
    msg.extend_from_slice(&0u32.to_le_bytes());
    msg.extend_from_slice(&0i64.to_le_bytes());
    msg.extend_from_slice(&0i32.to_le_bytes());
    msg.extend_from_slice(&(documents.len() as i32).to_le_bytes());
    for doc in documents {
        msg.extend_from_slice(doc);
    }
    msg
}

// Header parsing

fn bench_header_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("header");

    let header_bytes = [
        0x15, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xDD, 0x07, 0x00, 0x00,
    ];

    group.throughput(Throughput::Bytes(16));

    group.bench_function("parse_sync", |b| {
        b.iter(|| {
            let stream = SliceStream::new(&header_bytes);
            black_box(MessageHeader::parse_sync(&stream).unwrap());
        });
    });

    group.bench_function("parse_async", |b| {
        b.iter(|| {
            pollster::block_on(async {
                let stream = SliceStream::new(&header_bytes);
                black_box(MessageHeader::parse(&stream).await.unwrap());
            })
        });
    });

    group.bench_function("encode", |b| {
        let header = MessageHeader {
            message_length: 21,
            request_id: 1,
            response_to: 0,
            op_code: OpCode::Msg as i32,
        };
        b.iter(|| black_box(header.encode()));
    });

    group.finish();
}

// OP_MSG parsing

fn bench_op_msg_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_msg");

    let small_doc = build_bson_document(64);
    let medium_doc = build_bson_document(1024);
    let large_doc = build_bson_document(16 * 1024);

    for (name, doc) in [("64B", &small_doc), ("1KB", &medium_doc), ("16KB", &large_doc)] {
        let msg = build_op_msg(doc, false);
        group.throughput(Throughput::Bytes(msg.len() as u64));

        group.bench_with_input(BenchmarkId::new("parse_sync", name), &msg, |b, msg| {
            b.iter(|| {
                let stream = SliceStream::new(msg);
                let header = MessageHeader::parse_sync(&stream).unwrap();
                let body_len = header.body_length().unwrap();
                black_box(OpMsg::parse_sync(&stream, body_len).unwrap());
            });
        });
    }

    group.finish();
}

fn bench_op_msg_with_checksum(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_msg_checksum");

    let doc = build_bson_document(1024);
    let msg_no_checksum = build_op_msg(&doc, false);
    let msg_with_checksum = build_op_msg(&doc, true);

    group.throughput(Throughput::Bytes(msg_with_checksum.len() as u64));

    group.bench_function("without_checksum", |b| {
        b.iter(|| {
            let stream = SliceStream::new(&msg_no_checksum);
            let header = MessageHeader::parse_sync(&stream).unwrap();
            let body_len = header.body_length().unwrap();
            black_box(OpMsg::parse_sync(&stream, body_len).unwrap());
        });
    });

    group.bench_function("with_checksum_validation", |b| {
        b.iter(|| black_box(OpMsg::parse_with_checksum(&msg_with_checksum).unwrap()));
    });

    group.finish();
}

fn bench_op_msg_multi_section(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_msg_multi_section");

    let body_doc = build_bson_document(64);
    let seq_doc = build_bson_document(256);

    for num_docs in [1, 10, 100] {
        let seq_docs: Vec<Vec<u8>> = (0..num_docs).map(|_| seq_doc.clone()).collect();
        let seq_refs: Vec<&[u8]> = seq_docs.iter().map(|d| d.as_slice()).collect();
        let msg = build_op_msg_multi_section(&body_doc, &seq_refs, "documents");

        group.throughput(Throughput::Bytes(msg.len() as u64));

        group.bench_with_input(BenchmarkId::new("parse", format!("{}_docs", num_docs)), &msg, |b, msg| {
            b.iter(|| {
                let stream = SliceStream::new(msg);
                let header = MessageHeader::parse_sync(&stream).unwrap();
                let body_len = header.body_length().unwrap();
                black_box(OpMsg::parse_sync(&stream, body_len).unwrap());
            });
        });
    }

    group.finish();
}

// OP_COMPRESSED parsing (measures CVE-2025-14847 validation overhead)

fn bench_op_compressed_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_compressed");

    let doc = build_bson_document(1024);
    let inner_msg = build_op_msg(&doc, false);
    let inner_body = &inner_msg[16..];
    let msg = build_op_compressed(inner_body, 0);

    group.throughput(Throughput::Bytes(msg.len() as u64));

    group.bench_function("parse_with_cve_validation", |b| {
        b.iter(|| {
            let stream = SliceStream::new(&msg);
            let header = MessageHeader::parse_sync(&stream).unwrap();
            let body_len = header.body_length().unwrap();
            black_box(OpCompressed::parse_sync(&stream, &header, body_len).unwrap());
        });
    });

    group.finish();
}

// Legacy protocol

fn bench_op_query_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_query");

    let doc = build_bson_document(256);
    let msg = build_op_query("test.collection", &doc);

    group.throughput(Throughput::Bytes(msg.len() as u64));

    group.bench_function("parse", |b| {
        b.iter(|| {
            let stream = SliceStream::new(&msg);
            let header = MessageHeader::parse_sync(&stream).unwrap();
            let body_len = header.body_length().unwrap();
            black_box(OpQuery::parse_sync(&stream, body_len).unwrap());
        });
    });

    group.finish();
}

fn bench_op_reply_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_reply");

    let doc = build_bson_document(256);

    for num_docs in [1, 10, 100] {
        let docs: Vec<Vec<u8>> = (0..num_docs).map(|_| doc.clone()).collect();
        let doc_refs: Vec<&[u8]> = docs.iter().map(|d| d.as_slice()).collect();
        let msg = build_op_reply(&doc_refs);

        group.throughput(Throughput::Bytes(msg.len() as u64));

        group.bench_with_input(BenchmarkId::new("parse", format!("{}_docs", num_docs)), &msg, |b, msg| {
            b.iter(|| {
                let stream = SliceStream::new(msg);
                let header = MessageHeader::parse_sync(&stream).unwrap();
                let body_len = header.body_length().unwrap();
                black_box(OpReply::parse_sync(&stream, body_len).unwrap());
            });
        });
    }

    group.finish();
}

// CRC-32C

fn bench_checksum(c: &mut Criterion) {
    let mut group = c.benchmark_group("crc32c");

    for size in [64, 1024, 16 * 1024, 64 * 1024, 256 * 1024] {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("compute", format!("{}B", size)), &data, |b, data| {
            b.iter(|| black_box(crc32c::crc32c(data)))
        });
    }

    group.finish();
}

// BSON reading

fn bench_bson_reading(c: &mut Criterion) {
    let mut group = c.benchmark_group("bson_read");

    for size in [64, 256, 1024, 4096, 16384] {
        let doc = build_bson_document(size);

        group.throughput(Throughput::Bytes(doc.len() as u64));

        group.bench_with_input(BenchmarkId::new("read_document", format!("{}B", size)), &doc, |b, doc| {
            b.iter(|| {
                let stream = SliceStream::new(doc);
                black_box(stream.read_bson_document_sync().unwrap());
            });
        });
    }

    group.finish();
}

// Throughput

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");

    for size in [64, 256, 1024, 4096, 16384, 65536] {
        let doc = build_bson_document(size);
        let msg = build_op_msg(&doc, false);

        group.throughput(Throughput::Bytes(msg.len() as u64));

        group.bench_with_input(BenchmarkId::new("op_msg_e2e", format!("{}B_doc", size)), &msg, |b, msg| {
            b.iter(|| {
                let stream = SliceStream::new(msg);
                let header = MessageHeader::parse_sync(&stream).unwrap();
                let body_len = header.body_length().unwrap();
                let op_msg = OpMsg::parse_sync(&stream, body_len).unwrap();
                black_box(op_msg.body());
            });
        });
    }

    group.finish();
}

// OpMsgBuilder

fn bench_op_msg_builder(c: &mut Criterion) {
    use mongo_wire::OpMsgBuilder;

    let mut group = c.benchmark_group("op_msg_builder");

    let doc = build_bson_document(256);

    group.bench_function("build_simple", |b| {
        b.iter(|| black_box(OpMsgBuilder::new(1).body(&doc).build()));
    });

    let seq_docs: Vec<Vec<u8>> = (0..10).map(|_| build_bson_document(64)).collect();
    let seq_refs: Vec<&[u8]> = seq_docs.iter().map(|d| d.as_slice()).collect();

    group.bench_function("build_with_sequence", |b| {
        b.iter(|| black_box(OpMsgBuilder::new(1).body(&doc).document_sequence("documents", &seq_refs).build()));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_header_parsing,
    bench_op_msg_parsing,
    bench_op_msg_with_checksum,
    bench_op_msg_multi_section,
    bench_op_compressed_parsing,
    bench_op_query_parsing,
    bench_op_reply_parsing,
    bench_checksum,
    bench_bson_reading,
    bench_op_msg_builder,
    bench_throughput,
);

criterion_main!(benches);
