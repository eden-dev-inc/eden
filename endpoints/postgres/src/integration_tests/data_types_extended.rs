use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

// ── BYTEA ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_bytea() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_bytea (id SERIAL PRIMARY KEY, val BYTEA)").await;
    ctx.execute("INSERT INTO dt_bytea (val) VALUES (E'\\\\xDEADBEEF'), (E'\\\\x00'), (E'\\\\x')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_bytea ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // BYTEA is returned as hex string in text format
    let v0 = arr[0]["val"].as_str().unwrap();
    assert!(v0.contains("deadbeef") || v0.contains("DEADBEEF") || v0.contains("\\xdeadbeef"));
    let v1 = arr[1]["val"].as_str().unwrap();
    assert!(v1.contains("00") || v1.contains("\\x00"));

    ctx.stop().await;
}

// ── MONEY ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_money() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_money (id SERIAL PRIMARY KEY, val MONEY)").await;
    ctx.execute("INSERT INTO dt_money (val) VALUES ('$1,234.56'), ('$0.01'), ('-$99.99')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_money ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // MONEY is returned as string in text format
    assert!(arr[0]["val"].is_string());
    let v0 = arr[0]["val"].as_str().unwrap();
    assert!(v0.contains("1") && v0.contains("234"));

    ctx.stop().await;
}

// ── INTERVAL ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_interval() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_interval (id SERIAL PRIMARY KEY, val INTERVAL)").await;
    ctx.execute(
        "INSERT INTO dt_interval (val) VALUES ('1 year 2 months 3 days'), ('04:05:06'), ('1 hour 30 minutes')",
        &[],
    )
    .await;

    let rows = ctx.query("SELECT val FROM dt_interval ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert!(arr[0]["val"].is_string());
    // PG formats interval as "1 year 2 mons 3 days"
    let v0 = arr[0]["val"].as_str().unwrap();
    assert!(v0.contains("year") || v0.contains("mon"));
    let v1 = arr[1]["val"].as_str().unwrap();
    assert!(v1.contains("04:05:06"));

    ctx.stop().await;
}

// ── TIMETZ ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_timetz() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_timetz (id SERIAL PRIMARY KEY, val TIMETZ)").await;
    ctx.execute("INSERT INTO dt_timetz (val) VALUES ('14:30:00+05:30'), ('00:00:00+00'), ('23:59:59-08')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_timetz ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    let v0 = arr[0]["val"].as_str().unwrap();
    assert!(v0.contains("14:30:00"));
    assert!(v0.contains("+05:30"));

    ctx.stop().await;
}

// ── BIT / VARBIT ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_bit() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_bit (id SERIAL PRIMARY KEY, val BIT(8), vval BIT VARYING(16))").await;
    ctx.execute("INSERT INTO dt_bit (val, vval) VALUES (B'10101010', B'110'), (B'11111111', B'1')", &[]).await;

    let rows = ctx.query("SELECT val, vval FROM dt_bit ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["val"], "10101010");
    assert_eq!(arr[0]["vval"], "110");
    assert_eq!(arr[1]["val"], "11111111");
    assert_eq!(arr[1]["vval"], "1");

    ctx.stop().await;
}

// ── MACADDR / MACADDR8 ──────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_macaddr() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_mac (id SERIAL PRIMARY KEY, mac MACADDR, mac8 MACADDR8)").await;
    ctx.execute("INSERT INTO dt_mac (mac, mac8) VALUES ('08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05')", &[]).await;

    let row = ctx.query_one("SELECT mac, mac8 FROM dt_mac", &[]).await;
    assert_eq!(row["mac"], "08:00:2b:01:02:03");
    assert_eq!(row["mac8"], "08:00:2b:01:02:03:04:05");

    ctx.stop().await;
}

// ── CIDR ─────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_cidr() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_cidr (id SERIAL PRIMARY KEY, val CIDR)").await;
    ctx.execute("INSERT INTO dt_cidr (val) VALUES ('192.168.1.0/24'), ('10.0.0.0/8'), ('::1/128')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_cidr ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["val"], "192.168.1.0/24");
    assert_eq!(arr[1]["val"], "10.0.0.0/8");
    assert_eq!(arr[2]["val"], "::1/128");

    ctx.stop().await;
}

// ── Geometric types ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_point() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_point (id SERIAL PRIMARY KEY, val POINT)").await;
    ctx.execute("INSERT INTO dt_point (val) VALUES ('(1.5,2.5)'), ('(0,0)'), ('(-3.14,99.9)')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_point ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    let v0 = arr[0]["val"].as_str().unwrap();
    assert!(v0.contains("1.5") && v0.contains("2.5"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_line() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_line (id SERIAL PRIMARY KEY, val LINE)").await;
    ctx.execute("INSERT INTO dt_line (val) VALUES ('{1,2,3}'), ('{0,1,-1}')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_line ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert!(arr[0]["val"].is_string());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_lseg() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_lseg (id SERIAL PRIMARY KEY, val LSEG)").await;
    ctx.execute("INSERT INTO dt_lseg (val) VALUES ('[(0,0),(1,1)]'), ('[(-1,-1),(2,3)]')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_lseg ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let v0 = arr[0]["val"].as_str().unwrap();
    assert!(v0.contains("0") && v0.contains("1"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_box() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_box (id SERIAL PRIMARY KEY, val BOX)").await;
    ctx.execute("INSERT INTO dt_box (val) VALUES ('(1,1),(0,0)'), ('(5,5),(2,2)')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_box ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert!(arr[0]["val"].is_string());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_path() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_path (id SERIAL PRIMARY KEY, val PATH)").await;
    // Open path uses [] and closed path uses ()
    ctx.execute("INSERT INTO dt_path (val) VALUES ('[(0,0),(1,1),(2,0)]'), ('((0,0),(1,1),(2,0))')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_path ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert!(arr[0]["val"].is_string());
    assert!(arr[1]["val"].is_string());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_polygon() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_poly (id SERIAL PRIMARY KEY, val POLYGON)").await;
    ctx.execute("INSERT INTO dt_poly (val) VALUES ('((0,0),(1,0),(1,1),(0,1))')", &[]).await;

    let row = ctx.query_one("SELECT val FROM dt_poly", &[]).await;
    assert!(row["val"].is_string());
    let v = row["val"].as_str().unwrap();
    assert!(v.contains("0,0") && v.contains("1,1"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_circle() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_circle (id SERIAL PRIMARY KEY, val CIRCLE)").await;
    ctx.execute("INSERT INTO dt_circle (val) VALUES ('<(1,2),3>'), ('<(0,0),10>')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_circle ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let v0 = arr[0]["val"].as_str().unwrap();
    assert!(v0.contains("1") && v0.contains("2") && v0.contains("3"));

    ctx.stop().await;
}

// ── NUMERIC precision ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_numeric_direct() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_numeric_d (id SERIAL PRIMARY KEY, val NUMERIC)").await;
    ctx.execute("INSERT INTO dt_numeric_d (val) VALUES (123.456789), (0.000001), (1234567890.12), (0), (-42.5)", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_numeric_d ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 5);
    // NUMERIC is converted via f64 — check approximate values
    let v0 = arr[0]["val"].as_f64().unwrap();
    assert!((v0 - 123.456789).abs() < 0.0001);
    let v1 = arr[1]["val"].as_f64().unwrap();
    assert!((v1 - 0.000001).abs() < 0.000001);
    let v3 = arr[3]["val"].as_f64().unwrap();
    assert!((v3 - 0.0).abs() < 0.001);
    let v4 = arr[4]["val"].as_f64().unwrap();
    assert!((v4 - (-42.5)).abs() < 0.01);

    ctx.stop().await;
}

// ── Range types ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_int4range() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_i4range (id SERIAL PRIMARY KEY, val INT4RANGE)").await;
    ctx.execute("INSERT INTO dt_i4range (val) VALUES ('[1,10)'), ('(5,15]'), ('empty'), ('[,10)'), ('[5,)')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_i4range ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 5);
    // Ranges are returned as strings
    assert!(arr[0]["val"].is_string());
    let v0 = arr[0]["val"].as_str().unwrap();
    assert!(v0.contains("1") && v0.contains("10"));
    assert_eq!(arr[2]["val"], "empty");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_int8range() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_i8range (id SERIAL PRIMARY KEY, val INT8RANGE)").await;
    ctx.execute("INSERT INTO dt_i8range (val) VALUES ('[1,1000000000)'), ('empty')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_i8range ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert!(arr[0]["val"].as_str().unwrap().contains("1000000000"));
    assert_eq!(arr[1]["val"], "empty");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_numrange() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_numrange (id SERIAL PRIMARY KEY, val NUMRANGE)").await;
    ctx.execute("INSERT INTO dt_numrange (val) VALUES ('[1.5,3.5)'), ('(0.1,0.9]')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_numrange ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let v0 = arr[0]["val"].as_str().unwrap();
    assert!(v0.contains("1.5") && v0.contains("3.5"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_daterange() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_daterange (id SERIAL PRIMARY KEY, val DATERANGE)").await;
    ctx.execute("INSERT INTO dt_daterange (val) VALUES ('[2024-01-01,2024-12-31]'), ('empty')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_daterange ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let v0 = arr[0]["val"].as_str().unwrap();
    assert!(v0.contains("2024-01-01"));
    assert_eq!(arr[1]["val"], "empty");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_tsrange() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_tsrange (id SERIAL PRIMARY KEY, val TSRANGE)").await;
    ctx.execute("INSERT INTO dt_tsrange (val) VALUES ('[\"2024-01-01 00:00:00\",\"2024-12-31 23:59:59\")')", &[]).await;

    let row = ctx.query_one("SELECT val FROM dt_tsrange", &[]).await;
    let v = row["val"].as_str().unwrap();
    assert!(v.contains("2024-01-01") && v.contains("2024-12-31"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_tstzrange() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_tstzrange (id SERIAL PRIMARY KEY, val TSTZRANGE)").await;
    ctx.execute(
        "INSERT INTO dt_tstzrange (val) VALUES ('[\"2024-01-01 00:00:00+00\",\"2024-12-31 23:59:59+00\")')",
        &[],
    )
    .await;

    let row = ctx.query_one("SELECT val FROM dt_tstzrange", &[]).await;
    let v = row["val"].as_str().unwrap();
    assert!(v.contains("2024-01-01") && v.contains("2024-12-31"));

    ctx.stop().await;
}

// ── Range operations ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_range_operations() {
    let mut ctx = PostgresTestContext::new().await;

    // Containment
    let row = ctx.query_one("SELECT '[1,10)'::int4range @> 5 AS contains", &[]).await;
    assert_eq!(row["contains"], true);

    let row = ctx.query_one("SELECT '[1,10)'::int4range @> 15 AS contains", &[]).await;
    assert_eq!(row["contains"], false);

    // Overlap
    let row = ctx.query_one("SELECT '[1,10)'::int4range && '[5,15)'::int4range AS overlaps", &[]).await;
    assert_eq!(row["overlaps"], true);

    // Union
    let row = ctx.query_one("SELECT '[1,5)'::int4range + '[3,10)'::int4range AS combined", &[]).await;
    let v = row["combined"].as_str().unwrap();
    assert!(v.contains("1") && v.contains("10"));

    // Intersection
    let row = ctx.query_one("SELECT '[1,10)'::int4range * '[5,15)'::int4range AS intersected", &[]).await;
    let v = row["intersected"].as_str().unwrap();
    assert!(v.contains("5") && v.contains("10"));

    ctx.stop().await;
}

// ── Enum type ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_enum() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')").await;
    ctx.batch_execute("CREATE TABLE dt_enum (id SERIAL PRIMARY KEY, val mood)").await;
    ctx.execute("INSERT INTO dt_enum (val) VALUES ('happy'), ('sad'), ('ok')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_enum ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // Enum values returned as strings
    assert_eq!(arr[0]["val"], "happy");
    assert_eq!(arr[1]["val"], "sad");
    assert_eq!(arr[2]["val"], "ok");

    ctx.stop().await;
}

// ── Domain type ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_domain() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0)").await;
    ctx.batch_execute("CREATE TABLE dt_domain (id SERIAL PRIMARY KEY, val positive_int)").await;
    ctx.execute("INSERT INTO dt_domain (val) VALUES (42), (1), (100)", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_domain ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["val"], 42);
    assert_eq!(arr[1]["val"], 1);
    assert_eq!(arr[2]["val"], 100);

    // Violating the domain constraint should error
    let err = ctx.execute_err("INSERT INTO dt_domain (val) VALUES (-1)", &[]).await;
    let err_msg = err.to_string();
    assert!(err_msg.contains("positive_int") || err_msg.contains("check") || err_msg.contains("violates"));

    ctx.stop().await;
}

// ── Composite type ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_composite() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TYPE address AS (street TEXT, city TEXT, zip TEXT)").await;
    ctx.batch_execute("CREATE TABLE dt_composite (id SERIAL PRIMARY KEY, addr address)").await;
    ctx.execute("INSERT INTO dt_composite (addr) VALUES (ROW('123 Main St', 'Springfield', '12345'))", &[]).await;

    let row = ctx.query_one("SELECT addr FROM dt_composite", &[]).await;
    // Composite types are returned as strings in PG text format: (val1,val2,val3)
    assert!(row["addr"].is_string());
    let v = row["addr"].as_str().unwrap();
    assert!(v.contains("123 Main St") && v.contains("Springfield") && v.contains("12345"));

    // Access individual fields
    let row = ctx.query_one("SELECT (addr).street, (addr).city, (addr).zip FROM dt_composite", &[]).await;
    assert_eq!(row["street"], "123 Main St");
    assert_eq!(row["city"], "Springfield");
    assert_eq!(row["zip"], "12345");

    ctx.stop().await;
}

// ── BPCHAR (blank-padded character) ──────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_bpchar() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_bpchar (id SERIAL PRIMARY KEY, val CHAR(10))").await;
    ctx.execute("INSERT INTO dt_bpchar (val) VALUES ('hello'), ('ab')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_bpchar ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    // CHAR(10) pads with spaces
    let v0 = arr[0]["val"].as_str().unwrap();
    assert!(v0.starts_with("hello"));
    assert_eq!(v0.len(), 10);

    ctx.stop().await;
}

// ── XML type ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_xml() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_xml (id SERIAL PRIMARY KEY, val XML)").await;
    ctx.execute("INSERT INTO dt_xml (val) VALUES ('<root><item>hello</item></root>')", &[]).await;

    let row = ctx.query_one("SELECT val FROM dt_xml", &[]).await;
    assert!(row["val"].is_string());
    let v = row["val"].as_str().unwrap();
    assert!(v.contains("<root>") && v.contains("hello"));

    ctx.stop().await;
}

// ── PG_LSN type ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_pg_lsn() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT '16/B374D848'::pg_lsn AS lsn", &[]).await;
    assert!(row["lsn"].is_string());
    let v = row["lsn"].as_str().unwrap();
    assert!(v.contains("16/B374D848") || v.contains("16/b374d848"));

    ctx.stop().await;
}

// ── Multirange types (PostgreSQL 14+) ────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_int4multirange() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_i4mr (id SERIAL PRIMARY KEY, val INT4MULTIRANGE)").await;
    ctx.execute("INSERT INTO dt_i4mr (val) VALUES ('{[1,5), [10,15)}'), ('{[3,7]}'), ('{}')", &[]).await;

    let rows = ctx.query("SELECT val FROM dt_i4mr ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert!(arr[0]["val"].is_string());
    let v0 = arr[0]["val"].as_str().unwrap();
    assert!(v0.contains("[1,5)") && v0.contains("[10,15)"));
    assert_eq!(arr[2]["val"], "{}");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_int8multirange() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_i8mr (id SERIAL PRIMARY KEY, val INT8MULTIRANGE)").await;
    ctx.execute("INSERT INTO dt_i8mr (val) VALUES ('{[1,1000000000), [2000000000,3000000000)}')", &[]).await;

    let row = ctx.query_one("SELECT val FROM dt_i8mr", &[]).await;
    let v = row["val"].as_str().unwrap();
    assert!(v.contains("1000000000") && v.contains("2000000000"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_nummultirange() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_nummr (id SERIAL PRIMARY KEY, val NUMMULTIRANGE)").await;
    ctx.execute("INSERT INTO dt_nummr (val) VALUES ('{[1.5,3.5), [10.0,20.0)}')", &[]).await;

    let row = ctx.query_one("SELECT val FROM dt_nummr", &[]).await;
    let v = row["val"].as_str().unwrap();
    assert!(v.contains("1.5") && v.contains("20.0"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_datemultirange() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_datemr (id SERIAL PRIMARY KEY, val DATEMULTIRANGE)").await;
    ctx.execute("INSERT INTO dt_datemr (val) VALUES ('{[2024-01-01,2024-03-01), [2024-06-01,2024-09-01)}')", &[]).await;

    let row = ctx.query_one("SELECT val FROM dt_datemr", &[]).await;
    let v = row["val"].as_str().unwrap();
    assert!(v.contains("2024-01-01") && v.contains("2024-06-01"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_tsmultirange() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_tsmr (id SERIAL PRIMARY KEY, val TSMULTIRANGE)").await;
    ctx.execute(
        r#"INSERT INTO dt_tsmr (val) VALUES ('{["2024-01-01 00:00:00","2024-03-01 00:00:00"), ["2024-06-01 00:00:00","2024-09-01 00:00:00")}')"#,
        &[],
    )
    .await;

    let row = ctx.query_one("SELECT val FROM dt_tsmr", &[]).await;
    let v = row["val"].as_str().unwrap();
    assert!(v.contains("2024-01-01") && v.contains("2024-06-01"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_tstzmultirange() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_tstzmr (id SERIAL PRIMARY KEY, val TSTZMULTIRANGE)").await;
    ctx.execute(
        r#"INSERT INTO dt_tstzmr (val) VALUES ('{["2024-01-01 00:00:00+00","2024-03-01 00:00:00+00")}')"#,
        &[],
    )
    .await;

    let row = ctx.query_one("SELECT val FROM dt_tstzmr", &[]).await;
    let v = row["val"].as_str().unwrap();
    assert!(v.contains("2024-01-01") && v.contains("2024-03-01"));

    ctx.stop().await;
}

// ── Multirange operations ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_multirange_operations() {
    let mut ctx = PostgresTestContext::new().await;

    // Containment
    let row = ctx.query_one("SELECT '{[1,5), [10,15)}'::int4multirange @> 3 AS contains", &[]).await;
    assert_eq!(row["contains"], true);

    let row = ctx.query_one("SELECT '{[1,5), [10,15)}'::int4multirange @> 7 AS contains", &[]).await;
    assert_eq!(row["contains"], false);

    // Overlap
    let row = ctx.query_one("SELECT '{[1,5), [10,15)}'::int4multirange && '{[3,12)}'::int4multirange AS overlaps", &[]).await;
    assert_eq!(row["overlaps"], true);

    // Union
    let row = ctx.query_one("SELECT '{[1,5)}'::int4multirange + '{[3,10)}'::int4multirange AS combined", &[]).await;
    let v = row["combined"].as_str().unwrap();
    assert!(v.contains("1") && v.contains("10"));

    // isEmpty
    let row = ctx.query_one("SELECT isempty('{}'::int4multirange) AS is_empty", &[]).await;
    assert_eq!(row["is_empty"], true);

    ctx.stop().await;
}

// ── JSONPATH (PostgreSQL 12+) ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_jsonpath() {
    let mut ctx = PostgresTestContext::new().await;

    // JSONPATH is a compiled expression type for querying JSONB
    let row = ctx.query_one("SELECT '$.store.book[*].author'::jsonpath AS path", &[]).await;
    assert!(row["path"].is_string());
    let v = row["path"].as_str().unwrap();
    assert!(v.contains("store") && v.contains("author"));

    // Use jsonpath with jsonb_path_query
    ctx.batch_execute("CREATE TABLE dt_jsonpath (id SERIAL PRIMARY KEY, data JSONB)").await;
    ctx.execute(r#"INSERT INTO dt_jsonpath (data) VALUES ('{"a": 1, "b": [2, 3, 4]}')"#, &[]).await;

    let row = ctx.query_one("SELECT jsonb_path_exists(data, '$.b[*] ? (@ > 2)') AS has_match FROM dt_jsonpath", &[]).await;
    assert_eq!(row["has_match"], true);

    let row = ctx.query_one("SELECT jsonb_path_query_first(data, '$.a') AS val FROM dt_jsonpath", &[]).await;
    assert_eq!(row["val"], 1);

    ctx.stop().await;
}

// ── OID type ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_oid() {
    let mut ctx = PostgresTestContext::new().await;

    // OID is a 32-bit unsigned integer used for object identifiers
    let row = ctx.query_one("SELECT 'pg_class'::regclass::oid AS class_oid", &[]).await;
    assert!(row["class_oid"].is_number());
    let oid = row["class_oid"].as_i64().unwrap();
    assert!(oid > 0);

    ctx.stop().await;
}

// ── "char" type (single-byte, OID 18) ────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_internal_char() {
    let mut ctx = PostgresTestContext::new().await;

    // "char" (with quotes) is PG's internal single-byte character type, distinct from CHAR(n)
    let row = ctx.query_one("SELECT 'A'::\"char\" AS ch", &[]).await;
    assert_eq!(row["ch"], "A");

    let row = ctx.query_one("SELECT 'Z'::\"char\" AS ch", &[]).await;
    assert_eq!(row["ch"], "Z");

    ctx.stop().await;
}

// ── NAME type (OID 19) ──────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_name() {
    let mut ctx = PostgresTestContext::new().await;

    // NAME is a 63-byte fixed-length string used for identifiers
    let row = ctx.query_one("SELECT current_database()::name AS db_name", &[]).await;
    assert!(row["db_name"].is_string());
    assert_eq!(row["db_name"], "postgres");

    // Column names from pg_attribute are NAME type
    let row = ctx
        .query_one(
            "SELECT attname FROM pg_attribute WHERE attrelid = 'pg_class'::regclass AND attname = 'relname'",
            &[],
        )
        .await;
    assert_eq!(row["attname"], "relname");

    ctx.stop().await;
}

// ── TID type (tuple identifier) ──────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_tid() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_tid (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO dt_tid (val) VALUES ('hello')", &[]).await;

    // ctid is the physical row location
    let row = ctx.query_one("SELECT ctid FROM dt_tid WHERE id = 1", &[]).await;
    assert!(row["ctid"].is_string());
    let tid = row["ctid"].as_str().unwrap();
    // TID format is (page,offset) e.g., "(0,1)"
    assert!(tid.starts_with('(') && tid.ends_with(')'));
    assert!(tid.contains(','));

    ctx.stop().await;
}

// ── XID type (transaction ID) ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_xid() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_xid (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO dt_xid (val) VALUES ('hello')", &[]).await;

    // xmin is the inserting transaction's XID
    let row = ctx.query_one("SELECT xmin FROM dt_xid WHERE id = 1", &[]).await;
    assert!(row["xmin"].is_number());
    let xid = row["xmin"].as_i64().unwrap();
    assert!(xid > 0);

    ctx.stop().await;
}

// ── XID8 type (64-bit transaction ID, PG 13+) ───────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_xid8() {
    let mut ctx = PostgresTestContext::new().await;

    // pg_current_xact_id() returns xid8 (PG 13+)
    let row = ctx.query_one("SELECT pg_current_xact_id() AS txid", &[]).await;
    assert!(row["txid"].is_number());
    let txid = row["txid"].as_i64().unwrap();
    assert!(txid > 0);

    ctx.stop().await;
}

// ── CID type (command ID) ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_cid() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_cid (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO dt_cid (val) VALUES ('hello')", &[]).await;

    // cmin is the command ID within the inserting transaction
    let row = ctx.query_one("SELECT cmin FROM dt_cid WHERE id = 1", &[]).await;
    assert!(row["cmin"].is_number());

    ctx.stop().await;
}

// ── REGCLASS type ────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_regclass() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT 'pg_class'::regclass AS rc", &[]).await;
    assert!(row["rc"].is_string());
    assert_eq!(row["rc"], "pg_class");

    // Cast to OID to get numeric value
    let row = ctx.query_one("SELECT 'pg_class'::regclass::oid AS rc_oid", &[]).await;
    assert!(row["rc_oid"].is_number());

    ctx.stop().await;
}

// ── REGTYPE type ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_regtype() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT 'integer'::regtype AS rt", &[]).await;
    assert!(row["rt"].is_string());
    assert_eq!(row["rt"], "integer");

    let row = ctx.query_one("SELECT 'text'::regtype AS rt", &[]).await;
    assert_eq!(row["rt"], "text");

    ctx.stop().await;
}

// ── TSVECTOR / TSQUERY (full-text search) ────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_tsvector() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_tsv (id SERIAL PRIMARY KEY, doc TSVECTOR)").await;
    ctx.execute("INSERT INTO dt_tsv (doc) VALUES (to_tsvector('english', 'the quick brown fox'))", &[]).await;

    let row = ctx.query_one("SELECT doc FROM dt_tsv", &[]).await;
    assert!(row["doc"].is_string());
    let doc = row["doc"].as_str().unwrap();
    assert!(doc.contains("brown") && doc.contains("fox") && doc.contains("quick"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_tsquery() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT to_tsquery('english', 'quick & fox') AS q", &[]).await;
    assert!(row["q"].is_string());
    let q = row["q"].as_str().unwrap();
    assert!(q.contains("quick") && q.contains("fox"));

    // Full-text search match
    let row = ctx
        .query_one(
            "SELECT to_tsvector('english', 'the quick brown fox') @@ to_tsquery('english', 'quick & fox') AS matches",
            &[],
        )
        .await;
    assert_eq!(row["matches"], true);

    ctx.stop().await;
}

// ── MONEY arithmetic ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_money_arithmetic() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT ('$10.50'::money + '$3.25'::money) AS total", &[]).await;
    let v = row["total"].as_str().unwrap();
    assert!(v.contains("13.75"));

    let row = ctx.query_one("SELECT ('$100.00'::money * 0.15) AS tax", &[]).await;
    let v = row["tax"].as_str().unwrap();
    assert!(v.contains("15.00"));

    ctx.stop().await;
}
