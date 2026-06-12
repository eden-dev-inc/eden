use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

// ── BOOL[] ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_bool() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_bool (id SERIAL PRIMARY KEY, vals BOOLEAN[])").await;
    ctx.execute("INSERT INTO arr_bool (vals) VALUES (ARRAY[true, false, true])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_bool", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 3);
    assert_eq!(vals[0], true);
    assert_eq!(vals[1], false);
    assert_eq!(vals[2], true);

    ctx.stop().await;
}

// ── INT2[] ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_int2() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_int2 (id SERIAL PRIMARY KEY, vals SMALLINT[])").await;
    ctx.execute("INSERT INTO arr_int2 (vals) VALUES ('{1,2,-100}'::SMALLINT[])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_int2", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 3);
    assert_eq!(vals[0], 1);
    assert_eq!(vals[1], 2);
    assert_eq!(vals[2], -100);

    ctx.stop().await;
}

// ── INT8[] ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_int8() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_int8 (id SERIAL PRIMARY KEY, vals BIGINT[])").await;
    ctx.execute("INSERT INTO arr_int8 (vals) VALUES (ARRAY[9999999999::BIGINT, -1::BIGINT])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_int8", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0], 9999999999_i64);
    assert_eq!(vals[1], -1);

    ctx.stop().await;
}

// ── FLOAT4[] ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_float4() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_float4 (id SERIAL PRIMARY KEY, vals REAL[])").await;
    ctx.execute("INSERT INTO arr_float4 (vals) VALUES (ARRAY[1.5, -2.5, 0.0]::REAL[])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_float4", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 3);
    let v0 = vals[0].as_f64().unwrap();
    assert!((v0 - 1.5).abs() < 0.01);
    let v1 = vals[1].as_f64().unwrap();
    assert!((v1 - (-2.5)).abs() < 0.01);

    ctx.stop().await;
}

// ── FLOAT8[] ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_float8() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_float8 (id SERIAL PRIMARY KEY, vals DOUBLE PRECISION[])").await;
    ctx.execute("INSERT INTO arr_float8 (vals) VALUES (ARRAY[3.14159, 2.71828])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_float8", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    let v0 = vals[0].as_f64().unwrap();
    assert!((v0 - 3.14159).abs() < 0.0001);

    ctx.stop().await;
}

// ── NUMERIC[] ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_numeric() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_numeric (id SERIAL PRIMARY KEY, vals NUMERIC[])").await;
    ctx.execute("INSERT INTO arr_numeric (vals) VALUES (ARRAY[123.45, 0.01, 99999.99])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_numeric", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 3);
    let v0 = vals[0].as_f64().unwrap();
    assert!((v0 - 123.45).abs() < 0.01);

    ctx.stop().await;
}

// ── VARCHAR[] ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_varchar() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_varchar (id SERIAL PRIMARY KEY, vals VARCHAR[])").await;
    ctx.execute("INSERT INTO arr_varchar (vals) VALUES (ARRAY['hello', 'world']::VARCHAR[])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_varchar", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0], "hello");
    assert_eq!(vals[1], "world");

    ctx.stop().await;
}

// ── UUID[] ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_uuid() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"").await;
    ctx.batch_execute("CREATE TABLE arr_uuid (id SERIAL PRIMARY KEY, vals UUID[])").await;
    ctx.execute("INSERT INTO arr_uuid (vals) VALUES (ARRAY[uuid_generate_v4(), uuid_generate_v4()])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_uuid", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    // Each UUID should be a 36-char string
    assert_eq!(vals[0].as_str().unwrap().len(), 36);
    assert_eq!(vals[1].as_str().unwrap().len(), 36);

    ctx.stop().await;
}

// ── DATE[] ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_date() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_date (id SERIAL PRIMARY KEY, vals DATE[])").await;
    ctx.execute("INSERT INTO arr_date (vals) VALUES (ARRAY['2024-01-01'::DATE, '2024-12-31'::DATE])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_date", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0], "2024-01-01");
    assert_eq!(vals[1], "2024-12-31");

    ctx.stop().await;
}

// ── TIME[] ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_time() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_time (id SERIAL PRIMARY KEY, vals TIME[])").await;
    ctx.execute("INSERT INTO arr_time (vals) VALUES (ARRAY['14:30:00'::TIME, '00:00:00'::TIME])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_time", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0], "14:30:00");
    assert_eq!(vals[1], "00:00:00");

    ctx.stop().await;
}

// ── TIMESTAMP[] ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_timestamp() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_ts (id SERIAL PRIMARY KEY, vals TIMESTAMP[])").await;
    ctx.execute(
        "INSERT INTO arr_ts (vals) VALUES (ARRAY['2024-01-01 12:00:00'::TIMESTAMP, '2024-12-31 23:59:59'::TIMESTAMP])",
        &[],
    )
    .await;

    let row = ctx.query_one("SELECT vals FROM arr_ts", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert!(vals[0].as_str().unwrap().contains("2024-01-01"));
    assert!(vals[1].as_str().unwrap().contains("2024-12-31"));

    ctx.stop().await;
}

// ── TIMESTAMPTZ[] ────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_timestamptz() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_tstz (id SERIAL PRIMARY KEY, vals TIMESTAMPTZ[])").await;
    ctx.execute("INSERT INTO arr_tstz (vals) VALUES (ARRAY['2024-01-01 12:00:00+00'::TIMESTAMPTZ])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_tstz", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 1);
    let ts = vals[0].as_str().unwrap();
    assert!(ts.contains("2024-01-01"));
    assert!(ts.contains("+00"));

    ctx.stop().await;
}

// ── INTERVAL[] ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_interval() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_interval (id SERIAL PRIMARY KEY, vals INTERVAL[])").await;
    ctx.execute("INSERT INTO arr_interval (vals) VALUES (ARRAY['1 day'::INTERVAL, '2 hours'::INTERVAL])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_interval", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert!(vals[0].as_str().unwrap().contains("day") || vals[0].as_str().unwrap().contains("1"));
    assert!(vals[1].as_str().unwrap().contains("02:00:00") || vals[1].as_str().unwrap().contains("hour"));

    ctx.stop().await;
}

// ── JSON[] ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_json() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_json (id SERIAL PRIMARY KEY, vals JSON[])").await;
    ctx.execute(r#"INSERT INTO arr_json (vals) VALUES (ARRAY['{"a":1}'::JSON, '{"b":2}'::JSON])"#, &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_json", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0]["a"], 1);
    assert_eq!(vals[1]["b"], 2);

    ctx.stop().await;
}

// ── JSONB[] ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_jsonb() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_jsonb (id SERIAL PRIMARY KEY, vals JSONB[])").await;
    ctx.execute(r#"INSERT INTO arr_jsonb (vals) VALUES (ARRAY['[1,2]'::JSONB, '{"key":"val"}'::JSONB])"#, &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_jsonb", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0], serde_json::json!([1, 2]));
    assert_eq!(vals[1]["key"], "val");

    ctx.stop().await;
}

// ── INET[] ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_inet() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_inet (id SERIAL PRIMARY KEY, vals INET[])").await;
    ctx.execute("INSERT INTO arr_inet (vals) VALUES (ARRAY['192.168.1.1'::INET, '::1'::INET])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_inet", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0], "192.168.1.1");
    assert_eq!(vals[1], "::1");

    ctx.stop().await;
}

// ── CIDR[] ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_cidr() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_cidr (id SERIAL PRIMARY KEY, vals CIDR[])").await;
    ctx.execute("INSERT INTO arr_cidr (vals) VALUES (ARRAY['10.0.0.0/8'::CIDR, '192.168.0.0/16'::CIDR])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_cidr", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0], "10.0.0.0/8");
    assert_eq!(vals[1], "192.168.0.0/16");

    ctx.stop().await;
}

// ── MACADDR[] ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_macaddr() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_macaddr (id SERIAL PRIMARY KEY, vals MACADDR[])").await;
    ctx.execute(
        "INSERT INTO arr_macaddr (vals) VALUES (ARRAY['08:00:2b:01:02:03'::MACADDR, 'aa:bb:cc:dd:ee:ff'::MACADDR])",
        &[],
    )
    .await;

    let row = ctx.query_one("SELECT vals FROM arr_macaddr", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0], "08:00:2b:01:02:03");
    assert_eq!(vals[1], "aa:bb:cc:dd:ee:ff");

    ctx.stop().await;
}

// ── Array with NULLs ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_with_nulls() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_nulls (id SERIAL PRIMARY KEY, vals INT4[])").await;
    ctx.execute("INSERT INTO arr_nulls (vals) VALUES (ARRAY[1, NULL, 3])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_nulls", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 3);
    assert_eq!(vals[0], 1);
    assert!(vals[1].is_null());
    assert_eq!(vals[2], 3);

    ctx.stop().await;
}

// ── Empty array ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_empty() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_empty (id SERIAL PRIMARY KEY, vals INT4[])").await;
    ctx.execute("INSERT INTO arr_empty (vals) VALUES (ARRAY[]::INT4[])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_empty", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 0);

    ctx.stop().await;
}

// ── Array with special characters ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_text_special_chars() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_special (id SERIAL PRIMARY KEY, vals TEXT[])").await;
    ctx.execute(r#"INSERT INTO arr_special (vals) VALUES (ARRAY['hello world', 'it''s', 'a,b', 'c"d'])"#, &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_special", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 4);
    assert_eq!(vals[0], "hello world");
    assert_eq!(vals[1], "it's");
    assert_eq!(vals[2], "a,b");
    assert_eq!(vals[3], "c\"d");

    ctx.stop().await;
}

// ── Float array with NaN/Infinity ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_float_special_values() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_float_special (id SERIAL PRIMARY KEY, vals FLOAT8[])").await;
    ctx.execute(
        "INSERT INTO arr_float_special (vals) VALUES (ARRAY[1.5, 'NaN'::FLOAT8, 'Infinity'::FLOAT8, '-Infinity'::FLOAT8])",
        &[],
    )
    .await;

    let row = ctx.query_one("SELECT vals FROM arr_float_special", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 4);
    let v0 = vals[0].as_f64().unwrap();
    assert!((v0 - 1.5).abs() < 0.01);
    assert!(vals[1].is_null()); // NaN → null
    assert!(vals[2].is_null()); // Infinity → null
    assert!(vals[3].is_null()); // -Infinity → null

    ctx.stop().await;
}

// ── Multi-dimensional array ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_multidimensional() {
    let mut ctx = PostgresTestContext::new().await;

    // PostgreSQL supports multi-dimensional arrays
    // The text format is: {{1,2,3},{4,5,6}}
    // Our parser currently handles the outer {} and treats inner elements
    let row = ctx.query_one("SELECT array_ndims(ARRAY[[1,2],[3,4]]) AS ndims", &[]).await;
    assert_eq!(row["ndims"], 2);

    // Verify the cardinality
    let row = ctx.query_one("SELECT cardinality(ARRAY[[1,2,3],[4,5,6]]) AS card", &[]).await;
    assert_eq!(row["card"], 6);

    ctx.stop().await;
}

// ── BYTEA[] ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_bytea() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_bytea (id SERIAL PRIMARY KEY, vals BYTEA[])").await;
    ctx.execute(r"INSERT INTO arr_bytea (vals) VALUES (ARRAY[E'\\xDEAD'::BYTEA, E'\\xBEEF'::BYTEA])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_bytea", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    // BYTEA elements are returned as hex-escaped strings
    assert!(vals[0].is_string());
    assert!(vals[1].is_string());

    ctx.stop().await;
}

// ── MONEY[] ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_money() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_money (id SERIAL PRIMARY KEY, vals MONEY[])").await;
    ctx.execute("INSERT INTO arr_money (vals) VALUES (ARRAY['$1.50'::MONEY, '$2.75'::MONEY])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_money", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    // MONEY array elements are returned as strings (e.g., "$1.50")
    assert!(vals[0].is_string());
    let v0 = vals[0].as_str().unwrap();
    assert!(v0.contains("1.50"));
    let v1 = vals[1].as_str().unwrap();
    assert!(v1.contains("2.75"));

    ctx.stop().await;
}

// ── OID[] ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_oid() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_oid (id SERIAL PRIMARY KEY, vals OID[])").await;
    ctx.execute("INSERT INTO arr_oid (vals) VALUES (ARRAY[1::OID, 2::OID, 16384::OID])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_oid", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 3);
    assert_eq!(vals[0], 1);
    assert_eq!(vals[1], 2);
    assert_eq!(vals[2], 16384);

    ctx.stop().await;
}

// ── BIT[] ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_bit() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_bit (id SERIAL PRIMARY KEY, vals BIT(4)[])").await;
    ctx.execute("INSERT INTO arr_bit (vals) VALUES (ARRAY[B'1010', B'1111', B'0000'])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_bit", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 3);
    assert_eq!(vals[0], "1010");
    assert_eq!(vals[1], "1111");
    assert_eq!(vals[2], "0000");

    ctx.stop().await;
}

// ── VARBIT[] ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_varbit() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_varbit (id SERIAL PRIMARY KEY, vals BIT VARYING[])").await;
    ctx.execute("INSERT INTO arr_varbit (vals) VALUES (ARRAY[B'1', B'101', B'11111111'])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM arr_varbit", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 3);
    assert_eq!(vals[0], "1");
    assert_eq!(vals[1], "101");
    assert_eq!(vals[2], "11111111");

    ctx.stop().await;
}

// ── NAME[] ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_name() {
    let mut ctx = PostgresTestContext::new().await;

    // NAME[] via system catalog query
    let row = ctx.query_one("SELECT ARRAY['hello'::name, 'world'::name] AS vals", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0], "hello");
    assert_eq!(vals[1], "world");

    ctx.stop().await;
}

// ── "char"[] ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_char() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT ARRAY['A'::\"char\", 'B'::\"char\", 'C'::\"char\"] AS vals", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 3);
    assert_eq!(vals[0], "A");
    assert_eq!(vals[1], "B");
    assert_eq!(vals[2], "C");

    ctx.stop().await;
}

// ── MACADDR8[] ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_macaddr8() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_mac8 (id SERIAL PRIMARY KEY, vals MACADDR8[])").await;
    ctx.execute(
        "INSERT INTO arr_mac8 (vals) VALUES (ARRAY['08:00:2b:01:02:03:04:05'::MACADDR8, 'aa:bb:cc:dd:ee:ff:00:11'::MACADDR8])",
        &[],
    )
    .await;

    let row = ctx.query_one("SELECT vals FROM arr_mac8", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0], "08:00:2b:01:02:03:04:05");
    assert_eq!(vals[1], "aa:bb:cc:dd:ee:ff:00:11");

    ctx.stop().await;
}
