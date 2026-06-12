use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_bool() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_bool (id SERIAL PRIMARY KEY, val BOOLEAN)").await;
    ctx.execute("INSERT INTO dt_bool (val) VALUES ($1), ($2)", &[SqlParam::Bool(true), SqlParam::Bool(false)]).await;

    let rows = ctx.query("SELECT val FROM dt_bool ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["val"], true);
    assert_eq!(arr[1]["val"], false);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_int2() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_int2 (id SERIAL PRIMARY KEY, val SMALLINT)").await;
    ctx.execute(
        "INSERT INTO dt_int2 (val) VALUES ($1), ($2), ($3)",
        &[SqlParam::Int2(0), SqlParam::Int2(i16::MAX), SqlParam::Int2(i16::MIN)],
    )
    .await;

    let rows = ctx.query("SELECT val FROM dt_int2 ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["val"], 0);
    assert_eq!(arr[1]["val"], i16::MAX as i64);
    assert_eq!(arr[2]["val"], i16::MIN as i64);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_int4() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_int4 (id SERIAL PRIMARY KEY, val INTEGER)").await;
    ctx.execute(
        "INSERT INTO dt_int4 (val) VALUES ($1), ($2), ($3)",
        &[SqlParam::Int4(42), SqlParam::Int4(i32::MAX), SqlParam::Int4(i32::MIN)],
    )
    .await;

    let rows = ctx.query("SELECT val FROM dt_int4 ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["val"], 42);
    assert_eq!(arr[1]["val"], i32::MAX as i64);
    assert_eq!(arr[2]["val"], i32::MIN as i64);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_int8() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_int8 (id SERIAL PRIMARY KEY, val BIGINT)").await;
    ctx.execute("INSERT INTO dt_int8 (val) VALUES ($1), ($2)", &[SqlParam::Int8(9_999_999_999), SqlParam::Int8(-1)]).await;

    let rows = ctx.query("SELECT val FROM dt_int8 ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["val"], 9_999_999_999_i64);
    assert_eq!(arr[1]["val"], -1);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_float4() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_float4 (id SERIAL PRIMARY KEY, val REAL)").await;
    ctx.execute(
        "INSERT INTO dt_float4 (val) VALUES ($1), ($2)",
        &[SqlParam::Float4(std::f32::consts::PI), SqlParam::Float4(-0.5)],
    )
    .await;

    let rows = ctx.query("SELECT val FROM dt_float4 ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    let v0 = arr[0]["val"].as_f64().unwrap();
    assert!((v0 - f64::from(std::f32::consts::PI)).abs() < 0.01);
    let v1 = arr[1]["val"].as_f64().unwrap();
    assert!((v1 - (-0.5)).abs() < 0.01);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_float8() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_float8 (id SERIAL PRIMARY KEY, val DOUBLE PRECISION)").await;
    ctx.execute(
        "INSERT INTO dt_float8 (val) VALUES ($1), ($2)",
        &[SqlParam::Float8(std::f64::consts::E), SqlParam::Float8(0.0)],
    )
    .await;

    let rows = ctx.query("SELECT val FROM dt_float8 ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    let v0 = arr[0]["val"].as_f64().unwrap();
    assert!((v0 - std::f64::consts::E).abs() < 0.000001);
    assert_eq!(arr[1]["val"], 0.0);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_numeric() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_numeric (id SERIAL PRIMARY KEY, val NUMERIC(10,2))").await;
    ctx.execute("INSERT INTO dt_numeric (val) VALUES (123.45), (0.01), (99999.99)", &[]).await;

    let rows = ctx.query("SELECT val::FLOAT8 AS val FROM dt_numeric ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    // NUMERIC is cast to FLOAT8 for retrieval (f64 FromSql only supports FLOAT8)
    let v0 = arr[0]["val"].as_f64().unwrap();
    assert!((v0 - 123.45).abs() < 0.01);
    let v1 = arr[1]["val"].as_f64().unwrap();
    assert!((v1 - 0.01).abs() < 0.001);
    let v2 = arr[2]["val"].as_f64().unwrap();
    assert!((v2 - 99999.99).abs() < 0.01);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_text() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_text (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute(
        "INSERT INTO dt_text (val) VALUES ($1), ($2), ($3)",
        &[
            SqlParam::Text("hello world".to_string()),
            SqlParam::Text(String::new()),
            SqlParam::Text("unicode: \u{1F600}\u{1F4A9}".to_string()),
        ],
    )
    .await;

    let rows = ctx.query("SELECT val FROM dt_text ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["val"], "hello world");
    assert_eq!(arr[1]["val"], "");
    assert_eq!(arr[2]["val"], "unicode: \u{1F600}\u{1F4A9}");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_varchar() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_varchar (id SERIAL PRIMARY KEY, val VARCHAR(10))").await;
    ctx.execute("INSERT INTO dt_varchar (val) VALUES ($1)", &[SqlParam::Text("short".to_string())]).await;

    let row = ctx.query_one("SELECT val FROM dt_varchar", &[]).await;
    assert_eq!(row["val"], "short");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_json() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_json (id SERIAL PRIMARY KEY, val JSON)").await;

    let json_val = serde_json::json!({"key": "value", "nested": {"a": 1}});
    ctx.execute("INSERT INTO dt_json (val) VALUES ($1)", &[SqlParam::Json(json_val.clone())]).await;

    let row = ctx.query_one("SELECT val FROM dt_json", &[]).await;
    assert_eq!(row["val"]["key"], "value");
    assert_eq!(row["val"]["nested"]["a"], 1);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_jsonb() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_jsonb (id SERIAL PRIMARY KEY, val JSONB)").await;

    let json_val = serde_json::json!({"tags": ["a", "b"], "count": 42});
    ctx.execute("INSERT INTO dt_jsonb (val) VALUES ($1)", &[SqlParam::Json(json_val)]).await;

    // Test JSONB containment operator via simple_query
    let row = ctx.query_one("SELECT val FROM dt_jsonb WHERE val @> '{\"count\": 42}'", &[]).await;
    assert_eq!(row["val"]["count"], 42);

    // Test JSONB arrow operator
    let row = ctx.query_one("SELECT val->'tags' AS tags FROM dt_jsonb", &[]).await;
    let tags = row["tags"].as_array().unwrap();
    assert_eq!(tags.len(), 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_uuid() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";
         CREATE TABLE dt_uuid (id UUID DEFAULT uuid_generate_v4() PRIMARY KEY, name TEXT)",
    )
    .await;

    ctx.execute("INSERT INTO dt_uuid (name) VALUES ($1)", &[SqlParam::Text("test".to_string())]).await;

    let row = ctx.query_one("SELECT id, name FROM dt_uuid", &[]).await;
    // UUID is returned as string
    assert!(row["id"].is_string());
    let uuid_str = row["id"].as_str().unwrap();
    // Valid UUID format: 8-4-4-4-12 hex chars
    assert_eq!(uuid_str.len(), 36);
    assert_eq!(row["name"], "test");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_date() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_date (id SERIAL PRIMARY KEY, val DATE)").await;
    ctx.execute("INSERT INTO dt_date (val) VALUES ('2024-06-15')", &[]).await;

    let row = ctx.query_one("SELECT val FROM dt_date", &[]).await;
    assert_eq!(row["val"], "2024-06-15");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_time() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_time (id SERIAL PRIMARY KEY, val TIME)").await;
    ctx.execute("INSERT INTO dt_time (val) VALUES ('14:30:00')", &[]).await;

    let row = ctx.query_one("SELECT val FROM dt_time", &[]).await;
    assert_eq!(row["val"], "14:30:00");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_timestamp() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_ts (id SERIAL PRIMARY KEY, val TIMESTAMP)").await;
    ctx.execute("INSERT INTO dt_ts (val) VALUES ('2024-06-15 14:30:00')", &[]).await;

    let row = ctx.query_one("SELECT val FROM dt_ts", &[]).await;
    let ts = row["val"].as_str().unwrap();
    assert!(ts.contains("2024-06-15"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_timestamptz() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_tstz (id SERIAL PRIMARY KEY, val TIMESTAMPTZ)").await;
    ctx.execute("INSERT INTO dt_tstz (val) VALUES ('2024-06-15 14:30:00+00')", &[]).await;

    let row = ctx.query_one("SELECT val FROM dt_tstz", &[]).await;
    let ts = row["val"].as_str().unwrap();
    assert!(ts.contains("2024-06-15"));
    // PostgreSQL wire protocol uses "+00" offset format for UTC, not "UTC" literal
    assert!(ts.contains("+00"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_null() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_nulls (id SERIAL PRIMARY KEY, txt TEXT, num INT4, flag BOOLEAN)").await;

    // Insert with explicit NULLs
    ctx.execute(
        "INSERT INTO dt_nulls (txt, num, flag) VALUES ($1, $2, $3)",
        &[SqlParam::Null, SqlParam::Null, SqlParam::Null],
    )
    .await;

    let row = ctx.query_one("SELECT txt, num, flag FROM dt_nulls", &[]).await;
    assert!(row["txt"].is_null());
    assert!(row["num"].is_null());
    assert!(row["flag"].is_null());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_array_int() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_arr_int (id SERIAL PRIMARY KEY, vals INT4[])").await;
    ctx.execute("INSERT INTO dt_arr_int (vals) VALUES (ARRAY[1, 2, 3])", &[]).await;

    let row = ctx.query_one("SELECT vals FROM dt_arr_int", &[]).await;
    let vals = row["vals"].as_array().unwrap();
    assert_eq!(vals.len(), 3);
    assert_eq!(vals[0], 1);
    assert_eq!(vals[1], 2);
    assert_eq!(vals[2], 3);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_array_text() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_arr_text (id SERIAL PRIMARY KEY, tags TEXT[])").await;
    ctx.execute("INSERT INTO dt_arr_text (tags) VALUES (ARRAY['rust', 'postgres', 'eden'])", &[]).await;

    let row = ctx.query_one("SELECT tags FROM dt_arr_text", &[]).await;
    let tags = row["tags"].as_array().unwrap();
    assert_eq!(tags.len(), 3);
    assert_eq!(tags[0], "rust");
    assert_eq!(tags[2], "eden");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_inet() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dt_inet (id SERIAL PRIMARY KEY, addr INET)").await;
    ctx.execute("INSERT INTO dt_inet (addr) VALUES ('192.168.1.1'), ('::1')", &[]).await;

    let rows = ctx.query("SELECT addr FROM dt_inet ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["addr"], "192.168.1.1");
    assert_eq!(arr[1]["addr"], "::1");

    ctx.stop().await;
}
