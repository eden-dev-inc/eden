use crate::api::lib::query_typed::SqlParamType;
use crate::api::wrapper::input::{SqlParam, SqlType};
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_typed_bool() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE qt_bool (id SERIAL PRIMARY KEY, flag BOOLEAN)").await;
    ctx.execute("INSERT INTO qt_bool (flag) VALUES (true), (false)", &[]).await;

    let result = ctx.query_typed("SELECT flag FROM qt_bool WHERE flag = $1", &[SqlParamType(SqlParam::Bool(true), SqlType::Bool)]).await;
    assert_eq!(result["flag"], true);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_typed_int_types() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE qt_ints (id SERIAL PRIMARY KEY, s SMALLINT, i INTEGER, b BIGINT)").await;
    ctx.execute("INSERT INTO qt_ints (s, i, b) VALUES (1, 100, 9999999999)", &[]).await;

    // Query with typed Int2 param
    let result = ctx.query_typed("SELECT s, i, b FROM qt_ints WHERE s = $1", &[SqlParamType(SqlParam::Int2(1), SqlType::Int2)]).await;
    assert_eq!(result["s"], 1);
    assert_eq!(result["i"], 100);
    assert_eq!(result["b"], 9_999_999_999_i64);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_typed_float_types() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE qt_floats (id SERIAL PRIMARY KEY, r REAL, d DOUBLE PRECISION)").await;
    ctx.execute("INSERT INTO qt_floats (r, d) VALUES (3.141592653589793, 2.718281828)", &[]).await;

    let result = ctx
        .query_typed("SELECT r, d FROM qt_floats WHERE d > $1", &[SqlParamType(SqlParam::Float8(2.0), SqlType::Float8)])
        .await;
    let r = result["r"].as_f64().unwrap();
    assert!((r - std::f64::consts::PI).abs() < 0.01);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_typed_text_varchar() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE qt_str (id SERIAL PRIMARY KEY, txt TEXT, vc VARCHAR(50))").await;
    ctx.execute("INSERT INTO qt_str (txt, vc) VALUES ('hello', 'world')", &[]).await;

    // Query with Text type
    let result = ctx
        .query_typed(
            "SELECT txt, vc FROM qt_str WHERE txt = $1",
            &[SqlParamType(SqlParam::Text("hello".to_string()), SqlType::Text)],
        )
        .await;
    assert_eq!(result["txt"], "hello");
    assert_eq!(result["vc"], "world");

    // Query with Varchar type
    let result = ctx
        .query_typed(
            "SELECT txt, vc FROM qt_str WHERE vc = $1",
            &[SqlParamType(SqlParam::Text("world".to_string()), SqlType::Varchar)],
        )
        .await;
    assert_eq!(result["txt"], "hello");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_typed_json_jsonb() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE qt_json (id SERIAL PRIMARY KEY, jdata JSON, jbdata JSONB)").await;

    let json_val = serde_json::json!({"key": "value"});
    ctx.execute(
        "INSERT INTO qt_json (jdata, jbdata) VALUES ($1, $2)",
        &[SqlParam::Json(json_val.clone()), SqlParam::Json(json_val)],
    )
    .await;

    // Query with Json type
    let result = ctx
        .query_typed(
            "SELECT jdata, jbdata FROM qt_json WHERE jbdata @> $1",
            &[SqlParamType(SqlParam::Json(serde_json::json!({"key": "value"})), SqlType::Jsonb)],
        )
        .await;
    assert_eq!(result["jdata"]["key"], "value");
    assert_eq!(result["jbdata"]["key"], "value");

    ctx.stop().await;
}
