use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_implicit_int_to_float() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT (5 / 2)::INT4 AS int_div, (5.0 / 2)::FLOAT8 AS float_div", &[]).await;
    assert_eq!(row["int_div"], 2); // integer division
    let fd = row["float_div"].as_f64().unwrap();
    assert!((fd - 2.5).abs() < 0.01);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_text_to_int_cast() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT '42'::INT4 AS num, 42::TEXT AS txt", &[]).await;
    assert_eq!(row["num"], 42);
    assert_eq!(row["txt"], "42");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_bool_to_int_cast() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT true::INT4 AS t_int, false::INT4 AS f_int", &[]).await;
    assert_eq!(row["t_int"], 1);
    assert_eq!(row["f_int"], 0);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_int_widening() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                $1::INT2 AS i2,
                ($1::INT2)::INT4 AS i2_to_i4,
                ($1::INT2)::INT8 AS i2_to_i8",
            &[SqlParam::Int2(100)],
        )
        .await;
    assert_eq!(row["i2"], 100);
    assert_eq!(row["i2_to_i4"], 100);
    assert_eq!(row["i2_to_i8"], 100);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_overflow_cast_fails() {
    let mut ctx = PostgresTestContext::new().await;

    // INT4 max is 2147483647; casting larger INT8 to INT4 should fail
    let err = ctx.query_one_err("SELECT ($1::INT8)::INT4 AS val", &[SqlParam::Int8(3_000_000_000)]).await;
    let err_str = format!("{:?}", err);
    assert!(err_str.contains("error") || err_str.contains("Error") || err_str.contains("overflow"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_text_to_date_cast() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT '2024-06-15'::DATE::TEXT AS d, '14:30:00'::TIME::TEXT AS t", &[]).await;
    assert_eq!(row["d"], "2024-06-15");
    assert_eq!(row["t"], "14:30:00");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_text_to_json_cast() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT '{\"key\": \"value\"}'::JSONB AS obj", &[]).await;
    // JSONB is deserialized as a JSON object
    let obj = &row["obj"];
    assert_eq!(obj["key"], "value");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_cast() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT ARRAY[1, 2, 3]::TEXT[] AS text_arr, '{a,b,c}'::TEXT[] AS parsed_arr", &[]).await;
    let text_arr = row["text_arr"].as_array().unwrap();
    assert_eq!(text_arr.len(), 3);
    assert_eq!(text_arr[0], "1");
    assert_eq!(text_arr[2], "3");

    let parsed = row["parsed_arr"].as_array().unwrap();
    assert_eq!(parsed.len(), 3);
    assert_eq!(parsed[0], "a");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_null_safe_comparison() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE tc_null (id INT4, val TEXT)").await;
    ctx.execute("INSERT INTO tc_null VALUES (1, 'a'), (2, NULL), (3, 'b'), (4, NULL)", &[]).await;

    // IS DISTINCT FROM is null-safe inequality
    let rows = ctx
        .query(
            "SELECT id FROM tc_null WHERE val IS DISTINCT FROM $1 ORDER BY id",
            &[SqlParam::Text("a".to_string())],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3); // 2(NULL), 3('b'), 4(NULL) — NULL IS DISTINCT FROM 'a' is true

    // IS NOT DISTINCT FROM is null-safe equality
    let rows = ctx.query("SELECT id FROM tc_null WHERE val IS NOT DISTINCT FROM NULL ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2); // 2, 4

    ctx.stop().await;
}
