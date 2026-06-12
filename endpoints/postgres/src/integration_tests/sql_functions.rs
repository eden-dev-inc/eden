use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_string_functions() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                LENGTH($1) AS len,
                UPPER($1) AS upper_val,
                LOWER('HELLO') AS lower_val,
                TRIM('  space  ') AS trimmed,
                CONCAT($1, ' ', 'world') AS concatenated",
            &[SqlParam::Text("hello".to_string())],
        )
        .await;
    assert_eq!(row["len"], 5);
    assert_eq!(row["upper_val"], "HELLO");
    assert_eq!(row["lower_val"], "hello");
    assert_eq!(row["trimmed"], "space");
    assert_eq!(row["concatenated"], "hello world");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_string_substring() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT SUBSTRING($1 FROM 1 FOR 5) AS sub", &[SqlParam::Text("Hello World".to_string())]).await;
    assert_eq!(row["sub"], "Hello");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_string_position_replace() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                POSITION('world' IN 'hello world') AS pos,
                REPLACE('hello world', 'world', 'rust') AS replaced",
            &[],
        )
        .await;
    assert_eq!(row["pos"], 7);
    assert_eq!(row["replaced"], "hello rust");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_math_functions() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                ABS($1::INT4) AS abs_val,
                CEIL(4.2)::FLOAT8 AS ceil_val,
                FLOOR(4.8)::FLOAT8 AS floor_val,
                ROUND(4.567, 2)::FLOAT8 AS round_val,
                MOD(17, 5) AS mod_val,
                POWER(2, 10)::INT4 AS pow_val",
            &[SqlParam::Int4(-42)],
        )
        .await;
    assert_eq!(row["abs_val"], 42);
    let ceil = row["ceil_val"].as_f64().unwrap();
    assert!((ceil - 5.0).abs() < 0.01);
    let floor = row["floor_val"].as_f64().unwrap();
    assert!((floor - 4.0).abs() < 0.01);
    let round = row["round_val"].as_f64().unwrap();
    assert!((round - 4.57).abs() < 0.01);
    assert_eq!(row["mod_val"], 2);
    assert_eq!(row["pow_val"], 1024);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_math_random_and_generate_series() {
    let mut ctx = PostgresTestContext::new().await;

    // RANDOM() returns a value between 0 and 1
    let row = ctx.query_one("SELECT RANDOM() AS rnd", &[]).await;
    let rnd = row["rnd"].as_f64().unwrap();
    assert!((0.0..1.0).contains(&rnd));

    // generate_series produces rows
    let rows = ctx.query("SELECT * FROM generate_series(1, 5) AS s", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 5);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_date_functions() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                CURRENT_DATE IS NOT NULL AS has_date,
                CURRENT_TIMESTAMP IS NOT NULL AS has_ts",
            &[],
        )
        .await;
    assert_eq!(row["has_date"], true);
    assert_eq!(row["has_ts"], true);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_date_extract() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                EXTRACT(YEAR FROM DATE '2024-06-15')::INT4 AS yr,
                EXTRACT(MONTH FROM DATE '2024-06-15')::INT4 AS mo,
                EXTRACT(DAY FROM DATE '2024-06-15')::INT4 AS dy",
            &[],
        )
        .await;
    assert_eq!(row["yr"], 2024);
    assert_eq!(row["mo"], 6);
    assert_eq!(row["dy"], 15);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_date_interval() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT (DATE '2024-01-01' + INTERVAL '30 days')::DATE AS future_date", &[]).await;
    assert_eq!(row["future_date"], "2024-01-31");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_coalesce() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE fn_coalesce (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO fn_coalesce (val) VALUES (NULL), ('present')", &[]).await;

    let rows = ctx.query("SELECT COALESCE(val, 'default') AS result FROM fn_coalesce ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["result"], "default");
    assert_eq!(arr[1]["result"], "present");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_nullif() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT NULLIF($1::INT4, $2::INT4) AS result", &[SqlParam::Int4(5), SqlParam::Int4(5)]).await;
    assert!(row["result"].is_null());

    let row = ctx.query_one("SELECT NULLIF($1::INT4, $2::INT4) AS result", &[SqlParam::Int4(5), SqlParam::Int4(3)]).await;
    assert_eq!(row["result"], 5);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_case_when() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE fn_case (id SERIAL PRIMARY KEY, score INT4)").await;
    ctx.execute("INSERT INTO fn_case (score) VALUES (90), (75), (50), (30)", &[]).await;

    let rows = ctx
        .query(
            "SELECT score,
                CASE
                    WHEN score >= 80 THEN 'A'
                    WHEN score >= 60 THEN 'B'
                    WHEN score >= 40 THEN 'C'
                    ELSE 'F'
                END AS grade
             FROM fn_case ORDER BY id",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["grade"], "A");
    assert_eq!(arr[1]["grade"], "B");
    assert_eq!(arr[2]["grade"], "C");
    assert_eq!(arr[3]["grade"], "F");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cast() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                CAST('42' AS INT4) AS casted,
                '3.14'::FLOAT8 AS float_cast,
                42::TEXT AS text_cast",
            &[],
        )
        .await;
    assert_eq!(row["casted"], 42);
    let fc = row["float_cast"].as_f64().unwrap();
    assert!((fc - std::f64::consts::PI).abs() < 0.001);
    assert_eq!(row["text_cast"], "42");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_string_agg() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE fn_stragg (id SERIAL PRIMARY KEY, grp TEXT, val TEXT)").await;
    ctx.execute("INSERT INTO fn_stragg (grp, val) VALUES ('a', 'x'), ('a', 'y'), ('b', 'z')", &[]).await;

    let rows = ctx.query("SELECT grp, STRING_AGG(val, ',' ORDER BY val) AS agg FROM fn_stragg GROUP BY grp ORDER BY grp", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["grp"], "a");
    assert_eq!(arr[0]["agg"], "x,y");
    assert_eq!(arr[1]["grp"], "b");
    assert_eq!(arr[1]["agg"], "z");

    ctx.stop().await;
}
