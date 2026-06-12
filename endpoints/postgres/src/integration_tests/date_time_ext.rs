use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_date_trunc() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                DATE_TRUNC('month', TIMESTAMP '2024-06-15 14:30:45')::TEXT AS truncated_month,
                DATE_TRUNC('year', TIMESTAMP '2024-06-15 14:30:45')::TEXT AS truncated_year,
                DATE_TRUNC('hour', TIMESTAMP '2024-06-15 14:30:45')::TEXT AS truncated_hour",
            &[],
        )
        .await;
    assert_eq!(row["truncated_month"], "2024-06-01 00:00:00");
    assert_eq!(row["truncated_year"], "2024-01-01 00:00:00");
    assert_eq!(row["truncated_hour"], "2024-06-15 14:00:00");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_age_function() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT AGE(DATE '2024-06-15', DATE '2024-01-01')::TEXT AS diff", &[]).await;
    assert_eq!(row["diff"], "5 mons 14 days");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_to_char_timestamp() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT TO_CHAR(TIMESTAMP '2024-06-15 14:30:00', 'YYYY/MM/DD HH24:MI') AS formatted", &[]).await;
    assert_eq!(row["formatted"], "2024/06/15 14:30");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_to_char_number() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT TO_CHAR(1234567.89::FLOAT8, 'FM9,999,999.99') AS formatted", &[]).await;
    assert_eq!(row["formatted"], "1,234,567.89");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_to_date() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT TO_DATE('2024/06/15', 'YYYY/MM/DD')::TEXT AS parsed", &[]).await;
    assert_eq!(row["parsed"], "2024-06-15");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_to_timestamp() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT TO_TIMESTAMP('2024-06-15 14:30:00', 'YYYY-MM-DD HH24:MI:SS')::TEXT AS parsed", &[]).await;
    // to_timestamp returns timestamptz
    let parsed = row["parsed"].as_str().unwrap();
    assert!(parsed.starts_with("2024-06-15 14:30:00"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_now_and_clock_timestamp() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                NOW() IS NOT NULL AS has_now,
                CLOCK_TIMESTAMP() IS NOT NULL AS has_clock",
            &[],
        )
        .await;
    assert_eq!(row["has_now"], true);
    assert_eq!(row["has_clock"], true);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_date_arithmetic() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                (DATE '2024-01-31' + INTERVAL '1 month')::DATE::TEXT AS next_month,
                (DATE '2024-03-01' - INTERVAL '1 day')::DATE::TEXT AS prev_day,
                (DATE '2024-06-15' - DATE '2024-01-01') AS days_diff",
            &[],
        )
        .await;
    assert_eq!(row["next_month"], "2024-02-29"); // 2024 is a leap year
    assert_eq!(row["prev_day"], "2024-02-29");
    assert_eq!(row["days_diff"], 166);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_extract_epoch() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT EXTRACT(EPOCH FROM TIMESTAMP '2024-01-01 00:00:00')::FLOAT8 AS epoch", &[]).await;
    let epoch = row["epoch"].as_f64().unwrap();
    assert!(epoch > 1_700_000_000.0);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_make_date_make_time() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                MAKE_DATE(2024, 6, 15)::TEXT AS d,
                MAKE_TIME(14, 30, 0)::TEXT AS t",
            &[],
        )
        .await;
    assert_eq!(row["d"], "2024-06-15");
    assert_eq!(row["t"], "14:30:00");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_date_part() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                DATE_PART('dow', DATE '2024-06-15')::INT4 AS dow,
                DATE_PART('week', DATE '2024-06-15')::INT4 AS week,
                DATE_PART('quarter', DATE '2024-06-15')::INT4 AS quarter",
            &[],
        )
        .await;
    assert_eq!(row["dow"], 6); // Saturday
    assert_eq!(row["quarter"], 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_generate_series_timestamp() {
    let mut ctx = PostgresTestContext::new().await;

    let rows = ctx
        .query(
            "SELECT d::DATE::TEXT AS day FROM generate_series(
                DATE '2024-01-01',
                DATE '2024-01-05',
                INTERVAL '1 day'
            ) AS d",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 5);
    assert_eq!(arr[0]["day"], "2024-01-01");
    assert_eq!(arr[4]["day"], "2024-01-05");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_interval_arithmetic() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                (INTERVAL '1 hour' + INTERVAL '30 minutes')::TEXT AS combined,
                (INTERVAL '2 hours' * 3)::TEXT AS multiplied",
            &[],
        )
        .await;
    assert_eq!(row["combined"], "01:30:00");
    assert_eq!(row["multiplied"], "06:00:00");

    ctx.stop().await;
}
