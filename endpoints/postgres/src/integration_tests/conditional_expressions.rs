use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_greatest_least() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                GREATEST(1, 5, 3, 7, 2) AS g,
                LEAST(1, 5, 3, 7, 2) AS l",
            &[],
        )
        .await;
    assert_eq!(row["g"], 7);
    assert_eq!(row["l"], 1);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_greatest_least_with_nulls() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                GREATEST(NULL, 5, NULL, 3) AS g,
                LEAST(NULL, 5, NULL, 3) AS l",
            &[],
        )
        .await;
    // GREATEST/LEAST ignore NULLs
    assert_eq!(row["g"], 5);
    assert_eq!(row["l"], 3);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_case_simple() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ce_case (id SERIAL PRIMARY KEY, status INT4)").await;
    ctx.execute("INSERT INTO ce_case (status) VALUES (1), (2), (3), (4)", &[]).await;

    let rows = ctx
        .query(
            "SELECT status,
                CASE status
                    WHEN 1 THEN 'active'
                    WHEN 2 THEN 'pending'
                    WHEN 3 THEN 'closed'
                    ELSE 'unknown'
                END AS label
             FROM ce_case ORDER BY id",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["label"], "active");
    assert_eq!(arr[1]["label"], "pending");
    assert_eq!(arr[2]["label"], "closed");
    assert_eq!(arr[3]["label"], "unknown");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_case_searched_with_params() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                CASE
                    WHEN $1::INT4 > 100 THEN 'high'
                    WHEN $1::INT4 > 50 THEN 'medium'
                    ELSE 'low'
                END AS category",
            &[SqlParam::Int4(75)],
        )
        .await;
    assert_eq!(row["category"], "medium");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_coalesce_chain() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT COALESCE(NULL, NULL, NULL, 'fallback', 'ignored') AS result", &[]).await;
    assert_eq!(row["result"], "fallback");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_nullif_in_division() {
    let mut ctx = PostgresTestContext::new().await;

    // NULLIF commonly used to avoid division by zero
    let row = ctx
        .query_one(
            "SELECT
                (10.0 / NULLIF(0, 0))::FLOAT8 AS div_by_zero,
                (10.0 / NULLIF(2, 0))::FLOAT8 AS normal_div",
            &[],
        )
        .await;
    assert!(row["div_by_zero"].is_null()); // 10/NULL = NULL
    let nd = row["normal_div"].as_f64().unwrap();
    assert!((nd - 5.0).abs() < 0.01);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_case_with_aggregates() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ce_pivot (id SERIAL PRIMARY KEY, product TEXT, quarter INT4, revenue INT4)").await;
    ctx.execute(
        "INSERT INTO ce_pivot (product, quarter, revenue) VALUES
         ('A', 1, 100), ('A', 2, 150), ('A', 3, 200),
         ('B', 1, 80), ('B', 2, 120), ('B', 3, 90)",
        &[],
    )
    .await;

    // Pivot using CASE + aggregation
    let rows = ctx
        .query(
            "SELECT product,
                SUM(CASE WHEN quarter = 1 THEN revenue ELSE 0 END)::INT4 AS q1,
                SUM(CASE WHEN quarter = 2 THEN revenue ELSE 0 END)::INT4 AS q2,
                SUM(CASE WHEN quarter = 3 THEN revenue ELSE 0 END)::INT4 AS q3
             FROM ce_pivot GROUP BY product ORDER BY product",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["product"], "A");
    assert_eq!(arr[0]["q1"], 100);
    assert_eq!(arr[0]["q2"], 150);
    assert_eq!(arr[0]["q3"], 200);
    assert_eq!(arr[1]["product"], "B");
    assert_eq!(arr[1]["q1"], 80);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_in_and_not_in() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ce_in (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO ce_in (val) VALUES ('a'), ('b'), ('c'), ('d'), ('e')", &[]).await;

    let rows = ctx.query("SELECT val FROM ce_in WHERE val IN ('b', 'd') ORDER BY val", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["val"], "b");
    assert_eq!(arr[1]["val"], "d");

    let rows = ctx.query("SELECT val FROM ce_in WHERE val NOT IN ('b', 'd') ORDER BY val", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);

    ctx.stop().await;
}
