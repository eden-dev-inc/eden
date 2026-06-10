use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

async fn setup_agg_table(ctx: &mut PostgresTestContext) {
    ctx.batch_execute(
        "CREATE TABLE agg_test (
            id SERIAL PRIMARY KEY,
            department TEXT NOT NULL,
            salary INT4 NOT NULL,
            name TEXT
        )",
    )
    .await;
    ctx.execute(
        "INSERT INTO agg_test (department, salary, name) VALUES
         ('eng', 100, 'alice'),
         ('eng', 200, 'bob'),
         ('eng', 150, 'carol'),
         ('sales', 300, 'dave'),
         ('sales', 250, 'eve'),
         ('hr', 180, NULL)",
        &[],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_count_star() {
    let mut ctx = PostgresTestContext::new().await;
    setup_agg_table(&mut ctx).await;

    let row = ctx.query_one("SELECT COUNT(*) AS total FROM agg_test", &[]).await;
    assert_eq!(row["total"], 6);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_count_column() {
    let mut ctx = PostgresTestContext::new().await;
    setup_agg_table(&mut ctx).await;

    // COUNT(name) excludes NULLs
    let row = ctx.query_one("SELECT COUNT(name) AS named FROM agg_test", &[]).await;
    assert_eq!(row["named"], 5);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_sum() {
    let mut ctx = PostgresTestContext::new().await;
    setup_agg_table(&mut ctx).await;

    let row = ctx.query_one("SELECT SUM(salary) AS total_salary FROM agg_test", &[]).await;
    assert_eq!(row["total_salary"], 1180);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_avg() {
    let mut ctx = PostgresTestContext::new().await;
    setup_agg_table(&mut ctx).await;

    let row = ctx
        .query_one(
            "SELECT AVG(salary)::FLOAT8 AS avg_salary FROM agg_test WHERE department = $1",
            &[SqlParam::Text("eng".to_string())],
        )
        .await;
    // (100+200+150)/3 = 150.0
    let avg = row["avg_salary"].as_f64().unwrap();
    assert!((avg - 150.0).abs() < 0.01);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_min_max() {
    let mut ctx = PostgresTestContext::new().await;
    setup_agg_table(&mut ctx).await;

    let row = ctx.query_one("SELECT MIN(salary) AS min_sal, MAX(salary) AS max_sal FROM agg_test", &[]).await;
    assert_eq!(row["min_sal"], 100);
    assert_eq!(row["max_sal"], 300);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_group_by() {
    let mut ctx = PostgresTestContext::new().await;
    setup_agg_table(&mut ctx).await;

    let rows = ctx.query("SELECT department, COUNT(*) AS cnt FROM agg_test GROUP BY department ORDER BY department", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["department"], "eng");
    assert_eq!(arr[0]["cnt"], 3);
    assert_eq!(arr[1]["department"], "hr");
    assert_eq!(arr[1]["cnt"], 1);
    assert_eq!(arr[2]["department"], "sales");
    assert_eq!(arr[2]["cnt"], 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_group_by_having() {
    let mut ctx = PostgresTestContext::new().await;
    setup_agg_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT department, COUNT(*) AS cnt FROM agg_test GROUP BY department HAVING COUNT(*) > 1 ORDER BY department",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["department"], "eng");
    assert_eq!(arr[1]["department"], "sales");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_count_distinct() {
    let mut ctx = PostgresTestContext::new().await;
    setup_agg_table(&mut ctx).await;

    let row = ctx.query_one("SELECT COUNT(DISTINCT department) AS dept_count FROM agg_test", &[]).await;
    assert_eq!(row["dept_count"], 3);

    ctx.stop().await;
}
