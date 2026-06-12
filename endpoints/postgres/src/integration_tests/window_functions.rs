use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

async fn setup_window_table(ctx: &mut PostgresTestContext) {
    ctx.batch_execute(
        "CREATE TABLE win_test (
            id SERIAL PRIMARY KEY,
            dept TEXT NOT NULL,
            name TEXT NOT NULL,
            salary INT4 NOT NULL
        )",
    )
    .await;
    ctx.execute(
        "INSERT INTO win_test (dept, name, salary) VALUES
         ('eng', 'alice', 100),
         ('eng', 'bob', 120),
         ('eng', 'carol', 90),
         ('sales', 'dave', 110),
         ('sales', 'eve', 130),
         ('hr', 'frank', 95)",
        &[],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_window_row_number() {
    let mut ctx = PostgresTestContext::new().await;
    setup_window_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name, dept, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
             FROM win_test ORDER BY rn",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["name"], "eve"); // salary 130 → rn=1
    assert_eq!(arr[0]["rn"], 1);
    assert_eq!(arr[5]["rn"], 6);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_window_rank_dense_rank() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE win_rank (id SERIAL PRIMARY KEY, score INT4)").await;
    ctx.execute("INSERT INTO win_rank (score) VALUES (100), (90), (90), (80)", &[]).await;

    let rows = ctx
        .query(
            "SELECT score,
                RANK() OVER (ORDER BY score DESC) AS rnk,
                DENSE_RANK() OVER (ORDER BY score DESC) AS drnk
             FROM win_rank ORDER BY score DESC, id",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["rnk"], 1); // 100: rank 1
    assert_eq!(arr[1]["rnk"], 2); // 90: rank 2
    assert_eq!(arr[2]["rnk"], 2); // 90: rank 2 (tie)
    assert_eq!(arr[3]["rnk"], 4); // 80: rank 4 (skips 3)

    assert_eq!(arr[0]["drnk"], 1);
    assert_eq!(arr[1]["drnk"], 2);
    assert_eq!(arr[2]["drnk"], 2);
    assert_eq!(arr[3]["drnk"], 3); // dense_rank doesn't skip

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_window_lag_lead() {
    let mut ctx = PostgresTestContext::new().await;
    setup_window_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name, salary,
                LAG(salary) OVER (ORDER BY salary) AS prev_salary,
                LEAD(salary) OVER (ORDER BY salary) AS next_salary
             FROM win_test ORDER BY salary",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // First row has no previous
    assert!(arr[0]["prev_salary"].is_null());
    // Last row has no next
    assert!(arr[5]["next_salary"].is_null());
    // Middle rows have both
    assert!(!arr[2]["prev_salary"].is_null());
    assert!(!arr[2]["next_salary"].is_null());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_window_ntile() {
    let mut ctx = PostgresTestContext::new().await;
    setup_window_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name, NTILE(3) OVER (ORDER BY salary) AS bucket
             FROM win_test ORDER BY salary",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // 6 rows into 3 buckets: [1,1,2,2,3,3]
    assert_eq!(arr[0]["bucket"], 1);
    assert_eq!(arr[1]["bucket"], 1);
    assert_eq!(arr[2]["bucket"], 2);
    assert_eq!(arr[3]["bucket"], 2);
    assert_eq!(arr[4]["bucket"], 3);
    assert_eq!(arr[5]["bucket"], 3);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_window_first_last_value() {
    let mut ctx = PostgresTestContext::new().await;
    setup_window_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name,
                FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary) AS lowest_paid,
                LAST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS highest_paid
             FROM win_test ORDER BY dept, salary",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // eng: carol(90), alice(100), bob(120)
    assert_eq!(arr[0]["lowest_paid"], "carol");
    assert_eq!(arr[0]["highest_paid"], "bob");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_window_partition_running_total() {
    let mut ctx = PostgresTestContext::new().await;
    setup_window_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name, dept, salary,
                SUM(salary) OVER (PARTITION BY dept ORDER BY salary) AS running_total
             FROM win_test ORDER BY dept, salary",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // eng: carol(90)→90, alice(100)→190, bob(120)→310
    let eng: Vec<&serde_json::Value> = arr.iter().filter(|r| r["dept"] == "eng").collect();
    assert_eq!(eng[0]["running_total"], 90);
    assert_eq!(eng[1]["running_total"], 190);
    assert_eq!(eng[2]["running_total"], 310);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_window_percent_rank_cume_dist() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE win_pct (id SERIAL PRIMARY KEY, val INT4)").await;
    ctx.execute("INSERT INTO win_pct (val) VALUES (10), (20), (30), (40), (50)", &[]).await;

    let rows = ctx
        .query(
            "SELECT val,
                PERCENT_RANK() OVER (ORDER BY val)::FLOAT8 AS pct_rank,
                CUME_DIST() OVER (ORDER BY val)::FLOAT8 AS cume
             FROM win_pct ORDER BY val",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // percent_rank: (rank - 1) / (total - 1)
    let pr0 = arr[0]["pct_rank"].as_f64().unwrap();
    assert!((pr0 - 0.0).abs() < 0.01);
    let pr4 = arr[4]["pct_rank"].as_f64().unwrap();
    assert!((pr4 - 1.0).abs() < 0.01);

    // cume_dist: rank / total
    let cd0 = arr[0]["cume"].as_f64().unwrap();
    assert!((cd0 - 0.2).abs() < 0.01);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_window_nth_value() {
    let mut ctx = PostgresTestContext::new().await;
    setup_window_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name, salary,
                NTH_VALUE(name, 2) OVER (
                    ORDER BY salary
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS second_lowest
             FROM win_test ORDER BY salary",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // Second lowest salary is frank(95)
    assert_eq!(arr[0]["second_lowest"], "frank");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_window_frame_rows_range() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE win_frame (id SERIAL PRIMARY KEY, val INT4)").await;
    ctx.execute("INSERT INTO win_frame (val) VALUES (1), (2), (3), (4), (5)", &[]).await;

    // Moving average over 3 rows (current + 1 preceding + 1 following)
    let rows = ctx
        .query(
            "SELECT val,
                AVG(val::FLOAT8) OVER (
                    ORDER BY id
                    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                ) AS moving_avg
             FROM win_frame ORDER BY id",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // id=1: avg(1,2) = 1.5
    let ma0 = arr[0]["moving_avg"].as_f64().unwrap();
    assert!((ma0 - 1.5).abs() < 0.01);
    // id=3: avg(2,3,4) = 3.0
    let ma2 = arr[2]["moving_avg"].as_f64().unwrap();
    assert!((ma2 - 3.0).abs() < 0.01);
    // id=5: avg(4,5) = 4.5
    let ma4 = arr[4]["moving_avg"].as_f64().unwrap();
    assert!((ma4 - 4.5).abs() < 0.01);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_window_count_over() {
    let mut ctx = PostgresTestContext::new().await;
    setup_window_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name, dept,
                COUNT(*) OVER (PARTITION BY dept) AS dept_size
             FROM win_test ORDER BY dept, name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // eng has 3, hr has 1, sales has 2
    let eng: Vec<&serde_json::Value> = arr.iter().filter(|r| r["dept"] == "eng").collect();
    assert_eq!(eng[0]["dept_size"], 3);
    let hr: Vec<&serde_json::Value> = arr.iter().filter(|r| r["dept"] == "hr").collect();
    assert_eq!(hr[0]["dept_size"], 1);

    ctx.stop().await;
}
