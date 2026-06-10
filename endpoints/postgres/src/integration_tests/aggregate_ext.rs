use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_bool_and_or() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE agg_bool (id SERIAL PRIMARY KEY, grp TEXT, flag BOOLEAN)").await;
    ctx.execute(
        "INSERT INTO agg_bool (grp, flag) VALUES
         ('a', true), ('a', true), ('a', false),
         ('b', true), ('b', true)",
        &[],
    )
    .await;

    let rows = ctx
        .query(
            "SELECT grp, BOOL_AND(flag) AS all_true, BOOL_OR(flag) AS any_true
             FROM agg_bool GROUP BY grp ORDER BY grp",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["grp"], "a");
    assert_eq!(arr[0]["all_true"], false); // has one false
    assert_eq!(arr[0]["any_true"], true);
    assert_eq!(arr[1]["grp"], "b");
    assert_eq!(arr[1]["all_true"], true);
    assert_eq!(arr[1]["any_true"], true);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_every() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE agg_every (id SERIAL PRIMARY KEY, val INT4)").await;
    ctx.execute("INSERT INTO agg_every (val) VALUES (1), (2), (3), (4), (5)", &[]).await;

    let row = ctx.query_one("SELECT EVERY(val > 0) AS all_positive FROM agg_every", &[]).await;
    assert_eq!(row["all_positive"], true);

    let row = ctx.query_one("SELECT EVERY(val > 2) AS all_gt2 FROM agg_every", &[]).await;
    assert_eq!(row["all_gt2"], false);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_bit_and_or() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE agg_bit (id SERIAL PRIMARY KEY, val INT4)").await;
    ctx.execute(
        "INSERT INTO agg_bit (val) VALUES (12), (10), (14)", // 1100, 1010, 1110
        &[],
    )
    .await;

    let row = ctx.query_one("SELECT BIT_AND(val) AS band, BIT_OR(val) AS bor FROM agg_bit", &[]).await;
    assert_eq!(row["band"], 8); // 1000
    assert_eq!(row["bor"], 14); // 1110

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_count_filter() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE agg_filter (id SERIAL PRIMARY KEY, category TEXT, val INT4)").await;
    ctx.execute(
        "INSERT INTO agg_filter (category, val) VALUES
         ('a', 10), ('a', 20), ('b', 30), ('b', 40), ('a', 50)",
        &[],
    )
    .await;

    let row = ctx
        .query_one(
            "SELECT
                COUNT(*) FILTER (WHERE category = 'a')::INT4 AS count_a,
                SUM(val) FILTER (WHERE category = 'b')::INT4 AS sum_b,
                AVG(val::FLOAT8) FILTER (WHERE val > 20) AS avg_high
             FROM agg_filter",
            &[],
        )
        .await;
    assert_eq!(row["count_a"], 3);
    assert_eq!(row["sum_b"], 70);
    let avg = row["avg_high"].as_f64().unwrap();
    assert!((avg - 40.0).abs() < 0.01); // (30+40+50)/3

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_percentile_cont() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE agg_pctile (id SERIAL PRIMARY KEY, val INT4)").await;
    ctx.execute("INSERT INTO agg_pctile (val) VALUES (10), (20), (30), (40), (50)", &[]).await;

    let row = ctx
        .query_one(
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val)::FLOAT8 AS median
             FROM agg_pctile",
            &[],
        )
        .await;
    let median = row["median"].as_f64().unwrap();
    assert!((median - 30.0).abs() < 0.01);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_percentile_disc() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE agg_pdisc (id SERIAL PRIMARY KEY, val INT4)").await;
    ctx.execute("INSERT INTO agg_pdisc (val) VALUES (10), (20), (30), (40), (50)", &[]).await;

    let row = ctx
        .query_one(
            "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val)::INT4 AS median
             FROM agg_pdisc",
            &[],
        )
        .await;
    assert_eq!(row["median"], 30);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_mode() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE agg_mode (id SERIAL PRIMARY KEY, val INT4)").await;
    ctx.execute("INSERT INTO agg_mode (val) VALUES (1), (2), (2), (3), (3), (3), (4)", &[]).await;

    let row = ctx.query_one("SELECT MODE() WITHIN GROUP (ORDER BY val) AS mode_val FROM agg_mode", &[]).await;
    assert_eq!(row["mode_val"], 3);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_aggregate_distinct() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE agg_dist (id SERIAL PRIMARY KEY, grp TEXT, val INT4)").await;
    ctx.execute(
        "INSERT INTO agg_dist (grp, val) VALUES
         ('a', 1), ('a', 1), ('a', 2), ('b', 3), ('b', 3)",
        &[],
    )
    .await;

    let rows = ctx
        .query(
            "SELECT grp,
                COUNT(val)::INT4 AS total,
                COUNT(DISTINCT val)::INT4 AS distinct_count,
                SUM(val)::INT4 AS total_sum,
                SUM(DISTINCT val)::INT4 AS distinct_sum
             FROM agg_dist GROUP BY grp ORDER BY grp",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["total"], 3); // a: 3 rows
    assert_eq!(arr[0]["distinct_count"], 2); // a: 2 distinct (1, 2)
    assert_eq!(arr[0]["total_sum"], 4); // 1+1+2
    assert_eq!(arr[0]["distinct_sum"], 3); // 1+2
    assert_eq!(arr[1]["total"], 2);
    assert_eq!(arr[1]["distinct_count"], 1);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_aggregate_order_by() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE agg_ord (id SERIAL PRIMARY KEY, grp TEXT, val TEXT)").await;
    ctx.execute(
        "INSERT INTO agg_ord (grp, val) VALUES ('a', 'z'), ('a', 'x'), ('a', 'y'), ('b', 'm'), ('b', 'n')",
        &[],
    )
    .await;

    let rows = ctx
        .query(
            "SELECT grp, STRING_AGG(val, ',' ORDER BY val) AS ordered_vals
             FROM agg_ord GROUP BY grp ORDER BY grp",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["ordered_vals"], "x,y,z");
    assert_eq!(arr[1]["ordered_vals"], "m,n");

    ctx.stop().await;
}
