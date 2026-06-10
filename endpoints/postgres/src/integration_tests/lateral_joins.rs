use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

async fn setup_lateral_tables(ctx: &mut PostgresTestContext) {
    ctx.batch_execute(
        "CREATE TABLE lj_dept (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE lj_emp (
             id SERIAL PRIMARY KEY,
             name TEXT NOT NULL,
             dept_id INT4 REFERENCES lj_dept(id),
             salary INT4 NOT NULL
         )",
    )
    .await;
    ctx.execute("INSERT INTO lj_dept (name) VALUES ('Engineering'), ('Sales'), ('HR')", &[]).await;
    ctx.execute(
        "INSERT INTO lj_emp (name, dept_id, salary) VALUES
         ('alice', 1, 100), ('bob', 1, 120), ('carol', 2, 110),
         ('dave', 2, 90), ('eve', 1, 130)",
        &[],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_lateral_join_basic() {
    let mut ctx = PostgresTestContext::new().await;
    setup_lateral_tables(&mut ctx).await;

    // LATERAL join: for each dept, get top 2 earners
    let rows = ctx
        .query(
            "SELECT d.name AS dept, e.name AS emp, e.salary
             FROM lj_dept d
             JOIN LATERAL (
                 SELECT name, salary FROM lj_emp WHERE dept_id = d.id ORDER BY salary DESC LIMIT 2
             ) e ON true
             ORDER BY d.name, e.salary DESC",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // Engineering: eve(130), bob(120); Sales: carol(110), dave(90); HR: none
    assert_eq!(arr.len(), 4);
    assert_eq!(arr[0]["dept"], "Engineering");
    assert_eq!(arr[0]["emp"], "eve");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_lateral_left_join() {
    let mut ctx = PostgresTestContext::new().await;
    setup_lateral_tables(&mut ctx).await;

    // LEFT JOIN LATERAL: includes depts with no employees
    let rows = ctx
        .query(
            "SELECT d.name AS dept, e.name AS emp
             FROM lj_dept d
             LEFT JOIN LATERAL (
                 SELECT name FROM lj_emp WHERE dept_id = d.id ORDER BY salary DESC LIMIT 1
             ) e ON true
             ORDER BY d.name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3); // Engineering, HR, Sales
    // HR has no employees, so emp is null
    let hr = arr.iter().find(|r| r["dept"] == "HR").unwrap();
    assert!(hr["emp"].is_null());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_natural_join() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE nj_a (id INT4 PRIMARY KEY, name TEXT);
         CREATE TABLE nj_b (id INT4 PRIMARY KEY, name TEXT, extra TEXT)",
    )
    .await;
    ctx.execute("INSERT INTO nj_a VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')", &[]).await;
    ctx.execute("INSERT INTO nj_b VALUES (1, 'alice', 'x'), (2, 'bob', 'y'), (4, 'dave', 'z')", &[]).await;

    // NATURAL JOIN matches on all common column names (id, name)
    let rows = ctx.query("SELECT id, name, extra FROM nj_a NATURAL JOIN nj_b ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2); // id=1 name=alice, id=2 name=bob
    assert_eq!(arr[0]["name"], "alice");
    assert_eq!(arr[0]["extra"], "x");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_join_using() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE uj_orders (id SERIAL PRIMARY KEY, customer_id INT4, total INT4);
         CREATE TABLE uj_customers (customer_id INT4 PRIMARY KEY, name TEXT)",
    )
    .await;
    ctx.execute("INSERT INTO uj_customers VALUES (1, 'alice'), (2, 'bob')", &[]).await;
    ctx.execute("INSERT INTO uj_orders (customer_id, total) VALUES (1, 100), (1, 200), (2, 150)", &[]).await;

    // JOIN ... USING (column) instead of ON
    let rows = ctx.query("SELECT name, total FROM uj_orders JOIN uj_customers USING (customer_id) ORDER BY total", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["name"], "alice");
    assert_eq!(arr[0]["total"], 100);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_lateral_with_aggregate() {
    let mut ctx = PostgresTestContext::new().await;
    setup_lateral_tables(&mut ctx).await;

    // LATERAL with aggregate: dept stats
    let rows = ctx
        .query(
            "SELECT d.name AS dept, stats.cnt, stats.avg_salary
             FROM lj_dept d
             LEFT JOIN LATERAL (
                 SELECT
                     COUNT(*)::INT4 AS cnt,
                     AVG(salary)::FLOAT8 AS avg_salary
                 FROM lj_emp WHERE dept_id = d.id
             ) stats ON true
             ORDER BY d.name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);

    let eng = arr.iter().find(|r| r["dept"] == "Engineering").unwrap();
    assert_eq!(eng["cnt"], 3);

    let hr = arr.iter().find(|r| r["dept"] == "HR").unwrap();
    assert_eq!(hr["cnt"], 0);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cross_join_lateral_generate_series() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE lj_ranges (id SERIAL PRIMARY KEY, start_val INT4, end_val INT4)").await;
    ctx.execute("INSERT INTO lj_ranges (start_val, end_val) VALUES (1, 3), (10, 12)", &[]).await;

    let rows = ctx
        .query(
            "SELECT r.id, s.val
             FROM lj_ranges r
             CROSS JOIN LATERAL generate_series(r.start_val, r.end_val) AS s(val)
             ORDER BY r.id, s.val",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // id=1: 1,2,3; id=2: 10,11,12 → 6 rows
    assert_eq!(arr.len(), 6);
    assert_eq!(arr[0]["val"], 1);
    assert_eq!(arr[3]["val"], 10);

    ctx.stop().await;
}
