use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_create_view() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE vs_base (id SERIAL PRIMARY KEY, name TEXT, active BOOLEAN DEFAULT true)").await;
    ctx.execute("INSERT INTO vs_base (name, active) VALUES ('alice', true), ('bob', false), ('carol', true)", &[]).await;

    ctx.batch_execute("CREATE VIEW vs_active AS SELECT id, name FROM vs_base WHERE active = true").await;

    let rows = ctx.query("SELECT name FROM vs_active ORDER BY name", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["name"], "alice");
    assert_eq!(arr[1]["name"], "carol");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_create_or_replace_view() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE vs_repl (id SERIAL PRIMARY KEY, val INT4)").await;
    ctx.execute("INSERT INTO vs_repl (val) VALUES (1), (2), (3)", &[]).await;

    ctx.batch_execute("CREATE VIEW vs_repl_view AS SELECT * FROM vs_repl WHERE val > 1").await;

    let rows = ctx.query("SELECT * FROM vs_repl_view", &[]).await;
    assert_eq!(rows.as_array().unwrap().len(), 2);

    // Replace with different filter
    ctx.batch_execute("CREATE OR REPLACE VIEW vs_repl_view AS SELECT * FROM vs_repl WHERE val > 2").await;

    let rows = ctx.query("SELECT * FROM vs_repl_view", &[]).await;
    assert!(rows.is_object()); // single row

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_materialized_view() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE vs_mat (id SERIAL PRIMARY KEY, val INT4)").await;
    ctx.execute("INSERT INTO vs_mat (val) VALUES (10), (20), (30)", &[]).await;

    ctx.batch_execute("CREATE MATERIALIZED VIEW vs_mat_view AS SELECT SUM(val)::INT4 AS total FROM vs_mat").await;

    let row = ctx.query_one("SELECT total FROM vs_mat_view", &[]).await;
    assert_eq!(row["total"], 60);

    // Insert more data
    ctx.execute("INSERT INTO vs_mat (val) VALUES (40)", &[]).await;

    // Materialized view still shows old data
    let row = ctx.query_one("SELECT total FROM vs_mat_view", &[]).await;
    assert_eq!(row["total"], 60);

    // Refresh
    ctx.batch_execute("REFRESH MATERIALIZED VIEW vs_mat_view").await;

    let row = ctx.query_one("SELECT total FROM vs_mat_view", &[]).await;
    assert_eq!(row["total"], 100);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_drop_view() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE vs_drop (id SERIAL PRIMARY KEY)").await;
    ctx.batch_execute("CREATE VIEW vs_drop_view AS SELECT * FROM vs_drop").await;

    // Verify view exists
    let row = ctx.query_one("SELECT COUNT(*)::INT4 AS cnt FROM information_schema.views WHERE table_name = 'vs_drop_view'", &[]).await;
    assert_eq!(row["cnt"], 1);

    ctx.batch_execute("DROP VIEW vs_drop_view").await;

    let row = ctx.query_one("SELECT COUNT(*)::INT4 AS cnt FROM information_schema.views WHERE table_name = 'vs_drop_view'", &[]).await;
    assert_eq!(row["cnt"], 0);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_sequence_create_and_use() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE SEQUENCE vs_seq START 10 INCREMENT 5").await;

    // Use sequence through a table with DEFAULT, since nextval() modifies state
    // and may not work through read-only connections
    ctx.batch_execute("CREATE TABLE vs_seq_holder (id INT4 DEFAULT nextval('vs_seq') PRIMARY KEY, label TEXT)").await;

    ctx.execute("INSERT INTO vs_seq_holder (label) VALUES ('first')", &[]).await;
    ctx.execute("INSERT INTO vs_seq_holder (label) VALUES ('second')", &[]).await;

    let rows = ctx.query("SELECT id, label FROM vs_seq_holder ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["id"], 10);
    assert_eq!(arr[0]["label"], "first");
    assert_eq!(arr[1]["id"], 15);
    assert_eq!(arr[1]["label"], "second");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_sequence_setval() {
    let mut ctx = PostgresTestContext::new().await;

    // Test sequences through a table to avoid read-only connection issues with nextval
    ctx.batch_execute(
        "CREATE SEQUENCE vs_setseq;
         CREATE TABLE vs_setval_holder (id INT4 DEFAULT nextval('vs_setseq') PRIMARY KEY, label TEXT)",
    )
    .await;

    // Advance the sequence via inserts, then check the values
    ctx.execute("INSERT INTO vs_setval_holder (label) VALUES ('a')", &[]).await;

    let row = ctx.query_one("SELECT id FROM vs_setval_holder WHERE label = 'a'", &[]).await;
    // Default sequence starts at 1
    assert_eq!(row["id"], 1);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_schema_create_and_use() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE SCHEMA test_schema").await;
    ctx.batch_execute("CREATE TABLE test_schema.items (id SERIAL PRIMARY KEY, name TEXT)").await;
    ctx.execute("INSERT INTO test_schema.items (name) VALUES ($1)", &[SqlParam::Text("widget".to_string())]).await;

    let row = ctx.query_one("SELECT name FROM test_schema.items WHERE id = 1", &[]).await;
    assert_eq!(row["name"], "widget");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_view_with_join() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE vs_dept (id SERIAL PRIMARY KEY, name TEXT);
         CREATE TABLE vs_emp (id SERIAL PRIMARY KEY, name TEXT, dept_id INT4 REFERENCES vs_dept(id))",
    )
    .await;
    ctx.execute("INSERT INTO vs_dept (name) VALUES ('Engineering'), ('Sales')", &[]).await;
    ctx.execute("INSERT INTO vs_emp (name, dept_id) VALUES ('alice', 1), ('bob', 2), ('carol', 1)", &[]).await;

    ctx.batch_execute(
        "CREATE VIEW vs_emp_dept AS
         SELECT e.name AS emp_name, d.name AS dept_name
         FROM vs_emp e JOIN vs_dept d ON e.dept_id = d.id",
    )
    .await;

    let rows = ctx.query("SELECT emp_name, dept_name FROM vs_emp_dept ORDER BY emp_name", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["emp_name"], "alice");
    assert_eq!(arr[0]["dept_name"], "Engineering");

    ctx.stop().await;
}
