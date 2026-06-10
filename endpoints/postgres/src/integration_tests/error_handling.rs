use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_syntax_error() {
    let mut ctx = PostgresTestContext::new().await;

    let err = ctx.query_err("SELEC * FORM nothing", &[]).await;
    let err_msg = format!("{err}");
    // Should contain postgres error about syntax
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_table_not_found() {
    let mut ctx = PostgresTestContext::new().await;

    let err = ctx.query_err("SELECT * FROM nonexistent_table", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_column_not_found() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE err_col (id SERIAL PRIMARY KEY, name TEXT)").await;

    let err = ctx.query_err("SELECT nonexistent_column FROM err_col", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_unique_constraint_violation() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE err_unique (id INT PRIMARY KEY, name TEXT)").await;
    ctx.execute(
        "INSERT INTO err_unique (id, name) VALUES ($1, $2)",
        &[SqlParam::Int4(1), SqlParam::Text("first".to_string())],
    )
    .await;

    let err = ctx
        .execute_err(
            "INSERT INTO err_unique (id, name) VALUES ($1, $2)",
            &[SqlParam::Int4(1), SqlParam::Text("duplicate".to_string())],
        )
        .await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_not_null_constraint_violation() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE err_notnull (id SERIAL PRIMARY KEY, name TEXT NOT NULL)").await;

    let err = ctx.execute_err("INSERT INTO err_notnull (name) VALUES ($1)", &[SqlParam::Null]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_check_constraint_violation() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE err_check (id SERIAL PRIMARY KEY, age INT4 CHECK (age >= 0))").await;

    let err = ctx.execute_err("INSERT INTO err_check (age) VALUES ($1)", &[SqlParam::Int4(-1)]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_foreign_key_violation() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE err_parent (id INT PRIMARY KEY);
         CREATE TABLE err_child (id SERIAL PRIMARY KEY, parent_id INT REFERENCES err_parent(id))",
    )
    .await;

    let err = ctx.execute_err("INSERT INTO err_child (parent_id) VALUES ($1)", &[SqlParam::Int4(999)]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_type_mismatch() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE err_type (id SERIAL PRIMARY KEY, val INT4)").await;

    // Passing a text value where INT4 is expected
    let err = ctx.execute_err("INSERT INTO err_type (val) VALUES ($1)", &[SqlParam::Text("not_a_number".to_string())]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_division_by_zero() {
    let mut ctx = PostgresTestContext::new().await;

    let err = ctx.query_err("SELECT 1 / 0 AS result", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_invalid_parameter_count() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE err_params (id SERIAL PRIMARY KEY, a TEXT, b TEXT)").await;

    // Query expects 2 params but we provide 1
    let err = ctx.execute_err("INSERT INTO err_params (a, b) VALUES ($1, $2)", &[SqlParam::Text("only_one".to_string())]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}
