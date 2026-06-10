use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_batch_execute_multiple_creates() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE be_table_a (id SERIAL PRIMARY KEY, name TEXT);
         CREATE TABLE be_table_b (id SERIAL PRIMARY KEY, val INT4)",
    )
    .await;

    // Both tables should exist
    let result_a = ctx.query("SELECT * FROM be_table_a", &[]).await;
    assert_eq!(result_a, serde_json::Value::Null); // empty table

    let result_b = ctx.query("SELECT * FROM be_table_b", &[]).await;
    assert_eq!(result_b, serde_json::Value::Null); // empty table

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_batch_execute_create_and_insert() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE be_mixed (id SERIAL PRIMARY KEY, name TEXT);
         INSERT INTO be_mixed (name) VALUES ('alice');
         INSERT INTO be_mixed (name) VALUES ('bob')",
    )
    .await;

    let rows = ctx.query("SELECT name FROM be_mixed ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["name"], "alice");
    assert_eq!(arr[1]["name"], "bob");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_batch_execute_returns_success() {
    let mut ctx = PostgresTestContext::new().await;

    let result = ctx
        .batch_execute(
            "CREATE TABLE be_success (id SERIAL PRIMARY KEY);
             CREATE INDEX idx_be_success ON be_success (id)",
        )
        .await;
    // batch_execute returns "success" via EmptyOutput
    assert_eq!(result, serde_json::json!("success"));

    ctx.stop().await;
}
