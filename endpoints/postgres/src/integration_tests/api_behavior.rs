use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

async fn setup_api_table(ctx: &mut PostgresTestContext) {
    ctx.batch_execute("CREATE TABLE api_test (id SERIAL PRIMARY KEY, name TEXT NOT NULL, value INT4)").await;
    ctx.execute("INSERT INTO api_test (name, value) VALUES ('alice', 10), ('bob', 20), ('carol', 30)", &[]).await;
}

// --- query output shape tests ---

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_returns_null_for_zero_rows() {
    let mut ctx = PostgresTestContext::new().await;
    setup_api_table(&mut ctx).await;

    let result = ctx.query("SELECT * FROM api_test WHERE name = $1", &[SqlParam::Text("nonexistent".to_string())]).await;
    assert_eq!(result, serde_json::Value::Null);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_returns_object_for_one_row() {
    let mut ctx = PostgresTestContext::new().await;
    setup_api_table(&mut ctx).await;

    let result = ctx.query("SELECT name, value FROM api_test WHERE name = $1", &[SqlParam::Text("alice".to_string())]).await;
    // Single row returned as JSON object, not array
    assert!(result.is_object());
    assert_eq!(result["name"], "alice");
    assert_eq!(result["value"], 10);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_returns_array_for_multiple_rows() {
    let mut ctx = PostgresTestContext::new().await;
    setup_api_table(&mut ctx).await;

    let result = ctx.query("SELECT name, value FROM api_test ORDER BY id", &[]).await;
    assert!(result.is_array());
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["name"], "alice");
    assert_eq!(arr[2]["name"], "carol");

    ctx.stop().await;
}

// --- query_one tests ---

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_one_exactly_one_row() {
    let mut ctx = PostgresTestContext::new().await;
    setup_api_table(&mut ctx).await;

    let result = ctx.query_one("SELECT name, value FROM api_test WHERE name = $1", &[SqlParam::Text("bob".to_string())]).await;
    assert!(result.is_object());
    assert_eq!(result["name"], "bob");
    assert_eq!(result["value"], 20);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_one_zero_rows_errors() {
    let mut ctx = PostgresTestContext::new().await;
    setup_api_table(&mut ctx).await;

    let _err = ctx.query_one_err("SELECT * FROM api_test WHERE name = $1", &[SqlParam::Text("nobody".to_string())]).await;

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_one_multiple_rows_errors() {
    let mut ctx = PostgresTestContext::new().await;
    setup_api_table(&mut ctx).await;

    let _err = ctx.query_one_err("SELECT * FROM api_test", &[]).await;

    ctx.stop().await;
}

// --- query_opt tests ---

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_opt_zero_rows_null() {
    let mut ctx = PostgresTestContext::new().await;
    setup_api_table(&mut ctx).await;

    let result = ctx.query_opt("SELECT * FROM api_test WHERE name = $1", &[SqlParam::Text("nobody".to_string())]).await;
    assert_eq!(result, serde_json::Value::Null);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_opt_one_row_object() {
    let mut ctx = PostgresTestContext::new().await;
    setup_api_table(&mut ctx).await;

    let result = ctx.query_opt("SELECT name, value FROM api_test WHERE name = $1", &[SqlParam::Text("carol".to_string())]).await;
    assert!(result.is_object());
    assert_eq!(result["name"], "carol");
    assert_eq!(result["value"], 30);

    ctx.stop().await;
}

// --- execute tests ---

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_execute_returns_row_count() {
    let mut ctx = PostgresTestContext::new().await;
    setup_api_table(&mut ctx).await;

    // INSERT returns count
    let result = ctx.execute("INSERT INTO api_test (name, value) VALUES ('new', 99)", &[]).await;
    assert_eq!(result, serde_json::json!(1));

    // UPDATE returns count
    let result = ctx.execute("UPDATE api_test SET value = value + 1", &[]).await;
    assert_eq!(result, serde_json::json!(4)); // 3 original + 1 inserted

    // DELETE returns count
    let result = ctx.execute("DELETE FROM api_test WHERE name = $1", &[SqlParam::Text("new".to_string())]).await;
    assert_eq!(result, serde_json::json!(1));

    ctx.stop().await;
}

// --- batch_execute tests ---

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_batch_execute_returns_success() {
    let mut ctx = PostgresTestContext::new().await;

    let result = ctx.batch_execute("CREATE TABLE api_batch (id SERIAL PRIMARY KEY)").await;
    // batch_execute returns "success" string via EmptyOutput
    assert_eq!(result, serde_json::json!("success"));

    ctx.stop().await;
}

// --- simple_query tests ---

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_simple_query_returns_rows() {
    let mut ctx = PostgresTestContext::new().await;
    setup_api_table(&mut ctx).await;

    let result = ctx.simple_query("SELECT name, value FROM api_test ORDER BY id").await;
    // simple_query returns array for multiple rows
    assert!(result.is_array());
    let arr = result.as_array().unwrap();
    // simple_query returns an array containing the rows array + command complete
    // The output structure depends on PostgresSimpleQueryOutput serialization
    assert!(!arr.is_empty());

    ctx.stop().await;
}
