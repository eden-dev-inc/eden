use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_simple_query_insert_returns_affected_rows() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE sq_insert (id SERIAL PRIMARY KEY, name TEXT)").await;

    let result = ctx.simple_query("INSERT INTO sq_insert (name) VALUES ('a'), ('b')").await;
    // simple_query INSERT returns {"affected_rows": N}
    assert_eq!(result["affected_rows"], 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_simple_query_ddl() {
    let mut ctx = PostgresTestContext::new().await;

    let result = ctx.simple_query("CREATE TABLE sq_ddl (id SERIAL PRIMARY KEY)").await;
    // DDL via simple_query returns command complete with 0 affected rows
    assert!(result.get("affected_rows").is_some());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_simple_query_mixed_results() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE sq_mixed (id SERIAL PRIMARY KEY, val TEXT)").await;

    // Multiple statements in one simple_query call
    let result = ctx
        .simple_query(
            "INSERT INTO sq_mixed (val) VALUES ('a');
             SELECT val FROM sq_mixed;",
        )
        .await;
    // Multiple results returned as array
    assert!(result.is_array());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_simple_query_empty_result() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE sq_empty (id SERIAL PRIMARY KEY, val TEXT)").await;

    let result = ctx.simple_query("SELECT val FROM sq_empty").await;
    // Empty SELECT via simple_query
    // Based on PostgresSimpleQueryOutput: 0 rows + CommandComplete
    // The behavior depends on the output serialization
    assert!(result.get("affected_rows").is_some() || result.is_null());

    ctx.stop().await;
}
