use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_insert_single_row() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dml_insert (id SERIAL PRIMARY KEY, name TEXT NOT NULL, value INT4)").await;

    let result = ctx
        .execute(
            "INSERT INTO dml_insert (name, value) VALUES ($1, $2)",
            &[SqlParam::Text("alice".to_string()), SqlParam::Int4(42)],
        )
        .await;
    assert_eq!(result, serde_json::json!(1));

    let row = ctx.query_one("SELECT name, value FROM dml_insert WHERE name = $1", &[SqlParam::Text("alice".to_string())]).await;
    assert_eq!(row["name"], "alice");
    assert_eq!(row["value"], 42);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_insert_multiple_rows() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dml_multi_insert (id SERIAL PRIMARY KEY, name TEXT)").await;

    let result = ctx.execute("INSERT INTO dml_multi_insert (name) VALUES ('a'), ('b'), ('c')", &[]).await;
    assert_eq!(result, serde_json::json!(3));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_insert_returning() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dml_returning (id SERIAL PRIMARY KEY, name TEXT NOT NULL)").await;

    let row = ctx
        .query(
            "INSERT INTO dml_returning (name) VALUES ($1) RETURNING id, name",
            &[SqlParam::Text("bob".to_string())],
        )
        .await;
    assert_eq!(row["name"], "bob");
    assert_eq!(row["id"], 1);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_insert_on_conflict_do_nothing() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dml_upsert_nothing (id INT PRIMARY KEY, name TEXT)").await;

    ctx.execute(
        "INSERT INTO dml_upsert_nothing (id, name) VALUES ($1, $2)",
        &[SqlParam::Int4(1), SqlParam::Text("first".to_string())],
    )
    .await;

    // Conflict on PK: do nothing
    let result = ctx
        .execute(
            "INSERT INTO dml_upsert_nothing (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            &[SqlParam::Int4(1), SqlParam::Text("second".to_string())],
        )
        .await;
    assert_eq!(result, serde_json::json!(0));

    // Verify original value unchanged
    let row = ctx.query_one("SELECT name FROM dml_upsert_nothing WHERE id = $1", &[SqlParam::Int4(1)]).await;
    assert_eq!(row["name"], "first");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_insert_on_conflict_do_update() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dml_upsert_update (id INT PRIMARY KEY, name TEXT)").await;

    ctx.execute(
        "INSERT INTO dml_upsert_update (id, name) VALUES ($1, $2)",
        &[SqlParam::Int4(1), SqlParam::Text("original".to_string())],
    )
    .await;

    let result = ctx
        .execute(
            "INSERT INTO dml_upsert_update (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            &[SqlParam::Int4(1), SqlParam::Text("updated".to_string())],
        )
        .await;
    assert_eq!(result, serde_json::json!(1));

    let row = ctx.query_one("SELECT name FROM dml_upsert_update WHERE id = $1", &[SqlParam::Int4(1)]).await;
    assert_eq!(row["name"], "updated");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_update_single_row() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dml_update (id SERIAL PRIMARY KEY, name TEXT, value INT4)").await;
    ctx.execute(
        "INSERT INTO dml_update (name, value) VALUES ($1, $2)",
        &[SqlParam::Text("alice".to_string()), SqlParam::Int4(10)],
    )
    .await;

    let result = ctx
        .execute(
            "UPDATE dml_update SET value = $1 WHERE name = $2",
            &[SqlParam::Int4(99), SqlParam::Text("alice".to_string())],
        )
        .await;
    assert_eq!(result, serde_json::json!(1));

    let row = ctx.query_one("SELECT value FROM dml_update WHERE name = 'alice'", &[]).await;
    assert_eq!(row["value"], 99);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_update_multiple_rows() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dml_multi_update (id SERIAL PRIMARY KEY, status TEXT)").await;
    ctx.execute("INSERT INTO dml_multi_update (status) VALUES ('pending'), ('pending'), ('done')", &[]).await;

    let result = ctx
        .execute(
            "UPDATE dml_multi_update SET status = $1 WHERE status = $2",
            &[SqlParam::Text("processed".to_string()), SqlParam::Text("pending".to_string())],
        )
        .await;
    assert_eq!(result, serde_json::json!(2));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_update_returning() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dml_update_ret (id SERIAL PRIMARY KEY, name TEXT, value INT4)").await;
    ctx.execute("INSERT INTO dml_update_ret (name, value) VALUES ('x', 1), ('y', 2)", &[]).await;

    let rows = ctx.query("UPDATE dml_update_ret SET value = value * 10 RETURNING name, value", &[]).await;
    assert!(rows.is_array());
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_update_no_match() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dml_update_none (id SERIAL PRIMARY KEY, name TEXT)").await;
    ctx.execute("INSERT INTO dml_update_none (name) VALUES ('exists')", &[]).await;

    let result = ctx
        .execute(
            "UPDATE dml_update_none SET name = 'changed' WHERE name = $1",
            &[SqlParam::Text("nonexistent".to_string())],
        )
        .await;
    assert_eq!(result, serde_json::json!(0));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_delete_single_row() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dml_delete (id SERIAL PRIMARY KEY, name TEXT)").await;
    ctx.execute("INSERT INTO dml_delete (name) VALUES ('a'), ('b'), ('c')", &[]).await;

    let result = ctx.execute("DELETE FROM dml_delete WHERE name = $1", &[SqlParam::Text("b".to_string())]).await;
    assert_eq!(result, serde_json::json!(1));

    // Verify only 2 rows remain
    let rows = ctx.query("SELECT * FROM dml_delete", &[]).await;
    assert!(rows.is_array());
    assert_eq!(rows.as_array().unwrap().len(), 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_delete_all_rows() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dml_delete_all (id SERIAL PRIMARY KEY, name TEXT)").await;
    ctx.execute("INSERT INTO dml_delete_all (name) VALUES ('a'), ('b'), ('c')", &[]).await;

    let result = ctx.execute("DELETE FROM dml_delete_all", &[]).await;
    assert_eq!(result, serde_json::json!(3));

    let result = ctx.query("SELECT * FROM dml_delete_all", &[]).await;
    assert_eq!(result, serde_json::Value::Null);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_delete_returning() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE dml_delete_ret (id SERIAL PRIMARY KEY, name TEXT)").await;
    ctx.execute("INSERT INTO dml_delete_ret (name) VALUES ('x'), ('y')", &[]).await;

    let rows = ctx.query("DELETE FROM dml_delete_ret WHERE name = $1 RETURNING id, name", &[SqlParam::Text("x".to_string())]).await;
    // Single row returned as object (not array)
    assert_eq!(rows["name"], "x");

    ctx.stop().await;
}
