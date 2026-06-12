use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_generated_column_stored() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE gen_stored (
            id SERIAL PRIMARY KEY,
            price INT4 NOT NULL,
            quantity INT4 NOT NULL,
            total INT4 GENERATED ALWAYS AS (price * quantity) STORED
        )",
    )
    .await;
    ctx.execute("INSERT INTO gen_stored (price, quantity) VALUES (10, 5), (20, 3)", &[]).await;

    let rows = ctx.query("SELECT price, quantity, total FROM gen_stored ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["total"], 50);
    assert_eq!(arr[1]["total"], 60);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_generated_column_text_concat() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE gen_text (
            id SERIAL PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED
        )",
    )
    .await;
    ctx.execute(
        "INSERT INTO gen_text (first_name, last_name) VALUES ($1, $2)",
        &[SqlParam::Text("John".to_string()), SqlParam::Text("Doe".to_string())],
    )
    .await;

    let row = ctx.query_one("SELECT full_name FROM gen_text WHERE id = 1", &[]).await;
    assert_eq!(row["full_name"], "John Doe");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_generated_column_cannot_insert() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE gen_readonly (
            id SERIAL PRIMARY KEY,
            val INT4,
            doubled INT4 GENERATED ALWAYS AS (val * 2) STORED
        )",
    )
    .await;

    // Trying to insert into a generated column should fail
    let err = ctx.execute_err("INSERT INTO gen_readonly (val, doubled) VALUES (5, 10)", &[]).await;
    let err_str = format!("{:?}", err);
    assert!(err_str.contains("error") || err_str.contains("Error"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_generated_column_updates_on_base_change() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE gen_update (
            id SERIAL PRIMARY KEY,
            val INT4 NOT NULL,
            squared INT4 GENERATED ALWAYS AS (val * val) STORED
        )",
    )
    .await;
    ctx.execute("INSERT INTO gen_update (val) VALUES (5)", &[]).await;

    let row = ctx.query_one("SELECT squared FROM gen_update WHERE id = 1", &[]).await;
    assert_eq!(row["squared"], 25);

    // Update the base column
    ctx.execute("UPDATE gen_update SET val = 7 WHERE id = 1", &[]).await;

    let row = ctx.query_one("SELECT squared FROM gen_update WHERE id = 1", &[]).await;
    assert_eq!(row["squared"], 49);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_generated_column_in_where_clause() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE gen_where (
            id SERIAL PRIMARY KEY,
            width INT4 NOT NULL,
            height INT4 NOT NULL,
            area INT4 GENERATED ALWAYS AS (width * height) STORED
        )",
    )
    .await;
    ctx.execute("INSERT INTO gen_where (width, height) VALUES (10, 5), (3, 4), (8, 7), (2, 2)", &[]).await;

    let rows = ctx.query("SELECT width, height, area FROM gen_where WHERE area > 20 ORDER BY area", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2); // 10*5=50, 8*7=56
    assert_eq!(arr[0]["area"], 50);
    assert_eq!(arr[1]["area"], 56);

    ctx.stop().await;
}
