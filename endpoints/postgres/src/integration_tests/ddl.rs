use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_create_table() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ddl_create_test (id SERIAL PRIMARY KEY, name TEXT NOT NULL, value INT4)").await;

    // Table exists; query returns null for empty table
    let result = ctx.query("SELECT * FROM ddl_create_test", &[]).await;
    assert_eq!(result, serde_json::Value::Null);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_create_table_if_not_exists() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ddl_idempotent (id SERIAL PRIMARY KEY)").await;
    // Creating again with IF NOT EXISTS should not error
    ctx.batch_execute("CREATE TABLE IF NOT EXISTS ddl_idempotent (id SERIAL PRIMARY KEY)").await;

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_create_table_with_constraints() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE ddl_constraints (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            age INT4 CHECK (age >= 0),
            status TEXT DEFAULT 'active'
        )",
    )
    .await;

    // Insert a row relying on DEFAULT
    ctx.execute(
        "INSERT INTO ddl_constraints (email, age) VALUES ($1, $2)",
        &[
            crate::api::wrapper::input::SqlParam::Text("test@example.com".to_string()),
            crate::api::wrapper::input::SqlParam::Int4(25),
        ],
    )
    .await;

    let row = ctx
        .query_one(
            "SELECT email, age, status FROM ddl_constraints WHERE email = $1",
            &[crate::api::wrapper::input::SqlParam::Text("test@example.com".to_string())],
        )
        .await;
    assert_eq!(row["email"], "test@example.com");
    assert_eq!(row["age"], 25);
    assert_eq!(row["status"], "active");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_alter_table_add_column() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ddl_alter_add (id SERIAL PRIMARY KEY, name TEXT)").await;
    ctx.batch_execute("ALTER TABLE ddl_alter_add ADD COLUMN age INT4").await;

    ctx.execute(
        "INSERT INTO ddl_alter_add (name, age) VALUES ($1, $2)",
        &[
            crate::api::wrapper::input::SqlParam::Text("alice".to_string()),
            crate::api::wrapper::input::SqlParam::Int4(30),
        ],
    )
    .await;

    let row = ctx.query_one("SELECT name, age FROM ddl_alter_add WHERE name = 'alice'", &[]).await;
    assert_eq!(row["name"], "alice");
    assert_eq!(row["age"], 30);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_alter_table_drop_column() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ddl_alter_drop (id SERIAL PRIMARY KEY, name TEXT, temp_col TEXT)").await;
    ctx.batch_execute("ALTER TABLE ddl_alter_drop DROP COLUMN temp_col").await;

    ctx.execute(
        "INSERT INTO ddl_alter_drop (name) VALUES ($1)",
        &[crate::api::wrapper::input::SqlParam::Text("bob".to_string())],
    )
    .await;

    let row = ctx.query_one("SELECT * FROM ddl_alter_drop WHERE name = 'bob'", &[]).await;
    assert_eq!(row["name"], "bob");
    assert!(row.get("temp_col").is_none());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_alter_table_rename_column() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ddl_alter_rename (id SERIAL PRIMARY KEY, old_name TEXT)").await;
    ctx.batch_execute("ALTER TABLE ddl_alter_rename RENAME COLUMN old_name TO new_name").await;

    ctx.execute(
        "INSERT INTO ddl_alter_rename (new_name) VALUES ($1)",
        &[crate::api::wrapper::input::SqlParam::Text("renamed".to_string())],
    )
    .await;

    let row = ctx.query_one("SELECT new_name FROM ddl_alter_rename", &[]).await;
    assert_eq!(row["new_name"], "renamed");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_drop_table() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ddl_drop_me (id SERIAL PRIMARY KEY)").await;
    ctx.batch_execute("DROP TABLE ddl_drop_me").await;

    // Querying the dropped table should error
    let _err = ctx.query_err("SELECT * FROM ddl_drop_me", &[]).await;

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_drop_table_if_exists() {
    let mut ctx = PostgresTestContext::new().await;

    // Dropping a non-existent table with IF EXISTS should not error
    ctx.batch_execute("DROP TABLE IF EXISTS ddl_nonexistent").await;

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_create_index() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ddl_index_test (id SERIAL PRIMARY KEY, name TEXT, value INT4)").await;
    ctx.batch_execute("CREATE INDEX idx_ddl_name ON ddl_index_test (name)").await;

    // Verify index exists via pg_indexes
    let result = ctx
        .query_one(
            "SELECT indexname FROM pg_indexes WHERE tablename = 'ddl_index_test' AND indexname = 'idx_ddl_name'",
            &[],
        )
        .await;
    assert_eq!(result["indexname"], "idx_ddl_name");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_create_unique_index() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE ddl_unique_idx (id SERIAL PRIMARY KEY, code TEXT)").await;
    ctx.batch_execute("CREATE UNIQUE INDEX idx_unique_code ON ddl_unique_idx (code)").await;

    ctx.execute(
        "INSERT INTO ddl_unique_idx (code) VALUES ($1)",
        &[crate::api::wrapper::input::SqlParam::Text("ABC".to_string())],
    )
    .await;

    // Inserting duplicate should fail
    let _err = ctx
        .execute_err(
            "INSERT INTO ddl_unique_idx (code) VALUES ($1)",
            &[crate::api::wrapper::input::SqlParam::Text("ABC".to_string())],
        )
        .await;

    ctx.stop().await;
}
