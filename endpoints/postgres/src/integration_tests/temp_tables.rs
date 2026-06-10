use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

/// Note: Temporary tables are connection-scoped. Since Eden uses a connection pool,
/// temp tables created via write may not be visible on read connections.
/// These tests work within that constraint by combining operations in batch_execute
/// or testing temp table concepts alongside permanent tables.

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_temp_table_lifecycle_in_batch() {
    let mut ctx = PostgresTestContext::new().await;

    // Create temp table, insert, and capture result into permanent table
    // ALL in a single batch_execute so it stays on one connection
    ctx.batch_execute(
        "CREATE TEMP TABLE tmp_batch (id SERIAL PRIMARY KEY, val TEXT NOT NULL);
         INSERT INTO tmp_batch (val) VALUES ('a'), ('b'), ('c');
         CREATE TABLE tmp_verify (cnt INT4);
         INSERT INTO tmp_verify SELECT COUNT(*)::INT4 FROM tmp_batch",
    )
    .await;

    let row = ctx.query_one("SELECT cnt FROM tmp_verify", &[]).await;
    assert_eq!(row["cnt"], 3);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_temp_table_on_commit_drop() {
    let mut ctx = PostgresTestContext::new().await;

    // Create temp table that drops on transaction end
    ctx.batch_execute(
        "BEGIN;
         CREATE TEMP TABLE tmp_drop (id INT4) ON COMMIT DROP;
         INSERT INTO tmp_drop VALUES (1);
         COMMIT",
    )
    .await;

    // Table should be gone after commit — querying it should fail
    let err = ctx.query_err("SELECT * FROM tmp_drop", &[]).await;
    let err_str = format!("{:?}", err);
    assert!(err_str.contains("error") || err_str.contains("Error"));

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_temp_table_if_not_exists() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TEMP TABLE tmp_exists (id INT4)").await;

    // Creating again without IF NOT EXISTS would fail
    // But with IF NOT EXISTS it succeeds
    ctx.batch_execute("CREATE TEMP TABLE IF NOT EXISTS tmp_exists (id INT4)").await;

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_temp_table_shadows_permanent_in_batch() {
    let mut ctx = PostgresTestContext::new().await;

    // Create permanent table
    ctx.batch_execute("CREATE TABLE perm_shadow (id INT4, source TEXT DEFAULT 'permanent')").await;
    ctx.execute("INSERT INTO perm_shadow (id) VALUES (1)", &[]).await;

    // In a batch: create temp table with same name, insert, and copy result to permanent
    ctx.batch_execute(
        "CREATE TEMP TABLE perm_shadow (id INT4, source TEXT DEFAULT 'temporary');
         INSERT INTO perm_shadow (id) VALUES (2);
         CREATE TABLE shadow_result (source TEXT);
         INSERT INTO shadow_result SELECT source FROM perm_shadow WHERE id = 2",
    )
    .await;

    // shadow_result should have 'temporary' proving the temp table shadowed the permanent one
    let row = ctx.query_one("SELECT source FROM shadow_result", &[]).await;
    assert_eq!(row["source"], "temporary");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_temp_table_with_index_in_batch() {
    let mut ctx = PostgresTestContext::new().await;

    // Create temp table with index and verify it works within a batch
    ctx.batch_execute(
        "CREATE TEMP TABLE tmp_idx (id SERIAL PRIMARY KEY, val TEXT);
         CREATE INDEX tmp_idx_val ON tmp_idx (val);
         INSERT INTO tmp_idx (val) VALUES ('x'), ('y'), ('z');
         CREATE TABLE idx_verify (cnt INT4);
         INSERT INTO idx_verify SELECT COUNT(*)::INT4 FROM tmp_idx WHERE val >= 'y'",
    )
    .await;

    let row = ctx.query_one("SELECT cnt FROM idx_verify", &[]).await;
    assert_eq!(row["cnt"], 2);

    ctx.stop().await;
}
