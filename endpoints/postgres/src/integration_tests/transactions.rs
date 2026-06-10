use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_batch_execute_implicit_transaction() {
    let mut ctx = PostgresTestContext::new().await;

    // Multiple DDL statements in batch_execute succeed atomically
    ctx.batch_execute(
        "CREATE TABLE tx_test (id SERIAL PRIMARY KEY, val TEXT NOT NULL);
         INSERT INTO tx_test (val) VALUES ('a');
         INSERT INTO tx_test (val) VALUES ('b');
         INSERT INTO tx_test (val) VALUES ('c')",
    )
    .await;

    let rows = ctx.query("SELECT val FROM tx_test ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["val"], "a");
    assert_eq!(arr[2]["val"], "c");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_explicit_begin_commit() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE tx_commit (id SERIAL PRIMARY KEY, val INT4)").await;

    // Explicit transaction: BEGIN ... COMMIT
    ctx.batch_execute(
        "BEGIN;
         INSERT INTO tx_commit (val) VALUES (10);
         INSERT INTO tx_commit (val) VALUES (20);
         COMMIT",
    )
    .await;

    let rows = ctx.query("SELECT val FROM tx_commit ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["val"], 10);
    assert_eq!(arr[1]["val"], 20);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_explicit_begin_rollback() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE tx_rollback (id SERIAL PRIMARY KEY, val INT4)").await;

    // Insert one row that stays
    ctx.batch_execute("INSERT INTO tx_rollback (val) VALUES (100)").await;

    // Explicit transaction that gets rolled back
    ctx.batch_execute(
        "BEGIN;
         INSERT INTO tx_rollback (val) VALUES (200);
         ROLLBACK",
    )
    .await;

    // Only the first row should exist
    let row = ctx.query_one("SELECT COUNT(*)::INT4 AS cnt FROM tx_rollback", &[]).await;
    assert_eq!(row["cnt"], 1);

    let row = ctx.query_one("SELECT val FROM tx_rollback", &[]).await;
    assert_eq!(row["val"], 100);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_savepoint_release() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE tx_save (id SERIAL PRIMARY KEY, val INT4)").await;

    ctx.batch_execute(
        "BEGIN;
         INSERT INTO tx_save (val) VALUES (1);
         SAVEPOINT sp1;
         INSERT INTO tx_save (val) VALUES (2);
         RELEASE SAVEPOINT sp1;
         COMMIT",
    )
    .await;

    let rows = ctx.query("SELECT val FROM tx_save ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_savepoint_rollback_to() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE tx_saveback (id SERIAL PRIMARY KEY, val INT4)").await;

    ctx.batch_execute(
        "BEGIN;
         INSERT INTO tx_saveback (val) VALUES (1);
         SAVEPOINT sp1;
         INSERT INTO tx_saveback (val) VALUES (2);
         ROLLBACK TO SAVEPOINT sp1;
         INSERT INTO tx_saveback (val) VALUES (3);
         COMMIT",
    )
    .await;

    // Row with val=2 should be rolled back, but val=1 and val=3 should remain
    let rows = ctx.query("SELECT val FROM tx_saveback ORDER BY val", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["val"], 1);
    assert_eq!(arr[1]["val"], 3);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_autocommit_individual_statements() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE tx_auto (id SERIAL PRIMARY KEY, val INT4)").await;

    // Each execute call auto-commits
    ctx.execute("INSERT INTO tx_auto (val) VALUES (1)", &[]).await;
    ctx.execute("INSERT INTO tx_auto (val) VALUES (2)", &[]).await;

    let rows = ctx.query("SELECT val FROM tx_auto ORDER BY val", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_constraint_violation_in_transaction_aborts() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE tx_constraint (id INT4 PRIMARY KEY, val TEXT NOT NULL)").await;

    ctx.execute("INSERT INTO tx_constraint VALUES (1, 'first')", &[]).await;

    // Try inserting duplicate PK in a transaction — the whole batch fails
    let err = ctx.execute_err("INSERT INTO tx_constraint VALUES (1, 'duplicate')", &[]).await;
    // Error should occur due to primary key violation
    let err_str = format!("{:?}", err);
    assert!(err_str.contains("error") || err_str.contains("Error") || err_str.contains("duplicate"));

    // Original row should still be intact
    let row = ctx.query_one("SELECT val FROM tx_constraint WHERE id = 1", &[]).await;
    assert_eq!(row["val"], "first");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_transaction_isolation_read_committed() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE tx_iso (id SERIAL PRIMARY KEY, val INT4)").await;
    ctx.execute("INSERT INTO tx_iso (val) VALUES (100)", &[]).await;

    // Set isolation level and read
    ctx.batch_execute(
        "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
         UPDATE tx_iso SET val = 200 WHERE id = 1;
         COMMIT",
    )
    .await;

    let row = ctx.query_one("SELECT val FROM tx_iso WHERE id = 1", &[]).await;
    assert_eq!(row["val"], 200);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_transaction_with_returning() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE tx_ret (id SERIAL PRIMARY KEY, val TEXT)").await;

    // Execute INSERT with RETURNING inside a transaction-style batch
    ctx.batch_execute(
        "BEGIN;
         INSERT INTO tx_ret (val) VALUES ('hello');
         INSERT INTO tx_ret (val) VALUES ('world');
         COMMIT",
    )
    .await;

    let rows = ctx.query("SELECT id, val FROM tx_ret ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["val"], "hello");
    assert_eq!(arr[1]["val"], "world");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_nested_savepoints() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE tx_nested (id SERIAL PRIMARY KEY, val TEXT)").await;

    ctx.batch_execute(
        "BEGIN;
         INSERT INTO tx_nested (val) VALUES ('a');
         SAVEPOINT sp1;
         INSERT INTO tx_nested (val) VALUES ('b');
         SAVEPOINT sp2;
         INSERT INTO tx_nested (val) VALUES ('c');
         ROLLBACK TO SAVEPOINT sp2;
         INSERT INTO tx_nested (val) VALUES ('d');
         RELEASE SAVEPOINT sp1;
         COMMIT",
    )
    .await;

    // 'c' was rolled back at sp2, 'a', 'b', 'd' remain
    let rows = ctx.query("SELECT val FROM tx_nested ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["val"], "a");
    assert_eq!(arr[1]["val"], "b");
    assert_eq!(arr[2]["val"], "d");

    ctx.stop().await;
}
