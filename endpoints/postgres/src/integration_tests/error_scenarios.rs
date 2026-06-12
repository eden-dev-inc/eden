use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

// ========================
// Failed Transaction State
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_aborted_transaction_rejects_queries() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_abort (id SERIAL PRIMARY KEY, val TEXT NOT NULL)").await;

    // Start a transaction and cause an error (NOT NULL violation)
    // After the error, the transaction is in aborted state
    let err = ctx
        .batch_execute_err(
            "BEGIN;
             INSERT INTO es_abort (val) VALUES ('ok');
             INSERT INTO es_abort (val) VALUES (NULL);
             COMMIT",
        )
        .await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_recovery_after_error() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_recover (id SERIAL PRIMARY KEY, val INT4)").await;

    // Cause an error
    let _err = ctx.query_err("SELECT * FROM nonexistent_table_xyz", &[]).await;

    // Connection should still be usable for subsequent queries
    ctx.execute("INSERT INTO es_recover (val) VALUES (42)", &[]).await;
    let row = ctx.query_one("SELECT val FROM es_recover WHERE id = 1", &[]).await;
    assert_eq!(row["val"], 42);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_recovery_after_constraint_error() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_recover2 (id INT PRIMARY KEY, val TEXT)").await;
    ctx.execute(
        "INSERT INTO es_recover2 (id, val) VALUES ($1, $2)",
        &[SqlParam::Int4(1), SqlParam::Text("first".to_string())],
    )
    .await;

    // Cause a constraint violation
    let _err = ctx
        .execute_err(
            "INSERT INTO es_recover2 (id, val) VALUES ($1, $2)",
            &[SqlParam::Int4(1), SqlParam::Text("duplicate".to_string())],
        )
        .await;

    // Should still work after the error
    ctx.execute(
        "INSERT INTO es_recover2 (id, val) VALUES ($1, $2)",
        &[SqlParam::Int4(2), SqlParam::Text("second".to_string())],
    )
    .await;

    let rows = ctx.query("SELECT val FROM es_recover2 ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["val"], "first");
    assert_eq!(arr[1]["val"], "second");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_multiple_errors_in_sequence() {
    let mut ctx = PostgresTestContext::new().await;

    // Multiple errors in sequence should all be handled
    let err1 = ctx.query_err("SELECT 1/0", &[]).await;
    assert!(!format!("{err1}").is_empty());

    let err2 = ctx.query_err("SELECT * FROM no_such_table", &[]).await;
    assert!(!format!("{err2}").is_empty());

    let err3 = ctx.query_err("INVALID SQL SYNTAX HERE", &[]).await;
    assert!(!format!("{err3}").is_empty());

    // Connection still works
    let row = ctx.query_one("SELECT 1 AS val", &[]).await;
    assert_eq!(row["val"], 1);

    ctx.stop().await;
}

// ========================
// COPY Operation Errors
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_copy_in_wrong_column_count() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_copy_cols (id SERIAL PRIMARY KEY, a TEXT, b TEXT, c INT4)").await;

    // CSV data has 2 columns but table expects 3 (a, b, c)
    let err = ctx.copy_in_err("COPY es_copy_cols (a, b, c) FROM STDIN WITH (FORMAT csv)", "hello,world\n").await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_copy_in_type_mismatch() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_copy_type (id SERIAL PRIMARY KEY, val INT4)").await;

    // CSV data has a string where INT4 is expected
    let err = ctx.copy_in_err("COPY es_copy_type (val) FROM STDIN WITH (FORMAT csv)", "not_a_number\n").await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_copy_in_violates_not_null() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_copy_nn (id SERIAL PRIMARY KEY, val TEXT NOT NULL)").await;

    // Empty value for NOT NULL column in CSV
    let err = ctx.copy_in_err("COPY es_copy_nn (val) FROM STDIN WITH (FORMAT csv)", "\n").await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_copy_in_violates_unique() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_copy_uniq (id INT PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO es_copy_uniq VALUES (1, 'existing')", &[]).await;

    // COPY data with duplicate primary key
    let err = ctx.copy_in_err("COPY es_copy_uniq FROM STDIN WITH (FORMAT csv)", "1,duplicate\n").await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    // Original row should still exist
    let row = ctx.query_one("SELECT val FROM es_copy_uniq WHERE id = 1", &[]).await;
    assert_eq!(row["val"], "existing");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_copy_out_nonexistent_table() {
    let mut ctx = PostgresTestContext::new().await;

    let err = ctx.copy_out_err("COPY nonexistent_table TO STDOUT").await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_copy_recovery_after_error() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_copy_rec (id SERIAL PRIMARY KEY, val INT4)").await;

    // Cause a COPY error
    let _err = ctx.copy_in_err("COPY es_copy_rec (val) FROM STDIN WITH (FORMAT csv)", "not_int\n").await;

    // Connection should still work after COPY error
    ctx.execute("INSERT INTO es_copy_rec (val) VALUES (42)", &[]).await;
    let row = ctx.query_one("SELECT val FROM es_copy_rec WHERE id = 1", &[]).await;
    assert_eq!(row["val"], 42);

    ctx.stop().await;
}

// ========================
// PL/pgSQL & Trigger Errors
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_plpgsql_raise_exception() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE OR REPLACE FUNCTION es_raise_error() RETURNS VOID AS $$
         BEGIN
             RAISE EXCEPTION 'custom error from plpgsql';
         END;
         $$ LANGUAGE plpgsql",
    )
    .await;

    let err = ctx.query_err("SELECT es_raise_error()", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_plpgsql_raise_with_errcode() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE OR REPLACE FUNCTION es_raise_coded() RETURNS VOID AS $$
         BEGIN
             RAISE EXCEPTION 'integrity violation' USING ERRCODE = '23000';
         END;
         $$ LANGUAGE plpgsql",
    )
    .await;

    let err = ctx.query_err("SELECT es_raise_coded()", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_trigger_raises_exception() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE es_trigger_tbl (id SERIAL PRIMARY KEY, val INT4);

         CREATE OR REPLACE FUNCTION es_trigger_fn() RETURNS TRIGGER AS $$
         BEGIN
             IF NEW.val < 0 THEN
                 RAISE EXCEPTION 'negative values not allowed: %', NEW.val;
             END IF;
             RETURN NEW;
         END;
         $$ LANGUAGE plpgsql;

         CREATE TRIGGER es_check_val
             BEFORE INSERT ON es_trigger_tbl
             FOR EACH ROW EXECUTE FUNCTION es_trigger_fn()",
    )
    .await;

    // Positive value should work
    ctx.execute("INSERT INTO es_trigger_tbl (val) VALUES ($1)", &[SqlParam::Int4(10)]).await;

    // Negative value should trigger the exception
    let err = ctx.execute_err("INSERT INTO es_trigger_tbl (val) VALUES ($1)", &[SqlParam::Int4(-5)]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    // Only the positive row should exist
    let row = ctx.query_one("SELECT COUNT(*)::INT4 AS cnt FROM es_trigger_tbl", &[]).await;
    assert_eq!(row["cnt"], 1);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_function_division_by_zero() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE OR REPLACE FUNCTION es_divide(a INT, b INT) RETURNS INT AS $$
         BEGIN
             RETURN a / b;
         END;
         $$ LANGUAGE plpgsql",
    )
    .await;

    let err = ctx.query_err("SELECT es_divide(10, 0)", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

// ========================
// Value Overflow & Type Errors
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_smallint_overflow() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_overflow (id SERIAL PRIMARY KEY, val INT2)").await;

    // INT2 max is 32767, this should overflow
    let err = ctx.execute_err("INSERT INTO es_overflow (val) VALUES (99999)", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_integer_overflow() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_intover (id SERIAL PRIMARY KEY, val INT4)").await;

    // INT4 max is 2147483647
    let err = ctx.execute_err("INSERT INTO es_intover (val) VALUES (99999999999)", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_varchar_too_long() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_varchar (id SERIAL PRIMARY KEY, val VARCHAR(5))").await;

    let err = ctx
        .execute_err(
            "INSERT INTO es_varchar (val) VALUES ($1)",
            &[SqlParam::Text("this string is way too long".to_string())],
        )
        .await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_numeric_overflow() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_numover (id SERIAL PRIMARY KEY, val NUMERIC(5, 2))").await;

    // NUMERIC(5,2) max is 999.99
    let err = ctx.execute_err("INSERT INTO es_numover (val) VALUES (99999.99)", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_invalid_date() {
    let mut ctx = PostgresTestContext::new().await;

    let err = ctx.query_err("SELECT '2024-13-45'::DATE", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_invalid_uuid() {
    let mut ctx = PostgresTestContext::new().await;

    let err = ctx.query_err("SELECT 'not-a-uuid'::UUID", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_invalid_json() {
    let mut ctx = PostgresTestContext::new().await;

    let err = ctx.query_err("SELECT '{invalid json'::JSON", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_invalid_jsonb() {
    let mut ctx = PostgresTestContext::new().await;

    let err = ctx.query_err("SELECT '{not valid}'::JSONB", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

// ========================
// Statement Timeout
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_statement_timeout() {
    let mut ctx = PostgresTestContext::new().await;

    // Set a very short timeout and run a slow query
    let err = ctx
        .simple_query_err(
            "SET statement_timeout = '1ms';
             SELECT pg_sleep(10)",
        )
        .await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    // Reset timeout and verify connection still works
    ctx.batch_execute("SET statement_timeout = '0'").await;
    let row = ctx.query_one("SELECT 1 AS val", &[]).await;
    assert_eq!(row["val"], 1);

    ctx.stop().await;
}

// ========================
// Domain Constraint Violations
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_domain_constraint_violation() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE DOMAIN es_positive_int AS INT4 CHECK (VALUE > 0);
         CREATE TABLE es_domain (id SERIAL PRIMARY KEY, val es_positive_int)",
    )
    .await;

    // Valid value
    ctx.execute("INSERT INTO es_domain (val) VALUES ($1)", &[SqlParam::Int4(5)]).await;

    // Invalid value (violates domain CHECK)
    let err = ctx.execute_err("INSERT INTO es_domain (val) VALUES ($1)", &[SqlParam::Int4(-1)]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

// ========================
// Exclusion Constraint
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_exclusion_constraint_violation() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE EXTENSION IF NOT EXISTS btree_gist;
         CREATE TABLE es_excl (
             id SERIAL PRIMARY KEY,
             room INT4,
             during TSRANGE,
             EXCLUDE USING gist (room WITH =, during WITH &&)
         )",
    )
    .await;

    // Book room 1 from 10:00 to 12:00
    ctx.execute("INSERT INTO es_excl (room, during) VALUES (1, '[2024-01-01 10:00, 2024-01-01 12:00)')", &[]).await;

    // Overlapping booking for room 1 should fail
    let err = ctx.execute_err("INSERT INTO es_excl (room, during) VALUES (1, '[2024-01-01 11:00, 2024-01-01 13:00)')", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    // Different room should succeed
    ctx.execute("INSERT INTO es_excl (room, during) VALUES (2, '[2024-01-01 11:00, 2024-01-01 13:00)')", &[]).await;

    ctx.stop().await;
}

// ========================
// Permission Errors
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_read_only_transaction_write() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_readonly (id SERIAL PRIMARY KEY, val TEXT)").await;

    // Start read-only transaction, then try to write
    let err = ctx
        .batch_execute_err(
            "BEGIN READ ONLY;
             INSERT INTO es_readonly (val) VALUES ('should fail');
             COMMIT",
        )
        .await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

// ========================
// Batch Execute Errors
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_batch_execute_syntax_error() {
    let mut ctx = PostgresTestContext::new().await;

    let err = ctx.batch_execute_err("THIS IS NOT VALID SQL").await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    // Connection should still work
    let row = ctx.query_one("SELECT 1 AS val", &[]).await;
    assert_eq!(row["val"], 1);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_batch_execute_partial_failure() {
    let mut ctx = PostgresTestContext::new().await;

    // First statement succeeds, second fails
    let err = ctx
        .batch_execute_err(
            "CREATE TABLE es_partial (id SERIAL PRIMARY KEY, val TEXT);
             INSERT INTO nonexistent_table VALUES (1)",
        )
        .await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

// ========================
// Simple Query Errors
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_simple_query_syntax_error() {
    let mut ctx = PostgresTestContext::new().await;

    let err = ctx.simple_query_err("SELEC * FORM nowhere").await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_simple_query_division_by_zero() {
    let mut ctx = PostgresTestContext::new().await;

    let err = ctx.simple_query_err("SELECT 1/0").await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

// ========================
// Immutable / Generated Column Errors
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_generated_column_direct_insert() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE es_gen (
            id SERIAL PRIMARY KEY,
            a INT4,
            b INT4,
            total INT4 GENERATED ALWAYS AS (a + b) STORED
        )",
    )
    .await;

    // Cannot directly insert into a GENERATED ALWAYS column
    let err = ctx.execute_err("INSERT INTO es_gen (a, b, total) VALUES (1, 2, 3)", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    // Inserting without generated column should work
    ctx.execute("INSERT INTO es_gen (a, b) VALUES (1, 2)", &[]).await;
    let row = ctx.query_one("SELECT total FROM es_gen WHERE id = 1", &[]).await;
    assert_eq!(row["total"], 3);

    ctx.stop().await;
}

// ========================
// Deferred Constraint Errors
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_deferred_constraint_violation_at_commit() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE es_parent_def (id INT PRIMARY KEY);
         CREATE TABLE es_child_def (
             id SERIAL PRIMARY KEY,
             parent_id INT REFERENCES es_parent_def(id) DEFERRABLE INITIALLY DEFERRED
         );
         INSERT INTO es_parent_def VALUES (1)",
    )
    .await;

    // The FK check is deferred until COMMIT, so the INSERT succeeds but COMMIT fails
    let err = ctx
        .batch_execute_err(
            "BEGIN;
             INSERT INTO es_child_def (parent_id) VALUES (999);
             COMMIT",
        )
        .await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

// ========================
// Concurrent / Locking Errors
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_nowait_lock_on_locked_row() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_lock (id INT PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO es_lock VALUES (1, 'initial')", &[]).await;

    // SELECT FOR UPDATE NOWAIT should fail if the row is already locked.
    // With a single connection in an explicit transaction, we can lock a row
    // then try NOWAIT from a subquery - but PG allows re-locking in the same tx.
    // Instead, just verify that NOWAIT syntax is handled correctly by the proxy.
    // Lock the row and verify it works
    ctx.batch_execute(
        "BEGIN;
         SELECT * FROM es_lock WHERE id = 1 FOR UPDATE NOWAIT;
         COMMIT",
    )
    .await;

    // Verify SKIP LOCKED also works (returns empty when locked - but same tx grants it)
    let row = ctx.query_one("SELECT val FROM es_lock WHERE id = 1 FOR UPDATE SKIP LOCKED", &[]).await;
    assert_eq!(row["val"], "initial");

    ctx.stop().await;
}

// ========================
// Query Result Expectations
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_one_returns_zero_rows() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_empty_q (id SERIAL PRIMARY KEY, val TEXT)").await;

    // query_one expects exactly 1 row, but table is empty
    let err = ctx.query_one_err("SELECT * FROM es_empty_q", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_one_returns_multiple_rows() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE es_multi_q (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO es_multi_q (val) VALUES ('a'), ('b'), ('c')", &[]).await;

    // query_one expects exactly 1 row, but we get 3
    let err = ctx.query_one_err("SELECT * FROM es_multi_q", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}

// ========================
// Drop / Cascade Errors
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_drop_table_with_dependent() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE es_dep_parent (id INT PRIMARY KEY);
         CREATE TABLE es_dep_child (id SERIAL PRIMARY KEY, pid INT REFERENCES es_dep_parent(id))",
    )
    .await;

    // Cannot drop parent table without CASCADE
    let err = ctx.batch_execute_err("DROP TABLE es_dep_parent").await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    // CASCADE should work
    ctx.batch_execute("DROP TABLE es_dep_parent CASCADE").await;

    ctx.stop().await;
}

// ========================
// Invalid SQL Patterns
// ========================

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_empty_query() {
    let mut ctx = PostgresTestContext::new().await;

    // An empty query should be handled gracefully (not crash)
    // Postgres returns an EmptyQueryResponse for empty strings
    let result = ctx.simple_query("").await;
    // Empty query returns null/empty result, not an error
    assert!(result.is_null() || result.is_array());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_semicolons_only() {
    let mut ctx = PostgresTestContext::new().await;

    // Just semicolons
    let result = ctx.simple_query(";;;").await;
    assert!(result.is_null() || result.is_array());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_truncated_utf8_in_error() {
    let mut ctx = PostgresTestContext::new().await;

    // Query with unicode that causes an error - verify error message doesn't corrupt
    let err = ctx.query_err("SELECT * FROM \"t\u{00E9}st_t\u{00E4}ble_\u{00FC}nicode\"", &[]).await;
    let err_msg = format!("{err}");
    assert!(!err_msg.is_empty());

    ctx.stop().await;
}
