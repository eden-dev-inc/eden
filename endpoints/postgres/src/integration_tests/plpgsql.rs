use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_do_block_basic() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE do_test (id SERIAL PRIMARY KEY, val TEXT)").await;

    // DO block: anonymous PL/pgSQL that inserts rows
    ctx.batch_execute(
        "DO $$
         BEGIN
             INSERT INTO do_test (val) VALUES ('from_do_1');
             INSERT INTO do_test (val) VALUES ('from_do_2');
         END $$",
    )
    .await;

    let rows = ctx.query("SELECT val FROM do_test ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["val"], "from_do_1");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_do_block_with_loop() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE do_loop (id SERIAL PRIMARY KEY, val INT4)").await;

    ctx.batch_execute(
        "DO $$
         DECLARE
             i INT;
         BEGIN
             FOR i IN 1..10 LOOP
                 INSERT INTO do_loop (val) VALUES (i * i);
             END LOOP;
         END $$",
    )
    .await;

    let row = ctx.query_one("SELECT COUNT(*)::INT4 AS cnt FROM do_loop", &[]).await;
    assert_eq!(row["cnt"], 10);

    // Verify first and last squares
    let row = ctx.query_one("SELECT val FROM do_loop ORDER BY id LIMIT 1", &[]).await;
    assert_eq!(row["val"], 1); // 1*1

    let row = ctx.query_one("SELECT val FROM do_loop ORDER BY id DESC LIMIT 1", &[]).await;
    assert_eq!(row["val"], 100); // 10*10

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_create_function_and_call() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE FUNCTION add_numbers(a INT4, b INT4) RETURNS INT4 AS $$
         BEGIN
             RETURN a + b;
         END;
         $$ LANGUAGE plpgsql",
    )
    .await;

    let row = ctx.query_one("SELECT add_numbers(3, 7) AS result", &[]).await;
    assert_eq!(row["result"], 10);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_create_function_with_params() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE FUNCTION greet(name TEXT) RETURNS TEXT AS $$
         BEGIN
             RETURN 'Hello, ' || name || '!';
         END;
         $$ LANGUAGE plpgsql",
    )
    .await;

    let row = ctx.query_one("SELECT greet($1::TEXT) AS greeting", &[SqlParam::Text("World".to_string())]).await;
    assert_eq!(row["greeting"], "Hello, World!");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_function_returning_table() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE fn_data (id SERIAL PRIMARY KEY, category TEXT, val INT4);
         INSERT INTO fn_data (category, val) VALUES ('a', 10), ('a', 20), ('b', 30)",
    )
    .await;

    ctx.batch_execute(
        "CREATE FUNCTION get_by_category(cat TEXT)
         RETURNS TABLE(id INT4, val INT4) AS $$
         BEGIN
             RETURN QUERY SELECT fn_data.id, fn_data.val FROM fn_data WHERE fn_data.category = cat ORDER BY fn_data.id;
         END;
         $$ LANGUAGE plpgsql",
    )
    .await;

    let rows = ctx.query("SELECT * FROM get_by_category('a')", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["val"], 10);
    assert_eq!(arr[1]["val"], 20);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_trigger_function() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE trig_test (id SERIAL PRIMARY KEY, val TEXT, updated_at TIMESTAMP);

         CREATE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
         BEGIN
             NEW.updated_at = '2024-01-01 00:00:00'::TIMESTAMP;
             RETURN NEW;
         END;
         $$ LANGUAGE plpgsql;

         CREATE TRIGGER trig_before_insert
             BEFORE INSERT ON trig_test
             FOR EACH ROW EXECUTE FUNCTION set_updated_at()",
    )
    .await;

    ctx.execute("INSERT INTO trig_test (val) VALUES ($1)", &[SqlParam::Text("test".to_string())]).await;

    let row = ctx.query_one("SELECT val, updated_at::TEXT AS ts FROM trig_test WHERE id = 1", &[]).await;
    assert_eq!(row["val"], "test");
    assert_eq!(row["ts"], "2024-01-01 00:00:00");

    ctx.stop().await;
}
