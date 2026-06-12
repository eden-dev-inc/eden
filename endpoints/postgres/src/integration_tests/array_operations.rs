use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_literal() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_test (id SERIAL PRIMARY KEY, vals INT4[])").await;
    ctx.execute("INSERT INTO arr_test (vals) VALUES (ARRAY[1, 2, 3]), (ARRAY[4, 5])", &[]).await;

    let rows = ctx.query("SELECT vals FROM arr_test ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_access() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT (ARRAY['a','b','c'])[1] AS first, (ARRAY['a','b','c'])[3] AS third", &[]).await;
    assert_eq!(row["first"], "a");
    assert_eq!(row["third"], "c");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_length_func() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT array_length(ARRAY[10, 20, 30, 40], 1) AS len", &[]).await;
    assert_eq!(row["len"], 4);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_append_prepend() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                array_append(ARRAY[1, 2], 3) AS appended,
                array_prepend(0, ARRAY[1, 2]) AS prepended",
            &[],
        )
        .await;
    // These return arrays
    assert!(row["appended"].is_array());
    assert!(row["prepended"].is_array());

    let appended = row["appended"].as_array().unwrap();
    assert_eq!(appended.len(), 3);
    assert_eq!(appended[2], 3);

    let prepended = row["prepended"].as_array().unwrap();
    assert_eq!(prepended.len(), 3);
    assert_eq!(prepended[0], 0);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_cat() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT array_cat(ARRAY[1, 2], ARRAY[3, 4]) AS combined", &[]).await;
    let combined = row["combined"].as_array().unwrap();
    assert_eq!(combined.len(), 4);
    assert_eq!(combined[0], 1);
    assert_eq!(combined[3], 4);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_remove() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT array_remove(ARRAY[1, 2, 3, 2, 1], 2) AS removed", &[]).await;
    let removed = row["removed"].as_array().unwrap();
    assert_eq!(removed.len(), 3); // [1, 3, 1]
    for v in removed {
        assert_ne!(v, &serde_json::json!(2));
    }

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_position() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT array_position(ARRAY['a','b','c','d'], 'c') AS pos", &[]).await;
    assert_eq!(row["pos"], 3);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_unnest() {
    let mut ctx = PostgresTestContext::new().await;

    let rows = ctx.query("SELECT unnest(ARRAY['x', 'y', 'z']) AS val", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["val"], "x");
    assert_eq!(arr[1]["val"], "y");
    assert_eq!(arr[2]["val"], "z");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_agg() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_agg_test (id SERIAL PRIMARY KEY, grp TEXT, val TEXT)").await;
    ctx.execute("INSERT INTO arr_agg_test (grp, val) VALUES ('a', 'x'), ('a', 'y'), ('b', 'z')", &[]).await;

    let rows = ctx.query("SELECT grp, array_agg(val ORDER BY val) AS vals FROM arr_agg_test GROUP BY grp ORDER BY grp", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["grp"], "a");
    let a_vals = arr[0]["vals"].as_array().unwrap();
    assert_eq!(a_vals.len(), 2);
    assert_eq!(a_vals[0], "x");
    assert_eq!(a_vals[1], "y");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_any_all_with_array() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_any (id SERIAL PRIMARY KEY, name TEXT, score INT4)").await;
    ctx.execute("INSERT INTO arr_any (name, score) VALUES ('a', 10), ('b', 20), ('c', 30), ('d', 40)", &[]).await;

    // ANY: matches any element in the array
    let rows = ctx.query("SELECT name FROM arr_any WHERE score = ANY(ARRAY[10, 30]) ORDER BY name", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["name"], "a");
    assert_eq!(arr[1]["name"], "c");

    // ALL: must satisfy all elements
    let rows = ctx.query("SELECT name FROM arr_any WHERE score > ALL(ARRAY[10, 20]) ORDER BY name", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2); // 30, 40
    assert_eq!(arr[0]["name"], "c");
    assert_eq!(arr[1]["name"], "d");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_contains_overlap() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE arr_ops (id SERIAL PRIMARY KEY, tags TEXT[])").await;
    ctx.execute(
        "INSERT INTO arr_ops (tags) VALUES (ARRAY['rust','go']), (ARRAY['python','rust']), (ARRAY['java'])",
        &[],
    )
    .await;

    // @> contains
    let rows = ctx.query("SELECT id FROM arr_ops WHERE tags @> ARRAY['rust'] ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    // && overlap
    let rows = ctx.query("SELECT id FROM arr_ops WHERE tags && ARRAY['go', 'java'] ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2); // row 1 has 'go', row 3 has 'java'

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_slice() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT (ARRAY[10, 20, 30, 40, 50])[2:4] AS sliced", &[]).await;
    let sliced = row["sliced"].as_array().unwrap();
    assert_eq!(sliced.len(), 3); // [20, 30, 40]
    assert_eq!(sliced[0], 20);
    assert_eq!(sliced[2], 40);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_array_dims_cardinality() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT
                cardinality(ARRAY[1, 2, 3]) AS card,
                array_ndims(ARRAY[[1,2],[3,4]]) AS ndims",
            &[],
        )
        .await;
    assert_eq!(row["card"], 3);
    assert_eq!(row["ndims"], 2);

    ctx.stop().await;
}
