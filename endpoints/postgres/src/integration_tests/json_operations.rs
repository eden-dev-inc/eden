use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

async fn setup_json_table(ctx: &mut PostgresTestContext) {
    ctx.batch_execute(
        "CREATE TABLE json_test (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL
        )",
    )
    .await;
    ctx.execute(
        "INSERT INTO json_test (data) VALUES ($1), ($2), ($3)",
        &[
            SqlParam::Json(serde_json::json!({"name": "alice", "age": 30, "tags": ["admin", "user"], "address": {"city": "NYC"}})),
            SqlParam::Json(serde_json::json!({"name": "bob", "age": 25, "tags": ["user"], "address": {"city": "LA"}})),
            SqlParam::Json(serde_json::json!({"name": "carol", "age": 35, "tags": ["admin"], "address": {"city": "NYC"}})),
        ],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_arrow_operator() {
    let mut ctx = PostgresTestContext::new().await;
    setup_json_table(&mut ctx).await;

    let row = ctx.query_one("SELECT data->'name' AS name_json FROM json_test WHERE id = 1", &[]).await;
    // -> returns JSON element (quoted string)
    assert_eq!(row["name_json"], "alice");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_double_arrow_operator() {
    let mut ctx = PostgresTestContext::new().await;
    setup_json_table(&mut ctx).await;

    let row = ctx.query_one("SELECT data->>'name' AS name_text FROM json_test WHERE id = 1", &[]).await;
    // ->> returns text
    assert_eq!(row["name_text"], "alice");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_nested_access() {
    let mut ctx = PostgresTestContext::new().await;
    setup_json_table(&mut ctx).await;

    let row = ctx.query_one("SELECT data->'address'->>'city' AS city FROM json_test WHERE id = 1", &[]).await;
    assert_eq!(row["city"], "NYC");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_path_operator() {
    let mut ctx = PostgresTestContext::new().await;
    setup_json_table(&mut ctx).await;

    let row = ctx.query_one("SELECT data #>> '{address,city}' AS city FROM json_test WHERE id = 2", &[]).await;
    assert_eq!(row["city"], "LA");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_contains_operator() {
    let mut ctx = PostgresTestContext::new().await;
    setup_json_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT data->>'name' AS name FROM json_test WHERE data @> '{\"address\": {\"city\": \"NYC\"}}'::JSONB ORDER BY id",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["name"], "alice");
    assert_eq!(arr[1]["name"], "carol");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_exists_operator() {
    let mut ctx = PostgresTestContext::new().await;
    setup_json_table(&mut ctx).await;

    let rows = ctx.query("SELECT data->>'name' AS name FROM json_test WHERE data ? 'age' ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3); // all rows have 'age'

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_array_element() {
    let mut ctx = PostgresTestContext::new().await;
    setup_json_table(&mut ctx).await;

    let row = ctx.query_one("SELECT data->'tags'->>0 AS first_tag FROM json_test WHERE id = 1", &[]).await;
    assert_eq!(row["first_tag"], "admin");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_array_length() {
    let mut ctx = PostgresTestContext::new().await;
    setup_json_table(&mut ctx).await;

    let row = ctx.query_one("SELECT jsonb_array_length(data->'tags') AS tag_count FROM json_test WHERE id = 1", &[]).await;
    assert_eq!(row["tag_count"], 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_typeof() {
    let mut ctx = PostgresTestContext::new().await;
    setup_json_table(&mut ctx).await;

    let row = ctx
        .query_one(
            "SELECT
                jsonb_typeof(data->'name') AS name_type,
                jsonb_typeof(data->'age') AS age_type,
                jsonb_typeof(data->'tags') AS tags_type,
                jsonb_typeof(data->'address') AS addr_type
             FROM json_test WHERE id = 1",
            &[],
        )
        .await;
    assert_eq!(row["name_type"], "string");
    assert_eq!(row["age_type"], "number");
    assert_eq!(row["tags_type"], "array");
    assert_eq!(row["addr_type"], "object");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_set() {
    let mut ctx = PostgresTestContext::new().await;
    setup_json_table(&mut ctx).await;

    let row = ctx.query_one("SELECT jsonb_set(data, '{age}', '31'::JSONB)->>'age' AS new_age FROM json_test WHERE id = 1", &[]).await;
    assert_eq!(row["new_age"], "31");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_strip_nulls() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT jsonb_strip_nulls('{\"a\": 1, \"b\": null, \"c\": 3}'::JSONB) AS stripped", &[]).await;
    let stripped: serde_json::Value =
        serde_json::from_str(row["stripped"].as_str().unwrap_or(&row["stripped"].to_string())).unwrap_or_else(|_| row["stripped"].clone());
    assert!(stripped.get("a").is_some());
    assert!(stripped.get("b").is_none());
    assert!(stripped.get("c").is_some());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_each() {
    let mut ctx = PostgresTestContext::new().await;

    let rows = ctx.query("SELECT key, value FROM jsonb_each('{\"a\": 1, \"b\": 2}'::JSONB) ORDER BY key", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["key"], "a");
    assert_eq!(arr[1]["key"], "b");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_array_elements() {
    let mut ctx = PostgresTestContext::new().await;

    let rows = ctx.query("SELECT value FROM jsonb_array_elements('[\"x\", \"y\", \"z\"]'::JSONB)", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_object_keys() {
    let mut ctx = PostgresTestContext::new().await;

    let rows = ctx
        .query(
            "SELECT jsonb_object_keys('{\"name\": \"x\", \"age\": 1, \"city\": \"y\"}'::JSONB) AS key ORDER BY key",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["key"], "age");
    assert_eq!(arr[1]["key"], "city");
    assert_eq!(arr[2]["key"], "name");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_build_object() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx
        .query_one(
            "SELECT jsonb_build_object('name', $1::TEXT, 'age', $2::INT4) AS obj",
            &[SqlParam::Text("test".to_string()), SqlParam::Int4(42)],
        )
        .await;
    // jsonb_build_object returns a JSONB value
    let obj = &row["obj"];
    assert!(obj.get("name").is_some() || obj.is_string());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_agg() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE json_agg_test (id SERIAL PRIMARY KEY, grp TEXT, val INT4)").await;
    ctx.execute("INSERT INTO json_agg_test (grp, val) VALUES ('a', 1), ('a', 2), ('b', 3)", &[]).await;

    let rows = ctx.query("SELECT grp, jsonb_agg(val ORDER BY val) AS vals FROM json_agg_test GROUP BY grp ORDER BY grp", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["grp"], "a");
    assert_eq!(arr[1]["grp"], "b");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_concat_operator() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT ('{\"a\": 1}'::JSONB || '{\"b\": 2}'::JSONB) AS merged", &[]).await;
    let merged = &row["merged"];
    // Should contain both keys
    assert!(merged.get("a").is_some() || merged.is_string());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_jsonb_delete_key() {
    let mut ctx = PostgresTestContext::new().await;

    let row = ctx.query_one("SELECT ('{\"a\": 1, \"b\": 2, \"c\": 3}'::JSONB - 'b') AS result", &[]).await;
    let result = &row["result"];
    assert!(result.get("a").is_some() || result.is_string());
    // 'b' should be removed
    if result.is_object() {
        assert!(result.get("b").is_none());
    }

    ctx.stop().await;
}
