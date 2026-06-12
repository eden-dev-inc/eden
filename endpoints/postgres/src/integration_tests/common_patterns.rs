use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

async fn setup_pattern_table(ctx: &mut PostgresTestContext) {
    ctx.batch_execute(
        "CREATE TABLE pat_items (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price INT4 NOT NULL,
            active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT NOW()
        )",
    )
    .await;
    ctx.execute(
        "INSERT INTO pat_items (name, category, price, active) VALUES
         ('Widget A', 'tools', 100, true),
         ('Widget B', 'tools', 200, true),
         ('Gadget X', 'electronics', 300, false),
         ('Gadget Y', 'electronics', 150, true),
         ('Part Z', 'tools', 50, true),
         ('Part W', 'parts', 75, true),
         ('Gizmo Q', 'electronics', 500, true),
         ('Doohickey', 'parts', 25, false)",
        &[],
    )
    .await;
}

// --- Pagination patterns ---

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_offset_limit_pagination() {
    let mut ctx = PostgresTestContext::new().await;
    setup_pattern_table(&mut ctx).await;

    // Page 1 (first 3)
    let rows = ctx.query("SELECT name FROM pat_items ORDER BY id LIMIT 3 OFFSET 0", &[]).await;
    let page1 = rows.as_array().unwrap();
    assert_eq!(page1.len(), 3);
    assert_eq!(page1[0]["name"], "Widget A");

    // Page 2 (next 3)
    let rows = ctx.query("SELECT name FROM pat_items ORDER BY id LIMIT 3 OFFSET 3", &[]).await;
    let page2 = rows.as_array().unwrap();
    assert_eq!(page2.len(), 3);
    assert_eq!(page2[0]["name"], "Gadget Y");

    // Page 3 (remaining 2)
    let rows = ctx.query("SELECT name FROM pat_items ORDER BY id LIMIT 3 OFFSET 6", &[]).await;
    let page3 = rows.as_array().unwrap();
    assert_eq!(page3.len(), 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_keyset_pagination() {
    let mut ctx = PostgresTestContext::new().await;
    setup_pattern_table(&mut ctx).await;

    // Keyset/cursor pagination: more efficient than OFFSET for large datasets
    let rows = ctx.query("SELECT id, name FROM pat_items WHERE id > $1 ORDER BY id LIMIT 3", &[SqlParam::Int4(0)]).await;
    let page1 = rows.as_array().unwrap();
    assert_eq!(page1.len(), 3);

    // Use last ID from page 1 as cursor
    let last_id = page1[2]["id"].as_i64().unwrap() as i32;
    let rows = ctx.query("SELECT id, name FROM pat_items WHERE id > $1 ORDER BY id LIMIT 3", &[SqlParam::Int4(last_id)]).await;
    let page2 = rows.as_array().unwrap();
    assert_eq!(page2.len(), 3);
    // First item of page 2 should be right after last item of page 1
    assert!(page2[0]["id"].as_i64().unwrap() > last_id as i64);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_count_with_pagination() {
    let mut ctx = PostgresTestContext::new().await;
    setup_pattern_table(&mut ctx).await;

    // Common pattern: get total count + paginated data
    let row = ctx.query_one("SELECT COUNT(*)::INT4 AS total FROM pat_items WHERE active = true", &[]).await;
    assert_eq!(row["total"], 6);

    let rows = ctx.query("SELECT name FROM pat_items WHERE active = true ORDER BY name LIMIT 3", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);

    ctx.stop().await;
}

// --- Upsert patterns ---

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_upsert_on_conflict_do_update() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE pat_upsert (
            key TEXT PRIMARY KEY,
            value INT4 NOT NULL,
            version INT4 DEFAULT 1
        )",
    )
    .await;

    ctx.execute(
        "INSERT INTO pat_upsert (key, value) VALUES ($1, $2)",
        &[SqlParam::Text("k1".to_string()), SqlParam::Int4(100)],
    )
    .await;

    // Upsert: on conflict update value and increment version
    ctx.execute(
        "INSERT INTO pat_upsert (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, version = pat_upsert.version + 1",
        &[SqlParam::Text("k1".to_string()), SqlParam::Int4(200)],
    )
    .await;

    let row = ctx.query_one("SELECT value, version FROM pat_upsert WHERE key = 'k1'", &[]).await;
    assert_eq!(row["value"], 200);
    assert_eq!(row["version"], 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_upsert_with_where_clause() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE pat_cond_upsert (
            key TEXT PRIMARY KEY,
            value INT4 NOT NULL,
            locked BOOLEAN DEFAULT false
        )",
    )
    .await;

    ctx.execute("INSERT INTO pat_cond_upsert VALUES ('k1', 100, true), ('k2', 200, false)", &[]).await;

    // Only update if not locked
    ctx.execute(
        "INSERT INTO pat_cond_upsert (key, value) VALUES ('k1', 999)
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
         WHERE NOT pat_cond_upsert.locked",
        &[],
    )
    .await;

    // k1 is locked, should keep old value
    let row = ctx.query_one("SELECT value FROM pat_cond_upsert WHERE key = 'k1'", &[]).await;
    assert_eq!(row["value"], 100);

    // k2 is not locked, should update
    ctx.execute(
        "INSERT INTO pat_cond_upsert (key, value) VALUES ('k2', 999)
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
         WHERE NOT pat_cond_upsert.locked",
        &[],
    )
    .await;

    let row = ctx.query_one("SELECT value FROM pat_cond_upsert WHERE key = 'k2'", &[]).await;
    assert_eq!(row["value"], 999);

    ctx.stop().await;
}

// --- Search patterns ---

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_full_text_search_basic() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE pat_fts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', title || ' ' || body)) STORED
        )",
    )
    .await;
    ctx.execute(
        "INSERT INTO pat_fts (title, body) VALUES
         ('PostgreSQL Tutorial', 'Learn how to use PostgreSQL database'),
         ('Redis Guide', 'An introduction to Redis caching'),
         ('SQL Optimization', 'Tips for optimizing PostgreSQL queries')",
        &[],
    )
    .await;

    let rows = ctx.query("SELECT title FROM pat_fts WHERE tsv @@ to_tsquery('english', 'postgresql') ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["title"], "PostgreSQL Tutorial");
    assert_eq!(arr[1]["title"], "SQL Optimization");

    ctx.stop().await;
}

// --- Soft delete pattern ---

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_soft_delete_pattern() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE pat_soft (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            deleted_at TIMESTAMP DEFAULT NULL
        )",
    )
    .await;
    ctx.execute("INSERT INTO pat_soft (name) VALUES ('keep'), ('remove'), ('keep2')", &[]).await;

    // Soft delete
    ctx.execute("UPDATE pat_soft SET deleted_at = NOW() WHERE name = 'remove'", &[]).await;

    // Query only active records
    let rows = ctx.query("SELECT name FROM pat_soft WHERE deleted_at IS NULL ORDER BY id", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["name"], "keep");
    assert_eq!(arr[1]["name"], "keep2");

    // Query deleted records
    let row = ctx.query_one("SELECT name FROM pat_soft WHERE deleted_at IS NOT NULL", &[]).await;
    assert_eq!(row["name"], "remove");

    ctx.stop().await;
}

// --- Recursive delete pattern ---

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cascade_delete() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE pat_parent (id SERIAL PRIMARY KEY, name TEXT);
         CREATE TABLE pat_child (
             id SERIAL PRIMARY KEY,
             parent_id INT4 REFERENCES pat_parent(id) ON DELETE CASCADE,
             val TEXT
         )",
    )
    .await;
    ctx.execute("INSERT INTO pat_parent (name) VALUES ('p1'), ('p2')", &[]).await;
    ctx.execute("INSERT INTO pat_child (parent_id, val) VALUES (1, 'c1'), (1, 'c2'), (2, 'c3')", &[]).await;

    // Delete parent cascades to children
    ctx.execute("DELETE FROM pat_parent WHERE id = 1", &[]).await;

    let row = ctx.query_one("SELECT COUNT(*)::INT4 AS cnt FROM pat_child", &[]).await;
    assert_eq!(row["cnt"], 1); // only c3 remains

    let row = ctx.query_one("SELECT val FROM pat_child", &[]).await;
    assert_eq!(row["val"], "c3");

    ctx.stop().await;
}

// --- Bulk operations ---

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_bulk_insert_with_generate_series() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE pat_bulk (id SERIAL PRIMARY KEY, val INT4)").await;
    ctx.execute("INSERT INTO pat_bulk (val) SELECT g * 10 FROM generate_series(1, 100) AS g", &[]).await;

    let row = ctx.query_one("SELECT COUNT(*)::INT4 AS cnt, MIN(val) AS min_val, MAX(val) AS max_val FROM pat_bulk", &[]).await;
    assert_eq!(row["cnt"], 100);
    assert_eq!(row["min_val"], 10);
    assert_eq!(row["max_val"], 1000);

    ctx.stop().await;
}
