use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

async fn setup_select_table(ctx: &mut PostgresTestContext) {
    ctx.batch_execute(
        "CREATE TABLE sel_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT,
            value INT4,
            active BOOLEAN DEFAULT true
        )",
    )
    .await;
    ctx.execute(
        "INSERT INTO sel_test (name, category, value, active) VALUES
         ('alice', 'eng', 100, true),
         ('bob', 'eng', 200, true),
         ('carol', 'sales', 150, false),
         ('dave', 'sales', 300, true),
         ('eve', 'eng', 250, true)",
        &[],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_select_all() {
    let mut ctx = PostgresTestContext::new().await;
    setup_select_table(&mut ctx).await;

    let rows = ctx.query("SELECT * FROM sel_test", &[]).await;
    assert!(rows.is_array());
    assert_eq!(rows.as_array().unwrap().len(), 5);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_select_specific_columns() {
    let mut ctx = PostgresTestContext::new().await;
    setup_select_table(&mut ctx).await;

    let rows = ctx.query("SELECT name, value FROM sel_test", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 5);
    // Each row should only have name and value keys
    let first = &arr[0];
    assert!(first.get("name").is_some());
    assert!(first.get("value").is_some());
    assert!(first.get("category").is_none());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_select_with_alias() {
    let mut ctx = PostgresTestContext::new().await;
    setup_select_table(&mut ctx).await;

    let row = ctx.query_one("SELECT name AS employee_name, value AS salary FROM sel_test WHERE name = 'alice'", &[]).await;
    assert_eq!(row["employee_name"], "alice");
    assert_eq!(row["salary"], 100);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_select_where_equals() {
    let mut ctx = PostgresTestContext::new().await;
    setup_select_table(&mut ctx).await;

    let row = ctx.query_one("SELECT name, value FROM sel_test WHERE name = $1", &[SqlParam::Text("bob".to_string())]).await;
    assert_eq!(row["name"], "bob");
    assert_eq!(row["value"], 200);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_select_where_comparison() {
    let mut ctx = PostgresTestContext::new().await;
    setup_select_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name FROM sel_test WHERE value > $1 AND value < $2 ORDER BY name",
            &[SqlParam::Int4(100), SqlParam::Int4(300)],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // Values: alice=100, bob=200, carol=150, dave=300, eve=250
    // value > 100 AND value < 300 → bob(200), carol(150), eve(250)
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["name"], "bob");
    assert_eq!(arr[1]["name"], "carol");
    assert_eq!(arr[2]["name"], "eve");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_select_where_like() {
    let mut ctx = PostgresTestContext::new().await;
    setup_select_table(&mut ctx).await;

    let rows = ctx.query("SELECT name FROM sel_test WHERE name LIKE $1 ORDER BY name", &[SqlParam::Text("%a%".to_string())]).await;
    // alice, carol, dave all contain 'a'
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_select_where_in() {
    let mut ctx = PostgresTestContext::new().await;
    setup_select_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name FROM sel_test WHERE name IN ($1, $2) ORDER BY name",
            &[SqlParam::Text("alice".to_string()), SqlParam::Text("eve".to_string())],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["name"], "alice");
    assert_eq!(arr[1]["name"], "eve");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_select_where_between() {
    let mut ctx = PostgresTestContext::new().await;
    setup_select_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name FROM sel_test WHERE value BETWEEN $1 AND $2 ORDER BY value",
            &[SqlParam::Int4(150), SqlParam::Int4(250)],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3); // carol=150, bob=200, eve=250

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_select_where_is_null() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE sel_nulls (id SERIAL PRIMARY KEY, val TEXT)").await;
    ctx.execute("INSERT INTO sel_nulls (val) VALUES ('a'), (NULL), ('c'), (NULL)", &[]).await;

    let rows = ctx.query("SELECT id FROM sel_nulls WHERE val IS NULL", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    let rows = ctx.query("SELECT id FROM sel_nulls WHERE val IS NOT NULL", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_select_order_by_asc_desc() {
    let mut ctx = PostgresTestContext::new().await;
    setup_select_table(&mut ctx).await;

    let rows = ctx.query("SELECT name, value FROM sel_test ORDER BY value ASC", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["value"], 100);
    assert_eq!(arr[4]["value"], 300);

    let rows = ctx.query("SELECT name, value FROM sel_test ORDER BY value DESC", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["value"], 300);
    assert_eq!(arr[4]["value"], 100);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_select_limit_offset() {
    let mut ctx = PostgresTestContext::new().await;
    setup_select_table(&mut ctx).await;

    let rows = ctx.query("SELECT name FROM sel_test ORDER BY id LIMIT 2 OFFSET 1", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["name"], "bob");
    assert_eq!(arr[1]["name"], "carol");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_select_distinct() {
    let mut ctx = PostgresTestContext::new().await;
    setup_select_table(&mut ctx).await;

    let rows = ctx.query("SELECT DISTINCT category FROM sel_test ORDER BY category", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["category"], "eng");
    assert_eq!(arr[1]["category"], "sales");

    ctx.stop().await;
}
