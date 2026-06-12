use crate::api::wrapper::input::SqlParam;
use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

async fn setup_subquery_tables(ctx: &mut PostgresTestContext) {
    ctx.batch_execute(
        "CREATE TABLE sq_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL, price INT4 NOT NULL, category TEXT NOT NULL);
         CREATE TABLE sq_orders (id SERIAL PRIMARY KEY, product_id INT REFERENCES sq_products(id), quantity INT4 NOT NULL)",
    )
    .await;
    ctx.execute(
        "INSERT INTO sq_products (name, price, category) VALUES
         ('Widget', 10, 'A'),
         ('Gadget', 25, 'A'),
         ('Doohickey', 50, 'B'),
         ('Thingamajig', 100, 'B'),
         ('Whatsit', 5, 'C')",
        &[],
    )
    .await;
    ctx.execute(
        "INSERT INTO sq_orders (product_id, quantity) VALUES
         (1, 100), (2, 50), (3, 10)",
        &[],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_subquery_in_where() {
    let mut ctx = PostgresTestContext::new().await;
    setup_subquery_tables(&mut ctx).await;

    let rows = ctx.query("SELECT name FROM sq_products WHERE id IN (SELECT product_id FROM sq_orders) ORDER BY name", &[]).await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_subquery_exists() {
    let mut ctx = PostgresTestContext::new().await;
    setup_subquery_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name FROM sq_products p WHERE EXISTS (SELECT 1 FROM sq_orders o WHERE o.product_id = p.id) ORDER BY name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_subquery_not_exists() {
    let mut ctx = PostgresTestContext::new().await;
    setup_subquery_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name FROM sq_products p WHERE NOT EXISTS (SELECT 1 FROM sq_orders o WHERE o.product_id = p.id) ORDER BY name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // Thingamajig and Whatsit have no orders
    assert_eq!(arr.len(), 2);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_scalar_subquery() {
    let mut ctx = PostgresTestContext::new().await;
    setup_subquery_tables(&mut ctx).await;

    let row = ctx
        .query_one(
            "SELECT (SELECT COUNT(*) FROM sq_orders) AS order_count, (SELECT COUNT(*) FROM sq_products) AS product_count",
            &[],
        )
        .await;
    assert_eq!(row["order_count"], 3);
    assert_eq!(row["product_count"], 5);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_correlated_subquery() {
    let mut ctx = PostgresTestContext::new().await;
    setup_subquery_tables(&mut ctx).await;

    // Products priced above the average of their category
    let rows = ctx
        .query(
            "SELECT p.name, p.price FROM sq_products p
             WHERE p.price > (SELECT AVG(p2.price) FROM sq_products p2 WHERE p2.category = p.category)
             ORDER BY p.name",
            &[],
        )
        .await;
    // Category A: avg=17.5 -> Gadget(25) qualifies
    // Category B: avg=75 -> Thingamajig(100) qualifies
    // Category C: avg=5 -> none qualifies
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["name"], "Gadget");
    assert_eq!(arr[1]["name"], "Thingamajig");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_subquery_in_from() {
    let mut ctx = PostgresTestContext::new().await;
    setup_subquery_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT subq.category, subq.avg_price
             FROM (SELECT category, AVG(price)::FLOAT8 AS avg_price FROM sq_products GROUP BY category) AS subq
             ORDER BY subq.category",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["category"], "A");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_subquery_with_any() {
    let mut ctx = PostgresTestContext::new().await;
    setup_subquery_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name, price FROM sq_products WHERE price > ANY (SELECT price FROM sq_products WHERE category = $1) ORDER BY name",
            &[SqlParam::Text("A".to_string())],
        )
        .await;
    // Category A prices: 10, 25. price > ANY means price > 10
    let arr = rows.as_array().unwrap();
    // Gadget(25), Doohickey(50), Thingamajig(100) qualify
    assert_eq!(arr.len(), 3);

    ctx.stop().await;
}
