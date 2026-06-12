use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

async fn setup_advanced_tables(ctx: &mut PostgresTestContext) {
    ctx.batch_execute(
        "CREATE TABLE adv_employees (id SERIAL PRIMARY KEY, name TEXT NOT NULL, department TEXT, salary INT4);
         INSERT INTO adv_employees (name, department, salary) VALUES
         ('alice', 'eng', 100), ('bob', 'eng', 200), ('carol', 'sales', 150),
         ('dave', 'sales', 300), ('eve', 'eng', 250)",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cte_basic() {
    let mut ctx = PostgresTestContext::new().await;
    setup_advanced_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "WITH eng AS (SELECT name, salary FROM adv_employees WHERE department = 'eng')
             SELECT name FROM eng ORDER BY name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["name"], "alice");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cte_multiple() {
    let mut ctx = PostgresTestContext::new().await;
    setup_advanced_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "WITH
                eng AS (SELECT name, salary FROM adv_employees WHERE department = 'eng'),
                high_earners AS (SELECT name FROM eng WHERE salary > 150)
             SELECT name FROM high_earners ORDER BY name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["name"], "bob");
    assert_eq!(arr[1]["name"], "eve");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cte_recursive() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE adv_tree (id INT PRIMARY KEY, name TEXT, parent_id INT REFERENCES adv_tree(id));
         INSERT INTO adv_tree (id, name, parent_id) VALUES
         (1, 'root', NULL), (2, 'child1', 1), (3, 'child2', 1),
         (4, 'grandchild1', 2), (5, 'grandchild2', 2)",
    )
    .await;

    let rows = ctx
        .query(
            "WITH RECURSIVE tree AS (
                SELECT id, name, parent_id, 0 AS depth FROM adv_tree WHERE parent_id IS NULL
                UNION ALL
                SELECT t.id, t.name, t.parent_id, tree.depth + 1 FROM adv_tree t JOIN tree ON t.parent_id = tree.id
             )
             SELECT name, depth FROM tree ORDER BY depth, name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 5);
    assert_eq!(arr[0]["name"], "root");
    assert_eq!(arr[0]["depth"], 0);
    assert_eq!(arr[1]["depth"], 1); // child1 or child2

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_union() {
    let mut ctx = PostgresTestContext::new().await;
    setup_advanced_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name FROM adv_employees WHERE department = 'eng'
             UNION
             SELECT name FROM adv_employees WHERE salary > 200
             ORDER BY name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // eng: alice, bob, eve. salary>200: dave, eve. UNION deduplicates eve
    assert_eq!(arr.len(), 4);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_union_all() {
    let mut ctx = PostgresTestContext::new().await;
    setup_advanced_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name FROM adv_employees WHERE department = 'eng'
             UNION ALL
             SELECT name FROM adv_employees WHERE salary > 200
             ORDER BY name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // eng: alice, bob, eve. salary>200: dave, eve. UNION ALL keeps duplicates: 5 rows
    assert_eq!(arr.len(), 5);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_intersect() {
    let mut ctx = PostgresTestContext::new().await;
    setup_advanced_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name FROM adv_employees WHERE department = 'eng'
             INTERSECT
             SELECT name FROM adv_employees WHERE salary > 200",
            &[],
        )
        .await;
    // Only eve is in eng AND has salary > 200
    assert_eq!(rows["name"], "eve");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_except() {
    let mut ctx = PostgresTestContext::new().await;
    setup_advanced_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name FROM adv_employees WHERE department = 'eng'
             EXCEPT
             SELECT name FROM adv_employees WHERE salary > 200
             ORDER BY name",
            &[],
        )
        .await;
    // eng: alice, bob, eve. salary>200: dave, eve. EXCEPT removes eve
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["name"], "alice");
    assert_eq!(arr[1]["name"], "bob");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_window_row_number() {
    let mut ctx = PostgresTestContext::new().await;
    setup_advanced_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
             FROM adv_employees ORDER BY rn",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 5);
    assert_eq!(arr[0]["rn"], 1);
    assert_eq!(arr[0]["name"], "dave"); // highest salary=300

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_window_rank_dense_rank() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE adv_scores (id SERIAL PRIMARY KEY, name TEXT, score INT4);
         INSERT INTO adv_scores (name, score) VALUES
         ('a', 100), ('b', 90), ('c', 90), ('d', 80)",
    )
    .await;

    let rows = ctx
        .query(
            "SELECT name, score,
                RANK() OVER (ORDER BY score DESC) AS rank,
                DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank
             FROM adv_scores ORDER BY score DESC, name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["rank"], 1);
    assert_eq!(arr[0]["dense_rank"], 1);
    // b and c tie at rank 2
    assert_eq!(arr[1]["rank"], 2);
    assert_eq!(arr[2]["rank"], 2);
    // d is rank 4 (not 3) with RANK, but dense_rank 3
    assert_eq!(arr[3]["rank"], 4);
    assert_eq!(arr[3]["dense_rank"], 3);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_window_running_total() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE adv_sales (id SERIAL PRIMARY KEY, month INT4, amount INT4);
         INSERT INTO adv_sales (month, amount) VALUES (1, 100), (2, 200), (3, 150), (4, 300)",
    )
    .await;

    let rows = ctx
        .query(
            "SELECT month, amount,
                SUM(amount) OVER (ORDER BY month) AS running_total
             FROM adv_sales ORDER BY month",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr[0]["running_total"], 100);
    assert_eq!(arr[1]["running_total"], 300);
    assert_eq!(arr[2]["running_total"], 450);
    assert_eq!(arr[3]["running_total"], 750);

    ctx.stop().await;
}
