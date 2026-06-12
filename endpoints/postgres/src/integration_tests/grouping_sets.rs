use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

async fn setup_grouping_table(ctx: &mut PostgresTestContext) {
    ctx.batch_execute(
        "CREATE TABLE gs_test (
            id SERIAL PRIMARY KEY,
            region TEXT NOT NULL,
            product TEXT NOT NULL,
            amount INT4 NOT NULL
        )",
    )
    .await;
    ctx.execute(
        "INSERT INTO gs_test (region, product, amount) VALUES
         ('east', 'widget', 100),
         ('east', 'gadget', 200),
         ('west', 'widget', 150),
         ('west', 'gadget', 250),
         ('east', 'widget', 50)",
        &[],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_grouping_sets() {
    let mut ctx = PostgresTestContext::new().await;
    setup_grouping_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT region, product, SUM(amount)::INT4 AS total
             FROM gs_test
             GROUP BY GROUPING SETS ((region), (product), ())
             ORDER BY region NULLS LAST, product NULLS LAST",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // Rows: (east, NULL, 350), (west, NULL, 400), (NULL, gadget, 450),
    //       (NULL, widget, 300), (NULL, NULL, 750)
    assert_eq!(arr.len(), 5);

    // Grand total row (both null)
    let grand = arr.iter().find(|r| r["region"].is_null() && r["product"].is_null()).unwrap();
    assert_eq!(grand["total"], 750);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_rollup() {
    let mut ctx = PostgresTestContext::new().await;
    setup_grouping_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT region, product, SUM(amount)::INT4 AS total
             FROM gs_test
             GROUP BY ROLLUP (region, product)
             ORDER BY region NULLS LAST, product NULLS LAST",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // ROLLUP(region, product) gives: (region, product), (region, NULL), (NULL, NULL)
    // east+gadget=200, east+widget=150, east total=350
    // west+gadget=250, west+widget=150, west total=400
    // grand total=750
    assert_eq!(arr.len(), 7);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cube() {
    let mut ctx = PostgresTestContext::new().await;
    setup_grouping_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT region, product, SUM(amount)::INT4 AS total
             FROM gs_test
             GROUP BY CUBE (region, product)
             ORDER BY region NULLS LAST, product NULLS LAST",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // CUBE gives all combinations: (r,p), (r,NULL), (NULL,p), (NULL,NULL)
    // 2 regions * 2 products + 2 region totals + 2 product totals + 1 grand = 9
    assert_eq!(arr.len(), 9);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_grouping_function() {
    let mut ctx = PostgresTestContext::new().await;
    setup_grouping_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT
                region,
                product,
                GROUPING(region) AS grp_region,
                GROUPING(product) AS grp_product,
                SUM(amount)::INT4 AS total
             FROM gs_test
             GROUP BY ROLLUP (region, product)
             ORDER BY region NULLS LAST, product NULLS LAST",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();

    // Grand total row: GROUPING(region) = 1, GROUPING(product) = 1
    let grand = arr.iter().find(|r| r["region"].is_null() && r["product"].is_null()).unwrap();
    assert_eq!(grand["grp_region"], 1);
    assert_eq!(grand["grp_product"], 1);
    assert_eq!(grand["total"], 750);

    // Region subtotal: GROUPING(region) = 0, GROUPING(product) = 1
    let east_total = arr.iter().find(|r| r["region"] == "east" && r["product"].is_null()).unwrap();
    assert_eq!(east_total["grp_region"], 0);
    assert_eq!(east_total["grp_product"], 1);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_group_by_multiple_grouping_sets() {
    let mut ctx = PostgresTestContext::new().await;
    setup_grouping_table(&mut ctx).await;

    // Combining multiple GROUPING SETS
    let rows = ctx
        .query(
            "SELECT region, product, SUM(amount)::INT4 AS total
             FROM gs_test
             GROUP BY GROUPING SETS ((region, product), (region))
             ORDER BY region, product NULLS LAST",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // (east, gadget), (east, widget), (east, NULL), (west, gadget), (west, widget), (west, NULL)
    assert_eq!(arr.len(), 6);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_rollup_with_having() {
    let mut ctx = PostgresTestContext::new().await;
    setup_grouping_table(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT region, SUM(amount)::INT4 AS total
             FROM gs_test
             GROUP BY ROLLUP (region)
             HAVING SUM(amount) > 350
             ORDER BY region NULLS LAST",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // east=350 (not > 350), west=400 (yes), grand=750 (yes)
    assert_eq!(arr.len(), 2);

    ctx.stop().await;
}
