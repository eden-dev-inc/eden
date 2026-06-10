use crate::integration_tests::context::PostgresTestContext;
use serial_test::serial;

async fn setup_join_tables(ctx: &mut PostgresTestContext) {
    ctx.batch_execute(
        "CREATE TABLE j_departments (id INT PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE j_employees (id INT PRIMARY KEY, name TEXT NOT NULL, dept_id INT REFERENCES j_departments(id));
         CREATE TABLE j_projects (id INT PRIMARY KEY, title TEXT NOT NULL, lead_id INT REFERENCES j_employees(id))",
    )
    .await;

    ctx.execute("INSERT INTO j_departments (id, name) VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')", &[]).await;
    ctx.execute(
        "INSERT INTO j_employees (id, name, dept_id) VALUES
         (1, 'alice', 1),
         (2, 'bob', 1),
         (3, 'carol', 2),
         (4, 'dave', NULL)",
        &[],
    )
    .await;
    ctx.execute("INSERT INTO j_projects (id, title, lead_id) VALUES (1, 'ProjectA', 1), (2, 'ProjectB', 3)", &[]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_inner_join() {
    let mut ctx = PostgresTestContext::new().await;
    setup_join_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT e.name AS emp, d.name AS dept FROM j_employees e INNER JOIN j_departments d ON e.dept_id = d.id ORDER BY e.name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // dave has no dept_id, so excluded
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["emp"], "alice");
    assert_eq!(arr[0]["dept"], "Engineering");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_left_join() {
    let mut ctx = PostgresTestContext::new().await;
    setup_join_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT e.name AS emp, d.name AS dept FROM j_employees e LEFT JOIN j_departments d ON e.dept_id = d.id ORDER BY e.name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // All 4 employees included; dave has NULL dept
    assert_eq!(arr.len(), 4);
    let dave = arr.iter().find(|r| r["emp"] == "dave").unwrap();
    assert!(dave["dept"].is_null());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_right_join() {
    let mut ctx = PostgresTestContext::new().await;
    setup_join_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT e.name AS emp, d.name AS dept FROM j_employees e RIGHT JOIN j_departments d ON e.dept_id = d.id ORDER BY d.name, e.name",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // HR department has no employees: emp is NULL
    let hr = arr.iter().find(|r| r["dept"] == "HR").unwrap();
    assert!(hr["emp"].is_null());

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_full_outer_join() {
    let mut ctx = PostgresTestContext::new().await;
    setup_join_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT e.name AS emp, d.name AS dept FROM j_employees e FULL OUTER JOIN j_departments d ON e.dept_id = d.id ORDER BY e.name NULLS LAST",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    // 4 employees + 1 unmatched dept (HR) = 5 rows
    assert_eq!(arr.len(), 5);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cross_join() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute(
        "CREATE TABLE j_colors (color TEXT);
         CREATE TABLE j_sizes (size TEXT)",
    )
    .await;
    ctx.execute("INSERT INTO j_colors (color) VALUES ('red'), ('blue')", &[]).await;
    ctx.execute("INSERT INTO j_sizes (size) VALUES ('S'), ('M'), ('L')", &[]).await;

    let rows = ctx.query("SELECT color, size FROM j_colors CROSS JOIN j_sizes ORDER BY color, size", &[]).await;
    let arr = rows.as_array().unwrap();
    // 2 * 3 = 6 combinations
    assert_eq!(arr.len(), 6);

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_self_join() {
    let mut ctx = PostgresTestContext::new().await;

    ctx.batch_execute("CREATE TABLE j_org (id INT PRIMARY KEY, name TEXT NOT NULL, manager_id INT REFERENCES j_org(id))")
        .await;
    ctx.execute("INSERT INTO j_org (id, name, manager_id) VALUES (1, 'CEO', NULL), (2, 'VP', 1), (3, 'Dev', 2)", &[]).await;

    let rows = ctx
        .query(
            "SELECT e.name AS employee, m.name AS manager FROM j_org e LEFT JOIN j_org m ON e.manager_id = m.id ORDER BY e.id",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert!(arr[0]["manager"].is_null()); // CEO has no manager
    assert_eq!(arr[1]["manager"], "CEO");
    assert_eq!(arr[2]["manager"], "VP");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_multi_table_join() {
    let mut ctx = PostgresTestContext::new().await;
    setup_join_tables(&mut ctx).await;

    let rows = ctx
        .query(
            "SELECT p.title, e.name AS lead, d.name AS dept
             FROM j_projects p
             JOIN j_employees e ON p.lead_id = e.id
             JOIN j_departments d ON e.dept_id = d.id
             ORDER BY p.title",
            &[],
        )
        .await;
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["title"], "ProjectA");
    assert_eq!(arr[0]["lead"], "alice");
    assert_eq!(arr[0]["dept"], "Engineering");

    ctx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_join_with_alias() {
    let mut ctx = PostgresTestContext::new().await;
    setup_join_tables(&mut ctx).await;

    let row = ctx
        .query_one(
            "SELECT e.name, d.name AS department_name
             FROM j_employees AS e
             INNER JOIN j_departments AS d ON e.dept_id = d.id
             WHERE e.name = 'alice'",
            &[],
        )
        .await;
    assert_eq!(row["name"], "alice");
    assert_eq!(row["department_name"], "Engineering");

    ctx.stop().await;
}
