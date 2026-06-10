use clickhouse::Client;
use serde::Serialize;

/// Insert a batch of rows into a ClickHouse table.
pub async fn insert_batch<T: clickhouse::Row + Serialize>(
    client: &Client,
    table: &str,
    rows: &[T],
) -> Result<(), clickhouse::error::Error> {
    let mut insert = client.insert(table)?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}
