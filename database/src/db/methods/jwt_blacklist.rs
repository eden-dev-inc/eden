use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use chrono::Utc;
use eden_core::error::{EpError, ResultEP};

fn expiry_deadline_ms(ttl_secs: u64) -> i64 {
    let ttl_secs = i64::try_from(ttl_secs).unwrap_or(i64::MAX / 1000);
    Utc::now().timestamp_millis().saturating_add(ttl_secs.saturating_mul(1000))
}

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    pub async fn persist_jwt_blacklist_entry(&self, blacklist_key: &str, ttl_secs: u64) -> ResultEP<()> {
        let expires_at_ms = expiry_deadline_ms(ttl_secs);
        let conn = self.pg_connection().await?;

        conn.execute(
            "INSERT INTO jwt_blacklist (blacklist_key, expires_at_ms)
             VALUES ($1, $2)
             ON CONFLICT (blacklist_key) DO UPDATE SET expires_at_ms = EXCLUDED.expires_at_ms",
            &[&blacklist_key, &expires_at_ms],
        )
        .await
        .map_err(EpError::database)?;

        Ok(())
    }

    pub async fn jwt_blacklist_entry_exists(&self, blacklist_key: &str) -> ResultEP<bool> {
        let now_ms = Utc::now().timestamp_millis();
        let conn = self.pg_connection().await?;

        conn.execute("DELETE FROM jwt_blacklist WHERE expires_at_ms <= $1", &[&now_ms]).await.map_err(EpError::database)?;

        let row = conn
            .query_opt(
                "SELECT 1 FROM jwt_blacklist WHERE blacklist_key = $1 AND expires_at_ms > $2",
                &[&blacklist_key, &now_ms],
            )
            .await
            .map_err(EpError::database)?;

        Ok(row.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_deadline_saturates_large_ttls() {
        let deadline = expiry_deadline_ms(u64::MAX);
        assert!(deadline > Utc::now().timestamp_millis());
    }
}
