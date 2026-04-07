//! PostgreSQL connection, stats collection, and monitoring queries.

use native_tls::TlsConnector;
use postgres::Client;
use postgres_native_tls::MakeTlsConnector;

pub const HISTORY_SIZE: usize = 120;

#[derive(Clone, Debug, PartialEq)]
pub enum DbStatus {
    Connected,
    Error,
}

#[derive(Clone)]
pub struct DbStats {
    /// Display label (port extracted from URL)
    pub port: String,
    /// Total row count across all user tables
    pub rows: i64,
    /// Delta in row count since last tick
    pub rows_delta: i64,
    /// Transactions per second (computed from xact_commit + xact_rollback delta)
    pub tps: i64,
    /// Active connections to this database
    pub connected_clients: i64,
    /// Number of user tables in the database
    pub table_count: i64,
    /// Rolling history of row counts for charting
    pub rows_history: Vec<(f64, f64)>,
    /// Rolling history of TPS for charting
    pub tps_history: Vec<(f64, f64)>,
    /// Connection status
    pub status: DbStatus,
    /// Previous cumulative transaction count (for computing TPS delta)
    pub prev_xact_total: Option<i64>,
}

impl DbStats {
    pub fn new(port: String) -> Self {
        Self {
            port,
            rows: 0,
            rows_delta: 0,
            tps: 0,
            connected_clients: 0,
            table_count: 0,
            rows_history: Vec::with_capacity(HISTORY_SIZE),
            tps_history: Vec::with_capacity(HISTORY_SIZE),
            status: DbStatus::Connected,
            prev_xact_total: None,
        }
    }

    pub fn push_history(&mut self, tick: u64) {
        let x = tick as f64;

        if self.rows_history.len() >= HISTORY_SIZE {
            self.rows_history.remove(0);
        }
        if self.tps_history.len() >= HISTORY_SIZE {
            self.tps_history.remove(0);
        }

        self.rows_history.push((x, self.rows.max(0) as f64));
        self.tps_history.push((x, self.tps.max(0) as f64));
    }
}

/// Parsed components from a PostgreSQL URL for display and Eden API use.
#[derive(Clone, Debug)]
pub struct PgUrlParts {
    pub host: String,
    pub port: String,
    pub database: String,
    pub full_url: String,
}

/// Parse a PostgreSQL URL into its components.
/// Handles format: postgresql://user:pass@host:port/dbname
pub fn parse_pg_url(url: &str) -> PgUrlParts {
    // Strip scheme
    let without_scheme = url
        .strip_prefix("postgresql://")
        .or_else(|| url.strip_prefix("postgres://"))
        .unwrap_or(url);

    // Split off credentials (user:pass@)
    let after_creds = if let Some(idx) = without_scheme.rfind('@') {
        &without_scheme[idx + 1..]
    } else {
        without_scheme
    };

    // Split host:port/dbname?params
    let (host_port, database) = if let Some(idx) = after_creds.find('/') {
        let db_and_params = &after_creds[idx + 1..];
        let db = db_and_params
            .split('?')
            .next()
            .unwrap_or(db_and_params)
            .to_string();
        (&after_creds[..idx], db)
    } else {
        (after_creds, "postgres".to_string())
    };

    let (host, port) = if let Some(idx) = host_port.rfind(':') {
        let h = &host_port[..idx];
        let p = &host_port[idx + 1..];
        if p.parse::<u16>().is_ok() {
            (h.to_string(), p.to_string())
        } else {
            (host_port.to_string(), "5432".to_string())
        }
    } else {
        (host_port.to_string(), "5432".to_string())
    };

    PgUrlParts {
        host,
        port,
        database,
        full_url: url.to_string(),
    }
}

/// Create a new PostgreSQL client connection from a URL.
pub fn connect(url: &str) -> Result<Client, postgres::Error> {
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .map_err(|e| {
            // native_tls errors can't convert to postgres::Error directly,
            // so we connect with a known-bad URL to produce a connection error.
            // In practice this path should never be hit.
            log::error!("TLS connector build failed: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, format!("TLS error: {}", e))
        })
        .expect("TLS connector build should not fail");
    let tls = MakeTlsConnector::new(connector);
    Client::connect(url, tls)
}

/// Health-check a PostgreSQL connection.
pub fn check_connection(label: &str, url: &str) -> Result<(), String> {
    let parts = parse_pg_url(url);
    log::info!(
        "Connecting to {} PostgreSQL at {}:{}/{}...",
        label,
        parts.host,
        parts.port,
        parts.database
    );

    let mut client = connect(url).map_err(|e| {
        let mut msg = format!(
            "Failed to connect to {} PostgreSQL ({}:{}/{})",
            label, parts.host, parts.port, parts.database
        );
        msg.push_str(&format!("\n  Error: {}", e));
        // Walk the error source chain for full context
        let mut source = std::error::Error::source(&e);
        while let Some(cause) = source {
            msg.push_str(&format!("\n  Caused by: {}", cause));
            source = std::error::Error::source(cause);
        }
        msg
    })?;

    client
        .simple_query("SELECT 1")
        .map_err(|e| format!("Failed to ping {} PostgreSQL: {}", label, e))?;

    log::info!("  Connected to {} PostgreSQL", label);
    Ok(())
}

/// All stats returned from a single query round trip.
pub struct StatsSnapshot {
    pub total_rows: i64,
    pub table_count: i64,
    pub xact_total: i64,
    pub active_connections: i64,
}

/// Fetch all monitoring stats in a single query (one network round trip).
/// Uses n_live_tup estimate instead of exact COUNT(*) to avoid full table scans.
/// Uses simple_query to avoid creating a new prepared statement every tick.
pub fn query_all_stats(client: &mut Client) -> Result<StatsSnapshot, String> {
    let results = client
        .simple_query(
            "SELECT \
                COALESCE((SELECT sum(n_live_tup)::bigint FROM pg_stat_user_tables \
                    JOIN pg_class c ON pg_stat_user_tables.relid = c.oid WHERE c.relkind != 'm'), 0)::text, \
                COALESCE((SELECT count(*)::bigint FROM pg_stat_user_tables), 0)::text, \
                COALESCE((SELECT xact_commit + xact_rollback FROM pg_stat_database \
                    WHERE datname = current_database()), 0)::text, \
                COALESCE((SELECT count(*)::bigint FROM pg_stat_activity \
                    WHERE datname = current_database()), 0)::text",
        )
        .map_err(|e| format!("Failed to query stats: {}", e))?;

    // simple_query returns SimpleQueryMessage variants; extract the first DataRow.
    let row = results
        .into_iter()
        .find_map(|msg| {
            if let postgres::SimpleQueryMessage::Row(r) = msg {
                Some(r)
            } else {
                None
            }
        })
        .ok_or_else(|| "No row returned from stats query".to_string())?;

    let parse_col = |idx: usize, name: &str| -> Result<i64, String> {
        row.get(idx)
            .ok_or_else(|| format!("Missing column {name}"))?
            .parse::<i64>()
            .map_err(|e| format!("Failed to parse {name}: {e}"))
    };

    Ok(StatsSnapshot {
        total_rows: parse_col(0, "total_rows")?,
        table_count: parse_col(1, "table_count")?,
        xact_total: parse_col(2, "xact_total")?,
        active_connections: parse_col(3, "active_connections")?,
    })
}

/// Update DbStats using a persistent client connection.
/// If the client is None or broken, reconnects using the URL.
/// Returns true if the update succeeded, false otherwise.
pub fn update_stats(stats: &mut DbStats, client: &mut Option<Client>, url: &str) -> bool {
    // Ensure we have a connection, reconnecting if needed
    if client.is_none() {
        match connect(url) {
            Ok(c) => *client = Some(c),
            Err(e) => {
                log::warn!("Reconnect failed for {}: {}", stats.port, e);
                stats.status = DbStatus::Error;
                return false;
            }
        }
    }

    let c = client.as_mut().unwrap();

    // Test the connection with a simple query; reconnect on failure
    if c.simple_query("").is_err() {
        match connect(url) {
            Ok(new_c) => *client = Some(new_c),
            Err(e) => {
                log::warn!("Reconnect failed for {}: {}", stats.port, e);
                *client = None;
                stats.status = DbStatus::Error;
                return false;
            }
        }
    }

    let c = client.as_mut().unwrap();
    stats.status = DbStatus::Connected;

    match query_all_stats(c) {
        Ok(snap) => {
            let old_rows = stats.rows;
            stats.rows = snap.total_rows;
            stats.rows_delta = snap.total_rows - old_rows;
            stats.table_count = snap.table_count;
            stats.connected_clients = snap.active_connections;

            if let Some(prev) = stats.prev_xact_total {
                stats.tps = (snap.xact_total - prev).max(0);
            }
            stats.prev_xact_total = Some(snap.xact_total);
        }
        Err(_) => {
            // Connection may be broken, drop it for next tick reconnect
            *client = None;
            stats.status = DbStatus::Error;
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pg_url_full() {
        let parts = parse_pg_url("postgresql://user:pass@localhost:5432/mydb");
        assert_eq!(parts.host, "localhost");
        assert_eq!(parts.port, "5432");
        assert_eq!(parts.database, "mydb");
    }

    #[test]
    fn test_parse_pg_url_no_port() {
        let parts = parse_pg_url("postgresql://user:pass@myhost/mydb");
        assert_eq!(parts.host, "myhost");
        assert_eq!(parts.port, "5432");
        assert_eq!(parts.database, "mydb");
    }

    #[test]
    fn test_parse_pg_url_no_database() {
        let parts = parse_pg_url("postgresql://user:pass@localhost:5432");
        assert_eq!(parts.host, "localhost");
        assert_eq!(parts.port, "5432");
        assert_eq!(parts.database, "postgres");
    }

    #[test]
    fn test_parse_pg_url_postgres_scheme() {
        let parts = parse_pg_url("postgres://user:pass@host:5433/db");
        assert_eq!(parts.host, "host");
        assert_eq!(parts.port, "5433");
        assert_eq!(parts.database, "db");
    }

    #[test]
    fn test_parse_pg_url_with_query_params() {
        let parts = parse_pg_url(
            "postgresql://user:pass@host:5432/mydb?sslmode=require&channel_binding=require",
        );
        assert_eq!(parts.host, "host");
        assert_eq!(parts.port, "5432");
        assert_eq!(parts.database, "mydb");
    }

    #[test]
    fn test_db_stats_new() {
        let stats = DbStats::new("5432".to_string());
        assert_eq!(stats.port, "5432");
        assert_eq!(stats.rows, 0);
        assert_eq!(stats.tps, 0);
        assert_eq!(stats.status, DbStatus::Connected);
        assert!(stats.prev_xact_total.is_none());
    }

    #[test]
    fn test_db_stats_push_history() {
        let mut stats = DbStats::new("5432".to_string());
        stats.rows = 100;
        stats.tps = 50;
        stats.push_history(1);

        assert_eq!(stats.rows_history.len(), 1);
        assert_eq!(stats.rows_history[0], (1.0, 100.0));
        assert_eq!(stats.tps_history.len(), 1);
        assert_eq!(stats.tps_history[0], (1.0, 50.0));
    }

    #[test]
    fn test_db_stats_history_cap() {
        let mut stats = DbStats::new("5432".to_string());
        for i in 0..HISTORY_SIZE + 10 {
            stats.rows = i as i64;
            stats.push_history(i as u64);
        }
        assert_eq!(stats.rows_history.len(), HISTORY_SIZE);
    }
}
