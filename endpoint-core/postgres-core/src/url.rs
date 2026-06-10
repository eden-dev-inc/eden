use crate::connection::{PostgresConnection, SslMode};
use error::{EpError, ResultEP};

/// Parsed PostgreSQL connection parameters extracted from a connection URL.
///
/// Supports the standard PostgreSQL URI format:
/// `postgresql://[user[:password]@][host[:port]][/database][?param=value&...]`
#[derive(Debug, Clone)]
pub struct PostgresConnectionParsed {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub sslmode: SslMode,
    pub application_name: Option<String>,
}

impl PostgresConnectionParsed {
    pub fn from_connection(conn: &PostgresConnection) -> ResultEP<Self> {
        let url = &conn.url;

        // Strip the scheme
        let rest = url
            .strip_prefix("postgresql://")
            .or_else(|| url.strip_prefix("postgres://"))
            .ok_or_else(|| EpError::parse("Invalid PostgreSQL URL: must start with postgresql:// or postgres://"))?;

        // Split off query string
        let (path_part, query_string) = match rest.split_once('?') {
            Some((p, q)) => (p, Some(q)),
            None => (rest, None),
        };

        // Split userinfo from host/db: userinfo@host_db or just host_db
        let (userinfo, host_db) = match path_part.split_once('@') {
            Some((u, h)) => (Some(u), h),
            None => (None, path_part),
        };

        // Parse user:password
        let (user, password) = match userinfo {
            Some(info) => match info.split_once(':') {
                Some((u, p)) => (percent_decode(u), Some(percent_decode(p))),
                None => (percent_decode(info), None),
            },
            None => ("postgres".to_string(), None),
        };

        // Split host:port from /database
        let (host_port, database) = match host_db.split_once('/') {
            Some((hp, db)) => (hp, if db.is_empty() { user.clone() } else { percent_decode(db) }),
            None => (host_db, user.clone()),
        };

        // Parse host:port
        let (host, port) = match host_port.rsplit_once(':') {
            Some((h, p)) => {
                let port = p.parse::<u16>().map_err(|_| EpError::parse(format!("Invalid port: {p}")))?;
                (h.to_string(), port)
            }
            None => (
                if host_port.is_empty() {
                    "localhost".to_string()
                } else {
                    host_port.to_string()
                },
                5432,
            ),
        };

        // Parse query parameters
        let mut sslmode = conn.sslmode.clone().unwrap_or_default();
        let mut application_name = None;

        if let Some(qs) = query_string {
            for param in qs.split('&') {
                if let Some((key, value)) = param.split_once('=') {
                    match key {
                        "sslmode" => {
                            sslmode = match value {
                                "disable" => SslMode::Disable,
                                "prefer" => SslMode::Prefer,
                                "require" => SslMode::Require,
                                _ => sslmode,
                            };
                        }
                        "application_name" => {
                            application_name = Some(percent_decode(value));
                        }
                        _ => {} // Ignore unknown parameters
                    }
                }
            }
        }

        // Explicit sslmode on PostgresConnection overrides URL query param
        if conn.sslmode.is_some() {
            sslmode = conn.sslmode.clone().unwrap_or_default();
        }

        Ok(Self {
            host,
            port,
            user,
            password,
            database,
            sslmode,
            application_name,
        })
    }
}

/// Minimal percent-decoding for URL components.
fn percent_decode(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.bytes();

    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next();
            let lo = chars.next();
            if let (Some(hi), Some(lo)) = (hi, lo)
                && let (Some(hi_val), Some(lo_val)) = (hex_val(hi), hex_val(lo))
            {
                result.push((hi_val << 4 | lo_val) as char);
                continue;
            }
            // Invalid percent encoding — pass through literally
            result.push('%');
        } else {
            result.push(b as char);
        }
    }

    result
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_full_url() {
        let conn = PostgresConnection {
            url: "postgresql://myuser:mypass@dbhost:5433/mydb".to_string(),
            sslmode: None,
        };
        let parsed = PostgresConnectionParsed::from_connection(&conn).unwrap();
        assert_eq!(parsed.host, "dbhost");
        assert_eq!(parsed.port, 5433);
        assert_eq!(parsed.user, "myuser");
        assert_eq!(parsed.password.as_deref(), Some("mypass"));
        assert_eq!(parsed.database, "mydb");
    }

    #[test]
    fn test_parse_minimal_url() {
        let conn = PostgresConnection {
            url: "postgresql://localhost/testdb".to_string(),
            sslmode: None,
        };
        let parsed = PostgresConnectionParsed::from_connection(&conn).unwrap();
        assert_eq!(parsed.host, "localhost");
        assert_eq!(parsed.port, 5432);
        assert_eq!(parsed.user, "postgres");
        assert_eq!(parsed.password, None);
        assert_eq!(parsed.database, "testdb");
    }

    #[test]
    fn test_parse_with_sslmode_query() {
        let conn = PostgresConnection {
            url: "postgresql://user:pass@host:5432/db?sslmode=require".to_string(),
            sslmode: None,
        };
        let parsed = PostgresConnectionParsed::from_connection(&conn).unwrap();
        assert_eq!(parsed.sslmode, SslMode::Require);
    }

    #[test]
    fn test_explicit_sslmode_overrides_url() {
        let conn = PostgresConnection {
            url: "postgresql://user:pass@host:5432/db?sslmode=require".to_string(),
            sslmode: Some(SslMode::Disable),
        };
        let parsed = PostgresConnectionParsed::from_connection(&conn).unwrap();
        assert_eq!(parsed.sslmode, SslMode::Disable);
    }

    #[test]
    fn test_percent_encoded_password() {
        let conn = PostgresConnection {
            url: "postgresql://user:p%40ss%23word@host/db".to_string(),
            sslmode: None,
        };
        let parsed = PostgresConnectionParsed::from_connection(&conn).unwrap();
        assert_eq!(parsed.password.as_deref(), Some("p@ss#word"));
    }

    #[test]
    fn test_postgres_scheme() {
        let conn = PostgresConnection { url: "postgres://user@host/db".to_string(), sslmode: None };
        let parsed = PostgresConnectionParsed::from_connection(&conn).unwrap();
        assert_eq!(parsed.host, "host");
        assert_eq!(parsed.user, "user");
    }

    #[test]
    fn test_application_name() {
        let conn = PostgresConnection {
            url: "postgresql://user@host/db?application_name=eden_gateway".to_string(),
            sslmode: None,
        };
        let parsed = PostgresConnectionParsed::from_connection(&conn).unwrap();
        assert_eq!(parsed.application_name.as_deref(), Some("eden_gateway"));
    }

    // -- Target + Credentials roundtrip tests --

    use crate::connection::{PostgresCredentials, PostgresTarget};

    #[test]
    fn test_split_and_compose_roundtrip() {
        let original = PostgresConnection {
            url: "postgresql://myuser:mypass@dbhost:5433/mydb".to_string(),
            sslmode: Some(SslMode::Require),
        };

        let (target, creds) = original.split().expect("split");
        assert_eq!(target.host, "dbhost");
        assert_eq!(target.port, 5433);
        assert_eq!(target.database.as_deref(), Some("mydb"));
        assert_eq!(target.sslmode, Some(SslMode::Require));
        assert_eq!(creds.username, "myuser");
        assert_eq!(creds.password.as_deref(), Some("mypass"));

        // Recompose and verify the URL parses to the same values
        let recomposed = PostgresConnection::from_target_and_credentials(&target, &creds);
        let reparsed = PostgresConnectionParsed::from_connection(&recomposed).expect("reparse");
        assert_eq!(reparsed.host, "dbhost");
        assert_eq!(reparsed.port, 5433);
        assert_eq!(reparsed.user, "myuser");
        assert_eq!(reparsed.password.as_deref(), Some("mypass"));
        assert_eq!(reparsed.database, "mydb");
        assert_eq!(reparsed.sslmode, SslMode::Require);
    }

    #[test]
    fn test_compose_with_special_chars_in_password() {
        let target = PostgresTarget {
            host: "host".to_string(),
            port: 5432,
            database: Some("db".to_string()),
            sslmode: None,
            application_name: None,
        };
        let creds = PostgresCredentials {
            username: "user".to_string(),
            password: Some("p@ss#word!".to_string()),
        };

        let conn = PostgresConnection::from_target_and_credentials(&target, &creds);
        let parsed = PostgresConnectionParsed::from_connection(&conn).expect("parse");
        assert_eq!(parsed.password.as_deref(), Some("p@ss#word!"));
    }

    #[test]
    fn test_compose_different_credentials_same_target() {
        let target = PostgresTarget {
            host: "shared-db.internal".to_string(),
            port: 5432,
            database: Some("production".to_string()),
            sslmode: Some(SslMode::Require),
            application_name: None,
        };

        let reader = PostgresCredentials {
            username: "reader".to_string(),
            password: Some("readpass".to_string()),
        };
        let writer = PostgresCredentials {
            username: "writer".to_string(),
            password: Some("writepass".to_string()),
        };

        let read_conn = PostgresConnection::from_target_and_credentials(&target, &reader);
        let write_conn = PostgresConnection::from_target_and_credentials(&target, &writer);

        let read_parsed = PostgresConnectionParsed::from_connection(&read_conn).expect("read");
        let write_parsed = PostgresConnectionParsed::from_connection(&write_conn).expect("write");

        // Same target
        assert_eq!(read_parsed.host, write_parsed.host);
        assert_eq!(read_parsed.port, write_parsed.port);
        assert_eq!(read_parsed.database, write_parsed.database);

        // Different credentials
        assert_eq!(read_parsed.user, "reader");
        assert_eq!(write_parsed.user, "writer");
    }

    #[test]
    fn test_compose_with_application_name() {
        let target = PostgresTarget {
            host: "host".to_string(),
            port: 5432,
            database: Some("db".to_string()),
            sslmode: None,
            application_name: Some("eden_gateway".to_string()),
        };
        let creds = PostgresCredentials { username: "user".to_string(), password: None };

        let conn = PostgresConnection::from_target_and_credentials(&target, &creds);
        let parsed = PostgresConnectionParsed::from_connection(&conn).expect("parse");
        assert_eq!(parsed.application_name.as_deref(), Some("eden_gateway"));
    }
}
