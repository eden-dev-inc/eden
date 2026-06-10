use std::{borrow::Borrow, collections::HashMap, fmt};

use eden_core::{
    ep::{EpConfig, EpRequest, EpResponse, UpdateConfig, EP},
    error::EpError,
};
use sqlx::{
    sqlite::{SqlitePoolOptions, SqliteRow},
    Sqlite,
};
use sqlx::{Executor, Pool};

#[derive(Debug, Default)]
pub struct SqliteDB {
    pool: HashMap<String, Pool<Sqlite>>,
}

#[derive(Debug, Default, Clone)]
pub struct SqliteConfig {
    url: String,
    name: String,
}

pub struct SqliteRequest {
    name: String,
    sql: String,
}

pub struct SqliteResponse(String);

impl SqliteResponse {
    fn new(s: String) -> Self {
        Self(s)
    }
}

impl EpResponse for SqliteResponse {
    fn as_response(self: Box<Self>) -> Box<dyn EpResponse> {
        self
    }
}

impl fmt::Display for SqliteResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl EP for SqliteDB {
    fn new() -> Self {
        Self::default()
    }
    async fn connect(&mut self, config: Box<dyn EpConfig>) -> Result<(), EpError> {
        let config = match config.as_any().downcast_ref::<SqliteConfig>() {
            Some(mysql_config) => mysql_config.clone(),
            None => return Err(EpError::Connect("failed to downcast config".to_string())),
        };

        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect(config.url.as_str())
            .await
            .map_err(|e| EpError::Connect(e.to_string()))?;

        self.pool.insert(config.name, pool);

        Ok(())
    }
    async fn disconnect(&mut self, name: &str) -> Result<(), EpError> {
        match self.pool.remove(name) {
            Some(_) => Ok(()),
            None => Err(EpError::Connect(ConnectError::ConnectionNotFound)),
        }
    }
    fn update(
        &mut self,
        config: Box<dyn UpdateConfig>,
    ) -> Result<Option<Box<dyn EpConfig>>, EpError> {
        todo!()
    }
    async fn query(&mut self, query: Box<dyn EpRequest>) -> Result<Box<dyn EpResponse>, EpError> {
        let request = match query.as_any().downcast_ref::<SqliteRequest>() {
            Some(req) => req,
            None => return Err(EpError::Connect("failed to downcast request".to_string())),
        };

        let mut conn = match self.pool.get(&request.name) {
            Some(pool) => pool,
            None => return Err(EpError::Connect("failed to connect to server".to_string())),
        };

        // let rows = conn
        //     .fetch_all(sqlx::raw_sql(&request.sql))
        //     .await
        //     .map_err(|e| EpError::Request(e.to_string()))?;

        let rows: Vec<SqliteRow> = (sqlx::query(&request.sql))
            .fetch_all(conn)
            .await
            .map_err(|e| EpError::Request(e.to_string()))?;

        // let resp = serde_json::Value(&rows).map_err(|e| EpError::Request(e.to_string()))?;

        todo!()

        // let resp = "[".to_string()
        //     + rows
        //         .iter()
        //         .map(|row| {
        //             let mut s = vec![];
        //             for col in row.columns() {
        //                 let val = column_value(row, col).unwrap_or("(err)".to_string());
        //                 s.push(format!("\"{}\":\"{}\"", col.name(), val));
        //             }
        //             "{".to_string() + &s.join(",") + "}"
        //         }) //format!("{}", serde_json::to_string(&row).unwrap_or_default()))
        //         .collect::<Vec<String>>()
        //         .join(",")
        //         .as_str()
        //     + "]";
        // log::trace!("{} => {}", query, resp);

        // Ok(Box::new(SqliteResponse::new(resp)).as_response())
    }
    async fn execute(
        &mut self,
        execute: Box<dyn EpRequest>,
    ) -> Result<Box<dyn EpResponse>, EpError> {
        todo!()
    }
}
