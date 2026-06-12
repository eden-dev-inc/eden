use crate::connection::MssqlConnection;
use error::EpError;
use futures::Future;
use serde_json::Value;
use std::collections::HashMap;
use tiberius::Client;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

pub trait HttpRequests {
    fn delete(&self, body: Option<String>, headers: Option<HashMap<String, String>>) -> impl Future<Output = Result<Value, EpError>>;
    fn get(&self, body: Option<String>, headers: Option<HashMap<String, String>>) -> impl Future<Output = Result<Value, EpError>>;
    fn post(&self, body: Option<String>, headers: Option<HashMap<String, String>>) -> impl Future<Output = Result<Value, EpError>>;
    fn put(&self, body: Option<String>, headers: Option<HashMap<String, String>>) -> impl Future<Output = Result<Value, EpError>>;
}

pub struct MssqlClient {
    pub(crate) client: Client<Compat<TcpStream>>,
}

impl std::fmt::Debug for MssqlClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MssqlClient")
    }
}

impl MssqlClient {
    pub async fn new(conn: &MssqlConnection) -> Result<Self, EpError> {
        let config = tiberius::Config::from_ado_string(&conn.url).map_err(EpError::connect)?;

        let tcp = TcpStream::connect(config.get_addr()).await.map_err(EpError::connect)?;
        tcp.set_nodelay(true).map_err(EpError::connect)?;

        let client = Client::connect(config, tcp.compat_write()).await.map_err(EpError::connect)?;

        Ok(Self { client })
    }

    pub async fn simple_query(&mut self, query: &str) -> Result<(), EpError> {
        self.client.simple_query(query).await.map_err(EpError::request)?;
        Ok(())
    }
}
