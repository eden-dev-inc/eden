//! TCP/TLS stream and buffer types for ClickHouse native protocol connections.

use bytes::{Buf, BytesMut};
use eden_logger_internal::{ctx_with_trace, log_debug};
use ep_core::tls::GLOBAL_BUNDLE_PEM;
use error::{EpError, ResultEP};
use function_name::named;
use std::io::{self, BufReader};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::connection::ClickhouseConnection;

/// Raw TCP or TLS stream wrapper for ClickHouse native protocol connections.
// TODO: Consider boxing to reduce size differences between variants.
#[allow(clippy::large_enum_variant)]
pub enum ClickhouseStream {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl ClickhouseStream {
    /// Create a new ClickHouse stream connection.
    #[named]
    pub async fn new(config: &ClickhouseConnection) -> ResultEP<Self> {
        // Parse host and port from URL or use native_host/native_port
        let (host, port) = if let Some(native_host) = &config.native_host {
            (native_host.clone(), config.native_port.unwrap_or(9000))
        } else {
            // Try to parse from URL
            let url = &config.url;
            if url.starts_with("http://") || url.starts_with("https://") {
                // HTTP URL - extract host and use native port
                let without_scheme = url.strip_prefix("http://").or_else(|| url.strip_prefix("https://")).unwrap_or(url);
                let host = without_scheme.split(':').next().unwrap_or("localhost");
                let host = host.split('/').next().unwrap_or(host);
                (host.to_string(), config.native_port.unwrap_or(9000))
            } else {
                // Assume it's just a host
                (url.clone(), config.native_port.unwrap_or(9000))
            }
        };

        let addr = format!("{}:{}", host, port);
        let _ctx = ctx_with_trace!().with_feature("clickhouse_core");
        log_debug!(
            _ctx,
            "Connecting to ClickHouse native protocol",
            audience = eden_logger_internal::LogAudience::Internal,
            addr = &addr
        );

        let tcp_stream = TcpStream::connect(&addr).await.map_err(|e| EpError::connect(format!("Failed to connect to {}: {}", addr, e)))?;

        // Disable Nagle's algorithm for lower latency
        tcp_stream.set_nodelay(true)?;

        if config.native_tls.unwrap_or(false) {
            // Setup TLS connection
            log::debug!("ClickhouseClient using TLS connection");
            let mut root_store = rustls::RootCertStore::empty();
            let mut count_imported = 0;

            for root_cert in rustls_pemfile::certs(&mut BufReader::new(GLOBAL_BUNDLE_PEM.as_bytes())).flatten() {
                if let Err(e) = root_store.add(root_cert) {
                    log::debug!("Error importing root certificate: {e}");
                } else {
                    count_imported += 1;
                }
            }
            log::debug!("Imported {count_imported} certificates into root store");

            let tls_config = rustls::ClientConfig::builder().with_root_certificates(root_store).with_no_client_auth();

            let connector = TlsConnector::from(Arc::new(tls_config));
            let domain = rustls::pki_types::ServerName::try_from(host.clone())
                .map_err(|e| EpError::connect(format!("Invalid server name: {}", e)))?;

            let tls_stream =
                connector.connect(domain, tcp_stream).await.map_err(|e| EpError::connect(format!("TLS handshake failed: {}", e)))?;

            Ok(Self::Tls(tls_stream))
        } else {
            Ok(Self::Tcp(tcp_stream))
        }
    }

    /// Read data from the stream into a buffer with timeout.
    #[named]
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read_timeout = Duration::from_secs(30);

        match self {
            ClickhouseStream::Tcp(stream) => match timeout(read_timeout, stream.read(buf)).await {
                Ok(result) => result,
                Err(_) => {
                    let _ctx = ctx_with_trace!().with_feature("clickhouse_core");
                    log_debug!(_ctx, "Stream read timeout", audience = eden_logger_internal::LogAudience::Internal);
                    Ok(0)
                }
            },
            ClickhouseStream::Tls(stream) => match timeout(read_timeout, stream.read(buf)).await {
                Ok(result) => result,
                Err(_) => {
                    let _ctx = ctx_with_trace!().with_feature("clickhouse_core");
                    log_debug!(_ctx, "Stream read timeout", audience = eden_logger_internal::LogAudience::Internal);
                    Ok(0)
                }
            },
        }
    }

    /// Read directly into BytesMut (zero-copy).
    pub async fn read_buf(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
        let read_timeout = Duration::from_secs(30);

        match self {
            ClickhouseStream::Tcp(stream) => match timeout(read_timeout, stream.read_buf(buf)).await {
                Ok(result) => result,
                Err(_) => Ok(0),
            },
            ClickhouseStream::Tls(stream) => match timeout(read_timeout, stream.read_buf(buf)).await {
                Ok(result) => result,
                Err(_) => Ok(0),
            },
        }
    }

    /// Write data to the stream.
    pub async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            ClickhouseStream::Tcp(stream) => stream.write_all(buf).await,
            ClickhouseStream::Tls(stream) => stream.write_all(buf).await,
        }
    }

    /// Flush the stream.
    pub async fn flush(&mut self) -> io::Result<()> {
        match self {
            ClickhouseStream::Tcp(stream) => stream.flush().await,
            ClickhouseStream::Tls(stream) => stream.flush().await,
        }
    }

    /// Check if the stream is connected.
    pub async fn is_connected(&self) -> bool {
        match self {
            ClickhouseStream::Tcp(stream) => stream.writable().await.is_ok(),
            ClickhouseStream::Tls(stream) => stream.get_ref().0.writable().await.is_ok(),
        }
    }
}

/// Buffer for accumulating partial ClickHouse protocol frames.
pub struct ClickhouseBuffer {
    data: BytesMut,
}

impl ClickhouseBuffer {
    /// Create a new buffer with default capacity.
    pub fn new() -> Self {
        Self {
            data: BytesMut::with_capacity(65536), // 64KB initial
        }
    }

    /// Create a new buffer with specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self { data: BytesMut::with_capacity(capacity) }
    }

    /// Get the unprocessed data in the buffer.
    pub fn unprocessed(&self) -> &[u8] {
        &self.data[..]
    }

    /// Mark bytes as consumed - O(1) operation with BytesMut.
    pub fn consume(&mut self, n: usize) {
        self.data.advance(n);
    }

    /// Append new data to the buffer.
    pub fn append(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Split off the first n bytes and return as frozen Bytes - zero-copy.
    pub fn split_to_bytes(&mut self, n: usize) -> bytes::Bytes {
        self.data.split_to(n).freeze()
    }

    /// Drain all remaining bytes as frozen Bytes - zero-copy.
    pub fn drain_to_bytes(&mut self) -> bytes::Bytes {
        self.data.split().freeze()
    }

    /// Get current buffer length.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Direct mutable access for read_buf.
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.data
    }
}

impl Default for ClickhouseBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_operations() {
        let mut buf = ClickhouseBuffer::new();
        assert!(buf.is_empty());

        buf.append(b"hello world");
        assert_eq!(buf.len(), 11);
        assert_eq!(buf.unprocessed(), b"hello world");

        buf.consume(6);
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.unprocessed(), b"world");

        buf.clear();
        assert!(buf.is_empty());
    }
}
