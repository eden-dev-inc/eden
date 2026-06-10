use bytes::{Buf, BytesMut};
use ep_core::tls::GLOBAL_BUNDLE_PEM;
use error::{EpError, ResultEP};
use postgres_wire::types::startup::SSLRequest;
use std::io::{self, BufReader};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::connection::SslMode;

/// Raw TCP or TLS stream wrapper for PostgreSQL connections.
///
/// Handles PG-specific SSL negotiation: sends `SSLRequest` (8 bytes),
/// reads 1-byte response ('S' = upgrade to TLS, 'N' = stay plain).
#[allow(clippy::large_enum_variant)] // TLS variant necessarily larger due to rustls state
pub enum PgStream {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

/// Result of the SSL negotiation phase before TLS handshake.
enum SslNegotiation {
    /// Server accepted SSL ('S'), ready for TLS handshake.
    Accepted(TcpStream),
    /// Server refused SSL ('N'), returning the original TCP stream.
    Refused(TcpStream),
}

impl PgStream {
    /// Connect to a PostgreSQL server with optional SSL/TLS.
    ///
    /// Performs PG-specific SSL negotiation:
    /// 1. Opens a plain TCP connection
    /// 2. If sslmode != Disable, sends SSLRequest (8-byte message, code 80877103)
    /// 3. Reads 1-byte response: 'S' = upgrade to TLS, 'N' = stay plain
    /// 4. If 'S', performs TLS handshake via tokio-rustls
    pub async fn connect(host: &str, port: u16, sslmode: &SslMode) -> ResultEP<Self> {
        let addr = format!("{host}:{port}");

        match sslmode {
            SslMode::Disable => {
                let tcp_stream = Self::tcp_connect(&addr).await?;
                Ok(Self::Tcp(tcp_stream))
            }
            SslMode::Prefer => {
                let tcp_stream = Self::tcp_connect(&addr).await?;
                match Self::ssl_negotiate(tcp_stream).await {
                    Ok(SslNegotiation::Accepted(tcp_stream)) => {
                        match Self::tls_handshake(tcp_stream, host).await {
                            Ok(stream) => Ok(stream),
                            Err(_) => {
                                // TLS handshake failed — reconnect with plain TCP
                                let tcp_stream = Self::tcp_connect(&addr).await?;
                                Ok(Self::Tcp(tcp_stream))
                            }
                        }
                    }
                    Ok(SslNegotiation::Refused(tcp_stream)) => Ok(Self::Tcp(tcp_stream)),
                    Err(_) => {
                        // SSL negotiation failed (server closed connection) — reconnect plain
                        let tcp_stream = Self::tcp_connect(&addr).await?;
                        Ok(Self::Tcp(tcp_stream))
                    }
                }
            }
            SslMode::Require => {
                let tcp_stream = Self::tcp_connect(&addr).await?;
                match Self::ssl_negotiate(tcp_stream).await? {
                    SslNegotiation::Accepted(tcp_stream) => Self::tls_handshake(tcp_stream, host).await,
                    SslNegotiation::Refused(_) => Err(EpError::connect("SSL required but server does not support SSL")),
                }
            }
        }
    }

    /// Open a TCP connection with nodelay enabled.
    async fn tcp_connect(addr: &str) -> ResultEP<TcpStream> {
        let tcp_stream = TcpStream::connect(addr).await.map_err(|e| EpError::connect(format!("Failed to connect to {addr}: {e}")))?;
        tcp_stream.set_nodelay(true)?;
        Ok(tcp_stream)
    }

    /// Send SSLRequest and read the server's 1-byte response.
    /// Returns the TCP stream positioned after the SSL negotiation.
    async fn ssl_negotiate(mut tcp_stream: TcpStream) -> ResultEP<SslNegotiation> {
        let ssl_request = SSLRequest::encode();
        tcp_stream.write_all(&ssl_request).await.map_err(|e| EpError::connect(format!("Failed to send SSLRequest: {e}")))?;
        tcp_stream.flush().await.map_err(|e| EpError::connect(format!("Failed to flush SSLRequest: {e}")))?;

        let mut response = [0u8; 1];
        tcp_stream.read_exact(&mut response).await.map_err(|e| EpError::connect(format!("Failed to read SSL response: {e}")))?;

        match response[0] {
            b'S' => Ok(SslNegotiation::Accepted(tcp_stream)),
            b'N' => Ok(SslNegotiation::Refused(tcp_stream)),
            other => Err(EpError::connect(format!("Unexpected SSL response byte: {other:#04x}"))),
        }
    }

    /// Perform TLS handshake on a TCP stream that has already negotiated SSL.
    async fn tls_handshake(tcp_stream: TcpStream, host: &str) -> ResultEP<Self> {
        let tls_config = Self::build_tls_config()?;
        Self::tls_handshake_with_config(tcp_stream, host, tls_config).await
    }

    /// Perform TLS handshake with a specific rustls config.
    async fn tls_handshake_with_config(tcp_stream: TcpStream, host: &str, tls_config: rustls::ClientConfig) -> ResultEP<Self> {
        let connector = TlsConnector::from(Arc::new(tls_config));
        let domain =
            rustls::pki_types::ServerName::try_from(host.to_owned()).map_err(|e| EpError::connect(format!("Invalid server name: {e}")))?;

        let tls_stream = connector.connect(domain, tcp_stream).await.map_err(|e| EpError::connect(format!("TLS handshake failed: {e}")))?;

        Ok(Self::Tls(tls_stream))
    }

    /// Build a rustls ClientConfig using the global CA bundle.
    fn build_tls_config() -> ResultEP<rustls::ClientConfig> {
        let mut root_store = rustls::RootCertStore::empty();

        for root_cert in rustls_pemfile::certs(&mut BufReader::new(GLOBAL_BUNDLE_PEM.as_bytes())).flatten() {
            let _ = root_store.add(root_cert);
        }

        Ok(rustls::ClientConfig::builder().with_root_certificates(root_store).with_no_client_auth())
    }

    /// Connect with a custom TLS config (for client certificate auth or insecure mode).
    pub async fn connect_with_tls(host: &str, port: u16, tls_config: rustls::ClientConfig) -> ResultEP<Self> {
        let addr = format!("{host}:{port}");
        let tcp_stream = Self::tcp_connect(&addr).await?;

        match Self::ssl_negotiate(tcp_stream).await? {
            SslNegotiation::Accepted(tcp_stream) => Self::tls_handshake_with_config(tcp_stream, host, tls_config).await,
            SslNegotiation::Refused(_) => Err(EpError::connect("Server refused SSL")),
        }
    }

    /// Connect without SSL negotiation (plain TCP only).
    /// Used when connecting to a server that is known to not support SSL,
    /// or when SSL is explicitly disabled.
    pub async fn connect_plain(host: &str, port: u16) -> ResultEP<Self> {
        let addr = format!("{host}:{port}");
        let tcp_stream = Self::tcp_connect(&addr).await?;
        Ok(Self::Tcp(tcp_stream))
    }

    /// Read directly into BytesMut (zero-copy).
    pub async fn read_buf(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
        match self {
            PgStream::Tcp(stream) => stream.read_buf(buf).await,
            PgStream::Tls(stream) => stream.read_buf(buf).await,
        }
    }

    /// Write data to the stream.
    pub async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            PgStream::Tcp(stream) => stream.write_all(buf).await,
            PgStream::Tls(stream) => stream.write_all(buf).await,
        }
    }

    /// Flush the stream.
    pub async fn flush(&mut self) -> io::Result<()> {
        match self {
            PgStream::Tcp(stream) => stream.flush().await,
            PgStream::Tls(stream) => stream.flush().await,
        }
    }

    /// Check if the stream is still connected.
    pub async fn is_connected(&self) -> bool {
        match self {
            PgStream::Tcp(stream) => stream.writable().await.is_ok(),
            PgStream::Tls(stream) => stream.get_ref().0.writable().await.is_ok(),
        }
    }
}

/// Buffer for accumulating partial PostgreSQL wire protocol messages.
///
/// PG messages have the format: type(1) + length(4, big-endian, includes self).
/// Total message size = 1 + length. This buffer accumulates bytes until
/// complete messages can be extracted.
pub struct PgBuffer {
    data: BytesMut,
}

impl PgBuffer {
    pub fn new() -> Self {
        Self { data: BytesMut::with_capacity(8192) }
    }

    /// Get the unprocessed data in the buffer.
    pub fn unprocessed(&self) -> &[u8] {
        &self.data[..]
    }

    /// Number of unprocessed bytes.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Mark bytes as consumed — O(1) operation with BytesMut.
    pub fn consume(&mut self, n: usize) {
        self.data.advance(n);
    }

    /// Split off the first n bytes and return as frozen Bytes — zero-copy.
    pub fn split_to_bytes(&mut self, n: usize) -> bytes::Bytes {
        self.data.split_to(n).freeze()
    }

    /// Append new data to the buffer.
    pub fn append(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Direct mutable access for read_buf.
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.data
    }

    /// Drain all remaining bytes as frozen Bytes — zero-copy.
    pub fn drain_to_bytes(&mut self) -> bytes::Bytes {
        self.data.split().freeze()
    }
}

impl Default for PgBuffer {
    fn default() -> Self {
        Self::new()
    }
}
