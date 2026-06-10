use bytes::{Buf, BufMut, Bytes, BytesMut};
use eden_logger_internal::{ctx_with_trace, log_debug};
use ep_core::tls::GLOBAL_BUNDLE_PEM;
use error::{EpError, ResultEP};
use function_name::named;
use std::future::poll_fn;
use std::io::{self, BufReader, IoSlice};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::sleep;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::RedisConnection;

/// Raw TCP or TLS stream wrapper for Redis connections
#[allow(clippy::large_enum_variant)] // TLS variant necessarily larger due to rustls state
pub enum RedisStream {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

pub(crate) async fn read_buf_with_poll_interval<S>(stream: &mut S, buf: &mut BytesMut, poll_interval: Duration) -> io::Result<usize>
where
    S: tokio::io::AsyncRead + Unpin,
{
    tokio::select!(
        result = stream.read_buf(buf) => {
            match result? {
                0 => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "redis stream closed")),
                n => Ok(n),
            }
        }
        _ = sleep(poll_interval) => {
            log::debug!("Stream read timeout");
            Ok(0)
        }
    )
}

impl RedisStream {
    pub async fn new(config: &RedisConnection) -> ResultEP<Self> {
        let addr = format!("{}:{}", config.host, config.port.unwrap_or(6379));
        let connect_timeout = config.connect_timeout();
        let tcp_stream = tokio::time::timeout(connect_timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| EpError::connect(format!("TCP connect to {} timed out after {}s", addr, connect_timeout.as_secs())))?
            .map_err(|e| EpError::connect(format!("Failed to connect to {}: {}", addr, e)))?;

        // Disable Nagle's algorithm
        tcp_stream.set_nodelay(true)?;

        if let Some(tls_data) = &config.tls {
            // Setup TLS connection
            log::debug!("RedisClient using TLS connection");
            let mut root_store = rustls::RootCertStore::empty();
            let mut count_imported = 0;
            for root_cert in rustls_pemfile::certs(&mut BufReader::new(GLOBAL_BUNDLE_PEM.as_bytes())).flatten() {
                if let Err(e) = root_store.add(root_cert) {
                    log::debug!("RedisClient commenct: error importing root certificate: {e}");
                } else {
                    count_imported += 1;
                }
            }
            log::debug!("RedisClient imported {count_imported} certificates into root store");

            // Parse CA certificate
            let ca_cert = rustls_pemfile::certs(&mut tls_data.ca_cert.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| EpError::connect(format!("Invalid CA certificate: {}", e)))?;

            for cert in ca_cert {
                root_store.add(cert).map_err(|e| EpError::connect(format!("Failed to add CA cert: {}", e)))?;
            }

            let tls_config = if !tls_data.tls_cert.is_empty() && !tls_data.tls_key.is_empty() {
                // With client certificate
                let client_cert = rustls_pemfile::certs(&mut tls_data.tls_cert.as_bytes())
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| EpError::connect(format!("Invalid client certificate: {}", e)))?;

                let client_key = rustls_pemfile::private_key(&mut tls_data.tls_key.as_bytes())
                    .map_err(|e| EpError::connect(format!("Invalid client key: {}", e)))?
                    .ok_or_else(|| EpError::connect("No private key found"))?;

                rustls::ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_client_auth_cert(client_cert, client_key)
                    .map_err(|e| EpError::connect(format!("Failed to set client cert: {}", e)))?
            } else {
                // Without client certificate
                log::debug!("RedisClient: using TLS without client auth with {} root certificates", root_store.len());
                rustls::ClientConfig::builder().with_root_certificates(root_store).with_no_client_auth()
            };

            let tls_config = if config.insecure.unwrap_or(false) {
                // Set dangerous config if insecure
                rustls::ClientConfig::builder().dangerous().with_custom_certificate_verifier(Arc::new(NoVerifier)).with_no_client_auth()
            } else {
                tls_config
            };

            let connector = TlsConnector::from(Arc::new(tls_config));
            let domain = rustls::pki_types::ServerName::try_from(config.host.clone())
                .map_err(|e| EpError::connect(format!("Invalid server name: {}", e)))?;

            let tls_stream =
                connector.connect(domain, tcp_stream).await.map_err(|e| EpError::connect(format!("TLS handshake failed: {}", e)))?;

            Ok(Self::Tls(tls_stream))
        } else {
            Ok(Self::Tcp(tcp_stream))
        }
    }

    /// Read data from the stream into a buffer.
    /// Returns `Ok(0)` if no data arrives within the 1-second poll interval.
    #[named]
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            RedisStream::Tcp(stream) => tokio::select!(
                result = stream.read(buf) => {
                    match result? {
                        0 => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "redis stream closed")),
                        n => Ok(n),
                    }
                }
                _ = sleep(Duration::from_secs(1)) => {
                    let _ctx = ctx_with_trace!().with_feature("redis_core");
                    log_debug!(_ctx, "Stream read timeout", audience = eden_logger_internal::LogAudience::Internal);
                    Ok(0)
                }
            ),
            RedisStream::Tls(stream) => tokio::select!(
                result = stream.read(buf) => {
                    match result? {
                        0 => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "redis stream closed")),
                        n => Ok(n),
                    }
                }
                _ = sleep(Duration::from_secs(1)) => {
                    let _ctx = ctx_with_trace!().with_feature("redis_core");
                    log_debug!(_ctx, "TLS stream read timeout", audience = eden_logger_internal::LogAudience::Internal);
                    Ok(0)
                }
            ),
        }
    }

    /// Read directly into BytesMut (zero-copy).
    /// Returns `Ok(0)` if no data arrives within the 1-second poll interval.
    pub async fn read_buf(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
        match self {
            RedisStream::Tcp(stream) => read_buf_with_poll_interval(stream, buf, Duration::from_secs(1)).await,
            RedisStream::Tls(stream) => read_buf_with_poll_interval(stream, buf, Duration::from_secs(1)).await,
        }
    }

    /// Write data to the stream
    pub async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            RedisStream::Tcp(stream) => stream.write_all(buf).await,
            RedisStream::Tls(stream) => stream.write_all(buf).await,
        }
    }

    /// Flush the stream
    pub async fn flush(&mut self) -> io::Result<()> {
        match self {
            RedisStream::Tcp(stream) => stream.flush().await,
            RedisStream::Tls(stream) => stream.flush().await,
        }
    }

    pub async fn is_connected(&self) -> bool {
        match self {
            RedisStream::Tcp(stream) => stream.writable().await.is_ok(),
            RedisStream::Tls(stream) => stream.get_ref().0.writable().await.is_ok(),
        }
    }

    /// Split the stream into a read half and a write half so that a writer
    /// task and a reader task can drive the underlying connection
    /// concurrently. The TCP path uses `tokio::net::TcpStream::into_split`
    /// for true OS-level half-ownership; the TLS path uses
    /// `tokio::io::split` (BiLock-based) since rustls' TLS state machine
    /// shares state between the directions.
    pub fn into_split(self) -> (RedisStreamReader, RedisStreamWriter) {
        match self {
            RedisStream::Tcp(stream) => {
                let (read, write) = stream.into_split();
                (RedisStreamReader::Tcp(read), RedisStreamWriter::Tcp(write))
            }
            RedisStream::Tls(stream) => {
                let (read, write) = tokio::io::split(stream);
                (RedisStreamReader::Tls(read), RedisStreamWriter::Tls(write))
            }
        }
    }
}

/// Read half produced by `RedisStream::into_split`. Owned by the multiplexer's
/// reader task; reads RESP responses off the wire and dispatches them to
/// pending oneshot slots.
#[allow(clippy::large_enum_variant)]
pub enum RedisStreamReader {
    Tcp(OwnedReadHalf),
    Tls(ReadHalf<TlsStream<TcpStream>>),
    #[cfg(test)]
    Duplex(tokio::io::DuplexStream),
}

impl RedisStreamReader {
    /// Read into BytesMut with the same 1-second timeout-poll semantics as
    /// `RedisStream::read_buf`. Used by the un-split path and by callers
    /// that want the empty-read-budget protection.
    pub async fn read_buf(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
        match self {
            RedisStreamReader::Tcp(stream) => read_buf_with_poll_interval(stream, buf, Duration::from_secs(1)).await,
            RedisStreamReader::Tls(stream) => read_buf_with_poll_interval(stream, buf, Duration::from_secs(1)).await,
            #[cfg(test)]
            RedisStreamReader::Duplex(stream) => read_buf_with_poll_interval(stream, buf, Duration::from_secs(1)).await,
        }
    }

    /// Hot-path read without the per-call timeout `tokio::select!`. The
    /// multiplexer's reader_loop is the primary caller — under load the
    /// timeout never fires because the kernel buffer always has data, and
    /// the per-call timer setup is wasted CPU. Returns `Err(EOF)` if the
    /// stream is closed (matches the contract of `read_buf` on real EOF).
    pub async fn read_buf_no_timeout(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
        let n = match self {
            RedisStreamReader::Tcp(stream) => stream.read_buf(buf).await?,
            RedisStreamReader::Tls(stream) => stream.read_buf(buf).await?,
            #[cfg(test)]
            RedisStreamReader::Duplex(stream) => stream.read_buf(buf).await?,
        };
        if n == 0 {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "redis stream closed"))
        } else {
            Ok(n)
        }
    }

    /// Hot-path read without timeout, bounded to at most `limit` bytes
    /// appended to `buf` by this call.
    pub async fn read_buf_no_timeout_limited(&mut self, buf: &mut BytesMut, limit: usize) -> io::Result<usize> {
        if limit == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "redis stream read limit must be greater than zero"));
        }

        let n = match self {
            RedisStreamReader::Tcp(stream) => {
                let mut limited = buf.limit(limit);
                stream.read_buf(&mut limited).await?
            }
            RedisStreamReader::Tls(stream) => {
                let mut limited = buf.limit(limit);
                stream.read_buf(&mut limited).await?
            }
            #[cfg(test)]
            RedisStreamReader::Duplex(stream) => {
                let mut limited = buf.limit(limit);
                stream.read_buf(&mut limited).await?
            }
        };
        if n == 0 {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "redis stream closed"))
        } else {
            Ok(n)
        }
    }
}

/// Write half produced by `RedisStream::into_split`. Owned by the
/// multiplexer's writer task; sends RESP commands without awaiting a
/// response, freeing the dispatcher to grab the next batch.
#[allow(clippy::large_enum_variant)]
pub enum RedisStreamWriter {
    Tcp(OwnedWriteHalf),
    Tls(WriteHalf<TlsStream<TcpStream>>),
}

impl RedisStreamWriter {
    pub async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            RedisStreamWriter::Tcp(stream) => stream.write_all(buf).await,
            RedisStreamWriter::Tls(stream) => stream.write_all(buf).await,
        }
    }

    pub fn is_write_vectored(&self) -> bool {
        match self {
            RedisStreamWriter::Tcp(stream) => stream.is_write_vectored(),
            RedisStreamWriter::Tls(stream) => stream.is_write_vectored(),
        }
    }

    pub async fn write_all_vectored_chunks(&mut self, chunks: &[Bytes]) -> io::Result<()> {
        match self {
            RedisStreamWriter::Tcp(stream) => write_all_vectored_chunks(stream, chunks).await,
            RedisStreamWriter::Tls(stream) => write_all_vectored_chunks(stream, chunks).await,
        }
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        match self {
            RedisStreamWriter::Tcp(stream) => stream.flush().await,
            RedisStreamWriter::Tls(stream) => stream.flush().await,
        }
    }
}

const MAX_REDIS_VECTORED_CHUNKS_PER_WRITE: usize = 16;

async fn write_all_vectored_chunks<W>(writer: &mut W, chunks: &[Bytes]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut chunk_index = 0usize;
    let mut offset = 0usize;

    while chunk_index < chunks.len() {
        while chunk_index < chunks.len() && offset >= chunks[chunk_index].len() {
            chunk_index = chunk_index.saturating_add(1);
            offset = 0;
        }
        if chunk_index >= chunks.len() {
            return Ok(());
        }

        let written = poll_fn(|cx| {
            let mut slices = [IoSlice::new(&[]); MAX_REDIS_VECTORED_CHUNKS_PER_WRITE];
            let mut slice_count = 0usize;

            let first = &chunks[chunk_index][offset..];
            if !first.is_empty() {
                slices[slice_count] = IoSlice::new(first);
                slice_count = slice_count.saturating_add(1);
            }
            for chunk in &chunks[chunk_index.saturating_add(1)..] {
                if slice_count >= MAX_REDIS_VECTORED_CHUNKS_PER_WRITE {
                    break;
                }
                if !chunk.is_empty() {
                    slices[slice_count] = IoSlice::new(chunk);
                    slice_count = slice_count.saturating_add(1);
                }
            }

            Pin::new(&mut *writer).poll_write_vectored(cx, &slices[..slice_count])
        })
        .await?;

        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }

        let mut remaining = written;
        while remaining > 0 && chunk_index < chunks.len() {
            let available = chunks[chunk_index].len().saturating_sub(offset);
            if remaining < available {
                offset = offset.saturating_add(remaining);
                break;
            }

            remaining = remaining.saturating_sub(available);
            chunk_index = chunk_index.saturating_add(1);
            offset = 0;
        }
    }

    Ok(())
}

/// Insecure TLS verifier for development/testing
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

/// Buffer for accumulating partial Redis protocol frames
pub struct RedisBuffer {
    data: BytesMut,
}

impl RedisBuffer {
    pub fn new() -> Self {
        Self { data: BytesMut::with_capacity(8192) }
    }

    /// Get the unprocessed data in the buffer
    pub fn unprocessed(&self) -> &[u8] {
        &self.data[..]
    }

    /// Mark bytes as consumed - O(1) operation with BytesMut
    pub fn consume(&mut self, n: usize) {
        self.data.advance(n);
    }

    /// Split off the first n bytes and return as frozen Bytes - zero-copy
    pub fn split_to_bytes(&mut self, n: usize) -> bytes::Bytes {
        self.data.split_to(n).freeze()
    }

    /// Append new data to the buffer
    pub fn append(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    /// Clear the buffer
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Direct mutable access for read_buf
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.data
    }

    /// Drain all remaining bytes as frozen Bytes - zero-copy
    pub fn drain_to_bytes(&mut self) -> bytes::Bytes {
        self.data.split().freeze()
    }
}

impl Default for RedisBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncWrite, AsyncWriteExt, duplex};
    use tokio::time::Instant;

    #[derive(Clone, Copy)]
    enum ReadLoadProfile {
        Consistent,
        Variable,
        Malicious,
    }

    impl ReadLoadProfile {
        fn label(self) -> &'static str {
            match self {
                Self::Consistent => "consistent",
                Self::Variable => "variable",
                Self::Malicious => "malicious",
            }
        }

        fn poll_interval(self) -> Duration {
            match self {
                Self::Consistent => Duration::from_millis(5),
                Self::Variable => Duration::from_millis(20),
                Self::Malicious => Duration::from_millis(50),
            }
        }
    }

    fn read_profiles() -> [ReadLoadProfile; 3] {
        [ReadLoadProfile::Consistent, ReadLoadProfile::Variable, ReadLoadProfile::Malicious]
    }

    struct PartialVectoredWriter {
        data: Vec<u8>,
        max_per_write: usize,
        calls: usize,
    }

    impl AsyncWrite for PartialVectoredWriter {
        fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            let n = buf.len().min(self.max_per_write);
            self.data.extend_from_slice(&buf[..n]);
            self.calls = self.calls.saturating_add(1);
            Poll::Ready(Ok(n))
        }

        fn poll_write_vectored(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, bufs: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
            let mut remaining = self.max_per_write;
            let mut written = 0usize;
            for buf in bufs {
                if remaining == 0 {
                    break;
                }
                let n = buf.len().min(remaining);
                self.data.extend_from_slice(&buf[..n]);
                remaining = remaining.saturating_sub(n);
                written = written.saturating_add(n);
            }
            self.calls = self.calls.saturating_add(1);
            Poll::Ready(Ok(written))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn vectored_chunks_handle_partial_writes() {
        let chunks = [
            Bytes::from_static(b"abc"),
            Bytes::from_static(b"de"),
            Bytes::new(),
            Bytes::from_static(b"fgh"),
        ];
        let mut writer = PartialVectoredWriter { data: Vec::new(), max_per_write: 3, calls: 0 };

        write_all_vectored_chunks(&mut writer, &chunks).await.expect("vectored write");

        assert_eq!(writer.data, b"abcdefgh");
        assert!(writer.calls > 1);
    }

    #[tokio::test]
    async fn idle_duplex_reads_time_out_after_the_poll_interval() {
        for profile in read_profiles() {
            let (mut client_side, peer_side) = duplex(1024);
            let poll_interval = profile.poll_interval();
            let mut buffer = BytesMut::new();
            let hold_peer = tokio::spawn(async move {
                sleep(poll_interval + poll_interval).await;
                drop(peer_side);
            });

            let start = Instant::now();
            let read = read_buf_with_poll_interval(&mut client_side, &mut buffer, poll_interval).await.expect("idle read should not error");
            let elapsed = start.elapsed();

            assert_eq!(read, 0, "idle {} reads should still produce zero bytes", profile.label());
            assert!(
                elapsed >= poll_interval,
                "idle {} reads should wait at least one poll interval before timing out",
                profile.label()
            );

            let _ = hold_peer.await;
        }
    }

    #[tokio::test]
    async fn half_closed_duplex_reads_return_zero_immediately() {
        for profile in read_profiles() {
            let (mut client_side, peer_side) = duplex(1024);
            let poll_interval = profile.poll_interval();
            let mut buffer = BytesMut::new();
            drop(peer_side);

            let start = Instant::now();
            let read = read_buf_with_poll_interval(&mut client_side, &mut buffer, poll_interval).await;
            let elapsed = start.elapsed();

            assert!(read.is_err(), "half-closed {} reads should surface EOF distinctly", profile.label());
            assert!(
                elapsed < poll_interval,
                "half-closed {} reads should return before the timeout branch fires",
                profile.label()
            );
        }
    }

    #[tokio::test]
    async fn raw_no_timeout_limited_read_caps_each_append() {
        let (client_side, mut peer_side) = duplex(1024);
        let writer = tokio::spawn(async move {
            peer_side.write_all(b"abcdef").await.expect("duplex write should succeed");
        });
        let mut reader = RedisStreamReader::Duplex(client_side);
        let mut buffer = BytesMut::new();

        let first = reader.read_buf_no_timeout_limited(&mut buffer, 3).await.expect("first limited read should succeed");
        assert_eq!(first, 3);
        assert_eq!(&buffer[..], b"abc");

        let second = reader.read_buf_no_timeout_limited(&mut buffer, 3).await.expect("second limited read should succeed");
        assert_eq!(second, 3);
        assert_eq!(&buffer[..], b"abcdef");

        let _ = writer.await;
    }
}
