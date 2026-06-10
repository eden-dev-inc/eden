use crate::codec::{RedisBuffer, RedisStream};
use crate::connection::RedisConnection;
use eden_logger_internal::{ctx_with_trace, log_trace};
use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::{EpError, IoError, ProtocolError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use redis_protocol::resp2::decode::decode as decode_resp2;
use redis_protocol::resp3::decode::complete::decode as decode_resp3;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, rustls};

/// Redis client that maintains a single TCP connection
pub struct RedisClient {
    stream: RedisStream,
    buffer: RedisBuffer,
    protocol_version: u8,
    pending_responses: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RespBytes {
    Resp2(Vec<u8>),
    Resp3(Vec<u8>),
}

impl RespBytes {
    #[allow(clippy::wrong_self_convention)] // Consumes self intentionally to extract inner bytes
    pub fn to_bytes(self) -> Vec<u8> {
        match self {
            RespBytes::Resp2(bytes) | RespBytes::Resp3(bytes) => bytes,
        }
    }

    fn resp2(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Resp2(bytes.into())
    }

    fn resp3(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Resp3(bytes.into())
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Resp2(buffer) | Self::Resp3(buffer) => buffer.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Resp2(buffer) | Self::Resp3(buffer) => buffer.is_empty(),
        }
    }
}

impl ToOutput for RespBytes {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Redis, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Ok(bytes::Bytes::from(self.to_bytes()))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Database"))
    }
}

impl RedisClient {
    /// Create a new Redis client connection
    pub async fn connect(config: &RedisConnection) -> ResultEP<Self> {
        let addr = format!("{}:{}", config.host, config.port.unwrap_or(6379));
        let tcp_stream = TcpStream::connect(&addr).await.map_err(|e| EpError::connect(format!("Failed to connect to {}: {}", addr, e)))?;

        let stream = if let Some(tls_data) = &config.tls {
            // Setup TLS connection
            let mut root_store = rustls::RootCertStore::empty();

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

            RedisStream::Tls(tls_stream)
        } else {
            RedisStream::Tcp(tcp_stream)
        };

        let mut client = Self {
            stream,
            buffer: RedisBuffer::new(),
            protocol_version: config.protocol_version(),
            pending_responses: 0,
        };

        // Authenticate and select database
        client.initialize(config).await?;

        Ok(client)
    }

    /// Initialize connection with AUTH and SELECT if needed
    async fn initialize(&mut self, config: &RedisConnection) -> ResultEP<()> {
        // Send HELLO command to set protocol version
        if self.protocol_version == 3 {
            let hello_cmd = if let Some(ref username) = config.username {
                if let Some(ref password) = config.password {
                    format!(
                        "*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        username.len(),
                        username,
                        password.len(),
                        password
                    )
                } else {
                    "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n".to_string()
                }
            } else {
                "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n".to_string()
            };

            self.send_raw(hello_cmd.as_bytes()).await?;
            let _ = self.read_response().await?;
        } else {
            // RESP2: Use AUTH command if credentials provided
            if let Some(ref password) = config.password {
                let auth_cmd = if let Some(ref username) = config.username {
                    format!("*3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{}\r\n", username.len(), username, password.len(), password)
                } else {
                    format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", password.len(), password)
                };

                self.send_command_raw(auth_cmd.as_bytes()).await?;
            }
        }

        // Select database if specified
        if let Some(db) = config.db
            && db != 0
        {
            let select_cmd = format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", db.to_string().len(), db);
            self.send_command_raw(select_cmd.as_bytes()).await?;
        }

        Ok(())
    }

    /// Send raw command bytes without reading response (for pipelining)
    #[named]
    pub async fn send_raw(&mut self, command_bytes: &[u8]) -> ResultEP<()> {
        let _ctx = ctx_with_trace!().with_feature("redis_core");
        log_trace!(
            _ctx,
            "Send raw: {}",
            audience = eden_logger_internal::LogAudience::Internal,
            details = std::str::from_utf8(command_bytes).unwrap_or("<invalid utf8>")
        );

        self.stream.write_all(command_bytes).await.map_err(|e| EpError::Io(IoError::Write(e.to_string())))?;

        self.pending_responses += 1;
        Ok(())
    }

    /// Flush the write buffer
    pub async fn flush(&mut self) -> ResultEP<()> {
        self.stream.flush().await.map_err(|e| EpError::Io(IoError::Flush(e.to_string())))
    }

    /// Read a single response
    pub async fn read_response(&mut self) -> ResultEP<RespBytes> {
        if self.pending_responses == 0 {
            return Err(EpError::Protocol(ProtocolError::NoResponses));
        }

        let response = self.read_response_internal().await?;
        self.pending_responses -= 1;
        Ok(response)
    }

    /// Read multiple responses
    pub async fn read_responses(&mut self, count: usize) -> ResultEP<Vec<RespBytes>> {
        if count > self.pending_responses {
            return Err(EpError::Protocol(ProtocolError::MissingResponses(count, self.pending_responses)));
        }

        let mut responses = Vec::with_capacity(count);
        for _ in 0..count {
            responses.push(self.read_response_internal().await?);
        }
        self.pending_responses -= count;
        Ok(responses)
    }

    /// Read all pending responses
    pub async fn read_all_responses(&mut self) -> ResultEP<Vec<RespBytes>> {
        let count = self.pending_responses;
        self.read_responses(count).await
    }

    /// Send raw command bytes and read the response bytes (original behavior)
    pub async fn send_command_raw(&mut self, command_bytes: &[u8]) -> ResultEP<RespBytes> {
        self.send_raw(command_bytes).await?;
        self.flush().await?;
        self.read_response().await
    }

    /// Send multiple commands and read all responses (pipelining)
    pub async fn send_batch(&mut self, commands: &[&[u8]]) -> ResultEP<Vec<RespBytes>> {
        // Send all commands
        for cmd in commands {
            self.send_raw(cmd).await?;
        }
        self.flush().await?;

        // Read all responses
        self.read_responses(commands.len()).await
    }

    /// Internal method to read a single response from the buffer/stream
    #[named]
    async fn read_response_internal(&mut self) -> ResultEP<RespBytes> {
        loop {
            // Try to parse existing buffer
            let parse_result = if self.protocol_version == 2 {
                decode_resp2(self.buffer.unprocessed()).map(|opt| opt.map(|(_, consumed)| (2u8, consumed)))
            } else {
                decode_resp3(self.buffer.unprocessed()).map(|opt| opt.map(|(_, consumed)| (3u8, consumed)))
            };

            match parse_result {
                Ok(Some((version, consumed))) => {
                    let frame_bytes = self.buffer.unprocessed()[..consumed].to_vec();
                    self.buffer.consume(consumed);
                    return Ok(if version == 2 {
                        RespBytes::Resp2(frame_bytes)
                    } else {
                        RespBytes::Resp3(frame_bytes)
                    });
                }
                Ok(None) => {
                    // Need more data
                }
                Err(e) => {
                    if self.protocol_version == 2 {
                        return Err(EpError::Protocol(ProtocolError::RESP2(e.to_string())));
                    } else {
                        let _ctx = ctx_with_trace!().with_feature("redis_core");
                        log_trace!(
                            _ctx,
                            "RESP3 decode error, reading more data: {}",
                            audience = eden_logger_internal::LogAudience::Internal,
                            details = format!("{}", e)
                        );
                    }
                }
            }

            // Read more data from stream
            let mut temp_buf = vec![0u8; 1024 * 1024];
            let n = self.stream.read(&mut temp_buf).await.map_err(|e| EpError::Io(IoError::Read(e.to_string())))?;

            if n == 0 {
                return Err(EpError::Io(IoError::Closed("Connection closed by server".to_string())));
            }

            let _ctx = ctx_with_trace!().with_feature("redis_core");
            log_trace!(
                _ctx,
                "Read {} bytes from stream",
                audience = eden_logger_internal::LogAudience::Internal,
                details = format!("{}", n)
            );
            self.buffer.append(&temp_buf[..n]);
        }
    }

    /// Get the number of pending responses
    pub fn pending_count(&self) -> usize {
        self.pending_responses
    }
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

#[cfg(all(test, feature = "infra-tests"))]
mod tests {
    use super::*;
    use crate::connection::RedisConnection;
    use crate::test_utils::wait_for_redis_ready;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};

    pub async fn run_redis_test<F, Fut>(f: F)
    where
        F: FnOnce(RedisClient) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let (container, host, port) = initialize_redis().await;

        let config = RedisConnection { host, port: Some(port), ..Default::default() };

        let client = RedisClient::connect(&config).await.expect("Failed to connect to Redis");

        f(client).await;

        container.stop().await.expect("Failed to stop Redis test container");
    }

    pub async fn initialize_redis() -> (ContainerAsync<GenericImage>, String, u16) {
        use testcontainers_modules::testcontainers::{GenericImage, core::ContainerPort};

        let container = GenericImage::new("redis", "7.2.4")
            .with_exposed_port(ContainerPort::Tcp(6379))
            .start()
            .await
            .expect("Failed to start database");

        wait_for_redis_ready(&container).await;

        let host_ip = container.get_host().await.expect("Failed to get host address");
        let host_port = container.get_host_port_ipv4(6379).await.expect("Failed to get host port");

        (container, host_ip.to_string(), host_port)
    }

    #[tokio::test]
    async fn test_pipelining() {
        run_redis_test(|mut client| async move {
            let commands = vec![
                &b"*1\r\n$4\r\nPING\r\n"[..],
                &b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"[..],
                &b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"[..],
            ];

            let responses = client.send_batch(&commands).await.unwrap();
            assert_eq!(responses.len(), 3);
        })
        .await;
    }

    #[tokio::test]
    async fn test_manual_pipeline() {
        run_redis_test(|mut client| async move {
            // Send multiple commands
            client.send_raw(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
            client.send_raw(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
            client.flush().await.unwrap();

            assert_eq!(client.pending_count(), 2);

            // Read responses
            let _resp1 = client.read_response().await.unwrap();
            let _resp2 = client.read_response().await.unwrap();

            assert_eq!(client.pending_count(), 0);
        })
        .await;
    }

    #[tokio::test]
    async fn test_send_command_resp3() {
        run_redis_test(|mut client| async move {
            let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
            let result = client.send_command_raw(ping_cmd).await;
            assert!(result.is_ok());
        })
        .await;
    }
}
