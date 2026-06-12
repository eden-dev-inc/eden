use crate::codec::{PgBuffer, PgStream, PgStreamReader, PgStreamWriter};
use crate::url::PostgresConnectionParsed;
use bytes::{Bytes, BytesMut};
use error::{EpError, ResultEP};
use postgres_wire::error::backend;
use postgres_wire::scram::ScramClient;
use postgres_wire::types::auth::{Authentication, AuthenticationRequest};
use postgres_wire::types::startup::StartupMessage;
use postgres_wire::write::MessageBuilder;
use std::collections::HashMap;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use wire_stream::SliceStream;

use postgres_wire::parse::PgParseSync;

/// Raw wire protocol client for PostgreSQL.
///
/// Mirrors `RedisClient`: maintains a single TCP/TLS connection to a PostgreSQL
/// server and sends/receives raw wire protocol bytes. No deserialization of
/// query results — the proxy forwards raw bytes between client and server.
pub struct PostgresClient {
    config: PostgresConnectionParsed,
    stream: PgStream,
    buffer: PgBuffer,
    /// Backend process ID and secret key (from BackendKeyData message).
    backend_key_data: Option<(i32, i32)>,
    /// Server parameters received during startup (e.g., server_version, client_encoding).
    server_params: HashMap<String, String>,
    /// Current transaction status from the last ReadyForQuery message.
    /// 'I' = idle, 'T' = in transaction, 'E' = failed transaction.
    transaction_status: u8,
    /// Increments `eden.connections{db_type=postgres}` on creation, decrements on drop.
    _conn_guard: telemetry::ConnectionGuard,
}

pub struct PostgresClientWriter {
    writer: PgStreamWriter,
}

pub struct PostgresClientReader {
    reader: PgStreamReader,
    buffer: PgBuffer,
    transaction_status: u8,
    _conn_guard: telemetry::ConnectionGuard,
}

impl PostgresClient {
    /// Connect to a PostgreSQL server: TCP/TLS + startup handshake + authentication.
    pub async fn connect(config: &PostgresConnectionParsed) -> ResultEP<Self> {
        Self::connect_with_org_endpoint(config, telemetry::labels::SYSTEM_ORG_UUID, None).await
    }

    /// Connect with an optional endpoint UUID for connection metrics labeling.
    pub async fn connect_with_endpoint(config: &PostgresConnectionParsed, endpoint_uuid: Option<String>) -> ResultEP<Self> {
        Self::connect_with_org_endpoint(config, telemetry::labels::SYSTEM_ORG_UUID, endpoint_uuid).await
    }

    /// Connect with organization and endpoint UUIDs for connection metrics labeling.
    pub async fn connect_with_org_endpoint(
        config: &PostgresConnectionParsed,
        org_uuid: impl Into<String>,
        endpoint_uuid: Option<String>,
    ) -> ResultEP<Self> {
        let stream = PgStream::connect(&config.host, config.port, &config.sslmode).await?;

        let mut client = Self {
            config: config.clone(),
            stream,
            buffer: PgBuffer::new(),
            backend_key_data: None,
            server_params: HashMap::new(),
            transaction_status: b'I',
            _conn_guard: telemetry::ConnectionGuard::new_with_endpoint("postgres", org_uuid, endpoint_uuid),
        };

        client.startup_handshake().await?;

        Ok(client)
    }

    pub fn into_split(self) -> (PostgresClientWriter, PostgresClientReader) {
        let (writer, reader) = self.stream.into_split();
        (
            PostgresClientWriter { writer },
            PostgresClientReader {
                reader,
                buffer: self.buffer,
                transaction_status: self.transaction_status,
                _conn_guard: self._conn_guard,
            },
        )
    }

    /// Perform the PostgreSQL startup handshake and authentication.
    ///
    /// Sends StartupMessage, handles auth challenges (cleartext, MD5, SCRAM-SHA-256),
    /// then consumes ParameterStatus, BackendKeyData, and ReadyForQuery messages.
    async fn startup_handshake(&mut self) -> ResultEP<()> {
        // Build startup message
        let mut params = vec![
            ("user".to_string(), self.config.user.clone()),
            ("database".to_string(), self.config.database.clone()),
        ];
        if let Some(ref app_name) = self.config.application_name {
            params.push(("application_name".to_string(), app_name.clone()));
        }

        let startup = StartupMessage::new(params);
        let startup_bytes = startup.encode();

        self.stream
            .write_all(&startup_bytes)
            .await
            .map_err(|e| EpError::connect(format!("Failed to send StartupMessage: {e}")))?;
        self.stream.flush().await.map_err(|e| EpError::connect(format!("Failed to flush: {e}")))?;

        // Read auth response
        self.ensure_buffered_message().await?;
        let auth_request = self.parse_auth_request()?;

        match auth_request {
            AuthenticationRequest::Ok => {
                // No auth needed
            }
            AuthenticationRequest::CleartextPassword => {
                let password =
                    self.config.password.as_deref().ok_or_else(|| EpError::auth("Server requires password but none configured"))?;
                let auth_msg = Authentication::password(password);
                self.stream.write_all(&auth_msg.encode()).await.map_err(|e| EpError::connect(format!("Failed to send password: {e}")))?;
                self.stream.flush().await.map_err(|e| EpError::connect(format!("Flush error: {e}")))?;

                self.ensure_buffered_message().await?;
                let ok = self.parse_auth_request()?;
                if !ok.is_ok() {
                    return Err(EpError::auth("Cleartext password authentication failed"));
                }
            }
            AuthenticationRequest::MD5Password { salt } => {
                let password =
                    self.config.password.as_deref().ok_or_else(|| EpError::auth("Server requires password but none configured"))?;
                let auth_msg = Authentication::md5_password(&self.config.user, password, &salt);
                self.stream
                    .write_all(&auth_msg.encode())
                    .await
                    .map_err(|e| EpError::connect(format!("Failed to send MD5 password: {e}")))?;
                self.stream.flush().await.map_err(|e| EpError::connect(format!("Flush error: {e}")))?;

                self.ensure_buffered_message().await?;
                let ok = self.parse_auth_request()?;
                if !ok.is_ok() {
                    return Err(EpError::auth("MD5 password authentication failed"));
                }
            }
            AuthenticationRequest::SASL { mechanisms } => {
                if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                    return Err(EpError::auth(format!(
                        "Server requires SASL auth but only supports: {:?} (need SCRAM-SHA-256)",
                        mechanisms
                    )));
                }
                self.scram_auth().await?;
            }
            other => {
                return Err(EpError::auth(format!("Unsupported authentication method: {other:?}")));
            }
        }

        // Consume ParameterStatus, BackendKeyData, ReadyForQuery
        self.consume_startup_messages().await?;

        Ok(())
    }

    /// Perform SCRAM-SHA-256 authentication.
    async fn scram_auth(&mut self) -> ResultEP<()> {
        let password = self.config.password.as_deref().ok_or_else(|| EpError::auth("Server requires password but none configured"))?;

        // Generate a random nonce using simple approach
        let nonce = generate_nonce();

        let mut scram = ScramClient::new(&self.config.user, password, &nonce);

        // Step 1: Send SASLInitialResponse
        let client_first = scram.client_first_message();
        let sasl_initial = Authentication::sasl_initial("SCRAM-SHA-256", &client_first);
        self.stream
            .write_all(&sasl_initial.encode())
            .await
            .map_err(|e| EpError::connect(format!("Failed to send SASL initial: {e}")))?;
        self.stream.flush().await.map_err(|e| EpError::connect(format!("Flush error: {e}")))?;

        // Step 2: Read SASLContinue
        self.ensure_buffered_message().await?;
        let sasl_continue = self.parse_auth_request()?;
        let server_first = match sasl_continue {
            AuthenticationRequest::SASLContinue { data } => data,
            other => return Err(EpError::auth(format!("Expected SASLContinue, got: {other:?}"))),
        };

        // Step 3: Process server first, send client final
        let client_final = scram.process_server_first(&server_first).map_err(|e| EpError::auth(e.to_string()))?;
        let sasl_response = Authentication::sasl_response(&client_final);
        self.stream
            .write_all(&sasl_response.encode())
            .await
            .map_err(|e| EpError::connect(format!("Failed to send SASL response: {e}")))?;
        self.stream.flush().await.map_err(|e| EpError::connect(format!("Flush error: {e}")))?;

        // Step 4: Read SASLFinal
        self.ensure_buffered_message().await?;
        let sasl_final = self.parse_auth_request()?;
        let server_final = match sasl_final {
            AuthenticationRequest::SASLFinal { data } => data,
            other => return Err(EpError::auth(format!("Expected SASLFinal, got: {other:?}"))),
        };

        // Step 5: Verify server signature
        scram.verify_server_final(&server_final).map_err(|e| EpError::auth(e.to_string()))?;

        // Step 6: Read AuthenticationOk
        self.ensure_buffered_message().await?;
        let ok = self.parse_auth_request()?;
        if !ok.is_ok() {
            return Err(EpError::auth("SCRAM authentication: server did not send AuthenticationOk"));
        }

        Ok(())
    }

    /// Ensure the buffer has at least one complete message, reading from the stream if needed.
    async fn ensure_buffered_message(&mut self) -> ResultEP<()> {
        while !self.has_complete_message() {
            self.read_into_buffer().await?;
        }
        Ok(())
    }

    /// Read more data from the stream into the internal buffer.
    async fn read_into_buffer(&mut self) -> ResultEP<()> {
        let n = self.stream.read_buf(self.buffer.buffer_mut()).await.map_err(|e| EpError::connect(format!("Read error: {e}")))?;
        if n == 0 {
            return Err(EpError::connect("Connection closed by server"));
        }
        Ok(())
    }

    /// Parse an AuthenticationRequest from the front of the buffer.
    /// Consumes the parsed bytes from the buffer.
    ///
    /// If the server sent an ErrorResponse ('E') instead of an Authentication ('R')
    /// message, this extracts the error and returns it as an `EpError::auth`.
    fn parse_auth_request(&mut self) -> ResultEP<AuthenticationRequest> {
        let data = self.buffer.unprocessed();

        // Check if we have enough data for a message header
        if data.len() < 5 {
            return Err(EpError::auth("Incomplete auth response from server"));
        }

        // If server sent ErrorResponse instead of Authentication, parse and return the error
        if data[0] == backend::ERROR_RESPONSE {
            let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
            let total = 1 + length;
            if data.len() < total {
                return Err(EpError::auth("Incomplete ErrorResponse from server"));
            }
            let error_msg = Self::extract_error_message(&data[5..total]);
            self.buffer.consume(total);
            return Err(EpError::auth(format!("Server rejected connection: {error_msg}")));
        }

        let stream = SliceStream::new(data);
        let auth = AuthenticationRequest::parse_sync(&stream).map_err(|e| EpError::auth(format!("Failed to parse auth message: {e}")))?;
        let consumed = stream.consumed();
        self.buffer.consume(consumed);
        Ok(auth)
    }

    /// Consume post-auth startup messages: ParameterStatus, BackendKeyData, ReadyForQuery.
    /// Also handles ErrorResponse if the server rejects us after auth.
    async fn consume_startup_messages(&mut self) -> ResultEP<()> {
        loop {
            // Ensure we have data
            while !self.has_complete_message() {
                self.read_into_buffer().await?;
            }

            let data = self.buffer.unprocessed();
            let msg_type = data[0];

            match msg_type {
                backend::PARAMETER_STATUS => {
                    // 'S': ParameterStatus — name\0 value\0
                    let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
                    let total = 1 + length;
                    let payload = &data[5..total];

                    // Parse null-terminated name and value
                    if let Some(null_pos) = payload.iter().position(|&b| b == 0) {
                        let name = String::from_utf8_lossy(&payload[..null_pos]).to_string();
                        let value_start = null_pos + 1;
                        if let Some(null_pos2) = payload[value_start..].iter().position(|&b| b == 0) {
                            let value = String::from_utf8_lossy(&payload[value_start..value_start + null_pos2]).to_string();
                            self.server_params.insert(name, value);
                        }
                    }
                    self.buffer.consume(total);
                }
                backend::BACKEND_KEY_DATA => {
                    // 'K': BackendKeyData — pid(i32) + secret(i32)
                    let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
                    let total = 1 + length;
                    let pid = i32::from_be_bytes([data[5], data[6], data[7], data[8]]);
                    let secret = i32::from_be_bytes([data[9], data[10], data[11], data[12]]);
                    self.backend_key_data = Some((pid, secret));
                    self.buffer.consume(total);
                }
                backend::READY_FOR_QUERY => {
                    // 'Z': ReadyForQuery — transaction_status(u8)
                    // Total: 1 (type) + 4 (length=5) + 1 (status) = 6 bytes
                    self.transaction_status = data[5];
                    self.buffer.consume(6);
                    return Ok(());
                }
                backend::ERROR_RESPONSE => {
                    // 'E': ErrorResponse — parse the error message
                    let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
                    let total = 1 + length;
                    let error_msg = Self::extract_error_message(&data[5..total]);
                    self.buffer.consume(total);
                    return Err(EpError::connect(format!("Server error during startup: {error_msg}")));
                }
                backend::NOTICE_RESPONSE => {
                    // 'N': NoticeResponse — skip
                    let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
                    let total = 1 + length;
                    self.buffer.consume(total);
                }
                other => {
                    // Skip unknown message types
                    if data.len() >= 5 {
                        let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
                        let total = 1 + length;
                        self.buffer.consume(total);
                    } else {
                        return Err(EpError::connect(format!("Unexpected message type during startup: '{}'", other as char)));
                    }
                }
            }
        }
    }

    /// Check if there's at least one complete PG message in the buffer.
    /// PG message format: type(1) + length(4, big-endian, includes self).
    fn has_complete_message(&self) -> bool {
        let data = self.buffer.unprocessed();
        if data.len() < 5 {
            return false;
        }
        let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
        data.len() > length
    }

    /// Send raw query bytes and read the complete response until ReadyForQuery.
    ///
    /// This is the core method used by the proxy for raw passthrough.
    /// Writes `bytes` to the server, then reads until a ReadyForQuery ('Z')
    /// message is found. Returns all raw bytes including ReadyForQuery.
    ///
    /// Returns (raw_response_bytes, network_latency_us).
    pub async fn send_query_raw(&mut self, bytes: &[u8]) -> ResultEP<(Bytes, u64)> {
        let mut response = BytesMut::with_capacity(8192);
        let network_latency_us = self
            .send_query_raw_with_frame_handler(bytes, |frame| {
                response.extend_from_slice(&frame);
                Ok(())
            })
            .await?;

        Ok((response.freeze(), network_latency_us))
    }

    /// Send raw query bytes and handle each complete backend frame as soon as
    /// it is read, stopping after ReadyForQuery.
    ///
    /// This is the streaming variant of [`Self::send_query_raw`]. It keeps
    /// frame parsing in postgres-core while allowing gateway callers to
    /// forward, observe, or discard frames without first materializing the
    /// whole response group.
    pub async fn send_query_raw_with_frame_handler<F>(&mut self, bytes: &[u8], handle_frame: F) -> ResultEP<u64>
    where
        F: FnMut(Bytes) -> ResultEP<()>,
    {
        let io_start = Instant::now();

        self.stream.write_all(bytes).await.map_err(|e| EpError::request(format!("Write error: {e}")))?;
        self.stream.flush().await.map_err(|e| EpError::request(format!("Flush error: {e}")))?;

        self.read_until_ready_with_frame_handler(handle_frame).await?;

        let network_latency_us = io_start.elapsed().as_micros() as u64;
        Ok(network_latency_us)
    }

    /// Read from the server until a ReadyForQuery ('Z') message is received.
    /// Returns all accumulated raw bytes including the ReadyForQuery message.
    ///
    /// Data flow: stream → PgBuffer → scan for complete messages → copy to response.
    /// Partial messages stay in PgBuffer until more data arrives.
    pub async fn read_until_ready(&mut self) -> ResultEP<Bytes> {
        let mut response = BytesMut::with_capacity(8192);

        self.read_until_ready_with_frame_handler(|frame| {
            response.extend_from_slice(&frame);
            Ok(())
        })
        .await?;

        Ok(response.freeze())
    }

    /// Read complete backend frames until ReadyForQuery, invoking
    /// `handle_frame` for each frame before reading the next one.
    pub async fn read_until_ready_with_frame_handler<F>(&mut self, mut handle_frame: F) -> ResultEP<()>
    where
        F: FnMut(Bytes) -> ResultEP<()>,
    {
        loop {
            while self.has_complete_message() {
                let data = self.buffer.unprocessed();
                let msg_type = data[0];
                let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
                let total = 1 + length;

                let msg_bytes = self.buffer.split_to_bytes(total);
                if msg_type == backend::READY_FOR_QUERY {
                    self.transaction_status = msg_bytes[5];
                }

                let ready_for_query = msg_type == backend::READY_FOR_QUERY;
                handle_frame(msg_bytes)?;
                if ready_for_query {
                    return Ok(());
                }
            }

            let n = self.stream.read_buf(self.buffer.buffer_mut()).await.map_err(|e| EpError::request(format!("Read error: {e}")))?;

            if n == 0 {
                return Err(EpError::request("Connection closed by server while waiting for ReadyForQuery"));
            }
        }
    }

    /// Send raw bytes to the server and read until a specific message type is received.
    /// Returns all accumulated bytes up to and including the target message.
    ///
    /// Used for COPY protocol: send COPY command, read until CopyInResponse ('G')
    /// or CopyOutResponse ('H').
    pub async fn send_and_read_until(&mut self, bytes: &[u8], stop_type: u8) -> ResultEP<Bytes> {
        self.stream.write_all(bytes).await.map_err(|e| EpError::request(format!("Write error: {e}")))?;
        self.stream.flush().await.map_err(|e| EpError::request(format!("Flush error: {e}")))?;

        let mut response = BytesMut::new();

        loop {
            while self.has_complete_message() {
                let data = self.buffer.unprocessed();
                let msg_type = data[0];
                let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
                let total = 1 + length;

                let msg_bytes = self.buffer.split_to_bytes(total);
                response.extend_from_slice(&msg_bytes);

                if msg_type == stop_type {
                    return Ok(response.freeze());
                }

                // Also stop on ReadyForQuery (error case — server may send ErrorResponse + RFQ)
                if msg_type == backend::READY_FOR_QUERY {
                    self.transaction_status = msg_bytes[5];
                    return Ok(response.freeze());
                }
            }

            let n = self.stream.read_buf(self.buffer.buffer_mut()).await.map_err(|e| EpError::request(format!("Read error: {e}")))?;

            if n == 0 {
                return Err(EpError::request("Connection closed while waiting for response"));
            }
        }
    }

    /// Send raw bytes without reading a response.
    /// Used for forwarding CopyData/CopyDone/CopyFail from client to server.
    pub async fn send_no_response(&mut self, bytes: &[u8]) -> ResultEP<()> {
        self.stream.write_all(bytes).await.map_err(|e| EpError::request(format!("Write error: {e}")))?;
        self.stream.flush().await.map_err(|e| EpError::request(format!("Flush error: {e}")))?;
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────
    // COPY OUT — read bulk data from server
    // ─────────────────────────────────────────────────────────────────────

    /// Execute a COPY TO STDOUT query and return the raw data.
    ///
    /// Sends the SQL as a Q message, reads the server response which is:
    /// `CopyOutResponse ('H') → CopyData ('d')* → CopyDone ('c') → CommandComplete ('C') → ReadyForQuery ('Z')`
    ///
    /// Returns the concatenated payload of all CopyData messages (without the
    /// wire framing). The result is the raw CSV/text/binary data depending
    /// on the COPY format specified in the SQL query.
    pub async fn copy_out(&mut self, sql: &str) -> ResultEP<Vec<u8>> {
        let q_msg = build_query_message(sql);
        self.stream.write_all(&q_msg).await.map_err(|e| EpError::request(format!("Write error: {e}")))?;
        self.stream.flush().await.map_err(|e| EpError::request(format!("Flush error: {e}")))?;

        let mut result_data = Vec::new();
        let mut saw_copy_out = false;

        loop {
            while self.has_complete_message() {
                let data = self.buffer.unprocessed();
                let msg_type = data[0];
                let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
                let total = 1 + length;

                match msg_type {
                    backend::COPY_OUT_RESPONSE => {
                        saw_copy_out = true;
                        self.buffer.consume(total);
                    }
                    backend::COPY_DATA => {
                        // CopyData payload starts at offset 5, length includes the 4-byte length field
                        let payload = &data[5..total];
                        result_data.extend_from_slice(payload);
                        self.buffer.consume(total);
                    }
                    backend::COPY_DONE => {
                        self.buffer.consume(total);
                    }
                    backend::READY_FOR_QUERY => {
                        self.transaction_status = data[5];
                        self.buffer.consume(total);
                        return Ok(result_data);
                    }
                    backend::ERROR_RESPONSE => {
                        let error_msg = Self::extract_error_message(&data[5..total]);
                        self.buffer.consume(total);
                        // Continue reading to consume ReadyForQuery
                        let _ = self.drain_until_ready().await;
                        return Err(EpError::request(format!("COPY OUT error: {error_msg}")));
                    }
                    _ => {
                        // CommandComplete, NoticeResponse, etc.
                        self.buffer.consume(total);
                    }
                }
            }

            let n = self.stream.read_buf(self.buffer.buffer_mut()).await.map_err(|e| EpError::request(format!("Read error: {e}")))?;
            if n == 0 {
                if saw_copy_out {
                    return Ok(result_data);
                }
                return Err(EpError::request("Connection closed during COPY OUT"));
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // COPY IN — send bulk data to server
    // ─────────────────────────────────────────────────────────────────────

    /// Execute a COPY FROM STDIN query and send data to the server.
    ///
    /// Protocol:
    /// 1. Send Q message with the COPY ... FROM STDIN query
    /// 2. Read until CopyInResponse ('G') or ErrorResponse
    /// 3. Send one or more CopyData ('d') messages with the payload
    /// 4. Send CopyDone ('c')
    /// 5. Read CommandComplete + ReadyForQuery
    pub async fn copy_in(&mut self, sql: &str, data: &[u8]) -> ResultEP<u64> {
        let q_msg = build_query_message(sql);

        // Step 1: Send the COPY query
        self.stream.write_all(&q_msg).await.map_err(|e| EpError::request(format!("Write error: {e}")))?;
        self.stream.flush().await.map_err(|e| EpError::request(format!("Flush error: {e}")))?;

        // Step 2: Read until CopyInResponse ('G') or error
        loop {
            while self.has_complete_message() {
                let buf = self.buffer.unprocessed();
                let msg_type = buf[0];
                let length = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                let total = 1 + length;

                match msg_type {
                    backend::COPY_IN_RESPONSE => {
                        self.buffer.consume(total);
                        // Server is ready — proceed to send data
                        return self.send_copy_data_and_finish(data).await;
                    }
                    backend::ERROR_RESPONSE => {
                        let error_msg = Self::extract_error_message(&buf[5..total]);
                        self.buffer.consume(total);
                        let _ = self.drain_until_ready().await;
                        return Err(EpError::request(format!("COPY IN error: {error_msg}")));
                    }
                    backend::READY_FOR_QUERY => {
                        self.transaction_status = buf[5];
                        self.buffer.consume(total);
                        return Err(EpError::request("Server sent ReadyForQuery before CopyInResponse"));
                    }
                    _ => {
                        self.buffer.consume(total);
                    }
                }
            }

            let n = self.stream.read_buf(self.buffer.buffer_mut()).await.map_err(|e| EpError::request(format!("Read error: {e}")))?;
            if n == 0 {
                return Err(EpError::request("Connection closed waiting for CopyInResponse"));
            }
        }
    }

    /// Send CopyData + CopyDone, then read until ReadyForQuery.
    /// Returns the affected row count from CommandComplete.
    async fn send_copy_data_and_finish(&mut self, data: &[u8]) -> ResultEP<u64> {
        // Build CopyData message: 'd' + i32(len) + data
        // len includes itself (4 bytes) + data length
        let copy_data_len = (4 + data.len()) as i32;
        let mut copy_data_msg = Vec::with_capacity(5 + data.len());
        copy_data_msg.push(b'd');
        copy_data_msg.extend_from_slice(&copy_data_len.to_be_bytes());
        copy_data_msg.extend_from_slice(data);

        // Build CopyDone message: 'c' + i32(4)
        let copy_done_msg: [u8; 5] = [b'c', 0, 0, 0, 4];

        // Send CopyData + CopyDone together
        self.stream.write_all(&copy_data_msg).await.map_err(|e| EpError::request(format!("Write CopyData error: {e}")))?;
        self.stream.write_all(&copy_done_msg).await.map_err(|e| EpError::request(format!("Write CopyDone error: {e}")))?;
        self.stream.flush().await.map_err(|e| EpError::request(format!("Flush error: {e}")))?;

        // Read CommandComplete + ReadyForQuery
        let mut affected_rows: u64 = 0;
        loop {
            while self.has_complete_message() {
                let buf = self.buffer.unprocessed();
                let msg_type = buf[0];
                let length = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                let total = 1 + length;

                match msg_type {
                    backend::COMMAND_COMPLETE => {
                        // Extract row count from tag (e.g., "COPY 5")
                        let tag_bytes = &buf[5..total];
                        if let Some(null_pos) = tag_bytes.iter().position(|&b| b == 0) {
                            let tag = String::from_utf8_lossy(&tag_bytes[..null_pos]);
                            if let Some(count_str) = tag.rsplit(' ').next()
                                && let Ok(count) = count_str.parse::<u64>()
                            {
                                affected_rows = count;
                            }
                        }
                        self.buffer.consume(total);
                    }
                    backend::READY_FOR_QUERY => {
                        self.transaction_status = buf[5];
                        self.buffer.consume(total);
                        return Ok(affected_rows);
                    }
                    backend::ERROR_RESPONSE => {
                        let error_msg = Self::extract_error_message(&buf[5..total]);
                        self.buffer.consume(total);
                        let _ = self.drain_until_ready().await;
                        return Err(EpError::request(format!("COPY IN error after data: {error_msg}")));
                    }
                    _ => {
                        self.buffer.consume(total);
                    }
                }
            }

            let n = self.stream.read_buf(self.buffer.buffer_mut()).await.map_err(|e| EpError::request(format!("Read error: {e}")))?;
            if n == 0 {
                return Err(EpError::request("Connection closed after COPY IN data"));
            }
        }
    }

    /// Drain messages until ReadyForQuery, discarding everything.
    /// Used to clean up state after an error mid-protocol.
    async fn drain_until_ready(&mut self) -> ResultEP<()> {
        loop {
            while self.has_complete_message() {
                let data = self.buffer.unprocessed();
                let msg_type = data[0];
                let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
                let total = 1 + length;

                if msg_type == backend::READY_FOR_QUERY {
                    self.transaction_status = data[5];
                    self.buffer.consume(total);
                    return Ok(());
                }
                self.buffer.consume(total);
            }

            let n = self
                .stream
                .read_buf(self.buffer.buffer_mut())
                .await
                .map_err(|e| EpError::request(format!("Read error during drain: {e}")))?;
            if n == 0 {
                return Ok(()); // Connection closed — nothing more to drain
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Convenience methods for API handlers
    // ─────────────────────────────────────────────────────────────────────

    /// Execute batch SQL (DDL/DML) — no rows returned.
    ///
    /// Sends as a simple query (Q message), checks for ErrorResponse.
    pub async fn batch_execute(&mut self, sql: &str) -> ResultEP<()> {
        let q_msg = build_query_message(sql);
        let (raw, _) = self.send_query_raw(&q_msg).await?;
        crate::typed::check_for_error(&raw)
    }

    /// Execute simple query and return raw wire response bytes.
    ///
    /// The caller receives the complete backend response (RowDescription, DataRow*,
    /// CommandComplete, ReadyForQuery) as raw wire bytes. Parse lazily when needed.
    pub async fn simple_query_raw(&mut self, sql: &str) -> ResultEP<Bytes> {
        let q_msg = build_query_message(sql);
        let (raw, _) = self.send_query_raw(&q_msg).await?;
        Ok(raw)
    }

    /// Execute an extended query (Parse/Bind/Execute/Sync) with text-format parameters.
    ///
    /// Parameters are provided as `Option<&str>` — `None` means SQL NULL, `Some(text)`
    /// is the text-format representation of the value. Type OIDs are all 0 (server infers).
    ///
    /// Returns the raw wire response bytes (ParseComplete, BindComplete, RowDescription?,
    /// DataRow*, CommandComplete, ReadyForQuery).
    pub async fn query_params_raw(&mut self, sql: &str, params: &[Option<&str>]) -> ResultEP<Bytes> {
        if params.is_empty() {
            // Fast path: use simple query protocol
            return self.simple_query_raw(sql).await;
        }
        let msg = build_extended_query_message(sql, params);
        let (raw, _) = self.send_query_raw(&msg).await?;
        Ok(raw)
    }

    /// Execute an extended query with text-format parameters and explicit type OIDs.
    ///
    /// Like `query_params_raw` but sends explicit OIDs in the Parse message
    /// instead of letting the server infer types. Used by the `query_typed` API.
    pub async fn query_params_typed_raw(&mut self, sql: &str, params: &[Option<&str>], type_oids: &[i32]) -> ResultEP<Bytes> {
        let msg = build_extended_query_message_typed(sql, params, type_oids);
        let (raw, _) = self.send_query_raw(&msg).await?;
        Ok(raw)
    }

    /// Check if the connection is closed.
    ///
    /// Note: this is a best-effort check. The connection may fail on next use
    /// even if this returns `false`.
    pub fn is_closed(&self) -> bool {
        false // We can't synchronously check; operations will fail if disconnected
    }

    /// Reconnect to the server (close existing connection, re-establish, re-authenticate).
    pub async fn reconnect(&mut self) -> ResultEP<()> {
        // Drop old stream and buffer
        self.buffer.clear();
        self.server_params.clear();
        self.backend_key_data = None;

        self.stream = PgStream::connect(&self.config.host, self.config.port, &self.config.sslmode).await?;
        self.startup_handshake().await?;

        Ok(())
    }

    /// Check if the underlying stream appears connected.
    pub async fn is_connected(&self) -> bool {
        self.stream.is_connected().await
    }

    /// Get the current transaction status byte.
    /// 'I' = idle, 'T' = in transaction, 'E' = failed transaction.
    pub fn transaction_status(&self) -> u8 {
        self.transaction_status
    }

    /// Get the backend process ID (from BackendKeyData).
    pub fn backend_pid(&self) -> Option<i32> {
        self.backend_key_data.map(|(pid, _)| pid)
    }

    /// Get the full backend key data (process_id, secret_key) from BackendKeyData.
    pub fn backend_key_data(&self) -> Option<(i32, i32)> {
        self.backend_key_data
    }

    /// Get a server parameter value.
    pub fn server_param(&self, name: &str) -> Option<&str> {
        self.server_params.get(name).map(|s| s.as_str())
    }

    /// Get the parsed connection config.
    pub fn config(&self) -> &PostgresConnectionParsed {
        &self.config
    }

    /// Extract the human-readable message from an ErrorResponse payload.
    fn extract_error_message(payload: &[u8]) -> String {
        // ErrorResponse fields: field_type(u8) + value\0 ... terminated by \0
        let mut i = 0;
        let mut message = String::new();
        while i < payload.len() && payload[i] != 0 {
            let field_type = payload[i];
            i += 1;
            // Find null terminator
            let start = i;
            while i < payload.len() && payload[i] != 0 {
                i += 1;
            }
            if field_type == b'M' {
                // Message field
                message = String::from_utf8_lossy(&payload[start..i]).to_string();
            }
            if i < payload.len() {
                i += 1; // skip null
            }
        }
        if message.is_empty() { "Unknown error".to_string() } else { message }
    }
}

impl PostgresClientWriter {
    pub(crate) async fn write_query_raw_no_response(&mut self, bytes: &[u8]) -> ResultEP<()> {
        self.writer.write_all(bytes).await.map_err(|e| EpError::request(format!("Write error: {e}")))?;
        self.writer.flush().await.map_err(|e| EpError::request(format!("Flush error: {e}")))
    }
}

impl PostgresClientReader {
    fn has_complete_message(&self) -> bool {
        let data = self.buffer.unprocessed();
        if data.len() < 5 {
            return false;
        }
        let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
        data.len() > length
    }

    pub(crate) async fn read_response_group_raw_bytes(&mut self) -> ResultEP<Bytes> {
        let mut response = BytesMut::with_capacity(8192);
        loop {
            while self.has_complete_message() {
                let data = self.buffer.unprocessed();
                let msg_type = data[0];
                let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
                let total = 1 + length;

                let msg_bytes = self.buffer.split_to_bytes(total);
                if msg_type == backend::READY_FOR_QUERY {
                    self.transaction_status = msg_bytes[5];
                }
                let ready_for_query = msg_type == backend::READY_FOR_QUERY;
                response.extend_from_slice(&msg_bytes);
                if ready_for_query {
                    return Ok(response.freeze());
                }
            }

            let n = self.reader.read_buf(self.buffer.buffer_mut()).await.map_err(|e| EpError::request(format!("Read error: {e}")))?;
            if n == 0 {
                return Err(EpError::request("Connection closed by server while waiting for ReadyForQuery"));
            }
        }
    }
}

/// Build a Simple Query ('Q') message from a SQL string.
pub fn build_query_message(sql: &str) -> Vec<u8> {
    let mut builder = MessageBuilder::new();
    builder.begin(b'Q').write_cstring_str(sql);
    builder.finish_owned()
}

/// Build an extended query protocol message batch (Parse/Bind/Describe/Execute/Sync).
///
/// Uses text format for all parameters (format code 0) and requests text format results.
/// All parameter type OIDs are set to 0 (server infers types).
///
/// Message sequence:
/// - Parse: unnamed statement with the SQL and param type OIDs
/// - Bind: unnamed portal, text-format params
/// - Describe: portal (to get RowDescription for SELECT queries)
/// - Execute: unnamed portal, no row limit
/// - Sync: synchronization point
pub fn build_extended_query_message(sql: &str, params: &[Option<&str>]) -> Vec<u8> {
    let param_count = params.len() as i16;
    let mut buf = Vec::new();

    // ── Parse ────────────────────────────────────────────────────────────
    // 'P' + i32(len) + stmt_name\0 + query\0 + i16(param_count) + [i32(oid)]*n
    {
        let payload_len = 4 + 1 + sql.len() + 1 + 2 + (4 * params.len());
        buf.push(b'P');
        buf.extend_from_slice(&(payload_len as i32).to_be_bytes());
        buf.push(0); // unnamed statement
        buf.extend_from_slice(sql.as_bytes());
        buf.push(0); // null terminator for query
        buf.extend_from_slice(&param_count.to_be_bytes());
        for _ in 0..params.len() {
            buf.extend_from_slice(&0i32.to_be_bytes()); // OID 0 = server infers
        }
    }

    // ── Bind ─────────────────────────────────────────────────────────────
    // 'B' + i32(len) + portal\0 + stmt\0 + i16(format_count) + i16(param_count) + params + i16(result_format_count)
    {
        let mut bind_payload = Vec::new();
        bind_payload.push(0); // unnamed portal
        bind_payload.push(0); // unnamed statement
        bind_payload.extend_from_slice(&0i16.to_be_bytes()); // 0 format codes = all text
        bind_payload.extend_from_slice(&param_count.to_be_bytes());
        for param in params {
            match param {
                Some(val) => {
                    bind_payload.extend_from_slice(&(val.len() as i32).to_be_bytes());
                    bind_payload.extend_from_slice(val.as_bytes());
                }
                None => {
                    bind_payload.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
                }
            }
        }
        bind_payload.extend_from_slice(&0i16.to_be_bytes()); // 0 result format codes = all text

        buf.push(b'B');
        buf.extend_from_slice(&((4 + bind_payload.len()) as i32).to_be_bytes());
        buf.extend_from_slice(&bind_payload);
    }

    // ── Describe (portal) ────────────────────────────────────────────────
    // 'D' + i32(len) + 'P' + portal_name\0
    {
        buf.push(b'D');
        buf.extend_from_slice(&6i32.to_be_bytes()); // 4 + 1 + 1
        buf.push(b'P'); // describe portal
        buf.push(0); // unnamed portal
    }

    // ── Execute ──────────────────────────────────────────────────────────
    // 'E' + i32(len) + portal_name\0 + i32(max_rows)
    {
        buf.push(b'E');
        buf.extend_from_slice(&9i32.to_be_bytes()); // 4 + 1 + 4
        buf.push(0); // unnamed portal
        buf.extend_from_slice(&0i32.to_be_bytes()); // 0 = no row limit
    }

    // ── Sync ─────────────────────────────────────────────────────────────
    // 'S' + i32(4)
    {
        buf.push(b'S');
        buf.extend_from_slice(&4i32.to_be_bytes());
    }

    buf
}

/// Build an extended query protocol message batch with explicit type OIDs.
///
/// Same as `build_extended_query_message` but uses the provided type OIDs instead
/// of 0 (server infers). Used by the `query_typed` API where callers specify
/// explicit PostgreSQL types for each parameter.
pub fn build_extended_query_message_typed(sql: &str, params: &[Option<&str>], type_oids: &[i32]) -> Vec<u8> {
    let param_count = params.len() as i16;
    let mut buf = Vec::new();

    // ── Parse ────────────────────────────────────────────────────────────
    {
        let payload_len = 4 + 1 + sql.len() + 1 + 2 + (4 * params.len());
        buf.push(b'P');
        buf.extend_from_slice(&(payload_len as i32).to_be_bytes());
        buf.push(0); // unnamed statement
        buf.extend_from_slice(sql.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&param_count.to_be_bytes());
        for (i, _) in params.iter().enumerate() {
            let oid = type_oids.get(i).copied().unwrap_or(0);
            buf.extend_from_slice(&oid.to_be_bytes());
        }
    }

    // ── Bind ─────────────────────────────────────────────────────────────
    {
        let mut bind_payload = Vec::new();
        bind_payload.push(0); // unnamed portal
        bind_payload.push(0); // unnamed statement
        bind_payload.extend_from_slice(&0i16.to_be_bytes()); // 0 format codes = all text
        bind_payload.extend_from_slice(&param_count.to_be_bytes());
        for param in params {
            match param {
                Some(val) => {
                    bind_payload.extend_from_slice(&(val.len() as i32).to_be_bytes());
                    bind_payload.extend_from_slice(val.as_bytes());
                }
                None => {
                    bind_payload.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
                }
            }
        }
        bind_payload.extend_from_slice(&0i16.to_be_bytes()); // 0 result format codes = all text

        buf.push(b'B');
        buf.extend_from_slice(&((4 + bind_payload.len()) as i32).to_be_bytes());
        buf.extend_from_slice(&bind_payload);
    }

    // ── Describe (portal) ────────────────────────────────────────────────
    {
        buf.push(b'D');
        buf.extend_from_slice(&6i32.to_be_bytes());
        buf.push(b'P');
        buf.push(0);
    }

    // ── Execute ──────────────────────────────────────────────────────────
    {
        buf.push(b'E');
        buf.extend_from_slice(&9i32.to_be_bytes());
        buf.push(0);
        buf.extend_from_slice(&0i32.to_be_bytes());
    }

    // ── Sync ─────────────────────────────────────────────────────────────
    {
        buf.push(b'S');
        buf.extend_from_slice(&4i32.to_be_bytes());
    }

    buf
}

/// Generate a random nonce for SCRAM authentication.
/// Uses a simple approach based on the system time and address-space randomness.
fn generate_nonce() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::SystemTime;

    let mut hasher = DefaultHasher::new();
    SystemTime::now().hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);
    // Include a pointer value for additional entropy
    let stack_var = 0u8;
    let ptr = &stack_var as *const u8 as usize;
    ptr.hash(&mut hasher);
    let hash1 = hasher.finish();

    let mut hasher2 = DefaultHasher::new();
    hash1.hash(&mut hasher2);
    SystemTime::now().hash(&mut hasher2);
    let hash2 = hasher2.finish();

    format!("{hash1:016x}{hash2:016x}")
}
