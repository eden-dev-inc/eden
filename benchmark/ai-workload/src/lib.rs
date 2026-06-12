use bytes::Bytes;
use clap::Args;
use hdrhistogram::Histogram;
use rand::Rng;
use reqwest::header::{AUTHORIZATION, CONNECTION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde::Serialize;
use serde_json::json;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

#[derive(Debug, Clone, Args)]
pub struct ServeArgs {
    /// Listen address for the synthetic backend.
    #[arg(long, default_value = "127.0.0.1:18181")]
    pub listen: SocketAddr,
    /// Completion text bytes returned by the synthetic backend.
    #[arg(long, default_value_t = 1024)]
    pub completion_bytes: usize,
    /// SSE chunks returned for streaming requests.
    #[arg(long, default_value_t = 8)]
    pub stream_chunks: usize,
    /// Optional delay between SSE chunks.
    #[arg(long, default_value_t = 0)]
    pub stream_chunk_delay_us: u64,
    /// Optional fixed delay before non-streaming responses.
    #[arg(long, default_value_t = 0)]
    pub fixed_delay_us: u64,
}

#[derive(Debug, Clone, Args)]
pub struct LoadArgs {
    /// Target URL, for example http://127.0.0.1:18181/v1/chat/completions.
    #[arg(long)]
    pub target: String,
    /// Workload shape: llm, agent, or stream.
    #[arg(long, default_value = "llm")]
    pub workload: WorkloadKind,
    /// Phase name emitted into the JSON result.
    #[arg(long, default_value = "ai")]
    pub phase: String,
    /// Target offered request rate.
    #[arg(long, default_value_t = 100.0)]
    pub rate: f64,
    /// Timed measurement duration, for example 30s or 2m.
    #[arg(long, default_value = "30s")]
    pub duration: String,
    /// Warmup duration before measurement.
    #[arg(long, default_value = "5s")]
    pub warmup: String,
    /// Maximum in-flight requests before the load generator records shed.
    #[arg(long, default_value_t = 1024)]
    pub max_in_flight: usize,
    /// HTTP idle connection pool size.
    #[arg(long, default_value_t = 256)]
    pub connections: usize,
    /// Number of precomputed request bodies to rotate through.
    #[arg(long, default_value_t = 1024)]
    pub precomputed: usize,
    /// Approximate prompt payload bytes in each request.
    #[arg(long, default_value_t = 512)]
    pub prompt_bytes: usize,
    /// Requested output tokens.
    #[arg(long, default_value_t = 128)]
    pub max_tokens: u32,
    /// Model name sent in the OpenAI-compatible request body.
    #[arg(long, default_value = "synthetic-gpt")]
    pub model: String,
    /// Number of tool definitions for agent-shaped requests.
    #[arg(long, default_value_t = 8)]
    pub tool_count: usize,
    /// Optional bearer token sent to the target.
    #[arg(long, env = "AI_WORKLOAD_BEARER", default_value = "eden-gateway-bench")]
    pub bearer: String,
    /// Per-request timeout in milliseconds. Use 0 to disable.
    #[arg(long, default_value_t = 5_000)]
    pub request_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadKind {
    Llm,
    Agent,
    Stream,
}

impl std::str::FromStr for WorkloadKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "llm" | "chat" => Ok(Self::Llm),
            "agent" | "multi-agent" | "multi_agent" => Ok(Self::Agent),
            "stream" | "streaming" => Ok(Self::Stream),
            other => Err(format!("unsupported workload kind: {other}")),
        }
    }
}

impl std::fmt::Display for WorkloadKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Llm => f.write_str("llm"),
            Self::Agent => f.write_str("agent"),
            Self::Stream => f.write_str("stream"),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct LoadResult {
    pub phases: Vec<PhaseResult>,
}

#[derive(Debug, Serialize)]
pub struct PhaseResult {
    pub name: String,
    pub workload: String,
    pub target: String,
    pub offered_rate: f64,
    pub duration_secs: f64,
    pub elapsed_secs: f64,
    pub attempted: u64,
    pub completed: u64,
    pub errors: u64,
    pub status_errors: u64,
    pub shed: u64,
    pub req_s: f64,
    pub app_rx_gbps: f64,
    pub app_tx_gbps: f64,
    pub app_total_gbps: f64,
    pub app_rx_gb_s: f64,
    pub request_body_bytes: u64,
    pub response_body_bytes: u64,
    pub latency_us: Percentiles,
}

#[derive(Debug, Serialize)]
pub struct Percentiles {
    pub min: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

#[derive(Clone)]
struct BackendResponses {
    health: Bytes,
    chat_headers: Bytes,
    chat_body: Bytes,
    responses_headers: Bytes,
    responses_body: Bytes,
    responses_stream_body: Bytes,
    stream_headers: Bytes,
    stream_body: Bytes,
    fixed_delay: Duration,
    stream_chunk_delay: Duration,
    stream_chunks: Vec<Bytes>,
    responses_stream_chunks: Vec<Bytes>,
}

pub async fn serve(args: ServeArgs) -> io::Result<()> {
    let listener = TcpListener::bind(args.listen).await?;
    let responses = Arc::new(BackendResponses::new(&args)?);
    eprintln!(
        "ai-workload synthetic backend listening on {} completion_bytes={} stream_chunks={}",
        args.listen, args.completion_bytes, args.stream_chunks
    );

    loop {
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let responses = Arc::clone(&responses);
        tokio::spawn(async move {
            let _ = handle_backend_connection(socket, responses).await;
        });
    }
}

pub async fn load(args: LoadArgs) -> io::Result<LoadResult> {
    if args.rate <= 0.0 {
        return Err(invalid_input("rate must be positive"));
    }
    if args.max_in_flight == 0 {
        return Err(invalid_input("max-in-flight must be positive"));
    }
    if args.precomputed == 0 {
        return Err(invalid_input("precomputed must be positive"));
    }

    let warmup = parse_duration(&args.warmup)?;
    let duration = parse_duration(&args.duration)?;
    let mut client_builder = reqwest::Client::builder().http1_only().tcp_nodelay(true).pool_max_idle_per_host(args.connections);
    if args.request_timeout_ms > 0 {
        client_builder = client_builder.timeout(Duration::from_millis(args.request_timeout_ms));
    }
    let client = client_builder.build().map_err(other)?;
    let headers = Arc::new(headers_for_workload(&args)?);
    let requests = Arc::new(precompute_requests(&args)?);

    if !warmup.is_zero() {
        run_phase(
            "warmup",
            &args.target,
            args.workload,
            args.rate,
            warmup,
            args.max_in_flight,
            Arc::clone(&requests),
            Arc::clone(&headers),
            client.clone(),
        )
        .await?;
    }

    let phase = run_phase(
        &args.phase,
        &args.target,
        args.workload,
        args.rate,
        duration,
        args.max_in_flight,
        requests,
        headers,
        client,
    )
    .await?;
    Ok(LoadResult { phases: vec![phase] })
}

impl BackendResponses {
    fn new(args: &ServeArgs) -> io::Result<Self> {
        let completion = "x".repeat(args.completion_bytes);
        let chat = json!({
            "id": "chatcmpl-synthetic",
            "object": "chat.completion",
            "created": 1780700000_u64,
            "model": "synthetic-gpt",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": completion},
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 128,
                "completion_tokens": args.completion_bytes.max(1).div_ceil(4),
                "total_tokens": 128 + args.completion_bytes.max(1).div_ceil(4)
            }
        });
        let chat_body = Bytes::from(serde_json::to_vec(&chat).map_err(other)?);
        let chat_headers = response_headers(200, "application/json", chat_body.len(), false);
        let responses_api = json!({
            "id": "resp_synthetic",
            "object": "response",
            "created_at": 1780700000_i64,
            "status": "completed",
            "model": "synthetic-gpt",
            "output": [{
                "id": "msg_synthetic",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": completion,
                    "annotations": []
                }]
            }],
            "output_text": "x".repeat(args.completion_bytes),
            "usage": {
                "input_tokens": 128,
                "output_tokens": args.completion_bytes.max(1).div_ceil(4),
                "total_tokens": 128 + args.completion_bytes.max(1).div_ceil(4)
            }
        });
        let responses_body = Bytes::from(serde_json::to_vec(&responses_api).map_err(other)?);
        let responses_headers = response_headers(200, "application/json", responses_body.len(), false);
        let responses_stream_chunks = build_responses_stream_chunks(args.stream_chunks.max(1), args.completion_bytes);
        let responses_stream_body =
            Bytes::from(responses_stream_chunks.iter().flat_map(|chunk| chunk.iter().copied()).collect::<Vec<u8>>());

        let stream_chunks = build_stream_chunks(args.stream_chunks.max(1), args.completion_bytes);
        let stream_body = Bytes::from(stream_chunks.iter().flat_map(|chunk| chunk.iter().copied()).collect::<Vec<u8>>());
        let stream_headers = response_headers(200, "text/event-stream", stream_body.len(), false);
        let health_body = Bytes::from_static(b"{\"status\":\"ok\"}");
        let health = Bytes::from([response_headers(200, "application/json", health_body.len(), false), health_body].concat());

        Ok(Self {
            health,
            chat_headers,
            chat_body,
            responses_headers,
            responses_body,
            responses_stream_body,
            stream_headers,
            stream_body,
            fixed_delay: Duration::from_micros(args.fixed_delay_us),
            stream_chunk_delay: Duration::from_micros(args.stream_chunk_delay_us),
            stream_chunks,
            responses_stream_chunks,
        })
    }
}

async fn handle_backend_connection(mut socket: TcpStream, responses: Arc<BackendResponses>) -> io::Result<()> {
    let mut buffer = Vec::with_capacity(64 * 1024);
    loop {
        let Some(request) = read_http_request(&mut socket, &mut buffer).await? else {
            return Ok(());
        };

        if request.method == "GET" && matches!(request.path.as_str(), "/health" | "/v1/health") {
            socket.write_all(&responses.health).await?;
        } else if request.method == "POST" && matches!(request.path.as_str(), "/v1/chat/completions" | "/chat/completions" | "/api/chat") {
            if request_wants_stream(&request.body) {
                write_streaming_response(&mut socket, &responses).await?;
            } else {
                if !responses.fixed_delay.is_zero() {
                    tokio::time::sleep(responses.fixed_delay).await;
                }
                socket.write_all(&responses.chat_headers).await?;
                socket.write_all(&responses.chat_body).await?;
            }
        } else if request.method == "POST" && matches!(request.path.as_str(), "/v1/responses" | "/responses") {
            if request_wants_stream(&request.body) {
                write_responses_streaming_response(&mut socket, &responses).await?;
            } else {
                if !responses.fixed_delay.is_zero() {
                    tokio::time::sleep(responses.fixed_delay).await;
                }
                socket.write_all(&responses.responses_headers).await?;
                socket.write_all(&responses.responses_body).await?;
            }
        } else {
            let body = Bytes::from_static(b"{\"error\":{\"message\":\"not found\"}}");
            socket.write_all(&response_headers(404, "application/json", body.len(), request.close)).await?;
            socket.write_all(&body).await?;
        }

        if request.close {
            return Ok(());
        }
    }
}

fn request_wants_stream(body: &[u8]) -> bool {
    body.windows(b"\"stream\":true".len()).any(|window| window == b"\"stream\":true")
        || body.windows(b"\"stream\": true".len()).any(|window| window == b"\"stream\": true")
}

async fn write_streaming_response(socket: &mut TcpStream, responses: &BackendResponses) -> io::Result<()> {
    if responses.stream_chunk_delay.is_zero() {
        socket.write_all(&responses.stream_headers).await?;
        socket.write_all(&responses.stream_body).await
    } else {
        socket.write_all(&response_headers(200, "text/event-stream", responses.stream_body.len(), false)).await?;
        for chunk in &responses.stream_chunks {
            socket.write_all(chunk).await?;
            tokio::time::sleep(responses.stream_chunk_delay).await;
        }
        Ok(())
    }
}

async fn write_responses_streaming_response(socket: &mut TcpStream, responses: &BackendResponses) -> io::Result<()> {
    let headers = response_headers(200, "text/event-stream", responses.responses_stream_body.len(), false);
    if responses.stream_chunk_delay.is_zero() {
        socket.write_all(&headers).await?;
        socket.write_all(&responses.responses_stream_body).await
    } else {
        socket.write_all(&headers).await?;
        for chunk in &responses.responses_stream_chunks {
            socket.write_all(chunk).await?;
            tokio::time::sleep(responses.stream_chunk_delay).await;
        }
        Ok(())
    }
}

struct ParsedRequest {
    method: String,
    path: String,
    body: Vec<u8>,
    close: bool,
}

async fn read_http_request(socket: &mut TcpStream, buffer: &mut Vec<u8>) -> io::Result<Option<ParsedRequest>> {
    loop {
        if let Some(header_end) = find_header_end(buffer) {
            let header_bytes = &buffer[..header_end];
            let headers = std::str::from_utf8(header_bytes).map_err(other)?;
            let mut lines = headers.split("\r\n");
            let request_line = lines.next().ok_or_else(|| invalid_data("missing request line"))?;
            let mut request_parts = request_line.split_whitespace();
            let method = request_parts.next().ok_or_else(|| invalid_data("missing method"))?.to_string();
            let path = request_parts.next().ok_or_else(|| invalid_data("missing path"))?.to_string();
            let version = request_parts.next().unwrap_or("HTTP/1.1");
            let mut content_length = 0_usize;
            let mut close = version.eq_ignore_ascii_case("HTTP/1.0");
            for line in lines {
                if let Some((name, value)) = line.split_once(':') {
                    let name = name.trim();
                    let value = value.trim();
                    if name.eq_ignore_ascii_case("content-length") {
                        content_length = value.parse::<usize>().map_err(other)?;
                    } else if name.eq_ignore_ascii_case("connection") && value.eq_ignore_ascii_case("close") {
                        close = true;
                    }
                }
            }

            let body_start = header_end + 4;
            let total = body_start.saturating_add(content_length);
            if buffer.len() < total {
                read_more(socket, buffer).await?;
                continue;
            }
            let body = buffer[body_start..total].to_vec();
            buffer.drain(..total);
            return Ok(Some(ParsedRequest { method, path, body, close }));
        }

        if read_more(socket, buffer).await? == 0 {
            return if buffer.is_empty() {
                Ok(None)
            } else {
                Err(invalid_data("connection closed mid-request"))
            };
        }
    }
}

async fn read_more(socket: &mut TcpStream, buffer: &mut Vec<u8>) -> io::Result<usize> {
    let mut chunk = [0_u8; 16 * 1024];
    let read = socket.read(&mut chunk).await?;
    if read > 0 {
        buffer.extend_from_slice(&chunk[..read]);
    }
    Ok(read)
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn response_headers(status: u16, content_type: &str, content_length: usize, close: bool) -> Bytes {
    let status_text = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "OK",
    };
    let connection = if close { "close" } else { "keep-alive" };
    Bytes::from(format!(
        "HTTP/1.1 {status} {status_text}\r\ncontent-type: {content_type}\r\ncontent-length: {content_length}\r\nconnection: {connection}\r\n\r\n"
    ))
}

fn build_stream_chunks(chunk_count: usize, completion_bytes: usize) -> Vec<Bytes> {
    let per_chunk = completion_bytes.max(1).div_ceil(chunk_count);
    let mut chunks = Vec::with_capacity(chunk_count + 1);
    for index in 0..chunk_count {
        let delta = "x".repeat(per_chunk);
        let body = json!({
            "id": "chatcmpl-synthetic-stream",
            "object": "chat.completion.chunk",
            "created": 1780700000_u64,
            "model": "synthetic-gpt",
            "choices": [{
                "index": 0,
                "delta": {"content": delta},
                "finish_reason": if index + 1 == chunk_count {
                    json!("stop")
                } else {
                    serde_json::Value::Null
                }
            }]
        });
        let mut frame = Vec::new();
        frame.extend_from_slice(b"data: ");
        frame.extend_from_slice(&serde_json::to_vec(&body).unwrap_or_default());
        frame.extend_from_slice(b"\n\n");
        chunks.push(Bytes::from(frame));
    }
    chunks.push(Bytes::from_static(b"data: [DONE]\n\n"));
    chunks
}

fn build_responses_stream_chunks(chunk_count: usize, completion_bytes: usize) -> Vec<Bytes> {
    let per_chunk = completion_bytes.max(1).div_ceil(chunk_count);
    let mut chunks = Vec::with_capacity(chunk_count + 1);
    for _ in 0..chunk_count {
        let body = json!({
            "type": "response.output_text.delta",
            "delta": "x".repeat(per_chunk)
        });
        let mut frame = Vec::new();
        frame.extend_from_slice(b"data: ");
        frame.extend_from_slice(&serde_json::to_vec(&body).unwrap_or_default());
        frame.extend_from_slice(b"\n\n");
        chunks.push(Bytes::from(frame));
    }
    let completed = json!({
        "type": "response.completed",
        "response": {
            "usage": {
                "input_tokens": 128,
                "output_tokens": completion_bytes.max(1).div_ceil(4),
                "total_tokens": 128 + completion_bytes.max(1).div_ceil(4)
            }
        }
    });
    let mut frame = Vec::new();
    frame.extend_from_slice(b"data: ");
    frame.extend_from_slice(&serde_json::to_vec(&completed).unwrap_or_default());
    frame.extend_from_slice(b"\n\n");
    chunks.push(Bytes::from(frame));
    chunks
}

#[allow(clippy::too_many_arguments)]
async fn run_phase(
    name: &str,
    target: &str,
    workload: WorkloadKind,
    rate: f64,
    duration: Duration,
    max_in_flight: usize,
    requests: Arc<Vec<Bytes>>,
    headers: Arc<HeaderMap>,
    client: reqwest::Client,
) -> io::Result<PhaseResult> {
    let semaphore = Arc::new(Semaphore::new(max_in_flight));
    let attempted = Arc::new(AtomicU64::new(0));
    let shed = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();
    let interval = Duration::from_secs_f64(1.0 / rate);
    let phase_start = Instant::now();
    let deadline = phase_start + duration;
    let mut next_at = phase_start;
    let mut sequence = 0_usize;

    while next_at < deadline {
        tokio::time::sleep_until(next_at.into()).await;
        next_at += interval;

        let Ok(permit) = Arc::clone(&semaphore).try_acquire_owned() else {
            shed.fetch_add(1, Ordering::Relaxed);
            continue;
        };

        let body = requests[sequence % requests.len()].clone();
        sequence = sequence.wrapping_add(1);
        let client = client.clone();
        let target = target.to_string();
        let headers = Arc::clone(&headers);
        let attempted = Arc::clone(&attempted);
        tasks.spawn(async move {
            let _permit = permit;
            attempted.fetch_add(1, Ordering::Relaxed);
            send_one(client, target, headers, body).await
        });
    }

    let mut completed = 0_u64;
    let mut errors = 0_u64;
    let mut status_errors = 0_u64;
    let mut request_body_bytes = 0_u64;
    let mut response_body_bytes = 0_u64;
    let mut latency = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).map_err(other)?;

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(sample)) => {
                completed = completed.saturating_add(1);
                request_body_bytes = request_body_bytes.saturating_add(sample.request_body_bytes);
                response_body_bytes = response_body_bytes.saturating_add(sample.response_body_bytes);
                if sample.status_error {
                    status_errors = status_errors.saturating_add(1);
                }
                let _ = latency.record(sample.latency_us.max(1));
            }
            Ok(Err(_)) | Err(_) => {
                errors = errors.saturating_add(1);
            }
        }
    }

    let elapsed = phase_start.elapsed().as_secs_f64();
    let req_s = completed as f64 / elapsed.max(0.001);
    let app_rx_gbps = response_body_bytes as f64 * 8.0 / elapsed.max(0.001) / 1_000_000_000.0;
    let app_tx_gbps = request_body_bytes as f64 * 8.0 / elapsed.max(0.001) / 1_000_000_000.0;
    let app_total_gbps = (request_body_bytes + response_body_bytes) as f64 * 8.0 / elapsed.max(0.001) / 1_000_000_000.0;
    let app_rx_gb_s = response_body_bytes as f64 / elapsed.max(0.001) / 1_000_000_000.0;

    Ok(PhaseResult {
        name: name.to_string(),
        workload: workload.to_string(),
        target: target.to_string(),
        offered_rate: rate,
        duration_secs: duration.as_secs_f64(),
        elapsed_secs: elapsed,
        attempted: attempted.load(Ordering::Relaxed),
        completed,
        errors,
        status_errors,
        shed: shed.load(Ordering::Relaxed),
        req_s,
        app_rx_gbps,
        app_tx_gbps,
        app_total_gbps,
        app_rx_gb_s,
        request_body_bytes,
        response_body_bytes,
        latency_us: percentiles(&latency),
    })
}

async fn send_one(client: reqwest::Client, target: String, headers: Arc<HeaderMap>, body: Bytes) -> io::Result<Sample> {
    let request_body_bytes = u64::try_from(body.len()).map_err(other)?;
    let started = Instant::now();
    let response = client.post(target).headers((*headers).clone()).body(body).send().await.map_err(other)?;
    let status_error = !response.status().is_success();
    let bytes = response.bytes().await.map_err(other)?;
    let latency_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    Ok(Sample {
        latency_us,
        request_body_bytes,
        response_body_bytes: u64::try_from(bytes.len()).map_err(other)?,
        status_error,
    })
}

struct Sample {
    latency_us: u64,
    request_body_bytes: u64,
    response_body_bytes: u64,
    status_error: bool,
}

fn headers_for_workload(args: &LoadArgs) -> io::Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
    let auth_value = format!("Bearer {}", args.bearer);
    headers.insert(AUTHORIZATION, HeaderValue::from_str(&auth_value).map_err(other)?);
    if args.workload == WorkloadKind::Agent {
        insert_header(&mut headers, "x-eden-agent-id", "bench-agent-router")?;
        insert_header(&mut headers, "x-eden-agent-fingerprint", "sha256:bench")?;
        insert_header(&mut headers, "x-eden-agent-session", "bench-session")?;
        insert_header(&mut headers, "x-eden-agent-principal", "benchmark")?;
        insert_header(&mut headers, "x-eden-agent-tags", "tier=synthetic,workload=multi-agent")?;
    }
    Ok(headers)
}

fn insert_header(headers: &mut HeaderMap, name: &'static str, value: &'static str) -> io::Result<()> {
    headers.insert(HeaderName::from_static(name), HeaderValue::from_str(value).map_err(other)?);
    Ok(())
}

fn precompute_requests(args: &LoadArgs) -> io::Result<Vec<Bytes>> {
    let mut bodies = Vec::with_capacity(args.precomputed);
    let mut rng = rand::rng();
    for index in 0..args.precomputed {
        let prompt = random_ascii(args.prompt_bytes, &mut rng);
        let mut messages = vec![json!({"role": "user", "content": prompt})];
        if args.workload == WorkloadKind::Agent {
            messages.insert(
                0,
                json!({"role": "system", "content": "You are coordinating multiple synthetic agents. Return the shortest valid answer."}),
            );
        }
        let mut body = json!({
            "model": args.model.as_str(),
            "messages": messages,
            "temperature": 0.0,
            "max_tokens": args.max_tokens,
            "stream": args.workload == WorkloadKind::Stream,
            "metadata": {
                "request_index": index,
                "benchmark": "eden-ai-head-to-head"
            }
        });

        if args.workload == WorkloadKind::Agent {
            body["tools"] = json!(agent_tools(args.tool_count));
            body["tool_choice"] = json!("auto");
            body["parallel_tool_calls"] = json!(true);
        }

        bodies.push(Bytes::from(serde_json::to_vec(&body).map_err(other)?));
    }
    Ok(bodies)
}

fn agent_tools(tool_count: usize) -> Vec<serde_json::Value> {
    (0..tool_count)
        .map(|index| {
            json!({
                "type": "function",
                "function": {
                    "name": format!("agent_tool_{index}"),
                    "description": "Synthetic benchmark tool used to exercise gateway tool schemas.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "agent_id": {"type": "string"},
                            "budget_ms": {"type": "integer"}
                        },
                        "required": ["query"]
                    }
                }
            })
        })
        .collect()
}

fn random_ascii(size: usize, rng: &mut impl Rng) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789 ";
    let mut out = String::with_capacity(size);
    for _ in 0..size {
        let idx = rng.random_range(0..ALPHABET.len());
        out.push(ALPHABET[idx] as char);
    }
    out
}

fn percentiles(histogram: &Histogram<u64>) -> Percentiles {
    if histogram.is_empty() {
        return Percentiles { min: 0, p50: 0, p95: 0, p99: 0, p999: 0, max: 0 };
    }
    Percentiles {
        min: histogram.min(),
        p50: histogram.value_at_quantile(0.50),
        p95: histogram.value_at_quantile(0.95),
        p99: histogram.value_at_quantile(0.99),
        p999: histogram.value_at_quantile(0.999),
        max: histogram.max(),
    }
}

fn parse_duration(value: &str) -> io::Result<Duration> {
    let trimmed = value.trim();
    if let Some(seconds) = trimmed.strip_suffix('s') {
        return seconds.parse::<f64>().map(Duration::from_secs_f64).map_err(other);
    }
    if let Some(minutes) = trimmed.strip_suffix('m') {
        return minutes.parse::<f64>().map(|mins| Duration::from_secs_f64(mins * 60.0)).map_err(other);
    }
    if let Some(hours) = trimmed.strip_suffix('h') {
        return hours.parse::<f64>().map(|hrs| Duration::from_secs_f64(hrs * 3600.0)).map_err(other);
    }
    Err(invalid_input("duration must end with s, m, or h"))
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn other(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(error)
}

#[cfg(test)]
mod tests {
    use super::{LoadArgs, WorkloadKind, build_responses_stream_chunks, parse_duration, precompute_requests, request_wants_stream};

    #[test]
    fn parses_duration_suffixes() {
        assert_eq!(parse_duration("1s").map(|d| d.as_secs()).unwrap_or_default(), 1);
        assert_eq!(parse_duration("2m").map(|d| d.as_secs()).unwrap_or_default(), 120);
    }

    #[test]
    fn precomputed_agent_requests_include_tools() {
        let args = LoadArgs {
            target: "http://127.0.0.1:18181/v1/chat/completions".to_string(),
            workload: WorkloadKind::Agent,
            phase: "agent".to_string(),
            rate: 1.0,
            duration: "1s".to_string(),
            warmup: "0s".to_string(),
            max_in_flight: 1,
            connections: 1,
            precomputed: 2,
            prompt_bytes: 32,
            max_tokens: 16,
            model: "synthetic-route".to_string(),
            tool_count: 3,
            bearer: "eden-gateway-bench".to_string(),
            request_timeout_ms: 5_000,
        };
        let bodies = precompute_requests(&args).unwrap_or_default();
        assert_eq!(bodies.len(), 2);
        let body = std::str::from_utf8(&bodies[0]).unwrap_or_default();
        assert!(body.contains("\"model\":\"synthetic-route\""));
        assert!(body.contains("agent_tool_0"));
        assert!(body.contains("parallel_tool_calls"));
    }

    #[test]
    fn request_stream_detection_accepts_compact_and_spaced_json() {
        assert!(request_wants_stream(br#"{"stream":true}"#));
        assert!(request_wants_stream(br#"{"stream": true}"#));
        assert!(!request_wants_stream(br#"{"stream":false}"#));
    }

    #[test]
    fn responses_stream_chunks_emit_openai_responses_events() {
        let chunks = build_responses_stream_chunks(2, 16);
        let stream = chunks.iter().flat_map(|chunk| chunk.iter().copied()).collect::<Vec<u8>>();
        let stream = std::str::from_utf8(&stream).unwrap_or_default();

        assert!(stream.contains("\"type\":\"response.output_text.delta\""));
        assert!(stream.contains("\"delta\":\"xxxxxxxx\""));
        assert!(stream.contains("\"type\":\"response.completed\""));
        assert!(stream.contains("\"output_tokens\":4"));
        assert!(!stream.contains("chat.completion.chunk"));
    }
}
