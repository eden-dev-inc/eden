//! Default ClickHouse telemetry exporters for Eden metrics, traces, and logs.

use analytics_schema::telemetry::{LogRow, MetricRow, TraceRow, tables};
use chrono::{DateTime, Utc};
use database::db::lib::{ClickhouseConn, EdenTelemetryAnalyticsStorage};
use eden_logger_internal::EdenLog;
use fast_telemetry::clickhouse::{ClickHouseMetricBatch, ExpHistogramRow, GaugeRow, HistogramRow, SumRow};
use fast_telemetry::otlp::{build_resource, build_trace_export_request};
use fast_telemetry::span::{CompletedSpan, SpanAttribute, SpanCollector, SpanKind, SpanStatus, SpanValue};
use fast_telemetry_export::spans::SpanExportConfig;
use flate2::Compression;
use flate2::write::GzEncoder;
use prost::Message;
use serde::Serialize;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use telemetry::labels::{LABEL_TRAFFIC_CLASS, SYSTEM_ORG_UUID, TRAFFIC_CLASS_EXTERNAL, TRAFFIC_CLASS_INTERNAL};
use telemetry::metrics::ClickHouseMetricGroupBatch;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

const GZIP_THRESHOLD: usize = 1024;
type TelemetryStorageError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone)]
pub struct ClickHouseTelemetryConfig {
    pub service_name: String,
    pub node_uuid: String,
    pub interval: Duration,
    pub max_batch_size: usize,
}

impl ClickHouseTelemetryConfig {
    pub fn new(service_name: impl Into<String>, node_uuid: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            node_uuid: node_uuid.into(),
            interval: Duration::from_secs(2),
            max_batch_size: 512,
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
}

pub async fn ensure_tables(pool: &ClickhouseConn) -> Result<(), TelemetryStorageError> {
    pool.ensure_telemetry_tables().await.map_err(box_error)
}

pub async fn run_metrics<F>(pool: ClickhouseConn, config: ClickHouseTelemetryConfig, cancel: CancellationToken, mut collect: F)
where
    F: FnMut(&mut ClickHouseMetricGroupBatch, u64) + Send + 'static,
{
    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    let mut batches = ClickHouseMetricGroupBatch::new(&config.service_name, &config.node_uuid);
    log::info!(
        "Starting ClickHouse telemetry metric exporter: service={}, node={}, interval_ms={}, max_batch_size={}",
        config.service_name,
        config.node_uuid,
        config.interval.as_millis(),
        config.max_batch_size
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => return,
        }

        if let Err(err) = ensure_tables(&pool).await {
            log::error!("ClickHouse telemetry table init failed before metric export: {err}");
            continue;
        }

        batches.clear();
        collect(&mut batches, fast_telemetry::otlp::now_nanos());

        let timestamp = Utc::now();
        for (table, batch) in metric_groups(&batches) {
            let _ = write_metric_group(&pool, table, batch, &config, timestamp).await;
        }
    }
}

fn metric_groups(batches: &ClickHouseMetricGroupBatch) -> [(&'static str, &ClickHouseMetricBatch); 9] {
    [
        (tables::ANALYTICS, &batches.analytics),
        (tables::EDEN, &batches.eden),
        (tables::IAM, &batches.iam),
        (tables::ENDPOINT, &batches.endpoint),
        (tables::METADATA, &batches.metadata),
        (tables::PROXY, &batches.proxy),
        (tables::SNAPSHOT, &batches.snapshot),
        (tables::WORKLOAD, &batches.workload),
        (tables::VALIDATOR, &batches.validator),
    ]
}

async fn write_metric_group(
    pool: &ClickhouseConn,
    table: &str,
    batch: &ClickHouseMetricBatch,
    config: &ClickHouseTelemetryConfig,
    timestamp: DateTime<Utc>,
) -> Result<(), TelemetryStorageError> {
    let rows = metric_rows(batch, config, timestamp);
    if rows.is_empty() {
        return Ok(());
    }
    insert_rows(pool, table, &rows).await.map_err(|err| {
        log::error!("ClickHouse telemetry metric export failed for {table}: {err}");
        err
    })
}

fn metric_rows(batch: &ClickHouseMetricBatch, config: &ClickHouseTelemetryConfig, timestamp: DateTime<Utc>) -> Vec<MetricRow> {
    let mut rows = Vec::with_capacity(batch.total_rows());
    rows.extend(batch.sums.iter().map(|row| sum_row(row, config, timestamp)));
    rows.extend(batch.gauges.iter().map(|row| gauge_row(row, config, timestamp)));
    rows.extend(batch.histograms.iter().map(|row| histogram_row(row, config, timestamp)));
    rows.extend(batch.exp_histograms.iter().map(|row| exp_histogram_row(row, config, timestamp)));
    rows
}

fn sum_row(row: &SumRow, config: &ClickHouseTelemetryConfig, timestamp: DateTime<Utc>) -> MetricRow {
    let mut labels = attrs(&row.Attributes);
    let organization_uuid = ensure_metric_org_label(&mut labels);
    let metric_name = canonical_metric_name(&row.ScopeName, &row.MetricName);
    MetricRow {
        timestamp,
        organization_uuid,
        service_name: row.ServiceName.clone(),
        node_uuid: config.node_uuid.clone(),
        metric_name,
        metric_kind: "sum".to_string(),
        value: Some(row.Value),
        count: None,
        sum: None,
        bucket_bounds: Vec::new(),
        bucket_counts: Vec::new(),
        labels,
        scope: row.ScopeName.clone(),
    }
}

fn gauge_row(row: &GaugeRow, config: &ClickHouseTelemetryConfig, timestamp: DateTime<Utc>) -> MetricRow {
    let mut labels = attrs(&row.Attributes);
    let organization_uuid = ensure_metric_org_label(&mut labels);
    let metric_name = canonical_metric_name(&row.ScopeName, &row.MetricName);
    MetricRow {
        timestamp,
        organization_uuid,
        service_name: row.ServiceName.clone(),
        node_uuid: config.node_uuid.clone(),
        metric_name,
        metric_kind: "gauge".to_string(),
        value: Some(row.Value),
        count: None,
        sum: None,
        bucket_bounds: Vec::new(),
        bucket_counts: Vec::new(),
        labels,
        scope: row.ScopeName.clone(),
    }
}

fn histogram_row(row: &HistogramRow, config: &ClickHouseTelemetryConfig, timestamp: DateTime<Utc>) -> MetricRow {
    let mut labels = attrs(&row.Attributes);
    let organization_uuid = ensure_metric_org_label(&mut labels);
    let metric_name = canonical_metric_name(&row.ScopeName, &row.MetricName);
    MetricRow {
        timestamp,
        organization_uuid,
        service_name: row.ServiceName.clone(),
        node_uuid: config.node_uuid.clone(),
        metric_name,
        metric_kind: "histogram".to_string(),
        value: None,
        count: Some(row.Count),
        sum: Some(row.Sum),
        bucket_bounds: row.ExplicitBounds.clone(),
        bucket_counts: row.BucketCounts.clone(),
        labels,
        scope: row.ScopeName.clone(),
    }
}

fn exp_histogram_row(row: &ExpHistogramRow, config: &ClickHouseTelemetryConfig, timestamp: DateTime<Utc>) -> MetricRow {
    let mut labels = attrs(&row.Attributes);
    labels.push(("scale".to_string(), row.Scale.to_string()));
    labels.push(("positive_offset".to_string(), row.PositiveOffset.to_string()));
    labels.push(("zero_count".to_string(), row.ZeroCount.to_string()));
    let organization_uuid = ensure_metric_org_label(&mut labels);
    let bucket_bounds = exponential_histogram_bounds(row.Scale, row.PositiveOffset, row.PositiveBucketCounts.len());
    let metric_name = canonical_metric_name(&row.ScopeName, &row.MetricName);
    MetricRow {
        timestamp,
        organization_uuid,
        service_name: row.ServiceName.clone(),
        node_uuid: config.node_uuid.clone(),
        metric_name,
        metric_kind: "exponential_histogram".to_string(),
        value: None,
        count: Some(row.Count),
        sum: Some(row.Sum),
        bucket_bounds,
        bucket_counts: row.PositiveBucketCounts.clone(),
        labels,
        scope: row.ScopeName.clone(),
    }
}

fn attrs(attrs: &indexmap::IndexMap<String, String>) -> Vec<(String, String)> {
    attrs.iter().map(|(key, value)| (key.clone(), normalize_label_value(key, value))).collect()
}

fn canonical_metric_name(_scope_name: &str, metric_name: &str) -> String {
    if let Some(canonical) = canonical_fast_telemetry_metric_name(metric_name) {
        return canonical;
    }
    metric_name.to_string()
}

fn canonical_fast_telemetry_metric_name(metric_name: &str) -> Option<String> {
    if let Some(suffix) = metric_name.strip_prefix("gateway_redis_") {
        return Some(format!("gateway.redis.{suffix}"));
    }
    if let Some(suffix) = metric_name.strip_prefix("gateway.redis_") {
        return Some(format!("gateway.redis.{suffix}"));
    }
    if metric_name == "gateway_commands_total" || metric_name == "gateway.commands_total" {
        return Some("gateway.redis.commands_total".to_string());
    }
    if let Some(suffix) = metric_name.strip_prefix("gateway_command_").or_else(|| metric_name.strip_prefix("gateway.command_")) {
        return Some(format!("gateway.redis.command_{suffix}"));
    }
    if let Some(suffix) = metric_name.strip_prefix("gateway_") {
        return Some(format!("gateway.{suffix}"));
    }
    if let Some(suffix) = metric_name.strip_prefix("eden.endpoint_") {
        return Some(format!("eden.endpoint.{suffix}"));
    }
    if let Some(suffix) = metric_name.strip_prefix("eden.iam_") {
        return Some(format!("eden.iam.{suffix}"));
    }
    if let Some(suffix) = metric_name.strip_prefix("eden.validator_") {
        return Some(format!("eden.validator.{suffix}"));
    }
    if let Some(suffix) = metric_name.strip_prefix("eden_") {
        if suffix == "request_count" {
            return Some("eden.request_sent".to_string());
        }
        if let Some(rest) = suffix.strip_prefix("llm_gateway_") {
            return Some(format!("eden.llm.gateway.{rest}"));
        }
        if let Some(rest) = suffix.strip_prefix("llm_") {
            return Some(format!("eden.llm.{rest}"));
        }
        return Some(format!("eden.{suffix}"));
    }
    for prefix in [
        "analytics",
        "migration_governor",
        "migration_live",
        "migration",
        "redis",
        "snapshot",
        "workload",
    ] {
        let raw_prefix = format!("{prefix}_");
        if let Some(suffix) = metric_name.strip_prefix(&raw_prefix) {
            return Some(format!("{prefix}.{suffix}"));
        }
    }
    None
}

fn exponential_histogram_bounds(scale: i32, positive_offset: i32, count_len: usize) -> Vec<f64> {
    let base = 2_f64.powf(2_f64.powi(-scale));
    (0..count_len)
        .map(|index| {
            let bucket_index = positive_offset + index as i32;
            base.powi(bucket_index.saturating_add(1))
        })
        .filter(|value| value.is_finite())
        .collect()
}

fn normalize_label_value(key: &str, value: &str) -> String {
    match key {
        "endpoint_uuid" => value.strip_prefix("endpoint:").unwrap_or(value).to_string(),
        "interlay_uuid" => value.strip_prefix("interlay:").unwrap_or(value).to_string(),
        "org_uuid" | "organization_uuid" => normalize_organization_uuid(value),
        _ => value.to_string(),
    }
}

fn organization_uuid_from_pairs(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .find_map(|(key, value)| matches!(key.as_str(), "organization_uuid" | "org_uuid").then(|| normalize_organization_uuid(value)))
        .unwrap_or_default()
}

fn ensure_metric_org_label(labels: &mut Vec<(String, String)>) -> String {
    let organization_uuid = organization_uuid_from_pairs(labels);
    if has_organization_uuid(&organization_uuid) {
        organization_uuid
    } else {
        labels.push(("org_uuid".to_string(), SYSTEM_ORG_UUID.to_string()));
        SYSTEM_ORG_UUID.to_string()
    }
}

fn normalize_organization_uuid(value: &str) -> String {
    value.strip_prefix("org:").unwrap_or(value).to_string()
}

fn has_organization_uuid(value: &str) -> bool {
    !value.is_empty()
}

fn organization_uuid_or_system(value: &str) -> String {
    let normalized = normalize_organization_uuid(value);
    if has_organization_uuid(&normalized) {
        normalized
    } else {
        SYSTEM_ORG_UUID.to_string()
    }
}

pub async fn run_logs(
    pool: ClickhouseConn,
    config: ClickHouseTelemetryConfig,
    cancel: CancellationToken,
    mut receiver: mpsc::Receiver<EdenLog>,
) {
    let mut rows = Vec::with_capacity(config.max_batch_size);
    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;
    log::info!(
        "Starting ClickHouse telemetry log exporter: service={}, node={}, interval_ms={}, max_batch_size={}",
        config.service_name,
        config.node_uuid,
        config.interval.as_millis(),
        config.max_batch_size
    );

    loop {
        tokio::select! {
            Some(log) = receiver.recv() => {
                rows.push(log_row(log, &config));
                if rows.len() >= config.max_batch_size {
                    flush_logs(&pool, &mut rows).await;
                }
            }
            _ = interval.tick() => flush_logs(&pool, &mut rows).await,
            _ = cancel.cancelled() => {
                flush_logs(&pool, &mut rows).await;
                return;
            }
        }
    }
}

async fn flush_logs(pool: &ClickhouseConn, rows: &mut Vec<LogRow>) {
    if rows.is_empty() {
        return;
    }
    if let Err(err) = ensure_tables(pool).await {
        log::error!("ClickHouse telemetry table init failed before log export: {err}");
        return;
    }
    if let Err(err) = insert_rows(pool, tables::LOGS, rows).await {
        log::error!("ClickHouse telemetry log export failed: {err}");
    } else {
        rows.clear();
    }
}

fn log_row(log: EdenLog, config: &ClickHouseTelemetryConfig) -> LogRow {
    let audience = log.audience.as_str().to_string();
    let mut labels: Vec<_> = log.additional.into_iter().map(|(key, value)| (key.to_string(), value.to_string())).collect();
    if !labels.iter().any(|(key, _)| key == LABEL_TRAFFIC_CLASS) {
        let traffic_class = if audience.eq_ignore_ascii_case("internal") {
            TRAFFIC_CLASS_INTERNAL
        } else {
            TRAFFIC_CLASS_EXTERNAL
        };
        labels.push((LABEL_TRAFFIC_CLASS.to_string(), traffic_class.to_string()));
    }
    let organization_uuid = opt(log.request.organization_uuid);
    let organization_uuid = if organization_uuid.is_empty() {
        organization_uuid_from_pairs(&labels)
    } else {
        normalize_organization_uuid(&organization_uuid)
    };
    let organization_uuid = organization_uuid_or_system(&organization_uuid);

    LogRow {
        timestamp: log.timestamp,
        service_name: config.service_name.clone(),
        node_uuid: config.node_uuid.clone(),
        level: log.level.as_str().to_string(),
        audience,
        message: log.message,
        trace_id: opt(log.trace_id),
        span_id: opt(log.span_id),
        feature: opt(log.feature),
        function: opt(log.function),
        file: opt(log.file),
        line: log.line,
        eden_node_uuid: opt(log.request.eden_node_uuid),
        organization_uuid,
        organization_id: opt(log.request.organization_id),
        user_uuid: opt(log.request.user_uuid),
        user_id: opt(log.request.user_id),
        endpoint_uuid: opt(log.request.endpoint_uuid),
        endpoint_id: opt(log.request.endpoint_id),
        endpoint_kind: opt(log.request.endpoint_kind),
        error_code: opt(log.error_code),
        error_category: opt(log.error_category),
        labels,
    }
}

fn opt(value: Option<smol_str::SmolStr>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

pub async fn run_span_fanout(
    collector: Arc<SpanCollector>,
    otlp_config: Option<SpanExportConfig>,
    clickhouse: Option<(ClickhouseConn, ClickHouseTelemetryConfig)>,
    cancel: CancellationToken,
) {
    let max_batch_size = otlp_config
        .as_ref()
        .map(|config| config.max_batch_size)
        .or_else(|| clickhouse.as_ref().map(|(_, config)| config.max_batch_size))
        .unwrap_or(512);
    let interval_duration = otlp_config
        .as_ref()
        .map(|config| config.interval)
        .or_else(|| clickhouse.as_ref().map(|(_, config)| config.interval))
        .unwrap_or(Duration::from_secs(2));
    let otlp_sink = match otlp_config {
        Some(config) => {
            let url = format!("{}/v1/traces", config.endpoint.trim_end_matches('/'));
            let attr_refs: Vec<(&str, &str)> =
                config.resource_attributes.iter().map(|(key, value)| (key.as_str(), value.as_str())).collect();
            let resource = build_resource(&config.service_name, &attr_refs);
            let client = match reqwest::Client::builder().timeout(config.timeout).build() {
                Ok(client) => client,
                Err(err) => {
                    log::error!("Failed to build HTTP client for span fan-out exporter: {err}");
                    return;
                }
            };
            Some((config, url, resource, client))
        }
        None => None,
    };

    let mut interval = tokio::time::interval(interval_duration);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;
    let mut spans = Vec::with_capacity(max_batch_size);
    let mut encode = Vec::new();
    let mut gzip = Vec::new();
    log::info!(
        "Starting span fan-out exporter: otlp_enabled={}, clickhouse_enabled={}, interval_ms={}, max_batch_size={max_batch_size}",
        otlp_sink.is_some(),
        clickhouse.is_some(),
        interval_duration.as_millis()
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => {
                export_spans_once(
                    &collector,
                    &mut spans,
                    max_batch_size,
                    otlp_sink.as_ref(),
                    clickhouse.as_ref(),
                    &mut encode,
                    &mut gzip,
                )
                .await;
                return;
            }
        }

        export_spans_once(
            &collector,
            &mut spans,
            max_batch_size,
            otlp_sink.as_ref(),
            clickhouse.as_ref(),
            &mut encode,
            &mut gzip,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn export_spans_once(
    collector: &SpanCollector,
    spans: &mut Vec<CompletedSpan>,
    max_batch_size: usize,
    otlp_sink: Option<&(SpanExportConfig, String, fast_telemetry::otlp::pb::Resource, reqwest::Client)>,
    clickhouse: Option<&(ClickhouseConn, ClickHouseTelemetryConfig)>,
    encode: &mut Vec<u8>,
    gzip: &mut Vec<u8>,
) {
    spans.clear();
    collector.drain_into(spans);
    if spans.is_empty() {
        return;
    }
    spans.truncate(max_batch_size);

    if let Some((otlp_config, url, resource, client)) = otlp_sink {
        let otlp_spans = spans.iter().map(|span| span.to_otlp()).collect();
        let request = build_trace_export_request(resource, &otlp_config.scope_name, otlp_spans);
        encode.clear();
        if let Err(err) = request.encode(encode) {
            log::warn!("Span protobuf encode failed: {err}");
        } else {
            match send_otlp(client, url, encode, gzip, &otlp_config.headers).await {
                Ok(resp) if resp.status().is_success() => {}
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    log::warn!("Span export failed: status={status}, body={body}");
                }
                Err(err) => log::warn!("Span export request failed: {err}"),
            }
        }
    }

    if let Some((pool, config)) = clickhouse {
        write_spans(pool, config, spans).await;
    }
}

async fn write_spans(pool: &ClickhouseConn, config: &ClickHouseTelemetryConfig, spans: &[CompletedSpan]) {
    if let Err(err) = ensure_tables(pool).await {
        log::error!("ClickHouse telemetry table init failed before trace export: {err}");
        return;
    }
    let rows = spans.iter().map(|span| trace_row(span, config)).collect::<Vec<_>>();
    if rows.is_empty() {
        return;
    }
    if let Err(err) = insert_rows(pool, tables::TRACES, &rows).await {
        log::error!("ClickHouse telemetry trace export failed: {err}");
    }
}

async fn insert_rows<T>(pool: &ClickhouseConn, table: &str, rows: &[T]) -> Result<(), TelemetryStorageError>
where
    T: clickhouse::Row + Serialize + Sync,
{
    pool.insert_telemetry_rows(table, rows).await.map_err(box_error)
}

fn box_error<E>(error: E) -> TelemetryStorageError
where
    E: std::error::Error + Send + Sync + 'static,
{
    Box::new(error)
}

fn trace_row(span: &CompletedSpan, config: &ClickHouseTelemetryConfig) -> TraceRow {
    let status_message = match &span.status {
        SpanStatus::Error { message } => message.to_string(),
        _ => String::new(),
    };
    TraceRow {
        timestamp: ns_to_datetime(span.end_time_ns),
        organization_uuid: organization_uuid_or_system(&span_attribute(&span.attributes, &["organization_uuid", "org_uuid"])),
        service_name: config.service_name.clone(),
        node_uuid: config.node_uuid.clone(),
        trace_id: span.trace_id.to_string(),
        span_id: span.span_id.to_string(),
        parent_span_id: if span.parent_span_id.is_invalid() {
            String::new()
        } else {
            span.parent_span_id.to_string()
        },
        span_name: span.name.to_string(),
        span_kind: span_kind(span.kind).to_string(),
        start_time: ns_to_datetime(span.start_time_ns),
        end_time: ns_to_datetime(span.end_time_ns),
        duration_ns: span.end_time_ns.saturating_sub(span.start_time_ns),
        status: span_status(&span.status).to_string(),
        status_message,
        attributes: span_attrs(&span.attributes),
        events_json: span_events_json(&span.events),
    }
}

fn span_attrs(attrs: &[SpanAttribute]) -> Vec<(String, String)> {
    attrs.iter().map(|attr| (attr.key.to_string(), span_value(&attr.value))).collect()
}

fn span_attribute(attrs: &[SpanAttribute], keys: &[&str]) -> String {
    attrs.iter().find_map(|attr| keys.contains(&attr.key.as_ref()).then(|| span_value(&attr.value))).unwrap_or_default()
}

fn span_value(value: &SpanValue) -> String {
    match value {
        SpanValue::String(value) => value.to_string(),
        SpanValue::I64(value) => value.to_string(),
        SpanValue::F64(value) => value.to_string(),
        SpanValue::Bool(value) => value.to_string(),
        SpanValue::Uuid(value) => value.to_string(),
    }
}

fn span_events_json(events: &[fast_telemetry::span::SpanEvent]) -> String {
    #[derive(Serialize)]
    struct Event<'a> {
        name: &'a str,
        time_ns: u64,
        attributes: Vec<(String, String)>,
    }

    let events: Vec<_> = events
        .iter()
        .map(|event| Event {
            name: &event.name,
            time_ns: event.time_ns,
            attributes: span_attrs(&event.attributes),
        })
        .collect();
    serde_json::to_string(&events).unwrap_or_else(|_| "[]".to_string())
}

fn span_kind(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Internal => "internal",
        SpanKind::Server => "server",
        SpanKind::Client => "client",
        SpanKind::Producer => "producer",
        SpanKind::Consumer => "consumer",
    }
}

fn span_status(status: &SpanStatus) -> &'static str {
    match status {
        SpanStatus::Unset => "unset",
        SpanStatus::Ok => "ok",
        SpanStatus::Error { .. } => "error",
    }
}

fn ns_to_datetime(ns: u64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp((ns / 1_000_000_000) as i64, (ns % 1_000_000_000) as u32).unwrap_or_else(Utc::now)
}

fn gzip_compress(data: &[u8], out: &mut Vec<u8>) -> bool {
    if data.len() < GZIP_THRESHOLD {
        return false;
    }
    out.clear();
    let mut encoder = GzEncoder::new(out, Compression::fast());
    let _ = encoder.write_all(data);
    let _ = encoder.finish();
    true
}

async fn send_otlp(
    client: &reqwest::Client,
    url: &str,
    body: &[u8],
    gzip_buf: &mut Vec<u8>,
    extra_headers: &[(String, String)],
) -> Result<reqwest::Response, reqwest::Error> {
    let mut request = client.post(url).header("Content-Type", "application/x-protobuf");

    for (name, value) in extra_headers {
        request = request.header(name, value);
    }

    if gzip_compress(body, gzip_buf) {
        request.header("Content-Encoding", "gzip").body(gzip_buf.clone()).send().await
    } else {
        request.body(body.to_vec()).send().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fast_telemetry::clickhouse::ClickHouseExport;
    use fast_telemetry::{Counter, Distribution, SpanId, TraceId};
    use std::borrow::Cow;

    #[test]
    fn metric_rows_convert_native_sum_rows() {
        let counter = Counter::new(1);
        counter.inc();
        let mut batch = ClickHouseMetricBatch::with_scope("eden", "proxy");
        counter.export_clickhouse(&mut batch, "gateway.requests_total", "requests", 123);
        batch.sums[0].Attributes.insert("org_uuid".to_string(), "org:test-org".to_string());

        let rows = metric_rows(
            &batch,
            &ClickHouseTelemetryConfig::new("eden", "node-1"),
            DateTime::<Utc>::from_timestamp(0, 0).expect("epoch"),
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].metric_name, "gateway.requests_total");
        assert_eq!(rows[0].metric_kind, "sum");
        assert_eq!(rows[0].value, Some(1.0));
        assert_eq!(rows[0].scope, "proxy");
        assert_eq!(rows[0].node_uuid, "node-1");
        assert_eq!(rows[0].organization_uuid, "test-org");
    }

    #[test]
    fn metric_rows_store_gateway_metrics_with_dotted_gateway_namespace() {
        let counter = Counter::new(1);
        counter.inc();
        let mut batch = ClickHouseMetricBatch::with_scope("eden", "proxy");
        counter.export_clickhouse(&mut batch, "gateway_requests_total", "requests", 123);
        batch.sums[0].Attributes.insert("org_uuid".to_string(), "org:test-org".to_string());

        let rows = metric_rows(
            &batch,
            &ClickHouseTelemetryConfig::new("eden", "node-1"),
            DateTime::<Utc>::from_timestamp(0, 0).expect("epoch"),
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].metric_name, "gateway.requests_total");
    }

    #[test]
    fn metric_rows_canonicalize_eden_underscore_metric_names() {
        let counter = Counter::new(1);
        counter.inc();
        let mut batch = ClickHouseMetricBatch::with_scope("eden", "eden");
        counter.export_clickhouse(&mut batch, "eden_active_requests", "active requests", 123);
        batch.sums[0].Attributes.insert("org_uuid".to_string(), "org:test-org".to_string());

        let rows = metric_rows(
            &batch,
            &ClickHouseTelemetryConfig::new("eden", "node-1"),
            DateTime::<Utc>::from_timestamp(0, 0).expect("epoch"),
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].metric_name, "eden.active_requests");
    }

    #[test]
    fn metric_rows_assign_system_org_to_unscoped_rows() {
        let counter = Counter::new(1);
        counter.inc();
        let mut batch = ClickHouseMetricBatch::with_scope("eden", "analytics");
        counter.export_clickhouse(&mut batch, "analytics.events_sampled", "events sampled", 123);

        let rows = metric_rows(
            &batch,
            &ClickHouseTelemetryConfig::new("eden", "node-1"),
            DateTime::<Utc>::from_timestamp(0, 0).expect("epoch"),
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].organization_uuid, SYSTEM_ORG_UUID);
        assert!(rows[0].labels.iter().any(|(key, value)| key == "org_uuid" && value == SYSTEM_ORG_UUID));
    }

    #[test]
    fn metric_rows_convert_exponential_histogram_bounds_and_label_prefixes() {
        let distribution = Distribution::new(1);
        distribution.record(13);
        distribution.record(42);
        distribution.record(99);
        let mut batch = ClickHouseMetricBatch::with_scope("eden", "proxy");
        distribution.export_clickhouse(&mut batch, "gateway.overhead_duration_microseconds", "overhead", 123);
        batch.exp_histograms[0].Attributes.insert("org_uuid".to_string(), "org:test-org".to_string());
        batch.exp_histograms[0].Attributes.insert("endpoint_uuid".to_string(), "endpoint:endpoint-1".to_string());
        batch.exp_histograms[0].Attributes.insert("interlay_uuid".to_string(), "interlay:interlay-1".to_string());

        let rows = metric_rows(
            &batch,
            &ClickHouseTelemetryConfig::new("eden", "node-1"),
            DateTime::<Utc>::from_timestamp(0, 0).expect("epoch"),
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].metric_kind, "exponential_histogram");
        assert_eq!(rows[0].bucket_counts.len(), rows[0].bucket_bounds.len());
        assert_eq!(rows[0].bucket_bounds, vec![16.0, 32.0, 64.0, 128.0]);
        assert_eq!(rows[0].organization_uuid, "test-org");
        assert!(rows[0].labels.contains(&("endpoint_uuid".to_string(), "endpoint-1".to_string())));
        assert!(rows[0].labels.contains(&("interlay_uuid".to_string(), "interlay-1".to_string())));
    }

    #[test]
    fn trace_rows_preserve_span_identity_and_attributes() {
        let span = CompletedSpan {
            trace_id: TraceId::from_hex("00000000000000000000000000000001").expect("trace id"),
            span_id: SpanId::from_hex("0000000000000002").expect("span id"),
            parent_span_id: SpanId::INVALID,
            name: Cow::Borrowed("handle_request"),
            kind: SpanKind::Server,
            start_time_ns: 1_000_000_000,
            end_time_ns: 1_000_010_000,
            status: SpanStatus::Ok,
            attributes: vec![
                SpanAttribute::new("org_uuid", "org:test-org"),
                SpanAttribute::new("http.method", "GET"),
            ],
            events: Vec::new(),
        };

        let row = trace_row(&span, &ClickHouseTelemetryConfig::new("eden", "node-1"));
        assert_eq!(row.trace_id, "00000000000000000000000000000001");
        assert_eq!(row.span_id, "0000000000000002");
        assert_eq!(row.parent_span_id, "");
        assert_eq!(row.span_kind, "server");
        assert_eq!(row.status, "ok");
        assert_eq!(row.organization_uuid, "test-org");
        assert_eq!(
            row.attributes,
            vec![
                ("org_uuid".to_string(), "org:test-org".to_string()),
                ("http.method".to_string(), "GET".to_string())
            ]
        );
    }

    #[test]
    fn trace_rows_without_org_uuid_use_system_org() {
        let span = CompletedSpan {
            trace_id: TraceId::from_hex("00000000000000000000000000000001").expect("trace id"),
            span_id: SpanId::from_hex("0000000000000002").expect("span id"),
            parent_span_id: SpanId::INVALID,
            name: Cow::Borrowed("handle_request"),
            kind: SpanKind::Server,
            start_time_ns: 1_000_000_000,
            end_time_ns: 1_000_010_000,
            status: SpanStatus::Ok,
            attributes: vec![SpanAttribute::new("http.method", "GET")],
            events: Vec::new(),
        };

        let row = trace_row(&span, &ClickHouseTelemetryConfig::new("eden", "node-1"));
        assert_eq!(row.organization_uuid, SYSTEM_ORG_UUID);
        assert!(has_organization_uuid(&row.organization_uuid));
    }

    #[test]
    fn log_rows_use_request_label_or_system_org_uuid() {
        let config = ClickHouseTelemetryConfig::new("eden", "node-1");
        let timestamp = DateTime::<Utc>::from_timestamp(0, 0).expect("epoch");
        let with_request_org = EdenLog {
            timestamp,
            level: eden_logger_internal::LogLevel::Info,
            audience: eden_logger_internal::LogAudience::Internal,
            message: "request org".to_string(),
            trace_id: None,
            span_id: None,
            feature: None,
            function: None,
            file: None,
            line: None,
            request: eden_logger_internal::EdenRequestFields {
                organization_uuid: Some("org:request-org".into()),
                ..Default::default()
            },
            error_code: None,
            error_category: None,
            additional: Default::default(),
        };
        let request_row = log_row(with_request_org, &config);
        assert_eq!(request_row.organization_uuid, "request-org");
        assert!(has_organization_uuid(&request_row.organization_uuid));

        let mut additional = std::collections::HashMap::new();
        additional.insert("org_uuid".into(), "org:label-org".into());
        let with_label_org = EdenLog {
            timestamp,
            level: eden_logger_internal::LogLevel::Info,
            audience: eden_logger_internal::LogAudience::Internal,
            message: "label org".to_string(),
            trace_id: None,
            span_id: None,
            feature: None,
            function: None,
            file: None,
            line: None,
            request: Default::default(),
            error_code: None,
            error_category: None,
            additional,
        };
        let label_row = log_row(with_label_org, &config);
        assert_eq!(label_row.organization_uuid, "label-org");
        assert!(has_organization_uuid(&label_row.organization_uuid));

        let without_org = EdenLog {
            timestamp,
            level: eden_logger_internal::LogLevel::Info,
            audience: eden_logger_internal::LogAudience::Internal,
            message: "missing org".to_string(),
            trace_id: None,
            span_id: None,
            feature: None,
            function: None,
            file: None,
            line: None,
            request: Default::default(),
            error_code: None,
            error_category: None,
            additional: Default::default(),
        };
        let missing_row = log_row(without_org, &config);
        assert_eq!(missing_row.organization_uuid, SYSTEM_ORG_UUID);
        assert!(has_organization_uuid(&missing_row.organization_uuid));
    }

    #[test]
    fn metric_groups_route_to_expected_tables() {
        let batches = ClickHouseMetricGroupBatch::new("eden", "node-1");
        let tables: Vec<_> = metric_groups(&batches).iter().map(|(table, _)| *table).collect();

        assert_eq!(
            tables,
            vec![
                "analytics.analytics",
                "analytics.eden",
                "analytics.iam",
                "analytics.endpoint",
                "analytics.metadata",
                "analytics.migration",
                "analytics.migration_live",
                "analytics.proxy",
                "analytics.snapshot",
                "analytics.workload",
                "analytics.validator",
                "analytics.migration_governor",
            ]
        );
    }

    #[test]
    fn canonical_metric_name_matches_dashboard_contracts() {
        assert_eq!(canonical_metric_name("gateway", "gateway_requests_total"), "gateway.requests_total");
        assert_eq!(canonical_metric_name("gateway", "gateway_redis_commands_total"), "gateway.redis.commands_total");
        assert_eq!(
            canonical_metric_name("gateway", "gateway_redis_command_end_to_end_microseconds"),
            "gateway.redis.command_end_to_end_microseconds"
        );
        assert_eq!(
            canonical_metric_name("gateway", "gateway_command_duration_microseconds"),
            "gateway.redis.command_duration_microseconds"
        );
        assert_eq!(canonical_metric_name("eden", "eden_request_count"), "eden.request_sent");
        assert_eq!(canonical_metric_name("eden", "eden_eden_duration"), "eden.eden_duration");
        assert_eq!(canonical_metric_name("eden", "eden_llm_gateway_requests"), "eden.llm.gateway.requests");
        assert_eq!(
            canonical_metric_name("endpoint", "eden.endpoint_endpoint_duration"),
            "eden.endpoint.endpoint_duration"
        );
        assert_eq!(canonical_metric_name("workload", "workload_workload_ratio"), "workload.workload_ratio");
        assert_eq!(canonical_metric_name("validator", "eden.validator_safe_total"), "eden.validator.safe_total");
    }
}
