# Single Organization API Documentation

## Base URL

```
https://api.example.com/v1
```

## Authentication

Every request requires authentication using either:

1. Basic auth with username/password
2. Bearer token (recommended)

```
# Basic Auth
Authorization: Basic base64(username:password)

# Bearer Token (JWT)
Authorization: Bearer <your_token>
```

## Authentication Endpoints

### Login

```
POST /auth/login
```

Get a JWT token for subsequent requests.

**Request Body:**

```json
{
  "email": "user@example.com",
  "password": "password123"
}
```

**Response:**

```json
{
  "accessToken": "eyJ0eXAiOiJKV...",
  "refreshToken": "eyJ0eXAiOiJKV...",
  "expiresIn": 3600,
  "organizationId": "org_123"
}
```

### Refresh Token

```
POST /auth/refresh
```

Get new access token using refresh token.

**Request Body:**

```json
{
  "refreshToken": "eyJ0eXAiOiJKV..."
}
```

**Response:** Same as login response

## Organization Management

### Get Organization

```
GET /organization
```

Get details of user's organization.

**Response:**

```json
{
  "id": "org_123",
  "name": "Example Org",
  "createdAt": "2024-01-01T00:00:00Z"
}
```

### Update Organization

```
PATCH /organization
```

Update organization details.

**Request Body:**

```json
{
  "name": "Updated Name",
  "settings": {
    "setting1": "value1"
  }
}
```

## Endpoint Management

### List Endpoints

```
GET /endpoints
```

Get all endpoints in user's organization.

### Create Endpoint

```
POST /endpoints
```

Create new endpoint in organization.

**Request Body:**

```json
{
  "name": "New Endpoint",
  "connectionDetails": {
    "host": "example.com",
    "port": 443
  }
}
```

### Get Endpoint

```
GET /endpoints/{endpointId}
```

Get specific endpoint details.

### Update Endpoint

```
PATCH /endpoints/{endpointId}
```

Update endpoint details.

**Request Body:**

```json
{
  "name": "Updated Endpoint",
  "connectionDetails": {
    "host": "new.example.com"
  },
  "settings": {
    "timeout": 30
  }
}
```

### Delete Endpoint

```
DELETE /endpoints/{endpointId}
```

Delete an endpoint.

## Interlay Mirror Mode

Existing interlay create, patch, get, and list responses include `settings.mirror`.
When enabled, the interlay primary endpoint still serves the client response and
Eden mirrors eligible traffic asynchronously to the configured secondary
endpoints.

```json
{
  "settings": {
    "mirror": {
      "enabled": true,
      "mode": "mirror",
      "mirror_endpoint_uuids": ["7a2e4f1b-0000-0000-0000-000000000000"],
      "mirror_reads": true,
      "mirror_writes": true,
      "sample_ratio": 1.0,
      "max_in_flight_per_mirror": 128
    }
  }
}
```

Mirror endpoints must be direct endpoints in the same organization and of the
same endpoint kind as the primary. Mirror Mode is best effort and does not
replace data-replication or cutover workflows.

## IAM Permissions

### Get My Endpoint Access

```
GET /iam/access/endpoints/{endpointId}
```

Returns the caller's resolved control-plane and data-plane access for that
endpoint.

**Response:**

```json
{
  "control_plane": {
    "organization_perms": "RG",
    "endpoint_perms": "RCPA"
  },
  "data_plane": {
    "mode": "shared_rbac",
    "shared_perms": "r",
    "els_assignment": null
  }
}
```

### Get Endpoint Security Summary

```
GET /iam/security/endpoints/{endpointId}
```

Returns a redacted summary of shared credential slots plus ELS policy and
assignment counts for one endpoint.

### Set Endpoint Control-Plane Permissions

```
PUT /iam/control/endpoints/{endpointId}/subjects/{subject}
```

Sets one subject's exact non-empty control-plane grant. Use `DELETE` to revoke.

**Request Body:**

```json
{
  "perms": "RCPA"
}
```

### Set Endpoint Data-Plane Permissions

```
PUT /iam/data/endpoints/{endpointId}/subjects/{subject}
```

Sets one subject's exact non-empty shared runtime grant. Use `DELETE` to revoke.

**Request Body:**

```json
{
  "perms": "rw"
}
```

### List Endpoint Control-Plane Grants

```
GET /iam/control/endpoints/{endpointId}
```

### List Endpoint Shared Runtime Grants

```
GET /iam/data/endpoints/{endpointId}
```

## Removed Bulk Data Movement APIs

The bulk data movement API family is not included in this distribution.

## Metering API (V1)

### Ingest Metering Events

```
POST /v1/metering/events
```

Ingests billing-agnostic metering events. The server persists:

1. Raw immutable events (`metering_events`, `metering_event_measurements`)
2. Aggregated monthly totals (`metering_period_totals`)

Auth:

- Bearer metering ingest API key (`EDEN_METERING_INGEST_API_KEY`).
- Compatibility mode: if `EDEN_METERING_INGEST_USE_ADMIN_KEY=true`, the admin API key is accepted for metering ingest.
- Exporter compatibility requires the bearer token it sends to match the server's active metering ingest token. A mismatch returns `401`, and the current exporter treats that as a non-retryable drop.

**Request Body:**

```json
{
  "schema_version": 1,
  "source": {
    "service": "eden-service",
    "instance": "node-123"
  },
  "events": [
    {
      "event_id": "8b5f7744-f486-4f0f-b5db-187d52e2aa72",
      "org_uuid": "org_abc123",
      "license_id": null,
      "emitted_at": "2026-02-24T18:00:00Z",
      "window": {
        "start": "2026-02-24T17:55:00Z",
        "end": "2026-02-24T18:00:00Z",
        "mode": "delta"
      },
      "measurements": [
        {
          "meter_key": "io.operations",
          "quantity": 128940,
          "unit": "count",
          "dimensions": {}
        }
      ]
    }
  ]
}
```

**Response:**

```json
{
  "accepted_events": 12,
  "duplicate_events": 1,
  "rejected_events": 0
}
```

Validation rules:

1. `schema_version` must be `1`.
2. `events` must be non-empty.
3. `event_id` must be valid UUID.
4. `measurements` must be non-empty.
5. `quantity >= 0`.
6. `window.end > window.start`.
7. V1 accepts only `window.mode = "delta"`.
8. V1 meter keys are restricted to `io.operations`, `ai.tokens`, `bytes_in`, and `bytes_out`.

Idempotency and aggregation:

1. Duplicate `event_id` is a no-op and counted in `duplicate_events`.
2. Accepted events are written in one transaction for raw + aggregate writes.

## Operator Memory API

Optional endpoints exposed by `eden-memory-store` when the crate is compiled with the `api` feature and mounted by a host Actix service.

### Create Memory

```
POST /memory
```

Creates an operator-provided memory entry.

**Request Body:**

```json
{
  "user_id": "9eb7c2ae-0c8d-4d6e-b6e3-8fd053d4aa5f",
  "organization_uuid": "f9655a3d-fce6-4972-abf9-40ba655a9095",
  "kind": {
    "type": "ApprovalPattern",
    "action_type": "DROP TABLE users",
    "was_approved": true,
    "approver_role": null,
    "reason": null
  },
  "summary": "Production schema changes by this operator are usually approved after DBA review.",
  "knowledge_scope": {
    "scope_type": "Endpoint",
    "endpoint_uuid": "4a89fc5d-dbff-44dc-af4a-1534e6ab2ca8"
  },
  "confidence": 1.0
}
```

### List Memories

```
GET /memory/users/{user_id}?organization_uuid={uuid}&status=active&kind=approval_pattern&limit=50
```

Returns memories for the given user, newest first. `organization_uuid` is required for tenant isolation. `status`, `kind` and `limit` are optional query parameters.

### Get Memory

```
GET /memory/{id}
```

Returns the stored memory record or `404` if it does not exist.

### Update Memory Status

```
PATCH /memory/{id}/status
```

**Request Body:**

```json
{
  "status": "archived"
}
```

### Confirm Memory

```
POST /memory/{id}/confirm
```

Refreshes `last_confirmed_at`, restores the memory to `active`, and can optionally update confidence.

**Request Body:**

```json
{
  "confidence": 0.95
}
```

3. Aggregates upsert per `(org_uuid, billing_period_start, meter_key)`.
4. Default exporter `ai.tokens` emission is `kind=total` only.
5. If exporter split mode is enabled, raw ingest includes `kind=total`, `kind=prompt`, and `kind=completion` measurements, but V1 aggregates still roll up by `meter_key` only. For example, `100 total + 40 prompt + 60 completion` becomes a single `ai.tokens.total_quantity = 200`.

### Read Metering Totals (Internal)

```
GET /v1/metering/totals?org_uuid={org_uuid}&billing_period_start={YYYY-MM-DD}[&billing_period_end={YYYY-MM-DD}]
```

Returns aggregated period totals from `metering_period_totals` for one organization and billing period.

Auth:

- Bearer admin API key (`EDEN_ADMIN_API_KEY`).

Query parameters:

1. `org_uuid` (required, non-empty string)
2. `billing_period_start` (required, `YYYY-MM-DD`)
3. `billing_period_end` (optional, `YYYY-MM-DD`, must be greater than `billing_period_start` when present)

**Response:**

```json
{
  "org_uuid": "org_abc123",
  "billing_period_start": "2026-02-01",
  "billing_period_end": null,
  "meters": [
    {
      "meter_key": "io.operations",
      "total_quantity": 128940,
      "event_count": 17,
      "first_event_at": "2026-02-01T00:00:10Z",
      "last_event_at": "2026-02-28T23:59:58Z"
    },
    {
      "meter_key": "bytes_in",
      "total_quantity": 4096,
      "event_count": 3,
      "first_event_at": "2026-02-10T11:00:00Z",
      "last_event_at": "2026-02-11T11:00:00Z"
    }
  ]
}
```

Notes:

1. `meters` is empty when no totals exist for the requested org/period.
2. Meter keys currently supported are `io.operations`, `ai.tokens`, `bytes_in`, and `bytes_out`.
3. Totals are keyed by `org_uuid` + `billing_period_start`; `billing_period_end` is validated but does not need to exactly match stored canonical month-end boundaries.

### Evaluate Entitlements (Internal, MVP)

```
GET /v1/entitlements/evaluate?org_uuid={org_uuid}[&billing_period_start={YYYY-MM-DD}]
```

Computes deterministic entitlement decisions for an organization and billing period from:

1. Org subscription
2. Billing policy meter rules
3. Metering period totals

Current compatibility note:

1. Metering ingest writes totals under `org_uuid`.
2. The entitlement read path uses the same `org_uuid` value to look up those totals.

Auth:

- Bearer operator token

Query parameters:

1. `org_uuid` (required, non-empty string)
2. `billing_period_start` (optional, `YYYY-MM-DD`; defaults to current UTC month start)

**Response:**

```json
{
  "org_uuid": "org_abc123",
  "billing_period_start": "2026-02-01",
  "billing_period_end": "2026-03-01",
  "status": "warning",
  "reasons": [
    {
      "code": "near_limit",
      "message": "Usage 80 reached warning threshold 80 for meter io.bytes.in",
      "meter_key": "io.bytes.in"
    }
  ],
  "meters": [
    {
      "meter_key": "io.bytes.in",
      "status": "warning",
      "used_quantity": 80,
      "warning_quantity": 80,
      "hard_limit": 100,
      "reasons": [
        {
          "code": "near_limit",
          "message": "Usage 80 reached warning threshold 80 for meter io.bytes.in",
          "meter_key": "io.bytes.in"
        }
      ]
    }
  ]
}
```

Statuses:

1. `active`
2. `grace_period`
3. `warning`
4. `over_limit`
5. `suspended`

Typed errors:

1. `MISSING_SUBSCRIPTION` (`404`)
2. `MISSING_POLICY` (`404`)

### Query Endpoint

```
POST /endpoints/{endpointId}/query
```

Send query to specific endpoint.

**Request Body:**

```json
{
  "query": "SELECT * FROM table",
  "parameters": {
    "param1": "value1"
  }
}
```

## IAM Management

### Humans

#### List Humans

```
GET /iam/humans
```

Get all humans in organization.

#### Create Human

```
POST /iam/humans
```

Create new human in organization.

**Request Body:**

```json
{
  "username": "newuser",
  "email": "user@example.com",
  "roles": ["role_id_1", "role_id_2"]
}
```

#### Get Human

```
GET /iam/humans/{humanId}
```

Get specific human details.

#### Update Human

```
PATCH /iam/humans/{humanId}
```

Update human details.

**Request Body:**

```json
{
  "username": "updateduser",
  "roles": ["role_id_3"],
  "permissions": ["permission1"]
}
```

#### Delete Human

```
DELETE /iam/humans/{humanId}
```

Delete a human.

### Roles

#### List Roles

```
GET /iam/roles
```

Get all roles in organization.

#### Create Role

```
POST /iam/roles
```

Create new role in organization.

**Request Body:**

```json
{
  "name": "Admin",
  "permissions": ["read", "write", "delete"]
}
```

#### Get Role

```
GET /iam/roles/{roleId}
```

Get specific role details.

#### Update Role

```
PATCH /iam/roles/{roleId}
```

Update role details.

**Request Body:**

```json
{
  "name": "Super Admin",
  "description": "Updated role description",
  "permissions": ["read", "write", "delete", "admin"]
}
```

#### Delete Role

```
DELETE /iam/roles/{roleId}
```

Delete a role.

## Endpoint Metadata History

### Get Poll Metrics History

```
GET /endpoints/{endpointId}/metadata/history
```

Returns historical polling metrics for an endpoint from ClickHouse. Automatically selects the correct per-DB-kind table (Redis, PostgreSQL, MongoDB, Oracle, Cassandra or ClickHouse).

**Feature gate:** `poll-clickhouse`

**RBAC:** Read

**Query Parameters:**
| Parameter | Type | Default | Description |
|-------------|--------|---------|---------------------------------------------------------------------------------------|
| `range` | string | `24h` | Lookback window. Accepts flexible durations: `30m`, `6h`, `2h30m`, `7d` (max `365d`). |
| `frequency` | string | | Filter by collection frequency label (e.g. `10s`, `60s`). |
| `limit` | number | `200` | Maximum data points to return. |

**Response:**

```json
{
  "endpoint_uuid": "ep_abc123",
  "endpoint_kind": "redis",
  "points": [
    {
      "snapshot_time": "2026-02-22T12:00:00.000",
      "frequency": "10s",
      "collection_ms": 42,
      "had_fatal": false,
      "db_specific": {
        "connected_clients": 15,
        "used_memory_bytes": 104857600,
        "...": "..."
      }
    }
  ]
}
```

The `db_specific` object contains all columns from the DB-kind-specific poll table that are not part of the universal set (`snapshot_time`, `organization_uuid`, `endpoint_uuid`, `frequency`, `collection_ms`, `had_fatal`). The exact fields vary by endpoint kind.

---

## Organization Analytics

### Dashboard

```
GET /analytics/dashboard
```

Returns organization-wide analytics across endpoints for the selected time range.

**Availability:** The compatibility route returns an explicit unavailable response in this distribution. Use `/analytics/overview`, `/analytics/series`, and `/analytics/export` for live telemetry-backed dashboard data.

**RBAC:** Control-plane Read

**Query Parameters:**
| Parameter | Type | Default | Description |
|----------------|--------|---------|---------------------------------------------------------------------|
| `range` | string | `24h` | Lookback window. Accepts `all`, `5m`, `15m`, `1h`, `6h`, `24h`, `7d`, `30d`. |
| `time_range` | string | | Legacy alias for `range`. |
| `ep_kind` | string | | Optional endpoint kind filter. |
| `endpoints` | string | | Optional comma-separated endpoint UUID filter. |
| `endpoint_uuids` | string | | Legacy alias for `endpoints`. |
| `user_uuid` | string | | Optional user filter for traffic metrics. |
| `include_series_breakdown` | bool | `false` | Include nested `by_endpoint` maps in each time bucket for by-endpoint charts. |

**Response (excerpt):**

```json
{
  "kpi": {
    "total_requests": 22658767,
    "total_requests_prev": 21190211,
    "unique_users": 12,
    "unique_users_prev": 11,
    "avg_latency_ms": 0.074,
    "avg_latency_ms_prev": 0.081,
    "error_rate": 0.0,
    "error_rate_prev": 0.0
  },
  "time_series": [
    {
      "timestamp": "2026-04-15 19:00:00",
      "requests": 152430,
      "errors": 0,
      "unique_users": 4,
      "p50_latency_ms": 0.05,
      "p95_latency_ms": 0.12,
      "p99_latency_ms": 0.18,
      "error_rate": 0.0,
      "by_endpoint": {
        "endpoint:507b72cf-740b-4f0f-8733-9249da081965": {
          "requests": 92430,
          "errors": 0,
          "unique_users": 3,
          "avg_latency_ms": 0.06,
          "p50_latency_ms": 0.04,
          "p95_latency_ms": 0.11,
          "p99_latency_ms": 0.17,
          "error_rate": 0.0
        }
      }
    }
  ],
  "data_size_series": [
    {
      "timestamp": "2026-04-15 19:00:00",
      "total_bytes": 104857600,
      "by_endpoint": {
        "endpoint:507b72cf-740b-4f0f-8733-9249da081965": 62914560,
        "d9a2500c-db12-4d60-9a2e-9cdf2f3ddec7": 41943040
      }
    }
  ],
  "endpoints": [],
  "top_users": [],
  "latency_distribution": [],
  "error_breakdown": [],
  "filters_applied": {
    "time_range": "all"
  },
  "generated_at": "2026-04-15T19:10:20Z"
}
```

`time_series[].by_endpoint` and `data_size_series[].by_endpoint` are omitted by default to keep the dashboard payload small. Clients that need by-endpoint chart breakdowns can opt in with `include_series_breakdown=true`. When present, `time_series[].by_endpoint[*]` includes request/error counts plus average, `p50`, `p95`, and `p99` latency values for that endpoint bucket.

`data_size_series` is a gauge-like time series derived from database metadata snapshots, not from request counts. It is currently populated for Redis (`analytics.redis_poll_metrics.used_memory`), PostgreSQL, Oracle, and ClickHouse endpoints. Each point may also include a `by_endpoint` map with the latest known size per endpoint for that bucket, which powers the dashboard's by-endpoint size view. User-scoped dashboard queries return an empty `data_size_series`, because database size is endpoint state rather than a per-user metric.

### Connection Metrics

```
GET /analytics/connections
```

Returns current live connection counts plus historical connection metrics for the selected range.

**Availability:** Live connection summaries are available from in-process telemetry; historical data requires the telemetry store.

**RBAC:** Control-plane Read

**Query Parameters:**
| Parameter | Type | Default | Description |
|----------------|--------|---------|---------------------------------------------------------------------|
| `range` | string | `1h` | Lookback window. Accepts `5m`, `15m`, `1h`, `6h`, `24h`, `7d`, `30d`. |
| `since` | string | | Optional RFC3339 timestamp. When provided, historical buckets at or after this timestamp are returned so clients can merge only newly changed points. |

`since` is inclusive because buckets can continue accumulating samples until the bucket closes. Clients should merge returned points by timestamp, allowing the latest cached bucket to be replaced by the refreshed value.

---

## Endpoint-Scoped Analytics

These endpoints live under `/endpoints/{endpointId}/analytics/` and provide per-endpoint traffic analysis data from the configured analytics store when that data is available.

**Availability:** Verbose per-command analytics are not included in this distribution. Compatibility routes return explicit unavailable responses instead of silently falling through to a missing route.

**Duration format:** All `range` parameters accept flexible human-readable durations: single units (`30s`, `45m`, `6h`, `7d`), compound (`2h30m`, `1d6h30m`), case-insensitive, max `365d`. Default is `24h`.

### Command Rollups

```
GET /endpoints/{endpointId}/analytics/commands/rollups
```

Aggregated per-command traffic metrics bucketed by time window.

**RBAC:** Read

**Query Parameters:**
| Parameter | Type | Default | Description |
|---------------|--------|---------|------------------------------------------------------------------------------|
| `range` | string | `24h` | Lookback window. |
| `granularity` | string | `auto` | Time bucket size: `1m`, `1h`, or `auto` (1m for ranges <= 7d, 1h otherwise). |
| `command` | string | | Filter by exact command name (e.g. `GET`, `SET`). |
| `limit` | number | `1000` | Maximum rows to return. |

**Response:**

```json
[
  {
    "window_start": "2026-02-22T12:00:00.000",
    "command": "GET",
    "category": "read",
    "request_count": 15000,
    "success_count": 14950,
    "error_count": 50,
    "slow_count": 3,
    "dangerous_count": 0,
    "write_command_count": 0,
    "avg_latency_us": 120,
    "request_bytes_sum": 450000,
    "response_bytes_sum": 3200000,
    "target_count_sum": 15000,
    "cost_sum": 150,
    "bandwidth_cost": 36
  }
]
```

### Endpoint Metrics

```
GET /endpoints/{endpointId}/analytics/metrics
```

Point-in-time snapshots of endpoint-level operational metrics.

**RBAC:** Read

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|--------|---------|------------------------------|
| `range` | string | `24h` | Lookback window. |
| `limit` | number | `500` | Maximum snapshots to return. |
| `since` | string | unset | Optional RFC3339 timestamp; returns only snapshots after this timestamp. |

**Response:**

```json
[
  {
    "snapshot_time": "2026-02-22T12:00:00.000",
    "ops_per_sec": 1250.5,
    "total_commands": 5000000,
    "total_errors": 120,
    "slow_query_count": 8,
    "latency_p50_us": 95,
    "latency_p99_us": 450,
    "latency_p999_us": 2100,
    "error_rate": 0.000024,
    "cache_hit_rate": 0.92,
    "avg_pipeline_depth": 1.3,
    "transactions_committed": 500,
    "transactions_aborted": 2,
    "keys_with_ttl_pct": 0.85,
    "connections_opened": 10,
    "connections_closed": 8,
    "used_memory_bytes": 104857600,
    "connected_clients": 15,
    "used_cpu_sys": 2.5,
    "used_cpu_user": 4.1,
    "command_distribution": "{\"GET\": 60, \"SET\": 30, \"DEL\": 10}",
    "hot_keys": "[\"user:session:*\", \"cache:item:*\"]"
  }
]
```

### Anti-Patterns

```
GET /endpoints/{endpointId}/analytics/anti_patterns
```

Detected anti-pattern events with three view modes.

**RBAC:** Read

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|--------|-----------|---------------------------------------------|
| `range` | string | `24h` | Lookback window. |
| `view` | string | `summary` | View mode: `events`, `summary`, or `daily`. |
| `limit` | number | `500` | Maximum rows (ignored for `summary` view). |

**Response (view=events):**

```json
{
  "view": "events",
  "events": [
    {
      "detected_at": "2026-02-22T12:00:00.000",
      "pattern_type": "hot_key",
      "details": "Key user:session:abc accessed 50k times in 1m",
      "connection_id": 42,
      "occurrence_count": 50000
    }
  ]
}
```

**Response (view=summary):**

```json
{
  "view": "summary",
  "summary": [
    {
      "pattern_type": "hot_key",
      "total_occurrences": 150000,
      "unique_connections": 5,
      "latest_details": "Key user:session:abc accessed 50k times in 1m"
    }
  ]
}
```

**Response (view=daily):**

```json
{
  "view": "daily",
  "daily": [
    {
      "day": "2026-02-22",
      "pattern_type": "hot_key",
      "occurrence_count": 150000
    }
  ]
}
```

### Blocked Commands

```
GET /endpoints/{endpointId}/analytics/blocked_commands
```

Commands blocked by the security policy engine.

**RBAC:** Read

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|--------|-----------|--------------------------------------------------|
| `range` | string | `24h` | Lookback window. |
| `view` | string | `summary` | View mode: `events` or `summary`. |
| `command` | string | | Filter by exact command name (events view only). |
| `limit` | number | `500` | Maximum rows (ignored for `summary` view). |

**Response (view=events):**

```json
{
  "view": "events",
  "events": [
    {
      "event_time": "2026-02-22T12:00:00.000",
      "command": "FLUSHALL",
      "reason": "dangerous command blocked by policy",
      "severity": 3,
      "service": "app-backend"
    }
  ]
}
```

**Response (view=summary):**

```json
{
  "view": "summary",
  "items": [
    {
      "command": "FLUSHALL",
      "total_blocks": 42,
      "latest_reason": "dangerous command blocked by policy",
      "latest_severity": 3
    }
  ]
}
```

### PII Detections

```
GET /endpoints/{endpointId}/analytics/pii
```

PII (personally identifiable information) detection aggregates.

**RBAC:** Read

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|--------|-----------|--------------------------------------------|
| `range` | string | `24h` | Lookback window. |
| `view` | string | `summary` | View mode: `summary` or `timeseries`. |
| `limit` | number | `500` | Maximum rows (ignored for `summary` view). |

**Response (view=summary):**

```json
{
  "view": "summary",
  "items": [
    {
      "pii_type": "email",
      "total_detections": 1500,
      "representative_key_pattern": "user:*:profile",
      "representative_redacted_sample": "j***@example.com"
    }
  ]
}
```

**Response (view=timeseries):**

```json
{
  "view": "timeseries",
  "points": [
    {
      "window_start": "2026-02-22T12:00:00.000",
      "pii_type": "email",
      "detection_count": 50,
      "representative_key_pattern": "user:*:profile"
    }
  ]
}
```

### Audit Trail

```
GET /endpoints/{endpointId}/analytics/audit_trail
```

Per-command audit log with pagination. Includes a parallel count query for total matching events.

**RBAC:** Admin

**Query Parameters:**
| Parameter | Type | Default | Description |
|------------------|---------|---------|-----------------------------------------------|
| `range` | string | `24h` | Lookback window. |
| `command` | string | | Filter by exact command name. |
| `success` | boolean | | Filter by success status (`true` or `false`). |
| `min_latency_us` | number | | Minimum latency threshold in microseconds. |
| `limit` | number | `500` | Maximum event rows to return. |

**Response:**

```json
{
  "events": [
    {
      "event_time": "2026-02-22T12:00:00.000",
      "service": "app-backend",
      "command": "SET",
      "key": "user:123:session",
      "latency_us": 150,
      "success": true,
      "client_ip": "10.0.1.50"
    }
  ],
  "total_count": 25000
}
```

### Patterns

```
GET /endpoints/{endpointId}/analytics/patterns
```

Key pattern profiles derived from traffic analysis.

**RBAC:** Read

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|--------|---------|------------------|
| `range` | string | `24h` | Lookback window. |

**Response:** Array of `PatternProfile` objects (key pattern, request counts, latency stats).

### Recommendations

```
GET /endpoints/{endpointId}/analytics/recommendations
```

Actionable optimization recommendations generated by the recommendations engine (13 rules). Combines pattern profiles, endpoint context, stale pattern detection and total request volume.

**RBAC:** Read

**Query Parameters:**
| Parameter | Type | Default | Description |
|--------------------|---------|---------|------------------------------------------------------|
| `range` | string | `24h` | Lookback window. |
| `include_evidence` | boolean | `false` | Include raw evidence strings in each recommendation. |

**Response:** Array of recommendation objects with severity, category, title, description, and optionally evidence.

---

## Fleet-Wide Analytics

These endpoints provide fleet-level (cross-endpoint) analytics data.

**Availability:** Fleet-level live telemetry is available through `/analytics/overview`, `/analytics/series`, and `/analytics/export`. Verbose anomaly detector routes are not included in this distribution.

### Anomaly Detector Status

```
GET /analytics/anomalies/status
```

REST snapshot of per-endpoint anomaly detector states from the in-memory pipeline. Returns the current state of three detectors (divergence, latency, error_rate) per endpoint.

**RBAC:** Read

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------------|--------|---------|--------------------------------------------|
| `endpoint_uuid` | string | | Filter by endpoint UUID (substring match). |

**Response:**

```json
{
  "endpoints": [
    {
      "endpoint_uuid": "ep_abc123",
      "divergence": "normal",
      "latency": "watching",
      "error_rate": "normal"
    }
  ]
}
```

Detector states: `normal`, `watching`, `confirmed`.

### Anomaly Transitions

```
GET /analytics/anomalies/transitions
```

Historical anomaly detector state transitions persisted to ClickHouse.

**RBAC:** Read

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------------|--------|---------|---------------------------------------------------------------|
| `range` | string | `24h` | Lookback window. |
| `endpoint_uuid` | string | | Filter by endpoint UUID. |
| `detector` | string | | Filter by detector: `divergence`, `latency`, or `error_rate`. |
| `limit` | number | `500` | Maximum rows to return. |

**Response:**

```json
{
  "transitions": [
    {
      "transition_time": "2026-02-22T12:00:00.000",
      "endpoint_uuid": "ep_abc123",
      "detector": "latency",
      "from_level": "normal",
      "to_level": "watching"
    }
  ]
}
```

### Fleet Signals

```
GET /analytics/fleet/signals
```

Fleet-wide per-endpoint health signals from the in-memory `HealthPublisher`. Sub-100ms latency (no ClickHouse round-trip). Includes traffic rates, latency percentiles, anomaly levels and per-detector state summaries.

**RBAC:** Read

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------------|--------|-----------------|---------------------------------------------------------------------------------------|
| `anomaly_level` | string | | Minimum anomaly level filter: `none`, `low`, `medium`, `high`, `critical`. |
| `sort` | string | `anomaly_level` | Sort field (descending): `ops_per_sec`, `error_rate`, `latency_p99`, `anomaly_level`. |
| `limit` | number | `100` | Maximum endpoints to return. |

**Response:**

```json
{
  "endpoints": [
    {
      "endpoint_uuid": "ep_abc123",
      "ops_per_sec": 1250.5,
      "error_rate": 0.001,
      "latency_p50_us": 95,
      "latency_p99_us": 450,
      "slow_query_count": 3,
      "cache_hit_rate": 0.92,
      "anomaly_level": "high",
      "mode": "observing",
      "request_bytes_per_sec": 50000.0,
      "response_bytes_per_sec": 320000.0,
      "detector_summary": {
        "divergence": "normal",
        "latency": "confirmed",
        "error_rate": "watching"
      }
    }
  ],
  "endpoint_count": 42
}
```

`endpoint_count` reflects the total number of monitored endpoints (before filtering). `detector_summary` is present only for endpoints with active anomaly detectors.

---

## Tool result compaction

When the LLM calls a tool (SQL query, schema search, tool endpoint), the raw result is compacted before being fed back into the conversation. This happens in `tool_result_projection.rs`.

Tabular results are detected and projected into a compact columnar JSON:

```json
{
  "type": "table",
  "columns": ["id", "name"],
  "rows": [
    [1, "alpha"],
    [2, "beta"]
  ],
  "row_count": 2,
  "truncated": false
}
```

Hard limits: 50 rows, 2000 cells (rows x columns), 64 KB serialized. Individual string values are truncated to 512 bytes. When any limit is hit, the result is flagged `"truncated": true` and rows/columns are dropped from the end.

Non-tabular results (JSON blobs, plain text) are passed through with only a byte-size cap.

## Error Responses

All error responses follow this format:

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human readable message",
    "details": {
      "field1": ["validation error 1"],
      "field2": ["validation error 2"]
    }
  }
}
```

## Common Response Codes

- `200 OK`: Request successful
- `201 Created`: Resource created successfully
- `204 No Content`: Resource deleted successfully
- `400 Bad Request`: Invalid request parameters
- `401 Unauthorized`: Invalid or missing authentication
- `403 Forbidden`: Insufficient permissions
- `404 Not Found`: Resource not found
- `409 Conflict`: Resource conflict
- `422 Unprocessable Entity`: Validation error
- `500 Internal Server Error`: Server error

## Endpoint-Level Security (ELS)

ELS allows fine-grained, per-user credential and session-variable injection at the database proxy layer. Policies define _what_ credentials or variables to inject; user assignments control _who_ gets which policy. Configs are encrypted at rest (AES-256-GCM).

**RBAC:** All ELS operations require `Admin` access on the endpoint.

### Strategies

| Strategy      | Applies To | Config Shape                                         |
| ------------- | ---------- | ---------------------------------------------------- |
| `PostgresRLS` | PostgreSQL | `{ "variables": { "app.tenant_id": "value", ... } }` |

Additional strategies will be added for other endpoint types.

### Policy CRUD

#### Create Policy

```
POST /iam/els/endpoints/{endpoint}/policies
```

**Request Body:**

```json
{
  "name": "tenant-isolation",
  "strategy": "PostgresRLS",
  "config": {
    "variables": {
      "app.tenant_id": "tenant_abc"
    }
  }
}
```

**Response:**

```json
{
  "policy_uuid": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### List Policies

```
GET /iam/els/endpoints/{endpoint}/policies
```

Returns an array of redacted policies (config excluded).

#### Get Policy

```
GET /iam/els/endpoints/{endpoint}/policies/{policy_uuid}
```

Returns a single redacted policy.

#### Update Policy

```
PUT /iam/els/endpoints/{endpoint}/policies/{policy_uuid}
```

**Request Body:** Same shape as create. Re-caches all sync-mode user assignments.

#### Delete Policy

```
DELETE /iam/els/endpoints/{endpoint}/policies/{policy_uuid}
```

Cascades to all versions and user assignments for this policy.

#### Delete All Policies

```
DELETE /iam/els/endpoints/{endpoint}/policies
```

Removes all ELS policies for the endpoint.

### Version Lifecycle

Policies support versioned configs. Versions progress through: `draft` → `active` (via promote) → `rolled_back` (via rollback).

#### Create Draft Version

```
POST /iam/els/endpoints/{endpoint}/policies/{policy_uuid}/versions
```

**Request Body:**

```json
{
  "config": {
    "variables": {
      "app.tenant_id": "new_value"
    }
  }
}
```

**Response:**

```json
{
  "version": 2
}
```

#### List Versions

```
GET /iam/els/endpoints/{endpoint}/policies/{policy_uuid}/versions
```

Returns all versions (redacted, config excluded).

#### Get Version

```
GET /iam/els/endpoints/{endpoint}/policies/{policy_uuid}/versions/{version}
```

Returns a single redacted version.

#### Get Active Pointer

```
GET /iam/els/endpoints/{endpoint}/policies/{policy_uuid}/pointer
```

Returns the currently active version number for the policy.

#### Promote Version

```
POST /iam/els/endpoints/{endpoint}/policies/{policy_uuid}/versions/{version}/promote
```

Atomically promotes a draft version to active and demotes the previous active version.

#### Rollback Version

```
POST /iam/els/endpoints/{endpoint}/policies/{policy_uuid}/versions/{version}/rollback
```

Rolls back to a previous version, marking the current active as rolled back.

### User Assignment CRUD

Assignments bind a user to a policy. In `sync` mode the user always gets the policy's active config; in `copy` mode a snapshot is taken at assignment time.

#### Assign User

```
PUT /iam/els/endpoints/{endpoint}/users/{user_uuid}
```

**Request Body:**

```json
{
  "policy_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "mode": "sync"
}
```

`mode` is either `"sync"` or `"copy"`.

#### Get User Policy

```
GET /iam/els/endpoints/{endpoint}/users/{user_uuid}
```

Returns the effective redacted policy for the user.

#### List User Assignments

```
GET /iam/els/endpoints/{endpoint}/users
```

Returns all user assignments for the endpoint (redacted).

#### Unassign User

```
DELETE /iam/els/endpoints/{endpoint}/users/{user_uuid}
```

#### Unassign All Users

```
DELETE /iam/els/endpoints/{endpoint}/users
```

Removes all user assignments for the endpoint.

---

## LLM Gateway Control Plane API

### Export Gateway Snapshot

```
GET /llm/gateway_snapshot
```

Returns the service-managed data-plane snapshot for the caller's organization.
The snapshot contains gateway auth mode, hashed gateway key policies, the model
catalog, and route-stat rollups. Standalone `eden_gateway` processes can consume
the JSON through `EDEN_LLM_GATEWAY_CONTROL_PLANE_SNAPSHOT_JSON` or
`EDEN_LLM_GATEWAY_CONTROL_PLANE_SNAPSHOT_PATH`.

Gateway API keys created through the LLM control-plane API are persisted in
`llm_gateway_api_keys` and rehydrated when `eden_service` starts, so exported
snapshots survive service restarts.

For deployed standalone gateways, `eden_service` can publish a trusted
multi-organization snapshot file by setting
`EDEN_LLM_GATEWAY_CONTROL_PLANE_SNAPSHOT_PUBLISH_PATH`. Mount the same file into
`eden_gateway` and set `EDEN_LLM_GATEWAY_CONTROL_PLANE_SNAPSHOT_PATH` there.

### Gateway Dashboard

```
GET /llm/gateway/dashboard
```

Returns an authenticated AI gateway summary for the caller's organization. The
response includes in-memory API key/cache/route/budget counts, durable monthly
usage rollups from `llm_gateway_usage_rollups`, durable route rollups from
`llm_gateway_route_rollups`, and agent gateway connection/fingerprint/transport
usage totals.

The `cost_analysis` object summarizes month-to-date estimated spend, token
counts, route/cache savings, provider and model spend, api-key/agent/user cost
centers, daily cost buckets, generated cost alerts, provider reconciliation
status, and recent high-cost request events. Downstream attribution comes from
the OpenAI-compatible proxy headers `x-eden-agent-id`, `x-eden-user-id`, and
`x-eden-consumer-id`; api-key attribution is recorded automatically from the
gateway key used for the request.

The `capability_status` array intentionally marks response cache,
KV/prefix-cache movement, MCP/A2A agent gateway, org/user budgets, and
dashboards as foundations where appropriate. This keeps product wording aligned
with what is implemented versus the next customer-facing slices.

---

## LLM Agent Gateway API

### Register Agent Connection

```
POST /llm/agent-gateway/connections
```

Registers a live network session for an agent. The caller must have agent
configuration access and the agent must belong to the caller's organization.

**Request Body:**

```json
{
  "agent_id": "11111111-2222-3333-4444-555555555555",
  "transport": "a2a_http",
  "callback_url": "https://agent-worker.example.com/a2a",
  "node_id": "worker-1",
  "region": "us-east-1",
  "labels": { "pool": "prod" },
  "identity": {
    "fingerprint": "agent-seat-42",
    "instance_id": "worker-1",
    "principal": "planner",
    "tags": { "vendor": "eden", "tier": "prod" }
  },
  "rate_limit": {
    "requests_per_minute": 120,
    "total_tokens_per_minute": 200000,
    "max_active_streams": 8
  },
  "metrics": {
    "active_streams": 0,
    "queued_messages": 0,
    "avg_latency_ms": 25
  }
}
```

### Maintain Agent Connection

```
POST /llm/agent-gateway/connections/{session_id}/heartbeat
POST /llm/agent-gateway/connections/{session_id}/usage
POST /llm/agent-gateway/connections/{session_id}/drain
DELETE /llm/agent-gateway/connections/{session_id}
```

Heartbeats refresh the session TTL and update load metrics. Usage records
request, token, and cost counters by agent fingerprint and returns the gateway
rate-limit decision. Draining removes the session from new route selection
without immediately deleting it. Delete removes the session.

**Usage Request Body:**

```json
{
  "usage": {
    "request_count": 1,
    "prompt_tokens": 1200,
    "completion_tokens": 300,
    "total_tokens": 1500,
    "cost_microdollars": 90
  }
}
```

### Resolve Agent Route

```
GET /llm/agent-gateway/agents/{agent_id}/route
GET /llm/agent-gateway/connections
GET /llm/agent-gateway/usage
```

Route resolution returns the current least-loaded active network session for the
agent. Listing returns active sessions for the caller's organization. Usage
listing returns the current in-memory per-fingerprint windows used for rate
limiting and customer reporting.

---

## LLM Agent Investigation API

### Start Endpoint Investigation

```
POST /llm/agents/investigate
```

Triggers a read-only incident investigation for one endpoint. The request
requires `READ` access to the target endpoint and executes through an LLM
endpoint in the same organization.

**Request Body:**

```json
{
  "endpoint_uuid": "11111111-2222-3333-4444-555555555555",
  "llm_endpoint_uuid": "66666666-7777-8888-9999-aaaaaaaaaaaa",
  "severity": "high",
  "description": "Inspect recent errors and summarize likely root cause."
}
```

`llm_endpoint_uuid` is optional. Omit it when the organization has exactly one
LLM endpoint. Provide it when the organization has multiple LLM endpoints and
the investigation should execute against a specific one.

**Response:**

```json
{
  "run_id": "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
  "status": "running"
}
```

### Get Investigation Status

```
GET /llm/agents/investigate/{run_id}/status
```

Returns the current run state, plan, and recent execution events.

### Get Investigation Evidence

```
GET /llm/agents/investigate/{run_id}/evidence
```

Returns evidence records captured during the investigation run.

---

## Security Features

1. **Authentication Required**
   - Every request must include authentication
   - JWT tokens expire after 1 hour
   - Basic auth requires HTTPS

2. **Organization Isolation**
   - Users can only access their organization
   - Organization ID embedded in JWT token
   - Cross-organization access attempts return 403

3. **Rate Limiting**
   - 1000 requests per minute per organization
   - Rate limit headers included in responses:

   ```
   X-RateLimit-Limit: 1000
   X-RateLimit-Remaining: 999
   X-RateLimit-Reset: 1640995200
   ```

4. **Role-Based Access Control**
   - All actions require appropriate role permissions
   - Default roles: Admin, Editor, Viewer
   - Custom roles can be created
