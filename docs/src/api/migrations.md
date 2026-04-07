# Migration APIs

This reference covers the APIs for managing database migrations in Eden-MDBS.

## Overview

The Migration API enables zero-downtime database migrations with configurable strategies, data movement, and failure handling.

Migrations are also available via MCP for tool-based workflows. See the [MCP Tooling API](./mcp.md) for the streamable HTTP endpoint and tool list.

## Create Migration

Create a new migration definition.

### Request

```http
POST /api/v1/migrations
Content-Type: application/json
Authorization: Bearer <token>
```

### Body Parameters

| Field             | Type   | Required | Description                          |
| ----------------- | ------ | -------- | ------------------------------------ |
| `id`              | string | Yes      | Unique migration identifier          |
| `description`     | string | No       | Migration description                |
| `strategy`        | object | No       | Migration strategy (default: big_bang) |
| `data`            | object | No       | Data movement rules (default: none)  |
| `failure_handling`| string | No       | Failure behavior (default: rollback_all) |

### Example (Big Bang Strategy)

```bash
curl http://{host}:8000/api/v1/migrations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "test_migration",
    "description": "Redis migration",
    "strategy": {
      "type": "big_bang",
      "durability": true
    },
    "data": null,
    "failure_handling": null
  }'
```

### Example (Canary Strategy)

```bash
curl http://{host}:8000/api/v1/migrations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "redis_migration_v2",
    "description": "Migrate to new Redis cluster",
    "strategy": {
      "type": "canary",
      "read_percentage": 0.05,
      "write_mode": {
        "mode": "dual_write",
        "policy": "old_authoritative"
      }
    },
    "data": {
      "type": "scan",
      "replace": "replace"
    },
    "failure_handling": "rollback_all"
  }'
```

### Response

```json
{
  "status": "success",
  "data": {}
}
```

## List Migrations

Get all migrations in your organization.

### Request

```http
GET /api/v1/migrations
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/migrations \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "status": "success",
  "data": [
    {
      "id": "redis_migration_v2",
      "uuid": "550e8400-e29b-41d4-a716-446655440000",
      "description": "Migrate to new Redis cluster",
      "status": "pending",
      "strategy": {"type": "canary", "read_percentage": 0.05},
      "created_at": "2024-01-15T10:30:00Z",
      "updated_at": "2024-01-15T10:30:00Z"
    }
  ]
}
```

### Verbose Mode

Add `X-Eden-Verbose: true` header to get full migration details:

```bash
curl http://{host}:8000/api/v1/migrations \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Eden-Verbose: true"
```

## Get Migration

Get details of a specific migration.

### Request

```http
GET /api/v1/migrations/{migration_id}
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/migrations/redis_migration_v2 \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "redis_migration_v2",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "description": "Migrate to new Redis cluster",
    "status": "running",
    "strategy": {
      "type": "canary",
      "read_percentage": 0.25,
      "write_mode": {
        "mode": "dual_write",
        "policy": "old_authoritative"
      }
    },
    "data": {
      "type": "scan",
      "replace": "replace"
    },
    "failure_handling": "rollback_all",
    "apis": [],
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T11:00:00Z"
  }
}
```

### Verbose Response

With `X-Eden-Verbose: true`, includes full API schemas:

```json
{
  "status": "success",
  "data": {
    "id": "redis_migration_v2",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "status": "running",
    "strategy": {...},
    "apis": [
      {
        "id": "user_cache_api",
        "uuid": "...",
        "migration": {...}
      }
    ],
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T11:00:00Z"
  }
}
```

## Add API to Migration

Associate an API with a migration.

### Request

```http
POST /api/v1/migrations/{migration_id}/api/{api_id}
Content-Type: application/json
Authorization: Bearer <token>
```

### Body Parameters

| Field      | Type   | Required | Description                     |
| ---------- | ------ | -------- | ------------------------------- |
| `id`       | string | Yes      | API identifier                  |
| `bindings` | array  | Yes      | Template bindings for migration |

### Binding Object

| Field      | Type   | Required | Description              |
| ---------- | ------ | -------- | ------------------------ |
| `template` | string | Yes      | Template ID to bind      |
| `fields`   | object | No       | Field mappings           |

### Example

```bash
curl http://{host}:8000/api/v1/migrations/redis_migration_v2/api/user_cache_api \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "user_cache_api",
    "bindings": [
      {
        "template": "get_user_cache",
        "fields": {}
      },
      {
        "template": "set_user_cache",
        "fields": {}
      }
    ]
  }'
```

### Response

```json
{
  "status": "success",
  "data": "added Api to migration"
}
```

## Add Interlay to Migration

Associate an interlay with a migration for endpoint switching.

### Request

```http
POST /api/v1/migrations/{migration_id}/interlay/{interlay_id}
Content-Type: application/json
Authorization: Bearer <token>
```

### Body Parameters

| Field      | Type   | Required | Description                    |
| ---------- | ------ | -------- | ------------------------------ |
| `endpoint` | string | Yes      | Target endpoint ID to migrate to |

### Example

```bash
curl http://{host}:8000/api/v1/migrations/redis_migration_v2/interlay/cache_interlay \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "new_redis_cluster"
  }'
```

### Response

```json
{
  "status": "success",
  "data": "added Interlay to migration"
}
```

## Test Migration

Validate migration configuration before execution.

### Request

```http
POST /api/v1/migrations/{migration_id}/test
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/migrations/redis_migration_v2/test \
  -H "Authorization: Bearer $TOKEN" \
  -X POST
```

### Response

```json
{
  "status": "success",
  "data": {
    "valid": true,
    "apis_tested": 2,
    "interlays_tested": 1
  }
}
```

## Execute Migration

Start the migration process.

### Request

```http
POST /api/v1/migrations/{migration_id}/migrate
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/migrations/redis_migration_v2/migrate \
  -H "Authorization: Bearer $TOKEN" \
  -X POST
```

### Response

```json
{
  "status": "success",
  "data": "migration commit completed"
}
```

## Strategy Objects

### Big Bang

```json
{
  "type": "big_bang"
}
```

### Canary

```json
{
  "type": "canary",
  "read_percentage": 0.05,
  "write_mode": {
    "mode": "dual_write",
    "policy": "old_authoritative"
  }
}
```

**Write Modes**:

| Mode        | Description                                    |
| ----------- | ---------------------------------------------- |
| `dual_write`| Write to both systems, old is authoritative    |
| `cutover`   | Gradually shift writes to new system           |

**Write Policies** (for dual_write):

| Policy            | Description                              |
| ----------------- | ---------------------------------------- |
| `old_authoritative` | Old system determines success/failure  |
| `new_authoritative` | New system determines success/failure  |
| `best_effort`     | Success if either system succeeds        |

### Blue-Green

```json
{
  "type": "blue_green",
  "active_is_new": false
}
```

### Rolling Update

```json
{
  "type": "rolling_update",
  "migrated_instances": ["instance-1", "instance-2"],
  "total_instances": 5
}
```

### Shadow Traffic

```json
{
  "type": "shadow_traffic"
}
```

### Strangler Fig

```json
{
  "type": "strangler_fig",
  "features": {
    "feature_a": true,
    "feature_b": false
  }
}
```

### Feature Flag

```json
{
  "type": "feature_flag",
  "flags": {
    "new_database": true
  },
  "rollout_percentage": 0.25
}
```

### Geographic

```json
{
  "type": "geographic",
  "regions": {
    "us-east-1": true,
    "eu-west-1": false
  }
}
```

### Time Window

```json
{
  "type": "time_window",
  "window_start": "2024-01-15T02:00:00Z",
  "window_end": "2024-01-15T04:00:00Z",
  "executed": false
}
```

## Data Movement Objects

### None

```json
"none"
```

### Snapshot

```json
{
  "type": "snapshot",
  "replace": "none"
}
```

### Scan

```json
{
  "type": "scan",
  "replace": "replace"
}
```

**Replace Options**:

| Value     | Description                                |
| --------- | ------------------------------------------ |
| `none`    | Skip records that already exist in target  |
| `replace` | Overwrite target with source data          |
| `merge`   | Use database-specific merge logic          |

## Failure Handling

| Value          | Description                              |
| -------------- | ---------------------------------------- |
| `rollback_all` | Revert everything if any component fails |
| `allow_partial`| Allow some migrations to fail            |

Retry then rollback:
```json
{
  "type": "retry_then_all",
  "retry_count": 3
}
```

## Migration Status Values

| Status        | Description                            |
| ------------- | -------------------------------------- |
| `pending`     | Created but not configured             |
| `ready`       | APIs/Interlays added, ready to execute |
| `running`     | Migration in progress                  |
| `paused`      | Temporarily stopped                    |
| `completed`   | Finished successfully                  |
| `failed`      | Encountered errors                     |
| `rolling_back`| Reverting to original state            |
| `rolled_back` | Successfully reverted                  |

## Access Control

| Operation          | Required Access |
| ------------------ | --------------- |
| Create migration   | Admin           |
| List migrations    | Read            |
| Get migration      | Admin           |
| Add API            | Admin           |
| Add Interlay       | Admin           |
| Test migration     | Admin           |
| Execute migration  | Admin           |

## Error Responses

### Migration Not Found

```json
{
  "error": "Not found",
  "message": "Migration 'redis_migration_v2' does not exist"
}
```

### API Already Has Migration

```json
{
  "error": "Migration error",
  "message": "Api Schema already has an active migration"
}
```

### Interlay Already Has Migration

```json
{
  "error": "Migration error",
  "message": "Interlay Schema already has an active migration"
}
```

### Invalid Strategy

```json
{
  "error": "Bad Request",
  "message": "Migration strategy not yet implemented"
}
```

### Invalid Status Transition

```json
{
  "error": "Migration error",
  "message": "Cannot set RollingBack migration in status: Pending"
}
```

## Related

- [Migrations Guide](../guide/migrations.md) - Concepts and strategies
- [Endpoints](./endpoints.md) - Endpoint management
- [Templates](../advanced/templates.md) - Creating templates
