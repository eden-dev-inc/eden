# Interlays API

This reference covers the APIs for managing interlays in Eden-MDBS.

## Overview

Interlays are traffic routing layers that direct requests to underlying endpoints. They enable:

- Endpoint switching without application changes
- Migration traffic routing
- A/B testing between endpoints
- Gradual rollouts to new database systems

Interlays expose a local port that proxies traffic to the target endpoint, allowing seamless switching during migrations.

## List Interlays

Get all interlays in your organization.

### Request

```http
GET /api/v1/interlays
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/interlays \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "redis_interlay",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "endpoint": "550e8400-e29b-41d4-a716-446655440001",
    "port": 6366,
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

## Get Interlay

Get details of a specific interlay.

### Request

```http
GET /api/v1/interlays/{interlay}
Authorization: Bearer <token>
```

### Path Parameters

| Parameter  | Type   | Description          |
| ---------- | ------ | -------------------- |
| `interlay` | string | Interlay identifier  |

### Example

```bash
curl http://{host}:8000/api/v1/interlays/redis_interlay \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Eden-Verbose: true"
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "redis_interlay",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "endpoint": "550e8400-e29b-41d4-a716-446655440001",
    "port": 6366,
    "tls": false,
    "migration": null,
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

## Create Interlay

Create a new interlay.

### Request

```http
POST /api/v1/interlays
Content-Type: application/json
Authorization: Bearer <token>
```

### Body Parameters

| Field      | Type    | Required | Description                              |
| ---------- | ------- | -------- | ---------------------------------------- |
| `id`       | string  | Yes      | Unique interlay identifier               |
| `endpoint` | string  | Yes      | Target endpoint UUID                     |
| `port`     | integer | Yes      | Local port to expose for proxy traffic   |
| `settings` | object  | No       | Additional interlay settings             |
| `tls`      | boolean | No       | Enable TLS for the proxy (default: false)|

### Example

```bash
curl http://{host}:8000/api/v1/interlays \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "redis_interlay",
    "endpoint": "550e8400-e29b-41d4-a716-446655440001",
    "port": 6366,
    "settings": {},
    "tls": false
  }'
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "redis_interlay",
    "uuid": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

## Delete Interlay

Remove an interlay.

### Request

```http
DELETE /api/v1/interlays/{interlay}
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/interlays/redis_interlay \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

### Response

```json
{
  "status": "success",
  "data": "Interlay deleted successfully"
}
```

## Interlay with Migration

Interlays can be associated with migrations for traffic switching between endpoints.

### Add Interlay to Migration

```http
POST /api/v1/migrations/{migration}/interlay/{interlay}
Content-Type: application/json
Authorization: Bearer <token>
```

### Body Parameters

| Field                | Type   | Required | Description                              |
| -------------------- | ------ | -------- | ---------------------------------------- |
| `id`                 | string | Yes      | Migration interlay identifier            |
| `endpoint`           | string | Yes      | New target endpoint ID                   |
| `description`        | string | No       | Description of the migration             |
| `migration_strategy` | object | Yes      | Migration strategy configuration         |
| `migration_data`     | object | No       | Data movement configuration              |
| `migration_rules`    | object | Yes      | Traffic and error handling rules         |
| `testing_validation` | object | No       | Validation configuration                 |

### Migration Rules Object

| Field        | Type   | Description                              |
| ------------ | ------ | ---------------------------------------- |
| `traffic`    | object | Read/write traffic routing rules         |
| `error`      | string | Error handling (DoNothing, Rollback)     |
| `rollback`   | string | Rollback behavior (Ignore, Revert)       |
| `completion` | object | Completion milestone settings            |

### Traffic Rules

| Field   | Type   | Description                              |
| ------- | ------ | ---------------------------------------- |
| `read`  | string | Read traffic routing (Old, New, Replicated) |
| `write` | string | Write traffic routing (Old, New, Both)   |

### Example

```bash
curl http://{host}:8000/api/v1/migrations/test_migration/interlay/redis_interlay \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "migration_relay",
    "endpoint": "redis_test2",
    "description": "Redis migration interlay",
    "migration_strategy": {
      "type": "big_bang",
      "durability": true
    },
    "migration_data": {
      "Scan": {
        "replace": "None"
      }
    },
    "migration_rules": {
      "traffic": {
        "read": "Replicated",
        "write": "New"
      },
      "error": "DoNothing",
      "rollback": "Ignore",
      "completion": {
        "milestone": "Immediate",
        "require_manual_approval": false
      }
    }
  }'
```

### Response

```json
{
  "status": "success",
  "data": "added Interlay to migration"
}
```

## Use Cases

### Endpoint Switching

Use interlays to switch between endpoints without changing application code:

1. Create interlay pointing to original endpoint
2. Update applications to use interlay instead of direct endpoint
3. When ready to switch, update interlay to new endpoint
4. All traffic automatically routes to new endpoint

### Migration Support

Interlays integrate with the migration system for gradual traffic shifts:

1. Create migration with desired strategy
2. Add interlay to migration
3. Migration controls traffic distribution
4. After migration completes, interlay points to new endpoint

## Access Control

| Operation        | Required Access |
| ---------------- | --------------- |
| List interlays   | Read            |
| Get interlay     | Read            |
| Create interlay  | Admin           |
| Start interlay   | Admin           |
| Delete interlay  | Admin           |

## Error Responses

### Interlay Not Found

```json
{
  "error": "Not Found",
  "message": "Interlay 'cache_interlay' does not exist"
}
```

### Endpoint Not Found

```json
{
  "error": "Not Found",
  "message": "Endpoint 'redis_primary' does not exist"
}
```

### Interlay Already Has Migration

```json
{
  "error": "Migration error",
  "message": "Interlay Schema already has an active migration"
}
```

## Related

- [Migrations API](./migrations.md) - Migration configuration
- [Endpoints API](./endpoints.md) - Database connections
- [Migrations Guide](../guide/migrations.md) - Migration concepts
