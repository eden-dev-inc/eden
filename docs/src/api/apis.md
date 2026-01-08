# APIs Endpoint

This reference covers the APIs for managing API definitions in Eden-MDBS.

## Overview

APIs in Eden-MDBS are logical groupings that bind multiple templates together. They provide a way to:

- Execute multiple templates in sequence
- Define field mappings between templates
- Support migrations with dual execution paths
- Apply response logic to combine results

## List APIs

Get all APIs in your organization.

### Request

```http
GET /api/v1/apis
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/apis \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "user_operations",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "templates": [...],
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

### List Updated APIs

Get APIs updated since a specific timestamp.

```http
GET /api/v1/apis/updated
Content-Type: text/plain
Authorization: Bearer <token>
```

Body: ISO 8601 timestamp string

## Create API

Create a new API definition.

### Request

```http
POST /api/v1/apis
Content-Type: application/json
Authorization: Bearer <token>
```

### Body Parameters

| Field        | Type   | Required | Description                          |
| ------------ | ------ | -------- | ------------------------------------ |
| `id`         | string | Yes      | Unique API identifier                |
| `fields`     | array  | Yes      | Input field definitions              |
| `bindings`   | array  | Yes      | Template bindings                    |
| `description`| string | No       | API description                      |
| `response_logic` | object | No  | Logic for combining responses        |
| `migration`  | object | No       | Migration configuration              |

### Binding Object

| Field        | Type   | Required | Description                    |
| ------------ | ------ | -------- | ------------------------------ |
| `template_id`| string | Yes      | Template to bind               |
| `fields`     | array  | Yes      | Field mappings (pairs of [api_field, template_field]) |

### Example

```bash
curl http://{host}:8000/api/v1/apis \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "get_user_with_orders",
    "fields": [
      {"name": "user_id", "type": "integer", "required": true}
    ],
    "bindings": [
      {
        "template_id": "get_user",
        "fields": [["user_id", "id"]]
      },
      {
        "template_id": "get_user_orders",
        "fields": [["user_id", "customer_id"]]
      }
    ],
    "description": "Fetch user and their orders"
  }'
```

### Response

```json
{
  "status": "success",
  "data": "API created successfully"
}
```

## Get API

Get details of a specific API.

### Request

```http
GET /api/v1/apis/{api}
Authorization: Bearer <token>
```

### Path Parameters

| Parameter | Type   | Description      |
| --------- | ------ | ---------------- |
| `api`     | string | API identifier   |

### Example

```bash
curl http://{host}:8000/api/v1/apis/get_user_with_orders \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "get_user_with_orders",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "templates": [
      {
        "id": "get_user",
        "endpoint": "postgres_main",
        "query": "SELECT * FROM users WHERE id = {{id}}"
      },
      {
        "id": "get_user_orders",
        "endpoint": "postgres_main",
        "query": "SELECT * FROM orders WHERE customer_id = {{customer_id}}"
      }
    ],
    "migration": null,
    "response_logic": null,
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

## Execute API

Run an API with provided parameters.

### Request

```http
POST /api/v1/apis/{api}
Content-Type: application/json
Authorization: Bearer <token>
```

### Path Parameters

| Parameter | Type   | Description      |
| --------- | ------ | ---------------- |
| `api`     | string | API identifier   |

### Body

JSON object containing API parameter values.

### Example

```bash
curl http://{host}:8000/api/v1/apis/get_user_with_orders \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "user_id": 12345
  }'
```

### Response

```json
{
  "status": "success",
  "data": {
    "get_user": [
      {
        "id": 12345,
        "name": "John Doe",
        "email": "john@example.com"
      }
    ],
    "get_user_orders": [
      {
        "id": 1001,
        "customer_id": 12345,
        "total": 99.99,
        "status": "completed"
      },
      {
        "id": 1002,
        "customer_id": 12345,
        "total": 149.99,
        "status": "pending"
      }
    ]
  }
}
```

## Delete API

Remove an API definition.

### Request

```http
DELETE /api/v1/apis/{api}
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/apis/get_user_with_orders \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

### Response

```json
{
  "status": "success",
  "data": "API deleted successfully"
}
```

## API with Migration

APIs can include migration configuration for gradual traffic shifting.

### Migration Object

| Field              | Type   | Required | Description                      |
| ------------------ | ------ | -------- | -------------------------------- |
| `id`               | string | Yes      | Migration identifier             |
| `bindings`         | array  | Yes      | Template bindings for new path   |
| `migration_strategy` | object | Yes   | Strategy configuration           |
| `migration_rules`  | object | Yes      | Data movement rules              |
| `testing_validation` | object | No    | Validation configuration         |

### Example with Migration

```bash
curl http://{host}:8000/api/v1/apis \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "user_cache_api",
    "fields": [
      {"name": "user_id", "type": "string", "required": true}
    ],
    "bindings": [
      {
        "template_id": "get_user_cache_old",
        "fields": [["user_id", "key"]]
      }
    ],
    "migration": {
      "id": "redis_migration",
      "bindings": [
        {
          "template_id": "get_user_cache_new",
          "fields": [["user_id", "key"]]
        }
      ],
      "migration_strategy": {
        "type": "canary",
        "read_percentage": 0.1
      },
      "migration_rules": {
        "type": "scan",
        "replace": "replace"
      }
    }
  }'
```

## Access Control

| Operation      | Required Access |
| -------------- | --------------- |
| List APIs      | Read            |
| Get API        | Read            |
| Create API     | Admin           |
| Execute API    | Read            |
| Delete API     | Admin           |

## Error Responses

### API Not Found

```json
{
  "error": "Not Found",
  "message": "API 'get_user_with_orders' does not exist"
}
```

### Invalid Binding

```json
{
  "error": "Bad Request",
  "message": "Template 'get_user' not found"
}
```

### Field Mapping Error

```json
{
  "error": "Bad Request",
  "message": "Field 'user_id' not defined in API fields"
}
```

## Related

- [Templates API](./templates.md) - Template management
- [Migrations API](./migrations.md) - Migration configuration
- [Workflows API](./workflows.md) - Multi-step operations
