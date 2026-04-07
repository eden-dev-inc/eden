# Templates API

This reference covers the APIs for managing templates in Eden-MDBS.

## Overview

Templates are reusable, parameterized database operations. They use Handlebars-style syntax for variable substitution and can be executed with different parameters.

## List Templates

Get all templates in your organization.

### Request

```http
GET /api/v1/templates
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/templates \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "status": "success",
  "data": [
    {
      "id": "get_user",
      "uuid": "550e8400-e29b-41d4-a716-446655440000"
    },
    {
      "id": "insert_order",
      "uuid": "550e8400-e29b-41d4-a716-446655440001"
    }
  ]
}
```

### List Updated Templates

Get templates updated since a specific timestamp.

```http
GET /api/v1/templates/updated
Content-Type: text/plain
Authorization: Bearer <token>
```

Body: ISO 8601 timestamp string

## Create Template

Create a new template.

### Request

```http
POST /api/v1/templates
Content-Type: application/json
Authorization: Bearer <token>
```

### Body Parameters

| Field        | Type   | Required | Description                          |
| ------------ | ------ | -------- | ------------------------------------ |
| `id`         | string | Yes      | Unique template identifier           |
| `endpoint`   | string | Yes      | Target endpoint for this template    |
| `query`      | string | Yes      | Query with Handlebars placeholders   |
| `fields`     | array  | No       | Field definitions for parameters     |
| `description`| string | No       | Template description                 |

### Example

```bash
curl http://{host}:8000/api/v1/templates \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "get_user_by_id",
    "endpoint": "postgres_main",
    "query": "SELECT * FROM users WHERE id = {{user_id}}",
    "fields": [
      {
        "name": "user_id",
        "type": "integer",
        "required": true
      }
    ],
    "description": "Fetch a user by their ID"
  }'
```

### Response

```json
{
  "status": "success",
  "data": "Template created successfully"
}
```

## Get Template

Get details of a specific template.

### Request

```http
GET /api/v1/templates/{template}
Authorization: Bearer <token>
```

### Path Parameters

| Parameter  | Type   | Description           |
| ---------- | ------ | --------------------- |
| `template` | string | Template identifier   |

### Example

```bash
curl http://{host}:8000/api/v1/templates/get_user_by_id \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "get_user_by_id",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "endpoint": "postgres_main",
    "query": "SELECT * FROM users WHERE id = {{user_id}}",
    "fields": [
      {
        "name": "user_id",
        "type": "integer",
        "required": true
      }
    ],
    "description": "Fetch a user by their ID",
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

## Execute Template

Run a template with provided parameters.

### Request

```http
POST /api/v1/templates/{template}
Content-Type: application/json
Authorization: Bearer <token>
```

### Path Parameters

| Parameter  | Type   | Description           |
| ---------- | ------ | --------------------- |
| `template` | string | Template identifier   |

### Body

JSON object containing template parameter values.

### Example

```bash
curl http://{host}:8000/api/v1/templates/get_user_by_id \
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
  "data": [
    {
      "id": 12345,
      "name": "John Doe",
      "email": "john@example.com",
      "created_at": "2024-01-10T08:00:00Z"
    }
  ]
}
```

## Render Template

Preview the rendered query without executing it.

### Request

```http
POST /api/v1/templates/{template}/render
Content-Type: application/json
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/templates/get_user_by_id/render \
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
    "query": "SELECT * FROM users WHERE id = 12345",
    "endpoint": "postgres_main"
  }
}
```

## Update Template

Update an existing template.

### Request

```http
PATCH /api/v1/templates/{template}
Content-Type: application/json
Authorization: Bearer <token>
```

### Body Parameters

| Field        | Type   | Required | Description                |
| ------------ | ------ | -------- | -------------------------- |
| `query`      | string | No       | Updated query              |
| `fields`     | array  | No       | Updated field definitions  |
| `description`| string | No       | Updated description        |

### Example

```bash
curl http://{host}:8000/api/v1/templates/get_user_by_id \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "query": "SELECT id, name, email FROM users WHERE id = {{user_id}}",
    "description": "Fetch basic user info by ID"
  }'
```

### Response

```json
{
  "status": "success",
  "data": "Template updated successfully"
}
```

## Delete Template

Remove a template.

### Request

```http
DELETE /api/v1/templates/{template}
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/templates/get_user_by_id \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

### Response

```json
{
  "status": "success",
  "data": "Template deleted successfully"
}
```

## Field Types

Templates support the following field types:

| Type      | Description                    |
| --------- | ------------------------------ |
| `string`  | Text value                     |
| `integer` | Whole number                   |
| `float`   | Decimal number                 |
| `boolean` | True/false value               |
| `array`   | List of values                 |
| `object`  | Nested JSON object             |

## Template Syntax

Templates use Handlebars-style placeholders:

### Basic Substitution

```sql
SELECT * FROM users WHERE name = '{{name}}'
```

### Conditional Blocks

```sql
SELECT * FROM orders
WHERE 1=1
{{#if status}}
  AND status = '{{status}}'
{{/if}}
```

### Iteration

```sql
INSERT INTO tags (name) VALUES
{{#each tags}}
  ('{{this}}'){{#unless @last}},{{/unless}}
{{/each}}
```

## Access Control

| Operation          | Required Access |
| ------------------ | --------------- |
| List templates     | Read            |
| Get template       | Read            |
| Create template    | Admin           |
| Execute template   | Read            |
| Render template    | Read            |
| Update template    | Admin           |
| Delete template    | Admin           |

## Error Responses

### Template Not Found

```json
{
  "error": "Not Found",
  "message": "Template 'get_user_by_id' does not exist"
}
```

### Missing Required Field

```json
{
  "error": "Bad Request",
  "message": "Required field 'user_id' is missing"
}
```

### Invalid Field Type

```json
{
  "error": "Bad Request",
  "message": "Field 'user_id' must be an integer"
}
```

### Template Render Error

```json
{
  "error": "Bad Request",
  "message": "Failed to render template: invalid syntax at position 45"
}
```

## Related

- [Templates Guide](../advanced/templates.md) - Template development
- [Workflows API](./workflows.md) - Multi-step operations
- [Endpoints](./endpoints.md) - Database connections
