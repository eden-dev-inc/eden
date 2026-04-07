# Templates

Templates in Eden-MDBS are reusable, parameterized operations that define database queries, API calls, and data transformations. They provide a consistent and secure way to execute operations across different endpoints.

## What Are Templates?

Templates are structured definitions that:

- Define reusable operations that can be executed across endpoints
- Support parameterization through Handlebars templating
- Provide type safety and validation for inputs
- Integrate with RBAC for access control
- Can be composed into workflows

## Template Types

Eden supports four types of template operations:

| Type                     | Description                                    |
| ------------------------ | ---------------------------------------------- |
| **Read**                 | Query operations that retrieve data            |
| **Write**                | Operations that modify data (INSERT, UPDATE, DELETE) |
| **Transaction**          | Multi-operation transactions with rollback     |
| **TwoPhaseTransaction**  | Distributed transactions across endpoints      |

## Creating Templates

### Basic Structure

```json
{
  "id": "get_user_orders",
  "description": "Retrieve all orders for a specific user",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Read",
    "template": {
      "query": "SELECT * FROM orders WHERE user_id = {{user_id}} ORDER BY created_at DESC",
      "params": ["{{user_id}}"]
    },
    "endpoint_kind": "Postgres"
  }
}
```

### Create Template Request

```bash
curl http://{host}:8000/api/v1/templates \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "get_user_orders",
    "description": "Retrieve all orders for a specific user",
    "template": {
      "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
      "kind": "Read",
      "template": {
        "query": "SELECT * FROM orders WHERE user_id = {{user_id}}",
        "params": ["{{user_id}}"]
      },
      "endpoint_kind": "Postgres"
    }
  }'
```

**Response:**

```json
{
  "status": "success",
  "message": "success"
}
```

## Template Examples

### PostgreSQL Read Template

```json
{
  "id": "user_orders_summary",
  "description": "Get user with order statistics",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Read",
    "template": {
      "query": "SELECT u.*, COUNT(o.id) as order_count, SUM(o.total) as total_spent FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.id = {{user_id}} GROUP BY u.id",
      "params": ["{{user_id}}"]
    },
    "endpoint_kind": "Postgres"
  }
}
```

### PostgreSQL Write Template

```json
{
  "id": "create_order",
  "description": "Create a new order",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Write",
    "template": {
      "query": "INSERT INTO orders (user_id, product_id, quantity, total) VALUES ({{user_id}}, {{product_id}}, {{quantity}}, {{total}}) RETURNING id",
      "params": ["{{user_id}}", "{{product_id}}", "{{quantity}}", "{{total}}"]
    },
    "endpoint_kind": "Postgres"
  }
}
```

### Redis Template

```json
{
  "id": "cache_user_session",
  "description": "Store user session in cache",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440001",
    "kind": "Write",
    "template": {
      "operation": "set",
      "key": "session:{{user_id}}",
      "value": "{{session_data}}",
      "ttl": "{{ttl}}"
    },
    "endpoint_kind": "Redis"
  }
}
```

### MongoDB Template

```json
{
  "id": "find_user_documents",
  "description": "Find documents by user",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440002",
    "kind": "Read",
    "template": {
      "collection": "documents",
      "operation": "find",
      "filter": {
        "user_id": "{{user_id}}",
        "type": "{{doc_type}}"
      },
      "options": {
        "limit": "{{limit}}",
        "sort": {"created_at": -1}
      }
    },
    "endpoint_kind": "Mongo"
  }
}
```

### HTTP API Template

```json
{
  "id": "external_api_call",
  "description": "Call external API",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440003",
    "kind": "Write",
    "template": {
      "method": "POST",
      "path": "/api/users/{{user_id}}/sync",
      "headers": {
        "Content-Type": "application/json"
      },
      "body": {
        "data": "{{payload}}"
      }
    },
    "endpoint_kind": "Http"
  }
}
```

### Transaction Template

```json
{
  "id": "transfer_funds",
  "description": "Transfer funds between accounts",
  "template": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Transaction",
    "template": {
      "operations": [
        {
          "query": "UPDATE accounts SET balance = balance - {{amount}} WHERE id = {{from_account}}",
          "params": ["{{amount}}", "{{from_account}}"]
        },
        {
          "query": "UPDATE accounts SET balance = balance + {{amount}} WHERE id = {{to_account}}",
          "params": ["{{amount}}", "{{to_account}}"]
        },
        {
          "query": "INSERT INTO transactions (from_id, to_id, amount) VALUES ({{from_account}}, {{to_account}}, {{amount}})",
          "params": ["{{from_account}}", "{{to_account}}", "{{amount}}"]
        }
      ]
    },
    "endpoint_kind": "Postgres"
  }
}
```

## Handlebars Templating

Templates use Handlebars syntax for parameter substitution.

### Basic Substitution

```handlebars
SELECT * FROM users WHERE id = {{user_id}}
```

### Conditional Logic

```handlebars
SELECT * FROM orders WHERE user_id = {{user_id}}
{{#if status}}
AND status = '{{status}}'
{{/if}}
{{#if date_from}}
AND created_at >= '{{date_from}}'
{{/if}}
```

### Loops

```handlebars
SELECT * FROM products WHERE id IN (
{{#each product_ids}}
  {{this}}{{#unless @last}},{{/unless}}
{{/each}}
)
```

## Executing Templates

### Run Template

Execute a template with parameters:

```bash
curl http://{host}:8000/api/v1/templates/get_user_orders \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X POST \
  -d '{
    "user_id": 12345
  }'
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "rows": [
      {
        "id": 1001,
        "user_id": 12345,
        "product_id": 456,
        "total": 199.98,
        "created_at": "2024-01-15T10:30:00Z"
      }
    ],
    "row_count": 1
  }
}
```

### Render Template (Preview Only)

Preview the rendered template without executing:

```bash
curl http://{host}:8000/api/v1/templates/get_user_orders/render \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X POST \
  -d '{
    "user_id": 12345
  }'
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Read",
    "request": {
      "query": "SELECT * FROM orders WHERE user_id = 12345",
      "params": [12345]
    }
  }
}
```

## Retrieving Templates

### Get Template Details

```bash
curl http://{host}:8000/api/v1/templates/get_user_orders \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "get_user_orders",
    "uuid": "550e8400-e29b-41d4-a716-446655440004",
    "description": "Retrieve all orders for a specific user",
    "template": {
      "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
      "kind": "Read",
      "template": {
        "query": "SELECT * FROM orders WHERE user_id = {{user_id}}",
        "params": ["{{user_id}}"]
      },
      "endpoint_kind": "Postgres"
    },
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

### List All Templates

```bash
curl http://{host}:8000/api/v1/templates \
  -H "Authorization: Bearer $TOKEN"
```

## Updating Templates

```bash
curl http://{host}:8000/api/v1/templates/get_user_orders \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "description": "Updated description",
    "template": {
      "query": "SELECT * FROM orders WHERE user_id = {{user_id}} AND status != '\''cancelled'\'' ORDER BY created_at DESC"
    }
  }'
```

## Deleting Templates

```bash
curl http://{host}:8000/api/v1/templates/get_user_orders \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

**Response:**

```json
{
  "status": "success",
  "message": "success"
}
```

## Access Control

Templates integrate with Eden's RBAC system:

| Access Level | Permissions                              |
| ------------ | ---------------------------------------- |
| **Read**     | Execute Read templates                   |
| **Write**    | Execute Read and Write templates         |
| **Admin**    | Create, update, delete templates         |

## Error Handling

### Template Not Found

```json
{
  "error": "Not Found",
  "message": "Template get_user_orders not found"
}
```

### Missing Parameter

```json
{
  "error": "Bad Request",
  "message": "Required parameter missing: user_id"
}
```

### Invalid Template Syntax

```json
{
  "error": "Bad Request",
  "message": "Handlebars parsing error: Unclosed expression"
}
```

### Insufficient Permissions

```json
{
  "error": "Forbidden",
  "message": "Write access required for this template"
}
```

## Best Practices

### Template Design

- **Single responsibility**: Each template should do one thing well
- **Use parameters**: Never hardcode values that might change
- **Document clearly**: Write meaningful descriptions
- **Test thoroughly**: Verify templates with various parameter combinations

### Security

- **Parameterize queries**: Always use parameters to prevent SQL injection
- **Validate inputs**: Check parameter values before template execution
- **Use least privilege**: Grant minimum necessary access levels
- **Audit usage**: Monitor template execution for unusual patterns

### Performance

- **Optimize queries**: Write efficient SQL with proper indexing
- **Limit results**: Use LIMIT clauses to prevent large result sets
- **Cache wisely**: Consider caching frequently-used template results

## Related

- [Concepts](../getting-started/concepts.md) - Core concepts overview
- [Endpoints](../guide/endpoints.md) - Database connections
- [Workflows](./workflows.md) - Multi-step operations
- [RBAC](../guide/rbac.md) - Access control
