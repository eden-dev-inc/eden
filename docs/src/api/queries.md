# Query Execution API

This reference covers the APIs for executing queries against Eden-MDBS endpoints.

## Overview

Eden provides three query execution endpoints:

| Endpoint                              | Purpose                              |
| ------------------------------------- | ------------------------------------ |
| `/api/v1/endpoints/{id}/read`         | Read-only queries (SELECT)           |
| `/api/v1/endpoints/{id}/write`        | Data modification (INSERT, UPDATE, DELETE) |
| `/api/v1/endpoints/{id}/transaction`  | Multiple operations atomically       |

## Read Queries

Execute read-only queries to retrieve data.

### Request

```http
POST /api/v1/endpoints/{endpoint_id}/read
Content-Type: application/json
Authorization: Bearer <token>
```

### Basic Query

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "SELECT * FROM users WHERE status = '\''active'\'' LIMIT 10"
  }'
```

### Parameterized Query

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "SELECT * FROM users WHERE status = $1 AND created_at > $2 LIMIT $3",
    "params": ["active", "2024-01-01", 10]
  }'
```

### Response

```json
{
  "status": "success",
  "data": {
    "rows": [
      {
        "id": 1,
        "name": "John Doe",
        "email": "john@example.com",
        "status": "active",
        "created_at": "2024-01-15T10:30:00Z"
      },
      {
        "id": 2,
        "name": "Jane Smith",
        "email": "jane@example.com",
        "status": "active",
        "created_at": "2024-01-16T09:15:00Z"
      }
    ],
    "row_count": 2
  }
}
```

## Write Queries

Execute data modification queries.

### Request

```http
POST /api/v1/endpoints/{endpoint_id}/write
Content-Type: application/json
Authorization: Bearer <token>
```

### INSERT

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "INSERT INTO users (name, email, status) VALUES ($1, $2, $3) RETURNING id",
    "params": ["Alice Johnson", "alice@example.com", "active"]
  }'
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "rows": [{"id": 3}],
    "rows_affected": 1
  }
}
```

### UPDATE

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "UPDATE users SET status = $1, updated_at = NOW() WHERE id = $2",
    "params": ["inactive", 1]
  }'
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "rows_affected": 1
  }
}
```

### DELETE

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "DELETE FROM users WHERE status = $1 AND last_login < $2",
    "params": ["inactive", "2023-01-01"]
  }'
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "rows_affected": 5
  }
}
```

## Transaction Queries

Execute multiple operations atomically.

### Request

```http
POST /api/v1/endpoints/{endpoint_id}/transaction
Content-Type: application/json
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/transaction \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {
        "query": "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
        "params": [100.00, 1]
      },
      {
        "query": "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
        "params": [100.00, 2]
      },
      {
        "query": "INSERT INTO transfers (from_id, to_id, amount) VALUES ($1, $2, $3)",
        "params": [1, 2, 100.00]
      }
    ]
  }'
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "results": [
      {"rows_affected": 1},
      {"rows_affected": 1},
      {"rows_affected": 1}
    ]
  }
}
```

## Database-Specific Queries

### MongoDB

```bash
curl http://{host}:8000/api/v1/endpoints/my_mongo/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "collection": "products",
    "operation": "find",
    "filter": {
      "category": "electronics",
      "price": {"$lt": 1000}
    },
    "options": {
      "limit": 20,
      "sort": {"price": 1}
    }
  }'
```

### Redis

```bash
# GET
curl http://{host}:8000/api/v1/endpoints/my_redis/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "command": "GET",
    "args": ["user:123"]
  }'

# SET
curl http://{host}:8000/api/v1/endpoints/my_redis/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "command": "SET",
    "args": ["user:123", "{\"name\": \"John\"}", "EX", "3600"]
  }'
```

## Request Parameters

### Query Object

| Field    | Type     | Required | Description                         |
| -------- | -------- | -------- | ----------------------------------- |
| `query`  | string   | Yes      | SQL query or command                |
| `params` | array    | No       | Query parameters for placeholders   |

### Transaction Object

| Field        | Type  | Required | Description                    |
| ------------ | ----- | -------- | ------------------------------ |
| `operations` | array | Yes      | Array of query objects         |

## Access Control

| Operation   | Required Access Level |
| ----------- | --------------------- |
| Read        | Read                  |
| Write       | Write                 |
| Transaction | Write                 |

## Error Responses

### SQL Syntax Error

```json
{
  "error": "SQL syntax error",
  "message": "Syntax error at or near 'SELCT'"
}
```

### Permission Denied

```json
{
  "error": "Access denied",
  "details": "User does not have Write access to endpoint",
  "access_level": "Read",
  "required_level": "Write"
}
```

### Connection Error

```json
{
  "error": "Connection failed",
  "message": "Failed to connect to database within 30 seconds"
}
```

### Invalid Parameter Count

```json
{
  "error": "Bad Request",
  "message": "Query has 3 parameters but 2 were provided"
}
```

## Best Practices

### Use Parameterized Queries

Always use parameters instead of string concatenation:

```bash
# Good - use parameters
{"query": "SELECT * FROM users WHERE id = $1", "params": [123]}

# Bad - string concatenation (SQL injection risk)
{"query": "SELECT * FROM users WHERE id = 123"}
```

### Limit Result Sets

Always include LIMIT clauses for unbounded queries:

```bash
{"query": "SELECT * FROM logs ORDER BY created_at DESC LIMIT 100"}
```

### Use Transactions for Related Operations

Group related operations in transactions:

```bash
{
  "operations": [
    {"query": "UPDATE inventory SET quantity = quantity - 1 WHERE id = $1", "params": [123]},
    {"query": "INSERT INTO orders (product_id, quantity) VALUES ($1, 1)", "params": [123]}
  ]
}
```

### Handle Errors Gracefully

Check response status and handle errors appropriately in your application code.

## Related

- [Endpoints](./endpoints.md) - Endpoint management
- [Transactions](./transactions.md) - Transaction API details
- [Error Responses](./errors.md) - Error handling
- [Transactions Guide](../guide/transactions.md) - Transaction patterns
