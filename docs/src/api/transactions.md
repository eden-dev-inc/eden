# Transaction APIs

This reference covers the APIs for executing atomic transactions in Eden-MDBS.

## Overview

Transactions allow you to execute multiple database operations as a single atomic unit. All operations either succeed together or fail together, with automatic rollback on failure.

## Execute Transaction

Execute multiple operations atomically on a single endpoint.

### Request

```http
POST /api/v1/endpoints/{endpoint_id}/transaction
Content-Type: application/json
Authorization: Bearer <token>
```

### Path Parameters

| Parameter     | Type   | Description                  |
| ------------- | ------ | ---------------------------- |
| `endpoint_id` | string | The endpoint ID to query     |

### Body Parameters

| Field        | Type  | Required | Description              |
| ------------ | ----- | -------- | ------------------------ |
| `operations` | array | Yes      | Array of query objects   |

### Query Object

| Field    | Type   | Required | Description                       |
| -------- | ------ | -------- | --------------------------------- |
| `query`  | string | Yes      | SQL query or command              |
| `params` | array  | No       | Query parameters for placeholders |

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
        "query": "INSERT INTO transfers (from_id, to_id, amount, created_at) VALUES ($1, $2, $3, NOW())",
        "params": [1, 2, 100.00]
      }
    ]
  }'
```

### Success Response

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

### Failure Response

When any operation fails, all operations are rolled back:

```json
{
  "error": "Transaction failed",
  "message": "Operation 2 failed: insufficient balance",
  "details": {
    "failed_operation": 2,
    "rolled_back": true
  }
}
```

## Transaction with RETURNING

Capture results from operations using RETURNING:

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/transaction \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {
        "query": "INSERT INTO orders (user_id, total) VALUES ($1, $2) RETURNING id",
        "params": [123, 299.99]
      },
      {
        "query": "INSERT INTO order_items (order_id, product_id, quantity) VALUES ($1, $2, $3)",
        "params": [1, 456, 2]
      }
    ]
  }'
```

### Response

```json
{
  "status": "success",
  "data": {
    "results": [
      {"rows": [{"id": 789}], "rows_affected": 1},
      {"rows_affected": 1}
    ]
  }
}
```

## Common Patterns

### Money Transfer

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/transaction \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {
        "query": "UPDATE accounts SET balance = balance - $1 WHERE id = $2 AND balance >= $1",
        "params": [500.00, 1001]
      },
      {
        "query": "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
        "params": [500.00, 1002]
      },
      {
        "query": "INSERT INTO transfers (from_account, to_account, amount, status) VALUES ($1, $2, $3, $4)",
        "params": [1001, 1002, 500.00, "completed"]
      }
    ]
  }'
```

### Order with Inventory

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/transaction \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {
        "query": "UPDATE inventory SET quantity = quantity - $1 WHERE product_id = $2 AND quantity >= $1",
        "params": [5, 789]
      },
      {
        "query": "INSERT INTO orders (customer_id, product_id, quantity, status) VALUES ($1, $2, $3, $4) RETURNING id",
        "params": [123, 789, 5, "pending"]
      }
    ]
  }'
```

### User Registration

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/transaction \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {
        "query": "INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id",
        "params": ["user@example.com", "$2b$10$hash"]
      },
      {
        "query": "INSERT INTO profiles (user_id, display_name) VALUES ($1, $2)",
        "params": [1, "New User"]
      },
      {
        "query": "INSERT INTO settings (user_id, notifications) VALUES ($1, $2)",
        "params": [1, true]
      }
    ]
  }'
```

## Access Control

| Operation           | Required Access |
| ------------------- | --------------- |
| Execute transaction | Write           |

## Error Responses

### Constraint Violation

```json
{
  "error": "Transaction failed",
  "message": "violates check constraint \"accounts_balance_check\"",
  "details": {
    "failed_operation": 1,
    "rolled_back": true
  }
}
```

### Foreign Key Violation

```json
{
  "error": "Transaction failed",
  "message": "violates foreign key constraint \"orders_user_id_fkey\"",
  "details": {
    "failed_operation": 2,
    "rolled_back": true
  }
}
```

### Deadlock Detected

```json
{
  "error": "Transaction failed",
  "message": "deadlock detected"
}
```

### Lock Timeout

```json
{
  "error": "Transaction failed",
  "message": "lock wait timeout exceeded"
}
```

### Invalid Parameter Count

```json
{
  "error": "Bad Request",
  "message": "Operation 1: Query has 3 parameters but 2 were provided"
}
```

## Transaction Behavior

### Atomicity
All operations succeed or all fail. No partial commits.

### Isolation
Intermediate states are not visible to other transactions.

### Execution Order
Operations execute in the order provided in the array.

### Automatic Rollback
If any operation fails, all previous operations are automatically rolled back.

## Limitations

- Transactions are scoped to a single endpoint
- Cannot span multiple database types
- Maximum operations per transaction may be limited by configuration

## Best Practices

### Keep Transactions Short
Long transactions hold locks and can cause deadlocks or timeouts.

### Order Operations Consistently
Always access tables in the same order to prevent deadlocks.

### Validate Before Executing
Use constraints and conditions in queries to catch errors early:
```sql
UPDATE accounts SET balance = balance - $1 WHERE id = $2 AND balance >= $1
```

### Handle Failures Gracefully
Always check response status and handle rollback scenarios in your application.

## Related

- [Query Execution](./queries.md) - Single query operations
- [Workflow APIs](./workflows.md) - Multi-step operations
- [Transactions Guide](../guide/transactions.md) - Patterns and concepts
- [Transaction Examples](../examples/transactions.md) - Practical examples
