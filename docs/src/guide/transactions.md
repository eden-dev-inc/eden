# Transactions

Eden-MDBS supports atomic transactions for executing multiple database operations as a single unit of work. This guide covers single-endpoint transactions and considerations for cross-database operations.

## What Are Transactions?

Transactions ensure that a group of operations either all succeed or all fail together, maintaining data consistency. This is essential for operations like:

- Transferring funds between accounts
- Creating related records across multiple tables
- Updating inventory and order status together

## Single-Endpoint Transactions

### Basic Transaction

Execute multiple operations atomically on a single endpoint:

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
        "query": "INSERT INTO transactions (from_id, to_id, amount, created_at) VALUES ($1, $2, $3, NOW())",
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

### Transaction Behavior

- **Atomicity**: All operations succeed or all are rolled back
- **Isolation**: Intermediate states are not visible to other transactions
- **Order**: Operations execute in the order provided

### Transaction with RETURNING

Capture results from individual operations:

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
        "params": ["{{result[0].id}}", 456, 2]
      }
    ]
  }'
```

## Common Transaction Patterns

### Financial Transfer

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/transaction \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {
        "query": "UPDATE accounts SET balance = balance - $1 WHERE id = $2 AND balance >= $1",
        "params": [500.00, 1]
      },
      {
        "query": "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
        "params": [500.00, 2]
      },
      {
        "query": "INSERT INTO audit_log (action, amount, from_account, to_account) VALUES ($1, $2, $3, $4)",
        "params": ["transfer", 500.00, 1, 2]
      }
    ]
  }'
```

### Inventory and Order

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
        "query": "INSERT INTO orders (customer_id, product_id, quantity, status) VALUES ($1, $2, $3, $4)",
        "params": [123, 789, 5, "pending"]
      }
    ]
  }'
```

### Cascading Updates

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/transaction \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {
        "query": "UPDATE users SET status = $1, updated_at = NOW() WHERE id = $2",
        "params": ["inactive", 123]
      },
      {
        "query": "UPDATE sessions SET terminated_at = NOW() WHERE user_id = $1 AND terminated_at IS NULL",
        "params": [123]
      },
      {
        "query": "INSERT INTO user_status_history (user_id, old_status, new_status) VALUES ($1, $2, $3)",
        "params": [123, "active", "inactive"]
      }
    ]
  }'
```

## Transaction Templates

Create reusable transaction templates:

```bash
curl http://{host}:8000/api/v1/templates \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
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
          }
        ]
      },
      "endpoint_kind": "Postgres"
    }
  }'
```

Execute the template:

```bash
curl http://{host}:8000/api/v1/templates/transfer_funds \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X POST \
  -d '{
    "from_account": 1,
    "to_account": 2,
    "amount": 100.00
  }'
```

## Error Handling

### Transaction Rollback

If any operation fails, all previous operations are rolled back:

```json
{
  "error": "Transaction failed",
  "message": "Operation 2 failed: insufficient funds",
  "details": {
    "failed_operation": 2,
    "rolled_back": true
  }
}
```

### Common Errors

**Constraint Violation:**
```json
{
  "error": "Transaction failed",
  "message": "violates check constraint \"accounts_balance_check\""
}
```

**Deadlock:**
```json
{
  "error": "Transaction failed",
  "message": "deadlock detected"
}
```

**Timeout:**
```json
{
  "error": "Transaction failed",
  "message": "lock wait timeout exceeded"
}
```

## Access Control

Transactions require **Write** access to the endpoint:

| Access Level | Can Execute Transactions |
| ------------ | ------------------------ |
| Read         | No                       |
| Write        | Yes                      |
| Admin        | Yes                      |
| SuperAdmin   | Yes                      |

## Best Practices

### Transaction Design

- **Keep transactions short**: Long transactions hold locks longer
- **Order operations consistently**: Helps prevent deadlocks
- **Include validation**: Add checks (like `balance >= amount`) in your queries
- **Handle failures**: Design your application to handle rollbacks gracefully

### Performance

- **Minimize operations**: Only include necessary operations
- **Use indexes**: Ensure queries are optimized
- **Avoid nested transactions**: Use a single flat transaction

### Data Integrity

- **Use constraints**: Let the database enforce rules
- **Include audit trails**: Log important operations
- **Test edge cases**: Verify behavior with concurrent access

## Limitations

- Transactions are scoped to a single endpoint
- For cross-database operations, consider using workflows with compensating transactions
- Maximum number of operations per transaction may be limited

## Related

- [Endpoints](./endpoints.md) - Database connections
- [Templates](../advanced/templates.md) - Reusable operations
- [Workflows](./workflows.md) - Multi-step operations
- [Query Execution](../api/queries.md) - Basic queries
