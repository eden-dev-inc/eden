# Transaction Examples

This page provides practical examples of using transactions in Eden-MDBS.

## Basic Transaction

Execute multiple operations atomically:

```bash
curl http://{host}:8000/api/v1/endpoints/main_db/transaction \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {
        "query": "INSERT INTO orders (user_id, total, status) VALUES ($1, $2, $3) RETURNING id",
        "params": [123, 299.99, "pending"]
      },
      {
        "query": "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES ($1, $2, $3, $4)",
        "params": [1, 456, 2, 149.99]
      },
      {
        "query": "UPDATE inventory SET quantity = quantity - $1 WHERE product_id = $2",
        "params": [2, 456]
      }
    ]
  }'
```

## Money Transfer

Transfer funds between accounts with audit logging:

```bash
curl http://{host}:8000/api/v1/endpoints/main_db/transaction \
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
        "query": "INSERT INTO transfers (from_account, to_account, amount, status, created_at) VALUES ($1, $2, $3, $4, NOW())",
        "params": [1001, 1002, 500.00, "completed"]
      }
    ]
  }'
```

## E-commerce Order

Create an order with inventory check:

```bash
curl http://{host}:8000/api/v1/endpoints/main_db/transaction \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {
        "query": "UPDATE products SET stock = stock - $1 WHERE id = $2 AND stock >= $1 RETURNING id, stock",
        "params": [3, 789]
      },
      {
        "query": "INSERT INTO orders (customer_id, total_amount, status, created_at) VALUES ($1, $2, $3, NOW()) RETURNING id",
        "params": [456, 89.97, "confirmed"]
      },
      {
        "query": "INSERT INTO order_lines (order_id, product_id, quantity, unit_price) VALUES ($1, $2, $3, $4)",
        "params": [1, 789, 3, 29.99]
      },
      {
        "query": "INSERT INTO order_status_history (order_id, status, changed_at) VALUES ($1, $2, NOW())",
        "params": [1, "confirmed"]
      }
    ]
  }'
```

## User Registration

Create user with related records:

```bash
curl http://{host}:8000/api/v1/endpoints/main_db/transaction \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {
        "query": "INSERT INTO users (email, password_hash, created_at) VALUES ($1, $2, NOW()) RETURNING id",
        "params": ["user@example.com", "$2b$10$hashedpassword"]
      },
      {
        "query": "INSERT INTO user_profiles (user_id, display_name, created_at) VALUES ($1, $2, NOW())",
        "params": [1, "New User"]
      },
      {
        "query": "INSERT INTO user_settings (user_id, notifications_enabled, theme) VALUES ($1, $2, $3)",
        "params": [1, true, "light"]
      },
      {
        "query": "INSERT INTO audit_log (action, entity_type, entity_id, created_at) VALUES ($1, $2, $3, NOW())",
        "params": ["CREATE", "user", 1]
      }
    ]
  }'
```

## Soft Delete with Cascade

Soft delete a record and update related records:

```bash
curl http://{host}:8000/api/v1/endpoints/main_db/transaction \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {
        "query": "UPDATE users SET deleted_at = NOW(), status = $1 WHERE id = $2",
        "params": ["deleted", 123]
      },
      {
        "query": "UPDATE sessions SET terminated_at = NOW() WHERE user_id = $1 AND terminated_at IS NULL",
        "params": [123]
      },
      {
        "query": "UPDATE user_tokens SET revoked_at = NOW() WHERE user_id = $1 AND revoked_at IS NULL",
        "params": [123]
      },
      {
        "query": "INSERT INTO deletion_log (user_id, deleted_at, deleted_by) VALUES ($1, NOW(), $2)",
        "params": [123, "admin@company.com"]
      }
    ]
  }'
```

## Batch Update with Logging

Update multiple records and log the changes:

```bash
curl http://{host}:8000/api/v1/endpoints/main_db/transaction \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {
        "query": "UPDATE products SET price = price * 1.1 WHERE category = $1 RETURNING id, price",
        "params": ["electronics"]
      },
      {
        "query": "INSERT INTO price_change_log (category, change_percent, applied_at, applied_by) VALUES ($1, $2, NOW(), $3)",
        "params": ["electronics", 10, "pricing_system"]
      }
    ]
  }'
```

## Transaction Template

Create a reusable transaction template:

```bash
# Create the template
curl http://{host}:8000/api/v1/templates \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "process_payment",
    "description": "Process payment with balance update and logging",
    "template": {
      "endpoint_uuid": "YOUR_ENDPOINT_UUID",
      "kind": "Transaction",
      "template": {
        "operations": [
          {
            "query": "UPDATE accounts SET balance = balance - {{amount}} WHERE id = {{account_id}} AND balance >= {{amount}}",
            "params": ["{{amount}}", "{{account_id}}", "{{amount}}"]
          },
          {
            "query": "INSERT INTO payments (account_id, amount, reference, created_at) VALUES ({{account_id}}, {{amount}}, {{reference}}, NOW())",
            "params": ["{{account_id}}", "{{amount}}", "{{reference}}"]
          }
        ]
      },
      "endpoint_kind": "Postgres"
    }
  }'

# Execute the template
curl http://{host}:8000/api/v1/templates/process_payment \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X POST \
  -d '{
    "account_id": 1001,
    "amount": 99.99,
    "reference": "PAY-2024-001"
  }'
```

## Response Handling

### Successful Transaction

```json
{
  "status": "success",
  "data": {
    "results": [
      {"rows": [{"id": 1}], "rows_affected": 1},
      {"rows_affected": 1},
      {"rows_affected": 1}
    ]
  }
}
```

### Failed Transaction (Rolled Back)

```json
{
  "error": "Transaction failed",
  "message": "Operation 1 failed: new row for relation \"accounts\" violates check constraint \"accounts_balance_check\"",
  "details": {
    "failed_operation": 1,
    "rolled_back": true
  }
}
```

## Best Practices

### Order Operations Correctly
Put operations that are most likely to fail early to minimize work before rollback.

### Use Constraints
Let database constraints catch invalid states:
```sql
-- Check constraint ensures balance never goes negative
ALTER TABLE accounts ADD CONSTRAINT accounts_balance_check CHECK (balance >= 0);
```

### Keep Transactions Short
Long transactions hold locks and can cause deadlocks:
```bash
# Bad: Multiple unrelated operations
# Good: Split into separate transactions when operations are independent
```

### Handle Errors
Always check the response and handle failures in your application:
```javascript
const result = await executeTransaction(operations);
if (result.error) {
  // Handle rollback scenario
  await notifyUser("Transaction failed, please try again");
}
```

## Related

- [Transactions Guide](../guide/transactions.md) - Transaction concepts
- [Basic Examples](./basic.md) - Simple query examples
- [Templates](../advanced/templates.md) - Creating transaction templates
