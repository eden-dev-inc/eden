# Basic Examples

This page provides practical examples of common Eden-MDBS operations.

## Authentication

### Login and Get Token

```bash
# Login with username and password
curl http://{host}:8000/api/v1/auth/login \
  -u admin@company.com:your_password \
  -X POST

# Response: {"token": "eyJhbGciOiJIUzI1NiIs..."}

# Save token for subsequent requests
export TOKEN="eyJhbGciOiJIUzI1NiIs..."
```

### Refresh Token

```bash
curl http://{host}:8000/api/v1/auth/refresh \
  -H "Authorization: Bearer $TOKEN" \
  -X POST
```

## Endpoint Management

### Create PostgreSQL Endpoint

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "main_db",
    "kind": "Postgres",
    "config": {
      "write_conn": {
        "url": "postgresql://user:password@localhost:5432/myapp"
      }
    }
  }'
```

### Create Redis Endpoint

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "cache",
    "kind": "Redis",
    "config": {
      "write_conn": {
        "url": "redis://localhost:6379"
      }
    }
  }'
```

### Create MongoDB Endpoint

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "documents",
    "kind": "Mongo",
    "config": {
      "write_conn": {
        "url": "mongodb://user:password@localhost:27017/myapp"
      }
    }
  }'
```

### List All Endpoints

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Authorization: Bearer $TOKEN"
```

## Query Operations

### PostgreSQL Read Query

```bash
# Simple query
curl http://{host}:8000/api/v1/endpoints/main_db/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"query": "SELECT * FROM users LIMIT 10"}'

# Parameterized query
curl http://{host}:8000/api/v1/endpoints/main_db/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "SELECT * FROM users WHERE status = $1 AND role = $2",
    "params": ["active", "admin"]
  }'
```

### PostgreSQL Write Query

```bash
# INSERT
curl http://{host}:8000/api/v1/endpoints/main_db/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
    "params": ["John Doe", "john@example.com"]
  }'

# UPDATE
curl http://{host}:8000/api/v1/endpoints/main_db/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "UPDATE users SET status = $1 WHERE id = $2",
    "params": ["inactive", 123]
  }'

# DELETE
curl http://{host}:8000/api/v1/endpoints/main_db/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "DELETE FROM sessions WHERE expires_at < NOW()"
  }'
```

### MongoDB Operations

```bash
# Find documents
curl http://{host}:8000/api/v1/endpoints/documents/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "collection": "products",
    "operation": "find",
    "filter": {"category": "electronics", "price": {"$lt": 500}},
    "options": {"limit": 10, "sort": {"price": 1}}
  }'

# Insert document
curl http://{host}:8000/api/v1/endpoints/documents/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "collection": "products",
    "operation": "insertOne",
    "document": {
      "name": "Wireless Mouse",
      "category": "electronics",
      "price": 29.99
    }
  }'
```

### Redis Operations

```bash
# SET
curl http://{host}:8000/api/v1/endpoints/cache/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "command": "SET",
    "args": ["session:user123", "{\"id\":123,\"role\":\"admin\"}", "EX", "3600"]
  }'

# GET
curl http://{host}:8000/api/v1/endpoints/cache/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "command": "GET",
    "args": ["session:user123"]
  }'

# DELETE
curl http://{host}:8000/api/v1/endpoints/cache/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "command": "DEL",
    "args": ["session:user123"]
  }'
```

## User Management

### Create a User

```bash
curl http://{host}:8000/api/v1/iam/users \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "username": "developer@company.com",
    "password": "SecurePass123!",
    "description": "Backend Developer",
    "access_level": "Write"
  }'
```

### Get User Info

```bash
curl http://{host}:8000/api/v1/iam/users/developer@company.com \
  -H "Authorization: Bearer $TOKEN"
```

### Update User

```bash
curl http://{host}:8000/api/v1/iam/users/developer@company.com \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "description": "Senior Backend Developer",
    "access_level": "Admin"
  }'
```

## Access Control (RBAC)

### Grant Endpoint Access

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/subjects \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "subjects": [
      ["developer@company.com", "Write"],
      ["analyst@company.com", "Read"]
    ]
  }'
```

### Check User's Permissions

```bash
# Check your own permissions on an endpoint
curl http://{host}:8000/api/v1/iam/rbac/endpoints/main_db/subjects \
  -H "Authorization: Bearer $TOKEN"

# Check all permissions for an endpoint (Admin only)
curl http://{host}:8000/api/v1/iam/rbac/endpoints/main_db \
  -H "Authorization: Bearer $TOKEN"
```

### Remove User Access

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/main_db/subjects/developer@company.com \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

## Templates

### Create a Template

```bash
curl http://{host}:8000/api/v1/templates \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "get_user_orders",
    "description": "Get orders for a user with pagination",
    "template": {
      "endpoint_uuid": "YOUR_ENDPOINT_UUID",
      "kind": "Read",
      "template": {
        "query": "SELECT * FROM orders WHERE user_id = {{user_id}} ORDER BY created_at DESC LIMIT {{limit}} OFFSET {{offset}}",
        "params": ["{{user_id}}", "{{limit}}", "{{offset}}"]
      },
      "endpoint_kind": "Postgres"
    }
  }'
```

### Execute a Template

```bash
curl http://{host}:8000/api/v1/templates/get_user_orders \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X POST \
  -d '{
    "user_id": 123,
    "limit": 10,
    "offset": 0
  }'
```

### Preview Template (Render Only)

```bash
curl http://{host}:8000/api/v1/templates/get_user_orders/render \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X POST \
  -d '{
    "user_id": 123,
    "limit": 10,
    "offset": 0
  }'
```

## Transactions

### Execute a Transaction

```bash
curl http://{host}:8000/api/v1/endpoints/main_db/transaction \
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

## Organization Info

### Get Organization Details

```bash
curl http://{host}:8000/api/v1/organizations \
  -H "Authorization: Bearer $TOKEN"
```

### Get Verbose Organization Info

```bash
curl http://{host}:8000/api/v1/organizations \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Eden-Verbose: true"
```

## Complete Workflow Example

Here's a complete example of setting up and using Eden:

```bash
#!/bin/bash

HOST="your-eden-host"
ORG_TOKEN="your_organization_creation_token"

# 1. Create organization (one-time setup)
curl http://$HOST:8000/api/v1/new \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ORG_TOKEN" \
  -d '{
    "id": "my_company",
    "super_admins": [{"username": "admin", "password": "AdminPass123!"}]
  }'

# 2. Login as admin
TOKEN=$(curl -s http://$HOST:8000/api/v1/auth/login \
  -u admin:AdminPass123! \
  -X POST | jq -r '.token')

# 3. Create an endpoint
curl http://$HOST:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "app_db",
    "kind": "Postgres",
    "config": {
      "write_conn": {"url": "postgresql://user:pass@db:5432/app"}
    }
  }'

# 4. Create a user
curl http://$HOST:8000/api/v1/iam/users \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "username": "dev@company.com",
    "password": "DevPass123!",
    "access_level": "Write"
  }'

# 5. Grant endpoint access to user
curl http://$HOST:8000/api/v1/iam/rbac/endpoints/subjects \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"subjects": [["dev@company.com", "Write"]]}'

# 6. User can now query the endpoint
DEV_TOKEN=$(curl -s http://$HOST:8000/api/v1/auth/login \
  -u dev@company.com:DevPass123! \
  -X POST | jq -r '.token')

curl http://$HOST:8000/api/v1/endpoints/app_db/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $DEV_TOKEN" \
  -d '{"query": "SELECT * FROM users LIMIT 5"}'
```

## Related

- [First Steps](../getting-started/first-steps.md) - Getting started guide
- [API Reference](../api/overview.md) - Complete API documentation
- [Troubleshooting](../operations/troubleshooting.md) - Common issues
