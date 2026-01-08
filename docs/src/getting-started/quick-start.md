# Quick Start

Get started with Eden-MDBS in 5 minutes. This guide assumes you have a running Eden-MDBS instance.

## Prerequisites

- A running Eden-MDBS instance (with its URL and org creation token)
- curl or any HTTP client

## Step 1: Create an Organization

Every operation in Eden-MDBS is scoped to an organization. Organizations require a creation token (set via `EDEN_NEW_ORG_TOKEN` environment variable on the server).

```bash
curl http://{host}:8000/api/v1/new \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer {EDEN_NEW_ORG_TOKEN}" \
  -d '{
    "id": "quickstart",
    "super_admins": [
      {
        "username": "admin",
        "password": "secure_password_123"
      }
    ]
  }'
```

Response:

```json
{
  "id": "quickstart",
  "uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

You now have:

- Organization ID: `quickstart`
- Super admin user: `admin` with password `secure_password_123`

## Step 2: Login to Get a JWT Token

Eden-MDBS uses JWT tokens for authenticated requests. Login with basic auth to get a token:

```bash
curl http://{host}:8000/api/v1/auth/login \
  -u admin:secure_password_123 \
  -X POST
```

Response:

```json
{
  "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..."
}
```

Save this token - you'll use it for subsequent requests. For convenience in this guide, export it:

```bash
export TOKEN="<paste your token here>"
```

## Step 3: Connect a Database Endpoint

Connect a PostgreSQL database:

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "mydb",
    "kind": "Postgres",
    "config": {
      "write_conn": {
        "url": "postgresql://user:password@db-host:5432/database"
      }
    }
  }'
```

> **Note:** Replace the connection URL with your actual PostgreSQL credentials and host.

Response:

```json
{
  "id": "mydb",
  "uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

You've connected a PostgreSQL endpoint named `mydb`.

## Step 4: Execute Your First Query

Write to the endpoint (create a table):

```bash
curl http://{host}:8000/api/v1/endpoints/mydb/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT, email TEXT)"
  }'
```

Insert data:

```bash
curl http://{host}:8000/api/v1/endpoints/mydb/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "INSERT INTO users (name, email) VALUES ('\''Alice'\'', '\''alice@example.com'\''), ('\''Bob'\'', '\''bob@example.com'\'')"
  }'
```

Read data:

```bash
curl http://{host}:8000/api/v1/endpoints/mydb/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "SELECT * FROM users"
  }'
```

Response:

```json
{
  "rows": [
    { "id": 1, "name": "Alice", "email": "alice@example.com" },
    { "id": 2, "name": "Bob", "email": "bob@example.com" }
  ]
}
```

## Step 5: Connect Redis

Add Redis as a caching endpoint:

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "cache",
    "kind": "Redis",
    "config": {
      "write_conn": {
        "url": "redis://redis-host:6379"
      }
    }
  }'
```

Write a key:

```bash
curl http://{host}:8000/api/v1/endpoints/cache/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "command": "SET",
    "args": ["user:1", "{\"name\": \"Alice\", \"email\": \"alice@example.com\"}"]
  }'
```

Read the key:

```bash
curl http://{host}:8000/api/v1/endpoints/cache/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "command": "GET",
    "args": ["user:1"]
  }'
```

## Step 6: View API Documentation

Eden-MDBS includes auto-generated API documentation:

- **Swagger UI**: `http://{host}:8000/swagger-ui/`
- **OpenAPI JSON**: `http://{host}:8000/api-docs/openapi.json`

## What's Next?

Congratulations! You've:

- Created an organization
- Logged in and obtained a JWT token
- Connected PostgreSQL and Redis endpoints
- Executed read and write queries

### Continue Learning

- [First Steps](./first-steps.md) - Learn about users, roles, and permissions
- [Basic Concepts](./concepts.md) - Understand organizations, endpoints, and workflows
- [API Reference](../api/overview.md) - Explore all available APIs
- [Examples](../examples/basic.md) - See more advanced examples

### Add More Databases

Connect additional database types:

- [MongoDB](../guide/endpoints/mongo.md)
- [MySQL](../guide/endpoints/mysql.md)
- [Cassandra](../guide/endpoints/cassandra.md)
- [ClickHouse](../guide/endpoints/clickhouse.md)
- [Pinecone](../guide/endpoints/pinecone.md)

### Explore Advanced Features

- [Workflows](../guide/workflows.md) - Automate multi-step operations
- [Transactions](../guide/transactions.md) - Cross-database ACID transactions
- [RBAC](../guide/rbac.md) - Fine-grained access control

## Troubleshooting

### Authentication failed?

1. Make sure you're using the correct org creation token for creating organizations
2. For authenticated requests, ensure your JWT token is valid
3. JWT tokens expire - if you get authentication errors, login again to get a fresh token

### Need help?

- [Troubleshooting Guide](../operations/troubleshooting.md)
- [GitHub Issues](https://github.com/eden-dev-inc/eden-mdbs/issues)
- [Discussions](https://github.com/eden-dev-inc/eden-mdbs/discussions)
