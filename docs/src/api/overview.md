# API Overview

Eden-MDBS provides a REST API for interacting with your database infrastructure. All API endpoints are available at `http://{host}:8000/api/v1/`.

## Base URL

```
http://{host}:8000/api/v1
```

## Authentication

Most API endpoints require authentication via JWT tokens. See [Authentication](./authentication.md) for details.

### Authentication Methods

| Method             | Use Case               | Header Format                                      |
| ------------------ | ---------------------- | -------------------------------------------------- |
| Bearer Token       | Most API calls         | `Authorization: Bearer <jwt_token>`                |
| Basic Auth         | Login only             | `Authorization: Basic <base64(username:password)>` |
| Org Creation Token | Creating organizations | `Authorization: Bearer <EDEN_NEW_ORG_TOKEN>`       |

## API Categories

### Organization Management

| Endpoint                | Method | Description               |
| ----------------------- | ------ | ------------------------- |
| `/api/v1/new`           | POST   | Create a new organization |
| `/api/v1/organizations` | GET    | Get organization details  |
| `/api/v1/organizations` | PATCH  | Update organization       |
| `/api/v1/organizations` | DELETE | Delete organization       |

### Authentication

| Endpoint               | Method | Description             |
| ---------------------- | ------ | ----------------------- |
| `/api/v1/auth/login`   | POST   | Login and get JWT token |
| `/api/v1/auth/refresh` | POST   | Refresh JWT token       |

### Endpoints (Database Connections)

| Endpoint                             | Method | Description          |
| ------------------------------------ | ------ | -------------------- |
| `/api/v1/endpoints`                  | GET    | List all endpoints   |
| `/api/v1/endpoints`                  | POST   | Create new endpoint  |
| `/api/v1/endpoints/{id}`             | GET    | Get endpoint details |
| `/api/v1/endpoints/{id}`             | PATCH  | Update endpoint      |
| `/api/v1/endpoints/{id}`             | DELETE | Delete endpoint      |
| `/api/v1/endpoints/{id}/read`        | POST   | Execute read query   |
| `/api/v1/endpoints/{id}/write`       | POST   | Execute write query  |
| `/api/v1/endpoints/{id}/transaction` | POST   | Execute transaction  |

### MCP Tooling

| Endpoint                                      | Method | Description                                    |
| --------------------------------------------- | ------ | ---------------------------------------------- |
| `/api/v1/endpoints/{id}/mcp`                  | GET    | List MCP servers for an endpoint               |
| `/api/v1/endpoints/{id}/mcp/{mcp_server}`     | POST   | Send MCP JSON-RPC message (streamable HTTP)    |
| `/api/v1/endpoints/{id}/mcp/{mcp_server}`     | GET    | Open SSE stream for server-to-client messages  |
| `/api/v1/endpoints/{id}/mcp/{mcp_server}`     | DELETE | Close MCP session                              |
| `/api/v1/mcp/migrations`                      | POST   | Send MCP JSON-RPC message (streamable HTTP)    |
| `/api/v1/mcp/migrations`                      | GET    | Open SSE stream for server-to-client messages  |
| `/api/v1/mcp/migrations`                      | DELETE | Close MCP session                              |

### Users & IAM

| Endpoint                   | Method | Description      |
| -------------------------- | ------ | ---------------- |
| `/api/v1/iam/users`        | POST   | Create user      |
| `/api/v1/iam/users/{user}` | GET    | Get user details |
| `/api/v1/iam/users/{user}` | PATCH  | Update user      |
| `/api/v1/iam/users/{user}` | DELETE | Delete user      |

### RBAC (Role-Based Access Control)

| Endpoint                                  | Method | Description                  |
| ----------------------------------------- | ------ | ---------------------------- |
| `/api/v1/iam/rbac/endpoints/{id}`         | GET    | Get endpoint permissions     |
| `/api/v1/iam/rbac/endpoints/subjects`     | POST   | Add endpoint permissions     |
| `/api/v1/iam/rbac/organizations`          | GET    | Get organization permissions |
| `/api/v1/iam/rbac/organizations/subjects` | POST   | Add organization permissions |

### Templates

| Endpoint                 | Method | Description          |
| ------------------------ | ------ | -------------------- |
| `/api/v1/templates`      | GET    | List all templates   |
| `/api/v1/templates`      | POST   | Create template      |
| `/api/v1/templates/{id}` | GET    | Get template details |
| `/api/v1/templates/{id}` | POST   | Execute template     |
| `/api/v1/templates/{id}` | DELETE | Delete template      |

### Workflows

| Endpoint                 | Method | Description          |
| ------------------------ | ------ | -------------------- |
| `/api/v1/workflows`      | POST   | Create workflow      |
| `/api/v1/workflows/{id}` | GET    | Get workflow details |
| `/api/v1/workflows/{id}` | DELETE | Delete workflow      |

## Request Format

All requests should use JSON format:

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"key": "value"}'
```

## Response Format

Successful responses return JSON with a `status` field:

```json
{
  "status": "success",
  "data": { ... }
}
```

Error responses include error details:

```json
{
  "error": "Error Type",
  "message": "Detailed error message"
}
```

See [Error Responses](./errors.md) for complete error code reference.

## HTTP Status Codes

| Code | Meaning                                          |
| ---- | ------------------------------------------------ |
| 200  | Success                                          |
| 400  | Bad Request - Invalid input                      |
| 401  | Unauthorized - Invalid or missing authentication |
| 403  | Forbidden - Insufficient permissions             |
| 404  | Not Found - Resource doesn't exist               |
| 409  | Conflict - Resource already exists               |
| 429  | Too Many Requests - Rate limit exceeded          |
| 500  | Internal Server Error                            |

## Rate Limiting

Eden-MDBS supports configurable rate limiting via the `EDEN_RATE_LIMIT` environment variable. When rate limiting is enabled, responses include:

- `X-RateLimit-Limit`: Maximum requests per window
- `X-RateLimit-Remaining`: Remaining requests in current window
- `X-RateLimit-Reset`: Time when the rate limit resets

## API Documentation

Eden-MDBS provides auto-generated API documentation:

- **Swagger UI**: `http://{host}:8000/swagger-ui/`
- **OpenAPI JSON**: `http://{host}:8000/api-docs/openapi.json`

## Quick Examples

### Create Organization

```bash
curl http://{host}:8000/api/v1/new \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer {EDEN_NEW_ORG_TOKEN}" \
  -d '{
    "id": "my_company",
    "super_admins": [
      {"username": "admin", "password": "secure_password"}
    ]
  }'
```

### Login

```bash
curl http://{host}:8000/api/v1/auth/login \
  -u admin:secure_password \
  -X POST
```

### Create Endpoint

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "mydb",
    "kind": "Postgres",
    "config": {
      "write_conn": {
        "url": "postgresql://user:pass@host:5432/db"
      }
    }
  }'
```

### Execute Query

```bash
curl http://{host}:8000/api/v1/endpoints/mydb/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"query": "SELECT * FROM users LIMIT 10"}'
```

## Next Steps

- [Authentication APIs](./authentication.md) - Login, tokens, and refresh
- [Organization APIs](./organizations.md) - Managing organizations
- [Endpoint Management](./endpoints.md) - Database connections
- [Query Execution](./queries.md) - Running queries
- [Error Responses](./errors.md) - Error codes and handling
