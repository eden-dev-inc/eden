# Frequently Asked Questions

## General

### What is Eden-MDBS?

Eden-MDBS (Multiple Database Management System) is a unified API platform for managing and interacting with multiple database types and external services. It provides a single interface for querying databases, managing access control, and automating operations across your data infrastructure.

### What databases does Eden support?

Eden supports:

**Relational Databases:**
- PostgreSQL
- MySQL
- Microsoft SQL Server (MSSQL)
- Oracle

**NoSQL Databases:**
- MongoDB
- Redis
- Cassandra
- ClickHouse

**External Services:**
- HTTP APIs
- LLM integrations
- Pinecone (vector search)

### What is an organization in Eden?

An organization is the top-level container that provides multi-tenant isolation. Each organization has its own set of users, endpoints, templates, and workflows. Organizations are completely isolated from each other.

## Authentication

### How do I authenticate with Eden?

Eden uses JWT (JSON Web Token) authentication:

1. Login with your username and password using Basic Auth
2. Receive a JWT token
3. Include the token in the `Authorization: Bearer <token>` header for all API requests

### How long do tokens last?

Token expiration is configured by your Eden administrator. You can check the `exp` claim in your JWT token to see when it expires. Use the `/api/v1/auth/refresh` endpoint to get a new token before expiration.

### Can I use my UUID instead of username to login?

Yes. Eden automatically detects whether you're logging in with a string username or a UUID. Both work with the same login endpoint.

## Endpoints

### What's the difference between an endpoint and a database?

An **endpoint** is Eden's abstraction for a database connection. It wraps the connection configuration, handles pooling, and integrates with RBAC. A database is the actual data store that the endpoint connects to.

### Can I have multiple endpoints to the same database?

Yes. You might want separate endpoints with different connection pool sizes, timeouts, or access control configurations.

### Why use `write_conn` instead of just `url` in endpoint config?

Eden separates read and write connections to support read replicas. For simple setups, you only need `write_conn`. For read-heavy workloads, you can configure separate read replicas.

## Access Control

### What are the access levels?

Eden has four hierarchical access levels:

| Level        | Permissions                                      |
| ------------ | ------------------------------------------------ |
| Read         | View resources and execute read-only operations  |
| Write        | Read + execute write operations                  |
| Admin        | Write + manage users, endpoints, templates       |
| SuperAdmin   | Admin + manage other admins, organization config |

### Can a user have different access levels for different endpoints?

Yes. RBAC permissions can be set per-resource. A user might have Read access to one endpoint and Write access to another.

### How do organization-level and resource-level permissions interact?

Resource-specific permissions override organization-level permissions. If a user has organization-level Write access but Read access on a specific endpoint, they'll only have Read access to that endpoint.

## Queries

### How do I prevent SQL injection?

Always use parameterized queries:

```bash
# Safe - use parameters
curl http://{host}:8000/api/v1/endpoints/mydb/read \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"query": "SELECT * FROM users WHERE id = $1", "params": [123]}'
```

Never concatenate user input into queries.

### What's the difference between `/read` and `/write` endpoints?

- **`/read`** - For SELECT queries and other read-only operations
- **`/write`** - For INSERT, UPDATE, DELETE, and other data-modifying operations

Using the correct endpoint helps with connection routing (if you have read replicas) and RBAC enforcement.

### Can I run transactions?

Yes. Use the `/transaction` endpoint to execute multiple operations atomically:

```bash
curl http://{host}:8000/api/v1/endpoints/mydb/transaction \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {"query": "UPDATE accounts SET balance = balance - 100 WHERE id = 1"},
      {"query": "UPDATE accounts SET balance = balance + 100 WHERE id = 2"}
    ]
  }'
```

All operations succeed or all are rolled back.

## Templates

### Why use templates instead of direct queries?

Templates provide:
- **Reusability** - Define once, use many times
- **Security** - Parameters are properly escaped
- **Access control** - Templates have their own RBAC permissions
- **Documentation** - Templates are self-documenting with descriptions

### Can templates span multiple endpoints?

A single template targets one endpoint. For multi-endpoint operations, use workflows to orchestrate multiple templates.

### What templating syntax does Eden use?

Eden uses Handlebars syntax for parameter substitution:

```handlebars
SELECT * FROM users WHERE status = '{{status}}'
{{#if limit}}LIMIT {{limit}}{{/if}}
```

## Troubleshooting

### I'm getting "Unauthorized" errors

Check that:
1. Your token hasn't expired
2. You're including the token in the Authorization header: `Bearer <token>`
3. You have the required access level for the operation

### I'm getting "Forbidden" errors

This means your token is valid but you don't have permission for the requested operation. Contact your administrator to request appropriate access.

### My query is timing out

Try:
1. Optimizing your query (add indexes, reduce result set)
2. Adding a LIMIT clause
3. Checking the database server load

### Connection pool exhausted

This means all connections in the pool are in use. Options:
1. Wait and retry
2. Contact your administrator to increase pool size
3. Optimize long-running queries

## API

### What format do API responses use?

All responses are JSON. Successful responses include:

```json
{
  "status": "success",
  "data": { ... }
}
```

Error responses include:

```json
{
  "error": "Error Type",
  "message": "Detailed error message"
}
```

### Is there rate limiting?

Rate limiting is configurable by your Eden administrator. When enabled, you'll receive `429 Too Many Requests` responses if you exceed the limit. Check the `X-RateLimit-*` headers for current limits.

### Where can I find API documentation?

- **Swagger UI**: `http://{host}:8000/swagger-ui/`
- **OpenAPI JSON**: `http://{host}:8000/api-docs/openapi.json`

## Getting Help

### Where can I get support?

Contact your Eden administrator for support with your specific deployment.

### How do I report issues?

Report issues through your organization's support channels. Include:
- The API endpoint and request body
- The error response received
- Your user access level
- Timestamp of the issue
