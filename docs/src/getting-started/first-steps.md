# First Steps

After you have access to an Eden-MDBS instance and have created an organization, this guide walks you through the essential first steps to get productive.

## Step 1: Log In and Get Your Token

First, authenticate to get a JWT token for API access:

```bash
curl http://{host}:8000/api/v1/auth/login \
  -u your_username:your_password \
  -X POST
```

**Response:**

```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

Save this token for subsequent API calls:

```bash
export TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

## Step 2: Create Your First User

If you're a SuperAdmin or Admin, you can create additional users:

```bash
curl http://{host}:8000/api/v1/iam/users \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "username": "developer@company.com",
    "password": "SecurePassword123!",
    "description": "Development team member",
    "access_level": "Write"
  }'
```

### Access Level Options

| Level          | Description                                    |
| -------------- | ---------------------------------------------- |
| **Read**       | View and query resources                       |
| **Write**      | Read permissions plus modify data              |
| **Admin**      | Write permissions plus manage configurations   |
| **SuperAdmin** | Full control including other admin management  |

## Step 3: Connect Your First Endpoint

Endpoints are connections to your databases and services. Here's how to connect a PostgreSQL database:

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "my_postgres",
    "kind": "Postgres",
    "config": {
      "write_conn": {
        "url": "postgresql://user:password@db-host:5432/database"
      }
    }
  }'
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "my_postgres",
    "uuid": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

## Step 4: Run Your First Query

### Read Query

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "SELECT * FROM users LIMIT 10"
  }'
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "rows": [
      {"id": 1, "name": "John Doe", "email": "john@example.com"},
      {"id": 2, "name": "Jane Smith", "email": "jane@example.com"}
    ],
    "row_count": 2
  }
}
```

### Write Query

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
    "params": ["Alice Johnson", "alice@example.com"]
  }'
```

## Step 5: Grant Access to Your Team

Allow other users to access the endpoint:

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

## Step 6: Verify Your Setup

### Check Your Organization

```bash
curl http://{host}:8000/api/v1/organizations \
  -H "Authorization: Bearer $TOKEN"
```

### List Your Endpoints

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Authorization: Bearer $TOKEN"
```

### Check Your Permissions

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/my_postgres/subjects \
  -H "Authorization: Bearer $TOKEN"
```

## Common Endpoint Types

Eden supports multiple database and service types:

### Databases

| Kind           | Example Use Case              |
| -------------- | ----------------------------- |
| **Postgres**   | Primary relational data       |
| **MySQL**      | Legacy application data       |
| **Mongo**      | Document storage              |
| **Redis**      | Caching and sessions          |
| **Cassandra**  | High-throughput time series   |
| **ClickHouse** | Analytics and aggregations    |
| **Mssql**      | Enterprise SQL Server         |
| **Oracle**     | Enterprise Oracle databases   |

### Services

| Kind         | Example Use Case               |
| ------------ | ------------------------------ |
| **Http**     | External REST APIs             |
| **Llm**      | AI/ML model integrations       |
| **Pinecone** | Vector similarity search       |

## Next Steps

Now that you have the basics set up:

1. **[Concepts](./concepts.md)** - Understand Eden's core concepts
2. **[Endpoints](../guide/endpoints.md)** - Learn about different endpoint types
3. **[RBAC](../guide/rbac.md)** - Set up access control for your team
4. **[Templates](../advanced/templates.md)** - Create reusable query templates

## Quick Reference

| Task                  | Endpoint                              | Method |
| --------------------- | ------------------------------------- | ------ |
| Login                 | `/api/v1/auth/login`                  | POST   |
| Create user           | `/api/v1/iam/users`                   | POST   |
| Create endpoint       | `/api/v1/endpoints`                   | POST   |
| Read query            | `/api/v1/endpoints/{id}/read`         | POST   |
| Write query           | `/api/v1/endpoints/{id}/write`        | POST   |
| Grant access          | `/api/v1/iam/rbac/endpoints/subjects` | POST   |
| Get organization info | `/api/v1/organizations`               | GET    |
