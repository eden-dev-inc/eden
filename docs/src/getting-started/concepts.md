# Core Concepts

Understanding Eden-MDBS's core concepts will help you get the most out of the platform. This guide explains the fundamental building blocks and how they work together.

## Organizations

Organizations are the top-level containers in Eden-MDBS that provide **multi-tenant isolation**.

- Each organization is completely isolated from others
- All resources (users, endpoints, templates, workflows) belong to an organization
- Organizations are created with a special creation token
- Each organization has its own set of SuperAdmin users

```
Organization
├── Users (with access levels)
├── Endpoints (database connections)
├── Templates (reusable operations)
└── Workflows (multi-step automations)
```

## Users and Access Levels

Eden uses a **hierarchical access level system** with four tiers:

| Level          | Capabilities                                                |
| -------------- | ----------------------------------------------------------- |
| **Read**       | View resources and execute read-only queries                |
| **Write**      | All Read permissions + execute write queries                |
| **Admin**      | All Write permissions + manage users, endpoints, templates  |
| **SuperAdmin** | All Admin permissions + manage other admins, organization   |

Each level includes all permissions from lower levels. A Write user automatically has Read permissions.

### Permission Rules

- Users can only manage users with **lower** access levels
- Only SuperAdmins can create/modify Admin and SuperAdmin users
- Users can always modify their own profile information

## Endpoints

Endpoints are **managed connections** to external databases and services. They provide:

- **Unified interface** for different database types
- **Connection pooling** for performance
- **RBAC integration** for access control
- **Health monitoring** for reliability

### Supported Endpoint Types

**Relational Databases:**
- PostgreSQL, MySQL, Microsoft SQL Server, Oracle

**NoSQL Databases:**
- MongoDB, Redis, Cassandra, ClickHouse

**External Services:**
- HTTP APIs, LLM integrations, Pinecone (vector search)

### Endpoint Operations

| Operation       | Description                          | Access Required |
| --------------- | ------------------------------------ | --------------- |
| **Read**        | Query data without modification      | Read            |
| **Write**       | Insert, update, or delete data       | Write           |
| **Transaction** | Multiple operations in one atomic unit | Write         |

## Templates

Templates are **reusable, parameterized operations** that define database queries or API calls.

### Why Use Templates?

- **Reusability**: Define once, use many times with different parameters
- **Security**: Parameters are properly escaped to prevent injection
- **Consistency**: Ensure queries follow best practices
- **Access Control**: Templates have their own RBAC permissions

### Template Structure

```json
{
  "id": "get_user_orders",
  "kind": "Read",
  "template": {
    "query": "SELECT * FROM orders WHERE user_id = {{user_id}}",
    "params": ["{{user_id}}"]
  }
}
```

Templates use **Handlebars syntax** for parameter substitution:
- `{{parameter}}` - Simple value substitution
- `{{#if condition}}...{{/if}}` - Conditional logic
- `{{#each array}}...{{/each}}` - Loop over arrays

## Workflows

Workflows are **multi-step operations** that orchestrate multiple templates or actions.

Use workflows when you need to:
- Execute operations across multiple endpoints
- Implement conditional logic between steps
- Create complex data pipelines
- Automate multi-database transactions

## Role-Based Access Control (RBAC)

RBAC controls **who can access what resources** at a granular level.

### Resource-Level Permissions

You can grant different access levels per resource:

```
User: developer@company.com
├── Organization: Read
├── Endpoint "analytics_db": Read
├── Endpoint "app_db": Write
└── Template "user_report": Read
```

### Permission Hierarchy

1. **Resource-specific permission** takes precedence
2. **Organization-level permission** applies if no resource-specific permission
3. **No access** if neither exists

### Example

A user with organization-level **Write** access:
- Has Write access to all endpoints by default
- Can be granted **Admin** on specific endpoints (override higher)
- Can be restricted to **Read** on specific endpoints (override lower)

## Authentication

Eden uses **JWT (JSON Web Token)** authentication:

1. **Login** with username/password to get a token
2. **Include token** in Authorization header for API requests
3. **Refresh token** before expiration to maintain session

```
Authorization: Bearer <jwt_token>
```

### Token Contents

JWT tokens contain:
- User ID and UUID
- Organization ID and UUID
- Expiration timestamp

## API Structure

All API endpoints follow a consistent pattern:

```
http://{host}:8000/api/v1/{resource}
```

### Response Format

**Success:**
```json
{
  "status": "success",
  "data": { ... }
}
```

**Error:**
```json
{
  "error": "Error Type",
  "message": "Detailed error message"
}
```

## Putting It Together

Here's how the concepts work together in a typical workflow:

1. **Organization** is created with a SuperAdmin
2. **SuperAdmin** creates additional users with appropriate access levels
3. **Admin** creates endpoints to connect databases
4. **Admin** grants RBAC permissions to users for specific endpoints
5. **Users** authenticate and receive JWT tokens
6. **Users** execute queries against endpoints they have access to
7. **Templates** encapsulate common queries for reuse
8. **Workflows** orchestrate complex multi-step operations

## Next Steps

- **[Endpoints](../guide/endpoints.md)** - Connect your databases
- **[RBAC](../guide/rbac.md)** - Configure access control
- **[Templates](../advanced/templates.md)** - Create reusable operations
- **[API Reference](../api/overview.md)** - Full API documentation
