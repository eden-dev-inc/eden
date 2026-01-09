# Organizations

Organizations are the top-level entities in Eden-MDBS that provide isolated multi-tenant environments. They serve as the fundamental boundary for access control, resource management, and user isolation.

## What Are Organizations?

Organizations are multi-tenant containers that:

- **Provide complete isolation** between different customer environments
- **Serve as the root entity** for all RBAC (Role-Based Access Control) permissions
- **Manage collections** of users, endpoints, templates, workflows, and APIs
- **Enable resource quotas** and usage tracking

## Creating an Organization

Organizations require a creation token (set via `EDEN_NEW_ORG_TOKEN` environment variable on the server).

```bash
curl http://{host}:8000/api/v1/new \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer {EDEN_NEW_ORG_TOKEN}" \
  -d '{
    "id": "my_company",
    "super_admins": [
      {
        "username": "admin",
        "password": "secure_password_123"
      }
    ]
  }'
```

**Response:**

```json
{
  "id": "my_company",
  "uuid": "550e8400-e29b-41d4-a716-446655440000"
}
```

### What Happens During Creation

1. Organization schema is generated with a unique UUID
2. Organization is assigned to the current Eden node
3. Configuration is stored in PostgreSQL
4. Organization is cached in Redis for fast access
5. Initial RBAC structures are created
6. Super admin user is created

## Retrieving Organization Information

### Basic Information

```bash
curl http://{host}:8000/api/v1/organizations \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "my_company",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "description": "My Company's Eden Organization",
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z",
    "eden_nodes": 2,
    "super_admins": 1,
    "users": 15,
    "endpoints": 8,
    "templates": 25,
    "workflows": 5
  }
}
```

### Verbose Information

Include the `X-Eden-Verbose: true` header to get complete organization details including UUIDs:

```bash
curl http://{host}:8000/api/v1/organizations \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Eden-Verbose: true"
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "my_company",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "description": "My Company's Eden Organization",
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z",
    "eden_node_uuids": [
      "550e8400-e29b-41d4-a716-446655440001",
      "550e8400-e29b-41d4-a716-446655440002"
    ],
    "super_admin_uuids": ["550e8400-e29b-41d4-a716-446655440003"],
    "user_uuids": [
      "550e8400-e29b-41d4-a716-446655440004",
      "550e8400-e29b-41d4-a716-446655440005"
    ],
    "endpoint_uuids": ["550e8400-e29b-41d4-a716-446655440006"],
    "template_uuids": [],
    "workflow_uuids": []
  }
}
```

## Updating Organizations

Only SuperAdmin users can update organization information:

```bash
curl http://{host}:8000/api/v1/organizations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "description": "Updated description for our organization"
  }'
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "my_company",
    "uuid": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

### Updatable Fields

| Field         | Description                      |
| ------------- | -------------------------------- |
| `id`          | Organization identifier (string) |
| `description` | Organization description         |

## Deleting Organizations

> **Warning**: This permanently deletes the organization and ALL associated resources.

```bash
curl http://{host}:8000/api/v1/organizations \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "my_company",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "removed_objects": {
      "objects": {
        "deleted_from_cache": ["..."],
        "deleted_from_postgres": ["..."]
      },
      "rbac": {
        "removed_subjects": {
          "users": ["user_uuid_1", "user_uuid_2"],
          "roles": ["admin_role_uuid"]
        }
      }
    }
  }
}
```

### What Gets Deleted

- Organization configuration and metadata
- All users belonging to the organization
- All endpoints and their configurations
- All templates and their definitions
- All workflows and their logic
- All RBAC permissions and access controls
- All cached data related to the organization

## Access Levels

Eden uses a hierarchical access level system:

| Level          | Description                                            |
| -------------- | ------------------------------------------------------ |
| **Read**       | View and query resources                               |
| **Write**      | Read permissions plus modify data                      |
| **Admin**      | Write permissions plus manage users and configurations |
| **SuperAdmin** | Full control including other admin management          |

## Best Practices

### Organization Design

- **Naming Convention**: Use consistent, descriptive organization IDs
- **Environment Separation**: Create separate organizations for dev/staging/production
- **Resource Planning**: Plan resource limits and quotas before creation

### User Management

- **Principle of Least Privilege**: Grant minimum necessary access levels
- **Regular Reviews**: Audit user access periodically
- **Offboarding Process**: Remove users promptly when they leave
- **SuperAdmin Limits**: Minimize the number of SuperAdmin users

### Security

- **Access Monitoring**: Monitor SuperAdmin actions and changes
- **Audit Logging**: Maintain logs of all organization operations
- **Change Management**: Implement approval processes for organization changes

## Error Handling

### Organization Already Exists

```json
{
  "error": "Conflict",
  "message": "Organization with ID 'my_company' already exists"
}
```

### Organization Not Found

```json
{
  "error": "Not Found",
  "message": "Organization not found"
}
```

### Insufficient Permissions

```json
{
  "error": "Forbidden",
  "message": "SuperAdmin access required for organization operations"
}
```

## Related

- [Authentication](./authentication.md) - Login and tokens
- [Users](./users.md) - Managing users within organizations
- [RBAC](./rbac.md) - Role-based access control
- [Endpoints](./endpoints.md) - Database connections
