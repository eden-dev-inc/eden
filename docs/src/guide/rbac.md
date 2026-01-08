# Role-Based Access Control (RBAC)

Eden-MDBS provides fine-grained access control for resources like endpoints, templates, workflows, and organizations. RBAC allows you to control who can access what resources and what operations they can perform.

## Access Levels

Eden uses a four-tier hierarchical access level system:

| Level          | Description                                            |
| -------------- | ------------------------------------------------------ |
| **Read**       | View and query resources                               |
| **Write**      | Read permissions plus modify data                      |
| **Admin**      | Write permissions plus manage users and configurations |
| **SuperAdmin** | Full control including other admin management          |

Each level includes all permissions from lower levels. For example, Write includes Read permissions.

## Resource Types

RBAC applies to these resource types:

| Resource          | Description                                          |
| ----------------- | ---------------------------------------------------- |
| **Organizations** | Top-level access control for the entire organization |
| **Endpoints**     | Database and service connections                     |
| **Templates**     | Reusable query templates                             |
| **Workflows**     | Automated multi-step operations                      |

## Managing Endpoint Permissions

### Get Endpoint Permissions

View all subjects and their access levels for an endpoint:

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/my_database \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "users": {
      "user1@company.com": "Read",
      "user2@company.com": "Write",
      "admin@company.com": "Admin"
    }
  }
}
```

### Add Subjects to Endpoint

Grant access to multiple users:

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/subjects \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "subjects": [
      ["john@company.com", "Read"],
      ["jane@company.com", "Write"],
      ["admin@company.com", "Admin"]
    ]
  }'
```

**Response:**

```json
{
  "status": "success",
  "message": "added rbac rule for endpoint"
}
```

### Get Specific User's Permission

Check a user's access level for an endpoint:

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/my_database/subjects/john@company.com \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**

```json
{
  "status": "success",
  "data": "Read"
}
```

### Get Your Own Permission

Users can check their own permissions without Admin access:

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/my_database/subjects \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**

```json
{
  "status": "success",
  "data": "Write"
}
```

### Remove User from Endpoint

Revoke a user's access:

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/my_database/subjects/john@company.com \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

**Response:**

```json
{
  "status": "success",
  "data": "Read"
}
```

The response shows the access level that was removed.

### Remove All Endpoint Permissions

Remove all permissions for an endpoint (SuperAdmin only):

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/my_database \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

## Managing Organization Permissions

### Get Organization Permissions

```bash
curl http://{host}:8000/api/v1/iam/rbac/organizations \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "users": {
      "admin@company.com": "SuperAdmin",
      "manager@company.com": "Admin",
      "developer@company.com": "Write",
      "viewer@company.com": "Read"
    }
  }
}
```

### Add Subjects to Organization

```bash
curl http://{host}:8000/api/v1/iam/rbac/organizations/subjects \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "subjects": [
      ["newadmin@company.com", "Admin"],
      ["newdev@company.com", "Write"],
      ["contractor@company.com", "Read"]
    ]
  }'
```

### Get User's All Permissions

View all resources a user has access to:

```bash
curl http://{host}:8000/api/v1/iam/rbac/organizations/subjects/john@company.com \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "organizations": {
      "550e8400-e29b-41d4-a716-446655440000": "Admin"
    },
    "endpoints": {
      "550e8400-e29b-41d4-a716-446655440001": "Write",
      "550e8400-e29b-41d4-a716-446655440002": "Read"
    },
    "templates": {
      "550e8400-e29b-41d4-a716-446655440003": "Admin"
    },
    "workflows": {}
  }
}
```

### Remove User from Organization

```bash
curl http://{host}:8000/api/v1/iam/rbac/organizations/subjects/john@company.com \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

## Permission Requirements

To manage RBAC permissions, you need sufficient access:

| Operation              | Required Access                                 |
| ---------------------- | ----------------------------------------------- |
| View RBAC Info         | Admin                                           |
| Add Subjects           | Equal to or higher than the level being granted |
| Remove Subjects        | Equal to or higher than the level being removed |
| Delete All Permissions | SuperAdmin                                      |

### Example Permission Logic

- An **Admin** can grant **Read** or **Write** access
- An **Admin** cannot grant **Admin** or **SuperAdmin** access
- A **SuperAdmin** can grant any access level
- A **Write** user cannot manage RBAC at all

## Subject Input Format

When adding subjects, use this format:

```json
{
  "subjects": [
    ["username1", "AccessLevel1"],
    ["username2", "AccessLevel2"]
  ]
}
```

### Examples

**Single User:**

```json
{
  "subjects": [["john@company.com", "Read"]]
}
```

**Multiple Users with Different Access:**

```json
{
  "subjects": [
    ["developer@company.com", "Read"],
    ["teamlead@company.com", "Write"],
    ["manager@company.com", "Admin"]
  ]
}
```

**Mixed User Types (email and UUID):**

```json
{
  "subjects": [
    ["john@company.com", "Write"],
    ["550e8400-e29b-41d4-a716-446655440000", "Admin"]
  ]
}
```

## Permission Hierarchy

### Organization vs Resource Permissions

Organization-level permissions provide base access, while resource-specific permissions can override them:

1. **Resource-Specific Permission**: If user has explicit permission on resource, use that
2. **Organization Permission**: If no resource-specific permission, use organization level
3. **No Access**: If no permissions found, deny access

### Example

A user with organization-level **Write** access:

- Has Write access to all endpoints by default
- Can have **Admin** on specific endpoints (higher than org level)
- Can have **Read** on specific endpoints (lower than org level)

## Error Handling

### Insufficient Permissions

```json
{
  "error": "Forbidden",
  "message": "Insufficient access level to grant Admin permissions"
}
```

### Subject Not Found

```json
{
  "error": "Not Found",
  "message": "User john@company.com not found in organization"
}
```

### Endpoint Not Found

```json
{
  "error": "Not Found",
  "message": "Endpoint my_database not found"
}
```

### Invalid Access Level

```json
{
  "error": "Bad Request",
  "message": "Invalid access level: InvalidLevel"
}
```

## Best Practices

### Permission Design

- **Principle of Least Privilege**: Grant minimum necessary access
- **Regular Reviews**: Audit permissions periodically
- **Role-Based Assignment**: Group users by function rather than individual grants

### Access Level Management

- **Start Small**: Begin with Read access and escalate as needed
- **Admin Hierarchy**: Clearly define who can manage whom
- **SuperAdmin Restrictions**: Limit SuperAdmin to essential personnel

### Operational Procedures

- **Onboarding**: Standardize permission assignment for new users
- **Offboarding**: Immediately revoke all access when users leave
- **Role Changes**: Update permissions when job responsibilities change

### Monitoring

- **Access Logs**: Monitor who accesses what resources
- **Permission Changes**: Log all RBAC modifications
- **Unusual Activity**: Alert on unexpected permission usage

## Related

- [Organizations](./organizations.md) - Organization management
- [Endpoints](./endpoints.md) - Database connections
- [Authentication](./authentication.md) - Login and tokens
- [Users](./users.md) - User management
