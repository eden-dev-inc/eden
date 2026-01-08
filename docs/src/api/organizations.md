# Organization APIs

This reference covers the APIs for managing organizations in Eden-MDBS.

## Overview

Organizations are the top-level container for all resources in Eden-MDBS. Each organization has isolated endpoints, users, and access controls.

## Create Organization

Create a new organization. This requires the organization creation token.

The server must have the `EDEN_NEW_ORG_TOKEN` environment variable configured. Use that token value in the Authorization header.

### Request

```http
POST /api/v1/new
Content-Type: application/json
Authorization: Bearer <org_creation_token>
```

### Body Parameters

| Field          | Type   | Required | Description                    |
| -------------- | ------ | -------- | ------------------------------ |
| `id`           | string | Yes      | Unique organization identifier |
| `description`  | string | No       | Organization description       |
| `super_admins` | array  | Yes      | List of super admin users      |

### Super Admin Object

| Field        | Type   | Required | Description           |
| ------------ | ------ | -------- | --------------------- |
| `username`   | string | Yes      | Admin username        |
| `password`   | string | Yes      | Admin password        |
| `description`| string | No       | Admin description     |

### Example

```bash
# The token must match the EDEN_NEW_ORG_TOKEN environment variable on the server
curl http://{host}:8000/api/v1/new \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your_org_creation_token" \
  -d '{
    "id": "TestOrg",
    "description": "test organization",
    "super_admins": [
      {
        "username": "admin",
        "password": "password",
        "description": null
      }
    ]
  }'
```

### Response

```json
{
  "id": "my_company",
  "uuid": "550e8400-e29b-41d4-a716-446655440000"
}
```

## Get Organization

Retrieve organization details.

### Request

```http
GET /api/v1/organizations
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/organizations \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "status": "success",
  "data": {
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "name": "my_company",
    "created_at": "2024-01-15T10:30:00Z"
  }
}
```

## Update Organization

Update organization settings. Only super admins can update organization data.

### Request

```http
PATCH /api/v1/organizations
Content-Type: application/json
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/organizations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "id": "my_company_renamed"
  }'
```

### Response

```json
{
  "status": "success",
  "data": {
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "name": "my_company_renamed",
    "updated_at": "2024-01-16T10:30:00Z"
  }
}
```

## Delete Organization

Delete an organization and all its resources.

### Request

```http
DELETE /api/v1/organizations
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/organizations \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

### Response

```json
{
  "status": "success",
  "data": {
    "message": "Organization deleted successfully"
  }
}
```

**Warning**: This operation is irreversible and will delete all endpoints, users, templates, and workflows associated with the organization.

## Access Control

| Operation           | Required Access |
| ------------------- | --------------- |
| Create organization | None (public)   |
| Get organization    | Read            |
| Update organization | SuperAdmin      |
| Delete organization | SuperAdmin      |
| Get statistics      | Admin           |

## Error Responses

### Organization Already Exists

```json
{
  "error": "Conflict",
  "message": "Organization with name 'my_company' already exists"
}
```

### Invalid Name

```json
{
  "error": "Bad Request",
  "message": "Organization name must be alphanumeric with underscores only"
}
```

### Insufficient Permissions

```json
{
  "error": "Access denied",
  "message": "SuperAdmin access required for this operation"
}
```

## Organization Naming Rules

- Must be unique across the system
- Alphanumeric characters and underscores only
- 3-64 characters in length
- Cannot start with a number

**Valid names:**
- `my_company`
- `acme_corp_2024`
- `test_org`

**Invalid names:**
- `my-company` (hyphens not allowed)
- `2024_company` (starts with number)
- `ab` (too short)

## Related

- [Authentication](./authentication.md) - Login and tokens
- [Users](../guide/users.md) - User management
- [RBAC](../guide/rbac.md) - Access control
