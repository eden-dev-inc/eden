# Role-Based Access Control (RBAC) APIs

This reference covers the APIs for managing role-based access control in Eden-MDBS.

## Overview

Eden-MDBS uses a hierarchical permission system with five access levels:

| Level       | Description                                          |
| ----------- | ---------------------------------------------------- |
| `SuperAdmin`| Full control, can modify state data and manage admins |
| `Admin`     | Can add/remove Writers and Readers                    |
| `Write`     | Write access to the resource                          |
| `Read`      | Read-only access to the resource                      |
| `None`      | No access                                             |

## Endpoint Permissions

### Get Self Endpoint Permissions

Get the endpoints accessible by the authenticated user.

```http
GET /api/v1/iam/rbac/endpoints/subjects
Authorization: Bearer <token>
```

#### Example

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/subjects \
  -H "Authorization: Bearer $TOKEN"
```

#### Response

```json
{
  "status": "success",
  "data": "Admin"
}
```

### Add Endpoint Permission

Grant a subject access to an endpoint.

```http
POST /api/v1/iam/rbac/endpoints/subjects
Content-Type: application/json
Authorization: Bearer <token>
```

#### Body Parameters

| Field       | Type   | Required | Description                    |
| ----------- | ------ | -------- | ------------------------------ |
| `subject`   | string | Yes      | User or role identifier        |
| `entity`    | string | Yes      | Endpoint identifier            |
| `access`    | string | Yes      | Access level (Read/Write/Admin)|

#### Example

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/subjects \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "subject": "developer@company.com",
    "entity": "production_db",
    "access": "Read"
  }'
```

### Get Subject Endpoint Permissions

Get endpoint permissions for a specific subject.

```http
GET /api/v1/iam/rbac/endpoints/subjects/{subject}
Authorization: Bearer <token>
```

#### Example

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/subjects/developer@company.com \
  -H "Authorization: Bearer $TOKEN"
```

#### Response

```json
{
  "status": "success",
  "data": "Write"
}
```

### Delete Subject Endpoint Permissions

Remove a subject's endpoint permissions.

```http
DELETE /api/v1/iam/rbac/endpoints/subjects/{subject}
Authorization: Bearer <token>
```

### Get Endpoint Subjects

Get all subjects with access to a specific endpoint.

```http
GET /api/v1/iam/rbac/endpoints/{endpoint}
Authorization: Bearer <token>
```

#### Example

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/production_db \
  -H "Authorization: Bearer $TOKEN"
```

#### Response

```json
{
  "status": "success",
  "data": {
    "users": {
      "admin@company.com": "SuperAdmin",
      "developer@company.com": "Write"
    },
    "roles": {
      "developers": "Read"
    }
  }
}
```

### Delete Endpoint Permissions

Remove all permissions for an endpoint.

```http
DELETE /api/v1/iam/rbac/endpoints/{endpoint}
Authorization: Bearer <token>
```

## Organization Permissions

### Get Organization Permissions

Get the organization-level permission structure.

```http
GET /api/v1/iam/rbac/organizations
Authorization: Bearer <token>
```

### Add Organization Permission

Grant a subject access at the organization level.

```http
POST /api/v1/iam/rbac/organizations/subjects
Content-Type: application/json
Authorization: Bearer <token>
```

#### Body Parameters

| Field       | Type   | Required | Description              |
| ----------- | ------ | -------- | ------------------------ |
| `subject`   | string | Yes      | User or role identifier  |
| `access`    | string | Yes      | Access level             |

#### Example

```bash
curl http://{host}:8000/api/v1/iam/rbac/organizations/subjects \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "subject": "ops_team",
    "access": "Admin"
  }'
```

### Get Subject Organization Permissions

```http
GET /api/v1/iam/rbac/organizations/subjects/{subject}
Authorization: Bearer <token>
```

### Delete Subject Organization Permissions

```http
DELETE /api/v1/iam/rbac/organizations/subjects/{subject}
Authorization: Bearer <token>
```

### Delete Organization Permissions

Remove all permissions at the organization level.

```http
DELETE /api/v1/iam/rbac/organizations
Authorization: Bearer <token>
```

## Template Permissions

### Add Template Permission

Grant a subject access to templates.

```http
POST /api/v1/iam/rbac/templates/subjects
Content-Type: application/json
Authorization: Bearer <token>
```

#### Example

```bash
curl http://{host}:8000/api/v1/iam/rbac/templates/subjects \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "subject": "api_user",
    "entity": "get_user_template",
    "access": "Read"
  }'
```

### Get Subject Template Permissions

```http
GET /api/v1/iam/rbac/templates/subjects/{subject}
Authorization: Bearer <token>
```

### Delete Subject Template Permissions

```http
DELETE /api/v1/iam/rbac/templates/subjects/{subject}
Authorization: Bearer <token>
```

### Get Template Subjects

Get all subjects with access to a specific template.

```http
GET /api/v1/iam/rbac/templates/{template}
Authorization: Bearer <token>
```

### Delete Template Permissions

```http
DELETE /api/v1/iam/rbac/templates/{template}
Authorization: Bearer <token>
```

## Workflow Permissions

### Add Workflow Permission

Grant a subject access to workflows.

```http
POST /api/v1/iam/rbac/workflows/subjects
Content-Type: application/json
Authorization: Bearer <token>
```

#### Example

```bash
curl http://{host}:8000/api/v1/iam/rbac/workflows/subjects \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "subject": "automation_user",
    "entity": "data_sync_workflow",
    "access": "Write"
  }'
```

### Get Subject Workflow Permissions

```http
POST /api/v1/iam/rbac/workflows/subjects/{subject}
Authorization: Bearer <token>
```

### Delete Subject Workflow Permissions

```http
DELETE /api/v1/iam/rbac/workflows/subjects/{subject}
Authorization: Bearer <token>
```

### Get Workflow Subjects

Get all subjects with access to a specific workflow.

```http
GET /api/v1/iam/rbac/workflows/{workflow}
Authorization: Bearer <token>
```

## Subject Queries

### Get All Permissions for Subject

Get all permissions (across all resource types) for a specific subject.

```http
GET /api/v1/iam/rbac/subjects/{subject}
Authorization: Bearer <token>
```

#### Example

```bash
curl http://{host}:8000/api/v1/iam/rbac/subjects/developer@company.com \
  -H "Authorization: Bearer $TOKEN"
```

#### Response

```json
{
  "status": "success",
  "data": {
    "endpoints": {
      "production_db": "Read",
      "staging_db": "Write"
    },
    "templates": {
      "get_user": "Read"
    },
    "workflows": {},
    "organizations": {
      "my_company": "Read"
    }
  }
}
```

### Delete All Subject Permissions

Remove all permissions for a subject across all resource types.

```http
DELETE /api/v1/iam/rbac/subjects/{subject}
Authorization: Bearer <token>
```

### Get Subject Endpoints

Get all endpoints a subject has access to.

```http
GET /api/v1/iam/rbac/subjects/{subject}/endpoints
Authorization: Bearer <token>
```

### Get Subject Organizations

Get all organizations a subject has access to.

```http
GET /api/v1/iam/rbac/subjects/{subject}/organizations
Authorization: Bearer <token>
```

### Get Subject Templates

Get all templates a subject has access to.

```http
GET /api/v1/iam/rbac/subjects/{subject}/templates
Authorization: Bearer <token>
```

### Get Subject Workflows

Get all workflows a subject has access to.

```http
GET /api/v1/iam/rbac/subjects/{subject}/workflows
Authorization: Bearer <token>
```

## Access Control Matrix

| Operation                    | Required Access |
| ---------------------------- | --------------- |
| View own permissions         | Any             |
| View other user permissions  | Admin           |
| Grant Read access            | Admin           |
| Grant Write access           | Admin           |
| Grant Admin access           | SuperAdmin      |
| Revoke permissions           | Admin           |

## Error Responses

### Insufficient Permissions

```json
{
  "error": "Forbidden",
  "message": "Insufficient permissions to perform this action"
}
```

### Subject Not Found

```json
{
  "error": "Not Found",
  "message": "Subject does not exist"
}
```

### Invalid Access Level

```json
{
  "error": "Bad Request",
  "message": "Invalid access level. Must be one of: SuperAdmin, Admin, Write, Read, None"
}
```

## Related

- [Users API](./users.md) - User management
- [Authentication](./authentication.md) - Login and tokens
- [RBAC Guide](../guide/rbac.md) - Concepts and best practices
