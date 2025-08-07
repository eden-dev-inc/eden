# RBAC (Role-Based Access Control) Implementation Guide

Eden's RBAC system provides fine-grained access control for resources like endpoints, templates, workflows, and organizations. This guide covers the implementation of RBAC permissions, subject management, and access level enforcement across your Eden environment.

## RBAC Overview

Eden's RBAC system provides:
- **Resource-Based Permissions**: Control access to specific endpoints, templates, workflows, and organizations
- **Subject Management**: Grant/revoke access for individual users or groups
- **Hierarchical Access Levels**: Four-tier permission system (Read, Write, Admin, SuperAdmin)
- **Self-Service Access**: Users can check their own permissions
- **Bulk Operations**: Add multiple subjects with different access levels simultaneously
- **Organization Scoping**: All RBAC rules are scoped to specific organizations

## RBAC Architecture

### Core Concepts

1. **Entities**: Resources that can have permissions (endpoints, templates, workflows, organizations)
2. **Subjects**: Users who can be granted permissions  
3. **Access Levels**: Permission levels that define what operations are allowed
4. **RBAC Data**: The relationship between entity, subject, and access level

### Access Level Hierarchy

- **Read**: View and query resources
- **Write**: Read permissions plus modify data
- **Admin**: Write permissions plus manage users and configurations
- **SuperAdmin**: Full control including other admin management

### Permission Requirements

| Operation | Required Access Level |
|-----------|----------------------|
| View RBAC Info | Admin |
| Add/Modify Subjects | Equal to or higher than the access level being granted |
| Remove Subjects | Equal to or higher than the access level being removed |
| Delete All Permissions | SuperAdmin |

## Endpoint RBAC Management

### Get All RBAC Information for an Endpoint

Retrieve all subjects and their access levels for a specific endpoint:

```http
GET /api/v1/iam/rbac/endpoints/my_database_endpoint
Authorization: Bearer your_admin_jwt_token
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
    },
    "groups": {
      "dev_team": "Write",
      "admin_team": "Admin"
    }
  }
}
```

### Add Subjects to an Endpoint

Grant access to multiple users with different permission levels:

```http
POST /api/v1/iam/rbac/endpoints/subjects
Authorization: Bearer your_admin_jwt_token

{
  "subjects": [
    ["john.doe@company.com", "Read"],
    ["jane.smith@company.com", "Write"],
    ["admin.user@company.com", "Admin"]
  ]
}
```

**Response:**
```json
{
  "status": "success", 
  "message": "added rbac rule for endpoint"
}
```

### Get Specific Subject's Access Level

Check a specific user's permissions for an endpoint:

```http
GET /api/v1/iam/rbac/endpoints/my_database_endpoint/subjects/john.doe@company.com
Authorization: Bearer your_admin_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "data": "Read"
}
```

### Get Your Own Access Level

Users can check their own permissions without Admin access:

```http
GET /api/v1/iam/rbac/endpoints/my_database_endpoint/subjects
Authorization: Bearer your_user_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "data": "Write"
}
```

### Remove Subject from Endpoint

Revoke a user's access to an endpoint:

```http
DELETE /api/v1/iam/rbac/endpoints/my_database_endpoint/subjects/john.doe@company.com
Authorization: Bearer your_admin_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "data": "Read"
}
```

The response shows the access level that was removed.

### Remove All RBAC Rules for an Endpoint

Remove all permissions for an endpoint (SuperAdmin only):

```http
DELETE /api/v1/iam/rbac/endpoints/my_database_endpoint
Authorization: Bearer your_superadmin_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "removed_subjects": {
      "users": ["user1_uuid", "user2_uuid", "admin_uuid"],
      "groups": ["group1_uuid"]
    }
  }
}
```

## Subject Input Schema

### SubjectInput Structure

```json
{
  "subjects": [
    ["username1", "AccessLevel1"],
    ["username2", "AccessLevel2"],
    ["username3", "AccessLevel3"]
  ]
}
```

### Example Subject Inputs

#### Single User
```json
{
  "subjects": [
    ["john.doe@company.com", "Read"]
  ]
}
```

#### Multiple Users with Different Access Levels
```json
{
  "subjects": [
    ["developer1@company.com", "Read"],
    ["developer2@company.com", "Write"],
    ["teamlead@company.com", "Admin"],
    ["sysadmin@company.com", "SuperAdmin"]
  ]
}
```

#### Mixed User Types
```json
{
  "subjects": [
    ["john.doe@company.com", "Write"],
    ["550e8400-e29b-41d4-a716-446655440000", "Admin"],
    ["service_account", "Read"]
  ]
}
```

## RBAC Client Implementation

### TypeScript RBAC Client

```typescript
interface SubjectPermission {
  username: string;
  accessLevel: 'Read' | 'Write' | 'Admin' | 'SuperAdmin';
}

interface RbacResponse {
  users?: Record<string, string>;
  groups?: Record<string, string>;
}

class EdenRBACClient {
  private baseUrl: string;
  private token: string;

  constructor(baseUrl: string, token: string) {
    this.baseUrl = baseUrl;
    this.token = token;
  }

  async getEndpointPermissions(endpointId: string): Promise<RbacResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/iam/rbac/endpoints/${encodeURIComponent(endpointId)}`, {
      headers: {
        'Authorization': `Bearer ${this.token}`
      }
    });

    if (!response.ok) {
      throw new Error(`Failed to get endpoint permissions: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }

  async addSubjectsToEndpoint(endpointId: string, subjects: SubjectPermission[]): Promise<void> {
    const subjectsArray = subjects.map(s => [s.username, s.accessLevel]);
    
    const response = await fetch(`${this.baseUrl}/api/v1/iam/rbac/endpoints/subjects`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.token}`
      },
      body: JSON.stringify({
        subjects: subjectsArray
      })
    });

    if (!response.ok) {
      throw new Error(`Failed to add subjects: ${response.statusText}`);
    }
  }

  async getSubjectPermission(endpointId: string, username: string): Promise<string> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/iam/rbac/endpoints/${encodeURIComponent(endpointId)}/subjects/${encodeURIComponent(username)}`,
      {
        headers: {
          'Authorization': `Bearer ${this.token}`
        }
      }
    );

    if (!response.ok) {
      throw new Error(`Failed to get subject permission: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }

  async getMyPermission(endpointId: string): Promise<string> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/iam/rbac/endpoints/${encodeURIComponent(endpointId)}/subjects`,
      {
        headers: {
          'Authorization': `Bearer ${this.token}`
        }
      }
    );

    if (!response.ok) {
      throw new Error(`Failed to get own permission: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }

  async removeSubjectFromEndpoint(endpointId: string, username: string): Promise<string> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/iam/rbac/endpoints/${encodeURIComponent(endpointId)}/subjects/${encodeURIComponent(username)}`,
      {
        method: 'DELETE',
        headers: {
          'Authorization': `Bearer ${this.token}`
        }
      }
    );

    if (!response.ok) {
      throw new Error(`Failed to remove subject: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data; // Returns the access level that was removed
  }

  async removeAllEndpointPermissions(endpointId: string): Promise<RbacResponse> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/iam/rbac/endpoints/${encodeURIComponent(endpointId)}`,
      {
        method: 'DELETE',
        headers: {
          'Authorization': `Bearer ${this.token}`
        }
      }
    );

    if (!response.ok) {
      throw new Error(`Failed to remove all permissions: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }
}

// Usage examples
const rbac = new EdenRBACClient('https://api.eden.com', 'your-jwt-token');

// Get all permissions for an endpoint
const permissions = await rbac.getEndpointPermissions('my_database');
console.log('Endpoint permissions:', permissions);

// Add multiple subjects with different access levels
await rbac.addSubjectsToEndpoint('my_database', [
  { username: 'dev@company.com', accessLevel: 'Read' },
  { username: 'admin@company.com', accessLevel: 'Admin' }
]);

// Check a specific user's permission
const userPermission = await rbac.getSubjectPermission('my_database', 'dev@company.com');
console.log('User permission:', userPermission);

// Check your own permission
const myPermission = await rbac.getMyPermission('my_database');
console.log('My permission:', myPermission);

// Remove a user's access
const removedLevel = await rbac.removeSubjectFromEndpoint('my_database', 'dev@company.com');
console.log('Removed access level:', removedLevel);
```

### Python RBAC Client

```python
import requests
from typing import List, Dict, Tuple, Optional
from urllib.parse import quote

class EdenRBACClient:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

    def get_endpoint_permissions(self, endpoint_id: str) -> Dict:
        """Get all RBAC permissions for an endpoint"""
        encoded_id = quote(endpoint_id, safe='')
        response = requests.get(
            f'{self.base_url}/api/v1/iam/rbac/endpoints/{encoded_id}',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to get permissions: {response.text}')

    def add_subjects_to_endpoint(self, endpoint_id: str, 
                                subjects: List[Tuple[str, str]]) -> None:
        """Add multiple subjects with permissions to an endpoint"""
        response = requests.post(
            f'{self.base_url}/api/v1/iam/rbac/endpoints/subjects',
            headers=self.headers,
            json={'subjects': subjects}
        )
        
        if response.status_code != 200:
            raise Exception(f'Failed to add subjects: {response.text}')

    def get_subject_permission(self, endpoint_id: str, username: str) -> str:
        """Get a specific subject's permission for an endpoint"""
        encoded_endpoint = quote(endpoint_id, safe='')
        encoded_username = quote(username, safe='')
        
        response = requests.get(
            f'{self.base_url}/api/v1/iam/rbac/endpoints/{encoded_endpoint}/subjects/{encoded_username}',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to get subject permission: {response.text}')

    def get_my_permission(self, endpoint_id: str) -> str:
        """Get your own permission for an endpoint"""
        encoded_id = quote(endpoint_id, safe='')
        response = requests.get(
            f'{self.base_url}/api/v1/iam/rbac/endpoints/{encoded_id}/subjects',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to get own permission: {response.text}')

    def remove_subject_from_endpoint(self, endpoint_id: str, username: str) -> str:
        """Remove a subject's access to an endpoint"""
        encoded_endpoint = quote(endpoint_id, safe='')
        encoded_username = quote(username, safe='')
        
        response = requests.delete(
            f'{self.base_url}/api/v1/iam/rbac/endpoints/{encoded_endpoint}/subjects/{encoded_username}',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to remove subject: {response.text}')

    def remove_all_endpoint_permissions(self, endpoint_id: str) -> Dict:
        """Remove all RBAC permissions for an endpoint (SuperAdmin only)"""
        encoded_id = quote(endpoint_id, safe='')
        response = requests.delete(
            f'{self.base_url}/api/v1/iam/rbac/endpoints/{encoded_id}',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to remove all permissions: {response.text}')

# Usage examples
rbac = EdenRBACClient('https://api.eden.com', 'your-jwt-token')

# Get all permissions for an endpoint
permissions = rbac.get_endpoint_permissions('my_database')
print(f"Endpoint permissions: {permissions}")

# Add multiple subjects
rbac.add_subjects_to_endpoint('my_database', [
    ('dev@company.com', 'Read'),
    ('admin@company.com', 'Admin'),
    ('service_account', 'Write')
])

# Check specific user permission
user_permission = rbac.get_subject_permission('my_database', 'dev@company.com')
print(f"User permission: {user_permission}")

# Check your own permission
my_permission = rbac.get_my_permission('my_database')
print(f"My permission: {my_permission}")

# Remove user access
removed_level = rbac.remove_subject_from_endpoint('my_database', 'dev@company.com')
print(f"Removed access level: {removed_level}")
```

## Permission Validation Logic

### Access Level Requirements

When managing RBAC permissions, the system enforces these rules:

```typescript
// Permission validation examples
interface PermissionCheck {
  requesterLevel: string;
  targetLevel: string;
  operation: string;
  allowed: boolean;
}

const permissionExamples: PermissionCheck[] = [
  // Admin trying to grant Read access - ✅ Allowed
  { requesterLevel: 'Admin', targetLevel: 'Read', operation: 'grant', allowed: true },
  
  // Admin trying to grant Admin access - ❌ Not allowed (need SuperAdmin)
  { requesterLevel: 'Admin', targetLevel: 'Admin', operation: 'grant', allowed: false },
  
  // SuperAdmin granting any level - ✅ Allowed
  { requesterLevel: 'SuperAdmin', targetLevel: 'Admin', operation: 'grant', allowed: true },
  
  // Write user trying to grant access - ❌ Not allowed (need Admin+)
  { requesterLevel: 'Write', targetLevel: 'Read', operation: 'grant', allowed: false },
  
  // Admin removing Read access - ✅ Allowed
  { requesterLevel: 'Admin', targetLevel: 'Read', operation: 'remove', allowed: true },
  
  // Admin removing Admin access - ❌ Not allowed (need SuperAdmin)
  { requesterLevel: 'Admin', targetLevel: 'Admin', operation: 'remove', allowed: false }
];
```

### Subject Maximum Access Level

The `SubjectInput` automatically calculates the maximum access level being granted:

```json
{
  "subjects": [
    ["user1@company.com", "Read"],
    ["user2@company.com", "Write"], 
    ["user3@company.com", "Admin"]
  ]
}
```

For this input, `max_relation()` returns `Admin`, so the requester must have `Admin` or `SuperAdmin` access to execute this operation.

## Error Handling

### Common RBAC Error Scenarios

1. **Insufficient Permissions**
```json
{
  "error": "Forbidden",
  "message": "Insufficient access level to grant Admin permissions"
}
```

2. **Subject Not Found**
```json
{
  "error": "Not Found",
  "message": "User john.doe@company.com not found in organization"
}
```

3. **Endpoint Not Found**
```json
{
  "error": "Not Found", 
  "message": "Endpoint my_database not found"
}
```

4. **No Permission to Remove**
```json
{
  "error": "Forbidden",
  "message": "Cannot remove access level equal to or higher than your own"
}
```

5. **Invalid Access Level**
```json
{
  "error": "Bad Request",
  "message": "Invalid access level: InvalidLevel"
}
```

### Error Handling Best Practices

```typescript
async function safeRbacOperation<T>(operation: () => Promise<T>): Promise<T | null> {
  try {
    return await operation();
  } catch (error) {
    if (error.message.includes('Insufficient access')) {
      console.error('Permission denied - you need higher access level');
    } else if (error.message.includes('not found')) {
      console.error('Resource not found - check endpoint/user names');
    } else {
      console.error('Unexpected error:', error.message);
    }
    return null;
  }
}

// Usage
const result = await safeRbacOperation(() => 
  rbac.addSubjectsToEndpoint('my_endpoint', [
    { username: 'user@company.com', accessLevel: 'Admin' }
  ])
);
```

## Best Practices for RBAC Implementation

### 1. Permission Design
- **Principle of Least Privilege**: Grant minimum necessary access
- **Regular Reviews**: Audit permissions quarterly
- **Role-Based Assignment**: Group users by function rather than individual grants
- **Temporary Access**: Use time-limited permissions when possible

### 2. Access Level Management
- **Start Small**: Begin with Read access and escalate as needed
- **Admin Hierarchy**: Clearly define who can manage whom
- **SuperAdmin Restrictions**: Limit SuperAdmin access to essential personnel
- **Cross-Training**: Ensure multiple people can manage critical resources

### 3. Operational Procedures
- **Onboarding**: Standardize permission assignment for new users
- **Offboarding**: Immediately revoke all access when users leave
- **Role Changes**: Update permissions when job responsibilities change
- **Emergency Access**: Have procedures for urgent access needs

### 4. Monitoring and Auditing
- **Access Logs**: Monitor who accesses what resources
- **Permission Changes**: Log all RBAC modifications
- **Unusual Activity**: Alert on unexpected permission usage
- **Compliance**: Maintain audit trails for regulatory requirements

### 5. Integration Patterns
- **Group Management**: Consider implementing group-based permissions
- **External Identity**: Integrate with existing identity providers
- **API Keys**: Manage service account permissions separately
- **Temporary Tokens**: Use short-lived tokens for automated systems

## Organization RBAC Management

Organization-level RBAC controls user access and roles within the entire organization. This provides the highest level of access control and determines base permissions for all resources.

### Get All Organization RBAC Information

Retrieve all users and their organization-level access:

```http
GET /api/v1/iam/rbac/organizations
Authorization: Bearer your_admin_jwt_token
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
    },
    "groups": {
      "admin_group": "Admin",
      "dev_team": "Write"
    }
  }
}
```

### Add Subjects to Organization

Grant organization-level access to multiple users:

```http
POST /api/v1/iam/rbac/organizations/subjects
Authorization: Bearer your_admin_jwt_token

{
  "subjects": [
    ["new.admin@company.com", "Admin"],
    ["new.developer@company.com", "Write"],
    ["contractor@company.com", "Read"]
  ]
}
```

**Response:**
```json
{
  "status": "success",
  "message": "success"
}
```

**Permission Requirements:**
- Must have access level equal to or higher than the maximum level being granted
- SuperAdmin required to grant Admin or SuperAdmin access
- Admin required to grant Read or Write access

### Get Subject's Organization Access

Check a specific user's organization-level permissions:

```http
GET /api/v1/iam/rbac/organizations/subjects/john.doe@company.com
Authorization: Bearer your_admin_jwt_token
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
    "workflows": {
      "550e8400-e29b-41d4-a716-446655440004": "Write"
    }
  }
}
```

This response shows all resources the user has access to across the organization.

### Remove Subject from Organization

Revoke a user's organization-level access:

```http
DELETE /api/v1/iam/rbac/organizations/subjects/john.doe@company.com
Authorization: Bearer your_admin_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "data": "Admin"
}
```

The response shows the access level that was removed.

### Remove All Organization RBAC Rules

Remove all organization-level permissions (SuperAdmin only):

```http
DELETE /api/v1/iam/rbac/organizations
Authorization: Bearer your_superadmin_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "removed_subjects": {
      "users": ["user1_uuid", "user2_uuid", "admin_uuid"],
      "groups": ["group1_uuid"]
    }
  }
}
```

**⚠️ Warning**: This operation removes ALL organization-level permissions and should be used with extreme caution.

## Organization RBAC Client Implementation

### Extended TypeScript Client

```typescript
class EdenRBACClient {
  // ... previous endpoint methods ...

  async getOrganizationPermissions(): Promise<RbacResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/iam/rbac/organizations`, {
      headers: {
        'Authorization': `Bearer ${this.token}`
      }
    });

    if (!response.ok) {
      throw new Error(`Failed to get organization permissions: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }

  async addSubjectsToOrganization(subjects: SubjectPermission[]): Promise<void> {
    const subjectsArray = subjects.map(s => [s.username, s.accessLevel]);
    
    const response = await fetch(`${this.baseUrl}/api/v1/iam/rbac/organizations/subjects`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.token}`
      },
      body: JSON.stringify({
        subjects: subjectsArray
      })
    });

    if (!response.ok) {
      throw new Error(`Failed to add subjects to organization: ${response.statusText}`);
    }
  }

  async getSubjectAllPermissions(username: string): Promise<any> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/iam/rbac/organizations/subjects/${encodeURIComponent(username)}`,
      {
        headers: {
          'Authorization': `Bearer ${this.token}`
        }
      }
    );

    if (!response.ok) {
      throw new Error(`Failed to get subject permissions: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }

  async removeSubjectFromOrganization(username: string): Promise<string> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/iam/rbac/organizations/subjects/${encodeURIComponent(username)}`,
      {
        method: 'DELETE',
        headers: {
          'Authorization': `Bearer ${this.token}`
        }
      }
    );

    if (!response.ok) {
      throw new Error(`Failed to remove subject from organization: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }

  async removeAllOrganizationPermissions(): Promise<RbacResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/iam/rbac/organizations`, {
      method: 'DELETE',
      headers: {
        'Authorization': `Bearer ${this.token}`
      }
    });

    if (!response.ok) {
      throw new Error(`Failed to remove all organization permissions: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }
}

// Usage examples
const rbac = new EdenRBACClient('https://api.eden.com', 'your-jwt-token');

// Get all organization permissions
const orgPermissions = await rbac.getOrganizationPermissions();
console.log('Organization permissions:', orgPermissions);

// Add users to organization
await rbac.addSubjectsToOrganization([
  { username: 'newadmin@company.com', accessLevel: 'Admin' },
  { username: 'newdev@company.com', accessLevel: 'Write' }
]);

// Get all permissions for a specific user
const userAllPermissions = await rbac.getSubjectAllPermissions('john.doe@company.com');
console.log('User has access to:', userAllPermissions);

// Remove user from organization
const removedLevel = await rbac.removeSubjectFromOrganization('olduser@company.com');
console.log('Removed organization access level:', removedLevel);
```

### Extended Python Client

```python
class EdenRBACClient:
    # ... previous endpoint methods ...

    def get_organization_permissions(self) -> Dict:
        """Get all organization-level RBAC permissions"""
        response = requests.get(
            f'{self.base_url}/api/v1/iam/rbac/organizations',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to get organization permissions: {response.text}')

    def add_subjects_to_organization(self, subjects: List[Tuple[str, str]]) -> None:
        """Add multiple subjects with organization-level permissions"""
        response = requests.post(
            f'{self.base_url}/api/v1/iam/rbac/organizations/subjects',
            headers=self.headers,
            json={'subjects': subjects}
        )
        
        if response.status_code != 200:
            raise Exception(f'Failed to add subjects to organization: {response.text}')

    def get_subject_all_permissions(self, username: str) -> Dict:
        """Get all permissions (across all resources) for a subject"""
        encoded_username = quote(username, safe='')
        response = requests.get(
            f'{self.base_url}/api/v1/iam/rbac/organizations/subjects/{encoded_username}',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to get subject permissions: {response.text}')

    def remove_subject_from_organization(self, username: str) -> str:
        """Remove a subject's organization-level access"""
        encoded_username = quote(username, safe='')
        response = requests.delete(
            f'{self.base_url}/api/v1/iam/rbac/organizations/subjects/{encoded_username}',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to remove subject from organization: {response.text}')

    def remove_all_organization_permissions(self) -> Dict:
        """Remove all organization-level RBAC permissions (SuperAdmin only)"""
        response = requests.delete(
            f'{self.base_url}/api/v1/iam/rbac/organizations',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to remove all organization permissions: {response.text}')

# Usage examples
rbac = EdenRBACClient('https://api.eden.com', 'your-jwt-token')

# Get organization permissions
org_permissions = rbac.get_organization_permissions()
print(f"Organization permissions: {org_permissions}")

# Add subjects to organization
rbac.add_subjects_to_organization([
    ('newadmin@company.com', 'Admin'),
    ('newdev@company.com', 'Write'),
    ('viewer@company.com', 'Read')
])

# Get comprehensive permissions for a user
user_permissions = rbac.get_subject_all_permissions('john.doe@company.com')
print(f"User permissions across all resources: {user_permissions}")

# Remove user from organization
removed_level = rbac.remove_subject_from_organization('olduser@company.com')
print(f"Removed organization access level: {removed_level}")
```

## RBAC Permission Hierarchy

### Organization vs Resource Permissions

Organization-level permissions provide the foundation for resource access:

```typescript
interface PermissionHierarchy {
  organizationLevel: string;    // Base permission level
  resourceSpecific: {           // Can override organization level
    endpoints: Record<string, string>;
    templates: Record<string, string>;
    workflows: Record<string, string>;
  };
}

// Example: User with organization Write access but Admin on specific endpoint
const userPermissions: PermissionHierarchy = {
  organizationLevel: 'Write',
  resourceSpecific: {
    endpoints: {
      'critical_database': 'Admin',      // Higher than org level
      'read_only_endpoint': 'Read'       // Lower than org level
    },
    templates: {
      'admin_template': 'Admin'
    },
    workflows: {}
  }
};
```

### Permission Resolution Logic

Eden resolves permissions using this hierarchy:

1. **Resource-Specific Permission**: If user has explicit permission on resource, use that
2. **Organization Permission**: If no resource-specific permission, use organization level
3. **No Access**: If no permissions found, deny access

```typescript
function resolvePermission(
  userId: string, 
  resourceId: string, 
  resourceType: 'endpoint' | 'template' | 'workflow'
): AccessLevel | null {
  // 1. Check resource-specific permission
  const resourcePermission = getResourcePermission(userId, resourceId, resourceType);
  if (resourcePermission) {
    return resourcePermission;
  }
  
  // 2. Check organization-level permission
  const orgPermission = getOrganizationPermission(userId);
  if (orgPermission) {
    return orgPermission;
  }
  
  // 3. No access
  return null;
}
```

## Advanced RBAC Patterns

### Comprehensive Access Audit

Get complete permission overview for security audits:

```typescript
async function auditUserAccess(rbac: EdenRBACClient, username: string) {
  try {
    // Get user's comprehensive permissions
    const allPermissions = await rbac.getSubjectAllPermissions(username);
    
    console.log(`Access Audit for ${username}:`);
    console.log(`Organization Level: ${allPermissions.organizations || 'None'}`);
    
    if (allPermissions.endpoints) {
      console.log('\nEndpoint Access:');
      Object.entries(allPermissions.endpoints).forEach(([id, level]) => {
        console.log(`  ${id}: ${level}`);
      });
    }
    
    if (allPermissions.templates) {
      console.log('\nTemplate Access:');
      Object.entries(allPermissions.templates).forEach(([id, level]) => {
        console.log(`  ${id}: ${level}`);
      });
    }
    
    if (allPermissions.workflows) {
      console.log('\nWorkflow Access:');
      Object.entries(allPermissions.workflows).forEach(([id, level]) => {
        console.log(`  ${id}: ${level}`);
      });
    }
    
  } catch (error) {
    console.error(`Failed to audit access for ${username}:`, error.message);
  }
}

// Audit all organization users
async function auditOrganizationAccess(rbac: EdenRBACClient) {
  const orgPermissions = await rbac.getOrganizationPermissions();
  
  for (const username of Object.keys(orgPermissions.users || {})) {
    await auditUserAccess(rbac, username);
    console.log('\n' + '='.repeat(50) + '\n');
  }
}
```

### Bulk Permission Management

Efficiently manage permissions for multiple users:

```typescript
async function onboardNewTeam(rbac: EdenRBACClient, teamMembers: any[]) {
  // 1. Add all team members to organization
  const orgSubjects = teamMembers.map(member => ({
    username: member.email,
    accessLevel: member.role === 'lead' ? 'Admin' : 'Write'
  }));
  
  await rbac.addSubjectsToOrganization(orgSubjects);
  
  // 2. Grant specific endpoint access
  for (const endpoint of member.endpoints) {
    const endpointSubjects = teamMembers.map(member => ({
      username: member.email,
      accessLevel: member.endpointAccess[endpoint] || 'Read'
    }));
    
    await rbac.addSubjectsToEndpoint(endpoint, endpointSubjects);
  }
  
  console.log(`Successfully onboarded ${teamMembers.length} team members`);
}

async function offboardUser(rbac: EdenRBACClient, username: string) {
  try {
    // Get all user permissions first
    const allPermissions = await rbac.getSubjectAllPermissions(username);
    
    // Remove from all endpoints
    if (allPermissions.endpoints) {
      for (const endpointId of Object.keys(allPermissions.endpoints)) {
        await rbac.removeSubjectFromEndpoint(endpointId, username);
      }
    }
    
    // Remove from organization (this should cascade to other resources)
    await rbac.removeSubjectFromOrganization(username);
    
    console.log(`Successfully offboarded ${username}`);
    
  } catch (error) {
    console.error(`Failed to offboard ${username}:`, error.message);
  }
}
```

## Organization RBAC Security Considerations

### Critical Operations

Organization-level RBAC operations are particularly sensitive:

- **SuperAdmin Requirements**: Creating/removing organization admins requires SuperAdmin
- **Cascade Effects**: Organization permissions affect access to all resources
- **Audit Trails**: All organization RBAC changes should be logged and monitored
- **Emergency Access**: Maintain emergency SuperAdmin access procedures

### Best Practices for Organization RBAC

1. **Minimal SuperAdmin Access**: Limit SuperAdmin to essential personnel only
2. **Regular Access Reviews**: Audit organization permissions monthly
3. **Separation of Duties**: Don't grant both organization and resource admin to same user when possible
4. **Emergency Procedures**: Have documented procedures for emergency access scenarios
5. **Change Logging**: Log all organization permission changes with full context

This RBAC implementation guide provides the foundation for managing access control in your Eden environment. The system's hierarchical approach and fine-grained permissions ensure secure resource access while maintaining operational flexibility.