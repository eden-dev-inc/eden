# IAM (Identity and Access Management) Implementation Guide

Eden's IAM system provides comprehensive user management capabilities with hierarchical access controls and organization-scoped security. This guide covers user creation, management, and access level administration within your Eden environment.

## IAM Overview

Eden's IAM system provides:
- **User Management**: Create, read, update, and delete user accounts
- **Access Level Control**: Four-tier permission system (Read, Write, Admin, SuperAdmin)
- **Organization Scoping**: All users are scoped to specific organizations
- **Self-Service Capabilities**: Users can modify their own account information
- **Hierarchical Permissions**: Higher-level users can manage lower-level users
- **Secure Authentication**: Password management and credential verification

## Access Levels Hierarchy

Eden uses a four-tier access level system:

1. **Read** (Lowest): Can view resources they have access to
2. **Write**: Can read and modify data within their permissions
3. **Admin**: Can manage users, resources, and organization settings
4. **SuperAdmin** (Highest): Can manage other admins and all organization aspects

### Access Level Rules
- Users can only manage users with lower access levels
- Only SuperAdmins can create/modify Admin and SuperAdmin users
- Only SuperAdmins can change passwords for other users
- Users can always modify their own profile information

## User Management Operations

### Creating Users

#### Create a Standard User (Read/Write Access)

```http
POST /api/v1/iam/users
Content-Type: application/json
Authorization: Bearer your_admin_jwt_token

{
  "username": "john.doe@company.com",
  "password": "SecurePassword123!",
  "description": "Software Engineer - Frontend Team",
  "access_level": "Write"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "success"
}
```

#### Create an Admin User

```http
POST /api/v1/iam/users
Content-Type: application/json
Authorization: Bearer your_superadmin_jwt_token

{
  "username": "admin.user@company.com",
  "password": "AdminPassword456!",
  "description": "System Administrator",
  "access_level": "Admin"
}
```

**Requirements for User Creation:**
- **Admin Token**: Required to create Read/Write users
- **SuperAdmin Token**: Required to create Admin/SuperAdmin users
- **Unique Username**: Username must not exist in the organization
- **Strong Password**: Enforce password complexity requirements
- **Valid Access Level**: Must be a valid access level enum

#### User Creation Process

1. **Permission Verification**: System checks if requester has sufficient privileges
2. **Username Validation**: Verifies username doesn't already exist
3. **User Record Creation**: Creates user in database with encrypted password
4. **RBAC Assignment**: Assigns specified access level to user
5. **Cache Update**: Updates user cache for fast lookups

### Retrieving User Information

#### Get User Details

```http
GET /api/v1/iam/users/john.doe@company.com
Authorization: Bearer your_admin_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "id": "john.doe@company.com",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "description": "Software Engineer - Frontend Team",
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

**Note**: Password information is never returned in API responses for security.

### Updating User Information

#### Update User Profile (Self-Service)

```http
PATCH /api/v1/iam/users/john.doe@company.com
Content-Type: application/json
Authorization: Bearer johns_jwt_token

{
  "description": "Senior Software Engineer - Frontend Team Lead",
  "password": "NewSecurePassword789!"
}
```

#### Update User Access Level (Admin Only)

```http
PATCH /api/v1/iam/users/john.doe@company.com
Content-Type: application/json
Authorization: Bearer your_admin_jwt_token

{
  "access_level": "Admin"
}
```

#### Update User Username

```http
PATCH /api/v1/iam/users/john.doe@company.com
Content-Type: application/json
Authorization: Bearer your_admin_jwt_token

{
  "username": "john.smith@company.com"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "success"
}
```

#### Partial Update Fields

All fields in PATCH requests are optional:

```json
{
  "username": "new.username@company.com",      // Optional: Change username
  "password": "NewPassword123!",               // Optional: Change password  
  "description": "Updated role description",   // Optional: Change description
  "access_level": "Admin"                      // Optional: Change access level
}
```

#### Update Permission Rules

| Requester Level | Target User Level | Can Update | Restrictions |
|----------------|-------------------|------------|--------------|
| User (Self) | Self | ✅ All fields | Can change own username, password, description |
| Admin | Read/Write | ✅ All fields | Cannot change passwords of others |
| Admin | Admin | ❌ | Cannot modify users of same level |
| SuperAdmin | Any | ✅ All fields | Can modify any user including passwords |

### Deleting Users

#### Remove User from Organization

```http
DELETE /api/v1/iam/users/john.doe@company.com
Authorization: Bearer your_admin_jwt_token
```

**Response:**
```json
{
  "status": "success", 
  "message": "success"
}
```

**Deletion Process:**
1. **Permission Check**: Verifies Admin access level
2. **User Lookup**: Finds user in organization cache
3. **RBAC Cleanup**: Removes all RBAC permissions for user
4. **Database Cleanup**: Removes user record from database
5. **Cache Invalidation**: Clears user from cache systems

## User Input Schema

### UserInput Structure

```json
{
  "username": "string",           // Required: Unique identifier (email recommended)
  "password": "string",           // Required: User password (will be encrypted)
  "description": "string",        // Optional: User description/role
  "access_level": "AccessLevel"   // Optional: Defaults to "Read"
}
```

### OptionalUserInput Structure (PATCH)

```json
{
  "username": "string",           // Optional: New username
  "password": "string",           // Optional: New password
  "description": "string",        // Optional: New description
  "access_level": "AccessLevel"   // Optional: New access level
}
```

### Access Level Values

```json
{
  "access_level": "Read"          // Read-only access
}
{
  "access_level": "Write"         // Read and write access
}
{
  "access_level": "Admin"         // Administrative access
}
{
  "access_level": "SuperAdmin"    // Full administrative access
}
```

## Client Implementation Examples

### TypeScript IAM Client

```typescript
interface User {
  username: string;
  password: string;
  description?: string;
  access_level?: 'Read' | 'Write' | 'Admin' | 'SuperAdmin';
}

interface UserUpdate {
  username?: string;
  password?: string; 
  description?: string;
  access_level?: 'Read' | 'Write' | 'Admin' | 'SuperAdmin';
}

class EdenIAMClient {
  private baseUrl: string;
  private token: string;

  constructor(baseUrl: string, token: string) {
    this.baseUrl = baseUrl;
    this.token = token;
  }

  async createUser(user: User): Promise<void> {
    const response = await fetch(`${this.baseUrl}/api/v1/iam/users`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.token}`
      },
      body: JSON.stringify(user)
    });

    if (!response.ok) {
      const error = await response.text();
      throw new Error(`Failed to create user: ${error}`);
    }
  }

  async getUser(username: string): Promise<any> {
    const response = await fetch(`${this.baseUrl}/api/v1/iam/users/${encodeURIComponent(username)}`, {
      headers: {
        'Authorization': `Bearer ${this.token}`
      }
    });

    if (!response.ok) {
      throw new Error(`Failed to get user: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }

  async updateUser(username: string, updates: UserUpdate): Promise<void> {
    const response = await fetch(`${this.baseUrl}/api/v1/iam/users/${encodeURIComponent(username)}`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.token}`
      },
      body: JSON.stringify(updates)
    });

    if (!response.ok) {
      throw new Error(`Failed to update user: ${response.statusText}`);
    }
  }

  async deleteUser(username: string): Promise<void> {
    const response = await fetch(`${this.baseUrl}/api/v1/iam/users/${encodeURIComponent(username)}`, {
      method: 'DELETE',
      headers: {
        'Authorization': `Bearer ${this.token}`
      }
    });

    if (!response.ok) {
      throw new Error(`Failed to delete user: ${response.statusText}`);
    }
  }
}

// Usage examples
const iam = new EdenIAMClient('https://api.eden.com', 'your-jwt-token');

// Create a new user
await iam.createUser({
  username: 'jane.doe@company.com',
  password: 'SecurePassword123!',
  description: 'Backend Developer',
  access_level: 'Write'
});

// Get user information
const user = await iam.getUser('jane.doe@company.com');
console.log('User:', user);

// Update user description
await iam.updateUser('jane.doe@company.com', {
  description: 'Senior Backend Developer'
});

// Promote user to Admin
await iam.updateUser('jane.doe@company.com', {
  access_level: 'Admin'
});

// Delete user
await iam.deleteUser('jane.doe@company.com');
```

### Python IAM Client

```python
import requests
from typing import Optional, Dict, Any
from urllib.parse import quote

class EdenIAMClient:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

    def create_user(self, username: str, password: str, 
                   description: Optional[str] = None,
                   access_level: str = 'Read') -> None:
        """Create a new user"""
        user_data = {
            'username': username,
            'password': password,
            'access_level': access_level
        }
        if description:
            user_data['description'] = description

        response = requests.post(
            f'{self.base_url}/api/v1/iam/users',
            headers=self.headers,
            json=user_data
        )
        
        if response.status_code != 200:
            raise Exception(f'Failed to create user: {response.text}')

    def get_user(self, username: str) -> Dict[str, Any]:
        """Get user information"""
        encoded_username = quote(username, safe='')
        response = requests.get(
            f'{self.base_url}/api/v1/iam/users/{encoded_username}',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to get user: {response.text}')

    def update_user(self, username: str, **updates) -> None:
        """Update user information"""
        encoded_username = quote(username, safe='')
        response = requests.patch(
            f'{self.base_url}/api/v1/iam/users/{encoded_username}',
            headers=self.headers,
            json=updates
        )
        
        if response.status_code != 200:
            raise Exception(f'Failed to update user: {response.text}')

    def delete_user(self, username: str) -> None:
        """Delete user"""
        encoded_username = quote(username, safe='')
        response = requests.delete(
            f'{self.base_url}/api/v1/iam/users/{encoded_username}',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code != 200:
            raise Exception(f'Failed to delete user: {response.text}')

# Usage examples
iam = EdenIAMClient('https://api.eden.com', 'your-jwt-token')

# Create users with different access levels
iam.create_user(
    username='dev@company.com',
    password='DevPassword123!',
    description='Developer Team Member',
    access_level='Write'
)

iam.create_user(
    username='admin@company.com', 
    password='AdminPassword456!',
    description='System Administrator',
    access_level='Admin'
)

# Get user info
user = iam.get_user('dev@company.com')
print(f"User: {user}")

# Update user
iam.update_user('dev@company.com', 
                description='Senior Developer Team Lead',
                access_level='Admin')

# Change password
iam.update_user('dev@company.com', password='NewPassword789!')

# Delete user
iam.delete_user('dev@company.com')
```

## Error Handling

### Common Error Scenarios

1. **User Already Exists**
```json
{
  "error": "Bad Request",
  "message": "user john.doe@company.com exists"
}
```

2. **User Not Found**
```json
{
  "error": "Bad Request", 
  "message": "user john.doe@company.com doesn't exist"
}
```

3. **Insufficient Permissions**
```json
{
  "error": "Forbidden",
  "message": "Insufficient access level to perform this operation"
}
```

4. **Invalid Access Level**
```json
{
  "error": "Bad Request",
  "message": "Cannot modify user with equal or higher access level"
}
```

5. **Missing Required Fields**
```json
{
  "error": "Bad Request",
  "message": "Username and password are required"
}
```

### Error Handling Best Practices

```typescript
async function createUserWithErrorHandling(iam: EdenIAMClient, user: User) {
  try {
    await iam.createUser(user);
    console.log('User created successfully');
  } catch (error) {
    if (error.message.includes('exists')) {
      console.error('User already exists, consider updating instead');
    } else if (error.message.includes('Insufficient access')) {
      console.error('You do not have permission to create this user');
    } else {
      console.error('Unexpected error:', error.message);
    }
    throw error;
  }
}
```

## Security Considerations

### Password Security
- **Encryption**: All passwords are encrypted before storage
- **Complexity**: Enforce strong password requirements
- **Change Restrictions**: Only SuperAdmins can change other users' passwords
- **Self-Service**: Users can always change their own passwords

### Access Control
- **Principle of Least Privilege**: Grant minimum necessary access levels
- **Hierarchical Enforcement**: Users cannot elevate privileges above their own level
- **Organization Scoping**: All operations are scoped to the user's organization
- **Session Management**: Use JWT tokens with appropriate expiration times

### Audit and Monitoring
- **Operation Logging**: All IAM operations are logged with telemetry
- **Access Tracking**: Monitor user access patterns and privilege escalations
- **Failed Attempts**: Log failed authentication and authorization attempts
- **Regular Review**: Periodically review user access levels and remove unused accounts

## Best Practices for Implementation

### 1. User Lifecycle Management
- **Onboarding**: Create users with minimal privileges initially
- **Role Evolution**: Update access levels as user responsibilities change
- **Offboarding**: Promptly remove access when users leave the organization
- **Regular Audits**: Review user access levels quarterly

### 2. Username Conventions
- **Email Addresses**: Use email addresses as usernames for clarity
- **Consistent Format**: Establish and enforce naming conventions
- **Uniqueness**: Ensure usernames are unique within the organization
- **Case Sensitivity**: Be consistent with case handling

### 3. Access Level Assignment
- **Start Minimal**: Begin with Read access and elevate as needed
- **Role-Based**: Assign access levels based on job functions
- **Temporary Elevation**: Use temporary privilege elevation when possible
- **Documentation**: Document the rationale for access level assignments

### 4. Password Management
- **Complexity Requirements**: Enforce strong password policies
- **Regular Changes**: Encourage periodic password updates
- **Unique Passwords**: Ensure passwords are not reused across systems
- **Secure Recovery**: Implement secure password reset procedures

### 5. Integration Patterns
- **Single Sign-On**: Consider integrating with existing SSO solutions
- **Directory Services**: Integrate with LDAP/Active Directory when applicable
- **Multi-Factor Authentication**: Implement MFA for Admin and SuperAdmin users
- **Session Management**: Implement appropriate session timeout policies

## Testing IAM Operations

### Unit Test Examples

```typescript
describe('IAM User Management', () => {
  let iam: EdenIAMClient;
  
  beforeEach(() => {
    iam = new EdenIAMClient('https://api.test.com', 'test-token');
  });

  test('create user with valid data', async () => {
    const mockFetch = jest.spyOn(global, 'fetch').mockResolvedValueOnce({
      ok: true,
      json: async () => ({ status: 'success' })
    } as Response);

    await iam.createUser({
      username: 'test@example.com',
      password: 'TestPassword123!',
      access_level: 'Write'
    });

    expect(mockFetch).toHaveBeenCalledWith(
      'https://api.test.com/api/v1/iam/users',
      expect.objectContaining({
        method: 'POST',
        headers: expect.objectContaining({
          'Authorization': 'Bearer test-token'
        }),
        body: JSON.stringify({
          username: 'test@example.com',
          password: 'TestPassword123!',
          access_level: 'Write'
        })
      })
    );
  });

  test('handle user already exists error', async () => {
    jest.spyOn(global, 'fetch').mockResolvedValueOnce({
      ok: false,
      text: async () => 'user test@example.com exists'
    } as Response);

    await expect(iam.createUser({
      username: 'test@example.com',
      password: 'TestPassword123!'
    })).rejects.toThrow('Failed to create user: user test@example.com exists');
  });
});
```

This comprehensive IAM implementation guide provides all the necessary information to successfully manage users in your Eden environment, ensuring proper access control and security practices.