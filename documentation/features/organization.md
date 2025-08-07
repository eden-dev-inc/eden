# Organization Implementation Guide

Organizations in Eden are the top-level entities that provide isolated multi-tenant environments for users, resources, and data. They serve as the fundamental boundary for access control, resource management, and billing within the Eden platform.

## What Are Organizations?

Organizations are multi-tenant containers that:
- Provide complete isolation between different customer environments
- Serve as the root entity for all RBAC (Role-Based Access Control) permissions
- Manage collections of users, endpoints, templates, workflows, and APIs
- Enable resource quotas, billing, and usage tracking
- Support both simple and verbose data retrieval modes
- Integrate with Eden nodes for distributed deployment scenarios

## Organization Architecture

### Core Components

1. **Organization Schema**: The main organization configuration and metadata
2. **User Management**: Collection of users belonging to the organization
3. **Resource Collections**: Endpoints, templates, workflows, and APIs owned by the organization
4. **Node Assignment**: Association with specific Eden nodes for deployment
5. **RBAC Integration**: Organization-level access controls and permissions

### Organization Data Structure

```json
{
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
```

## Creating Organizations

### Step 1: Define Organization Input

```json
{
  "id": "my_company",
  "description": "My Company's Eden Organization for production workloads"
}
```

### Step 2: HTTP Request to Create Organization

```http
POST /api/v1/new
Content-Type: application/json

{
  "id": "my_company",
  "description": "My Company's Eden Organization for production workloads"
}
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

**What Happens During Creation:**
1. **Organization Schema Generation**: System creates organization with unique UUID
2. **Node Assignment**: Organization is automatically assigned to the current Eden node
3. **Database Storage**: Organization configuration is stored in PostgreSQL
4. **Cache Initialization**: Organization is cached in Redis for fast access
5. **RBAC Setup**: Initial RBAC structures are created for the organization

**Important Notes:**
- No authentication required for organization creation (public endpoint)
- Each organization gets a unique UUID for internal operations
- Organizations are automatically assigned to the Eden node handling the request
- Initial user and admin assignment happens through separate endpoints

## Retrieving Organization Information

### Basic Organization Information

```http
GET /api/v1/organizations
Authorization: Bearer your_jwt_token
```

**Response (Standard Mode):**
```json
{
  "status": "success",
  "data": {
    "id": "my_company",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "description": "My Company's Eden Organization for production workloads",
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

### Verbose Organization Information

Include the `X-Eden-Verbose: true` header to get complete organization details:

```http
GET /api/v1/organizations
Authorization: Bearer your_jwt_token
X-Eden-Verbose: true
```

**Response (Verbose Mode):**
```json
{
  "status": "success",
  "data": {
    "id": "my_company",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "description": "My Company's Eden Organization for production workloads",
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z",
    "eden_node_uuids": [
      "550e8400-e29b-41d4-a716-446655440001",
      "550e8400-e29b-41d4-a716-446655440002"
    ],
    "super_admin_uuids": [
      "550e8400-e29b-41d4-a716-446655440003"
    ],
    "user_uuids": [
      "550e8400-e29b-41d4-a716-446655440004",
      "550e8400-e29b-41d4-a716-446655440005",
      "550e8400-e29b-41d4-a716-446655440006"
    ],
    "endpoint_uuids": [
      "550e8400-e29b-41d4-a716-446655440007",
      "550e8400-e29b-41d4-a716-446655440008"
    ],
    "template_uuids": [
      "550e8400-e29b-41d4-a716-446655440009",
      "550e8400-e29b-41d4-a716-44665544000a"
    ],
    "workflow_uuids": [
      "550e8400-e29b-41d4-a716-44665544000b"
    ]
  }
}
```

### Response Mode Differences

| Mode | Use Case | Content |
|------|----------|---------|
| **Standard** | Dashboard displays, quick overview | Counts of resources, basic metadata |
| **Verbose** | Administrative tools, detailed analysis | Full UUID lists, complete relationships |

## Updating Organizations

### Modify Organization Configuration

Only SuperAdmin users can update organization information:

```http
PATCH /api/v1/organizations
Content-Type: application/json
Authorization: Bearer your_superadmin_jwt_token

{
  "id": "updated_company_name",
  "description": "Updated description for our production organization"
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "id": "updated_company_name",
    "uuid": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

### Partial Updates

You can update individual fields:

```json
{
  "description": "New organization description"
}
```

```json
{
  "id": "new_organization_id"
}
```

**Update Process:**
1. **SuperAdmin Verification**: Only SuperAdmin access level permitted
2. **Field Validation**: Updated fields are validated for format and uniqueness
3. **Database Update**: Changes are applied to PostgreSQL
4. **Cache Refresh**: Organization cache is updated in Redis
5. **Response Generation**: Updated organization info is returned

## Deleting Organizations

### Remove Organization

**⚠️ Critical Operation**: This permanently deletes the organization and all associated resources.

```http
DELETE /api/v1/organizations
Authorization: Bearer your_superadmin_jwt_token
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
        "deleted_from_cache": [
          "org_cache_uuid",
          "user_cache_uuid_1",
          "user_cache_uuid_2"
        ],
        "deleted_from_postgres": [
          "org_postgres_uuid",
          "user_postgres_uuid_1",
          "user_postgres_uuid_2"
        ]
      },
      "rbac": {
        "removed_subjects": {
          "users": [
            "user_uuid_1",
            "user_uuid_2",
            "user_uuid_3"
          ],
          "roles": [
            "admin_role_uuid",
            "user_role_uuid"
          ]
        }
      }
    }
  }
}
```

**Deletion Process:**
1. **SuperAdmin Verification**: Only SuperAdmin access level permitted
2. **Dependency Resolution**: System identifies all dependent resources
3. **RBAC Cleanup**: All RBAC permissions are removed
4. **Resource Deletion**: All associated endpoints, templates, workflows, APIs are deleted
5. **User Cleanup**: All users are removed from the organization
6. **Database Cleanup**: Organization data is removed from PostgreSQL
7. **Cache Invalidation**: All cached organization data is cleared from Redis

**What Gets Deleted:**
- Organization configuration and metadata
- All users belonging to the organization
- All endpoints and their configurations
- All templates and their definitions
- All workflows and their logic
- All APIs and their bindings
- All RBAC permissions and access controls
- All cached data related to the organization

## Organization Management Best Practices

### 1. Organization Design
- **Naming Convention**: Use consistent, descriptive organization IDs
- **Environment Separation**: Create separate organizations for dev/staging/production
- **Resource Planning**: Plan resource limits and quotas before creation
- **Access Control**: Define clear SuperAdmin and Admin roles

### 2. User Management
- **Principle of Least Privilege**: Grant minimum necessary access levels
- **Regular Reviews**: Audit user access quarterly
- **Offboarding Process**: Remove users promptly when they leave
- **SuperAdmin Limits**: Minimize the number of SuperAdmin users

### 3. Resource Organization
- **Logical Grouping**: Group related endpoints, templates, and workflows
- **Naming Standards**: Use consistent naming for easy identification
- **Documentation**: Document the purpose and usage of resources
- **Version Control**: Track changes to organization configuration

### 4. Security Considerations
- **Access Monitoring**: Monitor SuperAdmin actions and changes
- **Regular Backups**: Backup organization configurations regularly
- **Audit Logging**: Maintain detailed logs of all organization operations
- **Network Security**: Secure access to organization management endpoints

### 5. Operational Procedures
- **Change Management**: Implement approval processes for organization changes
- **Disaster Recovery**: Have procedures for organization restoration
- **Monitoring**: Monitor organization health and resource usage
- **Documentation**: Maintain up-to-date organization documentation

## Client Implementation Examples

### TypeScript Organization Client

```typescript
interface OrganizationInput {
  id: string;
  description?: string;
}

interface OrganizationUpdate {
  id?: string;
  description?: string;
}

interface OrganizationResponse {
  id: string;
  uuid: string;
  description?: string;
  created_at: string;
  updated_at: string;
  eden_nodes: number;
  super_admins: number;
  users: number;
  endpoints: number;
  templates: number;
  workflows: number;
}

class EdenOrganizationClient {
  private baseUrl: string;
  private token?: string;

  constructor(baseUrl: string, token?: string) {
    this.baseUrl = baseUrl;
    this.token = token;
  }

  async createOrganization(config: OrganizationInput): Promise<{ id: string; uuid: string }> {
    const response = await fetch(`${this.baseUrl}/api/v1/new`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(config)
    });

    if (!response.ok) {
      throw new Error(`Failed to create organization: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }

  async getOrganization(verbose: boolean = false): Promise<OrganizationResponse | any> {
    if (!this.token) {
      throw new Error('Authentication token required for organization retrieval');
    }

    const headers: Record<string, string> = {
      'Authorization': `Bearer ${this.token}`
    };

    if (verbose) {
      headers['X-Eden-Verbose'] = 'true';
    }

    const response = await fetch(`${this.baseUrl}/api/v1/organizations`, {
      headers
    });

    if (!response.ok) {
      throw new Error(`Failed to get organization: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }

  async updateOrganization(updates: OrganizationUpdate): Promise<{ id: string; uuid: string }> {
    if (!this.token) {
      throw new Error('Authentication token required for organization update');
    }

    const response = await fetch(`${this.baseUrl}/api/v1/organizations`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.token}`
      },
      body: JSON.stringify(updates)
    });

    if (!response.ok) {
      throw new Error(`Failed to update organization: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }

  async deleteOrganization(): Promise<any> {
    if (!this.token) {
      throw new Error('Authentication token required for organization deletion');
    }

    const response = await fetch(`${this.baseUrl}/api/v1/organizations`, {
      method: 'DELETE',
      headers: {
        'Authorization': `Bearer ${this.token}`
      }
    });

    if (!response.ok) {
      throw new Error(`Failed to delete organization: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data;
  }
}

// Usage examples
const orgClient = new EdenOrganizationClient('https://api.eden.com');

// Create a new organization (no authentication required)
const newOrg = await orgClient.createOrganization({
  id: 'acme_corp',
  description: 'ACME Corporation production environment'
});
console.log('Created organization:', newOrg);

// Get organization details (requires authentication)
const authClient = new EdenOrganizationClient('https://api.eden.com', 'your-jwt-token');

// Basic information
const orgInfo = await authClient.getOrganization();
console.log('Organization info:', orgInfo);

// Verbose information
const verboseInfo = await authClient.getOrganization(true);
console.log('Verbose organization info:', verboseInfo);

// Update organization (SuperAdmin required)
const updated = await authClient.updateOrganization({
  description: 'Updated ACME Corporation production environment'
});
console.log('Updated organization:', updated);

// Delete organization (SuperAdmin required - use with extreme caution)
const deletionResult = await authClient.deleteOrganization();
console.log('Deletion result:', deletionResult);
```

### Python Organization Client

```python
import requests
from typing import Dict, Any, Optional

class EdenOrganizationClient:
    def __init__(self, base_url: str, token: Optional[str] = None):
        self.base_url = base_url
        self.token = token

    def create_organization(self, config: Dict[str, Any]) -> Dict[str, str]:
        """Create a new organization (no authentication required)"""
        response = requests.post(
            f'{self.base_url}/api/v1/new',
            headers={'Content-Type': 'application/json'},
            json=config
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to create organization: {response.text}')

    def get_organization(self, verbose: bool = False) -> Dict[str, Any]:
        """Get organization information (requires authentication)"""
        if not self.token:
            raise Exception('Authentication token required for organization retrieval')

        headers = {'Authorization': f'Bearer {self.token}'}
        if verbose:
            headers['X-Eden-Verbose'] = 'true'

        response = requests.get(
            f'{self.base_url}/api/v1/organizations',
            headers=headers
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to get organization: {response.text}')

    def update_organization(self, updates: Dict[str, Any]) -> Dict[str, str]:
        """Update organization configuration (SuperAdmin required)"""
        if not self.token:
            raise Exception('Authentication token required for organization update')

        response = requests.patch(
            f'{self.base_url}/api/v1/organizations',
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {self.token}'
            },
            json=updates
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to update organization: {response.text}')

    def delete_organization(self) -> Dict[str, Any]:
        """Delete organization (SuperAdmin required - permanent operation)"""
        if not self.token:
            raise Exception('Authentication token required for organization deletion')

        response = requests.delete(
            f'{self.base_url}/api/v1/organizations',
            headers={'Authorization': f'Bearer {self.token}'}
        )
        
        if response.status_code == 200:
            return response.json()['data']
        else:
            raise Exception(f'Failed to delete organization: {response.text}')

# Usage examples
org_client = EdenOrganizationClient('https://api.eden.com')

# Create organization
new_org = org_client.create_organization({
    'id': 'startup_inc',
    'description': 'Startup Inc development environment'
})
print(f"Created organization: {new_org}")

# Get organization info (requires authentication)
auth_client = EdenOrganizationClient('https://api.eden.com', 'your-jwt-token')

# Basic information
org_info = auth_client.get_organization()
print(f"Organization info: {org_info}")

# Verbose information
verbose_info = auth_client.get_organization(verbose=True)
print(f"Verbose info: {verbose_info}")

# Update organization
updated = auth_client.update_organization({
    'description': 'Updated Startup Inc development environment'
})
print(f"Updated organization: {updated}")

# Delete organization (use with extreme caution)
# deletion_result = auth_client.delete_organization()
# print(f"Deletion result: {deletion_result}")
```

## Error Handling

### Common Organization Errors

1. **Organization Already Exists**
```json
{
  "error": "Conflict",
  "message": "Organization with ID 'my_company' already exists"
}
```

2. **Organization Not Found**
```json
{
  "error": "Not Found",
  "message": "Organization not found"
}
```

3. **Insufficient Permissions**
```json
{
  "error": "Forbidden",
  "message": "SuperAdmin access required for organization operations"
}
```

4. **Invalid Organization ID**
```json
{
  "error": "Bad Request",
  "message": "Invalid organization ID format"
}
```

5. **Missing Authentication**
```json
{
  "error": "Unauthorized",
  "message": "Authentication token required"
}
```

### Error Handling Best Practices

```typescript
async function safeOrganizationOperation<T>(operation: () => Promise<T>): Promise<T | null> {
  try {
    return await operation();
  } catch (error) {
    if (error.message.includes('SuperAdmin access required')) {
      console.error('Permission denied - SuperAdmin access required');
    } else if (error.message.includes('already exists')) {
      console.error('Organization already exists - choose a different ID');
    } else if (error.message.includes('Not Found')) {
      console.error('Organization not found - verify organization exists');
    } else if (error.message.includes('Authentication token required')) {
      console.error('Authentication required - provide valid JWT token');
    } else {
      console.error('Unexpected error:', error.message);
    }
    return null;
  }
}

// Usage
const result = await safeOrganizationOperation(() => 
  orgClient.updateOrganization({ description: 'New description' })
);
```

## Organization Integration Patterns

### Multi-Environment Setup

```typescript
class MultiEnvironmentOrganizations {
  private clients: Map<string, EdenOrganizationClient> = new Map();

  constructor(baseUrl: string) {
    // Create separate organizations for different environments
    this.clients.set('dev', new EdenOrganizationClient(baseUrl));
    this.clients.set('staging', new EdenOrganizationClient(baseUrl));
    this.clients.set('prod', new EdenOrganizationClient(baseUrl));
  }

  async setupEnvironments(companyId: string) {
    const environments = ['dev', 'staging', 'prod'];
    
    for (const env of environments) {
      const client = this.clients.get(env);
      if (client) {
        await client.createOrganization({
          id: `${companyId}_${env}`,
          description: `${companyId} ${env.toUpperCase()} environment`
        });
      }
    }
  }

  getClient(environment: string): EdenOrganizationClient | undefined {
    return this.clients.get(environment);
  }
}

// Usage
const multiEnv = new MultiEnvironmentOrganizations('https://api.eden.com');
await multiEnv.setupEnvironments('acme_corp');

const devClient = multiEnv.getClient('dev');
const prodClient = multiEnv.getClient('prod');
```

### Organization Monitoring

```typescript
class OrganizationMonitor {
  private client: EdenOrganizationClient;
  private thresholds: {
    maxUsers: number;
    maxEndpoints: number;
    maxTemplates: number;
  };

  constructor(client: EdenOrganizationClient, thresholds: any) {
    this.client = client;
    this.thresholds = thresholds;
  }

  async checkResourceUsage(): Promise<{
    status: 'ok' | 'warning' | 'critical';
    alerts: string[];
  }> {
    const org = await this.client.getOrganization();
    const alerts: string[] = [];
    let status: 'ok' | 'warning' | 'critical' = 'ok';

    if (org.users >= this.thresholds.maxUsers * 0.8) {
      alerts.push(`User count approaching limit: ${org.users}/${this.thresholds.maxUsers}`);
      status = 'warning';
    }

    if (org.endpoints >= this.thresholds.maxEndpoints * 0.8) {
      alerts.push(`Endpoint count approaching limit: ${org.endpoints}/${this.thresholds.maxEndpoints}`);
      status = 'warning';
    }

    if (org.templates >= this.thresholds.maxTemplates * 0.8) {
      alerts.push(`Template count approaching limit: ${org.templates}/${this.thresholds.maxTemplates}`);
      status = 'warning';
    }

    if (org.users >= this.thresholds.maxUsers || 
        org.endpoints >= this.thresholds.maxEndpoints || 
        org.templates >= this.thresholds.maxTemplates) {
      status = 'critical';
    }

    return { status, alerts };
  }
}

// Usage
const monitor = new OrganizationMonitor(authClient, {
  maxUsers: 100,
  maxEndpoints: 50,
  maxTemplates: 200
});

const usage = await monitor.checkResourceUsage();
console.log('Resource usage status:', usage);
```

## Advanced Organization Features

### Node Management

Organizations are automatically assigned to Eden nodes, but you can query node information:

```typescript
// Get verbose organization info to see node assignments
const verboseInfo = await authClient.getOrganization(true);
console.log('Eden nodes:', verboseInfo.eden_node_uuids);
```

### Bulk Operations

```typescript
async function bulkOrganizationSetup(configs: OrganizationInput[]) {
  const results = await Promise.allSettled(
    configs.map(config => orgClient.createOrganization(config))
  );

  const successful = results.filter(r => r.status === 'fulfilled');
  const failed = results.filter(r => r.status === 'rejected');

  console.log(`Created ${successful.length} organizations`);
  console.log(`Failed to create ${failed.length} organizations`);

  return { successful, failed };
}

// Usage
await bulkOrganizationSetup([
  { id: 'org1', description: 'First organization' },
  { id: 'org2', description: 'Second organization' },
  { id: 'org3', description: 'Third organization' }
]);
```

This comprehensive organization implementation guide provides all the necessary information to successfully create, manage, and operate organizations in your Eden environment, serving as the foundation for multi-tenant application deployment and resource management.