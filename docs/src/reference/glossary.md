# Glossary

Key terms and concepts used in Eden-MDBS.

## A

### Access Level
The permission tier assigned to a user. Eden has four levels: Read, Write, Admin, and SuperAdmin. Each level includes permissions from all lower levels.

### Admin
The third tier access level. Admins can manage users, endpoints, templates, and workflows in addition to read/write operations.

## B

### Basic Auth
HTTP Basic Authentication used for the login endpoint. Credentials are Base64-encoded in the format `username:password`.

### Bearer Token
The JWT token included in the `Authorization` header for authenticated API requests. Format: `Authorization: Bearer <token>`.

## E

### Endpoint
A managed connection to an external database or service. Endpoints abstract connection details, handle pooling, and integrate with RBAC.

### Endpoint Kind
The type of database or service an endpoint connects to (e.g., Postgres, Mongo, Redis, Http).

## H

### Handlebars
The templating syntax used in Eden templates for parameter substitution. Uses double curly braces: `{{parameter}}`.

## I

### IAM (Identity and Access Management)
The system for managing users and their permissions within an organization.

## J

### JWT (JSON Web Token)
The token format used for authentication. Contains user and organization identifiers along with expiration time.

## O

### Organization
The top-level multi-tenant container in Eden. Organizations provide complete isolation between different customer environments. All users, endpoints, templates, and workflows belong to an organization.

## P

### Parameterized Query
A query that uses placeholders for values instead of embedding them directly. Prevents SQL injection. Example: `SELECT * FROM users WHERE id = $1`.

## R

### RBAC (Role-Based Access Control)
The permission system that controls who can access what resources. Permissions can be set at both organization and resource levels.

### Read
The lowest access level. Read users can view resources and execute read-only queries on endpoints they have access to.

## S

### Subject
In RBAC context, an entity (user) that can be granted permissions on a resource.

### SuperAdmin
The highest access level. SuperAdmins have full control including managing other admins and organization settings.

## T

### Template
A reusable, parameterized operation definition. Templates define database queries or API calls that can be executed with different parameters.

### Transaction
A group of database operations executed atomically - either all succeed or all are rolled back.

## U

### UUID (Universally Unique Identifier)
A 128-bit identifier used for resources in Eden. Format: `550e8400-e29b-41d4-a716-446655440000`.

## W

### Workflow
A multi-step operation that orchestrates multiple templates or actions. Workflows enable complex data pipelines with conditional logic.

### Write
The second tier access level. Write users have Read permissions plus the ability to execute data modification operations.

## Access Level Hierarchy

```
SuperAdmin (highest)
    ↓
  Admin
    ↓
  Write
    ↓
  Read (lowest)
```

Each level includes all permissions from lower levels.

## Endpoint Types

| Type       | Category    | Description                     |
|------------|-------------|---------------------------------|
| Postgres   | Database    | PostgreSQL relational database  |
| MySQL      | Database    | MySQL/MariaDB database          |
| Mongo      | Database    | MongoDB document database       |
| Redis      | Database    | Redis key-value store           |
| Cassandra  | Database    | Apache Cassandra                |
| ClickHouse | Database    | ClickHouse analytics database   |
| Mssql      | Database    | Microsoft SQL Server            |
| Oracle     | Database    | Oracle database                 |
| Pinecone   | Vector DB   | Pinecone vector database        |
| Http       | Service     | External HTTP/REST API          |
| Llm        | Service     | Large Language Model service    |

## HTTP Status Codes

| Code | Meaning                     |
|------|-----------------------------|
| 200  | Success                     |
| 400  | Bad Request                 |
| 401  | Unauthorized                |
| 403  | Forbidden                   |
| 404  | Not Found                   |
| 409  | Conflict                    |
| 429  | Too Many Requests           |
| 500  | Internal Server Error       |

## API Base URL

```
http://{host}:8000/api/v1/
```

All API endpoints are prefixed with this base URL.
