# What is Eden-MDBS?

Eden-MDBS (Multiple Database Management System) is a **unified data infrastructure platform** that provides a single API to interact with multiple database systems.

## The Problem

Modern applications often need to work with multiple databases:

- **PostgreSQL** for transactional data
- **MongoDB** for document storage
- **Redis** for caching and sessions
- **Pinecone** for vector search
- **ClickHouse** for analytics

Managing each database requires:

- Learning different query languages (SQL, MongoDB query language, Redis commands, etc.)
- Integrating multiple client libraries
- Handling different connection patterns
- Managing credentials for each system
- Monitoring each database separately
- Coordinating transactions across systems

This complexity slows down development and increases operational overhead.

## The Solution

Eden-MDBS provides a **single REST API** that abstracts all database operations. Instead of learning multiple database clients, you make HTTP requests to Eden-MDBS, which handles the complexity of interacting with different databases.

## Core Concepts

### Organizations

An **organization** is a tenant in Eden-MDBS. Each organization has:

- Its own users and administrators
- Its own endpoints (database connections)
- Isolated data and permissions
- Independent access control

Organizations enable multi-tenancy, allowing a single Eden-MDBS instance to serve multiple teams or customers.

### Endpoints

An **endpoint** is a connected database or service. Each endpoint has:

- A unique name within the organization
- Connection configuration (URL, credentials, etc.)
- A type (postgres, mongo, redis, etc.)
- Access permissions

Think of endpoints as named database connections that you can query through the API.

### Authentication

Eden-MDBS supports multiple authentication methods:

- **Basic Auth** - Username and password
- **Bearer Tokens** - JWT tokens for programmatic access
- **API Keys** - Organization-scoped keys for services

All requests are scoped to an organization based on the authenticated user.

### Role-Based Access Control (RBAC)

Control who can access what with fine-grained permissions:

- **Subjects** - Users and roles
- **Resources** - Endpoints, workflows, templates
- **Actions** - Read, Write, Delete, Execute, Admin

Example: A "data_analyst" role can Read from analytics endpoints but cannot Write or Delete.

### Workflows

**Workflows** are sequences of operations across multiple endpoints that can be executed as a unit. Benefits:

- Coordinate operations across databases
- Reuse common patterns
- Atomic execution with rollback support
- Template-based for consistency

## Key Features

### 1. Multi-Database Support

Eden-MDBS currently supports:

| Database Type | Supported Systems                        |
| ------------- | ---------------------------------------- |
| Relational    | PostgreSQL, MySQL, MS SQL Server, Oracle |
| Document      | MongoDB                                  |
| Key-Value     | Redis                                    |
| Wide Column   | Cassandra                                |
| Analytics     | ClickHouse                               |
| Vector        | Pinecone                                 |
| HTTP          | Generic REST APIs                        |

### 2. Cross-Database Transactions

Execute ACID transactions across different database types:

```json
{
  "transaction": {
    "operations": [
      {
        "endpoint": "postgres_db",
        "query": "INSERT INTO orders ..."
      },
      {
        "endpoint": "mongo_db",
        "operation": "updateOne",
        "collection": "inventory"
      },
      {
        "endpoint": "redis_cache",
        "command": "SET",
        "key": "order:123"
      }
    ]
  }
}
```

If any operation fails, all changes are rolled back.

### 3. Built-in Observability

Every request includes:

- **Distributed Tracing** - OpenTelemetry traces across all operations
- **Metrics** - Request latency, error rates, database performance
- **Structured Logging** - Context-rich logs with trace correlation
- **Health Checks** - Monitor endpoint availability

### 4. Horizontal Scalability

Eden-MDBS scales horizontally:

- **Load Balancer** - Distribute requests across multiple nodes
- **Stateless API** - Any node can handle any request
- **Distributed Caching** - Shared Redis cache layer
- **Connection Pooling** - Efficient database connection management

### 5. Developer Friendly

- **REST API** - Use from any programming language
- **JSON Everywhere** - Simple, familiar data format
- **Comprehensive Docs** - Examples for every feature
- **Local Development** - Docker Compose for easy setup

## Architecture Principles

Eden-MDBS is built on several key principles:

### API-First Design

Everything is accessible through a well-documented REST API. No proprietary protocols or SDKs required (though client libraries are available).

### Database Agnostic

Eden-MDBS doesn't favor any particular database. Each endpoint type is a first-class citizen with the same capabilities.

### Security by Default

- All communications can use TLS
- Authentication required for all operations
- Fine-grained RBAC for access control
- Audit logging for compliance

### Cloud Native

- Containerized deployment
- Kubernetes-ready
- Horizontal scaling
- Graceful degradation

## Use Cases

### 1. Polyglot Persistence

Your application needs different databases for different purposes. Eden-MDBS provides a unified interface while letting you use the right tool for each job.

### 2. Database Migration

Migrating from one database to another? Connect both as endpoints and gradually shift traffic without changing your application code.

### 3. Multi-Tenant SaaS

Each customer is an organization with their own endpoints and data. Eden-MDBS handles the isolation and access control.

### 4. API Gateway for Databases

Expose databases to external services through a controlled API with authentication, rate limiting, and monitoring.

### 5. Data Integration

Coordinate data across multiple systems with workflows and cross-database transactions.

## Next Steps

Ready to get started?

1. [Quick Start](./quick-start.md) - Get Eden-MDBS running in 5 minutes
2. [Basic Concepts](./concepts.md) - Deeper dive into core concepts
3. [First Steps](./first-steps.md) - Your first organization and endpoint

## Learn More

- [Architecture Overview](../architecture/overview.md)
- [API Reference](../api/overview.md)
- [Examples](../examples/basic.md)
