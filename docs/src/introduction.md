# Introduction

Welcome to **Eden-MDBS** (Multiple Database Management System), a cloud infrastructure platform that provides a unified API for interacting with multiple database systems and web services.

## What is Eden-MDBS?

Eden-MDBS abstracts the complexity of managing diverse database infrastructure by providing:

- **Unified API** - Single REST API for 10+ database types
- **Cross-Database Transactions** - ACID transactions across multiple databases
- **Organization Management** - Multi-tenant architecture with role-based access control
- **Built-in Observability** - Comprehensive telemetry and monitoring
- **Horizontal Scalability** - Load balancing and distributed architecture

## Who is Eden-MDBS For?

Eden-MDBS is designed for:

- **Application Developers** - Build applications without managing multiple database drivers
- **Platform Engineers** - Deploy a unified data layer for your infrastructure
- **DevOps Teams** - Simplify database operations and monitoring
- **Startups** - Get started quickly without database vendor lock-in

## Key Features

### Multi-Database Support

Connect and query multiple database types through a single API:

- **Relational**: PostgreSQL, MySQL, Microsoft SQL Server, Oracle
- **NoSQL**: MongoDB, Cassandra
- **Key-Value**: Redis
- **Analytics**: ClickHouse
- **Vector**: Pinecone
- **HTTP**: Generic REST endpoints

### Advanced Capabilities

- **Cross-Database ACID Transactions** - Execute atomic operations across different database types
- **Workflow Templates** - Reusable patterns for complex operations
- **Role-Based Access Control (RBAC)** - Fine-grained permissions for users and resources
- **Caching Layer** - Built-in Redis caching for performance
- **OpenTelemetry Integration** - Full observability out of the box

### Developer Experience

- **Simple REST API** - Easy to use from any programming language
- **Comprehensive Documentation** - Examples, guides, and API references
- **Docker Support** - Run locally in minutes

## Quick Example

Create an organization and connect to PostgreSQL:

```bash
# Create an organization (requires org creation token)
curl http://localhost:8000/api/v1/new \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <EDEN_NEW_ORG_TOKEN>" \
  -d '{
    "id": "myorg",
    "super_admins": [{"username": "admin", "password": "password123"}]
  }'

# Login to get a JWT token
curl http://localhost:8000/api/v1/auth/login \
  -u admin:password123 \
  -X POST

# Connect a PostgreSQL endpoint (use JWT token from login)
curl http://localhost:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <JWT_TOKEN>" \
  -d '{
    "id": "mydb",
    "kind": "Postgres",
    "config": {
      "write_conn": {"url": "postgresql://localhost/mydb"}
    }
  }'
```

## Architecture Overview

Eden-MDBS uses a layered architecture:

```
┌─────────────────────────────────────┐
│      REST API (eden_service)        │
├─────────────────────────────────────┤
│   Communication Layer (gRPC)        │
├─────────────────────────────────────┤
│  Endpoint Cores (DB Abstractions)   │
├─────────────────────────────────────┤
│    Database Drivers & Connectors    │
└─────────────────────────────────────┘
```

## Getting Started

Ready to dive in? Start with our [Quick Start Guide](./getting-started/quick-start.md) or learn more about [What is Eden-MDBS](./getting-started/what-is-eden.md).

## Community & Support

- **GitHub**: [eden-dev-inc/eden-mdbs](https://github.com/eden-dev-inc/eden-mdbs)
- **Issues**: [Report bugs or request features](https://github.com/eden-dev-inc/eden-mdbs/issues)
- **Discussions**: [Ask questions and share ideas](https://github.com/eden-dev-inc/eden-mdbs/discussions)

## License

See [License](./appendix/license.md) for licensing information.
