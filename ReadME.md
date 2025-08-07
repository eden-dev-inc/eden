# Eden Platform Overview

Eden is a comprehensive data infrastructure platform that provides unified access to databases, APIs, and AI services through a powerful orchestration layer. It enables organizations to build, deploy, and manage complex data operations with enterprise-grade security, scalability, and observability.

## What is Eden?

Eden serves as a unified data platform that abstracts the complexity of managing multiple databases, services, and AI integrations. It provides a consistent interface for data operations while handling connection pooling, access control, monitoring, and AI-powered automation behind the scenes.

### Core Value Propositions

- **Unified Data Access**: Connect to multiple database types and services through a single platform
- **AI-Powered Operations**: Integrate AI models safely with your data infrastructure through MCP (Model Context Protocol)
- **Enterprise Security**: Comprehensive RBAC system with fine-grained access control
- **Operational Excellence**: Built-in monitoring, connection pooling, and health management
- **Developer Productivity**: Reusable templates, APIs, and workflows for rapid development

## Architecture Overview

Eden is built around several core concepts that work together to provide a complete data platform:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Applications  │    │      APIs       │    │   Workflows     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
┌─────────────────────────────────────────────────────────────────┐
│                          Eden Platform                          │
├─────────────────┬─────────────────┬─────────────────┬───────────┤
│   Templates     │   Endpoints     │      RBAC       │    MCP    │
│   (Operations)  │ (Connections)   │   (Security)    │   (AI)    │
└─────────────────┴─────────────────┴─────────────────┴───────────┘
         │                       │                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │     MongoDB     │    │      Redis      │
│     MySQL       │    │   ClickHouse    │    │    HTTP APIs    │
│   SQL Server    │    │   Cassandra     │    │   LLM Services  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Core Components

### 1. Organizations
**Multi-tenant isolation layer**
- Complete resource isolation between different customers/environments
- Root entity for all RBAC permissions and resource management
- Support for development, staging, and production environments
- Integrated billing and usage tracking

### 2. Endpoints
**Managed database and service connections**
- Unified interface for PostgreSQL, MongoDB, Redis, ClickHouse, MySQL, SQL Server, Cassandra
- HTTP and LLM service integrations
- Automatic connection pooling and health monitoring
- Built-in metadata collection and schema introspection

**Supported Endpoint Types:**
- **Databases**: PostgreSQL, MongoDB, Redis, ClickHouse, MySQL, SQL Server, Cassandra, Oracle
- **Vector Databases**: Pinecone
- **External Services**: HTTP APIs, LLM providers

### 3. Templates
**Reusable operation definitions**
- Parameterized database operations using Handlebars templating
- Support for Read, Write, Transaction, and Two-Phase Transaction operations
- Type safety and validation for inputs and outputs
- Foundation for APIs, workflows, and migrations

**Template Types:**
- **Read Templates**: Query operations for data retrieval
- **Write Templates**: Data modification operations (INSERT, UPDATE, DELETE)
- **Transaction Templates**: Multi-operation atomic transactions
- **Two-Phase Transaction Templates**: Distributed transactions across endpoints

### 4. APIs
**Composite endpoint orchestration**
- Combine multiple templates into single executable units
- Handle complex data flows with automatic array processing
- Field mapping between input data and template parameters
- Database migration capabilities with rollback support

### 5. Authentication & IAM
**Comprehensive identity management**
- JWT-based authentication with token refresh
- User management with hierarchical access levels (Read, Write, Admin, SuperAdmin)
- Organization-scoped security
- Self-service capabilities for profile management

### 6. RBAC (Role-Based Access Control)
**Fine-grained access control**
- Resource-level permissions for endpoints, templates, workflows, and organizations
- Subject management for users and groups
- Hierarchical access levels with inheritance
- Organization-scoped permission boundaries

### 7. MCP (Model Context Protocol)
**AI-powered database interactions**
- Secure AI model integration with database operations
- Command validation and safety classification system
- Support for PostgreSQL, MongoDB, Redis, and ClickHouse
- Real-time communication through Server-Sent Events

### 8. Migrations
**Database schema and data management**
- Coordinated schema changes across multiple APIs
- Atomic commit/rollback capabilities
- Distributed locking to prevent conflicts
- State tracking and progress monitoring

## Key Features

### Enterprise Security
- **Multi-layered Access Control**: Organization, resource, and operation-level permissions
- **Credential Management**: Encrypted storage of database credentials and API keys
- **Audit Logging**: Comprehensive logging of all operations and access patterns
- **Network Security**: TLS/SSL support for all database connections

### Operational Excellence
- **Connection Pooling**: Automatic connection management and optimization
- **Health Monitoring**: Real-time endpoint health checks and alerting
- **Metadata Management**: Automatic schema discovery and synchronization
- **Performance Monitoring**: Query performance tracking and optimization insights

### Developer Experience
- **Template System**: Reusable, parameterized operations for consistent development
- **API Composition**: Build complex operations from simple building blocks
- **Workflow Orchestration**: Multi-step business logic execution
- **Migration Management**: Safe schema evolution with rollback capabilities

### AI Integration
- **Safe AI Operations**: Validated AI model interactions with databases
- **Command Classification**: Three-tier safety system (Safe, Moderate, Dangerous)
- **Real-time Communication**: SSE-based protocol for live AI model interaction
- **Multi-database Support**: Unified AI interface across different database types

## Use Cases

### 1. Multi-Database Applications
Build applications that seamlessly work across PostgreSQL for transactions, MongoDB for documents, Redis for caching, and ClickHouse for analytics.

### 2. AI-Powered Data Analysis
Enable AI models to safely query and analyze your data with built-in safety controls and validation.

### 3. Microservices Data Layer
Provide a unified data access layer for microservices architectures with consistent security and monitoring.

### 4. ETL and Data Pipelines
Create complex data transformation workflows with built-in error handling and rollback capabilities.

### 5. Multi-tenant SaaS Platforms
Build SaaS applications with complete tenant isolation and fine-grained access control.

### 6. Database Schema Evolution
Manage database schema changes across environments with safe migration and rollback capabilities.

## Getting Started

### 1. Create Organization
```bash
curl -X POST "https://api.eden.com/api/v1/new" \
  -H "Content-Type: application/json" \
  -d '{"id": "my_company", "description": "My Company Eden Organization"}'
```

### 2. Authenticate
```bash
curl -X POST "https://api.eden.com/api/v1/auth/login" \
  -H "Authorization: Basic <base64-credentials>" \
  -d '{"id": "my_company"}'
```

### 3. Create Endpoint
```bash
curl -X POST "https://api.eden.com/api/v1/endpoints" \
  -H "Authorization: Bearer <jwt-token>" \
  -d '{
    "id": "main_database",
    "kind": "Postgres",
    "config": {
      "host": "localhost",
      "port": 5432,
      "database": "myapp",
      "username": "user",
      "password": "password"
    }
  }'
```

### 4. Create Template
```bash
curl -X POST "https://api.eden.com/api/v1/templates" \
  -H "Authorization: Bearer <jwt-token>" \
  -d '{
    "id": "get_users",
    "template": {
      "endpoint_uuid": "<endpoint-uuid>",
      "kind": "Read",
      "template": {
        "query": "SELECT * FROM users WHERE status = {{status}} LIMIT {{limit}}",
        "params": ["{{status}}", "{{limit}}"]
      },
      "endpoint_kind": "Postgres"
    }
  }'
```

### 5. Execute Template
```bash
curl -X POST "https://api.eden.com/api/v1/templates/get_users" \
  -H "Authorization: Bearer <jwt-token>" \
  -d '{"status": "active", "limit": 10}'
```

## Security Model

### Access Levels
- **Read**: View and query resources
- **Write**: Read permissions plus data modification
- **Admin**: Write permissions plus user and configuration management
- **SuperAdmin**: Full control including admin management

### Permission Hierarchy
1. **Organization Level**: Base permissions for all resources
2. **Resource Specific**: Override organization permissions for specific endpoints/templates
3. **Operation Level**: Runtime access control based on operation type

### Data Protection
- **Encryption at Rest**: All credentials and sensitive data encrypted
- **Encryption in Transit**: TLS/SSL for all database connections
- **Access Auditing**: Complete audit trail of all data access
- **Network Isolation**: Support for VPC and private network deployments

## Monitoring and Observability

### Built-in Metrics
- **Connection Pool Status**: Active/idle connection monitoring
- **Query Performance**: Execution time tracking and slow query detection
- **Error Rates**: Connection failures and query error monitoring
- **Resource Usage**: Database and endpoint utilization metrics

### Health Checks
- **Endpoint Health**: Automatic health monitoring for all connections
- **Service Health**: Platform component health and status
- **Dependency Health**: External service dependency monitoring

### Alerting
- **Performance Alerts**: Slow query and connection timeout alerts
- **Error Alerts**: Connection failure and execution error notifications
- **Capacity Alerts**: Resource utilization and capacity planning alerts

## Deployment Options

### Cloud Deployment
- Fully managed Eden cloud service
- Automatic scaling and high availability
- Global edge deployment for low latency

### Self-Hosted
- Docker container deployment
- Kubernetes operator support
- Private cloud and on-premises deployment

### Hybrid Deployment
- Cloud control plane with on-premises data plane
- Cross-cloud and multi-region deployment
- Edge computing and distributed deployments

## Integration Ecosystem

### Database Support
- **Relational**: PostgreSQL, MySQL, SQL Server, Oracle
- **NoSQL**: MongoDB, Cassandra
- **Cache/Memory**: Redis
- **Analytics**: ClickHouse
- **Vector**: Pinecone

### AI and ML Integration
- **LLM Providers**: OpenAI, Anthropic, AWS Bedrock, Azure OpenAI
- **Model Context Protocol**: Safe AI-database interaction
- **Vector Search**: Semantic search and embedding operations

### Development Tools
- **REST APIs**: Complete HTTP API for all operations
- **Client Libraries**: TypeScript, Python, and other language support
- **CLI Tools**: Command-line interface for management operations
- **Documentation**: Comprehensive API documentation and guides

## Best Practices

### Security
- Use principle of least privilege for access control
- Regularly audit user permissions and access patterns
- Implement network security and connection encryption
- Monitor and alert on unusual access patterns

### Performance
- Optimize database queries and use appropriate indexing
- Monitor connection pool usage and adjust sizes accordingly
- Use read replicas and caching where appropriate
- Implement query result caching for frequently accessed data

### Operations
- Use separate organizations for different environments
- Implement proper backup and disaster recovery procedures
- Monitor system health and set up appropriate alerting
- Plan capacity and scale resources proactively

### Development
- Use templates for reusable operations
- Implement comprehensive testing for templates and APIs
- Version control your Eden configurations
- Document your data models and API interfaces

## Support and Resources

### Documentation
- API Reference documentation for all endpoints
- Implementation guides for each feature
- Integration guides for supported databases
- Best practices and architectural guidance

### Community
- GitHub repository for issues and feature requests
- Community forum for questions and discussions
- Example applications and use cases
- Regular webinars and technical talks

### Enterprise Support
- Dedicated support channels for enterprise customers
- Professional services for implementation and optimization
- Training and certification programs
- Custom integration and development services

---

Eden provides the foundation for modern data-driven applications, combining the power of multiple databases, the flexibility of AI integration, and the security of enterprise-grade access control in a single, unified platform. Whether you're building a simple application or a complex data infrastructure, Eden scales to meet your needs while maintaining security, performance, and operational excellence.
