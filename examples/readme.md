# Eden Code Examples

A comprehensive collection of code examples demonstrating how to integrate with the Eden platform across multiple programming languages. Eden is a unified data infrastructure platform that provides seamless access to databases, APIs, and AI services through a powerful orchestration layer.

## What is Eden?

Eden serves as a unified data platform that abstracts the complexity of managing multiple databases, services, and AI integrations. It provides enterprise-grade security, scalability, and observability while enabling developers to build sophisticated data operations with ease.

**Key Features:**
- **Multi-Database Support**: PostgreSQL, MongoDB, Redis, ClickHouse, MySQL, SQL Server, and more
- **AI Integration**: Safe AI-database interactions through Model Context Protocol (MCP)
- **Enterprise Security**: Comprehensive RBAC with fine-grained access control
- **Template System**: Reusable, parameterized operations using Handlebars
- **API Orchestration**: Compose multiple operations into single executable units
- **Migration Management**: Safe schema evolution with rollback capabilities

## Repository Structure

This repository contains practical examples organized by programming language:

```
eden-examples/
├── README.md
├── python/
│   ├── README.md
│   ├── basic-operations/
│   ├── advanced-patterns/
│   ├── ai-integration/
│   └── production-examples/
├── javascript/
│   ├── README.md
│   ├── node-examples/
│   ├── browser-examples/
│   ├── react-integration/
│   └── real-world-apps/
├── typescript/
│   ├── README.md
│   ├── basic-usage/
│   ├── enterprise-patterns/
│   └── full-stack-examples/
├── go/
│   ├── README.md
│   ├── cli-tools/
│   ├── microservices/
│   └── performance-examples/
├── java/
│   ├── README.md
│   ├── spring-integration/
│   └── enterprise-applications/
├── csharp/
│   ├── README.md
│   ├── dotnet-core/
│   └── asp-net-examples/
├── php/
│   ├── README.md
│   ├── laravel-integration/
│   └── api-examples/
├── ruby/
│   ├── README.md
│   ├── rails-integration/
│   └── scripting-examples/
└── shared-resources/
    ├── sample-data/
    ├── docker-compose/
    └── documentation/
```

## Language Support

### [Python](./python/) 🐍
Comprehensive Python examples covering everything from basic database operations to advanced AI integrations.

**Highlights:**
- Eden client library usage and best practices
- Database operations across PostgreSQL, MongoDB, Redis
- AI-powered data analysis with MCP integration
- Production-ready patterns for web applications
- Migration and schema management examples

### [JavaScript/Node.js](./javascript/) ⚡
Modern JavaScript examples for both server-side and client-side applications.

**Highlights:**
- Node.js server applications with Eden integration
- Browser-based examples for client-side usage
- React.js integration patterns
- Real-world application examples
- Async/await patterns and error handling

### [TypeScript](./typescript/) 📘
Type-safe examples leveraging TypeScript's powerful type system with Eden.

**Highlights:**
- Fully typed Eden client implementations
- Enterprise application patterns
- Full-stack TypeScript applications
- Advanced type safety and validation
- Production deployment examples

### [Go](./go/) 🚀
High-performance Go examples showcasing Eden's capabilities in systems programming.

**Highlights:**
- CLI tools for Eden management
- Microservices architecture patterns
- High-performance data processing
- Concurrent operations and connection pooling
- Production deployment strategies

### [Java](./java/) ☕
Enterprise Java examples with Spring Boot and other popular frameworks.

**Highlights:**
- Spring Boot integration examples
- Enterprise application architectures
- JPA/Hibernate integration patterns
- Microservices with Eden
- Production configuration examples

### [C#/.NET](./csharp/) 💙
.NET examples covering both .NET Core and Framework applications.

**Highlights:**
- ASP.NET Core web application examples
- Entity Framework integration
- Dependency injection patterns
- Enterprise application examples
- Azure deployment patterns

### [PHP](./php/) 🐘
PHP examples with popular frameworks and modern development practices.

**Highlights:**
- Laravel integration examples
- RESTful API development
- Database abstraction patterns
- Modern PHP practices with Eden
- WordPress integration examples

### [Ruby](./ruby/) 💎
Ruby examples featuring Rails integration and scripting use cases.

**Highlights:**
- Ruby on Rails integration patterns
- ActiveRecord integration examples
- Scripting and automation examples
- API development patterns
- Testing strategies with Eden

## Getting Started

### Prerequisites

1. **Eden Account**: Sign up for an Eden account at [eden.com](https://eden.com)
2. **API Access**: Obtain your API credentials and organization details
3. **Development Environment**: Set up your preferred programming language environment

### Quick Start Guide

1. **Clone this repository**:
   ```bash
   git clone https://github.com/your-org/eden-examples.git
   cd eden-examples
   ```

2. **Choose your language** and navigate to the appropriate directory:
   ```bash
   cd python  # or javascript, go, java, etc.
   ```

3. **Follow the language-specific README** for setup instructions and examples.

4. **Configure your Eden credentials** (typically in environment variables):
   ```bash
   export EDEN_API_URL="https://api.eden.com"
   export EDEN_ORG_ID="your-organization-id"
   export EDEN_JWT_TOKEN="your-jwt-token"
   ```

## Common Patterns Across Languages

### 1. Organization Setup
Every Eden integration starts with organization setup:

```bash
# Create organization
curl -X POST "https://api.eden.com/api/v1/new" \
  -H "Content-Type: application/json" \
  -d '{"id": "my_company", "description": "My Company Eden Organization"}'

# Authenticate
curl -X POST "https://api.eden.com/api/v1/auth/login" \
  -H "Authorization: Basic <base64-credentials>" \
  -d '{"id": "my_company"}'
```

### 2. Endpoint Configuration
Connect to your databases and services:

- **PostgreSQL**: Relational database operations
- **MongoDB**: Document storage and queries
- **Redis**: Caching and session management
- **ClickHouse**: Analytics and time-series data
- **HTTP APIs**: External service integration

### 3. Template Creation
Build reusable, parameterized operations:

- **Read Templates**: Data retrieval operations
- **Write Templates**: Data modification operations
- **Transaction Templates**: Multi-operation atomicity
- **AI Templates**: MCP-enabled AI interactions

### 4. API Orchestration
Compose templates into complex workflows:

- **Field Mapping**: Connect input data to template parameters
- **Array Processing**: Handle bulk operations automatically
- **Migration Support**: Schema evolution with rollback

### 5. Security Integration
Implement proper access control:

- **RBAC Configuration**: Role-based access control
- **User Management**: Hierarchical permission systems
- **Audit Logging**: Track all operations and access

## Example Categories

### Basic Operations
- **Authentication**: Login, token management, and user sessions
- **CRUD Operations**: Create, read, update, delete patterns across databases
- **Connection Management**: Pool configuration and health monitoring

### Intermediate Patterns
- **Template Usage**: Parameterized queries and operations
- **API Composition**: Multi-template workflows
- **Error Handling**: Robust error handling and retry logic
- **Performance Optimization**: Connection pooling and query optimization

### Advanced Integration
- **AI-Powered Operations**: MCP integration for AI-database interactions
- **Migration Management**: Schema evolution and data transformations
- **Multi-Database Transactions**: Distributed transaction patterns
- **Real-time Operations**: Streaming and real-time data processing

### Production Examples
- **Enterprise Applications**: Full-scale application architectures
- **Microservices Integration**: Service mesh and API gateway patterns
- **Monitoring and Observability**: Metrics, logging, and alerting
- **Deployment Strategies**: Docker, Kubernetes, and cloud deployments

## Development Environment Setup

### Using Docker Compose

For consistent development environments, use the provided Docker Compose configurations:

```bash
cd shared-resources/docker-compose
docker-compose up -d
```

This provides:
- PostgreSQL database with sample data
- Redis cache instance
- MongoDB document store
- ClickHouse analytics database

### Sample Data

The `shared-resources/sample-data/` directory contains:
- **Database schemas**: SQL and NoSQL schema definitions
- **Test data**: Representative datasets for examples
- **Migration scripts**: Schema evolution examples
- **Configuration files**: Example Eden configurations

## Testing

Each language directory includes comprehensive testing examples:

- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end workflow testing
- **Performance Tests**: Load testing and benchmarking
- **Security Tests**: Access control and vulnerability testing

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details on:

- **Code Standards**: Language-specific coding conventions
- **Example Structure**: How to organize and document examples
- **Testing Requirements**: Required test coverage and patterns
- **Documentation**: Documentation standards and templates

### Adding New Examples

1. **Fork this repository**
2. **Create a feature branch**: `git checkout -b feature/new-example`
3. **Add your example** following the established patterns
4. **Include comprehensive documentation** and comments
5. **Add tests** for your example code
6. **Submit a pull request** with a detailed description

### Language Support

To add support for a new programming language:

1. **Create language directory** with appropriate structure
2. **Write language-specific README** with setup instructions
3. **Implement basic examples** covering core patterns
4. **Add integration tests** and documentation
5. **Submit pull request** for review

## Resources and Support

### Documentation
- **[Eden Platform Docs](https://docs.eden.com)**: Complete platform documentation
- **[API Reference](https://api.eden.com/docs)**: Comprehensive API documentation
- **[Best Practices Guide](https://docs.eden.com/best-practices)**: Production deployment guidance

### Community
- **[GitHub Issues](https://github.com/your-org/eden-examples/issues)**: Bug reports and feature requests
- **[Discord Community](https://discord.gg/eden)**: Real-time community support
- **[Stack Overflow](https://stackoverflow.com/questions/tagged/eden-platform)**: Technical Q&A

### Support Channels
- **Community Support**: GitHub issues and community forums
- **Enterprise Support**: Dedicated support channels for enterprise customers
- **Professional Services**: Implementation and optimization consulting

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for a detailed history of changes and updates.

---

**Ready to get started?** Choose your programming language and dive into the examples! Each language directory contains detailed setup instructions and progressively complex examples to help you master Eden integration.

For questions, issues, or contributions, please don't hesitate to reach out through our community channels or submit a GitHub issue.