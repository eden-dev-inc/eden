use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[clap(name = "adam-demo", about = "ADAM cross-database query simulator")]
pub struct Config {
    /// Eden API base URL
    #[clap(long, env = "EDEN_API_URL", default_value = "http://localhost:8000")]
    pub eden_api_url: String,

    /// Eden organization ID (created during setup if empty)
    #[clap(long, env = "EDEN_ORG_ID", default_value = "adam-demo")]
    pub eden_org_id: String,

    /// Eden JWT token (obtained during setup)
    #[clap(long, env = "EDEN_JWT_TOKEN", default_value = "")]
    pub eden_jwt_token: String,

    /// Industry vertical (retail, stonebreaker, finance, healthcare, insurance, tech, migration, bird)
    #[clap(long, env = "VERTICAL", default_value = "retail")]
    pub vertical: String,

    /// HTTP server bind address
    #[clap(long, env = "BIND_ADDRESS", default_value = "0.0.0.0:3000")]
    pub bind_address: String,

    /// Queries per second across all databases
    #[clap(long, env = "QUERIES_PER_SECOND", default_value_t = 100)]
    pub queries_per_second: u64,

    /// Maximum concurrent query workers
    #[clap(long, env = "MAX_WORKERS", default_value_t = 50)]
    pub max_workers: u32,

    /// PostgreSQL connection URL (for Eden endpoint registration)
    #[clap(
        long,
        env = "POSTGRES_URL",
        default_value = "postgresql://eden:eden@localhost:5632/ecommerce"
    )]
    pub postgres_url: String,

    /// MongoDB connection URL
    #[clap(
        long,
        env = "MONGO_URL",
        default_value = "mongodb://eden:eden@localhost:27217"
    )]
    pub mongo_url: String,

    /// Redis connection URL
    #[clap(long, env = "REDIS_URL", default_value = "redis://localhost:6579")]
    pub redis_url: String,

    /// ClickHouse connection URL
    #[clap(
        long,
        env = "CLICKHOUSE_URL",
        default_value = "http://eden:eden@localhost:8323/analytics"
    )]
    pub clickhouse_url: String,

    /// Weaviate vector DB connection URL
    #[clap(long, env = "WEAVIATE_URL", default_value = "http://localhost:8280")]
    pub weaviate_url: String,

    /// Optional local filesystem root for the Stonebreaker auxiliary document corpus
    #[clap(long, env = "STONEBREAKER_LOCALFS_ROOT", default_value = "")]
    pub stonebreaker_localfs_root: String,

    /// Tavily API key for web search (optional)
    #[clap(long, env = "TAVILY_API_KEY", default_value = "")]
    pub tavily_api_key: String,

    /// Google Workspace OAuth client ID (optional)
    #[clap(long, env = "GOOGLE_WORKSPACE_CLIENT_ID", default_value = "")]
    pub google_workspace_client_id: String,

    /// Google Workspace OAuth project ID (optional)
    #[clap(long, env = "GOOGLE_WORKSPACE_PROJECT_ID", default_value = "")]
    pub google_workspace_project_id: String,

    /// Google Workspace OAuth authorization URI
    #[clap(
        long,
        env = "GOOGLE_WORKSPACE_AUTH_URI",
        default_value = "https://accounts.google.com/o/oauth2/auth"
    )]
    pub google_workspace_auth_uri: String,

    /// Google Workspace OAuth token URI
    #[clap(
        long,
        env = "GOOGLE_WORKSPACE_TOKEN_URI",
        default_value = "https://oauth2.googleapis.com/token"
    )]
    pub google_workspace_token_uri: String,

    /// Google Workspace OAuth provider certificate URL
    #[clap(
        long,
        env = "GOOGLE_WORKSPACE_AUTH_PROVIDER_X509_CERT_URL",
        default_value = "https://www.googleapis.com/oauth2/v1/certs"
    )]
    pub google_workspace_auth_provider_x509_cert_url: String,

    /// Google Workspace OAuth client secret (optional)
    #[clap(long, env = "GOOGLE_WORKSPACE_CLIENT_SECRET", default_value = "")]
    pub google_workspace_client_secret: String,

    /// Google Workspace access token for HTTP endpoint registration (optional)
    #[clap(long, env = "GOOGLE_WORKSPACE_ACCESS_TOKEN", default_value = "")]
    pub google_workspace_access_token: String,

    /// Google Workspace API base URL
    #[clap(
        long,
        env = "GOOGLE_WORKSPACE_API_BASE_URL",
        default_value = "https://www.googleapis.com"
    )]
    pub google_workspace_api_base_url: String,

    /// Azure service principal app ID / client ID for ARM HTTP endpoint registration (optional)
    #[clap(long, env = "AZURE_APP_ID", default_value = "")]
    pub azure_app_id: String,

    /// Optional Azure service principal display name, used in the endpoint description
    #[clap(long, env = "AZURE_DISPLAY_NAME", default_value = "")]
    pub azure_display_name: String,

    /// Azure service principal password / client secret for ARM HTTP endpoint registration (optional)
    #[clap(long, env = "AZURE_PASSWORD", default_value = "")]
    pub azure_password: String,

    /// Azure tenant ID for service principal auth (optional)
    #[clap(long, env = "AZURE_TENANT", default_value = "")]
    pub azure_tenant: String,

    /// Azure subscription ID used to scope the ARM HTTP endpoint (optional)
    #[clap(long, env = "AZURE_SUBSCRIPTION_ID", default_value = "")]
    pub azure_subscription_id: String,

    /// Azure Resource Manager base URL
    #[clap(
        long,
        env = "AZURE_API_BASE_URL",
        default_value = "https://management.azure.com"
    )]
    pub azure_api_base_url: String,

    /// GitLab personal access token for HTTP endpoint registration (optional)
    #[clap(long, env = "GITLAB_ACCESS_TOKEN", default_value = "")]
    pub gitlab_access_token: String,

    /// GitLab API base URL
    #[clap(
        long,
        env = "GITLAB_API_BASE_URL",
        default_value = "https://gitlab.com/api/v4"
    )]
    pub gitlab_api_base_url: String,

    /// OpenRouter API key for LLM (optional)
    #[clap(long, env = "OPENROUTER_API_KEY", default_value = "")]
    pub openrouter_api_key: String,

    /// OpenRouter model identifier
    #[clap(
        long,
        env = "OPENROUTER_MODEL",
        default_value = "anthropic/claude-sonnet-4"
    )]
    pub openrouter_model: String,

    /// OpenAI API key for LLM (optional)
    #[clap(long, env = "OPENAI_API_KEY", default_value = "")]
    pub openai_api_key: String,

    /// OpenAI model identifier
    #[clap(long, env = "OPENAI_MODEL", default_value = "gpt-5.4-nano")]
    pub openai_model: String,

    /// Datadog API key (optional)
    #[clap(long, env = "DD_API_KEY", default_value = "")]
    pub dd_api_key: String,

    /// Datadog application key (optional)
    #[clap(long, env = "DD_APP_KEY", default_value = "")]
    pub dd_app_key: String,

    /// Datadog site (e.g., datadoghq.com, us5.datadoghq.com, datadoghq.eu)
    #[clap(long, env = "DD_SITE", default_value = "datadoghq.com")]
    pub dd_site: String,

    /// Eraser API key for diagram generation (optional)
    #[clap(long, env = "ERASER_API_KEY", default_value = "")]
    pub eraser_api_key: String,

    /// Metrics reporting interval in seconds
    #[clap(long, env = "METRICS_INTERVAL", default_value_t = 10)]
    pub metrics_interval: u64,

    /// PostgreSQL URL as seen by Eden (host-accessible, for endpoint registration)
    #[clap(long, env = "EDEN_POSTGRES_URL")]
    pub eden_postgres_url: Option<String>,

    /// MongoDB URL as seen by Eden
    #[clap(long, env = "EDEN_MONGO_URL")]
    pub eden_mongo_url: Option<String>,

    /// Redis URL as seen by Eden
    #[clap(long, env = "EDEN_REDIS_URL")]
    pub eden_redis_url: Option<String>,

    /// ClickHouse URL as seen by Eden
    #[clap(long, env = "EDEN_CLICKHOUSE_URL")]
    pub eden_clickhouse_url: Option<String>,

    /// Weaviate URL as seen by Eden
    #[clap(long, env = "EDEN_WEAVIATE_URL")]
    pub eden_weaviate_url: Option<String>,

    /// Secret token for creating new organizations in Eden
    #[clap(long, env = "EDEN_NEW_ORG_SECRET", default_value = "neworgsecret")]
    pub eden_new_org_secret: String,

    /// HTTP request timeout in seconds
    #[clap(long, env = "HTTP_TIMEOUT", default_value_t = 30)]
    pub http_timeout: u64,

    /// Max retries for Eden setup (auth + endpoint registration)
    #[clap(long, env = "SETUP_RETRIES", default_value_t = 5)]
    pub setup_retries: u32,
}
