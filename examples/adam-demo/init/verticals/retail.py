"""
Retail / E-commerce vertical (default).

This is the original adam-demo dataset. It delegates to the existing
load_data.py and generate_ecommerce_data.py scripts for backward compatibility.

Silos (5 databases, 1 per type):
  postgres       — Marketplace OLTP (users, orders, payments) from T-ECD
  mongodb        — Product catalog & cart documents from T-ECD
  redis          — Offers cache, leaderboards, sessions from T-ECD
  clickhouse     — Marketplace analytics (OLAP) from T-ECD
  weaviate       — Review & product embeddings

HuggingFace: t-tech/T-ECD
"""

from verticals.base import VerticalBase, DatabaseSilo


class RetailVertical(VerticalBase):
    name = "retail"
    description = "E-commerce / Retail"

    def silos(self) -> list[DatabaseSilo]:
        return [
            DatabaseSilo(name="postgres", db_type="postgres",
                         description="Marketplace OLTP -- users, brands, and marketplace events (T-ECD)",
                         url_env_var="POSTGRES_URL", eden_url_env_var="EDEN_POSTGRES_URL",
                         schema_file="retail/postgres.sql", hf_dataset="t-tech/T-ECD", team="Marketplace"),
            DatabaseSilo(name="mongodb", db_type="mongo",
                         description="Retail domain -- order and cart events as documents (T-ECD)",
                         url_env_var="MONGO_URL", eden_url_env_var="EDEN_MONGO_URL",
                         hf_dataset="t-tech/T-ECD", team="Retail"),
            DatabaseSilo(name="redis", db_type="redis",
                         description="Offers domain -- real-time engagement cache and leaderboards (T-ECD)",
                         url_env_var="REDIS_URL", eden_url_env_var="EDEN_REDIS_URL",
                         hf_dataset="t-tech/T-ECD", team="Offers"),
            DatabaseSilo(name="clickhouse", db_type="clickhouse",
                         description="Marketplace OLAP -- analytics on marketplace events (T-ECD)",
                         url_env_var="CLICKHOUSE_HOST", eden_url_env_var="EDEN_CLICKHOUSE_URL",
                         schema_file="retail/clickhouse.sql", hf_dataset="t-tech/T-ECD", team="Analytics"),
            DatabaseSilo(name="weaviate", db_type="weaviate",
                         description="Reviews domain -- vector search on review embeddings (T-ECD)",
                         url_env_var="WEAVIATE_URL", eden_url_env_var="EDEN_WEAVIATE_URL",
                         hf_dataset="t-tech/T-ECD", team="Search"),
        ]

    def load_silo(self, silo, scale: str):
        # Retail delegates to existing load_data.py + generate_ecommerce_data.py
        # This is handled specially by the entrypoint for backward compatibility
        pass
