"""
Stonebreaker benchmark vertical.

This run reuses the retail/e-commerce data plane because it exposes exactly
five heterogeneous sources with shared business keys:
  postgres, mongodb, redis, clickhouse, weaviate

Those five sources are then used by the Rust app's `stonebreaker` query pack
to replay benchmark-style tasks where each task requires exactly two sources
and leaves the other three available as distractors.
"""

from verticals.retail import RetailVertical


class StonebreakerVertical(RetailVertical):
    name = "stonebreaker"
    description = "Stonebraker-style two-source benchmark over five retail connectors"
