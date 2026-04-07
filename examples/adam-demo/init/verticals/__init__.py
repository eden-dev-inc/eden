"""
Industry vertical registry.

Each vertical defines domain-specific data generation, HuggingFace datasets,
and database schemas for the 5-database ADAM demo stack.
"""

from verticals.retail import RetailVertical
from verticals.finance import FinanceVertical
from verticals.healthcare import HealthcareVertical
from verticals.insurance import InsuranceVertical
from verticals.tech import TechVertical
from verticals.migration import MigrationVertical
from verticals.bird import BirdVertical
from verticals.stonebreaker import StonebreakerVertical

VERTICALS = {
    "retail": RetailVertical,
    "finance": FinanceVertical,
    "healthcare": HealthcareVertical,
    "insurance": InsuranceVertical,
    "tech": TechVertical,
    "migration": MigrationVertical,
    "bird": BirdVertical,
    "stonebreaker": StonebreakerVertical,
}


def get_vertical(name: str):
    """Get a vertical instance by name."""
    cls = VERTICALS.get(name)
    if cls is None:
        available = ", ".join(sorted(VERTICALS.keys()))
        raise ValueError(f"Unknown vertical '{name}'. Available: {available}")
    return cls()
