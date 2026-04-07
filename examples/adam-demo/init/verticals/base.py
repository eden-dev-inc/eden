"""
Abstract base class for industry verticals.

Each vertical defines a set of database *silos* — potentially multiple
instances of the same database type — representing different departments
or systems within an organization. This is what Eden unifies.

Example (Tech vertical):
  pg-network-security  (Postgres)  — SecOps team
  pg-saas-billing      (Postgres)  — Finance team
  clickhouse-events    (ClickHouse) — Product analytics
  mongo-issues         (MongoDB)   — Engineering
  redis-sessions       (Redis)     — Platform
  weaviate-logs        (Weaviate)  — SRE/On-call
"""

import time
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from dataclasses import dataclass

log = logging.getLogger("adam-init")

SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"


@dataclass
class DatabaseSilo:
    """A single database endpoint representing a team/department silo."""
    name: str                   # Eden endpoint name (e.g., "pg_network_security")
    db_type: str                # postgres, mongo, redis, clickhouse, weaviate
    description: str            # Human-readable description for Eden
    url_env_var: str            # Env var name for connection URL (e.g., "PG_NETWORK_SECURITY_URL")
    eden_url_env_var: str = ""  # Env var for Eden-facing URL (e.g., "EDEN_PG_NETWORK_SECURITY_URL")
    schema_file: str = ""       # Relative path under schemas/<vertical>/ (for PG/CH)
    hf_dataset: str = ""        # HuggingFace dataset ID
    hf_subset: str = ""         # HuggingFace config/subset name
    team: str = ""              # Which team "owns" this silo


class VerticalBase(ABC):
    """Base class all verticals must implement."""

    name: str = ""
    description: str = ""

    @abstractmethod
    def silos(self) -> list[DatabaseSilo]:
        """Return the list of database silos for this vertical."""
        ...

    @abstractmethod
    def load_silo(self, silo: DatabaseSilo, scale: str):
        """Download HuggingFace data and load it into the given silo."""
        ...

    def get_silo(self, name: str) -> DatabaseSilo:
        """Look up a silo by name."""
        for s in self.silos():
            if s.name == name:
                return s
        raise ValueError(f"No silo named '{name}' in {self.name} vertical")


class ProgressTracker:
    """Log-friendly progress tracker for batch operations."""

    def __init__(self, desc, total, log_every_pct=10):
        self.desc = desc
        self.total = total
        self.count = 0
        self.log_every_pct = log_every_pct
        self.last_pct = -1
        self.start = time.time()

    def update(self, n=1):
        self.count += n
        if self.total > 0:
            pct = int(100 * self.count / self.total)
            if pct >= self.last_pct + self.log_every_pct:
                self.last_pct = pct
                elapsed = time.time() - self.start
                rate = self.count / elapsed if elapsed > 0 else 0
                log.info(f"  [{self.desc}] {self.count:,}/{self.total:,} ({pct}%) -- {elapsed:.1f}s ({rate:,.0f}/s)")

    def finish(self):
        elapsed = time.time() - self.start
        rate = self.count / elapsed if elapsed > 0 else 0
        log.info(f"  [{self.desc}] Done: {self.count:,} in {elapsed:.1f}s ({rate:,.0f}/s)")


# ── Shared reference data ────────────────────────────────────────

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
    "Wei", "Yuki", "Raj", "Fatima", "Olga", "Hans", "Pierre", "Aisha",
    "Carlos", "Mei", "Ahmed", "Ingrid", "Hiroshi", "Priya", "Lars", "Sophia",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "Chen", "Wang", "Li", "Zhang", "Liu", "Kim", "Park", "Tanaka", "Sato", "Patel",
    "Singh", "Kumar", "Mueller", "Schmidt", "Fischer", "Rossi", "Colombo", "Dubois",
]

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "protonmail.com", "icloud.com"]

REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East", "Africa", "Oceania"]
COUNTRIES = {
    "North America": ["US", "CA", "MX"],
    "Europe": ["GB", "DE", "FR", "IT", "ES", "NL", "SE", "PL"],
    "Asia Pacific": ["JP", "CN", "IN", "KR", "AU", "SG", "TH", "VN"],
    "Latin America": ["BR", "AR", "CO", "CL", "PE"],
    "Middle East": ["AE", "SA", "IL", "TR"],
    "Africa": ["ZA", "NG", "KE", "EG"],
    "Oceania": ["AU", "NZ"],
}


DATA_DIR = Path(__file__).parent.parent / "data"


def load_local_or_stream(vertical: str, filename: str, hf_dataset: str, hf_split: str = "train",
                         hf_config: str = None, hf_parquet_path: str = None, limit: int = None):
    """Load a pre-downloaded Parquet file, or fall back to HuggingFace.

    Args:
        vertical: Vertical name (subdirectory under data/)
        filename: Local parquet filename to look for
        hf_dataset: HuggingFace dataset ID
        hf_split: HuggingFace split name
        hf_config: HuggingFace config/subset name (for load_dataset)
        hf_parquet_path: Direct path to parquet file in HF repo (bypasses load_dataset)
        limit: Max rows to return

    Returns:
        (iterator_of_dicts, total_count_or_none)
    """
    log = logging.getLogger("adam-init")

    # Check vertical subdir first, then root (for retail backward compat)
    local_path = DATA_DIR / vertical / filename
    if not local_path.exists():
        local_path = DATA_DIR / filename

    if local_path.exists():
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(local_path)
        total_rows = pf.metadata.num_rows
        actual = min(total_rows, limit) if limit else total_rows
        log.info(f"  Loading from local file: {local_path.name} ({total_rows:,} rows, reading {actual:,})")

        def _chunked_iter():
            """Yield rows as dicts in chunks without over-reading large parquet files."""
            yielded = 0
            batch_size = min(actual, 5_000) if actual else 5_000
            for batch in pf.iter_batches(batch_size=batch_size):
                remaining = (limit - yielded) if limit else None
                rows = batch.to_pylist()
                if remaining is not None:
                    rows = rows[:remaining]
                for rec in rows:
                    yield rec
                    yielded += 1
                if limit and yielded >= limit:
                    return

        return _chunked_iter(), actual

    # If a direct parquet path is given, download it from the HF repo
    if hf_parquet_path:
        log.info(f"  Downloading {hf_parquet_path} from {hf_dataset}...")
        from huggingface_hub import hf_hub_download
        local = hf_hub_download(repo_id=hf_dataset, filename=hf_parquet_path, repo_type="dataset")
        pf = pq.ParquetFile(local)
        total_rows = pf.metadata.num_rows
        actual = min(total_rows, limit) if limit else total_rows
        log.info(f"  {actual:,} rows to load from HF parquet")

        def _chunked_hf():
            yielded = 0
            batch_size = min(actual, 5_000) if actual else 5_000
            for batch in pf.iter_batches(batch_size=batch_size):
                remaining = (limit - yielded) if limit else None
                rows = batch.to_pylist()
                if remaining is not None:
                    rows = rows[:remaining]
                for rec in rows:
                    yield rec
                    yielded += 1
                if limit and yielded >= limit:
                    return

        return _chunked_hf(), actual

    # Fall back to HuggingFace streaming
    log.info(f"  Local file not found ({filename}), streaming from HuggingFace: {hf_dataset}")
    from datasets import load_dataset
    kwargs = {"split": hf_split, "streaming": True}
    if hf_config:
        kwargs["name"] = hf_config
    ds = load_dataset(hf_dataset, **kwargs)

    if limit:
        def limited_iter():
            for i, row in enumerate(ds):
                if i >= limit:
                    break
                yield row
        return limited_iter(), limit
    return ds, None


def rand_email(first, last, uid, rng):
    domain = rng.choice(EMAIL_DOMAINS)
    tag = uid % 10000
    return f"{first.lower()}.{last.lower()}{tag}@{domain}"


def rand_date(base_date, range_days, rng):
    from datetime import timedelta
    days = rng.randint(0, range_days)
    seconds = rng.randint(0, 86399)
    return base_date + timedelta(days=days, seconds=seconds)
