"""
ADAM Demo — Data Initializer (multi-vertical)

Dispatches to the appropriate vertical's data loaders based on the VERTICAL env var.

For retail (default), delegates to the original load_data and generate_ecommerce_data
scripts for backward compatibility. For other verticals, downloads HuggingFace datasets
and loads them into the vertical's database silos.

Usage:
  VERTICAL=retail python -u load_data.py    # Original e-commerce (T-ECD)
  VERTICAL=stonebreaker python -u load_data.py  # Stonebraker-style 5-source benchmark
  VERTICAL=tech python -u load_data.py      # Tech/SaaS (UNSW-NB15, GitHub Issues, etc.)
  VERTICAL=finance python -u load_data.py   # Banking (CiferAI fraud, SEC filings, etc.)
  VERTICAL=bird python -u load_data.py      # BIRD benchmark (SQLite imported into Postgres)
"""

import os
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("adam-init")


def clickhouse_init_disabled() -> bool:
    return os.environ.get("DISABLE_CLICKHOUSE_INIT", "1").lower() in {
        "1",
        "true",
        "yes",
    }


def load_retail():
    """Load retail vertical using the original scripts (backward compatible)."""
    # Import the original load functions directly
    import load_data_retail
    load_data_retail.main()


def load_vertical(vertical_name: str, scale: str):
    """Load a non-retail vertical by iterating its silos."""
    from verticals import get_vertical

    vertical = get_vertical(vertical_name)
    silos = vertical.silos()

    log.info(f"Vertical '{vertical.name}' has {len(silos)} database silos:")
    for s in silos:
        hf = f" [{s.hf_dataset}]" if s.hf_dataset else " [synthetic]"
        log.info(f"  {s.name:30s} ({s.db_type:10s}) — {s.team}{hf}")

    for silo in silos:
        if clickhouse_init_disabled() and silo.db_type == "clickhouse":
            log.warning("Skipping %s because DISABLE_CLICKHOUSE_INIT is enabled", silo.name)
            continue
        log.info(f"\n{'=' * 60}")
        log.info(f"Loading silo: {silo.name} ({silo.db_type}) — {silo.description}")
        log.info(f"{'=' * 60}")
        try:
            vertical.load_silo(silo, scale)
        except NotImplementedError as e:
            log.warning(f"Skipping {silo.name}: {e}")
        except Exception as e:
            log.error(f"Failed to load {silo.name}: {e}", exc_info=True)


def main():
    start = time.time()
    vertical = os.environ.get("VERTICAL", "retail").lower()
    scale = os.environ.get("SCALE", "demo").lower()

    log.info("=" * 60)
    log.info(f"ADAM Demo — Data Initialization")
    log.info(f"Vertical: {vertical}")
    log.info(f"Scale:    {scale}")
    log.info("=" * 60)

    if vertical in {"retail", "stonebreaker"}:
        load_retail()
    else:
        load_vertical(vertical, scale)

    elapsed = time.time() - start
    log.info(f"\n{'=' * 60}")
    log.info(f"Data initialization complete in {elapsed:.1f}s")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
