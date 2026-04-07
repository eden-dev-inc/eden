#!/usr/bin/env python3
"""
ADAM Demo — Pre-download HuggingFace datasets for all verticals and curated document corpora.

Run once per vertical to download datasets into data/<vertical>/ as Parquet files.
The Docker init container loads from these local files instead of streaming from HF.

Usage:
    pip install datasets pandas pyarrow tqdm huggingface_hub
    python3 download_datasets.py                  # Download all verticals
    python3 download_datasets.py retail            # Download only retail
    python3 download_datasets.py stonebreaker      # Stonebreaker reuses retail data
    python3 download_datasets.py tech finance      # Download tech and finance
    python3 download_datasets.py documents         # Download curated document corpora

Verticals and their datasets:
    retail      — t-tech/T-ECD (~574 MB)
    stonebreaker — retail T-ECD + document corpus for benchmark document retrieval
    tech        — UNSW-NB15, ecommerce-behavior, StackOverflow, CVE (~multi-GB)
    finance     — CiferAI fraud, credit-card, TroveLedger stocks, SEC filings (~multi-GB)
    healthcare  — Synthea 575K patients + CMS DE-SynPUF Medicare claims (~multi-GB)
    insurance   — freMTPL2, US Accidents (~multi-GB)
    documents   — curated Hugging Face document datasets (policy and legal PDFs/markdown)
"""

import sys
import time
import shutil
import csv
from pathlib import Path

import pandas as pd
from datasets import load_dataset
from tqdm import tqdm

DATA_DIR = Path(__file__).parent / "data"
DOCUMENTS_DIR = DATA_DIR / "documents"

# ── Row limits per vertical (for pre-download, use generous limits) ──
# These match the "large" scale — the loader can always use fewer rows.
LIMITS = {
    "tech": {
        "network_flows": 2_500_000,
        "user_events": 50_000_000,
        "stackoverflow": 5_000_000,
        "cve": 300_000,
    },
    "finance": {
        "core_txns": 21_000_000,
        "credit_txns": 1_850_000,
        "stock_bars": 10_000_000,
        "sec_filings": 245_000,
    },
    "healthcare": {
        "patients": 575_000,
        "encounters": 5_000_000,
        "conditions": 3_000_000,
        "observations": 5_000_000,
        "medications": 3_000_000,
        "payers": 100_000,
        "cms_bene": 116_000,
        "cms_ip": 67_000,
        "cms_op": 790_000,
        "cms_pde": 2_500_000,
        "cms_car": 5_500_000,
    },
    "insurance": {
        "policies_freq": 678_000,
        "policies_sev": 30_000,
        "accidents": 2_850_000,
    },
}


def save_parquet(df, path):
    """Save DataFrame to parquet and print stats."""
    df.to_parquet(path, index=False)
    size_mb = path.stat().st_size / 1024 / 1024
    print(f"  -> {path.name}: {len(df):,} rows, {size_mb:.1f} MB")
    return path


def stream_to_parquet(dataset_id, split, output_path, limit, config=None, columns=None, text_cap=None):
    """Stream a HF dataset and save as parquet with a row limit."""
    if output_path.exists():
        existing = pd.read_parquet(output_path)
        print(f"  Already exists: {output_path.name} ({len(existing):,} rows)")
        return

    print(f"  Downloading {dataset_id}" + (f" [{config}]" if config else "") + f" (limit: {limit:,})...")
    kwargs = {"split": split, "streaming": True}
    if config:
        kwargs["name"] = config

    ds = load_dataset(dataset_id, **kwargs)

    rows = []
    start = time.time()
    for i, row in enumerate(tqdm(ds, total=limit, desc=f"  {output_path.stem}", unit="row")):
        if i >= limit:
            break
        if columns:
            row = {k: row.get(k) for k in columns}
        if text_cap:
            for field in text_cap:
                if field in row and row[field]:
                    row[field] = str(row[field])[:text_cap[field]]
        rows.append(row)

    elapsed = time.time() - start
    print(f"  Downloaded {len(rows):,} rows in {elapsed:.1f}s")

    df = pd.DataFrame(rows)
    save_parquet(df, output_path)


def download_hf_parquet(repo_id, remote_path, output_path, limit=None):
    """Download a parquet file from a HF repo, optionally trimming rows.

    For very large files (>1GB), uses PyArrow to read only the needed rows
    instead of loading the entire file into memory.
    """
    from huggingface_hub import hf_hub_download

    if output_path.exists():
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(str(output_path))
        print(f"  Already exists: {output_path.name} ({pf.metadata.num_rows:,} rows)")
        return

    print(f"  Downloading {repo_id}/{remote_path}...")
    local = hf_hub_download(repo_id=repo_id, filename=remote_path, repo_type="dataset")

    import pyarrow.parquet as pq
    pf = pq.ParquetFile(local)
    total_rows = pf.metadata.num_rows
    print(f"  Source: {total_rows:,} rows, {pf.metadata.num_row_groups} row groups")

    if limit and total_rows > limit:
        # Read only enough row groups to satisfy the limit
        rows_read = 0
        batches = []
        for i in range(pf.metadata.num_row_groups):
            rg = pf.read_row_group(i)
            batches.append(rg)
            rows_read += rg.num_rows
            if rows_read >= limit:
                break
        import pyarrow as pa
        table = pa.concat_tables(batches)
        df = table.to_pandas().head(limit)
        print(f"  Trimmed to {len(df):,} rows")
    else:
        df = pd.read_parquet(local)

    save_parquet(df, output_path)


def download_hf_files(repo_id, output_dir, allow_suffixes, include_paths=None, max_files=None):
    """Download raw files from a Hugging Face dataset repo into a local directory.

    Returns:
        List[dict] rows suitable for a manifest.
    """
    from huggingface_hub import hf_hub_download, list_repo_tree

    output_dir.mkdir(parents=True, exist_ok=True)

    if include_paths is None:
        entries = list_repo_tree(repo_id, repo_type="dataset", recursive=True)
        candidate_paths = []
        for entry in entries:
            path = getattr(entry, "path", None) or getattr(entry, "rfilename", None)
            if not path:
                continue
            if Path(path).suffix.lower() in allow_suffixes:
                candidate_paths.append(path)
    else:
        candidate_paths = [path for path in include_paths if Path(path).suffix.lower() in allow_suffixes]

    candidate_paths = sorted(candidate_paths)
    if max_files is not None:
        candidate_paths = candidate_paths[:max_files]

    manifest_rows = []
    for remote_path in candidate_paths:
        local_cache_path = Path(
            hf_hub_download(repo_id=repo_id, filename=remote_path, repo_type="dataset")
        )
        local_path = output_dir / remote_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if not local_path.exists():
            shutil.copy2(local_cache_path, local_path)
            print(f"  Downloaded {remote_path}")
        else:
            print(f"  Already exists: {remote_path}")

        manifest_rows.append(
            {
                "repo_id": repo_id,
                "remote_path": remote_path,
                "local_path": str(local_path.relative_to(DATA_DIR)),
                "suffix": local_path.suffix.lower(),
                "size_bytes": local_path.stat().st_size,
            }
        )

    return manifest_rows


def write_manifest(rows, output_path):
    """Write a CSV manifest for downloaded document assets."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["repo_id", "remote_path", "local_path", "suffix", "size_bytes"]
    with output_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  -> {output_path.relative_to(DATA_DIR.parent)}: {len(rows):,} entries")


# ═══════════════════════════════════════════════════════════════
# Retail — T-ECD (uses existing download logic)
# ═══════════════════════════════════════════════════════════════

def download_retail():
    """Download retail datasets (uses existing download_datasets.py logic)."""
    outdir = DATA_DIR / "retail"
    outdir.mkdir(parents=True, exist_ok=True)

    # Check if already downloaded (retail data may also exist in data/ root for backward compat)
    existing_root = list(DATA_DIR.glob("tecd_*.parquet"))
    if existing_root:
        print(f"\n  Retail data already exists in data/ ({len(existing_root)} files)")
        # Copy to retail/ subdir for consistency
        for f in existing_root:
            dest = outdir / f.name
            if not dest.exists():
                import shutil
                shutil.copy2(f, dest)
                print(f"  Copied {f.name} -> retail/")
        return

    # Also check if retail/ already has data
    existing_retail = list(outdir.glob("tecd_*.parquet"))
    if existing_retail:
        print(f"\n  Retail data already exists in retail/ ({len(existing_retail)} files)")
        return

    # Download T-ECD using the huggingface_hub approach
    from huggingface_hub import hf_hub_download, list_repo_tree

    REPO_ID = "t-tech/T-ECD"
    BASE_PATH = "dataset/small"
    MAX_DAYS = 5

    def dl(remote):
        return hf_hub_download(repo_id=REPO_ID, filename=remote, repo_type="dataset")

    def read_safe(path):
        import pyarrow.parquet as pq
        schema = pq.read_schema(path)
        safe_cols = [f.name for f in schema if "list" not in str(f.type).lower() and "fixed_size" not in str(f.type).lower()]
        return pd.read_parquet(path, columns=safe_cols) if safe_cols else pd.read_parquet(path)

    # Users & brands
    print("\n  Users...")
    save_parquet(pd.read_parquet(dl(f"{BASE_PATH}/users.pq")), outdir / "tecd_users.parquet")
    print("  Brands...")
    save_parquet(read_safe(dl(f"{BASE_PATH}/brands.pq")), outdir / "tecd_brands.parquet")

    # Domain events & items
    for domain, name in [("marketplace", "marketplace"), ("retail", "retail"), ("offers", "offers")]:
        print(f"\n  {domain.title()} items...")
        try:
            save_parquet(read_safe(dl(f"{BASE_PATH}/{domain}/items.pq")), outdir / f"tecd_{name}_items.parquet")
        except Exception as e:
            print(f"  Warning: {e}")

        print(f"  {domain.title()} events (up to {MAX_DAYS} days)...")
        try:
            entries = list_repo_tree(REPO_ID, path_in_repo=f"{BASE_PATH}/{domain}/events/", repo_type="dataset")
            day_files = sorted([e.rfilename for e in entries if e.rfilename.endswith(".pq")])[:MAX_DAYS]
            frames = []
            for fpath in tqdm(day_files, desc=f"  {domain} days"):
                frames.append(read_safe(dl(fpath)))
            if frames:
                save_parquet(pd.concat(frames, ignore_index=True), outdir / f"tecd_{name}_events.parquet")
        except Exception as e:
            print(f"  Warning: {e}")

    # Reviews
    print("\n  Reviews...")
    try:
        entries = list_repo_tree(REPO_ID, path_in_repo=f"{BASE_PATH}/reviews/", repo_type="dataset")
        day_files = sorted([e.rfilename for e in entries if e.rfilename.endswith(".pq")])[:MAX_DAYS]
        frames = []
        for fpath in tqdm(day_files, desc="  reviews"):
            frames.append(read_safe(dl(fpath)))
        if frames:
            save_parquet(pd.concat(frames, ignore_index=True), outdir / "tecd_reviews.parquet")
    except Exception as e:
        print(f"  Warning: {e}")


# ═══════════════════════════════════════════════════════════════
# Tech — UNSW-NB15, ecommerce-behavior, StackOverflow, CVE
# ═══════════════════════════════════════════════════════════════

def download_tech():
    outdir = DATA_DIR / "tech"
    outdir.mkdir(parents=True, exist_ok=True)
    limits = LIMITS["tech"]

    # Postgres #1: Network security flows
    stream_to_parquet(
        "wwydmanski/UNSW-NB15", "train",
        outdir / "unsw_nb15.parquet",
        limits["network_flows"],
    )

    # ClickHouse: User behavior events (285M total, we take 50M)
    stream_to_parquet(
        "kevykibbz/ecommerce-behavior-data-from-multi-category-store_oct-nov_2019", "train",
        outdir / "user_events.parquet",
        limits["user_events"],
    )

    # Postgres #2: StackOverflow questions (23M total)
    stream_to_parquet(
        "pacovaldez/stackoverflow-questions", "train",
        outdir / "stackoverflow.parquet",
        limits["stackoverflow"],
        text_cap={"body": 10000},
    )

    # MongoDB: CVE vulnerability records (300K+)
    stream_to_parquet(
        "stasvinokur/cve-and-cwe-dataset-1999-2025", "train",
        outdir / "cve_vulnerabilities.parquet",
        limits["cve"],
    )


# ═══════════════════════════════════════════════════════════════
# Finance — CiferAI fraud, credit-card, TroveLedger stocks, SEC
# ═══════════════════════════════════════════════════════════════

def download_finance():
    outdir = DATA_DIR / "finance"
    outdir.mkdir(parents=True, exist_ok=True)
    limits = LIMITS["finance"]

    # Postgres #1: Fraud detection transactions (21M)
    stream_to_parquet(
        "CiferAI/Cifer-Fraud-Detection-Dataset-AF", "train",
        outdir / "fraud_transactions.parquet",
        limits["core_txns"],
    )

    # Postgres #2: Credit card transactions (1.85M)
    stream_to_parquet(
        "pointe77/credit-card-transaction", "train",
        outdir / "credit_card_transactions.parquet",
        limits["credit_txns"],
    )

    # ClickHouse: Stock OHLCV bars (TroveLedger, 40M+)
    # Note: TroveLedger uses 'validation' split
    stream_to_parquet(
        "Traders-Lab/TroveLedger", "validation",
        outdir / "stock_bars.parquet",
        limits["stock_bars"],
    )

    # MongoDB: SEC filings (245K documents with full text)
    stream_to_parquet(
        "PleIAs/SEC", "train",
        outdir / "sec_filings.parquet",
        limits["sec_filings"],
        text_cap={"text": 50000},
    )


def download_stonebreaker():
    """Download retail data plus a compact document corpus for stonebreaker."""
    download_retail()

    outdir = DATA_DIR / "stonebreaker"
    outdir.mkdir(parents=True, exist_ok=True)

    stream_to_parquet(
        "G4KMU/t2-ragbench", "turn_0",
        outdir / "benchmark_documents.parquet",
        1500,
        config="ConvFinQA",
        columns=[
            "id",
            "context_id",
            "question",
            "context",
            "file_name",
            "company_name",
            "company_symbol",
            "report_year",
            "page_number",
            "company_sector",
            "company_industry",
        ],
        text_cap={"question": 500, "context": 6000},
    )


# ═══════════════════════════════════════════════════════════════
# Healthcare — Synthea 575K patients (multi-table)
# ═══════════════════════════════════════════════════════════════

def download_cms_synpuf(outdir, limits):
    """Download CMS DE-SynPUF Medicare claims data from CMS.gov.

    Downloads ZIP archives, extracts CSVs, and converts to Parquet for
    consistency with the rest of the pipeline. The CMS SynPUF data has
    intentionally cryptic column names (e.g. SP_ALZHDMTA, BENE_ESRD_IND)
    — this is by design for the demo.
    """
    import urllib.request
    import zipfile
    import tempfile

    CMS_BASE = "https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads"
    CMS_ALT = "https://downloads.cms.gov/files"

    cms_files = {
        "cms_beneficiary": {
            "urls": [
                f"{CMS_BASE}/de1_0_2008_beneficiary_summary_file_sample_1.zip",
            ],
            "limit": limits.get("cms_bene", 116_000),
        },
        "cms_inpatient": {
            "urls": [
                f"{CMS_BASE}/de1_0_2008_to_2010_inpatient_claims_sample_1.zip",
            ],
            "limit": limits.get("cms_ip", 67_000),
        },
        "cms_outpatient": {
            "urls": [
                f"{CMS_BASE}/de1_0_2008_to_2010_outpatient_claims_sample_1.zip",
            ],
            "limit": limits.get("cms_op", 790_000),
        },
        "cms_pde": {
            "urls": [
                f"{CMS_ALT}/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.zip",
            ],
            "limit": limits.get("cms_pde", 2_500_000),
        },
        "cms_carrier": {
            "urls": [
                f"{CMS_ALT}/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.zip",
            ],
            "limit": limits.get("cms_car", 5_500_000),
        },
    }

    for name, info in cms_files.items():
        output_path = outdir / f"{name}.parquet"
        if output_path.exists():
            existing = pd.read_parquet(output_path)
            print(f"  Already exists: {output_path.name} ({len(existing):,} rows)")
            continue

        limit = info["limit"]
        downloaded = False

        for url in info["urls"]:
            fname = url.rsplit("/", 1)[-1]
            print(f"  Downloading {fname} (limit: {limit:,})...")
            try:
                with tempfile.TemporaryDirectory() as tmpdir:
                    zip_path = Path(tmpdir) / "download.zip"
                    urllib.request.urlretrieve(url, str(zip_path))

                    with zipfile.ZipFile(str(zip_path), "r") as zf:
                        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                        if not csv_names:
                            print(f"  WARNING: No CSV found in {fname}")
                            continue
                        csv_name = csv_names[0]
                        zf.extract(csv_name, tmpdir)
                        csv_path = Path(tmpdir) / csv_name
                        df = pd.read_csv(str(csv_path), nrows=limit, low_memory=False)
                        save_parquet(df, output_path)
                        downloaded = True
                        break
            except Exception as e:
                print(f"  WARNING: Failed from {url}: {e}")
                continue

        if not downloaded:
            print(f"  ERROR: Could not download {name} from any source.")


def download_healthcare():
    """Download Synthea parquet files directly from the HF repo.

    Uses PyArrow for large files (observations=10GB, medications=2.5GB)
    to avoid loading the entire file into memory.
    """
    outdir = DATA_DIR / "healthcare"
    outdir.mkdir(parents=True, exist_ok=True)
    limits = LIMITS["healthcare"]

    REPO = "richardyoung/synthea-575k-patients"
    tables = [
        ("patients", "data/patients.parquet", limits["patients"]),
        ("encounters", "data/encounters.parquet", limits["encounters"]),
        ("conditions", "data/conditions.parquet", limits["conditions"]),
        ("observations", "data/observations.parquet", limits["observations"]),
        ("medications", "data/medications.parquet", limits["medications"]),
        ("payers", "data/payers.parquet", limits["payers"]),
    ]

    for table, remote_path, limit in tables:
        output_path = outdir / f"synthea_{table}.parquet"
        download_hf_parquet(REPO, remote_path, output_path, limit)

    # CMS DE-SynPUF Medicare claims (legacy schema with cryptic column names)
    download_cms_synpuf(outdir, limits)


# ═══════════════════════════════════════════════════════════════
# Insurance — freMTPL2, US Accidents
# ═══════════════════════════════════════════════════════════════

def download_insurance():
    outdir = DATA_DIR / "insurance"
    outdir.mkdir(parents=True, exist_ok=True)
    limits = LIMITS["insurance"]

    # Postgres: French motor third-party liability (frequency)
    stream_to_parquet(
        "mabilton/fremtpl2", "train",
        outdir / "fremtpl2_freq.parquet",
        limits["policies_freq"],
        config="freMTPL2freq",
    )

    # Postgres: French motor third-party liability (severity)
    stream_to_parquet(
        "mabilton/fremtpl2", "train",
        outdir / "fremtpl2_sev.parquet",
        limits["policies_sev"],
        config="freMTPL2sev",
    )

    # MongoDB/ClickHouse: US Accidents (2.85M geo-located accident records)
    stream_to_parquet(
        "nateraw/us-accidents", "train",
        outdir / "us_accidents.parquet",
        limits["accidents"],
        text_cap={"Description": 1000},
    )


# ═══════════════════════════════════════════════════════════════
# Documents — curated raw document corpora from trending HF document datasets
# ═══════════════════════════════════════════════════════════════

def download_documents():
    """Download small curated document corpora for future ADAM document demos.

    Sources were selected from Hugging Face's trending document datasets so the
    bundle stays lightweight and broadly useful:
      - huggingface/policy-docs
      - huggingface-legal/takedown-notices
    """
    DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)

    print("\n  Hugging Face policy documents...")
    manifest_rows = []
    manifest_rows.extend(
        download_hf_files(
            "huggingface/policy-docs",
            DOCUMENTS_DIR / "huggingface_policy_docs",
            allow_suffixes={".pdf", ".md"},
        )
    )

    print("\n  Hugging Face legal takedown notices...")
    manifest_rows.extend(
        download_hf_files(
            "huggingface-legal/takedown-notices",
            DOCUMENTS_DIR / "huggingface_legal_takedown_notices",
            allow_suffixes={".pdf", ".md", ".csv"},
        )
    )

    write_manifest(manifest_rows, DOCUMENTS_DIR / "manifest.csv")


# ═══════════════════════════════════════════════════════════════

DOWNLOADERS = {
    "retail": download_retail,
    "stonebreaker": download_stonebreaker,
    "tech": download_tech,
    "finance": download_finance,
    "healthcare": download_healthcare,
    "insurance": download_insurance,
    "documents": download_documents,
}


def main():
    default_verticals = [name for name in DOWNLOADERS.keys() if name not in {"stonebreaker", "documents"}]
    verticals = sys.argv[1:] if len(sys.argv) > 1 else default_verticals

    for name in verticals:
        if name not in DOWNLOADERS:
            print(f"Unknown vertical: {name}")
            print(f"Available: {', '.join(DOWNLOADERS.keys())}")
            sys.exit(1)

    print("=" * 60)
    print("ADAM Demo — Dataset Downloader")
    print(f"Verticals: {', '.join(verticals)}")
    print(f"Output:    {DATA_DIR}/")
    print("=" * 60)

    for name in verticals:
        print(f"\n{'─' * 60}")
        print(f"Downloading: {name}")
        print(f"{'─' * 60}")
        start = time.time()
        try:
            DOWNLOADERS[name]()
        except Exception as e:
            print(f"\n  ERROR downloading {name}: {e}")
            import traceback
            traceback.print_exc()
        elapsed = time.time() - start
        print(f"\n  {name} complete in {elapsed:.1f}s")

    # Summary
    print(f"\n{'=' * 60}")
    print("Download Summary:")
    total_mb = 0
    for name in verticals:
        summary_dir = "retail" if name == "stonebreaker" else name
        vdir = DATA_DIR / summary_dir
        if vdir.exists():
            files = list(vdir.glob("*.parquet"))
            size = sum(f.stat().st_size for f in files) / 1024 / 1024
            total_mb += size
            print(f"  {name:15s} — {len(files)} files, {size:.0f} MB")
    # Also count root-level retail files
    root_files = list(DATA_DIR.glob("tecd_*.parquet"))
    if root_files:
        size = sum(f.stat().st_size for f in root_files) / 1024 / 1024
        total_mb += size
        print(f"  {'retail (root)':15s} — {len(root_files)} files, {size:.0f} MB")
    print(f"\n  Total: {total_mb:.0f} MB")
    print("=" * 60)


if __name__ == "__main__":
    main()
