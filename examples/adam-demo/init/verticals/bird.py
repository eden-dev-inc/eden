"""
BIRD vertical — import a BIRD SQLite database into Postgres for the ADAM demo.

Expected dataset layouts under `init/data/bird/`:

1. Extracted benchmark data directly:
   - dev.json
   - dev_databases/<db_id>/<db_id>.sqlite

2. The upstream repo-style layout:
   - data/dev.json
   - data/dev_databases/<db_id>/<db_id>.sqlite

The loader imports one selected database into Postgres, validates a subset of
gold SQL queries against the imported schema, and writes a manifest file that
the Rust demo app replays through Eden.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import tempfile
import urllib.request
import zipfile
from collections import Counter
from pathlib import Path

from psycopg2 import sql
from psycopg2.extras import execute_values

from verticals.base import DATA_DIR, DatabaseSilo, ProgressTracker, VerticalBase

log = logging.getLogger("adam-init")

DEFAULT_SPLIT = "dev"
DEFAULT_MAX_QUERIES = 150
DEFAULT_TIMEOUT_MS = 1500
DEFAULT_DEMO_DB_ID = "california_schools"
MANIFEST_FILENAME = "validated_queries.json"
DEFAULT_DATASET_URLS = {
    "dev": "https://bird-bench.oss-cn-beijing.aliyuncs.com/dev.zip",
    "train": "https://bird-bench.oss-cn-beijing.aliyuncs.com/train.zip",
}


class BirdVertical(VerticalBase):
    name = "bird"
    description = "BIRD text-to-SQL benchmark imported into Postgres"

    def silos(self):
        return [
            DatabaseSilo(
                name="pg_bird",
                db_type="postgres",
                description="BIRD benchmark database imported from SQLite",
                url_env_var="PG_BIRD_URL",
                eden_url_env_var="EDEN_PG_BIRD_URL",
                team="Benchmark",
            )
        ]

    def load_silo(self, silo, scale):
        if silo.name != "pg_bird":
            raise NotImplementedError(f"BIRD does not define silo '{silo.name}'")
        self._load_postgres_benchmark(silo)

    def _load_postgres_benchmark(self, silo):
        import psycopg2

        dataset_dir = Path(
            os.environ.get("BIRD_DATASET_DIR", str(DATA_DIR / "bird"))
        ).expanduser()
        split = os.environ.get("BIRD_SPLIT", DEFAULT_SPLIT).strip() or DEFAULT_SPLIT
        max_queries = int(os.environ.get("BIRD_MAX_QUERIES", DEFAULT_MAX_QUERIES))
        timeout_ms = int(os.environ.get("BIRD_QUERY_TIMEOUT_MS", DEFAULT_TIMEOUT_MS))
        force_reload = os.environ.get("BIRD_FORCE_RELOAD", "").lower() in {
            "1",
            "true",
            "yes",
        }

        manifest_path = Path(
            os.environ.get(
                "BIRD_QUERY_MANIFEST", str(dataset_dir / MANIFEST_FILENAME)
            )
        ).expanduser()
        self._ensure_dataset_available(dataset_dir, split)
        questions_path = self._resolve_questions_path(dataset_dir, split)
        databases_root = self._resolve_databases_root(dataset_dir, split)

        questions = self._load_questions(questions_path)
        selected_db_id = self._select_db_id(questions)
        sqlite_path = self._resolve_sqlite_path(databases_root, selected_db_id)
        pg_url = os.environ.get(silo.url_env_var)
        if not pg_url:
            raise RuntimeError(
                f"{silo.url_env_var} is required to load the BIRD benchmark"
            )

        manifest_path.parent.mkdir(parents=True, exist_ok=True)

        log.info("=" * 60)
        log.info("Loading BIRD benchmark into Postgres")
        log.info(f"Dataset root:   {dataset_dir}")
        log.info(f"Questions file: {questions_path}")
        log.info(f"Database root:  {databases_root}")
        log.info(f"Selected DB:    {selected_db_id}")
        log.info(f"SQLite source:  {sqlite_path}")
        log.info(f"Manifest file:  {manifest_path}")
        log.info("=" * 60)

        with psycopg2.connect(pg_url) as pg_conn:
            pg_conn.autocommit = False
            existing_db_id = self._read_meta(pg_conn, "db_id")
            needs_import = force_reload or existing_db_id != selected_db_id

            if needs_import:
                if force_reload:
                    log.info("Force reload requested; rebuilding imported BIRD database")
                elif existing_db_id:
                    log.info(
                        "Imported BIRD database changed from '%s' to '%s'; rebuilding",
                        existing_db_id,
                        selected_db_id,
                    )
                else:
                    log.info("No imported BIRD database found; starting fresh import")

                self._reset_public_tables(pg_conn)
                self._import_sqlite_database(sqlite_path, pg_conn)
                self._write_meta(
                    pg_conn,
                    {
                        "db_id": selected_db_id,
                        "split": split,
                        "sqlite_path": str(sqlite_path),
                    },
                )
                pg_conn.commit()
            else:
                log.info("Reusing already imported BIRD database '%s'", selected_db_id)

        with psycopg2.connect(pg_url) as pg_conn:
            pg_conn.autocommit = False
            validated_queries = self._validate_queries(
                pg_conn=pg_conn,
                questions=questions,
                selected_db_id=selected_db_id,
                max_queries=max_queries,
                timeout_ms=timeout_ms,
            )
            self._write_manifest(
                manifest_path=manifest_path,
                split=split,
                selected_db_id=selected_db_id,
                sqlite_path=sqlite_path,
                questions=questions,
                validated_queries=validated_queries,
            )
            self._write_meta(
                pg_conn,
                {
                    "db_id": selected_db_id,
                    "split": split,
                    "sqlite_path": str(sqlite_path),
                    "manifest_path": str(manifest_path),
                    "validated_queries": str(len(validated_queries)),
                },
            )
            pg_conn.commit()

        log.info(
            "BIRD manifest ready: %s (%s validated queries)",
            manifest_path,
            len(validated_queries),
        )

    def _resolve_questions_path(self, dataset_dir: Path, split: str) -> Path:
        candidates = []
        for root in self._candidate_dataset_roots(dataset_dir):
            candidates.append(root / f"{split}.json")
        for candidate in candidates:
            if candidate.exists():
                return candidate
        raise FileNotFoundError(
            f"Could not find BIRD questions for split '{split}' under {dataset_dir}. "
            f"Expected one of: {', '.join(str(path) for path in candidates)}"
        )

    def _resolve_databases_root(self, dataset_dir: Path, split: str) -> Path:
        candidates = []
        for root in self._candidate_dataset_roots(dataset_dir):
            candidates.append(root / f"{split}_databases")
        for candidate in candidates:
            if candidate.exists():
                return candidate
        raise FileNotFoundError(
            f"Could not find BIRD database directory for split '{split}' under {dataset_dir}. "
            f"Expected one of: {', '.join(str(path) for path in candidates)}"
        )

    def _candidate_dataset_roots(self, dataset_dir: Path) -> list[Path]:
        roots = [dataset_dir, dataset_dir / "data"]
        roots.extend(
            child for child in sorted(dataset_dir.iterdir()) if child.is_dir() and child.name != "__MACOSX"
        )
        return roots

    def _ensure_dataset_available(self, dataset_dir: Path, split: str):
        dataset_dir.mkdir(parents=True, exist_ok=True)

        try:
            self._resolve_questions_path(dataset_dir, split)
            self._resolve_databases_root(dataset_dir, split)
            log.info("BIRD dataset already present locally; skipping download")
            return
        except FileNotFoundError:
            pass

        dataset_url = os.environ.get("BIRD_DATASET_URL", "").strip()
        if not dataset_url:
            dataset_url = DEFAULT_DATASET_URLS.get(split, "")

        if not dataset_url:
            raise FileNotFoundError(
                f"BIRD dataset for split '{split}' is not staged locally under {dataset_dir}, "
                "and no BIRD_DATASET_URL was provided for automatic download."
            )

        archive_path = dataset_dir / f"{split}.zip"
        if archive_path.exists():
            log.info("Reusing existing BIRD archive: %s", archive_path)
        else:
            self._download_archive_once(dataset_url, archive_path)

        self._extract_archive_once(archive_path, dataset_dir, split)

        # Validate after extraction so failures are loud and actionable.
        self._resolve_questions_path(dataset_dir, split)
        self._resolve_databases_root(dataset_dir, split)

    def _download_archive_once(self, dataset_url: str, archive_path: Path):
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        log.info("Downloading BIRD dataset archive from %s", dataset_url)

        with tempfile.NamedTemporaryFile(
            dir=archive_path.parent, prefix=f"{archive_path.name}.", suffix=".part", delete=False
        ) as temp_handle:
            temp_path = Path(temp_handle.name)

        try:
            with urllib.request.urlopen(dataset_url) as response, temp_path.open("wb") as output:
                shutil.copyfileobj(response, output)
            temp_path.replace(archive_path)
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise

        size_mb = archive_path.stat().st_size / 1024 / 1024
        log.info("Downloaded BIRD archive once: %s (%.1f MB)", archive_path, size_mb)

    def _extract_archive_once(self, archive_path: Path, dataset_dir: Path, split: str):
        try:
            self._resolve_questions_path(dataset_dir, split)
            self._resolve_databases_root(dataset_dir, split)
            return
        except FileNotFoundError:
            pass

        marker_path = dataset_dir / f".{split}.extracted"
        if marker_path.exists():
            log.info(
                "BIRD extraction marker exists but dataset files are incomplete; re-extracting %s",
                archive_path,
            )

        log.info("Extracting BIRD archive: %s", archive_path)
        with zipfile.ZipFile(archive_path) as archive:
            archive.extractall(dataset_dir)

        nested_archives = sorted(
            path
            for path in dataset_dir.rglob(f"{split}_databases.zip")
            if "__MACOSX" not in path.parts
        )
        for nested_archive in nested_archives:
            target_dir = nested_archive.parent
            log.info("Extracting nested BIRD databases archive: %s", nested_archive)
            with zipfile.ZipFile(nested_archive) as archive:
                archive.extractall(target_dir)

        marker_path.write_text(f"{archive_path.name}\n", encoding="utf-8")
        log.info("BIRD archive extracted to %s", dataset_dir)

    def _load_questions(self, questions_path: Path) -> list[dict]:
        with questions_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, list) or not data:
            raise RuntimeError(f"BIRD questions file is empty or invalid: {questions_path}")
        return data

    def _select_db_id(self, questions: list[dict]) -> str:
        override = os.environ.get("BIRD_DB_ID", "").strip()
        if override:
            return override

        counts = Counter(
            item.get("db_id", "").strip() for item in questions if item.get("db_id")
        )
        if not counts:
            raise RuntimeError("No db_id values found in the BIRD questions file")

        if DEFAULT_DEMO_DB_ID in counts:
            log.info(
                "BIRD_DB_ID not set; defaulting to '%s' for the demo (%s benchmark questions available)",
                DEFAULT_DEMO_DB_ID,
                counts[DEFAULT_DEMO_DB_ID],
            )
            return DEFAULT_DEMO_DB_ID

        db_id, count = counts.most_common(1)[0]
        log.info(
            "BIRD_DB_ID not set; selected '%s' because it has the most benchmark questions (%s)",
            db_id,
            count,
        )
        return db_id

    def _resolve_sqlite_path(self, databases_root: Path, db_id: str) -> Path:
        candidates = [
            databases_root / db_id / f"{db_id}.sqlite",
            databases_root / db_id / f"{db_id}.db",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate

        db_dir = databases_root / db_id
        if db_dir.exists():
            for pattern in ("*.sqlite", "*.db"):
                matches = sorted(db_dir.glob(pattern))
                if matches:
                    return matches[0]

        raise FileNotFoundError(
            f"Could not find a SQLite database for db_id '{db_id}' under {databases_root}"
        )

    def _reset_public_tables(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                """
            )
            table_names = [row[0] for row in cur.fetchall()]
            for table_name in table_names:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                        sql.Identifier(table_name)
                    )
                )

    def _ensure_meta_table(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bird_benchmark_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

    def _read_meta(self, pg_conn, key: str) -> str | None:
        self._ensure_meta_table(pg_conn)
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM bird_benchmark_meta WHERE key = %s",
                (key,),
            )
            row = cur.fetchone()
        pg_conn.commit()
        return row[0] if row else None

    def _write_meta(self, pg_conn, values: dict[str, str]):
        self._ensure_meta_table(pg_conn)
        with pg_conn.cursor() as cur:
            for key, value in values.items():
                cur.execute(
                    """
                    INSERT INTO bird_benchmark_meta (key, value)
                    VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                    """,
                    (key, value),
                )

    def _import_sqlite_database(self, sqlite_path: Path, pg_conn):
        sqlite_conn = sqlite3.connect(sqlite_path)
        sqlite_conn.row_factory = sqlite3.Row
        try:
            table_names = self._sqlite_tables(sqlite_conn)
            if not table_names:
                raise RuntimeError(f"No user tables found in SQLite database {sqlite_path}")

            progress = ProgressTracker("BIRD import tables", len(table_names), log_every_pct=20)
            for table_name in table_names:
                column_defs, column_names = self._sqlite_table_schema(sqlite_conn, table_name)
                self._create_postgres_table(pg_conn, table_name, column_defs)
                self._copy_table_rows(sqlite_conn, pg_conn, table_name, column_defs, column_names)
                progress.update()
            progress.finish()
        finally:
            sqlite_conn.close()

    def _sqlite_tables(self, sqlite_conn) -> list[str]:
        cur = sqlite_conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
        return [row[0] for row in cur.fetchall()]

    def _sqlite_table_schema(self, sqlite_conn, table_name: str):
        cur = sqlite_conn.execute(f'PRAGMA table_info("{table_name}")')
        pragma_rows = cur.fetchall()
        if not pragma_rows:
            raise RuntimeError(f"Could not inspect schema for SQLite table '{table_name}'")

        pk_columns = []
        column_defs = []
        column_names = []
        for row in pragma_rows:
            column_name = row[1]
            sqlite_type = row[2] or "TEXT"
            nullable = row[3] == 0
            primary_key_position = row[5]
            pg_type = self._sqlite_type_to_postgres(sqlite_type)
            column_defs.append(
                {
                    "name": column_name,
                    "sqlite_type": sqlite_type,
                    "pg_type": pg_type,
                    "nullable": nullable,
                }
            )
            column_names.append(column_name)
            if primary_key_position:
                pk_columns.append((primary_key_position, column_name))

        pk_columns.sort(key=lambda item: item[0])
        if pk_columns:
            column_defs.append(
                {
                    "kind": "primary_key",
                    "columns": [name for _, name in pk_columns],
                }
            )

        return column_defs, column_names

    def _sqlite_type_to_postgres(self, sqlite_type: str) -> str:
        upper = sqlite_type.upper()
        if "INT" in upper:
            return "BIGINT"
        if any(token in upper for token in ("REAL", "FLOA", "DOUB")):
            return "DOUBLE PRECISION"
        if any(token in upper for token in ("NUMERIC", "DECIMAL")):
            return "NUMERIC"
        if "BOOL" in upper:
            return "BOOLEAN"
        if "BLOB" in upper:
            return "BYTEA"
        if "DATE" in upper and "TIME" not in upper:
            return "DATE"
        if "TIME" in upper:
            return "TIMESTAMP"
        if any(token in upper for token in ("CHAR", "CLOB", "TEXT")):
            return "TEXT"
        return "TEXT"

    def _create_postgres_table(self, pg_conn, table_name: str, column_defs: list[dict]):
        statements = []
        for column_def in column_defs:
            if column_def.get("kind") == "primary_key":
                pk_sql = sql.SQL("PRIMARY KEY ({})").format(
                    sql.SQL(", ").join(
                        sql.Identifier(name) for name in column_def["columns"]
                    )
                )
                statements.append(pk_sql)
                continue

            column_sql = sql.SQL("{} {}{}").format(
                sql.Identifier(column_def["name"]),
                sql.SQL(column_def["pg_type"]),
                sql.SQL("" if column_def["nullable"] else " NOT NULL"),
            )
            statements.append(column_sql)

        create_table = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(statements),
        )

        with pg_conn.cursor() as cur:
            cur.execute(create_table)

    def _copy_table_rows(
        self,
        sqlite_conn,
        pg_conn,
        table_name: str,
        column_defs: list[dict],
        column_names: list[str],
    ):
        value_columns = [item for item in column_defs if item.get("kind") != "primary_key"]
        select_sql = f'SELECT * FROM "{table_name}"'
        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(name) for name in column_names),
        )
        insert_sql_text = insert_sql.as_string(pg_conn)
        placeholder = "__EDEN_VALUES_PLACEHOLDER__"
        insert_sql_text = insert_sql_text.replace("%s", placeholder)
        insert_sql_text = insert_sql_text.replace("%", "%%")
        insert_sql_text = insert_sql_text.replace(placeholder, "%s")

        read_cur = sqlite_conn.execute(select_sql)
        write_cur = pg_conn.cursor()
        batch = []
        imported = 0
        try:
            for row in read_cur:
                batch.append(
                    tuple(
                        self._coerce_value(row[column["name"]], column["pg_type"])
                        for column in value_columns
                    )
                )
                if len(batch) >= 1000:
                    execute_values(write_cur, insert_sql_text, batch, page_size=1000)
                    imported += len(batch)
                    if imported % 10000 == 0:
                        log.info("  [%s] imported %s rows", table_name, f"{imported:,}")
                    batch = []

            if batch:
                execute_values(write_cur, insert_sql_text, batch, page_size=1000)
                imported += len(batch)

            log.info("  [%s] imported %s rows", table_name, f"{imported:,}")
        finally:
            write_cur.close()

    def _coerce_value(self, value, pg_type: str):
        if value is None:
            return None
        if pg_type == "BOOLEAN" and isinstance(value, int):
            return bool(value)
        if pg_type == "BYTEA" and isinstance(value, memoryview):
            return value.tobytes()
        return value

    def _validate_queries(
        self,
        pg_conn,
        questions: list[dict],
        selected_db_id: str,
        max_queries: int,
        timeout_ms: int,
    ) -> list[dict]:
        candidates = [item for item in questions if item.get("db_id") == selected_db_id]
        if not candidates:
            raise RuntimeError(
                f"No BIRD questions found for selected database '{selected_db_id}'"
            )

        log.info(
            "Validating up to %s BIRD SQL queries for db '%s' (available: %s)",
            max_queries,
            selected_db_id,
            len(candidates),
        )

        validated = []
        skipped = 0
        with pg_conn.cursor() as cur:
            cur.execute(sql.SQL("SET statement_timeout = %s"), (timeout_ms,))

            for index, item in enumerate(candidates):
                raw_sql = (item.get("SQL") or item.get("sql") or "").strip()
                question = (item.get("question") or "").strip()
                evidence = (item.get("evidence") or "").strip()

                if not raw_sql or not question:
                    skipped += 1
                    continue

                normalized_sql = raw_sql.rstrip().rstrip(";")
                try:
                    cur.execute("SAVEPOINT bird_validate_query")
                    cur.execute(sql.SQL("EXPLAIN {}").format(sql.SQL(normalized_sql)))
                    cur.execute("RELEASE SAVEPOINT bird_validate_query")
                except Exception as exc:
                    cur.execute("ROLLBACK TO SAVEPOINT bird_validate_query")
                    cur.execute("RELEASE SAVEPOINT bird_validate_query")
                    skipped += 1
                    log.debug(
                        "Skipping unsupported BIRD query %s for '%s': %s",
                        index,
                        selected_db_id,
                        exc,
                    )
                    continue

                validated.append(
                    {
                        "index": index,
                        "question": question,
                        "evidence": evidence,
                        "sql": normalized_sql,
                    }
                )
                if len(validated) >= max_queries:
                    break

        log.info(
            "Validated %s BIRD queries for '%s' (%s skipped)",
            len(validated),
            selected_db_id,
            skipped,
        )
        if not validated:
            raise RuntimeError(
                f"No BIRD SQL queries validated successfully for '{selected_db_id}'. "
                "Try another BIRD_DB_ID or force a reload."
            )
        return validated

    def _write_manifest(
        self,
        manifest_path: Path,
        split: str,
        selected_db_id: str,
        sqlite_path: Path,
        questions: list[dict],
        validated_queries: list[dict],
    ):
        total_for_db = sum(1 for item in questions if item.get("db_id") == selected_db_id)
        payload = {
            "format_version": 1,
            "vertical": "bird",
            "split": split,
            "db_id": selected_db_id,
            "sqlite_path": str(sqlite_path),
            "total_questions_for_db": total_for_db,
            "validated_queries": validated_queries,
        }
        with manifest_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
