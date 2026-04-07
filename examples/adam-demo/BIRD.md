# BIRD Dataset Staging

Place the BIRD benchmark files under `examples/adam-demo/init/data/bird/` before
starting the `bird` vertical.

If the directory is empty, the `bird` vertical can also download the official
BIRD archive automatically on first run via `BIRD_DATASET_URL`. The extracted
files are reused on later runs, so the download only happens once unless you
delete the cached files yourself.

Supported layouts:

```text
dev.json
dev_databases/<db_id>/<db_id>.sqlite
```

or:

```text
data/dev.json
data/dev_databases/<db_id>/<db_id>.sqlite
```

The loader imports one selected SQLite database into Postgres, validates a
subset of benchmark SQL, and writes `validated_queries.json` back into the same
directory for the Rust demo app to replay through Eden.
