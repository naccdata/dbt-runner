"""Test that the dbt-runner output is fw-dataset compatible and can be
served by the data-connect service.

Loads the dataset at s3://nacc-mqt/sandbox/dataset and validates:
1. fw-dataset can connect and populate all tables
2. full_table exists in DuckDB (data-connect startup check)
3. full_table has properties in dataset_description.json (validator check)
4. naccid column exists (required by structured_query_results)
5. data-connect query patterns work (joined/filtered CTEs)
6. full_table.schema.json exists on S3
7. Schema properties match actual parquet columns

Ref: data-connect source at
  nacc/data-connect/data_connect/dataset/services/dataset.py

Usage:
    python scripts/test_fw_dataset_loading.py --api-key <host:token>
"""

import argparse
import json
import logging
import textwrap

from fw_client import FWClient
from fw_dataset import Dataset
from fw_dataset.filesystem import get_storage_filesystem
from fw_dataset.models import Table

logging.basicConfig(level=logging.WARNING)

STORAGE_LABEL = "s3://nacc-mqt/sandbox/dataset"
BUCKET = "nacc-mqt/sandbox/dataset"
PREFIX = "flywheel-test/release-adrc/63a3718941edd378ce32a9d2"

NP_COLUMNS = [
    "npthal",
    "nphipscl",
    "nptdpb",
    "nptdpc",
    "nptdpd",
    "nptdpe",
    "npftdtau",
    "npftdtdp",
]

# CTE names used by data-connect (dataset/models/dataset.py:21-23)
JOINED_TABLE = "joined"
FILTERED_TABLE = "filtered"


def _get_properties(entry) -> dict:
    """Extract properties from a Table object or raw dict.

    data-connect's LocalDataset.validators (dataset/models/dataset.py:62-65)
    tries data_model.properties first, then falls back to properties.
    Table entries can be fw_dataset.models.Table objects (from schema files)
    or raw dicts (from dataset_description.json).
    """
    if isinstance(entry, Table):
        return entry.data_model.properties
    if isinstance(entry, dict):
        return entry.get("data_model", {}).get("properties", {}) or entry.get(
            "properties", {}
        )
    return {}


def get_s3_credentials(api_key: str) -> dict:
    """Fetch S3 credentials via Flywheel xfer API."""
    client = FWClient(api_key=api_key)
    storages = client.get("/xfer/storages")
    storage = next(s for s in storages["results"] if s["label"] == STORAGE_LABEL)
    creds = client.get(f"/xfer/storage-creds/{storage['_id']}")
    return {"url": creds["url"]}


def check(label: str, ok: bool, detail: str = ""):
    """Print a check result and raise on failure."""
    status = "PASS" if ok else "FAIL"
    msg = f"  [{status}] {label}"
    if detail:
        msg += f" — {detail}"
    print(msg)
    if not ok:
        raise AssertionError(msg)


def _check_dataset_description(dataset):
    """Validate dataset_description.json table entries.

    Ref: dataset/models/dataset.py:62-67 (LocalDataset.validators)
    """
    print("\n--- dataset_description.json checks ---")
    check("full_table in dataset.tables", "full_table" in dataset.tables)

    ft_props = _get_properties(dataset.tables["full_table"])
    check(
        "full_table has non-empty properties",
        len(ft_props) > 0,
        f"{len(ft_props)} properties",
    )

    for table_name, table_entry in dataset.tables.items():
        props = _get_properties(table_entry)
        check(
            f"table '{table_name}' has properties",
            len(props) > 0,
            f"{len(props)} properties",
        )


def _check_columns(conn):
    """Validate full_table columns and row count."""
    print("\n--- full_table column checks ---")
    cols = [
        desc[0] for desc in conn.sql("SELECT * FROM full_table LIMIT 0").description
    ]
    check("naccid column exists", "naccid" in cols)

    missing_np = [c for c in NP_COLUMNS if c not in cols]
    check(
        "all 8 NP columns present",
        not missing_np,
        f"missing: {missing_np}" if missing_np else str(NP_COLUMNS),
    )

    cnt = conn.sql("SELECT count(*) FROM full_table").fetchone()[0]
    check("full_table has rows", cnt > 0, f"{cnt} rows")
    return cols, cnt


def _check_query_patterns(conn, cnt):
    """Validate data-connect CTE query patterns.

    Ref: dataset/services/dataset.py:312-322, 365-374
    """
    print("\n--- data-connect query patterns ---")

    query_joined = textwrap.dedent(f"""\
    WITH
        {JOINED_TABLE} AS (SELECT * FROM full_table),
        {FILTERED_TABLE} AS (SELECT * FROM full_table WHERE 1=1)
    SELECT count(*) AS cnt FROM {JOINED_TABLE}
    """)
    result = conn.sql(query_joined).fetchone()[0]
    check("joined CTE query works", result == cnt, f"{result} rows")

    query_filtered = textwrap.dedent(f"""\
    WITH
        {JOINED_TABLE} AS (SELECT * FROM full_table),
        {FILTERED_TABLE} AS (SELECT * FROM full_table WHERE naccid IS NOT NULL)
    SELECT count(*) AS cnt FROM {FILTERED_TABLE}
    """)
    filtered_cnt = conn.sql(query_filtered).fetchone()[0]
    check(
        "filtered CTE query works",
        filtered_cnt > 0,
        f"{filtered_cnt} rows with naccid IS NOT NULL",
    )

    query_naccid = textwrap.dedent(f"""\
    WITH {FILTERED_TABLE} AS (SELECT * FROM full_table WHERE 1=1)
    SELECT naccid FROM {FILTERED_TABLE} LIMIT 5
    """)
    naccids = conn.sql(query_naccid).fetchall()
    check(
        "SELECT naccid from filtered CTE works",
        len(naccids) > 0,
        f"e.g. {naccids[0][0]}",
    )


def _check_schema_file(dataset, credentials, cols):
    """Validate full_table.schema.json on S3 and column alignment."""
    print("\n--- full_table.schema.json on S3 ---")
    schema_key = (
        f"{BUCKET}/{PREFIX}/versions/{dataset.version}/schemas/full_table.schema.json"
    )
    fs = get_storage_filesystem(dataset.fs.type_, credentials)
    with fs.open(schema_key, "r") as f:
        schema = json.load(f)

    check("schema file readable", True)
    check("schema has $schema key", "$schema" in schema, schema.get("$schema", ""))
    check("schema id is full_table", schema.get("id") == "full_table")
    schema_props = schema.get("properties", {})
    check(
        "schema has properties",
        len(schema_props) > 0,
        f"{len(schema_props)} properties",
    )

    # Schema ↔ parquet column alignment
    print("\n--- schema / parquet alignment ---")
    schema_cols = set(schema_props.keys())
    parquet_cols = set(cols)
    extra = schema_cols - parquet_cols
    check(
        "schema cols subset of parquet cols",
        not extra,
        f"extra in schema: {extra}" if extra else f"{len(schema_cols)} cols match",
    )
    unschemaed = parquet_cols - schema_cols
    if unschemaed:
        print(f"  [INFO] cols in parquet but not schema: {unschemaed}")


def main(api_key: str):
    credentials = get_s3_credentials(api_key)
    dataset = Dataset.get_dataset_from_filesystem("s3", BUCKET, PREFIX, credentials)
    print(f"Dataset: {dataset.name} (id={dataset.id})")
    print(f"Version: {dataset.version_label}\n")

    # 1. fw-dataset connect
    print("--- fw-dataset connect ---")
    conn = dataset.connect(version="latest", fully_populate=True)
    check("connect() succeeds", conn is not None)

    # 2. full_table in SHOW TABLES (data-connect startup)
    # Ref: dataset/services/dataset.py:244-248
    print("\n--- data-connect startup checks ---")
    tables = [t[0] for t in conn.sql("SHOW TABLES").fetchall()]
    check("SHOW TABLES returns results", len(tables) > 0, str(tables))
    check("full_table in SHOW TABLES", "full_table" in tables)

    # 3-7. Remaining checks
    _check_dataset_description(dataset)
    cols, cnt = _check_columns(conn)
    _check_query_patterns(conn, cnt)
    _check_schema_file(dataset, credentials, cols)

    print("\nAll checks passed.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Test fw-dataset compatibility of dbt-runner output "
            "and validate data-connect serving requirements"
        ),
    )
    parser.add_argument(
        "--api-key",
        required=True,
        help="Flywheel API key (e.g. host:token)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args.api_key)
