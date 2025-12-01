"""Configuration models for dbt runner gear."""

from pathlib import Path
from typing import NamedTuple


class DbtRunnerConfig(NamedTuple):
    """Configuration for dbt runner gear."""

    dbt_project_zip_path: Path
    storage_label: str
    source_prefix: str
    output_prefix: str
    debug: bool
