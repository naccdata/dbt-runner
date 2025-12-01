"""Validation module for pre-execution checks."""

import logging
import zipfile
from pathlib import Path

from .config import DbtRunnerConfig

log = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when validation fails."""

    pass


def validate_dbt_project(zip_path: Path, extract_dir: Path) -> Path:
    """Validate and extract dbt project zip file.

    Performs the following validation steps:
    1. Verifies the zip file exists at the specified path
    2. Validates the file is a valid zip archive
    3. Validates required files exist: dbt_project.yml and profiles.yml
    4. Validates the models/ directory is present

    Args:
        zip_path: Path to the dbt project zip file
        extract_dir: Directory to extract the project to

    Returns:
        Path to the extracted project directory

    Raises:
        ValidationError: If validation fails at any step
    """
    log.info("Validating dbt project zip file")

    # Check zip file exists
    if not zip_path.exists():
        raise ValidationError(f"dbt project zip file not found: {zip_path}")

    # Check if it's a valid zip file
    if not zipfile.is_zipfile(zip_path):
        raise ValidationError(f"File is not a valid zip archive: {zip_path}")

    # Extract the zip file
    extract_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Extracting dbt project to: {extract_dir}")

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
    except Exception as e:
        raise ValidationError(f"Failed to extract zip file: {str(e)}") from e

    # Find the project root (may be in a subdirectory)
    project_root = _find_dbt_project_root(extract_dir)

    # Validate required files exist
    required_files = ["dbt_project.yml", "profiles.yml"]
    missing_files = []

    for file_name in required_files:
        file_path = project_root / file_name
        if not file_path.exists():
            missing_files.append(file_name)

    if missing_files:
        raise ValidationError(
            f"dbt project missing required files: {', '.join(missing_files)}"
        )

    # Check for models directory
    models_dir = project_root / "models"
    if not models_dir.exists() or not models_dir.is_dir():
        raise ValidationError("dbt project missing 'models/' directory")

    log.info("dbt project validation successful")
    return project_root


def _find_dbt_project_root(extract_dir: Path) -> Path:
    """Find the root directory of the dbt project within extracted files.

    Args:
        extract_dir: Directory where zip was extracted

    Returns:
        Path to the project root containing dbt_project.yml

    Raises:
        ValidationError: If project root cannot be found
    """
    # Check if dbt_project.yml is in the extract_dir directly
    if (extract_dir / "dbt_project.yml").exists():
        return extract_dir

    # Search for dbt_project.yml in subdirectories (up to 2 levels deep)
    for yml_path in extract_dir.rglob("dbt_project.yml"):
        # Return the directory containing dbt_project.yml
        if yml_path.parent.relative_to(extract_dir).parts.__len__() <= 2:
            return yml_path.parent

    raise ValidationError("Could not find dbt_project.yml in extracted zip file")


def validate_source_data(source_dir: Path) -> None:
    """Validate that source data follows Flywheel dataset schema.

    Args:
        source_dir: Directory containing the downloaded dataset

    Raises:
        ValidationError: If validation fails
    """
    log.info("Validating source dataset structure")

    if not source_dir.exists():
        raise ValidationError(f"Source directory not found: {source_dir}")

    # Check for tables directory
    tables_dir = source_dir / "tables"
    if not tables_dir.exists() or not tables_dir.is_dir():
        raise ValidationError(
            "Source dataset missing 'tables/' directory. "
            "Expected Flywheel dataset schema structure."
        )

    # Check that there are parquet files in the tables directory
    parquet_files = list(tables_dir.rglob("*.parquet"))
    if not parquet_files:
        raise ValidationError(
            "No parquet files found in source dataset 'tables/' directory"
        )

    log.info(f"Found {len(parquet_files)} parquet files in source dataset")
    log.info("Source dataset validation successful")


def validate_config_params(config: DbtRunnerConfig) -> None:
    """Validate configuration parameters.

    Args:
        config: Parsed gear configuration

    Raises:
        ValidationError: If validation fails
    """
    log.info("Validating configuration parameters")

    missing_params = []

    if not config.storage_label:
        missing_params.append("storage_label")
    if not config.source_prefix:
        missing_params.append("source_prefix")
    if not config.output_prefix:
        missing_params.append("output_prefix")

    if missing_params:
        raise ValidationError(
            f"Missing required configuration parameters: {', '.join(missing_params)}"
        )

    # Check that paths don't have trailing slashes (can cause issues)
    if config.source_prefix.endswith("/"):
        raise ValidationError(
            "source_prefix should not end with '/'. Remove trailing slash."
        )

    if config.output_prefix.endswith("/"):
        raise ValidationError(
            "output_prefix should not end with '/'. Remove trailing slash."
        )

    log.info("Configuration parameters validation successful")
