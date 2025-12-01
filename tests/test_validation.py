"""Module to test validation.py"""

import tempfile
import zipfile
from pathlib import Path

import pytest

from fw_gear_dbt_runner.config import DbtRunnerConfig
from fw_gear_dbt_runner.validation import (
    ValidationError,
    validate_config_params,
    validate_dbt_project,
    validate_source_data,
)


def test_validate_config_params_success():
    """Test validate_config_params with valid parameters."""
    config = DbtRunnerConfig(
        dbt_project_zip_path=Path("/fake/path.zip"),
        storage_label="test-storage",
        source_prefix="path/to/source",
        output_prefix="path/to/output",
        debug=False,
    )
    # Should not raise any exception
    validate_config_params(config)


def test_validate_config_params_missing():
    """Test validate_config_params with missing parameters."""
    config = DbtRunnerConfig(
        dbt_project_zip_path=Path("/fake/path.zip"),
        storage_label="",
        source_prefix="source",
        output_prefix="output",
        debug=False,
    )
    with pytest.raises(ValidationError, match="Missing required configuration"):
        validate_config_params(config)


def test_validate_config_params_trailing_slash():
    """Test validate_config_params rejects trailing slashes."""
    config = DbtRunnerConfig(
        dbt_project_zip_path=Path("/fake/path.zip"),
        storage_label="test",
        source_prefix="source/",
        output_prefix="output",
        debug=False,
    )
    with pytest.raises(ValidationError, match="should not end with"):
        validate_config_params(config)


def test_validate_dbt_project_success():
    """Test validate_dbt_project with valid dbt project."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create a valid dbt project structure
        project_dir = tmpdir / "project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text("name: test\nversion: 1.0.0")
        (project_dir / "profiles.yml").write_text("test: {}")
        models_dir = project_dir / "models"
        models_dir.mkdir()

        # Create zip file
        zip_path = tmpdir / "project.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.write(project_dir / "dbt_project.yml", "dbt_project.yml")
            zf.write(project_dir / "profiles.yml", "profiles.yml")
            # Add models directory to zip
            zf.write(models_dir, "models/")

        # Extract and validate
        extract_dir = tmpdir / "extract"
        result = validate_dbt_project(zip_path, extract_dir)

        assert result.exists()
        assert (result / "dbt_project.yml").exists()


def test_validate_dbt_project_missing_files():
    """Test validate_dbt_project with missing required files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create zip without required files
        zip_path = tmpdir / "project.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("readme.txt", "test")

        extract_dir = tmpdir / "extract"

        with pytest.raises(ValidationError, match="Could not find dbt_project.yml"):
            validate_dbt_project(zip_path, extract_dir)


def test_validate_dbt_project_not_a_zip():
    """Test validate_dbt_project with non-zip file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create a non-zip file
        not_zip = tmpdir / "notzip.txt"
        not_zip.write_text("not a zip file")

        extract_dir = tmpdir / "extract"

        with pytest.raises(ValidationError, match="not a valid zip"):
            validate_dbt_project(not_zip, extract_dir)


def test_validate_source_data_success():
    """Test validate_source_data with valid dataset structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create valid dataset structure
        tables_dir = tmpdir / "tables" / "subjects"
        tables_dir.mkdir(parents=True)
        (tables_dir / "data.parquet").write_text("fake parquet data")

        # Should not raise exception
        validate_source_data(tmpdir)


def test_validate_source_data_missing_tables():
    """Test validate_source_data with missing tables directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        with pytest.raises(ValidationError, match="missing 'tables/' directory"):
            validate_source_data(tmpdir)


def test_validate_source_data_no_parquet():
    """Test validate_source_data with no parquet files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create tables directory but no parquet files
        (tmpdir / "tables").mkdir()

        with pytest.raises(ValidationError, match="No parquet files found"):
            validate_source_data(tmpdir)
