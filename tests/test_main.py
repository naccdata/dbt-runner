"""Module to test main.py"""

from pathlib import Path
from unittest.mock import MagicMock, patch

from fw_gear import GearContext as GearToolkitContext

from fw_gear_dbt_runner.config import DbtRunnerConfig
from fw_gear_dbt_runner.main import run


def test_run_validation_error():
    """Test run handles validation errors correctly."""
    # Create mock config with missing required params
    config = DbtRunnerConfig(
        dbt_project_zip_path=Path("/tmp/test.zip"),
        storage_label="",  # Empty should cause validation error
        source_prefix="source",
        output_prefix="output",
        debug=False,
    )

    gear_context = MagicMock(spec=GearToolkitContext)
    gear_context.work_dir = "/tmp/work"

    # Run should return non-zero exit code
    exit_code = run(config, gear_context)

    assert exit_code == 1


@patch("fw_gear_dbt_runner.main.validate_dbt_project")
@patch("fw_gear_dbt_runner.main.StorageManager")
@patch("fw_gear_dbt_runner.main._run_dbt")
@patch("fw_gear_dbt_runner.main._save_dbt_artifacts")
@patch("fw_gear_dbt_runner.main._find_external_model_outputs")
@patch("fw_gear_dbt_runner.main.validate_source_data")
def test_run_success(
    mock_validate_source,
    mock_find_outputs,
    mock_save_artifacts,
    mock_run_dbt,
    mock_storage_manager,
    mock_validate_project,
):
    """Test successful run execution."""
    # Setup mocks
    mock_validate_project.return_value = Path("/tmp/project")
    mock_run_dbt.return_value = Path("/tmp/project/target")
    mock_find_outputs.return_value = [Path("/tmp/project/target/main/model.parquet")]

    mock_storage = MagicMock()
    mock_storage_manager.return_value = mock_storage

    # Create config
    config = DbtRunnerConfig(
        dbt_project_zip_path=Path("/tmp/test.zip"),
        storage_label="test-storage",
        source_prefix="source",
        output_prefix="output",
        debug=False,
    )

    gear_context = MagicMock()
    gear_context.work_dir = "/tmp/work"
    gear_context.output_dir = "/tmp/output"
    # Mock the nested config.inputs.get() call
    api_key_mock = MagicMock()
    api_key_mock.get.return_value = "test-api-key"
    gear_context.config.inputs.get.return_value = api_key_mock

    # Run
    exit_code = run(config, gear_context)

    # Should return success
    assert exit_code == 0

    # Verify key functions were called
    mock_validate_project.assert_called_once()
    mock_storage_manager.assert_called_once_with("test-api-key", "test-storage")
    mock_run_dbt.assert_called_once()
    mock_find_outputs.assert_called_once()
    mock_save_artifacts.assert_called_once()
    # Verify upload was called
    mock_storage.upload_file.assert_called_once()
