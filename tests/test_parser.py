"""Module to test parser.py"""

from pathlib import Path
from unittest.mock import MagicMock

from fw_gear import GearContext as GearToolkitContext

from fw_gear_dbt_runner.config import DbtRunnerConfig
from fw_gear_dbt_runner.parser import parse_config


def test_parse_config():
    """Test parse_config extracts all configuration values correctly."""
    # Create mock gear context
    gear_context = MagicMock(spec=GearToolkitContext)

    # Mock config attribute
    mock_config = MagicMock()
    gear_context.config = mock_config

    # Mock config values
    mock_config.opts.get = MagicMock(
        side_effect=lambda key, default=None: {
            "debug": True,
            "storage_label": "test-storage",
            "source_prefix": "path/to/source",
            "output_prefix": "path/to/output",
        }.get(key, default)
    )

    # Mock input path
    mock_config.get_input_path = MagicMock(
        return_value="/flywheel/v0/input/dbt_project_zip/project.zip"
    )

    # Call parse_config
    config = parse_config(gear_context)

    # Assert all values are extracted correctly
    assert isinstance(config, DbtRunnerConfig)
    assert config.debug is True
    assert config.storage_label == "test-storage"
    assert config.source_prefix == "path/to/source"
    assert config.output_prefix == "path/to/output"
    assert config.dbt_project_zip_path == Path(
        "/flywheel/v0/input/dbt_project_zip/project.zip"
    )

    # Verify method calls
    mock_config.get_input_path.assert_called_once_with("dbt_project_zip")


def test_parse_config_defaults():
    """Test parse_config handles default values correctly."""
    gear_context = MagicMock(spec=GearToolkitContext)

    # Mock config attribute
    mock_config = MagicMock()
    gear_context.config = mock_config

    # Mock config with only required values
    mock_config.opts.get = MagicMock(
        side_effect=lambda key, default=None: {
            "storage_label": "test-storage",
            "source_prefix": "source",
            "output_prefix": "output",
        }.get(key, default)
    )

    mock_config.get_input_path = MagicMock(return_value="/input/project.zip")

    config = parse_config(gear_context)

    # Debug should default to False
    assert config.debug is False
