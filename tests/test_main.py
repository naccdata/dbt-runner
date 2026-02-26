"""Module to test main.py"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from fw_gear import GearContext as GearToolkitContext

from fw_gear_dbt_runner.config import DbtRunnerConfig
from fw_gear_dbt_runner.main import (
    _find_uploadable_outputs,
    _load_manifest,
    _upload_external_model_outputs,
    run,
)


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
@patch("fw_gear_dbt_runner.main._load_manifest")
@patch("fw_gear_dbt_runner.main._find_uploadable_outputs")
@patch("fw_gear_dbt_runner.main.validate_source_data")
def test_run_success(
    mock_validate_source,
    mock_find_outputs,
    mock_load_manifest,
    mock_save_artifacts,
    mock_run_dbt,
    mock_storage_manager,
    mock_validate_project,
):
    """Test successful run execution."""
    # Setup mocks
    mock_validate_project.return_value = Path("/tmp/project")
    mock_run_dbt.return_value = Path("/tmp/project/target")
    mock_load_manifest.return_value = {"nodes": {}}
    mock_find_outputs.return_value = [
        (Path("/tmp/project/target/tables/model.parquet"), "tables/model.parquet")
    ]

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


# -- _load_manifest tests ----------------------------------------------------


def test_load_manifest_success(tmp_path):
    """Valid manifest.json is loaded and returned as dict."""
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"nodes": {"a": 1}}))

    result = _load_manifest(manifest_path)
    assert result == {"nodes": {"a": 1}}


def test_load_manifest_missing_file(tmp_path, caplog):
    """Missing manifest.json returns None with warning."""
    result = _load_manifest(tmp_path / "manifest.json")
    assert result is None
    assert "not found" in caplog.text


def test_load_manifest_invalid_json(tmp_path, caplog):
    """Invalid JSON returns None with warning."""
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{bad json")

    result = _load_manifest(manifest_path)
    assert result is None
    assert "Failed to parse" in caplog.text


# -- _find_uploadable_outputs tests ------------------------------------------


def test_find_uploadable_string_meta(tmp_path):
    """Model with meta.upload as string path is returned."""
    output_file = tmp_path / "target" / "tables" / "full_table.parquet"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_bytes(b"fake-parquet")

    manifest = {
        "nodes": {
            "model.proj.full_table": {
                "resource_type": "model",
                "name": "full_table",
                "config": {
                    "meta": {"upload": "tables/full_table.parquet"},
                },
                "meta": {},
            }
        }
    }

    results = _find_uploadable_outputs(manifest, tmp_path)
    assert len(results) == 1
    resolved, rel = results[0]
    assert resolved == output_file
    assert rel == "tables/full_table.parquet"


def test_find_uploadable_non_string_meta_skipped(tmp_path, caplog):
    """Model with meta.upload as boolean logs a warning and is skipped."""
    manifest = {
        "nodes": {
            "model.proj.bad_model": {
                "resource_type": "model",
                "name": "bad_model",
                "config": {
                    "meta": {"upload": True},
                },
                "meta": {},
            }
        }
    }

    results = _find_uploadable_outputs(manifest, tmp_path)
    assert results == []
    assert "expected a string" in caplog.text


def test_find_uploadable_no_meta_key(tmp_path):
    """Model without meta.upload is skipped entirely."""
    manifest = {
        "nodes": {
            "model.proj.plain": {
                "resource_type": "model",
                "name": "plain",
                "config": {"meta": {}},
                "meta": {},
            }
        }
    }

    results = _find_uploadable_outputs(manifest, tmp_path)
    assert results == []


def test_find_uploadable_missing_file(tmp_path, caplog):
    """Model with meta.upload pointing to a missing file logs a warning."""
    manifest = {
        "nodes": {
            "model.proj.ghost": {
                "resource_type": "model",
                "name": "ghost",
                "config": {
                    "meta": {"upload": "does_not_exist.parquet"},
                },
                "meta": {},
            }
        }
    }

    results = _find_uploadable_outputs(manifest, tmp_path)
    assert results == []
    assert "file not found" in caplog.text


def test_find_uploadable_no_models(tmp_path):
    """Empty manifest produces no outputs and no error."""
    results = _find_uploadable_outputs({"nodes": {}}, tmp_path)
    assert results == []


def test_find_uploadable_meta_at_node_level(tmp_path):
    """meta.upload found at the node level (not inside config) works."""
    output_file = tmp_path / "target" / "schemas" / "t.schema.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text("{}")

    manifest = {
        "nodes": {
            "model.proj.schema_model": {
                "resource_type": "model",
                "name": "schema_model",
                "config": {"meta": {}},
                "meta": {"upload": "schemas/t.schema.json"},
            }
        }
    }

    results = _find_uploadable_outputs(manifest, tmp_path)
    assert len(results) == 1
    assert results[0][1] == "schemas/t.schema.json"


def test_find_uploadable_skips_non_model_nodes(tmp_path):
    """Nodes that are not models (e.g. tests, sources) are ignored."""
    output_file = tmp_path / "target" / "out.parquet"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_bytes(b"data")

    manifest = {
        "nodes": {
            "test.proj.my_test": {
                "resource_type": "test",
                "name": "my_test",
                "config": {"meta": {"upload": "out.parquet"}},
                "meta": {},
            }
        }
    }

    results = _find_uploadable_outputs(manifest, tmp_path)
    assert results == []


# -- _upload_external_model_outputs tests ------------------------------------


@patch("fw_gear_dbt_runner.main._find_uploadable_outputs")
@patch("fw_gear_dbt_runner.main._load_manifest")
def test_upload_nothing_when_no_outputs(mock_load, mock_find, tmp_path):
    """No upload calls when _find_uploadable_outputs returns empty."""
    mock_load.return_value = {"nodes": {}}
    mock_find.return_value = []
    storage = MagicMock()

    _upload_external_model_outputs(tmp_path / "target", tmp_path, storage, "output/")

    storage.upload_file.assert_not_called()


@patch("fw_gear_dbt_runner.main._find_uploadable_outputs")
@patch("fw_gear_dbt_runner.main._load_manifest")
def test_upload_calls_storage_for_each_output(mock_load, mock_find, tmp_path):
    """Each uploadable output triggers a storage upload."""
    mock_load.return_value = {"nodes": {}}
    mock_find.return_value = [
        (Path("/a/b.parquet"), "tables/b.parquet"),
        (Path("/a/c.json"), "schemas/c.schema.json"),
    ]
    storage = MagicMock()

    _upload_external_model_outputs(tmp_path / "target", tmp_path, storage, "out/")

    assert storage.upload_file.call_count == 2
    storage.upload_file.assert_any_call(
        Path("/a/b.parquet"), "out/", "tables/b.parquet"
    )
    storage.upload_file.assert_any_call(
        Path("/a/c.json"), "out/", "schemas/c.schema.json"
    )
