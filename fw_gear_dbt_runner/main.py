"""Main module for dbt runner gear."""

import json
import logging
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import List

from fw_gear import GearContext as GearToolkitContext

from .config import DbtRunnerConfig
from .storage import StorageManager
from .validation import (
    ValidationError,
    validate_config_params,
    validate_dbt_project,
    validate_source_data,
)

log = logging.getLogger(__name__)


def run(config: DbtRunnerConfig, gear_context: GearToolkitContext) -> int:
    """Execute dbt runner gear workflow.

    Args:
        config: Parsed gear configuration
        gear_context: Flywheel gear context

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    log.info("=" * 80)
    log.info("dbt Runner Gear - Starting execution")
    log.info("=" * 80)

    # Define working directories
    work_dir = Path(gear_context.work_dir)
    dbt_extract_dir = work_dir / "dbt_project"
    source_data_dir = work_dir / "source_data"
    work_dir / "output"

    try:
        # Step 1: Validate configuration parameters
        log.info("\n[1/7] Validating configuration parameters")
        validate_config_params(config)

        # Step 2: Validate and extract dbt project
        log.info("\n[2/7] Validating and extracting dbt project")
        project_root = validate_dbt_project(
            config.dbt_project_zip_path, dbt_extract_dir
        )
        log.info(f"dbt project root: {project_root}")

        # Step 3: Initialize storage manager and verify access
        log.info("\n[3/7] Initializing storage client")
        api_key = gear_context.config.inputs.get("api-key").get("key")
        storage_manager = StorageManager(api_key, config.storage_label)
        storage_manager.verify_access(config.source_prefix)

        # Step 4: Download source dataset
        log.info("\n[4/7] Downloading source dataset from external storage")
        storage_manager.download_dataset(config.source_prefix, source_data_dir)

        # Validate source data structure
        validate_source_data(source_data_dir)

        # Step 5: Run dbt
        log.info("\n[5/7] Executing dbt run")
        dbt_target_dir = _run_dbt(project_root)

        # Step 6: Upload results to external storage
        log.info("\n[6/7] Uploading results to external storage")
        _upload_external_model_outputs(
            dbt_target_dir, project_root, storage_manager, config.output_prefix
        )

        # Step 7: Save dbt artifacts as gear outputs
        log.info("\n[7/7] Saving dbt artifacts")
        _save_dbt_artifacts(dbt_target_dir, Path(gear_context.output_dir))

        log.info("\n" + "=" * 80)
        log.info("dbt Runner Gear - Completed successfully")
        log.info("=" * 80)
        return 0

    except ValidationError as e:
        log.error(f"\nValidation Error: {str(e)}", exc_info=config.debug)
        return 1
    except subprocess.CalledProcessError as e:
        log.error(f"\ndbt execution failed with exit code {e.returncode}")
        log.error(f"Command: {e.cmd}")
        if e.stdout:
            log.error(f"stdout:\n{e.stdout}")
        if e.stderr:
            log.error(f"stderr:\n{e.stderr}")
        return e.returncode
    except Exception as e:
        log.error(f"\nUnexpected error: {str(e)}", exc_info=True)
        return 1


def _create_model_output_directories(project_root: Path) -> None:
    """Create output directories for models with external materialization.

    Scans dbt model files for location configurations and creates
    necessary subdirectories to prevent "directory does not exist" errors.

    Args:
        project_root: Root directory of the dbt project
    """
    models_dir = project_root / "models"
    if not models_dir.exists():
        return

    # Pattern to match location in config blocks
    location_pattern = re.compile(r"location\s*=\s*['\"]([^'\"]+)['\"]")

    log.info("Scanning model files for output locations")
    locations_found = set()

    # Recursively find all .sql files
    for sql_file in models_dir.rglob("*.sql"):
        try:
            with open(sql_file, "r") as f:
                content = f.read()

            # Find all location configurations
            matches = location_pattern.findall(content)
            for location in matches:
                locations_found.add(location)

        except Exception as e:
            log.debug(f"Could not read {sql_file}: {e}")

    # Create parent directories for all locations
    for location in locations_found:
        location_path = Path(location)
        if not location_path.is_absolute():
            # Resolve relative to project root
            location_path = project_root / location_path

        # Create parent directory
        parent_dir = location_path.parent
        if parent_dir != project_root:
            parent_dir.mkdir(parents=True, exist_ok=True)
            log.info(
                f"Created output directory: {parent_dir.relative_to(project_root)}"
            )


def _run_dbt(project_root: Path) -> Path:
    """Execute dbt run command.

    Args:
        project_root: Root directory of the dbt project

    Returns:
        Path to the target directory containing dbt outputs

    Raises:
        subprocess.CalledProcessError: If dbt command fails
    """
    log.info(f"Running dbt from: {project_root}")

    # Change to project directory
    original_dir = Path.cwd()
    os.chdir(project_root)

    try:
        # Ensure target directory exists
        target_dir = project_root / "target"
        target_dir.mkdir(parents=True, exist_ok=True)
        log.info(f"Ensured target directory exists: {target_dir}")

        # Create subdirectories for model outputs
        _create_model_output_directories(project_root)

        # Run dbt debug first to check configuration
        log.info("Running dbt debug to verify configuration")
        result = subprocess.run(
            ["dbt", "debug"], capture_output=True, text=True, check=False
        )
        log.info(f"dbt debug output:\n{result.stdout}")
        if result.returncode != 0:
            log.warning(f"dbt debug had warnings:\n{result.stderr}")

        # Run dbt run
        log.info("Running dbt run")
        result = subprocess.run(
            ["dbt", "run"], capture_output=True, text=True, check=True
        )

        # Log output
        log.info(f"dbt run output:\n{result.stdout}")
        if result.stderr:
            log.info(f"dbt run stderr:\n{result.stderr}")

        target_dir = project_root / "target"
        return target_dir

    finally:
        # Always change back to original directory
        os.chdir(original_dir)


def _save_dbt_artifacts(dbt_target_dir: Path, gear_output_dir: Path) -> None:
    """Save dbt artifacts to gear output directory.

    Args:
        dbt_target_dir: Directory containing dbt target outputs
        gear_output_dir: Gear output directory
    """
    log.info("Saving dbt artifacts to gear outputs")

    gear_output_path = Path(gear_output_dir)
    gear_output_path.mkdir(parents=True, exist_ok=True)

    # List of artifacts to save
    artifacts = [
        "manifest.json",
        "run_results.json",
        "sources.json",
        "compiled",
    ]

    for artifact in artifacts:
        artifact_path = dbt_target_dir / artifact

        if artifact_path.exists():
            dest_path = gear_output_path / artifact

            if artifact_path.is_dir():
                # Copy directory
                shutil.copytree(artifact_path, dest_path, dirs_exist_ok=True)
                log.info(f"Saved artifact directory: {artifact}")
            else:
                # Copy file
                shutil.copy2(artifact_path, dest_path)
                log.info(f"Saved artifact: {artifact}")
        else:
            log.debug(f"Artifact not found (skipping): {artifact}")

    log.info("dbt artifacts saved successfully")


def _parse_external_models_from_manifest(manifest_path: Path) -> List[dict]:
    """Parse manifest.json to extract external model configurations.

    Args:
        manifest_path: Path to manifest.json

    Returns:
        List of dicts with 'name' and 'location' keys for external models
    """
    try:
        with open(manifest_path, "r") as f:
            manifest = json.load(f)

        log.info("Reading manifest.json to find external model outputs")
        nodes = manifest.get("nodes", {})
        external_models = []

        for node_data in nodes.values():
            if (
                node_data.get("resource_type") == "model"
                and node_data.get("config", {}).get("materialized") == "external"
            ):
                location = node_data.get("config", {}).get("location")
                if location:
                    external_models.append(
                        {"name": node_data.get("name"), "location": location}
                    )

        log.info(f"Found {len(external_models)} external models in manifest")
        return external_models

    except FileNotFoundError:
        log.warning(f"manifest.json not found at {manifest_path}")
    except json.JSONDecodeError as e:
        log.warning(f"Failed to parse manifest.json: {e}")
    except Exception as e:
        log.warning(f"Error reading manifest.json: {e}")

    return []


def _resolve_model_path(location: str, project_root: Path) -> Path:
    """Resolve model location to an absolute path.

    Args:
        location: Location from manifest (relative or absolute)
        project_root: Root directory of the dbt project

    Returns:
        Absolute path to the model output
    """
    if Path(location).is_absolute():
        return Path(location)
    return project_root / location


def _find_external_model_outputs(
    dbt_target_dir: Path, project_root: Path
) -> List[Path]:
    """Find all external model outputs by reading manifest.json.

    This function reads the dbt manifest to find models with external
    materialization and returns paths to their output files.

    Args:
        dbt_target_dir: Directory containing dbt target outputs
        project_root: Root directory of the dbt project

    Returns:
        List of paths to parquet files created by external models
    """
    manifest_path = dbt_target_dir / "manifest.json"
    parquet_files = []

    # Parse external models from manifest
    external_models = _parse_external_models_from_manifest(manifest_path)

    # Convert locations to absolute paths and verify they exist
    for model in external_models:
        parquet_path = _resolve_model_path(model["location"], project_root)

        if parquet_path.exists():
            parquet_files.append(parquet_path)
            log.info(f"Found external model output: {model['name']} at {parquet_path}")
        else:
            log.warning(
                f"External model {model['name']} location not found: {parquet_path}"
            )

    # Fallback: recursively find all parquet files under target/
    if not parquet_files:
        log.info("Falling back to recursive parquet file search in target directory")
        for parquet_file in dbt_target_dir.rglob("*.parquet"):
            if not parquet_file.name.endswith(".duckdb"):
                parquet_files.append(parquet_file)
                log.info(f"Found parquet file: {parquet_file}")

    return parquet_files


def _upload_external_model_outputs(
    dbt_target_dir: Path,
    project_root: Path,
    storage_manager: StorageManager,
    output_prefix: str,
) -> None:
    """Upload external model outputs to storage preserving subdirectory structure.

    Args:
        dbt_target_dir: Directory containing dbt target outputs
        project_root: Root directory of the dbt project
        storage_manager: Storage manager instance for uploads
        output_prefix: Path prefix in storage where files will be written
    """
    parquet_files = _find_external_model_outputs(dbt_target_dir, project_root)

    if parquet_files:
        log.info(f"Found {len(parquet_files)} parquet file(s) to upload")
        for parquet_file in parquet_files:
            # Calculate relative path to preserve subdirectory structure
            try:
                # Try to get path relative to target directory
                relative_path = parquet_file.relative_to(dbt_target_dir)
            except ValueError:
                # If file is not under target dir, use relative to project root
                try:
                    relative_path = parquet_file.relative_to(project_root)
                except ValueError:
                    # Fallback to just the filename
                    relative_path = parquet_file.name

            log.info(f"Uploading {relative_path} to external storage")
            storage_manager.upload_file(parquet_file, output_prefix, str(relative_path))
    else:
        log.warning("No external model outputs found to upload")
