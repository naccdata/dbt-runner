"""Parser module to parse gear config.json."""

from pathlib import Path

from fw_gear import GearContext as GearToolkitContext

from .config import DbtRunnerConfig


def parse_config(gear_context: GearToolkitContext) -> DbtRunnerConfig:
    """Parse gear configuration and inputs.

    Args:
        gear_context: Flywheel gear context

    Returns:
        DbtRunnerConfig with all parsed configuration values
    """
    # Get configuration values
    debug = gear_context.config.opts.get("debug", False)
    storage_label = gear_context.config.opts.get("storage_label")
    source_prefix = gear_context.config.opts.get("source_prefix")
    output_prefix = gear_context.config.opts.get("output_prefix")

    # Get input file path
    dbt_project_zip_path = Path(gear_context.config.get_input_path("dbt_project_zip"))

    return DbtRunnerConfig(
        dbt_project_zip_path=dbt_project_zip_path,
        storage_label=storage_label,
        source_prefix=source_prefix,
        output_prefix=output_prefix,
        debug=debug,
    )
