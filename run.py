#!/usr/bin/env python
"""The run script."""

import logging
import sys
from pathlib import Path

from fw_gear import GearContext

from fw_gear_dbt_runner.main import run
from fw_gear_dbt_runner.parser import parse_config

log = logging.getLogger(__name__)


def main(context: GearContext) -> None:  # pragma: no cover
    """Parse gear config and run dbt runner workflow."""
    config = parse_config(context)
    exit_code = run(config, context)

    # Set file.type for .sql files in the compiled directory
    if exit_code == 0:
        output_path = Path(context.output_dir)
        compiled_dir = output_path / "compiled"

        if compiled_dir.exists():
            sql_files = list(compiled_dir.rglob("*.sql"))
            for sql_file in sql_files:
                relative_path = sql_file.relative_to(output_path)
                try:
                    context.metadata.update_file_metadata(
                        str(relative_path),
                        container_type="analysis",
                        type="source code",
                    )
                    log.info(f"Set file type for {relative_path}")
                except Exception as e:
                    log.warning(f"Failed to set metadata for {relative_path}: {e}")

    sys.exit(exit_code)


if __name__ == "__main__":  # pragma: no cover
    with GearContext() as gear_context:
        gear_context.init_logging()
        main(gear_context)
