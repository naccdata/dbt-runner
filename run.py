#!/usr/bin/env python
"""The run script."""

import logging
import sys

from fw_gear import GearContext

from fw_gear_dbt_runner.main import run
from fw_gear_dbt_runner.parser import parse_config

log = logging.getLogger(__name__)


def main(context: GearContext) -> None:  # pragma: no cover
    """Parse gear config and run dbt runner workflow."""
    config = parse_config(context)
    exit_code = run(config, context)
    sys.exit(exit_code)


if __name__ == "__main__":  # pragma: no cover
    with GearContext() as gear_context:
        gear_context.init_logging()
        main(gear_context)
