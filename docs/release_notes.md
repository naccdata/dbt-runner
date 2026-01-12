# Release Notes

## Version 0.1.1-rc1

### Bug Fixes

- Fixed output file type for .sql files in the compiled directory to be set as
  "source code" so they open in the native editor instead of as generic files

### Maintenance

- Fixed markdown line length violations in CONTRIBUTING.md and
  docs/getting_started_with_dbt.md to comply with markdownlint 80-character limit

## Version 0.1.0-rc1

### Enhancements

- Added automatic creation of target subdirectories before dbt runs to prevent
  "directory does not exist" errors when models specify nested output locations
- Implemented subdirectory structure preservation when uploading model outputs to
  external storage, allowing organized output hierarchy
- Added comprehensive "Assumptions and Limitations" documentation to README and
  getting started guide covering model output requirements, supported features,
  execution model, and known limitations
- Initial implementation of dbt Runner gear for executing dbt projects on Flywheel
  datasets
- Added support for dbt-duckdb adapter to process parquet files locally
- Implemented external storage integration using fw-storage and fw-client libraries
  for downloading source datasets and uploading transformed results
- Added comprehensive validation for dbt project structure, configuration parameters,
  and storage access
- Implemented automatic extraction and validation of dbt project zip files
- Added support for saving dbt artifacts (manifest.json, run_results.json,
  sources.json, compiled/) as gear outputs
- Implemented structured logging with numbered execution phases for easy progress
  tracking
- Added detailed error handling with specific error messages for common failure
  scenarios
- Created comprehensive README documentation with usage examples, workflow diagrams,
  and use cases

### Bug Fixes

- Fixed storage upload API to use correct `set()` method instead of non-existent
  `put()` method for fw-storage compatibility with Google Cloud Storage and other
  backends
- Fixed test mock configuration to properly handle nested GearContext attribute
  access for API key retrieval

### Maintenance

- Simplified GearContext import in run.py by removing unnecessary aliasing from
  GearToolkitContext
- Updated .gitignore to exclude config files and zip artifacts from version control
- Updated .gitlab-ci.yml to disable DEBUG mode for cleaner CI output
- Added glibc-locale-posix to Dockerfile for VS Code Server compatibility

- Set up project structure based on Flywheel gear template
- Added dependencies: dbt-core >=1.9.0, dbt-duckdb >=1.9.0, fw-storage >=2.0.0,
  fw-client >=2.3.1
- Updated Python requirement to >=3.12
- Configured manifest.json with required inputs (dbt_project_zip) and config
  parameters (storage_label, source_prefix, output_prefix)
