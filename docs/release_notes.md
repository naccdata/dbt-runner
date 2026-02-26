# Release Notes

## 0.2.0

__Enhancements__:

- Upload selection is now user-configurable via dbt `meta.upload`
  instead of relying on hardcoded materialization type and glob
  patterns. Each model can declare
  `meta: {upload: "path/relative/to/target"}` to opt into upload,
  giving dbt project authors full control over what gets uploaded
- Supports any materialization type (external, table, Python models)
  as long as the model declares `meta.upload`

__Breaking Changes__:

- Models using `materialized='external'` are no longer automatically
  uploaded. All models that should be uploaded must now declare
  `meta: {upload: "<path>"}` in their config
- The fallback glob search for `*.parquet` files under `target/` has
  been removed
- The hardcoded `target/schemas/*.schema.json` upload has been
  removed. Python models must declare `meta.upload` to be uploaded

__Maintenance__:

- Removed `_find_external_model_outputs`,
  `_find_schema_json_outputs`, and
  `_parse_external_models_from_manifest` in favor of a single
  manifest-driven `_find_uploadable_outputs` function
- Added comprehensive unit tests for the new upload discovery logic
  covering string paths, non-string values, missing files, node-level
  meta, and non-model nodes

## 0.1.1 [2026-01-12]

__Fixes__:

- Fixed output file type for `.sql` files in the compiled directory to be set
  as "source code" so they open in the native editor instead of as generic
  files

__Maintenance__:

- Fixed markdown line length violations in `CONTRIBUTING.md` and
  `docs/getting_started_with_dbt.md` to comply with `markdownlint`
  88-character limit

__Documentation__:

- Updated `README.md` to recommend reaching out to Flywheel support for large
  datasets so that appropriate resources can be allocated
- Enhanced `docs/getting_started_with_dbt.md` with comprehensive dbt sources
  documentation, including benefits of using `{{ source() }}` function over
  direct `read_parquet()` calls
- Improved source data requirements section with recommended vs alternative
  approaches and common mistakes to avoid
- Added `{name}` placeholder documentation for DRY source configuration in
  `sources.yml`
- Updated all example SQL queries to use `{{ source() }}` function instead of
  direct `read_parquet()` calls

## 0.1.0 [2025-12-01]

__Enhancements__:

- Initial implementation of dbt Runner gear for executing dbt projects on
  Flywheel datasets
- Added support for `dbt-duckdb` adapter to process parquet files locally
- Implemented external storage integration using `fw-storage` and `fw-client`
  libraries for downloading source datasets and uploading transformed results
- Added comprehensive validation for dbt project structure, configuration
  parameters, and storage access
- Implemented automatic extraction and validation of dbt project zip files
- Added support for saving dbt artifacts (`manifest.json`, `run_results.json`,
  `sources.json`, `compiled/`) as gear outputs
- Implemented structured logging with numbered execution phases for easy
  progress tracking
- Added automatic creation of target subdirectories before dbt runs to prevent
  "directory does not exist" errors when models specify nested output
  locations
- Implemented subdirectory structure preservation when uploading model outputs
  to external storage, allowing organized output hierarchy
- Created comprehensive README documentation with usage examples, workflow
  diagrams, and use cases
- Added detailed "Assumptions and Limitations" documentation to README and
  getting started guide covering model output requirements, supported features,
  execution model, and known limitations

__Fixes__:

- Fixed storage upload API to use correct `set()` method instead of
  non-existent `put()` method for `fw-storage` compatibility with Google Cloud
  Storage and other backends
- Fixed test mock configuration to properly handle nested `GearContext`
  attribute access for API key retrieval

__Maintenance__:

- Set up project structure based on Flywheel gear template
- Configured `manifest.json` with required inputs (`dbt_project_zip`) and
  config parameters (`storage_label`, `source_prefix`, `output_prefix`)
- Updated Python requirement to `>=3.12`
- Added dependencies: `dbt-core>=1.9.0`, `dbt-duckdb>=1.9.0`,
  `fw-storage>=2.0.0`, `fw-client>=2.3.1`
- Added `glibc-locale-posix` to Dockerfile for VS Code Server compatibility
- Simplified `GearContext` import in `run.py` by removing unnecessary aliasing
  from `GearToolkitContext`

__Documentation__:

- Created comprehensive README with usage examples, workflow diagrams, and use
  cases
- Added getting started guide with step-by-step instructions for creating,
  testing, and deploying dbt projects
- Added `CONTRIBUTING.md` with dependency management, linting, and testing
  instructions
- Added detailed assumptions and limitations documentation covering model
  output requirements, supported features, execution model, and known
  limitations
