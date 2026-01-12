# Getting Started Guide: dbt Runner Gear

This guide walks you through creating a simple dbt project, testing it locally,
and deploying it with the dbt-runner gear.

## Prerequisites

- Python 3.12+
- Access to a Flywheel dataset exported to external storage
- dbt-core and dbt-duckdb installed locally (`pip install dbt-core dbt-duckdb`)

## Step 1: Create a Simple dbt Project

### 1.1 Initialize a new dbt project

```bash
mkdir my_flywheel_project
cd my_flywheel_project
```

### 1.2 Create the project structure

```bash
mkdir -p models/staging models/marts macros tests seeds
```

### 1.3 Create `dbt_project.yml`

```yaml
name: 'my_flywheel_project'
version: '1.0.0'
config-version: 2

profile: 'my_flywheel_project'

model-paths: ["models"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  my_flywheel_project:
    staging:
      +materialized: view
    marts:
      +materialized: external
```

**Note:** The `external` materialization writes models directly to parquet files
instead of storing them in the DuckDB database. We set `+materialized: external`
at the project level for marts, but each model should also specify its output
`location` using `{{ config() }}` in the SQL file (see example in step 1.8).
This ensures parquet files are written to the `target/` directory in an
organized way.

### 1.4 Create `profiles.yml`

```yaml
my_flywheel_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'target/dev.duckdb'
      threads: 4
      extensions:
        - parquet
```

### 1.5 Create the target directory

DuckDB requires the target directory to exist before creating the database file:

```bash
mkdir -p target/main
```

### 1.6 Create a sources configuration (`models/sources.yml`)

dbt sources allow you to document and reference your raw data tables. With the
dbt-duckdb adapter, the `external_location` in metadata tells dbt where to find
the parquet files, and you can reference them using `{{ source() }}` in your models.

```yaml
version: 2

sources:
  - name: flywheel_raw
    description: Raw Flywheel dataset tables
    meta:
      external_location: "../source_data/tables/{name}/*.parquet"

    tables:
      - name: subjects
        description: Subject-level data

      - name: sessions
        description: Session-level data

      - name: files
        description: File metadata
```

**Note:** The `external_location` at the source level uses `{name}` as a
placeholder that gets replaced with each table name. This keeps the configuration
DRY. The `../` prefix references the parent directory where `source_data/` is
located, matching the gear's directory structure.

**Benefits of using sources:**

- **Documentation**: Central place to document raw data tables
- **Lineage**: dbt tracks how data flows from sources to models
- **Testing**: Add data quality tests on source tables
- **Maintainability**: Update paths in one place instead of across multiple models
- **Alternative**: You can also use direct `read_parquet()` calls in your SQL,
  but sources are the recommended dbt best practice

### 1.7 Create a simple staging model (`models/staging/stg_subjects.sql`)

```sql
-- Stage subjects data with basic transformations

SELECT
    id AS subject_id,
    label AS subject_label,
    "parents.project" AS project_id,
    "parents.group" AS group_id,
    created,
    modified
FROM {{ source('flywheel_raw', 'subjects') }}
```

**Note:** The `{{ source('flywheel_raw', 'subjects') }}` function references the
source defined in `sources.yml`. dbt-duckdb uses the `external_location` metadata
to generate a `read_parquet()` call automatically. This approach provides better
documentation, lineage tracking, and allows you to add source tests.

### 1.8 Create a mart model (`models/marts/subject_summary.sql`)

```sql
{{
    config(
        materialized='external',
        location='target/main/subject_summary.parquet'
    )
}}

-- Create summary table of subjects

WITH subject_sessions AS (
    SELECT
        "parents.subject" AS subject_id,
        COUNT(*) AS session_count
    FROM {{ source('flywheel_raw', 'sessions') }}
    GROUP BY "parents.subject"
)

SELECT
    s.subject_id,
    s.subject_label,
    s.project_id,
    s.group_id,
    COALESCE(ss.session_count, 0) AS total_sessions,
    s.created,
    s.modified
FROM {{ ref('stg_subjects') }} s
LEFT JOIN subject_sessions ss ON s.subject_id = ss.subject_id
```

**Note:** The `{{ config() }}` block at the top specifies that this model
should be materialized as an external parquet file at the specified location.
Each model that should output to parquet needs its own config block with the
full path.

## Step 2: Test Locally

### 2.1 Download sample data from external storage

You can use the Flywheel SDK or API to download a small sample of your dataset
to test locally.

#### IMPORTANT: Directory Structure Requirement

The `source_data/` directory **must be in the parent folder** of your dbt project,
not inside it. This matches how the gear organizes directories:

```text
parent_folder/                    # Your workspace
├── my_flywheel_project/          # Your dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
└── source_data/                  # Data goes here (sibling to project)
    └── tables/
        ├── subjects/
        │   └── subjects.parquet
        ├── sessions/
        │   └── sessions.parquet
        └── files/
            └── files.parquet
```

Your dbt models will reference data using `../source_data/` (relative path to parent).

**Example Python script to download data:**

```python
import os
from pathlib import Path
from fw_client import FWClient
from fw_storage import create_storage_client

# Configuration
api_key = os.environ.get("FLYWHEEL_API_KEY")
storage_label = "GCP Project Data"
source_prefix = "datasets/project-123/export-2024-01"

# Initialize clients
client = FWClient(api_key=api_key)
storages = client.get("/xfer/storages")
storage = [s for s in storages["results"] if s["label"] == storage_label][0]
storage_creds = client.get(f"/xfer/storage-creds/{storage['_id']}")
storage_client = create_storage_client(storage_creds['url'])

# Download to source_data directory in parent folder (same as gear uses)
# Create source_data as sibling to your dbt project
local_dir = Path("../source_data")
local_dir.mkdir(parents=True, exist_ok=True)

for file_info in storage_client.ls(source_prefix):
    relative_path = file_info.path.replace(f"{source_prefix}/", "")
    local_path = local_dir / relative_path
    local_path.parent.mkdir(parents=True, exist_ok=True)

    with storage_client.get(file_info.path) as remote_file:
        with open(local_path, "wb") as local_file:
            local_file.write(remote_file.read())
```

### 2.2 Test dbt configuration

```bash
dbt debug
```

This should show all connections passing:

```text
Connection test: [OK connection ok]
All checks passed!
```

### 2.3 Run dbt models

```bash
dbt run
```

Verify that your models compile and execute successfully:

```text
Running with dbt=1.9.0
Found 2 models, 0 tests, 0 sources, 0 exposures, 0 metrics

Completed successfully

Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
```

### 2.4 Inspect the results

Your models are now written as parquet files in the `target/` directory, organized
by schema (e.g., `target/main/subject_summary.parquet`).

View the parquet files:

```bash
ls -lh target/main/
```

Query the parquet files using DuckDB:

```bash
duckdb
```

```sql
-- Query the parquet file directly
SELECT * FROM 'target/main/subject_summary.parquet' LIMIT 10;
.exit
```

Or use dbt show (which reads from the parquet file):

```bash
dbt show --select subject_summary --limit 10
```

### 2.5 Run tests (optional)

Add schema tests to your models in `models/schema.yml`:

```yaml
version: 2

models:
  - name: subject_summary
    description: Summary of subjects with session counts
    columns:
      - name: subject_id
        description: Unique subject identifier
        tests:
          - unique
          - not_null
      - name: total_sessions
        description: Number of sessions for the subject
        tests:
          - not_null
```

Then run tests:

```bash
dbt test
```

## Step 3: Create Zip File

### 3.1 Create the zip file

```bash
cd my_flywheel_project
zip -r ../my_flywheel_project.zip . \
  -x "*.git*" \
  -x "target/*" \
  -x "dbt_packages/*" \
  -x "logs/*" \
  -x "source_data/*" \
  -x ".user.yml" \
  -x "*.pyc" \
  -x "__pycache__/*"
```

**Important:** Ensure the zip contains:

- ✅ `dbt_project.yml`
- ✅ `profiles.yml`
- ✅ `models/` directory with all model files
- ✅ Any macros, seeds, or tests you want to include
- ❌ Do NOT include `target/`, `logs/`, or `source_data/` directories

### 3.2 Verify zip contents

```bash
unzip -l my_flywheel_project.zip
```

Should show:

```text
Archive:  my_flywheel_project.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
      123  2024-01-15 10:30   dbt_project.yml
       89  2024-01-15 10:30   profiles.yml
      456  2024-01-15 10:32   models/sources.yml
      234  2024-01-15 10:35   models/staging/stg_subjects.sql
      345  2024-01-15 10:40   models/marts/subject_summary.sql
        0  2024-01-15 10:30   macros/
        0  2024-01-15 10:30   tests/
---------                     -------
     1247                     7 files
```

## Step 4: Execute with dbt-runner Gear

### 4.1 Upload zip file to Flywheel

1. Log in to your Flywheel instance
2. Navigate to any container (Project, Subject, Session, or create an Analysis)
3. Click "Upload" or attach a file
4. Upload `my_flywheel_project.zip`

### 4.2 Configure and run the gear

1. Navigate to the "Run Gear" menu
2. Select **"dbt Runner"** gear
3. Configure inputs:
   - **dbt_project_zip**: Select your uploaded `my_flywheel_project.zip` file
4. Configure parameters:
   - **storage_label**: Label of your external storage
     (e.g., `"GCP Project Data"`)
   - **source_prefix**: Path to source dataset without trailing slash
     (e.g., `"datasets/project-123/export-2024-01"`)
   - **output_prefix**: Path for transformed outputs without trailing slash
     (e.g., `"datasets/project-123/transformed"`)
   - **debug**: `false` (set to `true` for detailed debugging)
5. Click "Run Gear"

### 4.3 Monitor execution

Watch the gear logs to see progress through the 7 phases:

```text
================================================================================
dbt Runner Gear - Starting execution
================================================================================

[1/7] Validating configuration parameters
Configuration parameters validation successful

[2/7] Validating and extracting dbt project
dbt project validation successful

[3/7] Initializing storage client
Storage client initialized successfully

[4/7] Downloading source dataset from external storage
Found 28 files to download
Dataset downloaded successfully

[5/7] Configuring dbt project
Source data directory: /flywheel/v0/work/source_data

[6/7] Executing dbt run
Running dbt from: /flywheel/v0/work/dbt_project
Completed successfully

[7/7] Uploading results to external storage
Uploading 2 files
Results uploaded successfully

================================================================================
dbt Runner Gear - Completed successfully
================================================================================
```

### 4.4 Review outputs

**Gear outputs (in Analysis container):**

- `manifest.json` - Complete dbt project manifest with model metadata
- `run_results.json` - Detailed results from the dbt run execution
- `sources.json` - Source metadata
- `compiled/` - Compiled SQL for all models

**External storage:**

- Navigate to the `output_prefix` location in your external storage
- The gear uploads all parquet files from the `target/` directory (organized by schema)
- Each model materialized with `external` creates a separate parquet file

**Example output structure:**

```text
datasets/project-123/transformed/
├── subject_summary.parquet
└── other_model.parquet
```

These parquet files contain your transformed data and can be:

- Read by other analytics tools
- Queried with DuckDB, Spark, or other engines
- Used as inputs for downstream workflows
- Shared with other teams or systems

## Assumptions and Limitations

Understanding the gear's assumptions and limitations will help you design effective
dbt projects and avoid common pitfalls.

### Model Output Location

**Requirement**: All external models must output to the `target/` directory.

- ✅ **Correct**: `location='target/main/model_name.parquet'`
- ❌ **Incorrect**: `location='output/model_name.parquet'`
- ❌ **Incorrect**: `location='/absolute/path/model_name.parquet'`

The gear only uploads parquet files found under the `target/` directory. Models
outputting to other locations will not be uploaded to external storage.

### Subfolder Structure Preservation

**Behavior**: Subdirectory structure under `target/` is preserved when uploading.

**Example**:

```sql
-- Model A
{{ config(location='target/staging/stg_subjects.parquet') }}

-- Model B
{{ config(location='target/marts/subject_summary.parquet') }}
```

**Uploads to external storage as**:

```text
{output_prefix}/
├── staging/
│   └── stg_subjects.parquet
└── marts/
    └── subject_summary.parquet
```

This allows you to organize your outputs by schema or category while maintaining
that structure in the destination bucket.

### Directory Auto-Creation

**Feature**: Target subdirectories are automatically created before dbt runs.

Before executing `dbt run`, the gear:

1. Scans all `.sql` files in the `models/` directory
2. Extracts `location='...'` configurations
3. Creates parent directories for all specified locations

**Why this matters**: DuckDB requires parent directories to exist before writing
parquet files. Without this feature, models outputting to `target/main/model.parquet`
would fail if `target/main/` didn't exist.

### Materialization Types

**Only external models are uploaded**: Models must use `materialized='external'`
to be uploaded to external storage.

**Comparison**:

| Materialization | Stored In       | Uploaded to Storage? |
| --------------- | --------------- | -------------------- |
| `view`          | DuckDB database | No                   |
| `table`         | DuckDB database | No                   |
| `external`      | Parquet file    | **Yes**              |
| `incremental`   | Not supported   | No                   |

**Best practice**: Use `view` for intermediate transformations and `external` for
final outputs you want to persist in external storage.

### Required Configuration

**Each external model needs explicit location**:

```sql
{{
    config(
        materialized='external',
        location='target/schema/model_name.parquet'
    )
}}
```

Without the `location` parameter, dbt-duckdb will choose a default location that
may not be under `target/`, and the gear won't upload the output.

### Source Data Requirements

**Recommended approach**: Use `{{ source() }}` function with sources defined in
`sources.yml`. This provides better documentation, lineage tracking, and testing:

```sql
-- Recommended: Use dbt sources
FROM {{ source('flywheel_raw', 'subjects') }}
```

**Alternative approach**: Direct `read_parquet()` calls also work but require
manual path management:

```sql
-- Also works: Direct parquet reading
FROM read_parquet('../source_data/tables/subjects/*.parquet')
```

**Path requirements**: Whether using sources or direct calls, all paths must:

- Use `../source_data/` prefix to reference the sibling directory
- Match the gear's directory structure:

```text
work/
├── dbt_project/          # Your dbt project (extracted from zip)
└── source_data/          # Downloaded dataset (sibling to project)
```

**Common mistakes**:

```sql
-- Incorrect (missing ../)
FROM read_parquet('source_data/tables/subjects/*.parquet')

-- Incorrect (absolute paths)
FROM read_parquet('/absolute/path/subjects/*.parquet')
```

**Local testing**: Replicate this structure locally by placing `source_data/`
as a sibling to your dbt project directory.

### Supported Adapter

**Only dbt-duckdb is supported**. The gear:

- Uses DuckDB for local processing of parquet files
- Does not connect to cloud warehouses (Snowflake, BigQuery, Redshift)
- Does not support other SQL engines (Postgres, MySQL, Spark SQL)

**Why DuckDB**: It's lightweight, fast for analytical queries on parquet, and
doesn't require external database infrastructure.

### Execution Model

**Full rebuild on every run**: The gear performs a complete rebuild of all models.

**Implications**:

- No incremental materialization support
- All models are re-executed regardless of whether source data changed
- Processing time scales with data size and model complexity
- No dbt state is persisted between runs

**For large datasets**: Consider filtering data in source queries or breaking
projects into smaller, focused transformations.

### Storage Behavior

**Overwrites without confirmation**: Files at `{output_prefix}/model_name.parquet`
are overwritten if they exist.

**No cleanup**: Previous outputs are not automatically removed. If you rename or
delete a model, old files remain in storage.

**Best practice**: Use timestamped or versioned output prefixes for production
workflows:

```text
datasets/project-123/transformed/2024-01-15/
datasets/project-123/transformed/2024-01-22/
```

### Known Limitations

#### No Incremental Models

Incremental materialization is not supported. All models are rebuilt on every run:

```sql
-- Not supported
{{ config(materialized='incremental') }}
```

**Workaround**: Use full refreshes and optimize query performance with DuckDB's
efficient parquet scanning.

#### No Snapshot Support

dbt snapshot functionality is not tested or supported in this version.

#### Seeds Not Actively Managed

While you can include seed CSV files in your project, the gear doesn't handle
uploading them to external storage or ensuring they're accessible to models.

**Workaround**: Convert seeds to parquet and include them in the source dataset.

#### Limited Error Recovery

If dbt execution fails mid-run, no outputs are uploaded to external storage. The
gear returns an error, and you must fix the issue and re-run.

**Partial success is not supported**: If 5 of 10 models succeed before an error,
those 5 models are not uploaded.

#### Container Resource Limits

Very large model outputs or complex transformations may encounter memory limits
within the gear container.

**Monitoring**: Watch gear logs for out-of-memory errors or timeouts.

## Troubleshooting Tips

### "No parquet files found"

- Verify `source_prefix` matches your dataset location exactly
- Ensure there are no leading or trailing slashes
- Check that the dataset export completed successfully

### "dbt project missing required files"

- Ensure zip contains `dbt_project.yml` and `profiles.yml` at the root level
- Check that files are at the top level of the zip, not in a subdirectory
- Use `unzip -l` to verify zip structure

### "Failed to read parquet"

- Check that your model SQL uses correct relative paths to source data
- Verify source paths in `sources.yml` use `../source_data/tables/...`
- Test paths locally using symbolic links before deployment

### "Model compilation failed"

- Test your dbt project locally first with sample data
- Run `dbt compile` locally to catch syntax errors
- Check that all `{{ ref() }}` and `{{ source() }}` references are correct

### "Storage access errors"

- Verify the storage label matches exactly (case-sensitive)
- Ensure your Flywheel instance has access to the external storage
- Check that you have read permissions on source_prefix and write permissions
  on output_prefix

### dbt run succeeds but no outputs uploaded

- Check gear logs for upload phase messages
- Verify models materialized to the target directory with `external` materialization
- Confirm parquet files exist in `target/<schema>/` directory (e.g., `target/main/`)
- Ensure output_prefix has write permissions

### Models not creating parquet files

- Verify models are configured with `materialized: external` in dbt_project.yml
- Check that the target directory exists and is writable
- Review dbt run output for any materialization errors
- Ensure dbt-duckdb adapter is properly installed

## Next Steps

- Add more complex transformations and models
- Implement data quality tests with dbt tests
- Use dbt macros for reusable SQL logic
- Set up incremental models for large datasets (future feature)
- Create documentation with dbt docs generate

## Additional Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt-duckdb Adapter](https://github.com/duckdb/dbt-duckdb)
- [Flywheel Dataset Schema](https://gitlab.com/flywheel-io/scientific-solutions/lib/fw-dataset#dataset-structure)
- [DuckDB Documentation](https://duckdb.org/docs/)
