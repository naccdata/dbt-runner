# Contributing

## Getting started

1. Follow instructions to
   [install uv](https://docs.astral.sh/uv/getting-started/installation/#installing-uv).
2. Follow instructions to [install pre-commit](https://pre-commit.com/#install)

After cloning the repo:

1. Run `uv venv && source .venv/bin/activate && uv sync` to install project and all
   dependencies (see __Dependency management__ below)
2. Run `pre-commit install` to install pre-commit hooks
   (see __Linting and Testing__ below)

## Dependency management

This gear uses [`uv`](https://docs.astral.sh/uv/) to manage dependencies,
develop, build and publish.

### Dependencies

Dependencies are listed in the `pyproject.toml` file.

#### Managing dependencies

* Adding: Use `uv add [--group dev] <dep>`
* Removing: Use `uv remove [--group dev] <dep>`
* Updating: Use `uv sync --upgrade <dep>` or `uv sync --upgrade` to update all deps.
  * Can also not update development dependencies with `--no-dev`
  * Update dry run: `--dry-run`

#### Using a different version of python

Poetry manages virtual environments and can create a virtual environment
with different versions of python,
however that version must be installed on the machine.

You can configure the Python version by using `uv env --python
<path/to/executable>`.
Additionally, it is important to ensure that
your Dockerfile is configured to use the same
Python version as the one specified in your Poetry environment.

## Linting and Testing

Local linting and testing scripts
are managed through [`pre-commit`](https://pre-commit.com/).
Pre-commit allows running hooks which can be defined locally, or in other
repositories. Default hooks to run on each commit:

* pyproject_export: Export python dependencies
  from `pyproject.toml` to `requirements.txt` with `uv`.
* gearcheck: Screen/fix common problems in flywheel gears.
* eolfix: Fix text files (i.e.: any source code) to enforce LF line endings
  and to ensure a single LF at EOF (no more, no less).
* hadolint: Lint Dockerfile to enforce best practices with hadolint. Uses
  shellcheck under the hood to lint the RUN instructions as well.
* jsonlint: Lint JSON files for syntax, uniform indentation and style with jsonlint.
  Formats .json files in-place to consistently use 2 spaces for indentation.
* linkcheck: Check text files for dead links with a custom qa-ci script `linkcheck.py`.
* markdownlint: Lint markdown files for syntax, line length, style and more
  with `markdownlint`.
* yamllint: Lint YAML files for syntax, uniform indentation and style with yamllint.
* ruff_format: Format python code with ruff to ensure uniform whitespace and style.
* ruff: Format python code with `ruff` to ensures uniform whitespace and
  style. You can define the configuration in the `pyproject.toml`, in the
  `tool.ruff` section.
* ruff_tests: Lint python tests using less strict defaults with static analyzer `ruff`.
* pytest: Build the Gear Docker image and run python tests and report code coverage
  with `pytest`.

These hooks will all run automatically on commit, but can also be run manually
or just be disabled.

More hooks can be enabled upon need. List of available
[hooks](https://gitlab.com/flywheel-io/tools/etc/qa-ci#table-of-contents).

### pre-commit usage

* Run hooks manually:
  * Run on all files: `pre-commit run -a`
  * Run on certain files: `pre-commit run --files test/*`
  * Run a specific pre-commit hook (`pytest` in this case):
    `pre-commit run --all-files pytest`
* Update (e.g. clean and install) hooks: `pre-commit clean && pre-commit install`
* Disable all hooks: `pre-commit uninstall`
* Enable all hooks: `pre-commit install`
* Skip a hook on commit: `SKIP=<hook-name> git commit`
* Skip all hooks on commit: `git commit --no-verify`

## Adding a contribution

Every contribution should be
associated with a ticket on the GEAR JIRA board, or be a hotfix.
You should contribute by creating
a branch titled with either `hotfix-<hotfix_name>` or `GEAR-<gear_num>-<description>`.
For now, other branch names will be accepted,
but soon branch names will be rejected
if they don't follow this pattern.

When contributing, make a Merge Request against the main branch.

### Merge requests

The merge request should contain at least two things:

1. Your relevant change
2. Update the corresponding entry under `docs/release_notes.md`

Adding the release notes does two things:

1. It makes it easier for the
   reviewer to identify what relevant changes
   they should expect and look for in the MR, and
2. It makes it easier to create a release.

#### Populating release notes

For example, if the gear is currently on version `0.2.1`
and you are working on a bugfix under the branch GEAR-999-my-bugfix.
When you create a merge request against `main`,
you should add a section to `docs/release_notes.md` such as the following:

```markdown
## 0.2.2

BUG:

* Fixed my-bug, see [GEAR-999](https://flywheelio.atlassian.net/browse/GEAR-999)

```

Where the rest of the file contains release notes for previous versions.
