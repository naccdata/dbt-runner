# -----------------------------------------------------------------------------
# Base stage: set up environment and install system dependencies
# -----------------------------------------------------------------------------

FROM flywheel/python:3.12-main AS base

# Set environment variable for the application directory
ENV FLYWHEEL="/flywheel/v0"
# Set the working directory
WORKDIR ${FLYWHEEL}

# Set the default entrypoint
ENTRYPOINT ["python", "/flywheel/v0/run.py"]



# -----------------------------------------------------------------------------
# Build stage: install production Python dependencies
# -----------------------------------------------------------------------------
FROM base AS build

# Copy production requirements and install them globally
COPY requirements.txt ${FLYWHEEL}/
RUN uv pip install -r requirements.txt --no-cache-dir



# -----------------------------------------------------------------------------
# Dev stage: install development dependencies and set up app in editable mode
# -----------------------------------------------------------------------------
FROM build AS dev
# Copy and install development-specific requirements
COPY requirements-dev.txt ${FLYWHEEL}/
RUN uv pip install -r requirements-dev.txt --no-cache-dir

# Copy the full application code and install the package without additional dependency resolution
COPY . .
RUN uv pip install --no-deps -e. --no-cache-dir



# -----------------------------------------------------------------------------
# Final production image: assemble the app for production use
# -----------------------------------------------------------------------------
FROM base

# Copy the virtual environment built in the 'build' stage
COPY --from=build /venv /venv

# Copy the complete application code
# And set ownership to the flywheel user
COPY --chown=flywheel:flywheel . .

# Install the package in editable mode (without additional dependency resolution)
RUN uv pip install --no-deps -e. --no-cache-dir

# Ensure the entrypoint script is executable
RUN chmod a+x "${FLYWHEEL}/run.py"

USER flywheel
