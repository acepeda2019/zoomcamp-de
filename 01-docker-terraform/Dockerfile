# Use official Python 3.13 slim image
FROM python:3.13-slim

# COPY UV binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory
WORKDIR /code

# Prevent Python from writing pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/code/.venv/bin:$PATH"

# Copy dependency files first (better layer chaching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./

# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

# copy the script to the container. 1st name is the source file, 2 is the destination
COPY ingest_data.py .

# define what to do first when the container runs
# in this example, we will just run the script
ENTRYPOINT ["python", "ingest_data.py"]
