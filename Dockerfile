FROM python:3.11-slim

WORKDIR /app

# Basic deps (optional but useful)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

# Copy minimal project files (compose에서 volume mount로 덮어씀)
COPY src ./src
COPY scripts ./scripts
COPY dbt ./dbt

ENV PYTHONPATH=/app/src
ENV DBT_PROFILES_DIR=/app/dbt

CMD ["bash"]
