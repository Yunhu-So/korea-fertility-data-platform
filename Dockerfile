FROM python:3.11-slim

WORKDIR /app

# Install basic tools such as git
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

# Copy minimal project files; compose mounts will override these during development
COPY src ./src
COPY scripts ./scripts
COPY dbt ./dbt

ENV PYTHONPATH=/app/src
ENV DBT_PROFILES_DIR=/app/dbt

CMD ["bash"]
