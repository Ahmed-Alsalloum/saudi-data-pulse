# Dagster dev image: webserver + daemon in one container (fine for the local
# stack; the production VM splits these into separate services).
FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml ./
COPY orchestration/ orchestration/
COPY api/ api/
COPY transform/ transform/
RUN pip install --no-cache-dir -e ".[s3]" \
    && cd transform && dbt deps --profiles-dir . \
    # bake the httpfs extension into the image so the first dbt run against
    # MinIO doesn't depend on the DuckDB extension CDN
    && python -c "import duckdb; duckdb.sql('INSTALL httpfs')"

ENV DAGSTER_HOME=/app/.dagster_home \
    DATA_LAKE_PATH=/data/lake \
    DUCKDB_PATH=/data/warehouse.duckdb
RUN mkdir -p $DAGSTER_HOME

EXPOSE 3000
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
