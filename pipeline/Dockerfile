FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
COPY pyproject.toml .python-version uv.lock ./

RUN uv sync

COPY ingest_data.py .
COPY green_tripdata_2025-11.parquet .
COPY taxi_zone_lookup.csv .

ENTRYPOINT ["uv", "run", "python", "ingest_data.py"]