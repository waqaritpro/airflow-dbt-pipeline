"""
Daily ELT ingestion DAG.

Extracts the past-day earthquake feed from the USGS Earthquake Hazards
Program API (public, no API key required) and loads it raw into the
`raw.earthquakes` table in the warehouse Postgres. A dbt task then
transforms that raw data into staging and mart models.
"""
from __future__ import annotations

import datetime
import json

import requests
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
WAREHOUSE_CONN_ID = "warehouse_postgres"
DBT_PROJECT_DIR = "/opt/airflow/include/dbt/earthquake_dbt"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"

CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS raw;"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.earthquakes (
    id TEXT PRIMARY KEY,
    mag DOUBLE PRECISION,
    place TEXT,
    event_time TIMESTAMPTZ,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    depth_km DOUBLE PRECISION,
    raw_payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

INSERT_SQL = """
INSERT INTO raw.earthquakes
    (id, mag, place, event_time, longitude, latitude, depth_km, raw_payload)
VALUES %s
ON CONFLICT (id) DO NOTHING;
"""


@dag(
    dag_id="ingest_earthquakes_daily",
    description="Daily ingest of USGS earthquake data, then dbt transform into marts",
    schedule="@daily",
    start_date=datetime.datetime(2026, 10, 1),
    catchup=False,
    tags=["elt", "ingestion", "dbt", "week2"],
)
def ingest_earthquakes_daily():
    @task
    def extract() -> list[dict]:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        return response.json()["features"]

    @task
    def load(features: list[dict]) -> int:
        from psycopg2.extras import execute_values

        rows = []
        for feature in features:
            props = feature["properties"]
            longitude, latitude, depth_km = feature["geometry"]["coordinates"]
            rows.append(
                (
                    feature["id"],
                    props.get("mag"),
                    props.get("place"),
                    datetime.datetime.fromtimestamp(
                        props["time"] / 1000, tz=datetime.timezone.utc
                    ),
                    longitude,
                    latitude,
                    depth_km,
                    json.dumps(feature),
                )
            )

        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(CREATE_SCHEMA_SQL)
                cursor.execute(CREATE_TABLE_SQL)
                if rows:
                    execute_values(cursor, INSERT_SQL, rows)
            conn.commit()
        finally:
            conn.close()

        return len(rows)

    transform = BashOperator(
        task_id="transform",
        bash_command=(
            f"{DBT_BIN} source freshness "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            f"&& {DBT_BIN} build "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    load(extract()) >> transform


ingest_earthquakes_daily()
