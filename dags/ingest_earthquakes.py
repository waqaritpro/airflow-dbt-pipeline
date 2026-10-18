"""
Daily ELT ingestion DAG.

Extracts the past-day earthquake feed from the USGS Earthquake Hazards
Program API (public, no API key required) and loads it raw into the
`raw.earthquakes` table in the warehouse Postgres. No transformation
happens here by design -- that's dbt's job in a later week of this build.
"""
from __future__ import annotations

import datetime
import json

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
WAREHOUSE_CONN_ID = "warehouse_postgres"

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
    description="Daily raw ingest of USGS earthquake data into the warehouse",
    schedule="@daily",
    start_date=datetime.datetime(2026, 10, 1),
    catchup=False,
    tags=["elt", "ingestion", "week1"],
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

    load(extract())


ingest_earthquakes_daily()
