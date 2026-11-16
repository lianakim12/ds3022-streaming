"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> one chart.

  python flow.py
"""
import os

import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from prefect import flow, get_run_logger, task

import sys

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\"))       # "raw" or "sample"
TAG = "" if NAME == "raw" else "_" + NAME
DB = f"opensky{TAG}.duckdb"                      # opensky_sample.duckdb for the sample
CHART = f"charts/aircraft_by_country{TAG}.png"     # the sample never overwrites your chart


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of quakes.duckdb
        con.execute(f"CREATE OR REPLACE TABLE raw_positions AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_positions").fetchone()[0]


@task
def stage():
    """Kind 1: the same Kafka record landed twice. DISTINCT keeps one copy."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_positions
            WHERE action <> 'malformed'
              AND icao24 IS NOT NULL AND time_position IS NOT NULL
        """)
        q = lambda sql: con.execute(sql).fetchone()[0]
        return {"malformed": q("SELECT count(*) FROM raw_positions WHERE action = 'malformed'"),
                "records": q("SELECT count(*) FROM stg_records")}


@task
def entities():
    """Kind 2: the same position in two snapshots. One row per (icao24, time_position)."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE positions AS
            SELECT icao24, callsign, origin_country, latitude, longitude,
                   baro_altitude, on_ground, velocity,
                   make_timestamp(time_position * 1000000) AS event_time,  -- seconds -> UTC
                   CAST(raw::JSON ->> '$.data.vertical_rate' AS DOUBLE) AS vertical_rate
            FROM stg_records
            QUALIFY row_number() OVER (PARTITION BY icao24, time_position
                                       ORDER BY snapshot_time DESC) = 1
        """)
        return con.execute("SELECT count(*) FROM positions").fetchone()[0]


@task
def marts():
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE mart_country AS
            SELECT origin_country, count(DISTINCT icao24) AS aircraft,
                round(100.0 * count(DISTINCT icao24) / sum(count(DISTINCT icao24)) OVER (), 1) AS pct_of_all
            FROM positions
            WHERE on_ground = false AND origin_country IS NOT NULL
            GROUP BY origin_country
            ORDER BY aircraft DESC
        """)
        return con.execute("SELECT count(*) FROM mart_country").fetchone()[0]


@task
def chart():
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute("""
            SELECT origin_country, aircraft
            FROM mart_country
            ORDER BY aircraft DESC
            LIMIT 10
        """).fetchall()
    os.makedirs("charts", exist_ok=True)
    plt.figure(figsize=(10, 6))
    countries = [r[0] for r in rows]
    aircraft = [r[1] for r in rows]
    plt.barh(countries[::-1], aircraft[::-1], color="#2a7fb8")
    plt.title("Top 10 Countries by Number of Observed Airborne Aircraft")
    plt.xlabel("Number of Aircraft")
    plt.ylabel("Country")
    plt.tight_layout()
    plt.savefig(CHART, dpi=120)
    return CHART


@flow(name="positions-pipeline")
def positions_pipeline():
    log = get_run_logger()
    raw = load_raw()
    n = stage()
    things = entities()
    marts()
    path = chart()
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", n["malformed"])
    log.info("Unique Kafka records:      %d", n["records"])
    log.info("Unique positions:          %d", things)
    log.info("Chart: %s", path)


if __name__ == "__main__":
    positions_pipeline()
