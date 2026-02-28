"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: taxi_type
    type: string
    description: Taxi type (yellow or green)
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: Trip pickup timestamp
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Trip dropoff timestamp
  - name: pickup_location_id
    type: integer
    description: TLC pickup location ID
  - name: dropoff_location_id
    type: integer
    description: TLC dropoff location ID
  - name: passenger_count
    type: integer
    description: Number of passengers
  - name: trip_distance
    type: double
    description: Trip distance
  - name: ratecode_id
    type: integer
    description: Rate code ID
  - name: store_and_fwd_flag
    type: string
    description: Store-and-forward flag
  - name: payment_type
    type: integer
    description: Payment type ID
  - name: fare_amount
    type: double
    description: Fare amount
  - name: extra
    type: double
    description: Extra charges
  - name: mta_tax
    type: double
    description: MTA tax
  - name: tip_amount
    type: double
    description: Tip amount
  - name: tolls_amount
    type: double
    description: Tolls amount
  - name: improvement_surcharge
    type: double
    description: Improvement surcharge
  - name: total_amount
    type: double
    description: Total amount
  - name: congestion_surcharge
    type: double
    description: Congestion surcharge
  - name: airport_fee
    type: double
    description: Airport fee
  - name: vendor_id
    type: integer
    description: Vendor ID
  - name: extracted_at
    type: timestamp
    description: Ingestion timestamp

@bruin"""

import io
import json
import os
from datetime import datetime, date

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

CANONICAL_COLUMNS = [
    "taxi_type",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "vendor_id",
    "extracted_at",
]


def _date_from_env(name: str) -> date:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return datetime.strptime(value, "%Y-%m-%d").date()


def _month_starts(start: date, end: date) -> list[date]:
    current = date(start.year, start.month, 1)
    months = []
    while current < end:
        months.append(current)
        current = current + relativedelta(months=1)
    return months


def _pick_col(df: pd.DataFrame, *candidates: str) -> pd.Series:
    for name in candidates:
        if name in df.columns:
            return df[name]
    return pd.Series([pd.NA] * len(df))


def _standardize(df: pd.DataFrame, taxi_type: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [col.lower() for col in df.columns]

    out = pd.DataFrame(
        {
            "taxi_type": taxi_type,
            "pickup_datetime": _pick_col(
                df, "pickup_datetime", "tpep_pickup_datetime", "lpep_pickup_datetime"
            ),
            "dropoff_datetime": _pick_col(
                df, "dropoff_datetime", "tpep_dropoff_datetime", "lpep_dropoff_datetime"
            ),
            "pickup_location_id": _pick_col(df, "pulocationid", "pickup_location_id"),
            "dropoff_location_id": _pick_col(df, "dolocationid", "dropoff_location_id"),
            "passenger_count": _pick_col(df, "passenger_count"),
            "trip_distance": _pick_col(df, "trip_distance"),
            "ratecode_id": _pick_col(df, "ratecodeid", "ratecode_id"),
            "store_and_fwd_flag": _pick_col(df, "store_and_fwd_flag"),
            "payment_type": _pick_col(df, "payment_type"),
            "fare_amount": _pick_col(df, "fare_amount"),
            "extra": _pick_col(df, "extra"),
            "mta_tax": _pick_col(df, "mta_tax"),
            "tip_amount": _pick_col(df, "tip_amount"),
            "tolls_amount": _pick_col(df, "tolls_amount"),
            "improvement_surcharge": _pick_col(df, "improvement_surcharge"),
            "total_amount": _pick_col(df, "total_amount"),
            "congestion_surcharge": _pick_col(df, "congestion_surcharge"),
            "airport_fee": _pick_col(df, "airport_fee"),
            "vendor_id": _pick_col(df, "vendorid", "vendor_id"),
            "extracted_at": pd.Timestamp.utcnow(),
        }
    )

    return out[CANONICAL_COLUMNS]


def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    start_date = _date_from_env("BRUIN_START_DATE")
    end_date = _date_from_env("BRUIN_END_DATE")
    vars_json = os.getenv("BRUIN_VARS", "{}")
    vars_dict = json.loads(vars_json)
    taxi_types = vars_dict.get("taxi_types", ["yellow", "green"])

    frames: list[pd.DataFrame] = []
    for month in _month_starts(start_date, end_date):
        for taxi_type in taxi_types:
            file_name = f"{taxi_type}_tripdata_{month.year}-{month.month:02d}.parquet"
            url = f"{BASE_URL}/{file_name}"
            response = requests.get(url, timeout=60)
            if response.status_code == 404:
                print(f"Skipping missing file: {url}")
                continue
            response.raise_for_status()

            df = pd.read_parquet(io.BytesIO(response.content))
            frames.append(_standardize(df, taxi_type))

    if not frames:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    return pd.concat(frames, ignore_index=True)
