/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

# TODO: Define output columns, mark primary keys, and add a few checks.
columns:
  - name: taxi_type
    type: string
    description: Taxi type (yellow/green)
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: Trip pickup timestamp
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Trip dropoff timestamp
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: TLC pickup location ID
    primary_key: true
  - name: dropoff_location_id
    type: integer
    description: TLC dropoff location ID
    primary_key: true
  - name: passenger_count
    type: integer
    description: Number of passengers
  - name: trip_distance
    type: double
    description: Trip distance
    checks:
      - name: non_negative
  - name: payment_type
    type: integer
    description: Payment type ID
  - name: payment_type_name
    type: string
    description: Payment type label
  - name: fare_amount
    type: double
    description: Fare amount
    checks:
      - name: non_negative
  - name: total_amount
    type: double
    description: Total amount
    checks:
      - name: non_negative
  - name: tip_amount
    type: double
    description: Tip amount
    checks:
      - name: non_negative

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: dropoff_after_pickup
    description: Dropoff time should not be before pickup time
    query: |
      SELECT COUNT(*) FROM staging.trips WHERE dropoff_datetime < pickup_datetime
    value: 0

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

WITH filtered AS (
  SELECT *
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        taxi_type,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        fare_amount,
        total_amount,
        payment_type
      ORDER BY extracted_at DESC
    ) AS rn
  FROM filtered
)
SELECT
  d.taxi_type,
  d.pickup_datetime,
  d.dropoff_datetime,
  d.pickup_location_id,
  d.dropoff_location_id,
  d.passenger_count,
  d.trip_distance,
  d.payment_type,
  p.payment_type_name,
  d.fare_amount,
  d.total_amount,
  d.tip_amount
FROM dedup d
LEFT JOIN ingestion.payment_lookup p
  ON d.payment_type = p.payment_type_id
WHERE d.rn = 1
  AND d.pickup_datetime IS NOT NULL
  AND d.dropoff_datetime IS NOT NULL
  AND d.dropoff_datetime >= d.pickup_datetime
  AND d.fare_amount >= 0
  AND d.total_amount >= 0
  AND d.tip_amount >= 0
