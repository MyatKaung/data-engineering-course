/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: taxi_type
    type: string
    description: Taxi type (yellow/green)
    primary_key: true
  - name: payment_type_name
    type: string
    description: Payment type label
    primary_key: true
  - name: pickup_date
    type: date
    description: Trip pickup date
    primary_key: true
  - name: trip_count
    type: bigint
    description: Number of trips
    checks:
      - name: non_negative
  - name: total_amount_sum
    type: double
    description: Total amount summed
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  taxi_type,
  payment_type_name,
  CAST(pickup_datetime AS DATE) AS pickup_date,
  COUNT(*) AS trip_count,
  SUM(total_amount) AS total_amount_sum
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  taxi_type,
  payment_type_name,
  CAST(pickup_datetime AS DATE)
