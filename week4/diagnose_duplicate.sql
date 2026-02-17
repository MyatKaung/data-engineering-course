-- ============================================================
-- Run these queries against your DuckDB to diagnose duplicates
-- duckdb nyc_taxi.duckdb < diagnose_duplicates.sql
-- ============================================================

-- ============================================================
-- 1. GREEN: Check if there are truly duplicate ROWS (all columns identical)
-- ============================================================
PRINT '=== GREEN: Total rows vs distinct rows ===';
SELECT
    count(*) as total_rows,
    count(distinct md5(
        coalesce(cast(VendorID as varchar), '')
        || '-' || coalesce(cast(lpep_pickup_datetime as varchar), '')
        || '-' || coalesce(cast(lpep_dropoff_datetime as varchar), '')
        || '-' || coalesce(cast(PULocationID as varchar), '')
        || '-' || coalesce(cast(DOLocationID as varchar), '')
        || '-' || coalesce(cast(total_amount as varchar), '')
    )) as distinct_tripids,
    count(*) - count(distinct md5(
        coalesce(cast(VendorID as varchar), '')
        || '-' || coalesce(cast(lpep_pickup_datetime as varchar), '')
        || '-' || coalesce(cast(lpep_dropoff_datetime as varchar), '')
        || '-' || coalesce(cast(PULocationID as varchar), '')
        || '-' || coalesce(cast(DOLocationID as varchar), '')
        || '-' || coalesce(cast(total_amount as varchar), '')
    )) as duplicate_count
FROM main.green_tripdata
WHERE lpep_pickup_datetime IS NOT NULL AND lpep_dropoff_datetime IS NOT NULL;

-- Show a sample of duplicated green rows — are they fully identical?
PRINT '=== GREEN: Sample duplicate rows (all columns) ===';
WITH keyed AS (
    SELECT *,
        md5(
            coalesce(cast(VendorID as varchar), '')
            || '-' || coalesce(cast(lpep_pickup_datetime as varchar), '')
            || '-' || coalesce(cast(lpep_dropoff_datetime as varchar), '')
            || '-' || coalesce(cast(PULocationID as varchar), '')
            || '-' || coalesce(cast(DOLocationID as varchar), '')
            || '-' || coalesce(cast(total_amount as varchar), '')
        ) as tripid
    FROM main.green_tripdata
    WHERE lpep_pickup_datetime IS NOT NULL AND lpep_dropoff_datetime IS NOT NULL
),
dups AS (
    SELECT tripid FROM keyed GROUP BY tripid HAVING count(*) > 1 LIMIT 3
)
SELECT k.*
FROM keyed k
JOIN dups d ON k.tripid = d.tripid
ORDER BY k.tripid, k.lpep_pickup_datetime
LIMIT 10;

-- ============================================================
-- 2. YELLOW: Same analysis
-- ============================================================
PRINT '=== YELLOW: Total rows vs distinct rows ===';
SELECT
    count(*) as total_rows,
    count(distinct md5(
        coalesce(cast(VendorID as varchar), '')
        || '-' || coalesce(cast(tpep_pickup_datetime as varchar), '')
        || '-' || coalesce(cast(tpep_dropoff_datetime as varchar), '')
        || '-' || coalesce(cast(PULocationID as varchar), '')
        || '-' || coalesce(cast(DOLocationID as varchar), '')
        || '-' || coalesce(cast(total_amount as varchar), '')
    )) as distinct_tripids
FROM main.yellow_tripdata
WHERE tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL;

-- Show sample duplicate yellow rows
PRINT '=== YELLOW: Sample duplicate rows ===';
WITH keyed AS (
    SELECT *,
        md5(
            coalesce(cast(VendorID as varchar), '')
            || '-' || coalesce(cast(tpep_pickup_datetime as varchar), '')
            || '-' || coalesce(cast(tpep_dropoff_datetime as varchar), '')
            || '-' || coalesce(cast(PULocationID as varchar), '')
            || '-' || coalesce(cast(DOLocationID as varchar), '')
            || '-' || coalesce(cast(total_amount as varchar), '')
        ) as tripid
    FROM main.yellow_tripdata
    WHERE tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL
),
dups AS (
    SELECT tripid FROM keyed GROUP BY tripid HAVING count(*) > 1 LIMIT 3
)
SELECT k.*
FROM keyed k
JOIN dups d ON k.tripid = d.tripid
ORDER BY k.tripid
LIMIT 10;

-- ============================================================
-- 3. FHV: Same analysis (84k duplicates is a LOT)
-- ============================================================
PRINT '=== FHV: Total rows vs distinct rows ===';
SELECT
    count(*) as total_rows,
    count(distinct md5(
        coalesce(cast(dispatching_base_num as varchar), '')
        || '-' || coalesce(cast(pickup_datetime as varchar), '')
        || '-' || coalesce(cast("dropOff_datetime" as varchar), '')
        || '-' || coalesce(cast(PUlocationID as varchar), '')
        || '-' || coalesce(cast(DOlocationID as varchar), '')
    )) as distinct_tripids
FROM main.fhv_tripdata
WHERE pickup_datetime IS NOT NULL AND "dropOff_datetime" IS NOT NULL;

-- FHV: How many NULL location IDs?
PRINT '=== FHV: NULL location counts ===';
SELECT
    count(*) as total,
    count(*) filter (where PUlocationID is null) as null_pu,
    count(*) filter (where DOlocationID is null) as null_do,
    count(*) filter (where PUlocationID is null and DOlocationID is null) as null_both
FROM main.fhv_tripdata
WHERE pickup_datetime IS NOT NULL AND "dropOff_datetime" IS NOT NULL;

-- FHV: Sample duplicates
PRINT '=== FHV: Sample duplicate rows ===';
WITH keyed AS (
    SELECT *,
        md5(
            coalesce(cast(dispatching_base_num as varchar), '')
            || '-' || coalesce(cast(pickup_datetime as varchar), '')
            || '-' || coalesce(cast("dropOff_datetime" as varchar), '')
            || '-' || coalesce(cast(PUlocationID as varchar), '')
            || '-' || coalesce(cast(DOlocationID as varchar), '')
        ) as tripid
    FROM main.fhv_tripdata
    WHERE pickup_datetime IS NOT NULL AND "dropOff_datetime" IS NOT NULL
),
dups AS (
    SELECT tripid FROM keyed GROUP BY tripid HAVING count(*) > 1 LIMIT 3
)
SELECT k.*
FROM keyed k
JOIN dups d ON k.tripid = d.tripid
ORDER BY k.tripid
LIMIT 10;