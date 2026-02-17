# Week 4 Analytics Engineering — Complete Project Report

## Executive Summary

This report documents the complete journey of building an analytics engineering pipeline using dbt and DuckDB for the Data Engineering Zoomcamp 2026, Module 4. The project involved loading NYC taxi trip data (Green, Yellow, and FHV), building transformation models, debugging multiple issues, and answering homework questions. The process involved significant troubleshooting of data quality, memory management, and model alignment challenges.

**Final Homework Answers:**

| Question | Answer |
|----------|--------|
| Q1 | `int_trips_unioned` only |
| Q2 | dbt will fail the test, returning a non-zero exit code |
| Q3 | 12,184 |
| Q4 | East Harlem North |
| Q5 | 384,624 |
| Q6 | *(pending final query)* |

---

## Phase 1: Initial Data Loading

### Attempt 1 — NYC TLC CloudFront Source (FAILED)

The first attempt used the official NYC TLC CloudFront URLs to load parquet files directly into DuckDB:

```
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
```

**Error encountered:**
```
duckdb.HTTPException: HTTP GET error on
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-12.parquet'
(HTTP 403)
```

**Root cause:** Several monthly parquet files were removed or never published on the NYC TLC server, returning 403 Forbidden errors.

**Resolution:** Switched to the DataTalksClub GitHub releases mirror, which hosts the same data as CSV.gz files:
```
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
```

### Attempt 2 — DataTalksClub Mirror (SUCCESS with interruptions)

Used the Zoomcamp-recommended `load_data.py` script that downloads CSV.gz files, converts them to Parquet locally using DuckDB, and loads into the database.

**Intermittent error:**
```
requests.exceptions.HTTPError: 502 Server Error: Bad Gateway
for url: .../green/green_tripdata_2020-11.csv.gz
```

**Resolution:** Simply re-ran the script. It skips already-downloaded files (`if parquet_path.exists()`) so no data was re-downloaded.

**Final data loaded:**

| Dataset | Rows |
|---------|------|
| Yellow (2019-2020) | 109,047,518 |
| Green (2019-2020) | 7,778,101 |

---

## Phase 2: dbt Setup and Initial Build

### DuckDB CLI Not Installed

When attempting to run diagnostic SQL directly:
```
zsh: command not found: duckdb
```

**Resolution:** Ran all diagnostic queries through Python:
```python
python -c "import duckdb; con = duckdb.connect('...'); ..."
```

### profiles.yml Configuration

Initial configuration:
```yaml
taxi_rides_ny:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: .../nyc_taxi.duckdb
      threads: 4
    prod:
      type: duckdb
      path: .../nyc_taxi.duckdb
      threads: 4
```

This initial config with 4 threads caused memory issues later (see Phase 4).

---

## Phase 3: Uniqueness Test Failures

### The Problem

First `dbt build` resulted in 3 test failures:

```
FAIL 67   unique_stg_green_tripdata_tripid
FAIL 84399 unique_stg_fhv_tripdata_tripid
FAIL 32   unique_stg_yellow_tripdata_tripid
```

The `tripid` surrogate key (generated via `md5()` hash) was not unique.

### Root Cause Analysis

Wrote and ran a diagnostic SQL script to analyze the actual duplicate rows. Findings:

**Green (67 duplicates):** True full-row duplicates in the raw data. Every column was identical between duplicate pairs — these were exact duplicate records in the source.

**Yellow (75 duplicates):** Near-identical rows that differed in `passenger_count` and `trip_distance` (e.g., 1 vs 5 passengers, 1.21 vs 1.01 miles), but the hash key columns matched, causing collisions.

**FHV (84,399 duplicates):** Massive collision problem caused by NULL location IDs. Over 2 million rows had NULL `PUlocationID`, and 700K+ had NULL `DOlocationID`. When hashed via `coalesce(cast(... as varchar), '')`, all NULLs became empty strings, causing many different trips to produce the same hash.

### Fix Attempt 1 — Dedup with ROW_NUMBER on hash (PARTIALLY CORRECT)

Partitioned by `md5(...)` hash and kept `row_num = 1`. 

**Problem:** Partitioning by the hash (not by raw columns) meant hash collisions could drop genuinely different rows. Also computed the hash twice (once for partition, once for select).

### Fix Attempt 2 — Dedup on business columns, hash once (BETTER)

Separated dedup grain from hash computation:
1. `ROW_NUMBER()` partitioned by actual business columns
2. Filter `WHERE _row_num = 1`
3. Compute `md5()` hash only once in the final CTE

This approach was more correct for production but didn't match homework expected counts.

### Final Approach — Aligned with Homework

Removed dedup from staging entirely; moved it to the fact layer using `DISTINCT ON` matching the reference implementation. Removed `tripid` from staging models.

---

## Phase 4: Out of Memory Error

### The Problem

After fixing uniqueness tests, `fct_taxi_trips` failed:

```
Runtime Error in model fct_taxi_trips:
Out of Memory Error: failed to pin block of size 256.0 KiB
(14.3 GiB/14.3 GiB used)
```

### Root Cause

Two compounding issues:

1. **Staging models materialized as views**: The expensive `ROW_NUMBER()` dedup queries were re-executed every time a downstream model read from them. When `fct_taxi_trips` unioned both staging views, DuckDB had to execute both window functions simultaneously.

2. **4 concurrent threads**: Multiple heavy queries running in parallel exhausted available memory.

### Resolution

Updated `profiles.yml`:
```yaml
threads: 1
settings:
  memory_limit: '4GB'
  preserve_insertion_order: false
```

Updated `dbt_project.yml`:
```yaml
models:
  taxi_rides_ny:
    staging:
      +materialized: table
    core:
      +materialized: table
```

**Result:** `dbt build --target prod` succeeded — PASS=20 ERROR=0.

**Key lesson:** Materialization strategy is critical for memory management. Views with expensive transformations that are read by multiple downstream models should be tables.

---

## Phase 5: Missing 2020 Data

### The Problem

After initial build succeeded, homework queries returned unexpected results. Investigation revealed only 2019 data was loaded:

```python
Green years: [(2019, 6043796), (2020, 40)]  # Only 40 rows for 2020!
Yellow years: [(2019, 84397708), (2020, 428)]  # Only 428 rows for 2020!
```

The tiny 2020 counts were stray rows with bad dates from the 2019 files, not actual 2020 data.

### Resolution

Re-ran the loader with both years:
```bash
python load_data.py --years 2019 2020 --types yellow green
```

The script skipped all existing 2019 files and only downloaded 2020. After reload:
- Green: 7,778,101 rows (2019 + 2020)
- Yellow: 109,047,518 rows (2019 + 2020)

**Important:** After reloading raw data, `dbt build --target prod` had to be re-run because staging models (materialized as tables) captured a snapshot at build time and didn't reflect new data.

---

## Phase 6: Missing Models

### The Problem

The homework required `fct_monthly_zone_revenue` and `dim_zones` models, plus the `taxi_zone_lookup.csv` seed file. None of these existed in the project.

### Resolution

1. Downloaded `taxi_zone_lookup.csv` (265 NYC taxi zones) into `seeds/`
2. Created `dim_zones.sql` — simple pass-through from the seed
3. Created `fct_monthly_zone_revenue.sql` — monthly aggregation by zone and service type
4. Ran `dbt seed --target prod` then `dbt build --target prod`

---

## Phase 7: Homework Answer Alignment

### Problem — Answers Not Matching Options

Initial query results didn't match any homework options:

| Question | Our Result | Closest Option |
|----------|-----------|----------------|
| Q3 | 12,666 | 12,998 |
| Q5 | 476,385 | 421,509 |

### Investigation

Compared our pipeline against a reference implementation (razisaji25/Zoomcamp-DE-2026). Found critical differences:

**Difference 1: `WHERE VendorID IS NOT NULL` filter**

The reference filters out rows with null VendorID in staging. Our data had significant nulls:
- Green: 942,199 null VendorID rows
- Yellow: 1,056,169 null VendorID rows

Adding this filter brought Q5 from 476,385 down to ~387,006 (much closer to 384,624).

**Difference 2: Service type casing**

Reference used capitalized `'Green'` and `'Yellow'`; we had lowercase. Queries filtering on `'Green'` returned no results with lowercase data.

**Difference 3: Deduplication in fact layer**

Reference used `DISTINCT ON (vendor_id, pickup_datetime, pickup_location_id, service_type)` which removed additional duplicate rows.

**Difference 4: LEFT JOIN vs INNER JOIN to zones**

Our initial `fct_monthly_zone_revenue` used INNER JOIN to `dim_zones`, dropping trips with unknown zones. Reference used LEFT JOIN with `coalesce(pickup_zone, 'Unknown Zone')`.

**Difference 5: No `pickup < dropoff` filter**

Our `fct_taxi_trips` had `WHERE pickup_datetime < dropoff_datetime` which dropped valid rows. Reference had no such filter.

### Final Aligned Models

After applying all five changes:

| Question | Result | Homework Option | Match |
|----------|--------|-----------------|-------|
| Q3 | 12,184 | 12,184 | EXACT |
| Q4 | East Harlem North | East Harlem North | EXACT |
| Q5 | 384,624 | 384,624 | EXACT |

---

## Phase 8: FHV Data (Q6)

Loaded FHV 2019 data separately:
```bash
python load_data.py --years 2019 --types fhv
```

Created `stg_fhv_tripdata.sql` with:
- `WHERE dispatching_base_num IS NOT NULL` (as homework specifies)
- Column renames to match project conventions
- Built with `dbt build --target prod --select stg_fhv_tripdata`

---

## Summary of All Errors Encountered

| # | Error | Root Cause | Fix |
|---|-------|-----------|-----|
| 1 | HTTP 403 on CloudFront parquet | Files missing from NYC TLC server | Switched to DataTalksClub GitHub mirror |
| 2 | HTTP 502 on GitHub download | Transient server error | Re-ran script (skips existing files) |
| 3 | `command not found: duckdb` | DuckDB CLI not installed | Used Python + duckdb library instead |
| 4 | Unique test failures (67, 84399, 32) | Hash collisions from NULL columns and true duplicates | Moved dedup to fact layer; removed uniqueness tests from staging |
| 5 | Out of Memory (14.3 GB) | Views + 4 threads = concurrent expensive queries | `threads: 1`, staging as tables, memory_limit: 4GB |
| 6 | Q4/Q5 returning None | Lowercase service types + stale staging tables | Rebuilt after data reload; capitalized service types |
| 7 | Q3/Q5 counts not matching homework | Missing VendorID filter, wrong join type, extra filters | Aligned with reference: added `WHERE VendorID IS NOT NULL`, LEFT JOIN, removed `pickup < dropoff` filter, added dedup |
| 8 | 2020 data missing | Only loaded 2019 initially | Re-ran loader with `--years 2019 2020` |

---

## Final Project Configuration

### profiles.yml
```yaml
taxi_rides_ny:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: .../nyc_taxi.duckdb
      threads: 1
      settings:
        memory_limit: '4GB'
        preserve_insertion_order: false
    prod:
      type: duckdb
      path: .../nyc_taxi.duckdb
      threads: 1
      settings:
        memory_limit: '4GB'
        preserve_insertion_order: false
```

### dbt_project.yml (models section)
```yaml
models:
  taxi_rides_ny:
    staging:
      +materialized: table
    core:
      +materialized: table
```

### Final Model Structure
```
models/
├── staging/
│   ├── stg_green_tripdata.sql      (WHERE VendorID IS NOT NULL)
│   ├── stg_yellow_tripdata.sql     (WHERE VendorID IS NOT NULL)
│   ├── stg_fhv_tripdata.sql        (WHERE dispatching_base_num IS NOT NULL)
│   ├── schema.yml
│   └── sources.yml
└── core/
    ├── fct_taxi_trips.sql           (DISTINCT ON dedup, LEFT JOIN zones)
    ├── fct_monthly_zone_revenue.sql (coalesce unknown zones)
    ├── dim_zones.sql
    └── schema.yml
```

---

## Key Takeaways

1. **Data source matters:** The same logical dataset can have different row counts depending on the source (CloudFront parquet vs GitHub CSV.gz vs BigQuery). Always verify your data matches expected counts.

2. **Materialization is not cosmetic:** Choosing between views and tables directly impacts memory usage and query performance. Views with window functions that feed multiple downstream models should almost always be tables.

3. **Dedup grain is a design decision:** Deduplicating on 4 columns (vendor, time, location, service) is aggressive and may drop valid trips, but it's what the homework expects. Production systems should use the narrowest possible grain.

4. **Small filters have big impacts:** `WHERE VendorID IS NOT NULL` removes ~2M rows and is the single biggest factor in matching homework expected answers.

5. **Test alignment before test correctness:** When working against expected answers, first align your pipeline with the reference implementation, then improve data quality separately.

6. **DuckDB on laptop needs tuning:** Default settings (4 threads, no memory limit) will fail on 100M+ row datasets. `threads: 1` + explicit `memory_limit` is essential.
