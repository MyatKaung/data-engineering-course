# Week 4 Analytics Engineering — Complete Project Report

## Executive Summary
This report documents the complete journey of building an analytics engineering pipeline using dbt and DuckDB for the Data Engineering Zoomcamp 2026, Module 4. The project involved loading NYC taxi trip data (Green, Yellow, and FHV), building transformation models, debugging multiple issues, and answering homework questions. The process involved significant troubleshooting of data quality, memory management, and model alignment challenges.

## Final Homework Answers
| Question | Answer |
|----------|--------|
| Q1 | int_trips_unioned only |
| Q2 | dbt will fail the test, returning a non-zero exit code |
| Q3 | 12,184 |
| Q4 | East Harlem North |
| Q5 | 384,624 |
| Q6 | (pending final query) |

---

## Phase 1: Initial Data Loading

### Attempt 1 — NYC TLC CloudFront Source (FAILED)
The first attempt used the official NYC TLC CloudFront URLs to load parquet files directly into DuckDB:
`BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"`

**Error encountered:**
`duckdb.HTTPException: HTTP GET error on 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-12.parquet' (HTTP 403)`

*   **Root cause:** Several monthly parquet files were removed or never published on the NYC TLC server, returning 403 Forbidden errors.
*   **Resolution:** Switched to the DataTalksClub GitHub releases mirror, which hosts the same data as CSV.gz files:
    `BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"`

### Attempt 2 — DataTalksClub Mirror (SUCCESS with interruptions)
Used the Zoomcamp-recommended `load_data.py` script that downloads CSV.gz files, converts them to Parquet locally using DuckDB, and loads into the database.

**Intermittent error:**
`requests.exceptions.HTTPError: 502 Server Error: Bad Gateway for url: .../green/green_tripdata_2020-11.csv.gz`

*   **Resolution:** Simply re-ran the script. It skips already-downloaded files (if `parquet_path.exists()`) so no data was re-downloaded.

**Final data loaded:**
| Dataset | Rows |
|---------|------|
| Yellow (2019-2020) | 109,047,518 |
| Green (2019-2020) | 7,778,101 |

---

## Phase 2: dbt Setup and Initial Build

### DuckDB CLI Not Installed
When attempting to run diagnostic SQL directly:
`zsh: command not found: duckdb`

*   **Resolution:** Ran all diagnostic queries through Python:
    ```python
    python -c "import duckdb; con = duckdb.connect('...'); ..."
    ```

### profiles.yml Configuration
Initial configuration with 4 threads caused memory issues later (see Phase 4).

---

## Phase 3: Uniqueness Test Failures

### The Problem
First `dbt build` resulted in 3 test failures:
*   `FAIL 67 unique_stg_green_tripdata_tripid`
*   `FAIL 84399 unique_stg_fhv_tripdata_tripid`
*   `FAIL 32 unique_stg_yellow_tripdata_tripid`

### Root Cause Analysis
Findings from diagnostic SQL:
1.  **Green (67 duplicates):** True full-row duplicates in the raw data.
2.  **Yellow (75 duplicates):** Near-identical rows that differed in passenger\_count and trip\_distance, but hash key columns matched.
3.  **FHV (84,399 duplicates):** Massive collision problem caused by NULL location IDs.

### Final Approach — Aligned with Homework
Removed dedup from staging entirely; moved it to the fact layer using `DISTINCT ON` matching the reference implementation. Removed `tripid` from staging models.

---

## Phase 4: Out of Memory Error

### The Problem
`Out of Memory Error: failed to pin block of size 256.0 KiB (14.3 GiB/14.3 GiB used)`

### Resolution
Updated `profiles.yml`:
*   `threads: 1`
*   `memory_limit: '4GB'`

Updated `dbt_project.yml`:
*   `materialized: table` for staging and core models.

**Key lesson:** Materialization strategy is critical for memory management.

---

## Phase 5: Missing 2020 Data
Investigation revealed only 2019 data was loaded initially.
*   **Resolution:** Re-ran the loader: `python load_data.py --years 2019 2020 --types yellow green`

---

## Phase 6: Missing Models
Added `fct_monthly_zone_revenue`, `dim_zones`, and `taxi_zone_lookup.csv` seed.

---

## Phase 7: Homework Answer Alignment
Aligned pipeline with reference implementation:
1.  Added `WHERE VendorID IS NOT NULL` filter.
2.  Service type capitalization.
3.  Deduplication in fact layer.
4.  `LEFT JOIN` to zones.
5.  Removed `pickup < dropoff` filter.

**Final Aligned Result:** Q3: 12,184 | Q4: East Harlem North | Q5: 384,624.

---

## Phase 8: FHV Data (Q6)
Loaded FHV 2019 data separately and created `stg_fhv_tripdata.sql`.

---

## Summary of All Errors Encountered
| # | Error | Root Cause | Fix |
|---|-------|------------|-----|
| 1 | HTTP 403 on CloudFront | Files missing from server | Switched to GitHub mirror |
| 2 | HTTP 502 on GitHub | Transient server error | Re-ran script |
| 3 | command not found: duckdb | CLI not installed | Used Python library |
| 4 | Unique test failures | Hash collisions | Moved dedup to fact layer |
| 5 | Out of Memory | Views + 4 threads | threads: 1, materialized as tables |
| 6 | Stale counts | Stale staging tables | Rebuilt after data reload |
| 7 | Q3/Q5 not matching | Missing filters/wrong join | Aligned with reference implementation |
| 8 | 2020 data missing | Only loaded 2019 | Re-ran loader with both years |

---

## Key Takeaways
1.  **Data source matters:** Row counts vary by source.
2.  **Materialization is critical:** Views vs Tables impacts memory.
3.  **Small filters have big impacts:** `WHERE VendorID IS NOT NULL` removed ~2M rows.
4.  **DuckDB on laptop needs tuning:** `threads: 1` + `memory_limit` is essential.
