# Week 4: Analytics Engineering with dbt

## Overview

This module covers analytics engineering using **dbt (Data Build Tool)** with **DuckDB** as the data warehouse. We transform raw NYC taxi trip data into analytics-ready models following dimensional modeling best practices.

## Data Sources

All data is sourced from the [DataTalksClub NYC TLC mirror](https://github.com/DataTalksClub/nyc-tlc-data/releases):

| Dataset | Years | Records |
|---------|-------|---------|
| Yellow Taxi | 2019–2020 | ~109M |
| Green Taxi | 2019–2020 | ~7.8M |
| FHV (For-Hire Vehicle) | 2019 | ~43M |

## Project Structure

```
taxi_rides_ny/
├── models/
│   ├── staging/
│   │   ├── stg_green_tripdata.sql     # Clean + rename green taxi columns
│   │   ├── stg_yellow_tripdata.sql    # Clean + rename yellow taxi columns
│   │   ├── stg_fhv_tripdata.sql       # Clean + rename FHV columns (Q6)
│   │   ├── schema.yml                 # Staging tests
│   │   └── sources.yml                # Raw table definitions
│   └── core/
│       ├── fct_taxi_trips.sql         # Union green+yellow, dedup, zone join
│       ├── fct_monthly_zone_revenue.sql # Monthly revenue aggregation
│       ├── dim_zones.sql              # Zone dimension from seed
│       └── schema.yml                 # Core tests
├── seeds/
│   └── taxi_zone_lookup.csv           # 265 NYC taxi zones
├── dbt_project.yml
├── packages.yml
└── load_data.py                       # Data ingestion script
```

## dbt Model Lineage

```
seeds/taxi_zone_lookup.csv
        │
        ▼
    dim_zones
        │
        ▼
raw.green_tripdata ──► stg_green_tripdata ──┐
                                            ├──► fct_taxi_trips ──► fct_monthly_zone_revenue
raw.yellow_tripdata ─► stg_yellow_tripdata ─┘

raw.fhv_tripdata ───► stg_fhv_tripdata (standalone for Q6)
```

## Key Design Decisions

### Staging Layer
- **`WHERE VendorID IS NOT NULL`**: Filters out incomplete records (~2M rows) where vendor information is missing
- **No deduplication in staging**: Kept simple; dedup happens in the fact layer
- **Column renaming**: Standardized names across green/yellow (e.g., `lpep_pickup_datetime` → `pickup_datetime`)

### Core Layer
- **Deduplication**: `DISTINCT ON (vendor_id, pickup_datetime, pickup_location_id, service_type)` in `fct_taxi_trips` removes duplicate trip records
- **Zone enrichment**: `LEFT JOIN` to `dim_zones` preserves all trips; unknown zones become `'Unknown Zone'`
- **Service types**: Capitalized (`'Green'`, `'Yellow'`) for readability
- **Revenue aggregation**: Grouped by pickup zone, month, and service type

### DuckDB Configuration
- **`threads: 1`**: Prevents memory pressure from concurrent heavy queries
- **`memory_limit: 4GB`**: Suitable for laptop execution
- **`preserve_insertion_order: false`**: Reduces memory usage
- **Staging materialized as `table`**: Pre-computes staging results so downstream models don't re-execute expensive scans

## Setup & Execution

### 1. Install Dependencies
```bash
python -m venv dbt-env
source dbt-env/bin/activate
pip install dbt-duckdb duckdb requests
```

### 2. Load Data
```bash
# Green + Yellow (2019-2020)
python load_data.py \
  --db-path nyc_taxi.duckdb \
  --years 2019 2020 \
  --types yellow green

# FHV (2019 only, for Q6)
python load_data.py \
  --db-path nyc_taxi.duckdb \
  --years 2019 \
  --types fhv
```

### 3. Build dbt Models
```bash
dbt deps          # Install dbt_utils package
dbt seed --target prod   # Load taxi_zone_lookup.csv
dbt build --target prod  # Build all models + run tests
```

### 4. FHV Staging (Q6 only)
```bash
dbt build --target prod --select stg_fhv_tripdata
```

## Homework Answers

| Question | Answer |
|----------|--------|
| Q1: `dbt run --select int_trips_unioned` builds... | `int_trips_unioned` only |
| Q2: New value 6 appears, `dbt test` result | Fail with non-zero exit code |
| Q3: Count of `fct_monthly_zone_revenue` | **12,184** |
| Q4: Best Green taxi zone (2020 revenue) | **East Harlem North** |
| Q5: Green taxi trips, Oct 2019 | **384,624** |
| Q6: Count of `stg_fhv_tripdata` | *(run query to confirm)* |

### Q6 Query
```sql
SELECT COUNT(*) FROM stg_fhv_tripdata;
```

## Lessons Learned

1. **Materialization matters**: Views with expensive window functions caused OOM errors; switching staging to tables solved it
2. **Data quality filters affect counts**: `WHERE VendorID IS NOT NULL` removes ~2M rows and is critical for matching expected homework answers
3. **Dedup grain matters**: Deduplicating on too few columns (4) is aggressive but matches the homework; production systems should use all business columns
4. **Memory management**: DuckDB on a laptop needs `threads: 1` and explicit memory limits for 100M+ row datasets
5. **Source data quirks**: Raw data contains rows with dates in 2008, 2035, etc. — these are bad records but don't affect results when aggregating by valid date ranges

## profiles.yml

```yaml
taxi_rides_ny:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /path/to/nyc_taxi.duckdb
      threads: 1
      settings:
        memory_limit: '4GB'
        preserve_insertion_order: false
    prod:
      type: duckdb
      path: /path/to/nyc_taxi.duckdb
      threads: 1
      settings:
        memory_limit: '4GB'
        preserve_insertion_order: false
```
