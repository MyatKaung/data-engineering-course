# Week 5 Bruin Homework: End-to-End Practical Guide

This guide documents a complete local setup and run of the NYC Taxi pipeline with Bruin, including the exact files, commands, and reasoning behind each step.

It assumes your Git root is:
`/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering`

It assumes your Bruin project lives at:
`/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week5/my-taxi-pipeline`

---

**1) Install Bruin CLI**

Goal: Install the CLI and make sure it is on your PATH.

```bash
curl -LsSf https://getbruin.com/install/cli | sh
source ~/.zshrc
bruin version
```

Expected: `bruin version` prints the installed version.

---

**2) Initialize the Zoomcamp template**

Goal: Scaffold the Bruin project with the Zoomcamp template.

```bash
mkdir -p /Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week5
cd /Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week5
bruin init zoomcamp my-taxi-pipeline
```

Expected: A new folder `my-taxi-pipeline` with a `pipeline/` directory and assets.

---

**3) Create the Bruin connection file**

Goal: Add a DuckDB connection. Bruin resolves the project root using Git, so `.bruin.yml` must live in the Git root, not just the pipeline folder.

Create:
`/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/.bruin.yml`

```yml
environments:
  default:
    connections:
      duckdb:
        - name: duckdb-default
          path: duckdb.db
```

Reasoning: This defines a local DuckDB file database for the pipeline.

---

**4) Configure `pipeline.yml`**

Goal: Define the pipeline name, schedule, start date, default connection, and variables.

File:
`/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week5/my-taxi-pipeline/pipeline/pipeline.yml`

```yml
name: nyc_taxi_pipeline
schedule: monthly
start_date: "2022-01-01"
default_connections:
  duckdb: duckdb-default
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

Reasoning: 
- `start_date` set to 2022-01-01 for lighter dev runs. For backfills, change to 2019-01-01 or earlier.
- `taxi_types` makes ingestion configurable.

---

**5) Seed lookup asset**

Goal: Load the payment type lookup CSV as a seed table.

File:
`/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week5/my-taxi-pipeline/pipeline/assets/ingestion/payment_lookup.asset.yml`

```yml
name: ingestion.payment_lookup
type: duckdb.seed
parameters:
  path: payment_lookup.csv
columns:
  - name: payment_type_id
    type: integer
    description: Payment type identifier from TLC data
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: payment_type_name
    type: string
    description: Payment type label
    checks:
      - name: not_null
```

Reasoning: Seed assets are ideal for small static lookup data and enable joins in staging.

---

**6) Python ingestion asset**

Goal: Fetch monthly NYC taxi parquet files from the TLC public endpoint and ingest into DuckDB.

File:
`/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week5/my-taxi-pipeline/pipeline/assets/ingestion/trips.py`

Key choices:
- `materialization.strategy: append` to preserve raw ingested data.
- Standardize columns so yellow/green files align.
- Use `BRUIN_START_DATE`, `BRUIN_END_DATE`, and pipeline vars.

The ingestion code downloads:
`https://d37ci6vzurychx.cloudfront.net/trip-data/<taxi>_tripdata_<YYYY-MM>.parquet`

---

**7) Python requirements**

Goal: Ensure the ingestion asset has required dependencies.

File:
`/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week5/my-taxi-pipeline/pipeline/assets/ingestion/requirements.txt`

```txt
pandas==2.2.2
requests==2.32.3
pyarrow==16.1.0
python-dateutil==2.9.0.post0
```

---

**8) Staging SQL asset**

Goal: Clean, deduplicate, and enrich trips. Use `time_interval` for incremental refreshes.

File:
`/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week5/my-taxi-pipeline/pipeline/assets/staging/trips.sql`

Key choices:
- `strategy: time_interval` with `incremental_key: pickup_datetime`
- Dedup using `ROW_NUMBER()`
- Join to `ingestion.payment_lookup`
- Filter invalid rows so quality checks pass

Important filter added:
`dropoff_datetime >= pickup_datetime` and non-negative amounts.

Reasoning: TLC data includes occasional negative fare/total values and time ordering issues. Filtering makes checks pass and reflects typical analytics expectations.

---

**9) Reports SQL asset**

Goal: Aggregate trips for reporting, aligned with the same incremental logic.

File:
`/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week5/my-taxi-pipeline/pipeline/assets/reports/trips_report.sql`

Key choices:
- `strategy: time_interval`
- `incremental_key: pickup_date`
- Aggregate by date, taxi_type, payment_type

---

**10) Validate**

Goal: Run static validation before executing.

```bash
cd /Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week5/my-taxi-pipeline
bruin validate ./pipeline/pipeline.yml --environment default
```

Expected: “No issues found”.

---

**11) Run the pipeline (small dev window)**

Goal: Load a single month for fast iteration.

```bash
bruin run ./pipeline/pipeline.yml \
  --environment default \
  --full-refresh \
  --start-date 2022-01-01 \
  --end-date 2022-02-01 \
  --var 'taxi_types=["yellow"]'
```

Expected: All 4 assets pass and all checks succeed.

---

**12) Verify results**

```bash
bruin query --connection duckdb-default --query "SELECT COUNT(*) AS n FROM ingestion.trips"
bruin query --connection duckdb-default --query "SELECT COUNT(*) AS n FROM staging.trips"
bruin query --connection duckdb-default --query "SELECT * FROM reports.trips_report ORDER BY trip_count DESC LIMIT 10"
```

Example results from a successful run:
- `ingestion.trips`: 2,463,931
- `staging.trips`: 2,411,357

---

**13) Optional: show lineage**

```bash
bruin lineage ./pipeline/assets/reports/trips_report.sql
```

---

**14) Optional: larger backfill**

When ready for more data, increase the window or update `start_date` back to 2019.

```bash
bruin run ./pipeline/pipeline.yml \
  --environment default \
  --full-refresh \
  --start-date 2019-01-01 \
  --end-date 2025-11-30 \
  --var 'taxi_types=["yellow","green"]'
```

Note: Larger backfills will use significant memory and disk. Start small.

---

**Homework Questions Mapping**

1. Required structure: `.bruin.yml` at root, `pipeline/` with `pipeline.yml` and `assets/`.
2. Incremental strategy: `time_interval` for time window refreshes.
3. Override vars: `bruin run --var 'taxi_types=["yellow"]'`.
4. Run downstream: `bruin run ingestion/trips.py --downstream`.
5. Quality check: `not_null`.
6. Lineage command: `bruin lineage`.
7. First run: `--full-refresh`.

---

**Current status**

The pipeline runs successfully on the January 2022 window with the updated staging filters and all checks passing.
