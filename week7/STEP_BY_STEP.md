# Week 7 Step-by-Step Plan

This checklist is ordered to reduce rework. Finish each checkpoint before moving to the next one.

## Checkpoint 1: Get the workshop locally

Goal: have access to the official `07-streaming/workshop` materials.

1. Clone the course repo if you do not already have it locally.
2. Go to `07-streaming/workshop`.
3. Confirm you can see `docker-compose.yml`, `src/producers`, `src/consumers`, and `src/job`.

## Checkpoint 2: Start the services

Goal: run Redpanda, Flink, and PostgreSQL locally.

```bash
cd 07-streaming/workshop
docker compose down -v
docker compose build
docker compose up -d
```

Verify:

- Redpanda is reachable on `localhost:9092`
- Flink UI is reachable at `http://localhost:8081`
- PostgreSQL is reachable on `localhost:5432`

## Checkpoint 3: Answer Q1

Goal: get the Redpanda version.

```bash
docker exec -it workshop-redpanda-1 rpk version
```

Record the version immediately.

## Checkpoint 4: Prepare the topic and producer for Q2

Goal: send the October 2025 green taxi rows into Redpanda.

Create topic:

```bash
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

Start from the workshop producer example and change:

- dataset URL to `green_tripdata_2025-10.parquet`
- topic to `green-trips`
- selected columns to the homework columns
- datetime values to string before JSON serialization
- total runtime measurement around the full send + flush

Expected result:

- all rows are published
- total runtime is printed for Q2

## Checkpoint 5: Build the Q3 consumer

Goal: count rows where `trip_distance > 5`.

Start from the workshop consumer example and change:

- topic to `green-trips`
- `auto_offset_reset='earliest'`
- deserialization to match the green trip JSON payload
- logic to count instead of printing demo rows

Expected result:

- a final integer count for Q3

## Checkpoint 6: Create the Flink source once

Goal: create a reusable source definition for Q4, Q5, and Q6.

Start from the workshop aggregation job and change:

- topic from `rides` to `green-trips`
- pickup field from `tpep_pickup_datetime` to `lpep_pickup_datetime`
- timestamp type from epoch milliseconds to string timestamps
- event-time expression to `TO_TIMESTAMP(...)`
- `env.set_parallelism(1)`

Keep this source DDL reusable across all three Flink jobs.

## Checkpoint 7: Solve Q4

Goal: 5-minute tumbling window by `PULocationID`.

1. Create a PostgreSQL result table with:
   - `window_start`
   - `PULocationID`
   - `num_trips`
2. Build a Flink job using a 5-minute tumbling window.
3. Write results into PostgreSQL.
4. Submit the job:

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_pu.py
```

5. Wait until results appear in PostgreSQL.
6. Query the top row:

```sql
SELECT PULocationID, num_trips
FROM q4_results
ORDER BY num_trips DESC
LIMIT 3;
```

## Checkpoint 8: Solve Q5

Goal: 5-minute session window by `PULocationID`.

1. Create a separate PostgreSQL result table.
2. Build a Flink job using a 5-minute session window.
3. Group by `PULocationID`.
4. Store at least:
   - `window_start`
   - `window_end`
   - `PULocationID`
   - `num_trips`
5. Query the longest session:

```sql
SELECT PULocationID, num_trips
FROM q5_results
ORDER BY num_trips DESC
LIMIT 1;
```

## Checkpoint 9: Solve Q6

Goal: 1-hour tumbling window for total `tip_amount`.

1. Create another PostgreSQL result table.
2. Build a Flink job with a 1-hour tumbling window.
3. Sum `tip_amount` per hour across all trips.
4. Query the highest hour:

```sql
SELECT window_start, total_tip_amount
FROM q6_results
ORDER BY total_tip_amount DESC
LIMIT 1;
```

## Checkpoint 10: Clean reruns and submit

If you accidentally publish the dataset multiple times, recreate the topic:

```bash
docker exec -it workshop-redpanda-1 rpk topic delete green-trips
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

Also clear or recreate the PostgreSQL result tables before rerunning Flink jobs.

Submit all answers before `2026-03-20 07:59` local time.
