# Week 7: Stream Processing

This folder contains my Module 7 homework work for the Data Engineering Zoomcamp. The assignment uses NYC Green Taxi trip data from October 2025 and builds a small streaming pipeline with Redpanda, Python, PyFlink, and PostgreSQL.

## Architecture

```text
green_tripdata_2025-10.parquet
        |
        v
Python Producer
        |
        v
Redpanda topic: green-trips
        |
        +--> Python Consumer (Q3)
        |
        +--> PyFlink Tumbling Window Job (Q4)
        |
        +--> PyFlink Session Window Job (Q5)
        |
        +--> PyFlink Hourly Tips Job (Q6)
                |
                v
           PostgreSQL
```

## Tech Stack

- `Redpanda`: Kafka-compatible message broker
- `Python`: producer and consumer scripts
- `PyFlink`: event-time window aggregations
- `PostgreSQL`: sink for Flink results
- `Docker Compose`: local infrastructure

## Final Answers

1. `Q1`: `v25.3.9`
2. `Q2`: `10 seconds`
3. `Q3`: `8506`
4. `Q4`: `74`
5. `Q5`: `81` in the homework form
6. `Q6`: `2025-10-16 18:00:00`

## Q5 Note

The computed longest session from both Flink and a direct parquet verification came out as `82`, but the homework choices top out at `81`. I submitted the multiple-choice value `81` because it is the only matching option in the assignment form.

## Repository Layout

- [STEP_BY_STEP.md](/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week7/STEP_BY_STEP.md): working notes and execution checklist
- [solutions/producer/green_producer.py](/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week7/solutions/producer/green_producer.py): Q2 producer
- [solutions/consumer/green_distance_consumer.py](/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week7/solutions/consumer/green_distance_consumer.py): Q3 consumer
- [solutions/flink/q4_tumbling_pu.py](/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week7/solutions/flink/q4_tumbling_pu.py): Q4 tumbling window job
- [solutions/flink/q5_session_pu.py](/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week7/solutions/flink/q5_session_pu.py): Q5 session window job
- [solutions/flink/q6_hourly_tips.py](/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week7/solutions/flink/q6_hourly_tips.py): Q6 hourly tips job
- [solutions/sql/q4_setup.sql](/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week7/solutions/sql/q4_setup.sql): Q4 Postgres table setup
- [solutions/sql/q5_setup.sql](/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week7/solutions/sql/q5_setup.sql): Q5 Postgres table setup
- [solutions/sql/q6_setup.sql](/Users/myatkaung/Desktop/MK_Data_Engineering/DataEngineering/week7/solutions/sql/q6_setup.sql): Q6 Postgres table setup

## Notes

- The official Zoomcamp `07-streaming/workshop` repo was cloned locally for development, but it is ignored from Git so this repository only keeps the homework-specific artifacts.
- The Flink Docker image needed extra setup on Apple Silicon: a full JDK, JNI headers, and native build tools so `pemja` could build successfully.
