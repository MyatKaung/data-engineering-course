# Week 6: Batch Processing with Spark

This project contains the homework solutions for Week 6 of the Data Engineering Zoomcamp. The tasks involve processing NYC Yellow Taxi trip data using PySpark.

## Dataset
- **Yellow Taxi Data**: November 2025
- **Zones Lookup**: [taxi_zone_lookup.csv](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)

## Prerequisites
- **Java**: Version 17
- **Spark**: 3.x
- **Python**: 3.11+
- **PySpark**: Installed via virtual environment

## Setup & Execution

### 1. Environment Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install pyspark
```

### 2. Run Homework Script
```bash
python homework_6.py
```

## Solution Logic & Results

### Q2: Average Parquet Size (MB)
- **Logic**: Repartitioned the dataset into 4 partitions using `df.repartition(4)` and calculated the average file size of the resulting `.parquet` files using `os.path.getsize`.
- **Result**: **24.41 MB** (~25MB)

### Q3: Trips on 2025-11-15
- **Logic**: Filtered the `tpep_pickup_datetime` using `F.to_date` to isolate trips starting on Nov 15th and performed a `.count()`.
- **Result**: **162,604**

### Q4: Longest Trip (hours)
- **Logic**: Calculated the difference between `dropoff` and `pickup` timestamps using `F.unix_timestamp`, divided by 3,600 to convert seconds to hours, and found the maximum value.
- **Result**: **90.6 hours**

### Q5: Spark UI Port
- **Logic**: The default local port for the Spark application dashboard.
- **Result**: **4040**

### Q6: Least frequent pickup zone
- **Logic**: Joined the trip data with the zone lookup table, grouped by `Zone`, counted occurrences, and sorted in ascending order to find the first (least frequent) entry.
- **Result**: **Arden Heights**


