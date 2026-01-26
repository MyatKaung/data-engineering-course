# Module 1 Homework: Docker & SQL

## Answers

**Question 1. Understanding Docker images**
- **Answer**: `25.3`
- *Command used*: `docker run --rm python:3.13 pip --version`

**Question 2. Understanding Docker networking and docker-compose**
- **Answer**: `postgres:5432` (or `db:5432`)
- *Reasoning*: The service name is `db`, container name is `postgres`. Internal port is `5432`.

**Question 3. Counting short trips**
- **Answer**: `8,007`
- *Query*:
```sql
SELECT count(*) 
FROM green_taxi_trips 
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01' 
  AND trip_distance <= 1;
```

**Question 4. Longest trip for each day**
- **Answer**: `2025-11-14`
- *Query*:
```sql
SELECT date(lpep_pickup_datetime) as pickup_day, MAX(trip_distance) as max_distance 
FROM green_taxi_trips 
WHERE lpep_pickup_datetime >= '2025-11-01' AND lpep_pickup_datetime < '2025-12-01'
AND trip_distance < 100 
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 1;
```

**Question 5. Biggest pickup zone**
- **Answer**: `East Harlem North`
- *Query*:
```sql
SELECT z."Zone", SUM(t.total_amount) as total 
FROM green_taxi_trips t 
JOIN zones z ON t."PULocationID" = z."LocationID" 
WHERE date(t.lpep_pickup_datetime) = '2025-11-18' 
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 1;
```

**Question 6. Largest tip**
- **Answer**: `Yorkville West`
- *Query*:
```sql
SELECT z_do."Zone", MAX(t.tip_amount) as max_tip 
FROM green_taxi_trips t 
JOIN zones z_pu ON t."PULocationID" = z_pu."LocationID" 
JOIN zones z_do ON t."DOLocationID" = z_do."LocationID" 
WHERE z_pu."Zone" = 'East Harlem North' 
  AND lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01' 
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 1;
```

**Question 7. Terraform Workflow**
- **Answer**: `terraform init, terraform apply -auto-approve, terraform destroy`

## Project Structure

```
Week1/
├── docker-compose.yaml      # Postgres and pgAdmin services
├── ingest_data.py           # Python script to ingest data
├── solve_homework.py        # Python script to run queries
├── terraform/               # Terraform config files
│   ├── main.tf
│   └── variables.tf
└── green_tripdata_2025-11.parquet (Downloaded)
└── taxi_zone_lookup.csv (Downloaded)
```

## How to Run

1. **Start Docker Infrastructure**:
   ```bash
   docker-compose up -d
   ```

2. **Setup Python Environment**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install pandas sqlalchemy psycopg2-binary pyarrow
   ```

3. **Ingest Data**:
   ```bash
   python ingest_data.py
   ```

4. **Solve Questions**:
   ```bash
   python solve_homework.py
   ```

## Terraform
To manage GCP resources:
1. Navigate to `terraform/`.
2. Update `variables.tf` with your project ID.
3. Run `terraform init`.
4. Run `terraform apply`.
