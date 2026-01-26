import pandas as pd
from sqlalchemy import create_engine

# Use port 5433 for localhost access
engine = create_engine('postgresql://postgres:postgres@localhost:5433/ny_taxi')

def run_query(query):
    return pd.read_sql(query, engine)

# Q3. Counting short trips
print("\n--- Question 3 ---")
q3 = """
SELECT count(*) as count
FROM green_taxi_trips 
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01' 
  AND trip_distance <= 1;
"""
print(run_query(q3))

# Q4. Longest trip for each day
print("\n--- Question 4 ---")
q4 = """
SELECT date(lpep_pickup_datetime) as pickup_day, MAX(trip_distance) as max_distance 
FROM green_taxi_trips 
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance < 100 
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 1;
"""
print(run_query(q4))

# Q5. Biggest pickup zone
print("\n--- Question 5 ---")
q5 = """
SELECT z."Zone", SUM(t.total_amount) as total 
FROM green_taxi_trips t 
JOIN zones z ON t."PULocationID" = z."LocationID" 
WHERE date(t.lpep_pickup_datetime) = '2025-11-18' 
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 1;
"""
print(run_query(q5))

# Q6. Largest tip
print("\n--- Question 6 ---")
q6 = """
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
"""
print(run_query(q6))
