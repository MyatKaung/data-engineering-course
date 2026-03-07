from __future__ import annotations

import glob
import os

from pyspark.sql import SparkSession, functions as F


def main() -> None:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("de_zoomcamp_hw6")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR") 


    yellow_df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
    zones_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("taxi_zone_lookup.csv")
    )

    # Q2
    output_dir = "yellow_2025_11_repartitioned"
    yellow_df.repartition(4).write.mode("overwrite").parquet(output_dir)
    parquet_files = glob.glob(f"{output_dir}/*.parquet")
    parquet_sizes = [os.path.getsize(path) for path in parquet_files]
    avg_size_mb = sum(parquet_sizes) / len(parquet_sizes) / (1024 * 1024)

    # Q3
    trips_on_15th = yellow_df.filter(
        F.to_date("tpep_pickup_datetime") == F.lit("2025-11-15")
    ).count()

    # Q4
    longest_trip_hours = (
        yellow_df.select(
            (
                (
                    F.unix_timestamp("tpep_dropoff_datetime")
                    - F.unix_timestamp("tpep_pickup_datetime")
                )
                / 3600.0
            ).alias("trip_hours")
        )
        .agg(F.max("trip_hours").alias("max_trip_hours"))
        .first()["max_trip_hours"]
    )

    # Q6
    least_frequent_zone = (
        yellow_df.join(
            zones_df,
            yellow_df["PULocationID"] == zones_df["LocationID"],
            "left",
        )
        .groupBy("Zone")
        .count()
        .orderBy(F.col("count").asc(), F.col("Zone").asc())
        .first()
    )

    print(f"Q2 avg parquet size (MB): {avg_size_mb:.2f}")
    print(f"Q3 trips on 2025-11-15: {trips_on_15th}")
    print(f"Q4 longest trip (hours): {float(longest_trip_hours):.1f}")
    print(f"Q6 least frequent pickup zone: {least_frequent_zone['Zone']}")
    print("Q5 Spark UI local port: 4040")


    print("\n" + "="*40)
    print("SPARK UI IS LIVE at: http://localhost:4040")
    print("Press ENTER in this terminal to stop Spark and exit.")
    print("="*40)
    input() 



    spark.stop()


if __name__ == "__main__":
    main()
