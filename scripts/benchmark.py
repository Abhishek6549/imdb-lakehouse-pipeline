"""
benchmark.py — proves the "OLAP engine is significantly faster than raw
Spark" requirement by running the same aggregation query two ways:

  1. Raw PySpark reading directly from the Parquet lake (no cluster
     coordination overhead removed — this is exactly how an analyst would
     query the lake without an OLAP layer).
  2. ClickHouse, querying the already-loaded imdb.titles table.

Both computes: average rating and title count per (title_type, decade).

Run inside the loader container (has pyspark not required — uses
spark-submit from the spark-master container for a fair comparison instead):

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /opt/scripts/benchmark.py \
        --lake-dir /opt/data/lake --clickhouse-host clickhouse \
  --clickhouse-user default --clickhouse-password imdb_pipeline
"""

import argparse
import os
import time

import clickhouse_connect
from pyspark.sql import SparkSession, functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="Spark vs ClickHouse benchmark")
    parser.add_argument("--lake-dir", default="data/lake")
    parser.add_argument("--clickhouse-host", default="localhost")
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-user", default=os.environ.get("CLICKHOUSE_USER", "default"))
    parser.add_argument("--clickhouse-password", default=os.environ.get("CLICKHOUSE_PASSWORD", ""))
    parser.add_argument("--runs", type=int, default=3, help="number of timed runs to average")
    return parser.parse_args()


def time_it(fn, runs):
    timings = []
    for _ in range(runs):
        start = time.perf_counter()
        fn()
        timings.append(time.perf_counter() - start)
    return min(timings), sum(timings) / len(timings)


def run_spark_query(spark, lake_dir):
    df = spark.read.parquet(f"{lake_dir}/titles")
    result = (
        df.filter(F.col("has_rating"))
        .groupBy("title_type", "decade")
        .agg(F.avg("average_rating").alias("avg_rating"), F.count("*").alias("title_count"))
        .orderBy("title_type", "decade")
    )
    result.collect()  # force full execution


def run_clickhouse_query(client):
    client.query(
        """
        SELECT title_type, decade, avg(average_rating) AS avg_rating, count() AS title_count
        FROM imdb.titles
        WHERE has_rating = 1
        GROUP BY title_type, decade
        ORDER BY title_type, decade
        """
    )


def main():
    args = parse_args()

    spark = SparkSession.builder.appName("imdb-benchmark").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    client = clickhouse_connect.get_client(
        host=args.clickhouse_host,
        port=args.clickhouse_port,
        username=args.clickhouse_user,
        password=args.clickhouse_password,
    )

    spark_best, spark_avg = time_it(lambda: run_spark_query(spark, args.lake_dir), args.runs)
    ch_best, ch_avg = time_it(lambda: run_clickhouse_query(client), args.runs)

    print("\n=== Benchmark: avg rating + count by (title_type, decade) ===")
    print(f"{'engine':<12}{'best (s)':<12}{'avg (s)':<12}")
    print(f"{'Spark':<12}{spark_best:<12.3f}{spark_avg:<12.3f}")
    print(f"{'ClickHouse':<12}{ch_best:<12.3f}{ch_avg:<12.3f}")
    if ch_best > 0:
        print(f"\nClickHouse was {spark_best / ch_best:.1f}x faster than Spark on best-case latency.")

    spark.stop()


if __name__ == "__main__":
    main()
