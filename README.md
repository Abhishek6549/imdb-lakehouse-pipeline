# IMDb Lakehouse → OLAP Pipeline

A local pipeline that cleans the IMDb dataset with PySpark, writes it out
as a partitioned Parquet lake, and loads it into ClickHouse for
sub-second analytics — with a benchmark proving ClickHouse is faster than
querying the lake with Spark directly.

```
IMDb / Kaggle  --->  Spark cluster  --->  Parquet lake  --->  ClickHouse  --->  Analytics
  data/raw/         etl_job.py         data/lake/        load_to_olap.py    sql/analytics_queries.sql
```

## Architecture

| Component | Role |
|---|---|
| `spark-master` / `spark-worker` | Apache Spark 4.0 cluster running `etl_job.py`. |
| `clickhouse` | The OLAP engine. Serves all analytics queries. |
| `loader` | Small Python container that runs `load_to_olap.py` and `benchmark.py`. |

## Prerequisites

- Docker + Docker Compose
- ~5GB free disk

## Quick start

```bash
# 1. Configure ClickHouse credentials (read from .env by docker-compose.yml)
cp .env.example .env && $EDITOR .env

# 2. Build and start everything
docker compose build spark-master spark-worker loader
docker compose up -d spark-master spark-worker clickhouse loader

# 3. Get the raw data (see "Data source" below for --source kaggle)
./scripts/download_data.sh --source imdb

# 4. Run the PySpark ETL: raw TSVs -> partitioned Parquet lake
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/scripts/etl_job.py --raw-dir /opt/data/raw --lake-dir /opt/data/lake

# 5. Load the lake into ClickHouse (applies sql/ddl_clickhouse.sql, then inserts)
docker compose exec loader python /opt/scripts/load_to_olap.py

# 6. Run the analytics queries
docker compose exec clickhouse clickhouse-client --database imdb --multiquery \
  < sql/analytics_queries.sql

# 7. Benchmark: ClickHouse vs. raw Spark on the same aggregation
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /opt/scripts/benchmark.py \
  --lake-dir /opt/data/lake --clickhouse-host clickhouse \
  --clickhouse-user default --clickhouse-password "$(grep CLICKHOUSE_PASSWORD .env | cut -d= -f2)"
```

Spark UI: http://localhost:8080 · ClickHouse HTTP: http://localhost:8123

On a full run (~12.6M titles, ~9.8M episodes), step 7 measures ClickHouse
at **25–30x faster** than raw Spark on the same aggregate query.

## Why ClickHouse

- **Built for OLAP, not ETL.** Spark's per-query cost includes driver/executor
  coordination and JVM codegen — fine for a one-time batch job over the raw
  data, but that fixed overhead dominates a query that should take
  milliseconds. ClickHouse has none of it: single process, vectorized execution.
- **Column-oriented storage.** An aggregation like `avg(rating) GROUP BY decade`
  only touches the columns it needs.
- **Partition pruning + skip indexes.** The `PARTITION BY`/`ORDER BY` in the
  DDL let ClickHouse skip whole partitions and index ranges before touching
  data, on top of explicit bloom-filter/minmax indexes.
- Spark stays for what it's good at (distributed cleaning/joining of the
  raw multi-GB dataset); ClickHouse takes over for low-latency reads.
  `benchmark.py` measures the difference directly rather than asserting it.

## Design notes

**Partitioning.** `titles` is partitioned by `(title_type, decade)` —
category-based (movies vs. series vs. shorts) and time-series in one
scheme. `episodes` is partitioned by `decade` alone, since episode
questions are almost always about trends over time. ClickHouse's
`PARTITION BY` mirrors the same columns, so pruning happens at both the
Parquet and ClickHouse layers.

**Schema.** See [`sql/ddl_clickhouse.sql`](sql/ddl_clickhouse.sql):
`imdb.titles` and `imdb.episodes` are `MergeTree` tables with primary
keys matching their partition columns, plus bloom-filter/minmax skip
indexes on `genres`, `average_rating`, and `num_votes`. `decade` and
`season_number`/`episode_number` are non-nullable (`0` = unknown) since
ClickHouse forbids `Nullable` columns in `PARTITION BY`/`ORDER BY`.
`imdb.genre_decade_stats` is a pre-aggregated rollup materialized once at
load time, for dashboard-style genre queries.

**Data source.** `scripts/download_data.sh` supports two sources:

- `--source kaggle` — downloads the real dataset from Kaggle's own
  API (works anonymously for this public dataset, no token required;
  falls back to the `kaggle` CLI if that ever changes). Note: Kaggle's
  mirror doesn't include `title.episode.tsv`, so that one file is always
  pulled from IMDb directly regardless of source.
- `--source imdb` (default) — pulls all three files directly from IMDb.

## Repo layout

```
docker-compose.yml      Spark cluster + ClickHouse + loader
docker/spark/            Dockerfile: apache/spark + clickhouse-connect
docker/loader/           Dockerfile for the loader container
scripts/
  download_data.sh       Kaggle or direct-from-IMDb download
  etl_job.py              PySpark transform -> partitioned Parquet lake
  load_to_olap.py         Parquet lake -> ClickHouse
  benchmark.py            Spark vs. ClickHouse timing comparison
sql/
  ddl_clickhouse.sql      Table DDL (engine, partitioning, indexes)
  analytics_queries.sql   Sample analytics queries
requirements.txt         For running scripts on the host, without Docker
PROMPTS.md               LLM usage log
```
