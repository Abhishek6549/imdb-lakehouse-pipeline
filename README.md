# IMDb Lakehouse -> OLAP Pipeline

A local pipeline that turns the IMDb non-commercial dataset into a
partitioned Parquet lake with PySpark, then loads it into ClickHouse for
sub-second analytics.

```
Kaggle / IMDb TSVs  --->  Spark cluster (docker)  --->  Parquet lake (Snappy, partitioned)  --->  ClickHouse  --->  Analytics queries
   data/raw/                 etl_job.py                     data/lake/                        load_to_olap.py       sql/analytics_queries.sql
```

## Architecture

| Component      | Role                                                                 |
|----------------|-----------------------------------------------------------------------|
| `spark-master` / `spark-worker` | Apache Spark 4.0 cluster (official `apache/spark` image) that runs `etl_job.py` to clean, join, and partition the raw IMDb TSVs into Parquet. |
| `clickhouse`   | OLAP engine. Serves all analytics queries against the loaded lake.    |
| `loader`       | Throwaway Python container (clickhouse-connect + pyarrow) used to run `load_to_olap.py` / `benchmark.py`. |

## Data

The Kaggle dataset ([ashirwadsangwan/imdb-dataset](https://www.kaggle.com/datasets/ashirwadsangwan/imdb-dataset))
is a repackaging of IMDb's own [non-commercial datasets](https://developer.imdb.com/non-commercial-datasets/).
It ships `name.basics`, `title.akas`, `title.basics`, `title.principals`, and
`title.ratings` — notably **not** `title.episode`, so episode data is always
pulled from IMDb directly regardless of which `--source` you use. This
pipeline needs three files in the end:

- `title.basics.tsv.gz` — titles, type, name, year, runtime, genres
- `title.ratings.tsv.gz` — average rating + vote count per title
- `title.episode.tsv.gz` — episode -> parent series, season/episode number

`scripts/download_data.sh` supports either source:

```bash
# Downloads the real ~1.8GB Kaggle dataset via Kaggle's own dataset-download
# API — works anonymously for this public dataset, no token needed. Falls
# back to the `kaggle` CLI (needs ~/.kaggle/kaggle.json, see .env.example)
# if that endpoint ever requires auth. Missing title.episode is fetched
# from IMDb directly since Kaggle's mirror doesn't include it.
./scripts/download_data.sh --source kaggle

# No login required at all — pulls the same underlying IMDb data directly
./scripts/download_data.sh --source imdb
```

## Running it end-to-end

```bash
# 0. Set a ClickHouse password (read by docker-compose.yml via .env)
cp .env.example .env && $EDITOR .env

# 1. Start the Spark cluster and ClickHouse
docker compose build spark-master spark-worker loader
docker compose up -d spark-master spark-worker clickhouse loader

# 2. Get the raw data
./scripts/download_data.sh --source imdb

# 3. Run the PySpark transformation (writes data/lake/titles, data/lake/episodes)
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/scripts/etl_job.py --raw-dir /opt/data/raw --lake-dir /opt/data/lake

# 4. Load the lake into ClickHouse (applies sql/ddl_clickhouse.sql, then inserts)
docker compose exec loader python /opt/scripts/load_to_olap.py

# 5. Run the analytics queries
docker compose exec clickhouse clickhouse-client --database imdb --multiquery \
  < sql/analytics_queries.sql

# 6. Prove ClickHouse is faster than raw Spark on the same aggregation
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /opt/scripts/benchmark.py \
  --lake-dir /opt/data/lake --clickhouse-host clickhouse \
  --clickhouse-user default --clickhouse-password "$(grep CLICKHOUSE_PASSWORD .env | cut -d= -f2)"
```

Spark master UI: http://localhost:8080 · ClickHouse HTTP: http://localhost:8123

## Partitioning strategy

- **`titles`** partitioned by `(title_type, decade)` — `title_type` gives
  category-based pruning (movie/tvSeries/short analytics rarely mix types),
  `decade` gives time-series pruning (a trend query over the 2010s only
  scans that decade's files/parts).
- **`episodes`** partitioned by `decade` (derived from the parent series'
  start year), since episode-level questions are almost always
  time-series ("how did ratings trend across seasons/years").

The same partition columns are used as the `PARTITION BY` in the
ClickHouse DDL (`sql/ddl_clickhouse.sql`), so pruning happens at both the
Parquet-file level and the ClickHouse-part level.

## Schema / DDL

See [`sql/ddl_clickhouse.sql`](sql/ddl_clickhouse.sql):

- `imdb.titles` — `MergeTree`, `ORDER BY (title_type, decade, tconst)` as
  the sparse primary index, plus a bloom-filter skip index on `genres` and
  minmax skip indexes on `average_rating` / `num_votes` for fast range
  filters.
- `imdb.episodes` — `MergeTree`, `ORDER BY (parent_tconst, season_number,
  episode_number)` so per-series/season lookups hit a contiguous index
  range.
- `imdb.genre_decade_stats` — a pre-aggregated rollup (genre x decade)
  materialized once at load time for dashboard-style queries that would
  otherwise `ARRAY JOIN` the full `genres` array on every request.

## Performance note: why ClickHouse

- **Purpose-built for OLAP, not ETL.** Spark's per-query cost includes
  driver/executor coordination, task scheduling, and JVM codegen —
  overhead that's worth it for a one-time batch join across 2GB+ of raw
  data, but fixed cost that dominates a query that should take
  milliseconds. ClickHouse has none of that: a single process, vectorized
  execution, no cluster coordination for a query this size.
- **Column-oriented storage + compression.** Each column is stored and
  compressed separately, so an aggregation like `avg(average_rating)
  GROUP BY decade` only reads the two columns it needs, not the whole
  row — versus Parquet-via-Spark, which still pays JVM/DataFrame
  overhead per task even with columnar pruning.
- **Sparse primary index + partition pruning.** The `ORDER BY` /
  `PARTITION BY` clauses mean ClickHouse can skip entire partitions and
  large index ranges before touching data, on top of the explicit
  bloom-filter/minmax skip indexes for `genres`, `average_rating`, and
  `num_votes`.
- **Right tool for the right stage.** Spark stays in the pipeline for
  what it's actually good at — distributed cleaning/joining/partitioning
  of the raw multi-GB dataset — while ClickHouse takes over for the
  read-heavy, low-latency analytics workload. `scripts/benchmark.py`
  runs the identical aggregation against both engines and prints the
  speedup.

## Repo layout

```
docker-compose.yml        Spark cluster + ClickHouse + loader
docker/spark/              Dockerfile adding clickhouse-connect to apache/spark
docker/loader/              Dockerfile + deps for the loader container
scripts/
  download_data.sh        Kaggle or direct-from-IMDb download
  etl_job.py               PySpark transform -> partitioned Parquet lake
  load_to_olap.py          Parquet lake -> ClickHouse
  benchmark.py             Spark vs. ClickHouse timing comparison
sql/
  ddl_clickhouse.sql       Table DDL (engine, partitioning, indexes)
  analytics_queries.sql    Sample analytics queries
requirements.txt           For running scripts on the host without Docker
PROMPTS.md                 LLM usage log for this challenge
```
