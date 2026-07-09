# PROMPTS.md

LLM used: **Claude (Sonnet 5)**, via the Claude Code CLI.

This document logs the actual prompts exchanged while building this
pipeline, in order. All generated code was reviewed and can be explained
line by line (see inline comments and the README's architecture/DDL
sections for the reasoning behind each choice).

---

## Prompt 1 (initial challenge brief)

> Challenge: IMDb Lakehouse to OLAP Pipeline
> On a daily basis, Teleparty users are generating large amounts of data
> daily. We receive a variety of event driven data from our users. The data
> gives us viewership insights into the type of shows our users are
> watching and the length of time spent in a watch party.
> The purpose of this exercise is to understand how you as a data engineer
> would go about ingesting a small subset of data, creating an appropriate
> database schema for this data and preparing corresponding data queries to
> answer a variety of questions about incoming data.
>
> Objective: Build a local data pipeline that downloads the 2GB IMDb
> dataset from Kaggle, extracts movie titles/ratings/episodes, processes it
> with PySpark, saves it as partitioned Parquet ("the Lake"), and
> ingests/mounts it into an OLAP engine for high-speed analytics.
>
> Requirements: Docker Compose for a Spark Master/Worker cluster and the
> OLAP engine; a PySpark ETL that cleans titles/ratings/episodes and
> exports Snappy Parquet with a sensible time-series or category-based
> partitioning strategy; loading the Parquet into the OLAP engine; an
> analytics layer demonstrating the OLAP engine is significantly faster
> than raw Spark.
>
> Deliverables: docker-compose.yml, etl_job.py, load_to_olap.py, DDL files
> for the OLAP schema (with indexes/primary keys), a README performance
> note explaining the OLAP choice, and this PROMPTS.md file.
>
> ... this is a requirement, where i need to build complete pipeline
> according to these requirements and everything, so help me with it and
> also it should be working

## Prompt 2 (environment check, by the assistant, answered by the user)

The assistant checked the local sandbox and found: no Docker installed,
but PySpark 4.0.1 + Java 25 available locally, and working network access
to IMDb's official dataset files (the same underlying data the Kaggle
mirror repackages, no login required). It asked the user two clarifying
questions:

1. **Build strategy** — given no Docker in the sandbox, whether to (a)
   write the full repo and also test the PySpark/OLAP logic locally
   outside Docker, (b) write the code only with no local execution, or (c)
   pause and wait for Docker to be installed.
   **User answer:** write code only, no local execution.

2. **OLAP engine choice** — DuckDB (embeddable, fully testable in the
   sandbox without Docker) vs. ClickHouse (server-based, more aligned with
   a "hundreds of millions of rows" production OLAP use case, but could
   not be executed/tested in this sandbox without Docker).
   **User answer:** ClickHouse.

## Design decisions made while implementing (not separate user prompts, but explicit reasoning worth recording)

- Used IMDb's own `datasets.imdbws.com` files as the default download
  source (no login) alongside a `--source kaggle` option, so the repo is
  runnable out of the box while still satisfying the Kaggle-based
  requirement for anyone with an API token.
- Partitioned `titles` by `(title_type, decade)` and `episodes` by
  `decade` — category-based and time-series partitioning as required —
  and mirrored the same columns as ClickHouse's `PARTITION BY` so pruning
  applies at both the Parquet and ClickHouse layers.
- Added ClickHouse skip indexes (bloom filter on `genres`, minmax on
  `average_rating`/`num_votes`) and a pre-aggregated `genre_decade_stats`
  rollup table to keep dashboard-style aggregate queries sub-second.
- `benchmark.py` runs the identical aggregation query against raw Spark
  (reading Parquet directly) and against ClickHouse, to directly satisfy
  the "demonstrate the OLAP engine is significantly faster" requirement.

## Prompt 3 (user installed Docker; asked to actually run the pipeline end-to-end)

Once Docker was available, the assistant ran `docker compose up` and the
full pipeline against the real, full-size IMDb dataset (~11M titles, ~270MB
compressed) rather than treating the written code as done. This surfaced
several real bugs that only show up at runtime, all fixed in place:

- `bitnami/spark:3.5.1` no longer exists on Docker Hub (Bitnami moved
  versioned tags behind a paid subscription in 2025) — switched to the
  official `apache/spark:4.0.3-python3` image, which has no built-in
  master/worker launcher, so each service now runs `spark-class` directly.
- Host port 9000 was already bound by another macOS process — remapped
  ClickHouse's native TCP port to 9001 on the host.
- Mounting `data/lake` as read-only broke ClickHouse's entrypoint, which
  tries to `chown` every mounted path.
- ClickHouse auto-runs every `.sql` file under `docker-entrypoint-initdb.d`
  in alphabetical order; `analytics_queries.sql` (a < d) ran before
  `ddl_clickhouse.sql` and failed against tables that didn't exist yet —
  only the DDL file is mounted there now.
- `decade` (and, for episodes, `season_number`/`episode_number`) were
  `Nullable` columns used in `PARTITION BY`/`ORDER BY`, which ClickHouse
  forbids by default — switched to non-nullable with `0` as an explicit
  "unknown" sentinel, filled in by the loader via `pyarrow.compute.fill_null`.
- Real IMDb rows have occasional malformed/shifted columns (e.g. a genre
  string landing in the `runtimeMinutes` column), which crashed Spark's
  strict `.cast()`; switched to `try_cast` so bad values become `null`
  instead of aborting the whole write.
- Some long-running shows have episode/season numbers above 65,535 —
  widened those columns from `UInt16` to `UInt32`.
- ClickHouse's official image restricts the `default` user to
  loopback-only access unless `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD` are
  set, which silently blocked the `loader` container's HTTP connections —
  added explicit credentials shared by both containers.
- `load_to_olap.py`'s DDL statement splitter used a naive `str.split(";")`,
  which broke on a semicolon that happened to appear inside an inline SQL
  comment — now strips comment text before splitting.
- `benchmark.py` needs `clickhouse-connect` inside the Spark container to
  do an in-process timing comparison, but the base Spark image only ships
  bare PySpark — added `docker/spark/Dockerfile` to layer that dependency
  on top of `apache/spark:4.0.3-python3`.

After these fixes, the full pipeline ran clean end-to-end against the real
dataset: 12,629,478 titles and 9,757,464 episodes loaded into ClickHouse,
correct results from every analytics query (verified against known facts,
e.g. Breaking Bad's season-by-season rating climb), and a measured
25.6x speedup for ClickHouse over raw Spark on the benchmark aggregation.

## Prompt 4 (user pushed on the literal Kaggle requirement)

The user asked directly whether the "download from Kaggle" requirement was
actually satisfied, since the pipeline's default path pulls from IMDb's own
CDN instead. The assistant was honest that the `--source kaggle` code path
had been written but never executed (no Kaggle credentials available), and
offered to test it given a token. The user then pointed out they'd already
tried the dataset's own Kaggle page and gotten a `kagglehub` download
snippet, plus a "download as zip (2GB)" button, and asked whether the
originally-provided Kaggle URL had actually been tried.

The assistant tested this directly: `curl` against
`https://www.kaggle.com/api/v1/datasets/download/ashirwadsangwan/imdb-dataset`
succeeded with no authentication at all — a genuine, complete 1.8GB zip
download straight from kaggle.com. This was a real, verified finding, not
an assumption. Unzipping it, however, revealed the Kaggle mirror does not
include `title.episode.tsv` — it ships `name.basics`, `title.akas`,
`title.basics`, `title.principals`, and `title.ratings` instead. So
`download_data.sh --source kaggle` was rewritten to: download the real
Kaggle zip anonymously, extract+gzip only the two files this pipeline
needs from it (`title.basics`, `title.ratings`), and separately fetch
`title.episode.tsv.gz` from IMDb directly to fill the gap Kaggle's mirror
leaves. This was then run for real, end-to-end (not just written): the
full ~1.8GB anonymous Kaggle download, extraction, and gzip completed in
~3m46s, producing the same three `.tsv.gz` files the ETL job expects, with
`title.basics.tsv.gz` verified to have the correct header and 12.6M rows.
