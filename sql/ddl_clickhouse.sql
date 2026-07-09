-- ddl_clickhouse.sql
-- Schema for the OLAP layer. Mirrors the partitioning strategy used when
-- writing the Parquet lake (title_type + decade) so ClickHouse's own
-- partition pruning lines up with how the data was already organized
-- upstream by Spark.
--
-- Auto-applied on first container start via docker-entrypoint-initdb.d
-- (see docker-compose.yml), or run manually with:
--   docker compose exec clickhouse clickhouse-client --multiquery < sql/ddl_clickhouse.sql

CREATE DATABASE IF NOT EXISTS imdb;

CREATE TABLE IF NOT EXISTS imdb.titles
(
    tconst           String,
    title_type       LowCardinality(String),
    primary_title    String,
    original_title   String,
    is_adult         UInt8,
    start_year       Nullable(UInt16),
    end_year         Nullable(UInt16),
    runtime_minutes  Nullable(UInt32),
    genres           Array(String),
    decade           UInt16, -- 0 = unknown/missing start_year (kept out of Nullable: used in PARTITION BY / ORDER BY)
    average_rating   Nullable(Float32),
    num_votes        Nullable(UInt32),
    has_rating       UInt8,

    -- data-skipping indexes: let ClickHouse skip whole granules without
    -- decompressing them, on top of partition pruning.
    INDEX idx_genres genres TYPE bloom_filter GRANULARITY 4,
    INDEX idx_rating average_rating TYPE minmax GRANULARITY 4,
    INDEX idx_votes num_votes TYPE minmax GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY (title_type, decade)
ORDER BY (title_type, decade, tconst)
PRIMARY KEY (title_type, decade, tconst)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS imdb.episodes
(
    episode_tconst    String,
    parent_tconst     String,
    episode_title     Nullable(String),
    series_title      Nullable(String),
    season_number     UInt32, -- 0 = unknown; some long-running talk shows exceed UInt16 (part of the sorting key, so can't be Nullable)
    episode_number    UInt32, -- 0 = unknown; same reasoning
    series_start_year Nullable(UInt16),
    decade            UInt16, -- 0 = unknown (see imdb.titles)
    average_rating    Nullable(Float32),
    num_votes         Nullable(UInt32),

    INDEX idx_ep_rating average_rating TYPE minmax GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY decade
ORDER BY (parent_tconst, season_number, episode_number)
PRIMARY KEY (parent_tconst, season_number, episode_number)
SETTINGS index_granularity = 8192;

-- Pre-aggregated rollup used by dashboards that slice ratings by genre and
-- decade. Populated once after load via `INSERT INTO ... SELECT` (see
-- load_to_olap.py) rather than kept live, since the lake is batch-refreshed.
CREATE TABLE IF NOT EXISTS imdb.genre_decade_stats
(
    genre          LowCardinality(String),
    decade         UInt16,
    title_count    UInt64,
    avg_rating     Float32,
    total_votes    UInt64
)
ENGINE = MergeTree
ORDER BY (genre, decade);
