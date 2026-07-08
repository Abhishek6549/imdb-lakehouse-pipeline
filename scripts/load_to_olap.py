"""
load_to_olap.py — loads the partitioned Parquet lake into ClickHouse.

Applies the DDL in sql/ddl_clickhouse.sql (idempotent, CREATE ... IF NOT
EXISTS), then streams each Parquet file under data/lake/{titles,episodes}
into the matching ClickHouse table using Arrow (no intermediate CSV, no
pandas copy), and finally materializes the genre_decade_stats rollup with a
single INSERT INTO ... SELECT run inside ClickHouse itself.

Run inside the `loader` container (has clickhouse-connect + pyarrow):
    docker compose exec loader python /opt/scripts/load_to_olap.py

Env vars (all have Docker Compose defaults):
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, LAKE_PATH
"""

import glob
import os
import time

import clickhouse_connect
import pyarrow.dataset as ds

LAKE_PATH = os.environ.get("LAKE_PATH", "data/lake")
DDL_PATH = os.environ.get("DDL_PATH", "sql/ddl_clickhouse.sql")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))

# Rename Arrow/Parquet columns -> ClickHouse column names where they differ,
# and cast booleans (Parquet) to UInt8 (ClickHouse) at insert time.
TITLES_COLUMNS = [
    "tconst",
    "title_type",
    "primary_title",
    "original_title",
    "is_adult",
    "start_year",
    "end_year",
    "runtime_minutes",
    "genres",
    "decade",
    "average_rating",
    "num_votes",
    "has_rating",
]

EPISODES_COLUMNS = [
    "episode_tconst",
    "parent_tconst",
    "episode_title",
    "series_title",
    "season_number",
    "episode_number",
    "series_start_year",
    "decade",
    "average_rating",
    "num_votes",
]


def get_client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)


def apply_ddl(client):
    with open(DDL_PATH) as f:
        raw_lines = f.readlines()
    # drop full-line comments before splitting into statements, otherwise a
    # leading "-- comment" line would make str.startswith("--") swallow the
    # real statement that follows it in the same chunk.
    ddl = "\n".join(line for line in raw_lines if not line.strip().startswith("--"))
    for statement in ddl.split(";"):
        statement = statement.strip()
        if not statement:
            continue
        client.command(statement)
    print("DDL applied.")


def load_table(client, table_fqn, lake_subdir, columns):
    dataset_path = os.path.join(LAKE_PATH, lake_subdir)
    dataset = ds.dataset(dataset_path, format="parquet", partitioning="hive")

    total_rows = 0
    start = time.time()
    for fragment in dataset.get_fragments():
        # pass the full dataset schema so hive partition columns
        # (title_type=/decade=/...) are materialized as real columns
        table = fragment.to_table(schema=dataset.schema)
        # bool -> uint8 for ClickHouse's UInt8 columns
        for bool_col in ("is_adult", "has_rating"):
            if bool_col in table.column_names:
                table = table.set_column(
                    table.column_names.index(bool_col),
                    bool_col,
                    table.column(bool_col).cast("uint8"),
                )
        # keep only + order the columns the target table expects
        table = table.select([c for c in columns if c in table.column_names])
        client.insert_arrow(table_fqn, table)
        total_rows += table.num_rows

    elapsed = time.time() - start
    print(f"Loaded {total_rows} rows into {table_fqn} in {elapsed:.2f}s")
    return total_rows


def build_genre_decade_rollup(client):
    client.command("TRUNCATE TABLE IF EXISTS imdb.genre_decade_stats")
    client.command(
        """
        INSERT INTO imdb.genre_decade_stats
        SELECT
            genre,
            decade,
            count() AS title_count,
            avg(average_rating) AS avg_rating,
            sum(num_votes) AS total_votes
        FROM imdb.titles
        ARRAY JOIN genres AS genre
        WHERE has_rating = 1
        GROUP BY genre, decade
        """
    )
    print("genre_decade_stats rollup materialized.")


def main():
    client = get_client()
    apply_ddl(client)
    load_table(client, "imdb.titles", "titles", TITLES_COLUMNS)
    load_table(client, "imdb.episodes", "episodes", EPISODES_COLUMNS)
    build_genre_decade_rollup(client)

    titles_count = client.command("SELECT count() FROM imdb.titles")
    episodes_count = client.command("SELECT count() FROM imdb.episodes")
    print(f"imdb.titles total rows: {titles_count}")
    print(f"imdb.episodes total rows: {episodes_count}")


if __name__ == "__main__":
    main()
