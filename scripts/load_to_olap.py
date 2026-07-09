import os
import time

import clickhouse_connect
import pyarrow.compute as pc
import pyarrow.dataset as ds

LAKE_PATH = os.environ.get("LAKE_PATH", "data/lake")
DDL_PATH = os.environ.get("DDL_PATH", "sql/ddl_clickhouse.sql")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

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

BOOL_COLUMNS = ("is_adult", "has_rating")

# columns that are 0 = "unknown" sentinels rather than Nullable, because
# they're part of ClickHouse's PARTITION BY / ORDER BY (see sql/ddl_clickhouse.sql)
SENTINEL_COLUMNS = (
    ("decade", "uint16"),
    ("season_number", "uint32"),
    ("episode_number", "uint32"),
)


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD
    )


def split_sql_statements(ddl_text):
    stripped = "\n".join(line.split("--", 1)[0] for line in ddl_text.splitlines())
    return [statement.strip() for statement in stripped.split(";") if statement.strip()]


def cast_bool_columns(table):
    for col in BOOL_COLUMNS:
        if col in table.column_names:
            table = table.set_column(table.column_names.index(col), col, table.column(col).cast("uint8"))
    return table


def fill_sentinel_columns(table):
    for col, arrow_type in SENTINEL_COLUMNS:
        if col in table.column_names:
            filled = pc.fill_null(table.column(col), 0).cast(arrow_type)
            table = table.set_column(table.column_names.index(col), col, filled)
    return table


def apply_ddl(client):
    with open(DDL_PATH) as f:
        ddl_text = f.read()
    for statement in split_sql_statements(ddl_text):
        client.command(statement)
    print("DDL applied.")


def load_table(client, table_fqn, lake_subdir, columns):
    dataset_path = os.path.join(LAKE_PATH, lake_subdir)
    dataset = ds.dataset(dataset_path, format="parquet", partitioning="hive")

    total_rows = 0
    start = time.time()
    for fragment in dataset.get_fragments():
        table = fragment.to_table(schema=dataset.schema)
        table = cast_bool_columns(table)
        table = fill_sentinel_columns(table)
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
