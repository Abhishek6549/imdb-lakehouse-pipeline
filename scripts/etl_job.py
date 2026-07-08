"""
etl_job.py — the PySpark transformation step of the IMDb lakehouse pipeline.

Reads the raw IMDb TSV exports (title.basics, title.ratings, title.episode),
cleans/casts/joins them, and writes two Snappy-compressed, partitioned Parquet
datasets into the "lake":

    data/lake/titles/title_type=<...>/decade=<...>/*.parquet
    data/lake/episodes/decade=<...>/*.parquet

Partitioning strategy:
  - `titles` is partitioned by (title_type, decade). title_type gives
    category-based pruning (movie / tvSeries / short / ... analytics rarely
    mix types), and decade gives time-series pruning (trend-over-time
    queries only need to scan the decades they ask about).
  - `episodes` is partitioned by decade (derived from the parent series'
    start_year) since almost every episode-level query is a time-series
    question ("how did this show's ratings trend over the years").

Run locally:
    spark-submit scripts/etl_job.py \
        --raw-dir data/raw --lake-dir data/lake

Run against the dockerized cluster:
    docker compose exec spark-master \
        spark-submit --master spark://spark-master:7077 \
        /opt/scripts/etl_job.py --raw-dir /opt/data/raw --lake-dir /opt/data/lake
"""

import argparse

from pyspark.sql import SparkSession, functions as F, types as T


def parse_args():
    parser = argparse.ArgumentParser(description="IMDb Lake ETL job")
    parser.add_argument("--raw-dir", default="data/raw", help="Directory with raw IMDb TSV(.gz) files")
    parser.add_argument("--lake-dir", default="data/lake", help="Output directory for partitioned Parquet")
    return parser.parse_args()


def build_spark(app_name="imdb-lake-etl"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def read_tsv(spark, path, schema):
    return (
        spark.read.option("sep", "\t")
        .option("header", True)
        .option("nullValue", "\\N")
        .schema(schema)
        .csv(path)
    )


TITLE_BASICS_SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType()),
        T.StructField("titleType", T.StringType()),
        T.StructField("primaryTitle", T.StringType()),
        T.StructField("originalTitle", T.StringType()),
        T.StructField("isAdult", T.StringType()),
        T.StructField("startYear", T.StringType()),
        T.StructField("endYear", T.StringType()),
        T.StructField("runtimeMinutes", T.StringType()),
        T.StructField("genres", T.StringType()),
    ]
)

TITLE_RATINGS_SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType()),
        T.StructField("averageRating", T.StringType()),
        T.StructField("numVotes", T.StringType()),
    ]
)

TITLE_EPISODE_SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType()),
        T.StructField("parentTconst", T.StringType()),
        T.StructField("seasonNumber", T.StringType()),
        T.StructField("episodeNumber", T.StringType()),
    ]
)


def clean_basics(basics_df):
    return (
        basics_df.dropDuplicates(["tconst"])
        .filter(F.col("tconst").isNotNull() & F.col("primaryTitle").isNotNull())
        .withColumn("is_adult", (F.col("isAdult") == "1"))
        .withColumn("start_year", F.col("startYear").cast(T.IntegerType()))
        .withColumn("end_year", F.col("endYear").cast(T.IntegerType()))
        .withColumn("runtime_minutes", F.col("runtimeMinutes").cast(T.IntegerType()))
        .withColumn(
            "genres",
            F.when(F.col("genres").isNotNull(), F.split(F.col("genres"), ","))
            .otherwise(F.array()),
        )
        .withColumn(
            "decade",
            F.when(F.col("start_year").isNotNull(), (F.floor(F.col("start_year") / 10) * 10).cast(T.IntegerType())),
        )
        .select(
            "tconst",
            F.col("titleType").alias("title_type"),
            F.col("primaryTitle").alias("primary_title"),
            F.col("originalTitle").alias("original_title"),
            "is_adult",
            "start_year",
            "end_year",
            "runtime_minutes",
            "genres",
            "decade",
        )
    )


def clean_ratings(ratings_df):
    return (
        ratings_df.dropDuplicates(["tconst"])
        .withColumn("average_rating", F.col("averageRating").cast(T.FloatType()))
        .withColumn("num_votes", F.col("numVotes").cast(T.IntegerType()))
        .select("tconst", "average_rating", "num_votes")
    )


def clean_episodes(episodes_df):
    return (
        episodes_df.dropDuplicates(["tconst"])
        .filter(F.col("parentTconst").isNotNull())
        .withColumn("season_number", F.col("seasonNumber").cast(T.IntegerType()))
        .withColumn("episode_number", F.col("episodeNumber").cast(T.IntegerType()))
        .select(
            F.col("tconst").alias("episode_tconst"),
            F.col("parentTconst").alias("parent_tconst"),
            "season_number",
            "episode_number",
        )
    )


def build_titles_table(basics, ratings):
    return (
        basics.join(ratings, on="tconst", how="left")
        .withColumn("has_rating", F.col("average_rating").isNotNull())
        .repartition("title_type", "decade")
    )


def build_episodes_table(episodes, basics, ratings):
    episode_titles = basics.select(
        F.col("tconst").alias("episode_tconst"),
        F.col("primary_title").alias("episode_title"),
    )
    episode_ratings = ratings.select(
        F.col("tconst").alias("episode_tconst"),
        F.col("average_rating"),
        F.col("num_votes"),
    )
    parent_years = basics.select(
        F.col("tconst").alias("parent_tconst"),
        F.col("primary_title").alias("series_title"),
        F.col("start_year").alias("series_start_year"),
        F.col("decade"),
    )

    return (
        episodes.join(episode_titles, on="episode_tconst", how="left")
        .join(episode_ratings, on="episode_tconst", how="left")
        .join(parent_years, on="parent_tconst", how="left")
        .repartition("decade")
    )


def main():
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    basics_raw = read_tsv(spark, f"{args.raw_dir}/title.basics.tsv.gz", TITLE_BASICS_SCHEMA)
    ratings_raw = read_tsv(spark, f"{args.raw_dir}/title.ratings.tsv.gz", TITLE_RATINGS_SCHEMA)
    episodes_raw = read_tsv(spark, f"{args.raw_dir}/title.episode.tsv.gz", TITLE_EPISODE_SCHEMA)

    basics = clean_basics(basics_raw)
    ratings = clean_ratings(ratings_raw)
    episodes = clean_episodes(episodes_raw)

    titles_table = build_titles_table(basics, ratings)
    episodes_table = build_episodes_table(episodes, basics, ratings)

    titles_path = f"{args.lake_dir}/titles"
    episodes_path = f"{args.lake_dir}/episodes"

    (
        titles_table.write.mode("overwrite")
        .partitionBy("title_type", "decade")
        .option("compression", "snappy")
        .parquet(titles_path)
    )

    (
        episodes_table.write.mode("overwrite")
        .partitionBy("decade")
        .option("compression", "snappy")
        .parquet(episodes_path)
    )

    print(f"titles written to {titles_path}: {titles_table.count()} rows")
    print(f"episodes written to {episodes_path}: {episodes_table.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
