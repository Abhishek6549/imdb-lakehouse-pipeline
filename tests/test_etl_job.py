from etl_job import (
    TITLE_BASICS_SCHEMA,
    TITLE_EPISODE_SCHEMA,
    TITLE_RATINGS_SCHEMA,
    build_episodes_table,
    build_titles_table,
    clean_basics,
    clean_episodes,
    clean_ratings,
    try_cast_int,
)


def basics_df(spark, rows):
    return spark.createDataFrame(rows, schema=TITLE_BASICS_SCHEMA)


def ratings_df(spark, rows):
    return spark.createDataFrame(rows, schema=TITLE_RATINGS_SCHEMA)


def episodes_df(spark, rows):
    return spark.createDataFrame(rows, schema=TITLE_EPISODE_SCHEMA)


def test_try_cast_int_returns_null_for_malformed_value(spark):
    df = spark.createDataFrame([("Documentary",)], "runtimeMinutes string")
    result = df.select(try_cast_int("runtimeMinutes").alias("v")).collect()
    assert result[0]["v"] is None


def test_try_cast_int_casts_valid_value(spark):
    df = spark.createDataFrame([("90",)], "runtimeMinutes string")
    result = df.select(try_cast_int("runtimeMinutes").alias("v")).collect()
    assert result[0]["v"] == 90


def test_clean_basics_drops_rows_with_null_primary_title(spark):
    rows = [
        ("tt1", "movie", "A Real Title", "A Real Title", "0", "1994", None, "90", "Drama"),
        ("tt2", "movie", None, None, "0", "1995", None, "100", "Drama"),
    ]
    result = clean_basics(basics_df(spark, rows)).collect()
    assert [r["tconst"] for r in result] == ["tt1"]


def test_clean_basics_dedupes_on_tconst(spark):
    rows = [
        ("tt1", "movie", "Title", "Title", "0", "1994", None, "90", "Drama"),
        ("tt1", "movie", "Title", "Title", "0", "1994", None, "90", "Drama"),
    ]
    result = clean_basics(basics_df(spark, rows)).collect()
    assert len(result) == 1


def test_clean_basics_derives_decade_from_start_year(spark):
    rows = [("tt1", "movie", "Title", "Title", "0", "1994", None, "90", "Drama")]
    result = clean_basics(basics_df(spark, rows)).collect()
    assert result[0]["decade"] == 1990


def test_clean_basics_decade_is_null_when_start_year_missing(spark):
    rows = [("tt1", "movie", "Title", "Title", "0", None, None, "90", "Drama")]
    result = clean_basics(basics_df(spark, rows)).collect()
    assert result[0]["decade"] is None


def test_clean_basics_splits_genres_into_array(spark):
    rows = [("tt1", "short", "Title", "Title", "0", "1894", None, "1", "Documentary,Short")]
    result = clean_basics(basics_df(spark, rows)).collect()
    assert result[0]["genres"] == ["Documentary", "Short"]


def test_clean_basics_null_genres_become_empty_array(spark):
    rows = [("tt1", "short", "Title", "Title", "0", "1894", None, "1", None)]
    result = clean_basics(basics_df(spark, rows)).collect()
    assert result[0]["genres"] == []


def test_clean_basics_survives_malformed_runtime(spark):
    # regression test: raw IMDb rows occasionally have a shifted/malformed
    # column (e.g. a genre string where runtimeMinutes should be); this
    # must not raise, and should resolve to a null runtime instead.
    rows = [("tt1", "movie", "Title", "Title", "0", "1994", None, "Documentary", "Drama")]
    result = clean_basics(basics_df(spark, rows)).collect()
    assert result[0]["runtime_minutes"] is None


def test_clean_ratings_casts_numeric_columns(spark):
    rows = [("tt1", "8.5", "1000")]
    result = clean_ratings(ratings_df(spark, rows)).collect()
    assert result[0]["average_rating"] == 8.5
    assert result[0]["num_votes"] == 1000


def test_clean_episodes_filters_null_parent(spark):
    rows = [
        ("tt1", "tt0", "1", "1"),
        ("tt2", None, "1", "2"),
    ]
    result = clean_episodes(episodes_df(spark, rows)).collect()
    assert [r["episode_tconst"] for r in result] == ["tt1"]


def test_build_titles_table_flags_titles_with_ratings(spark):
    basics = clean_basics(
        basics_df(
            spark,
            [
                ("tt1", "movie", "Rated", "Rated", "0", "1994", None, "90", "Drama"),
                ("tt2", "movie", "Unrated", "Unrated", "0", "1994", None, "90", "Drama"),
            ],
        )
    )
    ratings = clean_ratings(ratings_df(spark, [("tt1", "8.0", "500")]))
    result = {r["tconst"]: r["has_rating"] for r in build_titles_table(basics, ratings).collect()}
    assert result == {"tt1": True, "tt2": False}


def test_build_episodes_table_attaches_series_and_episode_titles(spark):
    basics = clean_basics(
        basics_df(
            spark,
            [
                ("tt0", "tvSeries", "The Series", "The Series", "0", "2008", "2013", None, "Drama"),
                ("tt1", "tvEpisode", "Pilot", "Pilot", "0", "2008", None, "60", "Drama"),
            ],
        )
    )
    ratings = clean_ratings(ratings_df(spark, [("tt1", "9.0", "1000")]))
    episodes = clean_episodes(episodes_df(spark, [("tt1", "tt0", "1", "1")]))

    result = build_episodes_table(episodes, basics, ratings).collect()
    assert len(result) == 1
    row = result[0]
    assert row["series_title"] == "The Series"
    assert row["episode_title"] == "Pilot"
    assert row["average_rating"] == 9.0
    assert row["decade"] == 2000
