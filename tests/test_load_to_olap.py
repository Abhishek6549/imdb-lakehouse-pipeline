import pyarrow as pa

from load_to_olap import cast_bool_columns, fill_sentinel_columns, split_sql_statements


def test_split_sql_statements_ignores_full_line_comments():
    ddl = "-- a header comment\nCREATE DATABASE IF NOT EXISTS imdb;"
    assert split_sql_statements(ddl) == ["CREATE DATABASE IF NOT EXISTS imdb"]


def test_split_sql_statements_handles_semicolon_inside_inline_comment():
    # regression test: a trailing comment like "-- 0 = unknown; some note"
    # must not be mistaken for a second statement boundary.
    ddl = "CREATE TABLE t (\n    a UInt16 -- 0 = unknown; see docs\n);"
    statements = split_sql_statements(ddl)
    assert len(statements) == 1
    assert "CREATE TABLE t" in statements[0]


def test_split_sql_statements_splits_multiple_statements():
    ddl = "CREATE DATABASE a; CREATE DATABASE b;"
    assert split_sql_statements(ddl) == ["CREATE DATABASE a", "CREATE DATABASE b"]


def test_cast_bool_columns_converts_to_uint8():
    table = pa.table({"is_adult": pa.array([True, False], type=pa.bool_())})
    result = cast_bool_columns(table)
    assert result.column("is_adult").type == pa.uint8()
    assert result.column("is_adult").to_pylist() == [1, 0]


def test_cast_bool_columns_ignores_missing_columns():
    table = pa.table({"tconst": pa.array(["tt1"])})
    result = cast_bool_columns(table)
    assert result.column_names == ["tconst"]


def test_fill_sentinel_columns_replaces_null_decade_with_zero():
    table = pa.table({"decade": pa.array([1990, None, 2020], type=pa.int32())})
    result = fill_sentinel_columns(table)
    assert result.column("decade").to_pylist() == [1990, 0, 2020]
    assert result.column("decade").type == pa.uint16()


def test_fill_sentinel_columns_handles_season_and_episode_number():
    table = pa.table(
        {
            "season_number": pa.array([1, None], type=pa.int32()),
            "episode_number": pa.array([None, 5], type=pa.int32()),
        }
    )
    result = fill_sentinel_columns(table)
    assert result.column("season_number").to_pylist() == [1, 0]
    assert result.column("episode_number").to_pylist() == [0, 5]
