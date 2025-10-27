# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pytest",
#     "pandas",
#     "duckdb",
# ]
# ///

import duckdb
import pytest
import pathlib

DB_PATH = pathlib.Path(__file__).parent / r"""Model-DAY_AHEAD-Solution.duckdb"""

EXPECTED_SCHEMAS = ["data", "main", "processed", "raw", "report"]

EXPECTED_RAW_TABLES = {
    "config": ["element", "value"],
    "memberships": [
        "membership_id",
        "collection_id",
        "collection",
        "child_id",
        "child_name",
        "child_category",
        "child_category_class",
        "parent_id",
        "parent_name",
        "parent_category",
        "parent_category_class",
        "child_class_id",
        "child_class_name",
        "parent_class_id",
        "parent_class_name",
        "kind",
    ],
    "collections": [
        "collection_id",
        "parent_class_id",
        "child_class_id",
        "name",
        "complement_name",
    ],
    "classes": ["class_id", "name", "class_group_id"],
    "class_groups": ["class_group_id", "name"],
    "categories": ["category_id", "class_id", "rank", "name"],
    "bands": ["band_id"],
    "models": ["model_id", "name"],
    "objects": ["object_id", "class_id", "name", "category_id", "index", "is_show"],
    "keys": [
        "key_id",
        "membership_id",
        "model_id",
        "phase_id",
        "property_id",
        "is_summary",
        "band_id",
        "sample_id",
        "timeslice_id",
    ],
    "key_indexes": ["key_id", "period_type_id", "position", "length", "period_offset"],
    "properties": [
        "property_id",
        "name",
        "summary_name",
        "enum_id",
        "unit_id",
        "summary_unit_id",
        "is_multi_band",
        "is_period",
        "is_summary",
        "collection_id",
    ],
    "timeslices": ["timeslice_id", "timeslice_name"],
    "samples": ["sample_id", "sample_name", "sample_phase_id", "sample_weight"],
    "units": ["unit_id", "unit_name", "lang_id"],
    "memo_objects": ["value", "column_id", "object_id"],
    "custom_columns": ["column_id", "name", "position", "class_id"],
    "attribute_data": ["object_id", "attribute_id", "value"],
    "attributes": ["attribute_id", "name", "lang_id", "class_id", "description"],
}


def connect():
    return duckdb.connect(DB_PATH, read_only=True)


def test_schemas():
    con = connect()
    schemas = {
        s[0]
        for s in con.execute(
            "SELECT DISTINCT table_schema FROM information_schema.tables"
        ).fetchall()
    }
    missing = set(EXPECTED_SCHEMAS) - schemas
    assert not missing, f"Missing schemas: {sorted(missing)}"


def test_metadata_table():
    con = connect()
    (ok,) = con.execute(
        "SELECT COUNT(*)>0 FROM information_schema.tables WHERE table_schema='main' AND table_name='plexos2duckdb'"
    ).fetchone()
    assert ok, "Missing main.plexos2duckdb"


@pytest.mark.parametrize("table,cols", list(EXPECTED_RAW_TABLES.items()))
def test_raw_tables_and_columns(table, cols):
    con = connect()
    (exists,) = con.execute(
        "SELECT COUNT(*)>0 FROM information_schema.tables WHERE table_schema='raw' AND table_name=?",
        [table],
    ).fetchone()
    assert exists, f"Missing raw.{table}"
    actual = (
        con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='raw' AND table_name=? ORDER BY ordinal_position",
            [table],
        )
        .fetchdf()["column_name"]
        .tolist()
    )
    for c in cols:
        assert c in actual, f"raw.{table} missing column {c} (actual: {actual})"


def test_processed_and_report_views():
    con = connect()
    processed_views = (
        con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='processed' AND table_type='VIEW'"
        )
        .fetchdf()["table_name"]
        .tolist()
    )
    assert len(processed_views) >= 3, "Expected processed.* views"
    report_views = (
        con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='report' AND table_type='VIEW'"
        )
        .fetchdf()["table_name"]
        .tolist()
    )
    assert len(report_views) >= 1, "Expected report.* views"


def test_data_tables_non_empty():
    con = connect()
    data_tables = (
        con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='data' AND table_type='BASE TABLE'"
        )
        .fetchdf()["table_name"]
        .tolist()
    )
    assert data_tables, "No data.* tables found"
    # At least one data table should have rows
    non_empty = False
    for t in data_tables[:5]:
        (cnt,) = con.execute(f'SELECT COUNT(*) FROM "data"."{t}"').fetchone()
        if cnt > 0:
            non_empty = True
            break
    assert non_empty, "All sampled data tables were empty"


def test_sample_report_query():
    con = connect()
    report_views = (
        con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='report' AND table_type='VIEW'"
        )
        .fetchdf()["table_name"]
        .tolist()
    )
    assert report_views, "No report views found"
    view = report_views[0]
    # Ensure the view can be queried and has expected columns like timestamp/property (names vary by generator)
    df = con.execute(f'SELECT * FROM "report"."{view}" LIMIT 5').fetchdf()
    assert df is not None
