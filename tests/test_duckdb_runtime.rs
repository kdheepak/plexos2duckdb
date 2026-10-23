use std::{fs::File, sync::Arc};

use duckdb::{
    Connection,
    arrow::{
        array::{ArrayRef, Float64Array, Int64Array},
        record_batch::RecordBatch,
    },
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

#[test]
fn bundled_engine_uses_duckdb_v2() -> Result<(), Box<dyn std::error::Error>> {
    let con = Connection::open_in_memory()?;
    let version: String = con.query_row("SELECT version()", [], |row| row.get(0))?;
    assert!(
        version.starts_with("v2."),
        "unexpected DuckDB version: {version}"
    );
    Ok(())
}

#[test]
fn arrow_appender_preserves_large_batches() -> Result<(), Box<dyn std::error::Error>> {
    // Cross DuckDB's vector boundaries and the converter's 262,144-value batch size.
    let count = 262_145_i64;
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..count)) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Float64Array::from_iter_values(
                (0..count).map(|id| id as f64 / 4.0),
            )) as ArrayRef,
        ),
    ])?;
    let con = Connection::open_in_memory()?;
    con.execute_batch("CREATE TABLE readings (id BIGINT, value DOUBLE)")?;
    {
        let mut appender = con.appender("readings")?;
        appender.append_record_batch(batch)?;
        appender.flush()?;
    }

    let actual: (i64, i64, i64, i64, i64) = con.query_row(
        "SELECT count(*), count(DISTINCT id), min(id), max(id),
                count(*) FILTER (WHERE value IS DISTINCT FROM id / 4.0)
         FROM readings",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    assert_eq!(actual, (count, count, 0, count - 1, 0));
    Ok(())
}

#[test]
fn arrow_parquet_round_trip_needs_no_extension_downloads() -> Result<(), Box<dyn std::error::Error>>
{
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("data.parquet");
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        (
            "value",
            Arc::new(Float64Array::from(vec![Some(1.25), None, Some(-2.5)])) as ArrayRef,
        ),
    ])?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(File::create(&path)?, batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;

    let con = Connection::open_in_memory()?;
    con.execute_batch(
        "SET autoinstall_known_extensions = false;
         SET autoload_known_extensions = false;",
    )?;
    let rows = con
        .prepare("SELECT id, value FROM read_parquet(?) ORDER BY id")?
        .query_map([path.to_string_lossy().as_ref()], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Option<f64>>(1)?))
        })?
        .collect::<duckdb::Result<Vec<_>>>()?;
    assert_eq!(rows, vec![(1, Some(1.25)), (2, None), (3, Some(-2.5))]);
    Ok(())
}
