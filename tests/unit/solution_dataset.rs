use super::*;
use std::io::{Read as _, Write as _};

#[test]
fn table_units_use_the_last_index_and_validate_earlier_units() -> Result<()> {
    let mut dataset = SolutionDataset::default()
        .with_zip_file("tests/data/plexos-solution-files/Model_Base_LT_Solution.zip")?;
    let (table_name, indexes) = dataset
        .table_key_index_mapping
        .iter()
        .filter(|(_, indexes)| indexes.len() > 1)
        .min_by(|left, right| left.0.cmp(right.0))
        .map(|(name, indexes)| (name.clone(), indexes.clone()))
        .ok_or_else(|| eyre!("fixture needs a table with multiple keys"))?;
    dataset.key_index = indexes
        .iter()
        .map(|index| dataset.key_index[*index].clone())
        .collect();
    let first_key = dataset.key_index.first().expect("multiple indexes").key_id;
    let last_key = dataset.key_index.last().expect("multiple indexes").key_id;
    assert_ne!(first_key, last_key);
    let unit_id = i64::MAX;
    assert!(
        dataset
            .unit
            .insert(
                unit_id,
                Unit {
                    id: unit_id,
                    value: "last-unit".to_owned(),
                    ..Default::default()
                }
            )
            .is_none()
    );
    let mut property = dataset
        .property(dataset.key(last_key)?.property_id)?
        .clone();
    property.property_id = i64::MAX;
    property.unit_id = unit_id;
    property.summary_unit_id = unit_id;
    assert!(
        dataset
            .property
            .insert(property.property_id, property)
            .is_none()
    );
    dataset
        .key
        .get_mut(&last_key)
        .expect("source key exists")
        .property_id = i64::MAX;
    let final_offset = 17;
    dataset
        .key_index
        .last_mut()
        .expect("multiple indexes")
        .period_offset = final_offset;
    dataset.update_table_key_indexes_mapping()?;
    assert_eq!(
        dataset.table_key_index_mapping[&table_name],
        (0..dataset.key_index.len()).collect::<Vec<_>>()
    );
    assert_eq!(
        dataset.table_units_mapping.get(&table_name),
        Some(&("last-unit".to_owned(), final_offset))
    );

    let missing_unit = i64::MAX - 1;
    let expected_error = dataset
        .unit(missing_unit)
        .expect_err("unit must be absent")
        .to_string();
    let mut property = dataset
        .property(dataset.key(first_key)?.property_id)?
        .clone();
    property.property_id = i64::MAX - 1;
    property.unit_id = missing_unit;
    property.summary_unit_id = missing_unit;
    assert!(
        dataset
            .property
            .insert(property.property_id, property)
            .is_none()
    );
    dataset
        .key
        .get_mut(&first_key)
        .expect("source key exists")
        .property_id = i64::MAX - 1;
    let error = dataset
        .update_table_key_indexes_mapping()
        .expect_err("an earlier missing unit must fail");
    assert_eq!(error.to_string(), expected_error);
    Ok(())
}

#[test]
fn shuffled_key_records_produce_the_same_mapping_and_missing_keys_fail() -> Result<()> {
    let path = std::path::Path::new("tests/data/plexos-solution-files/Model_Base_LT_Solution.zip");
    let (mut source, _) = SolutionDataset::read_zip_archive(path, "", &mut None)?;
    let mut text = String::new();
    source
        .archive
        .by_index(source.index)?
        .read_to_string(&mut text)?;
    let ordered = SolutionDataset::default().with_xml_string(&text)?;
    let doc = roxmltree::Document::parse(&text)?;
    let nodes = doc
        .root_element()
        .children()
        .filter(roxmltree::Node::is_element)
        .collect::<Vec<_>>();
    let mut key_nodes = nodes
        .iter()
        .copied()
        .filter(|node| node.tag_name().name() == "t_key")
        .collect::<Vec<_>>();
    let mut index_nodes = nodes
        .iter()
        .copied()
        .filter(|node| node.tag_name().name() == "t_key_index")
        .collect::<Vec<_>>();
    assert!(key_nodes.len() > 1 && index_nodes.len() > 1);
    let mut shuffled_text = String::with_capacity(text.len());
    let mut previous_end = 0;
    for node in nodes {
        shuffled_text.push_str(&text[previous_end..node.range().start]);
        let replacement = match node.tag_name().name() {
            "t_key" => key_nodes.pop().expect("one replacement per key"),
            "t_key_index" => index_nodes.pop().expect("one replacement per index"),
            _ => node,
        };
        shuffled_text.push_str(&text[replacement.range()]);
        previous_end = node.range().end;
    }
    shuffled_text.push_str(&text[previous_end..]);
    let mut shuffled = SolutionDataset::default().with_xml_string(&shuffled_text)?;
    assert_eq!(
        ordered.table_key_index_mapping,
        shuffled.table_key_index_mapping
    );
    assert_eq!(ordered.table_units_mapping, shuffled.table_units_mapping);
    let missing = shuffled.key_index[0].key_id;
    assert!(shuffled.key.shift_remove(&missing).is_some());
    let error = shuffled
        .update_table_key_indexes_mapping()
        .expect_err("missing source key must fail");
    assert_eq!(error.to_string(), format!("Key with {missing} not found"));
    Ok(())
}

#[test]
fn repeated_ids_preserve_periods_objects_and_phases() -> Result<()> {
    let text = r#"<SolutionDataset>
        <t_key_index><key_id>21</key_id><period_type_id>1</period_type_id><position>0</position><length>2</length><period_offset>3</period_offset></t_key_index>
        <t_key_index><key_id>21</key_id><period_type_id>4</period_type_id><position>0</position><length>1</length><period_offset>0</period_offset></t_key_index>
        <t_attribute_data><object_id>1</object_id><attribute_id>5</attribute_id><value>10</value></t_attribute_data>
        <t_attribute_data><object_id>2</object_id><attribute_id>5</attribute_id><value>20</value></t_attribute_data>
        <t_attribute_data><attribute_id>5</attribute_id><value>30</value></t_attribute_data>
        <t_attribute_data><object_id/><attribute_id>6</attribute_id><value>40</value></t_attribute_data>
        <t_sample><sample_id>1</sample_id><sample_name>scenario</sample_name></t_sample>
        <t_sample_weight><sample_id>1</sample_id><phase_id>3</phase_id><value>0.25</value></t_sample_weight>
        <t_sample_weight><sample_id>1</sample_id><phase_id>4</phase_id><value>0.75</value></t_sample_weight>
    </SolutionDataset>"#;
    let mut dataset = SolutionDataset::default();
    xml::read_records(text.as_bytes(), |node| {
        dataset.parse_xml_record(node.tag_name().name(), &node)
    })?;
    dataset.sort_xml_records();
    assert_eq!(dataset.key_index.len(), 2);
    assert_eq!(dataset.attribute_data.len(), 4);
    assert_eq!(dataset.sample_weight.len(), 2);

    let mut con = duckdb::Connection::open_in_memory()?;
    con.execute_batch("CREATE SCHEMA raw;")?;
    dataset.populate_table_key_indexes(&mut con, &mut None)?;
    dataset.populate_table_attribute_data(&mut con, &mut None)?;
    dataset.populate_table_samples(&mut con, &mut None)?;
    let indexes = con.prepare("SELECT key_id, period_type_id, position, length, period_offset FROM raw.key_indexes ORDER BY period_type_id")?
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, u64>(2)?, row.get::<_, u64>(3)?, row.get::<_, i64>(4)?)))?
        .collect::<duckdb::Result<Vec<_>>>()?;
    assert_eq!(indexes, vec![(21, 1, 0, 2, 3), (21, 4, 0, 1, 0)]);
    let attributes = con.prepare("SELECT object_id, attribute_id, value::DOUBLE FROM raw.attribute_data ORDER BY value::DOUBLE")?
        .query_map([], |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, i64>(1)?, row.get::<_, f64>(2)?)))?
        .collect::<duckdb::Result<Vec<_>>>()?;
    assert_eq!(
        attributes,
        vec![
            (Some(1), 5, 10.0),
            (Some(2), 5, 20.0),
            (None, 5, 30.0),
            (None, 6, 40.0)
        ]
    );
    let weights = con.prepare("SELECT sample_id, sample_phase_id, sample_weight FROM raw.samples ORDER BY sample_phase_id")?
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, f64>(2)?)))?
        .collect::<duckdb::Result<Vec<_>>>()?;
    assert_eq!(weights, vec![(1, 3, 0.25), (1, 4, 0.75)]);
    Ok(())
}

#[test]
fn repeated_key_reads_each_period_and_report_rows_are_not_duplicated() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let mut period_data = indexmap::IndexMap::new();
    for (period, values) in [(1, vec![10.0_f64, 20.0]), (4, vec![30.0])] {
        let path = temp_dir.path().join(format!("t_data_{period}.BIN"));
        let mut file = std::fs::File::create(&path)?;
        for value in values {
            file.write_all(&value.to_le_bytes())?;
        }
        drop(file);
        period_data.insert(period, std::fs::File::open(path)?);
    }
    let mut key = indexmap::IndexMap::with_hasher(ahash::RandomState::new());
    key.insert(
        21,
        Key {
            key_id: 21,
            phase_id: 4,
            is_summary: true,
            sample_id: 1,
            band_id: 2,
            membership_id: 3,
            property_id: 4,
            model_id: 1,
            timeslice_id: 0,
        },
    );
    let dataset = SolutionDataset {
        key,
        key_index: vec![
            KeyIndex {
                key_id: 21,
                period_type_id: 1,
                length: 2,
                position: 0,
                period_offset: 3,
            },
            KeyIndex {
                key_id: 21,
                period_type_id: 4,
                length: 1,
                position: 0,
                period_offset: 0,
            },
        ],
        sample: [(
            1,
            Sample {
                sample_id: 1,
                name: Some("scenario".to_owned()),
            },
        )]
        .into_iter()
        .collect(),
        sample_weight: vec![
            SampleWeight {
                sample_id: 1,
                phase_id: 3,
                weight: 0.25,
            },
            SampleWeight {
                sample_id: 1,
                phase_id: 4,
                weight: 0.75,
            },
        ],
        table_key_index_mapping: std::collections::HashMap::from([
            ("ST__Day__Fixture__Metric".to_owned(), vec![0]),
            ("ST__Year__Fixture__Metric".to_owned(), vec![1]),
        ]),
        ..Default::default()
    }
    .with_period_data(period_data);
    let path = temp_dir.path().join("periods.duckdb");
    dataset.to_duckdb(&path).with_data_write_threads(2).run()?;
    let con = duckdb::Connection::open(path)?;
    for (table, expected) in [
        ("ST__Day__Fixture__Metric", vec![(4_i64, 10.0), (5, 20.0)]),
        ("ST__Year__Fixture__Metric", vec![(1, 30.0)]),
    ] {
        let actual = con
            .prepare(&format!(
                "SELECT block_id, value FROM data.\"{table}\" ORDER BY block_id"
            ))?
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?)))?
            .collect::<duckdb::Result<Vec<_>>>()?;
        assert_eq!(actual, expected);
        let report = con
            .prepare(&format!(
                "SELECT sample_name, value FROM report.\"{table}\" ORDER BY value"
            ))?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
            })?
            .collect::<duckdb::Result<Vec<_>>>()?;
        assert_eq!(
            report,
            expected
                .into_iter()
                .map(|(_, value)| ("scenario".to_owned(), value))
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[test]
fn renamed_zip_selects_solution_xml_instead_of_parameters() -> Result<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let zip_path = temp_dir.path().join("renamed.zip");
    let mut writer = zip::ZipWriter::new(std::fs::File::create(&zip_path)?);
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    writer.start_file("PLEXOS_Param.xml", options)?;
    writer.write_all(b"<UndocumentedParam><Attributes/></UndocumentedParam>")?;
    writer.start_file("Model actual Solution.xml", options)?;
    writer.write_all(
        b"<SolutionDataset xmlns=\"urn:solution\"><t_model><model_id>1</model_id><name>actual</name></t_model></SolutionDataset>",
    )?;
    writer.finish()?;

    let dataset = SolutionDataset::default().with_zip_file(&zip_path)?;
    assert_eq!(dataset.model.len(), 1);
    assert_eq!(dataset.model.get(&1).expect("model exists").name, "actual");
    Ok(())
}
