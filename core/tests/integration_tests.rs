use std::cmp::min;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReader;
use arrow_schema::Schema;
use bytes::BytesMut;
use console::Style;
use pgpq::ArrowToPostgresBinaryEncoder;
use postgres::{Client, NoTls};
use postgresql_embedded::blocking::PostgreSQL;
use postgresql_embedded::Settings;
use similar::{ChangeTag, TextDiff};

fn read_batches(file: PathBuf) -> (Vec<RecordBatch>, Schema) {
    let file = File::open(file).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();
    let schema = (*reader.schema()).clone();
    let batches = reader.map(|v| v.unwrap()).collect();
    (batches, schema)
}

fn run_test_case(case: &str) {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/testdata/{case}.arrow"));
    let (batches, schema) = read_batches(path);
    let mut encoder = ArrowToPostgresBinaryEncoder::try_new(&schema).unwrap();
    let mut buf = BytesMut::new();
    encoder.write_header(&mut buf);
    for batch in batches {
        encoder.write_batch(&batch, &mut buf).unwrap();
    }
    encoder.write_footer(&mut buf).unwrap();

    let snap_file =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/snapshots/{case}.bin"));
    if !snap_file.exists() {
        fs::write(snap_file.clone(), &buf[..]).unwrap();
        panic!("wrote new snap at {snap_file:?}")
    } else {
        let existing = fs::read(snap_file).unwrap();
        let n_chars = min(buf.len(), 50);
        assert_eq!(
            existing,
            &buf[..],
            "values did not match. First {n_chars} bytes shown",
        )
    }
}

// These tests are generated in generate_test_data.py

#[test]
fn test_bool() {
    run_test_case("bool")
}

#[test]
fn test_uint8() {
    run_test_case("uint8")
}

#[test]
fn test_uint16() {
    run_test_case("uint16")
}

#[test]
fn test_uint32() {
    run_test_case("uint32")
}

#[test]
fn test_int8() {
    run_test_case("int8")
}

#[test]
fn test_int16() {
    run_test_case("int16")
}

#[test]
fn test_int32() {
    run_test_case("int32")
}

#[test]
fn test_int64() {
    run_test_case("int64")
}

#[test]
fn test_float32() {
    run_test_case("float32")
}

#[test]
fn test_float64() {
    run_test_case("float64")
}

#[test]
fn test_decimal32() {
    run_test_case("decimal32")
}

#[test]
fn test_decimal64() {
    run_test_case("decimal64")
}

#[test]
fn test_decimal128() {
    run_test_case("decimal128")
}

#[test]
fn test_timestamp_us_notz() {
    run_test_case("timestamp_us_notz")
}

#[test]
fn test_timestamp_ms_notz() {
    run_test_case("timestamp_ms_notz")
}

#[test]
fn test_timestamp_s_notz() {
    run_test_case("timestamp_s_notz")
}

#[test]
fn test_timestamp_us_tz() {
    run_test_case("timestamp_us_tz")
}

#[test]
fn test_timestamp_ms_tz() {
    run_test_case("timestamp_ms_tz")
}

#[test]
fn test_timestamp_s_tz() {
    run_test_case("timestamp_s_tz")
}

#[test]
fn test_time_s() {
    run_test_case("time_s")
}

#[test]
fn test_time_ms() {
    run_test_case("time_ms")
}

#[test]
fn test_time_us() {
    run_test_case("time_us")
}

#[test]
fn test_date32() {
    run_test_case("date32")
}

#[test]
fn test_duration_us() {
    run_test_case("duration_us")
}

#[test]
fn test_duration_ms() {
    run_test_case("duration_ms")
}

#[test]
fn test_duration_s() {
    run_test_case("duration_s")
}

#[test]
fn test_binary() {
    run_test_case("binary")
}

#[test]
fn test_large_binary() {
    run_test_case("large_binary")
}

#[test]
fn test_string() {
    run_test_case("string")
}

#[test]
fn test_large_string() {
    run_test_case("large_string")
}

#[test]
fn test_string_view() {
    run_test_case("string_view")
}

#[test]
fn test_bool_nullable() {
    run_test_case("bool_nullable")
}

#[test]
fn test_uint8_nullable() {
    run_test_case("uint8_nullable")
}

#[test]
fn test_uint16_nullable() {
    run_test_case("uint16_nullable")
}

#[test]
fn test_uint32_nullable() {
    run_test_case("uint32_nullable")
}

#[test]
fn test_int8_nullable() {
    run_test_case("int8_nullable")
}

#[test]
fn test_int16_nullable() {
    run_test_case("int16_nullable")
}

#[test]
fn test_int32_nullable() {
    run_test_case("int32_nullable")
}

#[test]
fn test_int64_nullable() {
    run_test_case("int64_nullable")
}

#[test]
fn test_float32_nullable() {
    run_test_case("float32_nullable")
}

#[test]
fn test_float64_nullable() {
    run_test_case("float64_nullable")
}

#[test]
fn test_decimal32_nullable() {
    run_test_case("decimal32_nullable")
}

#[test]
fn test_decimal64_nullable() {
    run_test_case("decimal64_nullable")
}

#[test]
fn test_decimal128_nullable() {
    run_test_case("decimal128_nullable")
}

#[test]
fn test_timestamp_us_notz_nullable() {
    run_test_case("timestamp_us_notz_nullable")
}

#[test]
fn test_timestamp_ms_notz_nullable() {
    run_test_case("timestamp_ms_notz_nullable")
}

#[test]
fn test_timestamp_s_notz_nullable() {
    run_test_case("timestamp_s_notz_nullable")
}

#[test]
fn test_timestamp_us_tz_nullable() {
    run_test_case("timestamp_us_tz_nullable")
}

#[test]
fn test_timestamp_ms_tz_nullable() {
    run_test_case("timestamp_ms_tz_nullable")
}

#[test]
fn test_timestamp_s_tz_nullable() {
    run_test_case("timestamp_s_tz_nullable")
}

#[test]
fn test_time_s_nullable() {
    run_test_case("time_s_nullable")
}

#[test]
fn test_time_ms_nullable() {
    run_test_case("time_ms_nullable")
}

#[test]
fn test_time_us_nullable() {
    run_test_case("time_us_nullable")
}

#[test]
fn test_date32_nullable() {
    run_test_case("date32_nullable")
}

#[test]
fn test_duration_us_nullable() {
    run_test_case("duration_us_nullable")
}

#[test]
fn test_duration_ms_nullable() {
    run_test_case("duration_ms_nullable")
}

#[test]
fn test_duration_s_nullable() {
    run_test_case("duration_s_nullable")
}

#[test]
fn test_binary_nullable() {
    run_test_case("binary_nullable")
}

#[test]
fn test_large_binary_nullable() {
    run_test_case("large_binary_nullable")
}

#[test]
fn test_string_nullable() {
    run_test_case("string_nullable")
}

#[test]
fn test_large_string_nullable() {
    run_test_case("large_string_nullable")
}

#[test]
fn test_string_view_nullable() {
    run_test_case("string_view_nullable")
}

#[test]
fn test_list_bool() {
    run_test_case("list_bool")
}

#[test]
fn test_list_uint8() {
    run_test_case("list_uint8")
}

#[test]
fn test_list_uint16() {
    run_test_case("list_uint16")
}

#[test]
fn test_list_uint32() {
    run_test_case("list_uint32")
}

#[test]
fn test_list_int8() {
    run_test_case("list_int8")
}

#[test]
fn test_list_int16() {
    run_test_case("list_int16")
}

#[test]
fn test_list_int32() {
    run_test_case("list_int32")
}

#[test]
fn test_list_int64() {
    run_test_case("list_int64")
}

#[test]
fn test_list_float32() {
    run_test_case("list_float32")
}

#[test]
fn test_list_float64() {
    run_test_case("list_float64")
}

#[test]
fn test_list_decimal32() {
    run_test_case("list_decimal32")
}

#[test]
fn test_list_decimal64() {
    run_test_case("list_decimal64")
}

#[test]
fn test_list_decimal128() {
    run_test_case("list_decimal128")
}

#[test]
fn test_list_timestamp_us_notz() {
    run_test_case("list_timestamp_us_notz")
}

#[test]
fn test_list_timestamp_ms_notz() {
    run_test_case("list_timestamp_ms_notz")
}

#[test]
fn test_list_timestamp_s_notz() {
    run_test_case("list_timestamp_s_notz")
}

#[test]
fn test_list_timestamp_us_tz() {
    run_test_case("list_timestamp_us_tz")
}

#[test]
fn test_list_timestamp_ms_tz() {
    run_test_case("list_timestamp_ms_tz")
}

#[test]
fn test_list_timestamp_s_tz() {
    run_test_case("list_timestamp_s_tz")
}

#[test]
fn test_list_time_s() {
    run_test_case("list_time_s")
}

#[test]
fn test_list_time_ms() {
    run_test_case("list_time_ms")
}

#[test]
fn test_list_time_us() {
    run_test_case("list_time_us")
}

#[test]
fn test_list_date32() {
    run_test_case("list_date32")
}

#[test]
fn test_list_duration_us() {
    run_test_case("list_duration_us")
}

#[test]
fn test_list_duration_ms() {
    run_test_case("list_duration_ms")
}

#[test]
fn test_list_duration_s() {
    run_test_case("list_duration_s")
}

#[test]
fn test_list_binary() {
    run_test_case("list_binary")
}

#[test]
fn test_list_large_binary() {
    run_test_case("list_large_binary")
}

#[test]
fn test_list_string() {
    run_test_case("list_string")
}

#[test]
fn test_list_large_string() {
    run_test_case("list_large_string")
}

#[test]
fn test_list_string_view() {
    run_test_case("list_string_view")
}

#[test]
fn test_list_bool_nullable() {
    run_test_case("list_bool_nullable")
}

#[test]
fn test_list_uint8_nullable() {
    run_test_case("list_uint8_nullable")
}

#[test]
fn test_list_uint16_nullable() {
    run_test_case("list_uint16_nullable")
}

#[test]
fn test_list_uint32_nullable() {
    run_test_case("list_uint32_nullable")
}

#[test]
fn test_list_int8_nullable() {
    run_test_case("list_int8_nullable")
}

#[test]
fn test_list_int16_nullable() {
    run_test_case("list_int16_nullable")
}

#[test]
fn test_list_int32_nullable() {
    run_test_case("list_int32_nullable")
}

#[test]
fn test_list_int64_nullable() {
    run_test_case("list_int64_nullable")
}

#[test]
fn test_list_float32_nullable() {
    run_test_case("list_float32_nullable")
}

#[test]
fn test_list_float64_nullable() {
    run_test_case("list_float64_nullable")
}

#[test]
fn test_list_decimal32_nullable() {
    run_test_case("list_decimal32_nullable")
}

#[test]
fn test_list_decimal64_nullable() {
    run_test_case("list_decimal64_nullable")
}

#[test]
fn test_list_decimal128_nullable() {
    run_test_case("list_decimal128_nullable")
}

#[test]
fn test_list_timestamp_us_notz_nullable() {
    run_test_case("list_timestamp_us_notz_nullable")
}

#[test]
fn test_list_timestamp_ms_notz_nullable() {
    run_test_case("list_timestamp_ms_notz_nullable")
}

#[test]
fn test_list_timestamp_s_notz_nullable() {
    run_test_case("list_timestamp_s_notz_nullable")
}

#[test]
fn test_list_timestamp_us_tz_nullable() {
    run_test_case("list_timestamp_us_tz_nullable")
}

#[test]
fn test_list_timestamp_ms_tz_nullable() {
    run_test_case("list_timestamp_ms_tz_nullable")
}

#[test]
fn test_list_timestamp_s_tz_nullable() {
    run_test_case("list_timestamp_s_tz_nullable")
}

#[test]
fn test_list_time_s_nullable() {
    run_test_case("list_time_s_nullable")
}

#[test]
fn test_list_time_ms_nullable() {
    run_test_case("list_time_ms_nullable")
}

#[test]
fn test_list_time_us_nullable() {
    run_test_case("list_time_us_nullable")
}

#[test]
fn test_list_date32_nullable() {
    run_test_case("list_date32_nullable")
}

#[test]
fn test_list_duration_us_nullable() {
    run_test_case("list_duration_us_nullable")
}

#[test]
fn test_list_duration_ms_nullable() {
    run_test_case("list_duration_ms_nullable")
}

#[test]
fn test_list_duration_s_nullable() {
    run_test_case("list_duration_s_nullable")
}

#[test]
fn test_list_binary_nullable() {
    run_test_case("list_binary_nullable")
}

#[test]
fn test_list_large_binary_nullable() {
    run_test_case("list_large_binary_nullable")
}

#[test]
fn test_list_string_nullable() {
    run_test_case("list_string_nullable")
}

#[test]
fn test_list_large_string_nullable() {
    run_test_case("list_large_string_nullable")
}

#[test]
fn test_list_string_view_nullable() {
    run_test_case("list_string_view_nullable")
}

#[test]
fn test_list_nullable_bool() {
    run_test_case("list_nullable_bool")
}

#[test]
fn test_list_nullable_uint8() {
    run_test_case("list_nullable_uint8")
}

#[test]
fn test_list_nullable_uint16() {
    run_test_case("list_nullable_uint16")
}

#[test]
fn test_list_nullable_uint32() {
    run_test_case("list_nullable_uint32")
}

#[test]
fn test_list_nullable_int8() {
    run_test_case("list_nullable_int8")
}

#[test]
fn test_list_nullable_int16() {
    run_test_case("list_nullable_int16")
}

#[test]
fn test_list_nullable_int32() {
    run_test_case("list_nullable_int32")
}

#[test]
fn test_list_nullable_int64() {
    run_test_case("list_nullable_int64")
}

#[test]
fn test_list_nullable_float32() {
    run_test_case("list_nullable_float32")
}

#[test]
fn test_list_nullable_float64() {
    run_test_case("list_nullable_float64")
}

#[test]
fn test_list_nullable_decimal32() {
    run_test_case("list_nullable_decimal32")
}

#[test]
fn test_list_nullable_decimal64() {
    run_test_case("list_nullable_decimal64")
}

#[test]
fn test_list_nullable_decimal128() {
    run_test_case("list_nullable_decimal128")
}

#[test]
fn test_list_nullable_timestamp_us_notz() {
    run_test_case("list_nullable_timestamp_us_notz")
}

#[test]
fn test_list_nullable_timestamp_ms_notz() {
    run_test_case("list_nullable_timestamp_ms_notz")
}

#[test]
fn test_list_nullable_timestamp_s_notz() {
    run_test_case("list_nullable_timestamp_s_notz")
}

#[test]
fn test_list_nullable_timestamp_us_tz() {
    run_test_case("list_nullable_timestamp_us_tz")
}

#[test]
fn test_list_nullable_timestamp_ms_tz() {
    run_test_case("list_nullable_timestamp_ms_tz")
}

#[test]
fn test_list_nullable_timestamp_s_tz() {
    run_test_case("list_nullable_timestamp_s_tz")
}

#[test]
fn test_list_nullable_time_s() {
    run_test_case("list_nullable_time_s")
}

#[test]
fn test_list_nullable_time_ms() {
    run_test_case("list_nullable_time_ms")
}

#[test]
fn test_list_nullable_time_us() {
    run_test_case("list_nullable_time_us")
}

#[test]
fn test_list_nullable_date32() {
    run_test_case("list_nullable_date32")
}

#[test]
fn test_list_nullable_duration_us() {
    run_test_case("list_nullable_duration_us")
}

#[test]
fn test_list_nullable_duration_ms() {
    run_test_case("list_nullable_duration_ms")
}

#[test]
fn test_list_nullable_duration_s() {
    run_test_case("list_nullable_duration_s")
}

#[test]
fn test_list_nullable_binary() {
    run_test_case("list_nullable_binary")
}

#[test]
fn test_list_nullable_large_binary() {
    run_test_case("list_nullable_large_binary")
}

#[test]
fn test_list_nullable_string() {
    run_test_case("list_nullable_string")
}

#[test]
fn test_list_nullable_large_string() {
    run_test_case("list_nullable_large_string")
}

#[test]
fn test_list_nullable_string_view() {
    run_test_case("list_nullable_string_view")
}

#[test]
fn test_list_nullable_bool_nullable() {
    run_test_case("list_nullable_bool_nullable")
}

#[test]
fn test_list_nullable_uint8_nullable() {
    run_test_case("list_nullable_uint8_nullable")
}

#[test]
fn test_list_nullable_uint16_nullable() {
    run_test_case("list_nullable_uint16_nullable")
}

#[test]
fn test_list_nullable_uint32_nullable() {
    run_test_case("list_nullable_uint32_nullable")
}

#[test]
fn test_list_nullable_int8_nullable() {
    run_test_case("list_nullable_int8_nullable")
}

#[test]
fn test_list_nullable_int16_nullable() {
    run_test_case("list_nullable_int16_nullable")
}

#[test]
fn test_list_nullable_int32_nullable() {
    run_test_case("list_nullable_int32_nullable")
}

#[test]
fn test_list_nullable_int64_nullable() {
    run_test_case("list_nullable_int64_nullable")
}

#[test]
fn test_list_nullable_float32_nullable() {
    run_test_case("list_nullable_float32_nullable")
}

#[test]
fn test_list_nullable_float64_nullable() {
    run_test_case("list_nullable_float64_nullable")
}

#[test]
fn test_list_nullable_decimal32_nullable() {
    run_test_case("list_nullable_decimal32_nullable")
}

#[test]
fn test_list_nullable_decimal64_nullable() {
    run_test_case("list_nullable_decimal64_nullable")
}

#[test]
fn test_list_nullable_decimal128_nullable() {
    run_test_case("list_nullable_decimal128_nullable")
}

#[test]
fn test_list_nullable_timestamp_us_notz_nullable() {
    run_test_case("list_nullable_timestamp_us_notz_nullable")
}

#[test]
fn test_list_nullable_timestamp_ms_notz_nullable() {
    run_test_case("list_nullable_timestamp_ms_notz_nullable")
}

#[test]
fn test_list_nullable_timestamp_s_notz_nullable() {
    run_test_case("list_nullable_timestamp_s_notz_nullable")
}

#[test]
fn test_list_nullable_timestamp_us_tz_nullable() {
    run_test_case("list_nullable_timestamp_us_tz_nullable")
}

#[test]
fn test_list_nullable_timestamp_ms_tz_nullable() {
    run_test_case("list_nullable_timestamp_ms_tz_nullable")
}

#[test]
fn test_list_nullable_timestamp_s_tz_nullable() {
    run_test_case("list_nullable_timestamp_s_tz_nullable")
}

#[test]
fn test_list_nullable_time_s_nullable() {
    run_test_case("list_nullable_time_s_nullable")
}

#[test]
fn test_list_nullable_time_ms_nullable() {
    run_test_case("list_nullable_time_ms_nullable")
}

#[test]
fn test_list_nullable_time_us_nullable() {
    run_test_case("list_nullable_time_us_nullable")
}

#[test]
fn test_list_nullable_date32_nullable() {
    run_test_case("list_nullable_date32_nullable")
}

#[test]
fn test_list_nullable_duration_us_nullable() {
    run_test_case("list_nullable_duration_us_nullable")
}

#[test]
fn test_list_nullable_duration_ms_nullable() {
    run_test_case("list_nullable_duration_ms_nullable")
}

#[test]
fn test_list_nullable_duration_s_nullable() {
    run_test_case("list_nullable_duration_s_nullable")
}

#[test]
fn test_list_nullable_binary_nullable() {
    run_test_case("list_nullable_binary_nullable")
}

#[test]
fn test_list_nullable_large_binary_nullable() {
    run_test_case("list_nullable_large_binary_nullable")
}

#[test]
fn test_list_nullable_string_nullable() {
    run_test_case("list_nullable_string_nullable")
}

#[test]
fn test_list_nullable_large_string_nullable() {
    run_test_case("list_nullable_large_string_nullable")
}

#[test]
fn test_list_nullable_string_view_nullable() {
    run_test_case("list_nullable_string_view_nullable")
}

/// Confirm that the binary snapshots are loaded to Postgres correctly.
#[test]
fn validate_snapshots() {
    let settings = Settings {
        timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    };
    let mut postgresql = PostgreSQL::new(settings);
    postgresql.setup().unwrap();
    postgresql.start().unwrap();
    postgresql.create_database("test").unwrap();
    let settings = postgresql.settings();

    let mut client = Client::connect(
        format!(
            "host=localhost port={} user={} password={} dbname=test",
            settings.port, settings.username, settings.password
        )
        .as_str(),
        NoTls,
    )
    .unwrap();

    let binary_snapshots_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/snapshots");
    let csv_snapshots_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/snapshots_csv");
    let arrow_data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/testdata");
    let mut failed = vec![];
    let mut created = vec![];

    for entry in fs::read_dir(binary_snapshots_path)
        .unwrap()
        .filter_map(Result::ok)
    {
        let path = entry.path();
        if !(path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("bin")) {
            continue;
        }

        let name = path.file_stem().unwrap().to_str().unwrap().to_owned();
        let binary_content = fs::read(path.clone()).unwrap();
        let (_, schema) = read_batches(arrow_data_path.join(format!("{name}.arrow")));
        let encoder = ArrowToPostgresBinaryEncoder::try_new(&schema).unwrap();
        let columns_and_types = encoder
            .schema()
            .columns
            .iter()
            .map(|c| format!("\"{}\" {}", c.0, c.1.data_type.name().unwrap()))
            .collect::<Vec<_>>()
            .join(", ");

        client
            .execute(
                &format!("create table \"{name}\" ({columns_and_types})"),
                &[],
            )
            .unwrap();

        // load snapshot data to Postgres
        let mut writer = client
            .copy_in(format!("copy \"{name}\" from stdin binary").as_str())
            .unwrap();
        writer.write_all(&binary_content).unwrap();
        writer.finish().unwrap();

        // export to csv
        let mut pg_csv = String::new();
        client
            .copy_out(
                format!(
                    "copy (select * from \"{name}\" order by ctid) to stdout (format csv, header true, null 'null')"
                )
                .as_str(),
            )
            .unwrap()
            .read_to_string(&mut pg_csv)
            .unwrap();

        // compare against the existing csv; if it does not exist, create a new one.
        let csv_snapshot_file = csv_snapshots_path.join(format!("{name}.csv"));
        if csv_snapshot_file.exists() {
            let csv_snapshot = fs::read_to_string(csv_snapshot_file).unwrap();
            if csv_snapshot != pg_csv {
                pretty_print_diff(TextDiff::from_lines(&csv_snapshot, &pg_csv));
                failed.push(name);
            }
        } else {
            let mut file = File::create(csv_snapshot_file).unwrap();
            write!(file, "{}", pg_csv).unwrap();
            created.push(name.clone());
            failed.push(name);
        }
    }

    postgresql.stop().unwrap();

    println!("created csv snapshots: {:?}", created);
    assert_eq!(failed, Vec::<String>::new());
}

// from https://github.com/mitsuhiko/similar/blob/main/examples/terminal.rs
fn pretty_print_diff(diff: TextDiff<'_, '_, '_, str>) {
    for op in diff.ops() {
        for change in diff.iter_changes(op) {
            let (sign, style) = match change.tag() {
                ChangeTag::Delete => ("-", Style::new().red()),
                ChangeTag::Insert => ("+", Style::new().green()),
                ChangeTag::Equal => (" ", Style::new()),
            };
            print!("{}{}", style.apply_to(sign).bold(), style.apply_to(change));
        }
    }
}
