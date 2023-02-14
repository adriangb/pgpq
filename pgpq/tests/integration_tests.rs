use std::cmp::min;
use std::fs::{self, File};
use std::path::PathBuf;

use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReader;
use arrow_schema::Schema;
use bytes::BytesMut;
use pgpq::ArrowToPostgresBinaryEncoder;

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
fn test_list_nullable_list_bool() {
    run_test_case("list_nullable_list_bool")
}

#[test]
fn test_list_nullable_list_uint8() {
    run_test_case("list_nullable_list_uint8")
}

#[test]
fn test_list_nullable_list_uint16() {
    run_test_case("list_nullable_list_uint16")
}

#[test]
fn test_list_nullable_list_uint32() {
    run_test_case("list_nullable_list_uint32")
}

#[test]
fn test_list_nullable_list_int8() {
    run_test_case("list_nullable_list_int8")
}

#[test]
fn test_list_nullable_list_int16() {
    run_test_case("list_nullable_list_int16")
}

#[test]
fn test_list_nullable_list_int32() {
    run_test_case("list_nullable_list_int32")
}

#[test]
fn test_list_nullable_list_int64() {
    run_test_case("list_nullable_list_int64")
}

#[test]
fn test_list_nullable_list_float32() {
    run_test_case("list_nullable_list_float32")
}

#[test]
fn test_list_nullable_list_float64() {
    run_test_case("list_nullable_list_float64")
}

#[test]
fn test_list_nullable_list_timestamp_us_notz() {
    run_test_case("list_nullable_list_timestamp_us_notz")
}

#[test]
fn test_list_nullable_list_timestamp_ms_notz() {
    run_test_case("list_nullable_list_timestamp_ms_notz")
}

#[test]
fn test_list_nullable_list_timestamp_s_notz() {
    run_test_case("list_nullable_list_timestamp_s_notz")
}

#[test]
fn test_list_nullable_list_timestamp_us_tz() {
    run_test_case("list_nullable_list_timestamp_us_tz")
}

#[test]
fn test_list_nullable_list_timestamp_ms_tz() {
    run_test_case("list_nullable_list_timestamp_ms_tz")
}

#[test]
fn test_list_nullable_list_timestamp_s_tz() {
    run_test_case("list_nullable_list_timestamp_s_tz")
}

#[test]
fn test_list_nullable_list_time_s() {
    run_test_case("list_nullable_list_time_s")
}

#[test]
fn test_list_nullable_list_time_ms() {
    run_test_case("list_nullable_list_time_ms")
}

#[test]
fn test_list_nullable_list_time_us() {
    run_test_case("list_nullable_list_time_us")
}

#[test]
fn test_list_nullable_list_duration_us() {
    run_test_case("list_nullable_list_duration_us")
}

#[test]
fn test_list_nullable_list_duration_ms() {
    run_test_case("list_nullable_list_duration_ms")
}

#[test]
fn test_list_nullable_list_duration_s() {
    run_test_case("list_nullable_list_duration_s")
}

#[test]
fn test_list_nullable_list_binary() {
    run_test_case("list_nullable_list_binary")
}

#[test]
fn test_list_nullable_list_large_binary() {
    run_test_case("list_nullable_list_large_binary")
}

#[test]
fn test_list_nullable_list_string() {
    run_test_case("list_nullable_list_string")
}

#[test]
fn test_list_nullable_list_large_string() {
    run_test_case("list_nullable_list_large_string")
}

#[test]
fn test_list_nullable_list_bool_nullable() {
    run_test_case("list_nullable_list_bool_nullable")
}

#[test]
fn test_list_nullable_list_uint8_nullable() {
    run_test_case("list_nullable_list_uint8_nullable")
}

#[test]
fn test_list_nullable_list_uint16_nullable() {
    run_test_case("list_nullable_list_uint16_nullable")
}

#[test]
fn test_list_nullable_list_uint32_nullable() {
    run_test_case("list_nullable_list_uint32_nullable")
}

#[test]
fn test_list_nullable_list_int8_nullable() {
    run_test_case("list_nullable_list_int8_nullable")
}

#[test]
fn test_list_nullable_list_int16_nullable() {
    run_test_case("list_nullable_list_int16_nullable")
}

#[test]
fn test_list_nullable_list_int32_nullable() {
    run_test_case("list_nullable_list_int32_nullable")
}

#[test]
fn test_list_nullable_list_int64_nullable() {
    run_test_case("list_nullable_list_int64_nullable")
}

#[test]
fn test_list_nullable_list_float32_nullable() {
    run_test_case("list_nullable_list_float32_nullable")
}

#[test]
fn test_list_nullable_list_float64_nullable() {
    run_test_case("list_nullable_list_float64_nullable")
}

#[test]
fn test_list_nullable_list_timestamp_us_notz_nullable() {
    run_test_case("list_nullable_list_timestamp_us_notz_nullable")
}

#[test]
fn test_list_nullable_list_timestamp_ms_notz_nullable() {
    run_test_case("list_nullable_list_timestamp_ms_notz_nullable")
}

#[test]
fn test_list_nullable_list_timestamp_s_notz_nullable() {
    run_test_case("list_nullable_list_timestamp_s_notz_nullable")
}

#[test]
fn test_list_nullable_list_timestamp_us_tz_nullable() {
    run_test_case("list_nullable_list_timestamp_us_tz_nullable")
}

#[test]
fn test_list_nullable_list_timestamp_ms_tz_nullable() {
    run_test_case("list_nullable_list_timestamp_ms_tz_nullable")
}

#[test]
fn test_list_nullable_list_timestamp_s_tz_nullable() {
    run_test_case("list_nullable_list_timestamp_s_tz_nullable")
}

#[test]
fn test_list_nullable_list_time_s_nullable() {
    run_test_case("list_nullable_list_time_s_nullable")
}

#[test]
fn test_list_nullable_list_time_ms_nullable() {
    run_test_case("list_nullable_list_time_ms_nullable")
}

#[test]
fn test_list_nullable_list_time_us_nullable() {
    run_test_case("list_nullable_list_time_us_nullable")
}

#[test]
fn test_list_nullable_list_duration_us_nullable() {
    run_test_case("list_nullable_list_duration_us_nullable")
}

#[test]
fn test_list_nullable_list_duration_ms_nullable() {
    run_test_case("list_nullable_list_duration_ms_nullable")
}

#[test]
fn test_list_nullable_list_duration_s_nullable() {
    run_test_case("list_nullable_list_duration_s_nullable")
}

#[test]
fn test_list_nullable_list_binary_nullable() {
    run_test_case("list_nullable_list_binary_nullable")
}

#[test]
fn test_list_nullable_list_large_binary_nullable() {
    run_test_case("list_nullable_list_large_binary_nullable")
}

#[test]
fn test_list_nullable_list_string_nullable() {
    run_test_case("list_nullable_list_string_nullable")
}

#[test]
fn test_list_nullable_list_large_string_nullable() {
    run_test_case("list_nullable_list_large_string_nullable")
}
