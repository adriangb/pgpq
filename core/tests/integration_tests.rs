mod harness;

use std::cmp::min;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::PathBuf;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use bytes::BytesMut;
use console::Style;
use pgpq::ArrowToPostgresBinaryEncoder;
use pgpq::pg_schema::{Column, PostgresType};
use similar::{ChangeTag, TextDiff};

use harness::cases::{Case, all_cases, custom_encoder_cases, read_batches};
use harness::db::TestDb;
use harness::value::Value;

/// The composite OIDs the test corpus encodes with.
///
/// Composite OIDs belong to the database being loaded, so pgpq makes the caller supply them
/// (#96). The snapshots are byte-exact files that predate that, and they were produced with the
/// old hard-coded 16385 — which is also what a *fresh* cluster allocates for the first type the
/// generated DDL creates, so `validate_snapshots` still loads them. Naming the value here keeps
/// the snapshots reproducible and puts the assumption where it can be seen, rather than inside
/// the library where it silently applied to every user's database.
fn corpus_encoder(schema: &Schema) -> ArrowToPostgresBinaryEncoder {
    let encoder = ArrowToPostgresBinaryEncoder::try_new(schema).unwrap();
    let oids: HashMap<String, u32> = encoder
        .composite_type_names()
        .into_iter()
        .map(|name| (name, 16_385))
        .collect();
    if oids.is_empty() {
        return encoder;
    }
    encoder.with_composite_oids(&oids).unwrap()
}

fn run_test_case(case: &str) {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/testdata/{case}.arrow"));
    let (batches, schema) = read_batches(&path);
    let mut encoder = corpus_encoder(&schema);
    let mut buf = BytesMut::new();
    encoder.write_header(&mut buf).unwrap();
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

/// Byte level snapshots for the cases whose encoders are *not* the inferred ones.
///
/// [`run_test_case`] covers the generated `testdata/*.arrow` corpus, but it always builds the
/// encoder with `try_new`, so nothing pinned the bytes of a case built with
/// `try_new_with_encoders`. These live in a subdirectory of `tests/snapshots` because they have no
/// `.arrow` file behind them, which is what [`validate_snapshots`] keys off.
fn custom_encoder_snapshot(case: &Case) {
    let (buf, _) = harness::db::encode(&case.schema, &case.batches, case.encoders.as_ref())
        .unwrap_or_else(|err| panic!("{}: encoding failed: {err}", case.name));

    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/snapshots/custom_encoders");
    fs::create_dir_all(&dir).unwrap();
    let snap_file = dir.join(format!("{}.bin", case.name));
    if !snap_file.exists() {
        fs::write(&snap_file, &buf[..]).unwrap();
        panic!("wrote new snap at {snap_file:?}")
    }
    assert_eq!(
        fs::read(&snap_file).unwrap(),
        &buf[..],
        "{} did not match {snap_file:?}",
        case.name
    );
}

#[test]
fn validate_custom_encoder_snapshots() {
    let cases = custom_encoder_cases();
    assert!(!cases.is_empty());
    for case in &cases {
        custom_encoder_snapshot(case);
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
fn test_uint64() {
    run_test_case("uint64")
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
fn test_float16() {
    run_test_case("float16")
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
fn test_fixed_size_binary() {
    run_test_case("fixed_size_binary")
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
fn test_uint64_nullable() {
    run_test_case("uint64_nullable")
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
fn test_float16_nullable() {
    run_test_case("float16_nullable")
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
fn test_fixed_size_binary_nullable() {
    run_test_case("fixed_size_binary_nullable")
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
fn test_list_uint64() {
    run_test_case("list_uint64")
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
fn test_list_float16() {
    run_test_case("list_float16")
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
fn test_list_fixed_size_binary() {
    run_test_case("list_fixed_size_binary")
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
fn test_list_uint64_nullable() {
    run_test_case("list_uint64_nullable")
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
fn test_list_float16_nullable() {
    run_test_case("list_float16_nullable")
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
fn test_list_fixed_size_binary_nullable() {
    run_test_case("list_fixed_size_binary_nullable")
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
fn test_list_nullable_uint64() {
    run_test_case("list_nullable_uint64")
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
fn test_list_nullable_float16() {
    run_test_case("list_nullable_float16")
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
fn test_list_nullable_fixed_size_binary() {
    run_test_case("list_nullable_fixed_size_binary")
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
fn test_list_nullable_uint64_nullable() {
    run_test_case("list_nullable_uint64_nullable")
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
fn test_list_nullable_float16_nullable() {
    run_test_case("list_nullable_float16_nullable")
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
fn test_list_nullable_fixed_size_binary_nullable() {
    run_test_case("list_nullable_fixed_size_binary_nullable")
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

#[test]
fn test_large_list_int32() {
    run_test_case("large_list_int32")
}

#[test]
fn test_large_list_string() {
    run_test_case("large_list_string")
}

#[test]
fn test_large_list_int32_nullable() {
    run_test_case("large_list_int32_nullable")
}

#[test]
fn test_large_list_string_nullable() {
    run_test_case("large_list_string_nullable")
}

#[test]
fn test_large_list_nullable_int32() {
    run_test_case("large_list_nullable_int32")
}

#[test]
fn test_large_list_nullable_string() {
    run_test_case("large_list_nullable_string")
}

#[test]
fn test_large_list_nullable_int32_nullable() {
    run_test_case("large_list_nullable_int32_nullable")
}

#[test]
fn test_large_list_nullable_string_nullable() {
    run_test_case("large_list_nullable_string_nullable")
}

#[test]
fn test_fixed_size_list_bool() {
    run_test_case("fixed_size_list_bool")
}

#[test]
fn test_fixed_size_list_int32() {
    run_test_case("fixed_size_list_int32")
}

#[test]
fn test_fixed_size_list_int64() {
    run_test_case("fixed_size_list_int64")
}

#[test]
fn test_fixed_size_list_float64() {
    run_test_case("fixed_size_list_float64")
}

#[test]
fn test_fixed_size_list_decimal128() {
    run_test_case("fixed_size_list_decimal128")
}

#[test]
fn test_fixed_size_list_timestamp_us_notz() {
    run_test_case("fixed_size_list_timestamp_us_notz")
}

#[test]
fn test_fixed_size_list_binary() {
    run_test_case("fixed_size_list_binary")
}

#[test]
fn test_fixed_size_list_fixed_size_binary() {
    run_test_case("fixed_size_list_fixed_size_binary")
}

#[test]
fn test_fixed_size_list_string() {
    run_test_case("fixed_size_list_string")
}

#[test]
fn test_fixed_size_list_bool_nullable() {
    run_test_case("fixed_size_list_bool_nullable")
}

#[test]
fn test_fixed_size_list_int32_nullable() {
    run_test_case("fixed_size_list_int32_nullable")
}

#[test]
fn test_fixed_size_list_int64_nullable() {
    run_test_case("fixed_size_list_int64_nullable")
}

#[test]
fn test_fixed_size_list_float64_nullable() {
    run_test_case("fixed_size_list_float64_nullable")
}

#[test]
fn test_fixed_size_list_decimal128_nullable() {
    run_test_case("fixed_size_list_decimal128_nullable")
}

#[test]
fn test_fixed_size_list_timestamp_us_notz_nullable() {
    run_test_case("fixed_size_list_timestamp_us_notz_nullable")
}

#[test]
fn test_fixed_size_list_binary_nullable() {
    run_test_case("fixed_size_list_binary_nullable")
}

#[test]
fn test_fixed_size_list_fixed_size_binary_nullable() {
    run_test_case("fixed_size_list_fixed_size_binary_nullable")
}

#[test]
fn test_fixed_size_list_string_nullable() {
    run_test_case("fixed_size_list_string_nullable")
}

#[test]
fn test_fixed_size_list_nullable_bool() {
    run_test_case("fixed_size_list_nullable_bool")
}

#[test]
fn test_fixed_size_list_nullable_int32() {
    run_test_case("fixed_size_list_nullable_int32")
}

#[test]
fn test_fixed_size_list_nullable_int64() {
    run_test_case("fixed_size_list_nullable_int64")
}

#[test]
fn test_fixed_size_list_nullable_float64() {
    run_test_case("fixed_size_list_nullable_float64")
}

#[test]
fn test_fixed_size_list_nullable_decimal128() {
    run_test_case("fixed_size_list_nullable_decimal128")
}

#[test]
fn test_fixed_size_list_nullable_timestamp_us_notz() {
    run_test_case("fixed_size_list_nullable_timestamp_us_notz")
}

#[test]
fn test_fixed_size_list_nullable_binary() {
    run_test_case("fixed_size_list_nullable_binary")
}

#[test]
fn test_fixed_size_list_nullable_fixed_size_binary() {
    run_test_case("fixed_size_list_nullable_fixed_size_binary")
}

#[test]
fn test_fixed_size_list_nullable_string() {
    run_test_case("fixed_size_list_nullable_string")
}

#[test]
fn test_fixed_size_list_nullable_bool_nullable() {
    run_test_case("fixed_size_list_nullable_bool_nullable")
}

#[test]
fn test_fixed_size_list_nullable_int32_nullable() {
    run_test_case("fixed_size_list_nullable_int32_nullable")
}

#[test]
fn test_fixed_size_list_nullable_int64_nullable() {
    run_test_case("fixed_size_list_nullable_int64_nullable")
}

#[test]
fn test_fixed_size_list_nullable_float64_nullable() {
    run_test_case("fixed_size_list_nullable_float64_nullable")
}

#[test]
fn test_fixed_size_list_nullable_decimal128_nullable() {
    run_test_case("fixed_size_list_nullable_decimal128_nullable")
}

#[test]
fn test_fixed_size_list_nullable_timestamp_us_notz_nullable() {
    run_test_case("fixed_size_list_nullable_timestamp_us_notz_nullable")
}

#[test]
fn test_fixed_size_list_nullable_binary_nullable() {
    run_test_case("fixed_size_list_nullable_binary_nullable")
}

#[test]
fn test_fixed_size_list_nullable_fixed_size_binary_nullable() {
    run_test_case("fixed_size_list_nullable_fixed_size_binary_nullable")
}

#[test]
fn test_fixed_size_list_nullable_string_nullable() {
    run_test_case("fixed_size_list_nullable_string_nullable")
}

#[test]
fn test_struct_with_two_primitive_cols() {
    run_test_case("struct_with_two_primitive_cols")
}

#[test]
fn test_nested_struct() {
    run_test_case("nested_struct")
}

#[test]
fn test_struct_with_list() {
    run_test_case("struct_with_list")
}

/// Roundtrip every case through embedded Postgres and compare the *typed* values Postgres hands
/// back against the values implied by the source Arrow arrays.
///
/// This is the value level correctness gate: the CSV based [`validate_snapshots`] below only
/// compares text, which can hide differences that happen to render identically.
///
/// A single embedded Postgres instance is shared by every case; each case runs inside a
/// transaction that is rolled back, so the table and any composite types it needs disappear again.
#[test]
fn validate_roundtrip_values() {
    let cases = all_cases();
    assert!(!cases.is_empty(), "no roundtrip cases were found");

    let mut db = TestDb::start().expect("failed to start embedded postgres");
    let mut failures: Vec<String> = Vec::new();

    for case in &cases {
        let expected = case.expected();
        match db.roundtrip(
            &case.name,
            &case.schema,
            &case.batches,
            case.encoders.as_ref(),
        ) {
            Ok(actual) => {
                if let Some(diff) = describe_mismatch(case, &expected, &actual) {
                    failures.push(diff);
                }
            }
            Err(err) => failures.push(format!("{}: roundtrip failed: {err}", case.name)),
        }
    }

    println!(
        "checked {} cases / {} rows",
        cases.len(),
        harness::cases::total_rows(&cases)
    );
    assert!(
        failures.is_empty(),
        "{} of {} roundtrip cases did not match:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

/// Render the first difference between the expected and actual values of a case, if any.
fn describe_mismatch(
    case: &Case,
    expected: &[Vec<Value>],
    actual: &[Vec<Value>],
) -> Option<String> {
    if expected.len() != actual.len() {
        return Some(format!(
            "{}: expected {} rows, got {}",
            case.name,
            expected.len(),
            actual.len()
        ));
    }
    for (row, (expected_row, actual_row)) in expected.iter().zip(actual).enumerate() {
        if expected_row.len() != actual_row.len() {
            return Some(format!(
                "{}: row {row} has {} columns, expected {}",
                case.name,
                actual_row.len(),
                expected_row.len()
            ));
        }
        for (col, (expected_value, actual_value)) in expected_row.iter().zip(actual_row).enumerate()
        {
            // `semantically_equals` rather than `!=`: identical for every non-float variant, and
            // for floats it is both looser (NaN equals NaN) and stricter (`-0.0` does not equal
            // `0.0`) than `PartialEq`. See `harness::value::Value::semantically_equals`.
            if !expected_value.semantically_equals(actual_value) {
                let name = case
                    .schema
                    .fields()
                    .get(col)
                    .map(|f| f.name().clone())
                    .unwrap_or_else(|| col.to_string());
                return Some(format!(
                    "{}: row {row} column {name}:\n  expected: {expected_value:?}\n  actual:   {actual_value:?}",
                    case.name
                ));
            }
        }
    }
    None
}

/// Pin every array type OID in [`PostgresType::array_oid`] against `pg_type`.
///
/// These OIDs are hard coded because Postgres guarantees them for built-in types, and they are
/// load bearing: `record_recv` rejects a composite field whose declared OID names a type other
/// than the column's, so a struct with an array field only loads if the array OID is exactly
/// right. This checks the table rather than trusting it.
/// A nested composite must be encoded with the OID the *target database* gave it.
///
/// The old hard-coded `16385` survived only because a fresh cluster has no user objects and hands
/// that OID out first. Any database that has already created user-defined types shifts the
/// numbering, and the value pgpq wrote then named some other type entirely — which is what
/// `record_recv` checks. This test creates unrelated types first so the composite lands
/// elsewhere, then shows the encoder follows the server rather than a constant (#96).
#[test]
fn nested_composite_uses_the_databases_own_oid() {
    let (batches, schema) = read_batches(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/testdata/nested_struct.arrow"),
    );

    let mut db = TestDb::start().expect("failed to start embedded postgres");
    // Burn the low user OIDs so the composite cannot land on the old placeholder.
    db.client()
        .batch_execute(
            "create type unrelated_a as (x int4); \
             create type unrelated_b as (y int4); \
             create type unrelated_c as (z int4);",
        )
        .unwrap();

    // Create the nested composite's type exactly as the DDL would, to read its real OID.
    db.client()
        .batch_execute("create type s_t as (b float4);")
        .unwrap();
    let real_oid: u32 = db
        .client()
        .query_one("select oid from pg_type where typname = 's_t'", &[])
        .unwrap()
        .get(0);
    assert_ne!(
        real_oid, 16_385,
        "this database was supposed to have shifted the composite off the old placeholder"
    );
    db.client().batch_execute("drop type s_t;").unwrap();

    // The OID really is on the wire: the same batch encoded with the database's OID and with the
    // old placeholder differ, and the real one appears in the bytes.
    let with_real = encode_nested_struct(&schema, &batches, real_oid);
    let with_placeholder = encode_nested_struct(&schema, &batches, 16_385);
    assert_ne!(
        with_real, with_placeholder,
        "the composite field header should carry the OID"
    );
    assert!(
        with_real.windows(4).any(|w| w == real_oid.to_be_bytes()),
        "the encoded buffer should contain the database's own OID"
    );

    // And the roundtrip works on this database, because the harness asks `pg_type` for the OID
    // and hands it to `with_composite_oids` rather than assuming one.
    let rows = db
        .roundtrip("nested_struct_shifted", &schema, &batches, None)
        .expect("nested composite must load when the database's own OID is used");
    assert_eq!(rows.len(), 1);
}

/// Encode the `nested_struct` corpus case, declaring `oid` for its inner composite.
fn encode_nested_struct(schema: &Schema, batches: &[RecordBatch], oid: u32) -> BytesMut {
    let mut encoder = ArrowToPostgresBinaryEncoder::try_new(schema)
        .unwrap()
        .with_composite_oids(&HashMap::from([("s_t".to_string(), oid)]))
        .unwrap();
    let mut buf = BytesMut::new();
    encoder.write_header(&mut buf).unwrap();
    for batch in batches {
        encoder.write_batch(batch, &mut buf).unwrap();
    }
    encoder.write_footer(&mut buf).unwrap();
    buf
}

/// Every scalar OID pgpq writes must be the OID Postgres actually has for that type.
///
/// These OIDs go out on the wire in composite field headers and array element headers, where
/// `record_recv`/`array_recv` compare them against the column's real type and reject a mismatch.
/// `Json` claimed jsonb's 3802 (#96) with nothing to catch it: a scalar column's OID is never
/// written, so the error only surfaced once a JSON column was nested inside an array or composite.
#[test]
fn scalar_oids_match_pg_type() {
    // (pgpq type, the `pg_type.typname` it maps to)
    let expected: Vec<(PostgresType, &str)> = vec![
        (PostgresType::Bool, "bool"),
        (PostgresType::Bytea, "bytea"),
        (PostgresType::Char, "char"),
        (PostgresType::Int2, "int2"),
        (PostgresType::Int4, "int4"),
        (PostgresType::Int8, "int8"),
        (PostgresType::Text, "text"),
        (PostgresType::Float4, "float4"),
        (PostgresType::Float8, "float8"),
        (PostgresType::Numeric, "numeric"),
        (PostgresType::Date, "date"),
        (PostgresType::Time, "time"),
        (PostgresType::Timestamp, "timestamp"),
        (PostgresType::Interval, "interval"),
        (PostgresType::Json, "json"),
        (PostgresType::Jsonb, "jsonb"),
    ];

    let mut db = TestDb::start().expect("failed to start embedded postgres");
    let client = db.client();
    for (tp, typname) in expected {
        let oid: u32 = client
            .query_one(
                "select oid from pg_type \
                 where typname = $1 and typnamespace = 'pg_catalog'::regnamespace",
                &[&typname],
            )
            .unwrap_or_else(|e| panic!("looking up {typname}: {e}"))
            .get(0);
        assert_eq!(tp.oid(), Some(oid), "oid for {tp:?} ({typname})");
    }
}

#[test]
fn array_oids_match_pg_type() {
    // (pgpq type, the `pg_type.typname` of the element type it maps to)
    let expected: Vec<(PostgresType, &str)> = vec![
        (PostgresType::Bool, "bool"),
        (PostgresType::Bytea, "bytea"),
        (PostgresType::Char, "char"),
        (PostgresType::Int2, "int2"),
        (PostgresType::Int4, "int4"),
        (PostgresType::Int8, "int8"),
        (PostgresType::Text, "text"),
        (PostgresType::Float4, "float4"),
        (PostgresType::Float8, "float8"),
        (PostgresType::Numeric, "numeric"),
        (PostgresType::Date, "date"),
        (PostgresType::Time, "time"),
        (PostgresType::Timestamp, "timestamp"),
        (PostgresType::Interval, "interval"),
        (PostgresType::Json, "json"),
        (PostgresType::Jsonb, "jsonb"),
    ];

    let mut db = TestDb::start().expect("failed to start embedded postgres");
    let client = db.client();
    for (tp, typname) in expected {
        let typarray: u32 = client
            .query_one(
                "select typarray from pg_type \
                 where typname = $1 and typnamespace = 'pg_catalog'::regnamespace",
                &[&typname],
            )
            .unwrap_or_else(|e| panic!("looking up {typname}: {e}"))
            .get(0);
        assert_eq!(
            tp.array_oid(),
            Some(typarray),
            "array oid for {tp:?} (_{typname})"
        );
    }

    // Nested cases have no stable OID: Postgres creates the array type together with the
    // composite, and has no array-of-arrays type at all.
    let int4 = Box::new(Column {
        name: "item".to_string(),
        data_type: PostgresType::Int4,
        nullable: true,
    });
    assert_eq!(PostgresType::List(int4.clone()).array_oid(), None);
    assert_eq!(
        PostgresType::UserDefined {
            fields: vec![int4],
            oid: None,
        }
        .array_oid(),
        None
    );
}

/// Confirm that the binary snapshots are loaded to Postgres correctly.
#[test]
fn validate_snapshots() {
    let mut db = TestDb::start().expect("failed to start embedded postgres");
    let client = db.client();

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
        let (_, schema) = read_batches(&arrow_data_path.join(format!("{name}.arrow")));
        let encoder = ArrowToPostgresBinaryEncoder::try_new(&schema).unwrap();

        // Use ddl() to generate the CREATE TABLE statement (with any required CREATE TYPE for structs)
        let ddl = encoder.schema().ddl(&name, false);
        client.batch_execute(&ddl).unwrap();

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

    println!("created csv snapshots: {:?}", created);
    assert_eq!(failed, Vec::<String>::new());
}

// from https://github.com/mitsuhiko/similar/blob/main/examples/terminal.rs
// similar 3 dropped the third ('bufs) lifetime from `TextDiff`.
fn pretty_print_diff(diff: TextDiff<'_, '_, str>) {
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
