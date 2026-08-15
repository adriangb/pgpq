mod harness;

use std::cmp::min;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::PathBuf;

use bytes::BytesMut;
use console::Style;
use pgpq::ArrowToPostgresBinaryEncoder;
use similar::{ChangeTag, TextDiff};

use harness::cases::{all_cases, custom_encoder_cases, int8_char_case, read_batches, Case};
use harness::db::TestDb;
use harness::value::Value;

fn run_test_case(case: &str) {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/testdata/{case}.arrow"));
    let (batches, schema) = read_batches(&path);
    let mut encoder = ArrowToPostgresBinaryEncoder::try_new(&schema).unwrap();
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
    // Not part of `custom_encoder_cases` because Postgres will not load it; see
    // `int8_as_char_is_rejected_by_postgres`. The bytes are still worth pinning.
    custom_encoder_snapshot(&int8_char_case());
}

/// KNOWN BUG: an `Int8` column encoded as `PostgresType::Char` cannot be loaded into Postgres.
///
/// `Int8EncoderBuilder::new_with_output(_, Char)` changes only the *declared* column type; the
/// payload stays the two byte big-endian `i16` of the default `INT2` encoding (`Char`'s
/// `TypeSize` is `Fixed(2)`). But `PostgresType::Char`'s DDL name is `CHAR`, which Postgres
/// resolves to `bpchar` — a text type — so it reads those two bytes as UTF-8 and rejects them.
/// Every `i8` value fails, because the high byte is `0x00` for non-negative values and `0xff` for
/// negative ones and neither is valid UTF-8.
///
/// (`PostgresType::Char` also reports OID 18, which is Postgres' internal one byte `"char"`, a
/// third type again — and one that would reject a two byte payload as well.)
///
/// This test asserts the behaviour as it stands today so the gap is visible rather than merely
/// untested; it is expected to fail, loudly, when the encoding is fixed. Tracked as
/// <https://github.com/adriangb/pgpq/issues/95>.
#[test]
fn int8_as_char_is_rejected_by_postgres() {
    let case = int8_char_case();
    let mut db = TestDb::start().expect("failed to start embedded postgres");

    let err = db
        .roundtrip(
            &case.name,
            &case.schema,
            &case.batches,
            case.encoders.as_ref(),
        )
        .expect_err("Int8 -> Char now loads; the known bug is fixed, update this test");

    let message = err.to_string();
    assert!(
        format!("{err:?}").contains("invalid byte sequence for encoding"),
        "unexpected failure: {message} / {err:?}"
    );
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
fn test_struct_with_two_primitive_cols() {
    run_test_case("struct_with_two_primitive_cols")
}

#[test]
fn test_nested_struct() {
    run_test_case("nested_struct")
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
