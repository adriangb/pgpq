//! End-to-end checks that the NUMERIC wire format we produce for decimals means to Postgres what
//! we think it means.
//!
//! These are the regressions from <https://github.com/adriangb/pgpq/issues/79>: values whose
//! encoding used to be silently wrong (a dropped leading zero base-10000 group), used to panic
//! (a fractional part padded past the width of the backing integer), or used to underflow
//! (a negative Arrow scale). Byte-for-byte snapshots cannot catch the first class of bug because
//! the bytes are self-consistently wrong, so everything here is asserted against what Postgres
//! itself reads back.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Decimal128Array, Decimal32Array, Decimal64Array, RecordBatch};
use arrow_schema::{Field, Schema};
use bytes::BytesMut;
use pgpq::ArrowToPostgresBinaryEncoder;
use postgres::{Client, NoTls};
use postgresql_embedded::blocking::PostgreSQL;
use postgresql_embedded::Settings;

/// 38 nines: the largest magnitude a `Decimal128(38, _)` can hold.
const MAX_PRECISION_38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

/// A single-column case: an array of decimals and the text Postgres must return for each row.
struct Case {
    name: &'static str,
    array: Arc<dyn Array>,
    expected: Vec<&'static str>,
}

fn case(name: &'static str, array: Arc<dyn Array>, expected: Vec<&'static str>) -> Case {
    assert_eq!(array.len(), expected.len(), "{name}");
    Case {
        name,
        array,
        expected,
    }
}

fn decimal32(precision: u8, scale: i8, values: Vec<i32>) -> Arc<dyn Array> {
    Arc::new(
        Decimal32Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

fn decimal64(precision: u8, scale: i8, values: Vec<i64>) -> Arc<dyn Array> {
    Arc::new(
        Decimal64Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

fn decimal128(precision: u8, scale: i8, values: Vec<i128>) -> Arc<dyn Array> {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

fn cases() -> Vec<Case> {
    vec![
        // The reported corruption: the leading base-10000 group of the fractional part is zero.
        // This used to encode as `ndigits=2 weight=0 digits=[1, 1]`, i.e. `1.0001`.
        case(
            "leading_zero_group",
            decimal64(18, 8, vec![100_000_001, 1, 100_000_000, -100_000_001]),
            vec!["1.00000001", "0.00000001", "1.00000000", "-1.00000001"],
        ),
        // Was off by 10^4.
        case(
            "leading_zero_group_pure_fraction",
            decimal64(17, 14, vec![6_538_030]),
            vec!["0.00000006538030"],
        ),
        // Was off by 10^8.
        case(
            "leading_zero_groups_pure_fraction",
            decimal64(12, 10, vec![1, -1, 0]),
            vec!["0.0000000001", "-0.0000000001", "0.0000000000"],
        ),
        // Every scale puts the significant digit in a different base-10000 group.
        case(
            "all_scales",
            decimal64(18, 5, vec![1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]),
            vec![
                "0.00001", "0.00010", "0.00100", "0.01000", "0.10000", "1.00000", "10.00000",
            ],
        ),
        // Used to panic: "attempt to multiply with overflow" (scale >= 9 for i32).
        case(
            "decimal32_max_scale",
            decimal32(9, 9, vec![999_999_999, 1, 0, -999_999_999]),
            vec!["0.999999999", "0.000000001", "0.000000000", "-0.999999999"],
        ),
        // Used to panic (scale >= 17 for i64).
        case(
            "decimal64_max_scale",
            decimal64(18, 17, vec![99_999_999_999_999_999, 1]),
            vec!["0.99999999999999999", "0.00000000000000001"],
        ),
        case(
            "decimal64_full_precision",
            decimal64(18, 6, vec![999_999_999_999_999_999]),
            vec!["999999999999.999999"],
        ),
        // Precision 29..=38 is not covered by the property-test harness (`rust_decimal` tops out
        // at a 96 bit mantissa), and scale >= 37 used to panic.
        case(
            "decimal128_max_precision",
            decimal128(38, 0, vec![MAX_PRECISION_38, -MAX_PRECISION_38]),
            vec![
                "99999999999999999999999999999999999999",
                "-99999999999999999999999999999999999999",
            ],
        ),
        case(
            "decimal128_max_precision_and_scale",
            decimal128(38, 38, vec![MAX_PRECISION_38, 1]),
            vec![
                "0.99999999999999999999999999999999999999",
                "0.00000000000000000000000000000000000001",
            ],
        ),
        case(
            "decimal128_max_precision_scale_37",
            decimal128(38, 37, vec![MAX_PRECISION_38]),
            vec!["9.9999999999999999999999999999999999999"],
        ),
        // Arrow permits negative scales (`value * 10^-scale`); `byte_size_hint` used to underflow
        // on them.
        case(
            "negative_scale",
            decimal64(9, -2, vec![123, -7, 0, 999_999_999]),
            vec!["12300", "-700", "0", "99999999900"],
        ),
        // A shift by a whole number of base-10000 groups.
        case(
            "negative_scale_group_aligned",
            decimal32(9, -8, vec![1, -1]),
            vec!["100000000", "-100000000"],
        ),
        case(
            "negative_scale_extreme",
            decimal128(38, -38, vec![1]),
            vec!["100000000000000000000000000000000000000"],
        ),
    ]
}

#[test]
fn decimals_roundtrip_through_postgres() {
    let mut postgresql = PostgreSQL::new(Settings {
        timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    });
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

    let mut failures: Vec<String> = vec![];
    for Case {
        name,
        array,
        expected,
    } in cases()
    {
        let schema = Schema::new(vec![Field::new(
            "value",
            array.data_type().clone(),
            array.is_nullable(),
        )]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![array]).unwrap();

        let mut encoder = ArrowToPostgresBinaryEncoder::try_new(&schema).unwrap();
        let mut buf = BytesMut::new();
        encoder.write_header(&mut buf).unwrap();
        encoder.write_batch(&batch, &mut buf).unwrap();
        encoder.write_footer(&mut buf).unwrap();

        client
            .batch_execute(&encoder.schema().ddl(name, false))
            .unwrap();
        let mut writer = client
            .copy_in(format!("copy \"{name}\" from stdin binary").as_str())
            .unwrap();
        std::io::Write::write_all(&mut writer, &buf[..]).unwrap();
        writer.finish().unwrap();

        let actual: Vec<String> = client
            .query(
                format!("select value::text from \"{name}\" order by ctid").as_str(),
                &[],
            )
            .unwrap()
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        if actual != expected {
            failures.push(format!(
                "{name}: expected {expected:?}, postgres read {actual:?}"
            ));
        }
    }

    postgresql.stop().unwrap();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
