//! Property based roundtrip tests: generate Arrow data, encode it with `pgpq`, `COPY` it into
//! embedded Postgres and check that the values Postgres hands back are the ones that went in.
//!
//! This reuses the harness built for the example based suite (`tests/harness`): a generated
//! [`Case`] is exactly the same shape as a hand written one, so [`TestDb::roundtrip`] and the
//! Arrow -> [`Value`] expectation mapping are shared verbatim.
//!
//! Layout
//! ------
//! * *value strategies* produce the scalar values of one Arrow type, deliberately over-sampling
//!   adversarial values (`NaN`, `±inf`, `-0.0`, `i64::MIN`, empty strings, unicode boundaries, …);
//! * *array strategies* wrap those in null masks (all valid / all null / random) so every column
//!   sees the null and zero-row edge cases;
//! * *case strategies* combine several columns and several batches (including empty batches) into
//!   a [`Case`];
//! * [`proptest_roundtrip`] runs one strategy ("family") per group of types against a single
//!   embedded Postgres instance.
//!
//! Case counts default to something that keeps `cargo test` in the tens of seconds and can be
//! raised with proptest's own `PROPTEST_CASES` environment variable, e.g.
//! `PROPTEST_CASES=512 cargo test --test proptest_roundtrip`.
//!
//! Shrunk counterexamples are persisted under `tests/proptest-regressions/`, which is committed:
//! a failure found in CI comes back as a deterministic regression case on the next run.

mod harness;

use std::cell::{Cell, RefCell};
use std::fmt::Debug;
use std::sync::Arc;

use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Decimal128Type, Decimal32Type, Decimal64Type,
    DurationMicrosecondType, DurationMillisecondType, DurationSecondType, Float16Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, LargeBinaryArray, LargeListArray, LargeStringArray,
    ListArray, PrimitiveArray, RecordBatch, StringArray, StringViewArray, StructArray,
};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use proptest::prelude::*;
use proptest::test_runner::{Config, FileFailurePersistence, TestCaseError, TestRunner};

use harness::cases::Case;
use harness::db::TestDb;
use harness::value::Value;

// ---------------------------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------------------------

/// Cases per family when `PROPTEST_CASES` is not set.
const DEFAULT_CASES: u32 = 64;
/// Maximum rows in a single generated batch.
const MAX_ROWS: usize = 6;
/// Maximum number of batches per case (a case with several batches exercises the repeated
/// `write_batch` path, and a zero-row batch in the middle is a useful edge case).
const MAX_BATCHES: usize = 3;
/// Maximum number of columns per case.
const MAX_COLUMNS: usize = 3;
/// Maximum number of elements in a generated list.
const MAX_LIST_LEN: usize = 4;
/// Maximum number of fields in a generated struct.
const MAX_STRUCT_FIELDS: usize = 3;

/// Significant decimal digits the *harness* can carry through a NUMERIC.
///
/// `rust_decimal::Decimal` (used on both sides: to express the expected value and to decode what
/// Postgres hands back) holds a 96 bit mantissa and a non-negative scale of at most 28, so the
/// Decimal128 space is drawn at precision 28 rather than at Arrow's 38 digits, and a negative
/// scale — which the harness materialises as `value * 10^-scale`, i.e. `precision + (-scale)`
/// significant digits — is floored accordingly (see [`scale_range`]).
///
/// This is an expectation-side limit, not an encoder limit: the encoder handles the full Arrow
/// range, which `core/tests/decimal_wire_format.rs` and the unit tests in `core/src/encoders.rs`
/// pin down byte-exactly, and Postgres itself would accept the wider values.
const MAX_HARNESS_NUMERIC_DIGITS: u8 = 28;

/// Timestamp bounds (microseconds since the Unix epoch) covering 1400-01-01 .. 2400-01-01. Wide
/// enough to exercise negative values and the Postgres epoch shift, narrow enough that every
/// `TimeUnit` variant can represent it without overflowing.
const MIN_TIMESTAMP_US: i64 = -17_987_443_200_000_000;
const MAX_TIMESTAMP_US: i64 = 13_569_465_600_000_000;

/// Date bounds in days since the Unix epoch: 0001-01-01 .. 9999-12-31. Postgres accepts a wider
/// range (4713 BC .. 5874897 AD) but reserves `i32::MIN`/`i32::MAX` for `-infinity`/`infinity`.
const MIN_DATE_DAYS: i32 = -719_162;
const MAX_DATE_DAYS: i32 = 2_932_896;

const MAX_TIME_US: i64 = 86_400_000_000 - 1;

// ---------------------------------------------------------------------------------------------
// Generic helpers
// ---------------------------------------------------------------------------------------------

/// Turn a list of strategies into a strategy over the list of their values.
///
/// Proptest has `collection::vec` for *homogeneous* strategies; this is the heterogeneous
/// equivalent needed to build one column per generated `DataType`.
fn all_of<T: Clone + Debug + 'static>(strategies: Vec<BoxedStrategy<T>>) -> BoxedStrategy<Vec<T>> {
    strategies.into_iter().fold(
        Just(Vec::new()).boxed(),
        |acc: BoxedStrategy<Vec<T>>, next| {
            (acc, next)
                .prop_map(|(mut acc, next)| {
                    acc.push(next);
                    acc
                })
                .boxed()
        },
    )
}

/// A validity mask of `len` entries (`true` == not null).
///
/// The all-valid and all-null variants are drawn explicitly so that "no nulls at all" and "every
/// value is null" are reached reliably rather than only by chance.
fn validity(len: usize) -> BoxedStrategy<Vec<bool>> {
    if len == 0 {
        return Just(Vec::new()).boxed();
    }
    prop_oneof![
        4 => Just(vec![true; len]),
        1 => Just(vec![false; len]),
        5 => prop::collection::vec(prop::bool::weighted(0.8), len..=len),
    ]
    .boxed()
}

/// `len` optional values drawn from `inner`, with nulls sprinkled by [`validity`].
fn opt_vec<T: Clone + Debug + 'static>(
    inner: BoxedStrategy<T>,
    len: usize,
) -> BoxedStrategy<Vec<Option<T>>> {
    (prop::collection::vec(inner, len..=len), validity(len))
        .prop_map(|(values, valid)| {
            values
                .into_iter()
                .zip(valid)
                .map(|(value, valid)| if valid { Some(value) } else { None })
                .collect()
        })
        .boxed()
}

fn null_buffer(valid: &[bool]) -> Option<NullBuffer> {
    if valid.iter().all(|v| *v) {
        None
    } else {
        Some(NullBuffer::from(valid.to_vec()))
    }
}

// ---------------------------------------------------------------------------------------------
// Scalar value strategies
// ---------------------------------------------------------------------------------------------

/// Signed/unsigned integer strategies: uniform values plus the boundary values that historically
/// break sign handling and width promotion.
macro_rules! int_strategy {
    ($name:ident, $ty:ty, [$($extra:expr),* $(,)?]) => {
        fn $name() -> BoxedStrategy<$ty> {
            prop_oneof![
                5 => any::<$ty>(),
                3 => prop::sample::select(vec![<$ty>::MIN, <$ty>::MAX, 0 as $ty, 1 as $ty $(, $extra)*]),
            ]
            .boxed()
        }
    };
}

int_strategy!(i8s, i8, [-1]);
int_strategy!(i16s, i16, [-1]);
int_strategy!(i32s, i32, [-1]);
int_strategy!(i64s, i64, [-1]);
int_strategy!(u8s, u8, []);
int_strategy!(u16s, u16, []);
int_strategy!(u32s, u32, []);
int_strategy!(u64s, u64, []);

fn f32s() -> BoxedStrategy<f32> {
    prop_oneof![
        5 => proptest::num::f32::ANY,
        3 => prop::sample::select(vec![
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            0.0,
            -0.0,
            1.0,
            -1.0,
            f32::MIN,
            f32::MAX,
            f32::MIN_POSITIVE,
            -f32::MIN_POSITIVE,
            f32::EPSILON,
        ]),
    ]
    .boxed()
}

fn f64s() -> BoxedStrategy<f64> {
    prop_oneof![
        5 => proptest::num::f64::ANY,
        3 => prop::sample::select(vec![
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            0.0,
            -0.0,
            1.0,
            -1.0,
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
            -f64::MIN_POSITIVE,
            f64::EPSILON,
        ]),
    ]
    .boxed()
}

/// Strings avoid `NUL`: Postgres rejects it in `text` regardless of what pgpq does with it, so
/// generating it would only test Postgres' input validation.
fn strings() -> BoxedStrategy<String> {
    prop_oneof![
        6 => proptest::string::string_regex("[^\u{0}]{0,24}").unwrap(),
        3 => prop::sample::select(
            [
                "",
                " ",
                "\t",
                "\n",
                "\r\n",
                "\\",
                "\"",
                "'",
                "\u{7f}",   // last 1 byte code point
                "\u{80}",   // first 2 byte code point
                "\u{7ff}",  // last 2 byte code point
                "\u{800}",  // first 3 byte code point
                "\u{ffff}", // last 3 byte code point
                "\u{10000}", // first 4 byte code point
                "\u{10ffff}", // last code point
                "é",
                "e\u{301}", // combining accent
                "日本語",
                "🦀🦀🦀",
                "\u{202e}rtl",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        ),
        1 => proptest::string::string_regex("[a-zA-Z0-9 ]{512,2048}").unwrap(),
    ]
    .boxed()
}

fn binaries() -> BoxedStrategy<Vec<u8>> {
    prop_oneof![
        6 => prop::collection::vec(any::<u8>(), 0..=32),
        2 => Just(Vec::new()),
        1 => prop::collection::vec(any::<u8>(), 512..=2048),
    ]
    .boxed()
}

/// Values that fit `precision` decimal digits, biased towards the boundaries.
///
/// The whole range is generated: the leading-zero-fractional-group corruption that used to force
/// an exclusion here (issue #79) was fixed by PR #85, so values such as `1.00000001` — whose most
/// significant base-10000 fractional group is zero — are now covered.
macro_rules! decimal_values {
    ($name:ident, $ty:ty) => {
        fn $name(precision: u8) -> BoxedStrategy<$ty> {
            let max = (10 as $ty).pow(precision as u32) - 1;
            prop_oneof![
                5 => -max..=max,
                3 => prop::sample::select(vec![0, 1, -1, max, -max]),
            ]
            .boxed()
        }
    };
}

decimal_values!(decimal32_values, i32);
decimal_values!(decimal64_values, i64);
decimal_values!(decimal128_values, i128);

fn timestamp_values(unit: TimeUnit) -> BoxedStrategy<i64> {
    let divisor = match unit {
        TimeUnit::Second => 1_000_000,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1,
        TimeUnit::Nanosecond => unreachable!("pgpq rejects nanosecond timestamps"),
    };
    let min = MIN_TIMESTAMP_US / divisor;
    let max = MAX_TIMESTAMP_US / divisor;
    // The Postgres epoch (2000-01-01) is where the encoder's rebasing arithmetic happens.
    let pg_epoch = 946_684_800_000_000 / divisor;
    prop_oneof![
        5 => min..=max,
        3 => prop::sample::select(vec![0, 1, -1, pg_epoch, pg_epoch - 1, pg_epoch + 1, min, max]),
    ]
    .boxed()
}

fn date_values() -> BoxedStrategy<i32> {
    prop_oneof![
        5 => MIN_DATE_DAYS..=MAX_DATE_DAYS,
        3 => prop::sample::select(vec![0, 1, -1, 10_957, MIN_DATE_DAYS, MAX_DATE_DAYS]),
    ]
    .boxed()
}

fn time_values(unit: TimeUnit) -> BoxedStrategy<i64> {
    let divisor = match unit {
        TimeUnit::Second => 1_000_000,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1,
        TimeUnit::Nanosecond => unreachable!("pgpq rejects nanosecond times"),
    };
    let max = MAX_TIME_US / divisor;
    prop_oneof![
        5 => 0i64..=max,
        3 => prop::sample::select(vec![0, 1, max]),
    ]
    .boxed()
}

fn duration_values(unit: TimeUnit) -> BoxedStrategy<i64> {
    // Durations are converted to microseconds, so bound them such that the conversion cannot
    // overflow i64 (the encoder returns an error rather than panicking, but an error would only
    // tell us the bound was wrong).
    let max = match unit {
        TimeUnit::Second => i64::MAX / 1_000_000,
        TimeUnit::Millisecond => i64::MAX / 1_000,
        TimeUnit::Microsecond => i64::MAX,
        TimeUnit::Nanosecond => unreachable!("pgpq rejects nanosecond durations"),
    };
    prop_oneof![
        5 => -max..=max,
        3 => prop::sample::select(vec![0, 1, -1, max, -max]),
    ]
    .boxed()
}

// ---------------------------------------------------------------------------------------------
// Array strategies
// ---------------------------------------------------------------------------------------------

fn prim_array<T>(
    values: BoxedStrategy<T::Native>,
    len: usize,
    data_type: DataType,
) -> BoxedStrategy<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: Debug + Clone + 'static,
{
    opt_vec(values, len)
        .prop_map(move |values| {
            Arc::new(PrimitiveArray::<T>::from_iter(values).with_data_type(data_type.clone()))
                as ArrayRef
        })
        .boxed()
}

/// An array of exactly `len` rows of type `data_type`.
///
/// The returned array's `data_type()` is always equal to `data_type`, which is what lets several
/// batches of the same column share one schema.
fn array_of(data_type: DataType, len: usize) -> BoxedStrategy<ArrayRef> {
    let dt = data_type.clone();
    match data_type {
        DataType::Boolean => opt_vec(any::<bool>().boxed(), len)
            .prop_map(|values| Arc::new(BooleanArray::from(values)) as ArrayRef)
            .boxed(),
        DataType::Int8 => prim_array::<Int8Type>(i8s(), len, dt),
        DataType::Int16 => prim_array::<Int16Type>(i16s(), len, dt),
        DataType::Int32 => prim_array::<Int32Type>(i32s(), len, dt),
        DataType::Int64 => prim_array::<Int64Type>(i64s(), len, dt),
        DataType::UInt8 => prim_array::<UInt8Type>(u8s(), len, dt),
        DataType::UInt16 => prim_array::<UInt16Type>(u16s(), len, dt),
        DataType::UInt32 => prim_array::<UInt32Type>(u32s(), len, dt),
        DataType::UInt64 => prim_array::<UInt64Type>(u64s(), len, dt),
        DataType::Float16 => prim_array::<Float16Type>(
            f32s()
                .prop_map(<Float16Type as ArrowPrimitiveType>::Native::from_f32)
                .boxed(),
            len,
            dt,
        ),
        DataType::Float32 => prim_array::<Float32Type>(f32s(), len, dt),
        DataType::Float64 => prim_array::<Float64Type>(f64s(), len, dt),
        DataType::Decimal32(precision, _) => {
            prim_array::<Decimal32Type>(decimal32_values(precision), len, dt)
        }
        DataType::Decimal64(precision, _) => {
            prim_array::<Decimal64Type>(decimal64_values(precision), len, dt)
        }
        DataType::Decimal128(precision, _) => {
            prim_array::<Decimal128Type>(decimal128_values(precision), len, dt)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            prim_array::<TimestampSecondType>(timestamp_values(TimeUnit::Second), len, dt)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            prim_array::<TimestampMillisecondType>(timestamp_values(TimeUnit::Millisecond), len, dt)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            prim_array::<TimestampMicrosecondType>(timestamp_values(TimeUnit::Microsecond), len, dt)
        }
        DataType::Date32 => prim_array::<Date32Type>(date_values(), len, dt),
        DataType::Time32(TimeUnit::Second) => prim_array::<Time32SecondType>(
            time_values(TimeUnit::Second).prop_map(|v| v as i32).boxed(),
            len,
            dt,
        ),
        DataType::Time32(TimeUnit::Millisecond) => prim_array::<Time32MillisecondType>(
            time_values(TimeUnit::Millisecond)
                .prop_map(|v| v as i32)
                .boxed(),
            len,
            dt,
        ),
        DataType::Time64(TimeUnit::Microsecond) => {
            prim_array::<Time64MicrosecondType>(time_values(TimeUnit::Microsecond), len, dt)
        }
        DataType::Duration(TimeUnit::Second) => {
            prim_array::<DurationSecondType>(duration_values(TimeUnit::Second), len, dt)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            prim_array::<DurationMillisecondType>(duration_values(TimeUnit::Millisecond), len, dt)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            prim_array::<DurationMicrosecondType>(duration_values(TimeUnit::Microsecond), len, dt)
        }
        DataType::Binary => opt_vec(binaries(), len)
            .prop_map(|values| Arc::new(BinaryArray::from_iter(values)) as ArrayRef)
            .boxed(),
        DataType::LargeBinary => opt_vec(binaries(), len)
            .prop_map(|values| Arc::new(LargeBinaryArray::from_iter(values)) as ArrayRef)
            .boxed(),
        DataType::Utf8 => opt_vec(strings(), len)
            .prop_map(|values| Arc::new(StringArray::from_iter(values)) as ArrayRef)
            .boxed(),
        DataType::LargeUtf8 => opt_vec(strings(), len)
            .prop_map(|values| Arc::new(LargeStringArray::from_iter(values)) as ArrayRef)
            .boxed(),
        DataType::Utf8View => opt_vec(strings(), len)
            .prop_map(|values| Arc::new(StringViewArray::from_iter(values)) as ArrayRef)
            .boxed(),
        DataType::List(field) => list_array(field, len, false),
        DataType::LargeList(field) => list_array(field, len, true),
        DataType::Struct(fields) => struct_array(fields, len),
        other => unreachable!("no array strategy for {other:?}"),
    }
}

fn list_array(field: Arc<Field>, len: usize, large: bool) -> BoxedStrategy<ArrayRef> {
    let inner = field.data_type().clone();
    (
        prop::collection::vec(0usize..=MAX_LIST_LEN, len..=len),
        validity(len),
    )
        .prop_flat_map(move |(lengths, valid)| {
            // A null list occupies no slots in the child array.
            let lengths: Vec<usize> = lengths
                .iter()
                .zip(&valid)
                .map(|(len, valid)| if *valid { *len } else { 0 })
                .collect();
            let total: usize = lengths.iter().sum();
            (Just(lengths), Just(valid), array_of(inner.clone(), total))
        })
        .prop_map(move |(lengths, valid, values)| {
            let mut offsets = Vec::with_capacity(lengths.len() + 1);
            let mut running = 0usize;
            offsets.push(running);
            for len in &lengths {
                running += len;
                offsets.push(running);
            }
            let nulls = null_buffer(&valid);
            if large {
                let offsets: Vec<i64> = offsets.iter().map(|o| *o as i64).collect();
                Arc::new(LargeListArray::new(
                    field.clone(),
                    OffsetBuffer::new(offsets.into()),
                    values,
                    nulls,
                )) as ArrayRef
            } else {
                let offsets: Vec<i32> = offsets.iter().map(|o| *o as i32).collect();
                Arc::new(ListArray::new(
                    field.clone(),
                    OffsetBuffer::new(offsets.into()),
                    values,
                    nulls,
                )) as ArrayRef
            }
        })
        .boxed()
}

fn struct_array(fields: Fields, len: usize) -> BoxedStrategy<ArrayRef> {
    let children = all_of(
        fields
            .iter()
            .map(|field| array_of(field.data_type().clone(), len))
            .collect(),
    );
    (children, validity(len))
        .prop_map(move |(children, valid)| {
            Arc::new(StructArray::new(
                fields.clone(),
                children,
                null_buffer(&valid),
            )) as ArrayRef
        })
        .boxed()
}

// ---------------------------------------------------------------------------------------------
// DataType strategies
// ---------------------------------------------------------------------------------------------

fn integer_type() -> BoxedStrategy<DataType> {
    prop::sample::select(vec![
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ])
    .boxed()
}

fn float_type() -> BoxedStrategy<DataType> {
    prop::sample::select(vec![
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
    ])
    .boxed()
}

/// The scales a decimal of `precision` digits is drawn from.
///
/// Arrow's own bound is `-precision..=precision` and the encoder covers all of it, negative scales
/// (meaning `value * 10^-scale`) included. The floor here is the harness limit documented on
/// [`MAX_HARNESS_NUMERIC_DIGITS`]: a negative scale widens the value to `precision + (-scale)`
/// significant digits, which still has to fit `rust_decimal::Decimal`.
fn scale_range(precision: u8) -> std::ops::RangeInclusive<i8> {
    let precision = precision as i8;
    let floor = (precision - MAX_HARNESS_NUMERIC_DIGITS as i8).max(-precision);
    floor..=precision
}

fn decimal_type() -> BoxedStrategy<DataType> {
    prop_oneof![
        (1u8..=9)
            .prop_flat_map(|p| (Just(p), scale_range(p)))
            .prop_map(|(p, s)| DataType::Decimal32(p, s)),
        (1u8..=18)
            .prop_flat_map(|p| (Just(p), scale_range(p)))
            .prop_map(|(p, s)| DataType::Decimal64(p, s)),
        (1u8..=MAX_HARNESS_NUMERIC_DIGITS)
            .prop_flat_map(|p| (Just(p), scale_range(p)))
            .prop_map(|(p, s)| DataType::Decimal128(p, s)),
    ]
    .boxed()
}

fn timezone() -> BoxedStrategy<Option<Arc<str>>> {
    prop::sample::select(vec![
        None,
        Some(Arc::from("UTC")),
        Some(Arc::from("America/New_York")),
        Some(Arc::from("+05:30")),
    ])
    .boxed()
}

fn temporal_type() -> BoxedStrategy<DataType> {
    prop_oneof![
        3 => (
            prop::sample::select(vec![
                TimeUnit::Second,
                TimeUnit::Millisecond,
                TimeUnit::Microsecond,
            ]),
            timezone(),
        )
            .prop_map(|(unit, tz)| DataType::Timestamp(unit, tz)),
        3 => prop::sample::select(vec![
            DataType::Date32,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
        ]),
        2 => prop::sample::select(vec![
            DataType::Duration(TimeUnit::Second),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Microsecond),
        ]),
    ]
    .boxed()
}

fn text_binary_type() -> BoxedStrategy<DataType> {
    prop::sample::select(vec![
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Binary,
        DataType::LargeBinary,
    ])
    .boxed()
}

/// Every scalar (non nested) type pgpq supports.
fn scalar_type() -> BoxedStrategy<DataType> {
    prop_oneof![
        integer_type(),
        float_type(),
        decimal_type(),
        temporal_type(),
        text_binary_type(),
    ]
    .boxed()
}

fn list_type() -> BoxedStrategy<DataType> {
    (scalar_type(), any::<bool>())
        .prop_map(|(inner, large)| {
            let field = Arc::new(Field::new("item", inner, true));
            if large {
                DataType::LargeList(field)
            } else {
                DataType::List(field)
            }
        })
        .boxed()
}

/// A flat struct of scalars.
///
/// NOTE: struct fields are deliberately restricted to scalars. `StructEncoderBuilder::try_new`
/// panics for a struct with a list field because `PostgresType::List` has no OID; that is a known
/// gap tracked in `tests/harness/cases.rs`, not something this suite is meant to rediscover on
/// every run.
fn struct_type() -> BoxedStrategy<DataType> {
    prop::collection::vec(scalar_type(), 1..=MAX_STRUCT_FIELDS)
        .prop_map(|inner| {
            let fields: Vec<Field> = inner
                .into_iter()
                .enumerate()
                .map(|(i, dt)| Field::new(format!("f{i}"), dt, true))
                .collect();
            DataType::Struct(Fields::from(fields))
        })
        .boxed()
}

// ---------------------------------------------------------------------------------------------
// Case strategy
// ---------------------------------------------------------------------------------------------

fn build_case(name: &'static str, columns: Vec<Vec<ArrayRef>>) -> Case {
    let num_batches = columns[0].len();
    let fields: Vec<Field> = columns
        .iter()
        .enumerate()
        .map(|(i, batches)| {
            // Declaring a column NOT NULL when it happens to contain no nulls is free extra
            // coverage of the generated DDL.
            let nullable = batches.iter().any(|array| array.null_count() > 0);
            Field::new(format!("c{i}"), batches[0].data_type().clone(), nullable)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let batches = (0..num_batches)
        .map(|batch| {
            let arrays: Vec<ArrayRef> = columns.iter().map(|c| c[batch].clone()).collect();
            RecordBatch::try_new(schema.clone(), arrays).expect("generated an invalid batch")
        })
        .collect();
    Case {
        name: name.to_string(),
        schema: (*schema).clone(),
        batches,
        encoders: None,
        expected_override: None,
    }
}

fn case_strategy(name: &'static str, column_type: BoxedStrategy<DataType>) -> BoxedStrategy<Case> {
    (
        // Row counts per batch; `0` produces an empty batch.
        prop::collection::vec(0usize..=MAX_ROWS, 1..=MAX_BATCHES),
        prop::collection::vec(column_type, 1..=MAX_COLUMNS),
    )
        .prop_flat_map(|(row_counts, column_types)| {
            all_of(
                column_types
                    .into_iter()
                    .map(|dt| {
                        all_of(
                            row_counts
                                .iter()
                                .map(|len| array_of(dt.clone(), *len))
                                .collect(),
                        )
                    })
                    .collect(),
            )
        })
        .prop_map(move |columns| build_case(name, columns))
        .boxed()
}

// ---------------------------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------------------------

/// Value equality that treats `NaN` as equal to itself.
///
/// `NaN != NaN` under `PartialEq`, but "Postgres gave back a NaN where the Arrow array held a
/// NaN" is exactly the behaviour we want to assert.
fn values_equal(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Float4(a), Value::Float4(b)) => a == b || (a.is_nan() && b.is_nan()),
        (Value::Float8(a), Value::Float8(b)) => a == b || (a.is_nan() && b.is_nan()),
        (Value::Array(a), Value::Array(b)) | (Value::Record(a), Value::Record(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| values_equal(a, b))
        }
        _ => expected == actual,
    }
}

fn rows_equal(expected: &[Vec<Value>], actual: &[Vec<Value>]) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(e, a)| e.len() == a.len() && e.iter().zip(a).all(|(e, a)| values_equal(e, a)))
}

// ---------------------------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------------------------

fn config(family: &str) -> Config {
    let defaults = Config::default();
    let cases = if std::env::var("PROPTEST_CASES").is_ok() {
        defaults.cases
    } else {
        DEFAULT_CASES
    };
    // A `&'static str` is required here; the leak is bounded by the number of families.
    let path: &'static str =
        Box::leak(format!("tests/proptest-regressions/{family}.txt").into_boxed_str());
    Config {
        cases,
        max_shrink_iters: 256,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(path))),
        ..defaults
    }
}

fn run_family(
    db: &RefCell<TestDb>,
    name: &'static str,
    column_type: BoxedStrategy<DataType>,
    rows_checked: &Cell<usize>,
) -> Result<(), String> {
    let strategy = case_strategy(name, column_type);
    let mut runner = TestRunner::new(config(name));
    runner
        .run(&strategy, |case| {
            let expected = case.expected();
            let actual = db
                .borrow_mut()
                .roundtrip(&case.name, &case.schema, &case.batches, None)
                .map_err(|err| TestCaseError::fail(format!("roundtrip failed: {err}")))?;
            rows_checked.set(rows_checked.get() + actual.len());
            prop_assert!(
                rows_equal(&expected, &actual),
                "schema {:?}\nexpected {expected:#?}\nactual   {actual:#?}",
                case.schema,
            );
            Ok(())
        })
        .map_err(|err| format!("{err}"))
}

/// Roundtrip randomly generated Arrow data through Postgres, one family of types at a time.
///
/// Every family runs even if an earlier one failed so a single run reports every broken type
/// group rather than only the first.
#[test]
fn proptest_roundtrip() {
    let db = RefCell::new(TestDb::start().expect("failed to start embedded postgres"));

    let families: Vec<(&'static str, BoxedStrategy<DataType>)> = vec![
        ("integers", integer_type()),
        ("floats", float_type()),
        ("decimals", decimal_type()),
        ("temporal", temporal_type()),
        ("text_and_binary", text_binary_type()),
        ("lists", list_type()),
        ("structs", struct_type()),
        (
            "mixed",
            prop_oneof![4 => scalar_type(), 2 => list_type(), 1 => struct_type()].boxed(),
        ),
    ];

    let rows_checked = Cell::new(0);
    let mut failures = Vec::new();
    let family_count = families.len();
    for (name, column_type) in families {
        if let Err(err) = run_family(&db, name, column_type, &rows_checked) {
            failures.push(format!("--- family `{name}` ---\n{err}"));
        }
    }

    println!(
        "checked {} rows across {family_count} families",
        rows_checked.get()
    );
    // A strategy that silently degenerated to empty batches would otherwise pass vacuously.
    assert!(rows_checked.get() > 0, "no rows were roundtripped");
    assert!(
        failures.is_empty(),
        "{} property families failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
