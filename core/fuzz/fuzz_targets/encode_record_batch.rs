//! Structure aware fuzzing of the full `pgpq` encode path.
//!
//! Raw byte fuzzing does not apply to pgpq: its input is a typed Arrow `RecordBatch`, not an
//! untrusted byte stream. So instead of feeding the fuzzer's bytes to the encoder, the bytes are
//! used as an entropy source (`arbitrary::Unstructured`) to *build* a schema and a matching set of
//! record batches, which are then pushed through the real API in the correct order:
//!
//! ```text
//! ArrowToPostgresBinaryEncoder::try_new -> write_header -> write_batch* -> write_footer
//! ```
//!
//! The property under test is "encoding valid Arrow data in the documented call order never
//! panics". Calling those methods out of order is a plain `ErrorKind` rather than a panic, so
//! there is nothing for this target to find there; it always uses the documented order. Encoder
//! *errors* are values, not failures: they are propagated and ignored.

#![no_main]

use std::sync::Arc;

use arbitrary::{Arbitrary, Unstructured};
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Decimal128Type, Decimal32Type, Decimal64Type,
    DurationMicrosecondType, DurationMillisecondType, DurationSecondType, Float16Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampSecondType,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray,
    PrimitiveArray, RecordBatch, StringArray, StringViewArray, StructArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use pgpq::ArrowToPostgresBinaryEncoder;

const MAX_COLUMNS: usize = 4;
const MAX_BATCHES: usize = 3;
const MAX_ROWS: usize = 32;
const MAX_LIST_LEN: usize = 4;
const MAX_STRUCT_FIELDS: usize = 3;

/// The scalar Arrow types pgpq claims to support.
///
/// `Decimal` carries no precision/scale here: those are drawn separately from the fuzzer's
/// entropy (see `scalar_type`).
#[derive(Debug, Clone, Copy, Arbitrary)]
enum Scalar {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Decimal32,
    Decimal64,
    Decimal128,
    // The second and millisecond variants are drawn from the full `i64`/`i32` range on purpose:
    // their encoders convert to microseconds with a `checked_mul`, and that overflow path is not
    // reachable from the proptest suite, whose values are bounded to what Postgres accepts.
    TimestampSecond,
    TimestampMillisecond,
    TimestampMicrosecond,
    Date32,
    Time32Second,
    Time32Millisecond,
    Time64Microsecond,
    DurationSecond,
    DurationMillisecond,
    DurationMicrosecond,
    Utf8,
    LargeUtf8,
    Utf8View,
    Binary,
    LargeBinary,
}

#[derive(Debug, Clone, Arbitrary)]
enum Column {
    Scalar(Scalar),
    List(Scalar),
    LargeList(Scalar),
    /// Flat struct of scalars.
    ///
    /// KNOWN BUG (see `core/tests/proptest_roundtrip.rs`): a struct with a *list* field makes
    /// `StructEncoderBuilder::try_new` panic because `PostgresType::List` has no OID. Struct
    /// fields are restricted to scalars so the fuzzer explores everything else instead of
    /// rediscovering that one gap on every run.
    Struct(Vec<Scalar>),
}

/// Arrow's bounds for a `Decimal128`: up to 38 significant digits and a scale anywhere in
/// `-precision..=precision`. The whole space is fuzzed — the overflow and underflow that used to
/// keep it narrower (issue #79) were fixed by PR #85, and unlike the proptest suite this target
/// has no expectation side, so `rust_decimal`'s 28 digit ceiling does not apply here either.
const MAX_DECIMAL_PRECISION: u8 = 38;
/// The narrower Arrow decimals top out at what their storage width can hold.
const MAX_DECIMAL32_PRECISION: u8 = 9;
const MAX_DECIMAL64_PRECISION: u8 = 18;

/// A `DataType::DecimalN(precision, scale)` with both drawn from the fuzzer's entropy.
fn decimal_type(
    max_precision: u8,
    make: fn(u8, i8) -> DataType,
    u: &mut Unstructured<'_>,
) -> arbitrary::Result<DataType> {
    let precision = u.int_in_range(1..=max_precision)?;
    let scale = u.int_in_range(-(precision as i8)..=precision as i8)?;
    Ok(make(precision, scale))
}

fn scalar_type(scalar: Scalar, u: &mut Unstructured<'_>) -> arbitrary::Result<DataType> {
    Ok(match scalar {
        Scalar::Boolean => DataType::Boolean,
        Scalar::Int8 => DataType::Int8,
        Scalar::Int16 => DataType::Int16,
        Scalar::Int32 => DataType::Int32,
        Scalar::Int64 => DataType::Int64,
        Scalar::UInt8 => DataType::UInt8,
        Scalar::UInt16 => DataType::UInt16,
        Scalar::UInt32 => DataType::UInt32,
        Scalar::UInt64 => DataType::UInt64,
        Scalar::Float16 => DataType::Float16,
        Scalar::Float32 => DataType::Float32,
        Scalar::Float64 => DataType::Float64,
        Scalar::Decimal32 => decimal_type(MAX_DECIMAL32_PRECISION, DataType::Decimal32, u)?,
        Scalar::Decimal64 => decimal_type(MAX_DECIMAL64_PRECISION, DataType::Decimal64, u)?,
        Scalar::Decimal128 => decimal_type(MAX_DECIMAL_PRECISION, DataType::Decimal128, u)?,
        Scalar::TimestampSecond => DataType::Timestamp(TimeUnit::Second, timezone(u)?),
        Scalar::TimestampMillisecond => DataType::Timestamp(TimeUnit::Millisecond, timezone(u)?),
        Scalar::TimestampMicrosecond => DataType::Timestamp(TimeUnit::Microsecond, timezone(u)?),
        Scalar::Date32 => DataType::Date32,
        Scalar::Time32Second => DataType::Time32(TimeUnit::Second),
        Scalar::Time32Millisecond => DataType::Time32(TimeUnit::Millisecond),
        Scalar::Time64Microsecond => DataType::Time64(TimeUnit::Microsecond),
        Scalar::DurationSecond => DataType::Duration(TimeUnit::Second),
        Scalar::DurationMillisecond => DataType::Duration(TimeUnit::Millisecond),
        Scalar::DurationMicrosecond => DataType::Duration(TimeUnit::Microsecond),
        Scalar::Utf8 => DataType::Utf8,
        Scalar::LargeUtf8 => DataType::LargeUtf8,
        Scalar::Utf8View => DataType::Utf8View,
        Scalar::Binary => DataType::Binary,
        Scalar::LargeBinary => DataType::LargeBinary,
    })
}

fn timezone(u: &mut Unstructured<'_>) -> arbitrary::Result<Option<Arc<str>>> {
    Ok(if u.arbitrary()? {
        Some(Arc::from("UTC"))
    } else {
        None
    })
}

fn nulls(len: usize, u: &mut Unstructured<'_>) -> arbitrary::Result<Option<NullBuffer>> {
    let mut valid = Vec::with_capacity(len);
    for _ in 0..len {
        valid.push(u.arbitrary::<bool>()?);
    }
    Ok(if valid.iter().all(|v| *v) {
        None
    } else {
        Some(NullBuffer::from(valid))
    })
}

fn opt_vec<'a, T: Arbitrary<'a>>(
    len: usize,
    u: &mut Unstructured<'a>,
) -> arbitrary::Result<Vec<Option<T>>> {
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        out.push(if u.arbitrary::<bool>()? {
            Some(u.arbitrary::<T>()?)
        } else {
            None
        });
    }
    Ok(out)
}

fn scalar_array(
    data_type: &DataType,
    len: usize,
    u: &mut Unstructured<'_>,
) -> arbitrary::Result<ArrayRef> {
    let array: ArrayRef = match data_type {
        DataType::Boolean => Arc::new(BooleanArray::from(opt_vec::<bool>(len, u)?)),
        DataType::Int8 => Arc::new(Int8Array::from(opt_vec::<i8>(len, u)?)),
        DataType::Int16 => Arc::new(Int16Array::from(opt_vec::<i16>(len, u)?)),
        DataType::Int32 => Arc::new(Int32Array::from(opt_vec::<i32>(len, u)?)),
        DataType::Int64 => Arc::new(Int64Array::from(opt_vec::<i64>(len, u)?)),
        DataType::UInt8 => Arc::new(UInt8Array::from(opt_vec::<u8>(len, u)?)),
        DataType::UInt16 => Arc::new(UInt16Array::from(opt_vec::<u16>(len, u)?)),
        DataType::UInt32 => Arc::new(UInt32Array::from(opt_vec::<u32>(len, u)?)),
        DataType::UInt64 => Arc::new(UInt64Array::from(opt_vec::<u64>(len, u)?)),
        DataType::Float16 => {
            // `half::f16` is not `Arbitrary`, so draw an `f32` and narrow it. Every f32 (NaN and
            // the infinities included) has an f16 image, so nothing is lost as coverage.
            let values: Vec<Option<_>> = opt_vec::<f32>(len, u)?
                .into_iter()
                .map(|v| v.map(<Float16Type as ArrowPrimitiveType>::Native::from_f32))
                .collect();
            Arc::new(PrimitiveArray::<Float16Type>::from_iter(values))
        }
        DataType::Float32 => Arc::new(Float32Array::from(opt_vec::<f32>(len, u)?)),
        DataType::Float64 => Arc::new(Float64Array::from(opt_vec::<f64>(len, u)?)),
        DataType::Decimal32(precision, _) => {
            let bound = 10i32.pow(*precision as u32);
            let values: Vec<Option<i32>> = opt_vec::<i32>(len, u)?
                .into_iter()
                .map(|v| v.map(|v| v % bound))
                .collect();
            Arc::new(
                PrimitiveArray::<Decimal32Type>::from_iter(values)
                    .with_data_type(data_type.clone()),
            )
        }
        DataType::Decimal64(precision, _) => {
            let bound = 10i64.pow(*precision as u32);
            let values: Vec<Option<i64>> = opt_vec::<i64>(len, u)?
                .into_iter()
                .map(|v| v.map(|v| v % bound))
                .collect();
            Arc::new(
                PrimitiveArray::<Decimal64Type>::from_iter(values)
                    .with_data_type(data_type.clone()),
            )
        }
        DataType::Decimal128(precision, _) => {
            // Keep every value inside the declared precision; Arrow itself rejects wider ones.
            let bound = 10i128.pow(*precision as u32);
            let values: Vec<Option<i128>> = opt_vec::<i128>(len, u)?
                .into_iter()
                .map(|v| v.map(|v| v % bound))
                .collect();
            Arc::new(
                PrimitiveArray::<Decimal128Type>::from_iter(values)
                    .with_data_type(data_type.clone()),
            )
        }
        DataType::Timestamp(TimeUnit::Second, _) => Arc::new(
            PrimitiveArray::<TimestampSecondType>::from_iter(opt_vec::<i64>(len, u)?)
                .with_data_type(data_type.clone()),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Arc::new(
            PrimitiveArray::<TimestampMillisecondType>::from_iter(opt_vec::<i64>(len, u)?)
                .with_data_type(data_type.clone()),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Arc::new(
            PrimitiveArray::<TimestampMicrosecondType>::from_iter(opt_vec::<i64>(len, u)?)
                .with_data_type(data_type.clone()),
        ),
        DataType::Date32 => Arc::new(PrimitiveArray::<Date32Type>::from_iter(opt_vec::<i32>(
            len, u,
        )?)),
        DataType::Time32(TimeUnit::Second) => Arc::new(
            PrimitiveArray::<Time32SecondType>::from_iter(opt_vec::<i32>(len, u)?),
        ),
        DataType::Time32(TimeUnit::Millisecond) => Arc::new(
            PrimitiveArray::<Time32MillisecondType>::from_iter(opt_vec::<i32>(len, u)?),
        ),
        DataType::Time64(TimeUnit::Microsecond) => Arc::new(
            PrimitiveArray::<Time64MicrosecondType>::from_iter(opt_vec::<i64>(len, u)?),
        ),
        DataType::Duration(TimeUnit::Second) => Arc::new(
            PrimitiveArray::<DurationSecondType>::from_iter(opt_vec::<i64>(len, u)?),
        ),
        DataType::Duration(TimeUnit::Millisecond) => Arc::new(
            PrimitiveArray::<DurationMillisecondType>::from_iter(opt_vec::<i64>(len, u)?),
        ),
        DataType::Duration(TimeUnit::Microsecond) => {
            Arc::new(PrimitiveArray::<DurationMicrosecondType>::from_iter(
                opt_vec::<i64>(len, u)?,
            ))
        }
        DataType::Utf8 => Arc::new(StringArray::from_iter(opt_vec::<String>(len, u)?)),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from_iter(opt_vec::<String>(len, u)?)),
        DataType::Utf8View => Arc::new(StringViewArray::from_iter(opt_vec::<String>(len, u)?)),
        DataType::Binary => Arc::new(BinaryArray::from_iter(opt_vec::<Vec<u8>>(len, u)?)),
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from_iter(opt_vec::<Vec<u8>>(len, u)?)),
        other => unreachable!("unhandled scalar type {other:?}"),
    };
    Ok(array)
}

/// Build an array of exactly `len` rows whose `data_type()` equals `data_type`, so that every
/// batch of a column shares one schema.
fn array_of(
    data_type: &DataType,
    len: usize,
    u: &mut Unstructured<'_>,
) -> arbitrary::Result<ArrayRef> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) => {
            let large = matches!(data_type, DataType::LargeList(_));
            let mut lengths = Vec::with_capacity(len);
            for _ in 0..len {
                lengths.push(u.int_in_range(0..=MAX_LIST_LEN)?);
            }
            let total: usize = lengths.iter().sum();
            let values = array_of(field.data_type(), total, u)?;
            let mut offsets = Vec::with_capacity(len + 1);
            let mut running = 0usize;
            offsets.push(running);
            for length in &lengths {
                running += length;
                offsets.push(running);
            }
            // A null list still consumes its slots here, which is legal in Arrow and a shape the
            // example based tests never produce.
            let nulls = nulls(len, u)?;
            Ok(if large {
                let offsets: Vec<i64> = offsets.iter().map(|o| *o as i64).collect();
                Arc::new(LargeListArray::new(
                    field.clone(),
                    OffsetBuffer::new(offsets.into()),
                    values,
                    nulls,
                ))
            } else {
                let offsets: Vec<i32> = offsets.iter().map(|o| *o as i32).collect();
                Arc::new(ListArray::new(
                    field.clone(),
                    OffsetBuffer::new(offsets.into()),
                    values,
                    nulls,
                ))
            })
        }
        DataType::Struct(fields) => {
            let mut children = Vec::with_capacity(fields.len());
            for field in fields.iter() {
                children.push(array_of(field.data_type(), len, u)?);
            }
            let nulls = nulls(len, u)?;
            Ok(Arc::new(StructArray::new(fields.clone(), children, nulls)))
        }
        scalar => scalar_array(scalar, len, u),
    }
}

fn column_type(column: &Column, u: &mut Unstructured<'_>) -> arbitrary::Result<DataType> {
    Ok(match column {
        Column::Scalar(scalar) => scalar_type(*scalar, u)?,
        Column::List(scalar) => {
            DataType::List(Arc::new(Field::new("item", scalar_type(*scalar, u)?, true)))
        }
        Column::LargeList(scalar) => {
            DataType::LargeList(Arc::new(Field::new("item", scalar_type(*scalar, u)?, true)))
        }
        Column::Struct(scalars) => {
            let mut fields = Vec::new();
            for (i, scalar) in scalars.iter().take(MAX_STRUCT_FIELDS).enumerate() {
                fields.push(Field::new(format!("f{i}"), scalar_type(*scalar, u)?, true));
            }
            if fields.is_empty() {
                fields.push(Field::new("f0", DataType::Int32, true));
            }
            DataType::Struct(Fields::from(fields))
        }
    })
}

fn run(u: &mut Unstructured<'_>) -> arbitrary::Result<()> {
    let num_columns = u.int_in_range(1..=MAX_COLUMNS)?;
    let mut data_types = Vec::with_capacity(num_columns);
    for _ in 0..num_columns {
        let column: Column = u.arbitrary()?;
        data_types.push(column_type(&column, u)?);
    }

    let num_batches = u.int_in_range(1..=MAX_BATCHES)?;
    let mut row_counts = Vec::with_capacity(num_batches);
    for _ in 0..num_batches {
        row_counts.push(u.int_in_range(0..=MAX_ROWS)?);
    }

    // Columns first so that a column's nullability can be derived from the data.
    let mut columns: Vec<Vec<ArrayRef>> = Vec::with_capacity(num_columns);
    for data_type in &data_types {
        let mut batches = Vec::with_capacity(num_batches);
        for rows in &row_counts {
            batches.push(array_of(data_type, *rows, u)?);
        }
        columns.push(batches);
    }

    let fields: Vec<Field> = columns
        .iter()
        .enumerate()
        .map(|(i, batches)| {
            let nullable = batches.iter().any(|array| array.null_count() > 0);
            Field::new(format!("c{i}"), batches[0].data_type().clone(), nullable)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let mut batches = Vec::with_capacity(num_batches);
    for batch in 0..num_batches {
        let arrays: Vec<ArrayRef> = columns.iter().map(|c| c[batch].clone()).collect();
        match RecordBatch::try_new(schema.clone(), arrays) {
            Ok(batch) => batches.push(batch),
            // Only reachable if the generation above is inconsistent with the schema; not the
            // property under test.
            Err(_) => return Ok(()),
        }
    }

    let mut encoder = match ArrowToPostgresBinaryEncoder::try_new(&schema) {
        Ok(encoder) => encoder,
        Err(_) => return Ok(()),
    };
    // Exercise the schema/DDL derivation too: it is part of the public surface and walks the same
    // type tree.
    let _ = encoder.schema().ddl("fuzz", false);

    let mut buf = BytesMut::new();
    if encoder.write_header(&mut buf).is_err() {
        return Ok(());
    }
    for batch in &batches {
        if encoder.write_batch(batch, &mut buf).is_err() {
            return Ok(());
        }
    }
    let _ = encoder.write_footer(&mut buf);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
