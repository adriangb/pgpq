//! A normalized value representation shared by both sides of a roundtrip test.
//!
//! [`Value`] is produced two ways:
//!
//! * from an Arrow array, via [`arrow_value`] / [`arrow_column`] — this is the *expected* side and
//!   encodes the Arrow -> Postgres type mapping that `pgpq`'s encoders promise;
//! * from a Postgres row, via the [`FromSql`] implementation — this is the *actual* side and is a
//!   fully typed decode of the binary wire format that Postgres hands back.
//!
//! Comparing the two is a much stronger check than diffing the CSV text export.

use std::error::Error;

use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal32Array, Decimal64Array,
    Decimal128Array, DurationMicrosecondArray, DurationMillisecondArray, DurationSecondArray,
    FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray,
    ListArray, StringArray, StringViewArray, StructArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, Days, NaiveDate, NaiveDateTime, NaiveTime};
use postgres_types::{FromSql, Kind, Type};
use rust_decimal::Decimal;

/// A Postgres `interval`, which has no `FromSql` implementation in `postgres-types`.
///
/// The binary layout is `int64 microseconds`, `int32 days`, `int32 months`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    pub microseconds: i64,
    pub days: i32,
    pub months: i32,
}

impl Interval {
    pub fn from_microseconds(microseconds: i64) -> Self {
        Interval {
            microseconds,
            days: 0,
            months: 0,
        }
    }
}

/// A decoded Postgres value, or the equivalent value derived from an Arrow array.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Numeric(Decimal),
    Text(String),
    Bytea(Vec<u8>),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(NaiveDateTime),
    Interval(Interval),
    /// A Postgres array. Elements may be `Null`.
    Array(Vec<Value>),
    /// A Postgres composite (user defined) type, i.e. an Arrow struct.
    Record(Vec<Value>),
}

impl Value {
    pub fn numeric_from_i128(value: i128, scale: i8) -> Value {
        let decimal = if scale >= 0 {
            Decimal::try_from_i128_with_scale(value, scale as u32)
                .unwrap_or_else(|e| panic!("{value} with scale {scale} is not representable: {e}"))
        } else {
            let scaled = value * 10_i128.pow((-scale) as u32);
            Decimal::try_from_i128_with_scale(scaled, 0)
                .unwrap_or_else(|e| panic!("{value} with scale {scale} is not representable: {e}"))
        };
        // Postgres does not preserve the exact trailing-zero representation of a NUMERIC across
        // every operation, so compare on the normalized (mathematical) value.
        Value::Numeric(decimal.normalize())
    }
}

// -------------------------------------------------------------------------------------------
// Decoding: Postgres -> Value
// -------------------------------------------------------------------------------------------

fn take<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a [u8], Box<dyn Error + Sync + Send>> {
    if buf.len() < n {
        return Err("unexpected end of Postgres binary payload".into());
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(head)
}

fn read_i32(buf: &mut &[u8]) -> Result<i32, Box<dyn Error + Sync + Send>> {
    Ok(i32::from_be_bytes(take(buf, 4)?.try_into().unwrap()))
}

/// Decode the binary representation of a Postgres array (`array_recv` in Postgres' source).
fn decode_array(member: &Type, mut raw: &[u8]) -> Result<Vec<Value>, Box<dyn Error + Sync + Send>> {
    let ndim = read_i32(&mut raw)?;
    let _has_nulls = read_i32(&mut raw)?;
    let _element_oid = read_i32(&mut raw)?;
    if ndim == 0 {
        return Ok(Vec::new());
    }
    if ndim != 1 {
        return Err(format!("expected a one dimensional array, got {ndim} dimensions").into());
    }
    let len = read_i32(&mut raw)?;
    let _lower_bound = read_i32(&mut raw)?;
    let mut values = Vec::with_capacity(len.max(0) as usize);
    for _ in 0..len {
        values.push(decode_field(member, &mut raw)?);
    }
    Ok(values)
}

/// Decode the binary representation of a Postgres composite (`record_recv` in Postgres' source).
///
/// The per-field OID on the wire is deliberately ignored in favour of the OID that Postgres
/// reports for the resolved composite type: `pgpq` writes a placeholder OID for nested composite
/// fields, and the point of this decoder is to check *values*, not the OID bookkeeping.
fn decode_record(
    fields: &[postgres_types::Field],
    mut raw: &[u8],
) -> Result<Vec<Value>, Box<dyn Error + Sync + Send>> {
    let n = read_i32(&mut raw)?;
    if n as usize != fields.len() {
        return Err(format!(
            "composite has {n} fields on the wire but {} in its type definition",
            fields.len()
        )
        .into());
    }
    let mut values = Vec::with_capacity(fields.len());
    for field in fields {
        let _oid = read_i32(&mut raw)?;
        values.push(decode_field(field.type_(), &mut raw)?);
    }
    Ok(values)
}

/// Read a length prefixed field (`-1` meaning NULL) and decode it.
fn decode_field(ty: &Type, raw: &mut &[u8]) -> Result<Value, Box<dyn Error + Sync + Send>> {
    let len = read_i32(raw)?;
    if len < 0 {
        return Ok(Value::Null);
    }
    let body = take(raw, len as usize)?;
    Value::from_sql(ty, body)
}

impl<'a> FromSql<'a> for Value {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Value, Box<dyn Error + Sync + Send>> {
        match ty.kind() {
            Kind::Array(member) => return Ok(Value::Array(decode_array(member, raw)?)),
            Kind::Composite(fields) => return Ok(Value::Record(decode_record(fields, raw)?)),
            _ => {}
        }
        let value = match ty.oid() {
            16 => Value::Bool(bool::from_sql(ty, raw)?), // bool
            17 => Value::Bytea(<Vec<u8>>::from_sql(ty, raw)?), // bytea
            21 => Value::Int2(i16::from_sql(ty, raw)?),  // int2
            23 => Value::Int4(i32::from_sql(ty, raw)?),  // int4
            20 => Value::Int8(i64::from_sql(ty, raw)?),  // int8
            700 => Value::Float4(f32::from_sql(ty, raw)?), // float4
            701 => Value::Float8(f64::from_sql(ty, raw)?), // float8
            1700 => Value::Numeric(Decimal::from_sql(ty, raw)?.normalize()), // numeric
            1082 => Value::Date(NaiveDate::from_sql(ty, raw)?), // date
            1083 => Value::Time(NaiveTime::from_sql(ty, raw)?), // time
            1114 => Value::Timestamp(NaiveDateTime::from_sql(ty, raw)?), // timestamp
            1186 => {
                // interval; postgres-types has no FromSql implementation for it
                let mut raw = raw;
                let microseconds = i64::from_be_bytes(take(&mut raw, 8)?.try_into().unwrap());
                let days = read_i32(&mut raw)?;
                let months = read_i32(&mut raw)?;
                Value::Interval(Interval {
                    microseconds,
                    days,
                    months,
                })
            }
            18 | 25 | 1042 | 1043 => Value::Text(String::from_utf8(raw.to_vec())?), // char/text/bpchar/varchar
            114 => Value::Text(String::from_utf8(raw.to_vec())?),                   // json
            3802 => {
                // jsonb: a one byte version header followed by the (normalized) json text
                let (version, body) = raw.split_first().ok_or("empty jsonb payload")?;
                if *version != 1 {
                    return Err(format!("unsupported jsonb version {version}").into());
                }
                Value::Text(String::from_utf8(body.to_vec())?)
            }
            other => return Err(format!("no decoder for Postgres type oid {other}").into()),
        };
        Ok(value)
    }

    fn from_sql_null(_ty: &Type) -> Result<Value, Box<dyn Error + Sync + Send>> {
        Ok(Value::Null)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

// -------------------------------------------------------------------------------------------
// Expectation: Arrow -> Value
// -------------------------------------------------------------------------------------------

fn downcast<T: 'static>(array: &dyn Array) -> &T {
    array
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("could not downcast {:?}", array.data_type()))
}

fn timestamp(value: i64, unit: &TimeUnit) -> Value {
    let micros = match unit {
        TimeUnit::Second => value * 1_000_000,
        TimeUnit::Millisecond => value * 1_000,
        TimeUnit::Microsecond => value,
        TimeUnit::Nanosecond => value / 1_000,
    };
    let seconds = micros.div_euclid(1_000_000);
    let nanos = micros.rem_euclid(1_000_000) * 1_000;
    Value::Timestamp(
        DateTime::from_timestamp(seconds, nanos as u32)
            .expect("timestamp out of range")
            .naive_utc(),
    )
}

fn time_from_micros(micros: i64) -> Value {
    let seconds = micros.div_euclid(1_000_000);
    let nanos = micros.rem_euclid(1_000_000) * 1_000;
    Value::Time(
        NaiveTime::from_num_seconds_from_midnight_opt(seconds as u32, nanos as u32)
            .expect("time out of range"),
    )
}

fn date_from_days(days: i32) -> Value {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = if days >= 0 {
        epoch.checked_add_days(Days::new(days as u64))
    } else {
        epoch.checked_sub_days(Days::new(days.unsigned_abs() as u64))
    };
    Value::Date(date.expect("date out of range"))
}

/// The value `pgpq` is expected to land in Postgres for row `index` of `array`.
///
/// This mirrors the Arrow -> Postgres type mapping implemented by `pgpq::encoders`, e.g. Arrow
/// `UInt32` becomes an `INT8` and Arrow `Duration` becomes an `INTERVAL`.
pub fn arrow_value(array: &dyn Array, index: usize) -> Value {
    if array.is_null(index) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Boolean => Value::Bool(downcast::<BooleanArray>(array).value(index)),
        DataType::Int8 => Value::Int2(downcast::<Int8Array>(array).value(index) as i16),
        DataType::Int16 => Value::Int2(downcast::<Int16Array>(array).value(index)),
        DataType::Int32 => Value::Int4(downcast::<Int32Array>(array).value(index)),
        DataType::Int64 => Value::Int8(downcast::<Int64Array>(array).value(index)),
        DataType::UInt8 => Value::Int2(downcast::<UInt8Array>(array).value(index) as i16),
        DataType::UInt16 => Value::Int4(downcast::<UInt16Array>(array).value(index) as i32),
        DataType::UInt32 => Value::Int8(downcast::<UInt32Array>(array).value(index) as i64),
        DataType::UInt64 => {
            Value::numeric_from_i128(downcast::<UInt64Array>(array).value(index) as i128, 0)
        }
        DataType::Float16 => Value::Float4(downcast::<Float16Array>(array).value(index).to_f32()),
        DataType::Float32 => Value::Float4(downcast::<Float32Array>(array).value(index)),
        DataType::Float64 => Value::Float8(downcast::<Float64Array>(array).value(index)),
        DataType::Decimal32(_, scale) => Value::numeric_from_i128(
            downcast::<Decimal32Array>(array).value(index) as i128,
            *scale,
        ),
        DataType::Decimal64(_, scale) => Value::numeric_from_i128(
            downcast::<Decimal64Array>(array).value(index) as i128,
            *scale,
        ),
        DataType::Decimal128(_, scale) => {
            Value::numeric_from_i128(downcast::<Decimal128Array>(array).value(index), *scale)
        }
        DataType::Timestamp(unit, _) => {
            // A time zone annotation does not change the underlying (UTC) instant, and pgpq maps
            // every Arrow timestamp to a Postgres TIMESTAMP (without time zone).
            let value = match unit {
                TimeUnit::Second => downcast::<TimestampSecondArray>(array).value(index),
                TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(array).value(index),
                TimeUnit::Microsecond => downcast::<TimestampMicrosecondArray>(array).value(index),
                TimeUnit::Nanosecond => downcast::<TimestampNanosecondArray>(array).value(index),
            };
            timestamp(value, unit)
        }
        DataType::Date32 => date_from_days(downcast::<Date32Array>(array).value(index)),
        DataType::Time32(TimeUnit::Second) => {
            time_from_micros(downcast::<Time32SecondArray>(array).value(index) as i64 * 1_000_000)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            time_from_micros(downcast::<Time32MillisecondArray>(array).value(index) as i64 * 1_000)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            time_from_micros(downcast::<Time64MicrosecondArray>(array).value(index))
        }
        DataType::Duration(TimeUnit::Second) => Value::Interval(Interval::from_microseconds(
            downcast::<DurationSecondArray>(array).value(index) * 1_000_000,
        )),
        DataType::Duration(TimeUnit::Millisecond) => Value::Interval(Interval::from_microseconds(
            downcast::<DurationMillisecondArray>(array).value(index) * 1_000,
        )),
        DataType::Duration(TimeUnit::Microsecond) => Value::Interval(Interval::from_microseconds(
            downcast::<DurationMicrosecondArray>(array).value(index),
        )),
        DataType::Binary => Value::Bytea(downcast::<BinaryArray>(array).value(index).to_vec()),
        DataType::LargeBinary => {
            Value::Bytea(downcast::<LargeBinaryArray>(array).value(index).to_vec())
        }
        DataType::BinaryView => {
            Value::Bytea(downcast::<BinaryViewArray>(array).value(index).to_vec())
        }
        DataType::FixedSizeBinary(_) => Value::Bytea(
            downcast::<FixedSizeBinaryArray>(array)
                .value(index)
                .to_vec(),
        ),
        DataType::Utf8 => Value::Text(downcast::<StringArray>(array).value(index).to_string()),
        DataType::LargeUtf8 => {
            Value::Text(downcast::<LargeStringArray>(array).value(index).to_string())
        }
        DataType::Utf8View => {
            Value::Text(downcast::<StringViewArray>(array).value(index).to_string())
        }
        DataType::List(_) => {
            let values = downcast::<ListArray>(array).value(index);
            Value::Array(arrow_column(values.as_ref()))
        }
        DataType::LargeList(_) => {
            let values = downcast::<LargeListArray>(array).value(index);
            Value::Array(arrow_column(values.as_ref()))
        }
        DataType::FixedSizeList(_, _) => {
            let values = downcast::<FixedSizeListArray>(array).value(index);
            Value::Array(arrow_column(values.as_ref()))
        }
        DataType::Struct(_) => {
            let struct_array = downcast::<StructArray>(array);
            Value::Record(
                struct_array
                    .columns()
                    .iter()
                    .map(|column| arrow_value(column.as_ref(), index))
                    .collect(),
            )
        }
        other => panic!("no expectation mapping for Arrow type {other:?}"),
    }
}

/// [`arrow_value`] for every row of `array`.
pub fn arrow_column(array: &dyn Array) -> Vec<Value> {
    (0..array.len()).map(|i| arrow_value(array, i)).collect()
}

/// The expected table contents (row major) for a set of record batches.
pub fn expected_rows(batches: &[arrow_array::RecordBatch]) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for batch in batches {
        let columns: Vec<Vec<Value>> = batch
            .columns()
            .iter()
            .map(|column| arrow_column(column.as_ref()))
            .collect();
        for row in 0..batch.num_rows() {
            rows.push(columns.iter().map(|column| column[row].clone()).collect());
        }
    }
    rows
}
