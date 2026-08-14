#![allow(clippy::redundant_closure_call)]

use arrow_array::{
    self, Array, ArrowNativeTypeOp, Decimal128Array, Decimal32Array, Decimal64Array,
    GenericStringArray, OffsetSizeTrait, StringViewArray,
};
use arrow_schema::{DataType, Field, TimeUnit};
use bytes::{BufMut, BytesMut};
use enum_dispatch::enum_dispatch;
use std::{any::type_name, convert::identity, sync::Arc};

use crate::error::ErrorKind;
use crate::pg_schema::{Column, PostgresType, TypeSize};

#[inline]
fn downcast_checked<'a, T: 'static>(arr: &'a dyn Array, field: &str) -> Result<&'a T, ErrorKind> {
    match arr.as_any().downcast_ref::<T>() {
        Some(v) => Ok(v),
        None => Err(ErrorKind::mismatched_column_type(
            field,
            type_name::<T>(),
            arr.data_type(),
        )),
    }
}

#[enum_dispatch]
pub trait Encode: std::fmt::Debug {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), ErrorKind>;
    fn byte_size_hint(&self) -> Result<usize, ErrorKind>;
}

#[enum_dispatch(Encode)]
#[derive(Debug)]
pub enum Encoder<'a> {
    Boolean(BooleanEncoder<'a>),
    UInt8(UInt8Encoder<'a>),
    UInt16(UInt16Encoder<'a>),
    UInt32(UInt32Encoder<'a>),
    UInt64(UInt64Encoder<'a>),
    Int8(Int8Encoder<'a>),
    Int16(Int16Encoder<'a>),
    Int32(Int32Encoder<'a>),
    Int64(Int64Encoder<'a>),
    Float16(Float16Encoder<'a>),
    Float32(Float32Encoder<'a>),
    Float64(Float64Encoder<'a>),
    Decimal32(Decimal32Encoder<'a>),
    Decimal64(Decimal64Encoder<'a>),
    Decimal128(Decimal128Encoder<'a>),
    TimestampMicrosecond(TimestampMicrosecondEncoder<'a>),
    TimestampMillisecond(TimestampMillisecondEncoder<'a>),
    TimestampSecond(TimestampSecondEncoder<'a>),
    Date32(Date32Encoder<'a>),
    Time32Millisecond(Time32MillisecondEncoder<'a>),
    Time32Second(Time32SecondEncoder<'a>),
    Time64Microsecond(Time64MicrosecondEncoder<'a>),
    DurationMicrosecond(DurationMicrosecondEncoder<'a>),
    DurationMillisecond(DurationMillisecondEncoder<'a>),
    DurationSecond(DurationSecondEncoder<'a>),
    Binary(BinaryEncoder<'a>),
    LargeBinary(LargeBinaryEncoder<'a>),
    String(StringEncoder<'a>),
    LargeString(LargeStringEncoder<'a>),
    StringView(StringViewEncoder<'a>),
    List(ListEncoder<'a>),
    LargeList(LargeListEncoder<'a>),
    Struct(StructEncoder<'a>),
}

#[inline]
const fn type_size_fixed(size: TypeSize) -> usize {
    match size {
        TypeSize::Fixed(v) => v,
        _ => panic!("attempted to extract a fixed size for a variable sized type"),
    }
}

macro_rules! impl_encode {
    ($struct_name:ident, $field_size:expr, $transform:expr, $write:expr) => {
        impl<'a> Encode for $struct_name<'a> {
            fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), ErrorKind> {
                if self.arr.is_null(row) {
                    buf.put_i32(-1)
                } else {
                    buf.put_i32($field_size as i32);
                    let v = self.arr.value(row);
                    let tv = $transform(v);
                    $write(buf, tv);
                }
                Ok(())
            }
            fn byte_size_hint(&self) -> Result<usize, ErrorKind> {
                let null_count = self.arr.null_count();
                let item_count = self.arr.len();
                Ok((item_count - null_count) * $field_size + item_count)
            }
        }
    };
}

macro_rules! impl_encode_fallible {
    ($struct_name:ident, $field_size:expr, $transform:expr, $write:expr) => {
        impl<'a> Encode for $struct_name<'a> {
            fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), ErrorKind> {
                if self.arr.is_null(row) {
                    buf.put_i32(-1)
                } else {
                    buf.put_i32($field_size as i32);
                    let v = self.arr.value(row);
                    let tv = $transform(&self.field, v)?;
                    $write(buf, tv);
                }
                Ok(())
            }
            fn byte_size_hint(&self) -> Result<usize, ErrorKind> {
                let null_count = self.arr.null_count();
                let item_count = self.arr.len();
                Ok((item_count - null_count) * $field_size + item_count)
            }
        }
    };
}

#[derive(Debug)]
pub struct BooleanEncoder<'a> {
    arr: &'a arrow_array::BooleanArray,
}
impl_encode!(
    BooleanEncoder,
    type_size_fixed(PostgresType::Bool.size()),
    u8::from,
    BufMut::put_u8
);

#[derive(Debug)]
pub struct UInt8Encoder<'a> {
    arr: &'a arrow_array::UInt8Array,
}
impl_encode!(
    UInt8Encoder,
    type_size_fixed(PostgresType::Int2.size()),
    i16::from,
    BufMut::put_i16
);

#[derive(Debug)]
pub struct UInt16Encoder<'a> {
    arr: &'a arrow_array::UInt16Array,
}
impl_encode!(
    UInt16Encoder,
    type_size_fixed(PostgresType::Int4.size()),
    i32::from,
    BufMut::put_i32
);

#[derive(Debug)]
pub struct UInt32Encoder<'a> {
    arr: &'a arrow_array::UInt32Array,
}
impl_encode!(
    UInt32Encoder,
    type_size_fixed(PostgresType::Int8.size()),
    i64::from,
    BufMut::put_i64
);

#[derive(Debug)]
pub struct Int8Encoder<'a> {
    arr: &'a arrow_array::Int8Array,
}
impl_encode!(
    Int8Encoder,
    type_size_fixed(PostgresType::Int2.size()),
    i16::from,
    BufMut::put_i16
);

#[derive(Debug)]
pub struct Int16Encoder<'a> {
    arr: &'a arrow_array::Int16Array,
}
impl_encode!(
    Int16Encoder,
    type_size_fixed(PostgresType::Int2.size()),
    identity,
    BufMut::put_i16
);

#[derive(Debug)]
pub struct Int32Encoder<'a> {
    arr: &'a arrow_array::Int32Array,
}
impl_encode!(
    Int32Encoder,
    type_size_fixed(PostgresType::Int4.size()),
    identity,
    BufMut::put_i32
);

#[derive(Debug)]
pub struct Int64Encoder<'a> {
    arr: &'a arrow_array::Int64Array,
}
impl_encode!(
    Int64Encoder,
    type_size_fixed(PostgresType::Int8.size()),
    identity,
    BufMut::put_i64
);

#[derive(Debug)]
pub struct Float16Encoder<'a> {
    arr: &'a arrow_array::Float16Array,
}
impl_encode!(
    Float16Encoder,
    type_size_fixed(PostgresType::Float4.size()),
    f32::from,
    BufMut::put_f32
);

#[derive(Debug)]
pub struct Float32Encoder<'a> {
    arr: &'a arrow_array::Float32Array,
}
impl_encode!(
    Float32Encoder,
    type_size_fixed(PostgresType::Float4.size()),
    identity,
    BufMut::put_f32
);

#[derive(Debug)]
pub struct Float64Encoder<'a> {
    arr: &'a arrow_array::Float64Array,
}
impl_encode!(
    Float64Encoder,
    type_size_fixed(PostgresType::Float8.size()),
    identity,
    BufMut::put_f64
);

/// Number of base-10000 groups a value of `precision` decimal digits can span once its digits
/// are aligned to the base-10000 group boundaries of the Postgres NUMERIC format.
///
/// A run of `precision` decimal digits covers `ceil(precision / 4)` groups when it happens to be
/// aligned and one more when it straddles a boundary, which is the case for every scale
/// (including negative ones, whose trailing zero groups are never emitted).
#[inline]
fn numeric_group_count_hint(precision: u8) -> usize {
    (precision as usize).div_ceil(4) + 1
}

/// Upper bound on the number of base-10000 groups any decimal we can encode occupies.
///
/// The widest backing type is `i128`, whose magnitude has at most 39 decimal digits, and those
/// digits span at most `numeric_group_count_hint(39) == 11` groups.
const MAX_NUMERIC_GROUPS: usize = 11;

macro_rules! encode_decimal {
    ($name:ident, $int:ty, $uint:ty) => {
        /// Encode `value * 10^-scale` in Postgres' binary NUMERIC representation.
        ///
        /// The wire format (see `numeric_send` in `src/backend/utils/adt/numeric.c`) is
        /// `ndigits: i16`, `weight: i16`, `sign: i16`, `dscale: i16` followed by `ndigits`
        /// base-10000 digits, most significant first. The encoded number is
        /// `sum(digits[i] * 10000^(weight - i))`; `sign` is `0x0000` for positive and `0x4000`
        /// for negative; `dscale` is the *display* scale (digits shown after the decimal point).
        ///
        /// Two properties of the format drive the implementation below:
        ///
        /// * the base-10000 groups are aligned on the decimal point, not on the value's digits,
        ///   so the alignment depends only on `scale`;
        /// * `weight` is the base-10000 exponent of the *leading* digit, so it must be derived
        ///   from the position of the digits that are actually emitted (a zero group between the
        ///   decimal point and the first significant fractional digit is part of the value's
        ///   magnitude and must either be emitted or accounted for in `weight`).
        ///
        /// Digits are extracted from the least significant end so that no intermediate value ever
        /// exceeds the backing integer type, and `scale` is never used as an exponent, so
        /// arbitrarily large and negative scales are handled without overflow.
        fn $name(value: $int, scale: i8, buf: &mut BytesMut) {
            const NBASE: $uint = 10_000;

            let sign: i16 = if value < 0 { 0x4000 } else { 0 };
            // `unsigned_abs` so that `<$int>::MIN` cannot overflow the negation.
            let mut magnitude: $uint = value.unsigned_abs();

            // The least significant digit of `magnitude` has decimal exponent `-scale`. Shifting
            // the magnitude left by `shift` digits places it on a group boundary, i.e. makes its
            // exponent a multiple of four. `rem_euclid` keeps `shift` in `0..=3` for negative
            // scales too (a negative scale simply means the value has implicit trailing zeros).
            let shift = (-(scale as i32)).rem_euclid(4) as u32;
            // Base-10000 exponent of the least significant group that we are about to emit.
            let low_group_exponent = (-(scale as i32) - shift as i32) / 4;

            // Split the magnitude into base-10000 groups, least significant group first. The
            // lowest group only takes the low `4 - shift` decimal digits, shifted up by `shift`;
            // the multiplication is therefore bounded by `10^4 - 1` and cannot overflow. (The
            // previous implementation multiplied the whole fractional part by `10^shift` up
            // front, which overflowed the backing type at scale >= 9 / 17 / 37.)
            let split = (10 as $uint).pow(4 - shift);
            let mut groups = [0i16; MAX_NUMERIC_GROUPS];
            let mut n_groups = 0usize;
            if magnitude > 0 {
                groups[0] = ((magnitude % split) as u32 * 10_u32.pow(shift)) as i16;
                n_groups = 1;
                magnitude /= split;
                while magnitude > 0 {
                    groups[n_groups] = (magnitude % NBASE) as i16;
                    n_groups += 1;
                    magnitude /= NBASE;
                }
            }

            // The last group written is always non-zero, so there are never leading zero digits.
            // `weight` follows directly from where the groups sit relative to the decimal point,
            // which is what makes interior and leading zero groups come out right.
            let (weight, trailing_zero_groups) = if n_groups == 0 {
                // Zero: Postgres canonicalises this to `ndigits = 0, weight = 0, sign = +`.
                (0, 0)
            } else {
                let weight = low_group_exponent + n_groups as i32 - 1;
                // Postgres stores numerics without trailing zero groups; those live at the front
                // of `groups`, which is ordered least significant first. Dropping them does not
                // change `weight`, which describes the leading digit.
                let trailing = groups[..n_groups].iter().take_while(|d| **d == 0).count();
                (weight, trailing)
            };
            let digits = &groups[trailing_zero_groups..n_groups];

            buf.put_i32(8 + 2 * digits.len() as i32); // num of bytes
            buf.put_i16(digits.len() as i16);
            buf.put_i16(weight as i16);
            buf.put_i16(sign);
            // `dscale` is the number of digits displayed after the decimal point and cannot be
            // negative on the wire; a negative Arrow scale means the value is an integer.
            buf.put_i16(scale.max(0) as i16);
            // postgres expects the digits to be encoded from largest to smallest, so we
            // need to iterate the slice in reverse
            for d in digits.iter().rev() {
                buf.put_i16(*d);
            }
        }
    };
}

encode_decimal!(encode_decimal_32, i32, u32);
encode_decimal!(encode_decimal_64, i64, u64);
encode_decimal!(encode_decimal_128, i128, u128);

macro_rules! decimal_encoder {
    ($encoder:ident, $arr:ty, $decimal_encoder:ident) => {
        #[derive(Debug)]
        pub struct $encoder<'a> {
            arr: &'a $arr,
        }

        impl<'a> Encode for $encoder<'a> {
            fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), ErrorKind> {
                if self.arr.is_null(row) {
                    buf.put_i32(-1);
                } else {
                    $decimal_encoder(self.arr.value(row), self.arr.scale(), buf)
                }
                Ok(())
            }

            fn byte_size_hint(&self) -> Result<usize, ErrorKind> {
                // Derived from the precision alone: the scale only shifts where the base-10000
                // group boundaries fall, and Arrow allows it to be negative (the old
                // `precision - scale` underflowed for those).
                let numeric_integers = numeric_group_count_hint(self.arr.precision());
                Ok(self.arr.len() * (8 + 2 * numeric_integers))
            }
        }
    };
}

decimal_encoder!(Decimal32Encoder, Decimal32Array, encode_decimal_32);
decimal_encoder!(Decimal64Encoder, Decimal64Array, encode_decimal_64);
decimal_encoder!(Decimal128Encoder, Decimal128Array, encode_decimal_128);

#[derive(Debug)]
pub struct UInt64Encoder<'a> {
    arr: &'a arrow_array::UInt64Array,
}

impl<'a> Encode for UInt64Encoder<'a> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), ErrorKind> {
        if self.arr.is_null(row) {
            buf.put_i32(-1);
        } else {
            // since postgres does not support unsigned values, it must be promoted to the next
            // largest type. in this case, we will encoded it as a numeric (with no decimal places)
            let value = self.arr.value(row) as i128;
            encode_decimal_128(value, 0, buf);
        }
        Ok(())
    }

    fn byte_size_hint(&self) -> Result<usize, ErrorKind> {
        let numeric_integers = 5;
        Ok(self.arr.len() * (8 + 2 * numeric_integers))
    }
}

const PG_BASE_TIMESTAMP_OFFSET_US: i64 = 946_684_800_000_000; // microseconds between 2000-01-01 at midnight (Postgres's epoch) and 1970-01-01 (Arrow's / UNIX epoch)
const PG_BASE_TIMESTAMP_OFFSET_MS: i64 = 946_684_800_000; // milliseconds between 2000-01-01 at midnight (Postgres's epoch) and 1970-01-01 (Arrow's / UNIX epoch)
const PG_BASE_TIMESTAMP_OFFSET_S: i64 = 946_684_800; // seconds between 2000-01-01 at midnight (Postgres's epoch) and 1970-01-01 (Arrow's / UNIX epoch)

#[inline(always)]
fn convert_arrow_timestamp_microseconds_to_pg_timestamp(
    _field: &str,
    timestamp_us: i64,
) -> Result<i64, ErrorKind> {
    // adjust the timestamp from microseconds since 1970-01-01 to microseconds since 2000-01-01 checking for overflows and underflow
    timestamp_us
        .checked_sub(PG_BASE_TIMESTAMP_OFFSET_US)
        .ok_or_else(|| ErrorKind::Encode {
            reason: "Underflow converting microseconds since 1970-01-01 (Arrow) to microseconds since 2000-01-01 (Postgres)".to_string(),
        })
}

/// Convert from Arrow timestamps (milliseconds since 1970-01-01) to Postgres timestamps (microseconds since 2000-01-01)
#[inline(always)]
fn convert_arrow_timestamp_milliseconds_to_pg_timestamp(
    _field: &str,
    timestamp_ms: i64,
) -> Result<i64, ErrorKind> {
    let timestamp_ms = timestamp_ms.checked_sub(PG_BASE_TIMESTAMP_OFFSET_MS).ok_or_else(|| ErrorKind::Encode {
        reason: "Underflow converting milliseconds since 1970-01-01 (Arrow) to microseconds since 2000-01-01 (Postgres)".to_string(),
    })?;
    // convert to microseconds, checking for overflows
    timestamp_ms
        .checked_mul(1_000)
        .ok_or_else(|| ErrorKind::Encode {
            reason: "Overflow converting milliseconds to microseconds".to_string(),
        })
}

#[inline(always)]
fn convert_arrow_timestamp_seconds_to_pg_timestamp(
    _field: &str,
    timestamp_s: i64,
) -> Result<i64, ErrorKind> {
    let timestamp_s = timestamp_s.checked_sub(PG_BASE_TIMESTAMP_OFFSET_S).ok_or_else(|| ErrorKind::Encode {
        reason: "Underflow converting seconds since 1970-01-01 (Arrow) to microseconds since 2000-01-01 (Postgres)".to_string(),
    })?;
    // convert to microseconds, checking for overflows
    timestamp_s
        .checked_mul(1_000_000)
        .ok_or_else(|| ErrorKind::Encode {
            reason: "Overflow converting seconds to microseconds".to_string(),
        })
}

#[derive(Debug)]
pub struct TimestampMicrosecondEncoder<'a> {
    arr: &'a arrow_array::TimestampMicrosecondArray,
    field: String,
}
impl_encode_fallible!(
    TimestampMicrosecondEncoder,
    type_size_fixed(PostgresType::Timestamp.size()),
    convert_arrow_timestamp_microseconds_to_pg_timestamp,
    BufMut::put_i64
);

#[derive(Debug)]
pub struct TimestampMillisecondEncoder<'a> {
    arr: &'a arrow_array::TimestampMillisecondArray,
    field: String,
}
impl_encode_fallible!(
    TimestampMillisecondEncoder,
    type_size_fixed(PostgresType::Timestamp.size()),
    convert_arrow_timestamp_milliseconds_to_pg_timestamp,
    BufMut::put_i64
);

#[derive(Debug)]
pub struct TimestampSecondEncoder<'a> {
    arr: &'a arrow_array::TimestampSecondArray,
    field: String,
}
impl_encode_fallible!(
    TimestampSecondEncoder,
    type_size_fixed(PostgresType::Timestamp.size()),
    convert_arrow_timestamp_seconds_to_pg_timestamp,
    BufMut::put_i64
);

const PG_BASE_DATE_OFFSET: i32 = 10_957; // Number of days between PostgreSQL's epoch (2000-01-01) and Arrow's / UNIX epoch (1970-01-01)

#[inline(always)]
fn convert_arrow_date32_to_postgres_date(_field: &str, date: i32) -> Result<i32, ErrorKind> {
    // adjust the date from days since 1970-01-01 to days since 2000-01-01 checking for overflows and underflow
    date.checked_sub(PG_BASE_DATE_OFFSET).ok_or_else(|| ErrorKind::Encode {
        reason: "Underflow converting days since 1970-01-01 (Arrow) to days since 2000-01-01 (Postgres)".to_string(),
    })
}

#[derive(Debug)]
pub struct Date32Encoder<'a> {
    arr: &'a arrow_array::Date32Array,
    field: String,
}
impl_encode_fallible!(
    Date32Encoder,
    4,
    convert_arrow_date32_to_postgres_date,
    BufMut::put_i32
);

fn convert_arrow_time_seconds_to_postgres_time(
    _field: &str,
    time_s: i32,
) -> Result<i64, ErrorKind> {
    // convert to microseconds, checking for overflows
    let time_s = time_s as i64;
    time_s
        .checked_mul(1_000_000)
        .ok_or_else(|| ErrorKind::Encode {
            reason: "Overflow converting seconds to microseconds".to_string(),
        })
}

fn convert_arrow_time_milliseconds_to_postgres_time(
    _field: &str,
    time_ms: i32,
) -> Result<i64, ErrorKind> {
    // convert to microseconds, checking for overflows
    let time_ms = time_ms as i64;
    time_ms.checked_mul(1_000).ok_or_else(|| ErrorKind::Encode {
        reason: "Overflow converting milliseconds to microseconds".to_string(),
    })
}

#[derive(Debug)]
pub struct Time32MillisecondEncoder<'a> {
    arr: &'a arrow_array::Time32MillisecondArray,
    field: String,
}
impl_encode_fallible!(
    Time32MillisecondEncoder,
    type_size_fixed(PostgresType::Time.size()),
    convert_arrow_time_milliseconds_to_postgres_time,
    BufMut::put_i64
);

#[derive(Debug)]
pub struct Time32SecondEncoder<'a> {
    arr: &'a arrow_array::Time32SecondArray,
    field: String,
}
impl_encode_fallible!(
    Time32SecondEncoder,
    type_size_fixed(PostgresType::Time.size()),
    convert_arrow_time_seconds_to_postgres_time,
    BufMut::put_i64
);

#[derive(Debug)]
pub struct Time64MicrosecondEncoder<'a> {
    arr: &'a arrow_array::Time64MicrosecondArray,
}
impl_encode!(Time64MicrosecondEncoder, 8, identity, BufMut::put_i64);

#[derive(Debug)]
pub struct DurationMicrosecondEncoder<'a> {
    arr: &'a arrow_array::DurationMicrosecondArray,
}
impl_encode!(DurationMicrosecondEncoder, 16, identity, write_duration);

const NUM_US_PER_MS: i64 = 1_000;
const NUM_US_PER_S: i64 = 1_000_000;

#[inline]
fn write_duration(buf: &mut BytesMut, duration_us: i64) {
    buf.put_i64(duration_us);
    buf.put_i32(0); // days
    buf.put_i32(0); // months
}

#[derive(Debug)]
pub struct DurationMillisecondEncoder<'a> {
    arr: &'a arrow_array::DurationMillisecondArray,
    field: String,
}
impl_encode_fallible!(
    DurationMillisecondEncoder,
    type_size_fixed(PostgresType::Interval.size()),
    |_: &str, v: i64| v.mul_checked(NUM_US_PER_MS).map_err(|_| {
        ErrorKind::Encode {
            reason: "Overflow encoding millisecond duration as microseconds".to_string(),
        }
    }),
    write_duration
);

#[derive(Debug)]
pub struct DurationSecondEncoder<'a> {
    arr: &'a arrow_array::DurationSecondArray,
    field: String,
}

impl_encode_fallible!(
    DurationSecondEncoder,
    type_size_fixed(PostgresType::Interval.size()),
    |_: &str, v: i64| v.mul_checked(NUM_US_PER_S).map_err(|_| {
        ErrorKind::Encode {
            reason: "Overflow encoding seconds duration as microseconds".to_string(),
        }
    }),
    write_duration
);

#[derive(Debug)]
pub struct GenericBinaryEncoder<'a, T: OffsetSizeTrait> {
    arr: &'a arrow_array::GenericBinaryArray<T>,
    field: String,
}

impl<T: OffsetSizeTrait> Encode for GenericBinaryEncoder<'_, T> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), ErrorKind> {
        if self.arr.is_null(row) {
            buf.put_i32(-1);
        } else {
            let v = self.arr.value(row);
            let len = v.len();
            match i32::try_from(len) {
                Ok(l) => buf.put_i32(l),
                Err(_) => return Err(ErrorKind::field_too_large(&self.field, len)),
            }
            buf.extend_from_slice(v);
        }
        Ok(())
    }
    fn byte_size_hint(&self) -> Result<usize, ErrorKind> {
        let mut total = 0;
        for row in 0..self.arr.len() {
            total += self.arr.value(row).len();
        }
        Ok(total)
    }
}

type BinaryEncoder<'a> = GenericBinaryEncoder<'a, i32>;
type LargeBinaryEncoder<'a> = GenericBinaryEncoder<'a, i64>;

pub trait GenericStrArray: arrow_array::Array {
    fn value(&self, row: usize) -> &str;
}

#[derive(Debug)]
pub struct GenericStrEncoder<'a, T: GenericStrArray> {
    arr: &'a T,
    field: String,
    output: StringOutputType,
}

impl<'a, T: GenericStrArray> Encode for GenericStrEncoder<'a, T> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), ErrorKind> {
        if self.arr.is_null(row) {
            buf.put_i32(-1);
        } else {
            let v = self.arr.value(row).as_bytes();
            let mut len = v.len();
            if matches!(self.output, StringOutputType::Jsonb) {
                len += 1;
            }
            match i32::try_from(len) {
                Ok(l) => buf.put_i32(l),
                Err(_) => return Err(ErrorKind::field_too_large(&self.field, len)),
            }
            if matches!(self.output, StringOutputType::Jsonb) {
                buf.put_u8(1) // JSONB format version
            }
            buf.extend_from_slice(v);
        }
        Ok(())
    }

    fn byte_size_hint(&self) -> Result<usize, ErrorKind> {
        let mut total = 0;
        for row in 0..self.arr.len() {
            total += self.arr.value(row).len();
        }
        if matches!(self.output, StringOutputType::Jsonb) {
            total += self.arr.len() // For JSONB format version
        }
        Ok(total)
    }
}

impl<T: OffsetSizeTrait> GenericStrArray for GenericStringArray<T> {
    fn value(&self, row: usize) -> &str {
        self.value(row)
    }
}

impl GenericStrArray for StringViewArray {
    fn value(&self, row: usize) -> &str {
        self.value(row)
    }
}

type StringEncoder<'a> = GenericStrEncoder<'a, GenericStringArray<i32>>;
type LargeStringEncoder<'a> = GenericStrEncoder<'a, GenericStringArray<i64>>;
type StringViewEncoder<'a> = GenericStrEncoder<'a, StringViewArray>;

#[derive(Debug)]
pub struct GenericListEncoder<'a, T: OffsetSizeTrait> {
    arr: &'a arrow_array::GenericListArray<T>,
    field: String,
    inner_encoder_builder: Arc<EncoderBuilder>,
}

impl<T: OffsetSizeTrait> Encode for GenericListEncoder<'_, T> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), ErrorKind> {
        if self.arr.is_null(row) {
            buf.put_i32(-1);
        } else {
            let val = self.arr.value(row);
            let inner_encoder = self.inner_encoder_builder.try_new(&val)?;

            let base_idx = buf.len();
            buf.put_i32(0); // the total number of bytes this element takes up, insert later
            buf.put_i32(1); // num dimensions, we only support 1
            buf.put_i32((val.null_count() != 0) as i32); // nulls flag, true if any item is null
            let inner_tp_oid = self.inner_encoder_builder.schema().data_type.oid().unwrap();
            buf.put_i32(inner_tp_oid as i32);
            // put the dimension length
            buf.put_i32(val.len() as i32);
            // put the dimension lower bound, always 1
            buf.put_i32(1);

            for inner_row in 0..val.len() {
                inner_encoder.encode(inner_row, buf)?;
            }

            let total_len = buf.len() - base_idx - 4; // end - start - 4 bytes for the size i32 itself

            match i32::try_from(total_len) {
                Ok(v) => buf[base_idx..base_idx + 4].copy_from_slice(&v.to_be_bytes()),
                Err(_) => return Err(ErrorKind::field_too_large(&self.field, total_len)),
            };
        }
        Ok(())
    }
    fn byte_size_hint(&self) -> Result<usize, ErrorKind> {
        let mut total = 0;
        for row in 0..self.arr.len() {
            if !self.arr.is_null(row) {
                let val = self.arr.value(row);
                let inner_encoder = self.inner_encoder_builder.try_new(&val)?;
                let size = inner_encoder.byte_size_hint()?;
                total += size;
            }
        }
        Ok(total)
    }
}

type ListEncoder<'a> = GenericListEncoder<'a, i32>;
type LargeListEncoder<'a> = GenericListEncoder<'a, i64>;

#[derive(Debug)]
pub struct StructEncoder<'a> {
    arr: &'a arrow_array::StructArray,
    field: String,
    field_encoders: Vec<Encoder<'a>>,
    field_oids: Vec<u32>,
}

impl<'a> Encode for StructEncoder<'a> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), ErrorKind> {
        if self.arr.is_null(row) {
            buf.put_i32(-1);
        } else {
            let base_idx = buf.len();
            buf.put_i32(0); // Placeholder for the total size

            // Put the number of fields
            buf.put_i32(self.field_encoders.len() as i32);

            for (encoder, oid) in self.field_encoders.iter().zip(&self.field_oids) {
                buf.put_u32(*oid);
                encoder.encode(row, buf)?;
            }

            let total_len = buf.len() - base_idx - 4;
            match i32::try_from(total_len) {
                Ok(v) => buf[base_idx..base_idx + 4].copy_from_slice(&v.to_be_bytes()),
                Err(_) => return Err(ErrorKind::field_too_large(&self.field, total_len)),
            };
        }
        Ok(())
    }

    fn byte_size_hint(&self) -> Result<usize, ErrorKind> {
        let mut total = 4 + 4; // 4 bytes for the length, 4 bytes for the number of fields
        for encoder in &self.field_encoders {
            total += encoder.byte_size_hint()?;
        }
        Ok(total)
    }
}

#[enum_dispatch]
pub trait BuildEncoder: std::fmt::Debug + PartialEq {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind>;
    fn schema(&self) -> Column;
    fn field(&self) -> Arc<Field>;
}

macro_rules! impl_encoder_builder_stateless {
    ($struct_name:ident, $enum_name:expr, $encoder_name:ident, $pg_data_type:expr, $check_data_type:expr) => {
        impl $struct_name {
            pub fn new(field: Arc<Field>) -> Result<Self, ErrorKind> {
                if !$check_data_type(field.data_type()) {
                    return Err(ErrorKind::FieldTypeNotSupported {
                        encoder: stringify!($struct_name).to_string(),
                        tp: field.data_type().clone(),
                        field: field.name().clone(),
                    });
                }
                Ok(Self { field })
            }
        }
        impl BuildEncoder for $struct_name {
            fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
                Ok($enum_name($encoder_name {
                    arr: downcast_checked(arr, &self.field.name())?,
                }))
            }
            fn schema(&self) -> Column {
                Column {
                    name: self.field.name().clone(),
                    data_type: $pg_data_type.clone(),
                    nullable: self.field.is_nullable(),
                }
            }
            fn field(&self) -> Arc<Field> {
                self.field.clone()
            }
        }
    };
}

macro_rules! impl_encoder_builder_stateless_with_field {
    ($struct_name:ident, $enum_name:expr, $encoder_name:ident, $pg_data_type:expr, $check_data_type:expr) => {
        impl $struct_name {
            pub fn new(field: Arc<Field>) -> Result<Self, ErrorKind> {
                if !$check_data_type(field.data_type()) {
                    return Err(ErrorKind::FieldTypeNotSupported {
                        encoder: stringify!($struct_name).to_string(),
                        tp: field.data_type().clone(),
                        field: field.name().clone(),
                    });
                }
                Ok(Self { field })
            }
        }
        impl BuildEncoder for $struct_name {
            fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
                let field = self.field.name();
                let arr = downcast_checked(arr, &field)?;
                Ok($enum_name($encoder_name {
                    arr,
                    field: field.to_string(),
                }))
            }
            fn schema(&self) -> Column {
                Column {
                    name: self.field.name().clone(),
                    data_type: $pg_data_type.clone(),
                    nullable: self.field.is_nullable(),
                }
            }
            fn field(&self) -> Arc<Field> {
                self.field.clone()
            }
        }
    };
}

macro_rules! impl_encoder_builder_stateless_with_variable_output {
    ($struct_name:ident, $enum_name:expr, $encoder_name:ident, $pg_data_type:expr, $allowed_pg_data_types:expr, $check_data_type:expr) => {
        impl $struct_name {
            pub fn new(field: Arc<Field>) -> Result<Self, ErrorKind> {
                if !$check_data_type(field.data_type()) {
                    return Err(ErrorKind::FieldTypeNotSupported {
                        encoder: stringify!($struct_name).to_string(),
                        tp: field.data_type().clone(),
                        field: field.name().clone(),
                    });
                }
                Ok(Self {
                    field,
                    output: $pg_data_type,
                })
            }
            pub fn new_with_output(
                field: Arc<Field>,
                output: PostgresType,
            ) -> Result<Self, ErrorKind> {
                if !$allowed_pg_data_types.contains(&output) {
                    return Err(ErrorKind::unsupported_encoding(
                        &field.name(),
                        &output,
                        &[PostgresType::Char, PostgresType::Int2],
                    ));
                }
                Ok(Self { field, output })
            }
        }
        impl BuildEncoder for $struct_name {
            fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
                let field = self.field.name();
                let arr = downcast_checked(arr, &field)?;
                Ok($enum_name($encoder_name { arr }))
            }
            fn schema(&self) -> Column {
                Column {
                    name: self.field.name().clone(),
                    data_type: self.output.clone(),
                    nullable: self.field.is_nullable(),
                }
            }
            fn field(&self) -> Arc<Field> {
                self.field.clone()
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq)]
pub struct BooleanEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    BooleanEncoderBuilder,
    Encoder::Boolean,
    BooleanEncoder,
    PostgresType::Bool,
    |dt: &DataType| matches!(dt, DataType::Boolean)
);

#[derive(Debug, Clone, PartialEq)]
pub struct UInt8EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    UInt8EncoderBuilder,
    Encoder::UInt8,
    UInt8Encoder,
    PostgresType::Int2,
    |dt: &DataType| matches!(dt, DataType::UInt8)
);

#[derive(Debug, Clone, PartialEq)]
pub struct UInt16EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    UInt16EncoderBuilder,
    Encoder::UInt16,
    UInt16Encoder,
    PostgresType::Int4,
    |dt: &DataType| matches!(dt, DataType::UInt16)
);

#[derive(Debug, Clone, PartialEq)]
pub struct UInt32EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    UInt32EncoderBuilder,
    Encoder::UInt32,
    UInt32Encoder,
    PostgresType::Int8,
    |dt: &DataType| matches!(dt, DataType::UInt32)
);

#[derive(Debug, Clone, PartialEq)]
pub struct UInt64EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    UInt64EncoderBuilder,
    Encoder::UInt64,
    UInt64Encoder,
    PostgresType::Numeric,
    |dt: &DataType| matches!(dt, DataType::UInt64)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Int8EncoderBuilder {
    field: Arc<Field>,
    output: PostgresType,
}
impl_encoder_builder_stateless_with_variable_output!(
    Int8EncoderBuilder,
    Encoder::Int8,
    Int8Encoder,
    PostgresType::Int2,
    [PostgresType::Char, PostgresType::Int2],
    |dt: &DataType| matches!(dt, DataType::Int8)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Int16EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Int16EncoderBuilder,
    Encoder::Int16,
    Int16Encoder,
    PostgresType::Int2,
    |dt: &DataType| matches!(dt, DataType::Int16)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Int32EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Int32EncoderBuilder,
    Encoder::Int32,
    Int32Encoder,
    PostgresType::Int4,
    |dt: &DataType| matches!(dt, DataType::Int32)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Int64EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Int64EncoderBuilder,
    Encoder::Int64,
    Int64Encoder,
    PostgresType::Int8,
    |dt: &DataType| matches!(dt, DataType::Int64)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Float16EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Float16EncoderBuilder,
    Encoder::Float16,
    Float16Encoder,
    PostgresType::Float4,
    |dt: &DataType| matches!(dt, DataType::Float16)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Float32EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Float32EncoderBuilder,
    Encoder::Float32,
    Float32Encoder,
    PostgresType::Float4,
    |dt: &DataType| matches!(dt, DataType::Float32)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Float64EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Float64EncoderBuilder,
    Encoder::Float64,
    Float64Encoder,
    PostgresType::Float8,
    |dt: &DataType| matches!(dt, DataType::Float64)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Decimal32EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Decimal32EncoderBuilder,
    Encoder::Decimal32,
    Decimal32Encoder,
    PostgresType::Numeric,
    |dt: &DataType| matches!(dt, DataType::Decimal32(_, _))
);

#[derive(Debug, Clone, PartialEq)]
pub struct Decimal64EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Decimal64EncoderBuilder,
    Encoder::Decimal64,
    Decimal64Encoder,
    PostgresType::Numeric,
    |dt: &DataType| matches!(dt, DataType::Decimal64(_, _))
);

#[derive(Debug, Clone, PartialEq)]
pub struct Decimal128EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Decimal128EncoderBuilder,
    Encoder::Decimal128,
    Decimal128Encoder,
    PostgresType::Numeric,
    |dt: &DataType| matches!(dt, DataType::Decimal128(_, _))
);

#[derive(Debug, Clone, PartialEq)]
pub struct TimestampMicrosecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    TimestampMicrosecondEncoderBuilder,
    Encoder::TimestampMicrosecond,
    TimestampMicrosecondEncoder,
    PostgresType::Timestamp,
    |dt: &DataType| matches!(dt, DataType::Timestamp(TimeUnit::Microsecond, _))
);

#[derive(Debug, Clone, PartialEq)]
pub struct TimestampMillisecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    TimestampMillisecondEncoderBuilder,
    Encoder::TimestampMillisecond,
    TimestampMillisecondEncoder,
    PostgresType::Timestamp,
    |dt: &DataType| matches!(dt, DataType::Timestamp(TimeUnit::Millisecond, _))
);

#[derive(Debug, Clone, PartialEq)]
pub struct TimestampSecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    TimestampSecondEncoderBuilder,
    Encoder::TimestampSecond,
    TimestampSecondEncoder,
    PostgresType::Timestamp,
    |dt: &DataType| matches!(dt, DataType::Timestamp(TimeUnit::Second, _))
);

#[derive(Debug, Clone, PartialEq)]
pub struct Date32EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    Date32EncoderBuilder,
    Encoder::Date32,
    Date32Encoder,
    PostgresType::Date,
    |dt: &DataType| matches!(dt, DataType::Date32)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Time32MillisecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    Time32MillisecondEncoderBuilder,
    Encoder::Time32Millisecond,
    Time32MillisecondEncoder,
    PostgresType::Time,
    |dt: &DataType| matches!(dt, DataType::Time32(TimeUnit::Millisecond))
);

#[derive(Debug, Clone, PartialEq)]
pub struct Time32SecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    Time32SecondEncoderBuilder,
    Encoder::Time32Second,
    Time32SecondEncoder,
    PostgresType::Time,
    |dt: &DataType| matches!(dt, DataType::Time32(TimeUnit::Second))
);

#[derive(Debug, Clone, PartialEq)]
pub struct Time64MicrosecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Time64MicrosecondEncoderBuilder,
    Encoder::Time64Microsecond,
    Time64MicrosecondEncoder,
    PostgresType::Time,
    |dt: &DataType| matches!(dt, DataType::Time64(TimeUnit::Microsecond))
);

#[derive(Debug, Clone, PartialEq)]
pub struct DurationMicrosecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    DurationMicrosecondEncoderBuilder,
    Encoder::DurationMicrosecond,
    DurationMicrosecondEncoder,
    PostgresType::Interval,
    |dt: &DataType| matches!(dt, DataType::Duration(TimeUnit::Microsecond))
);

#[derive(Debug, Clone, PartialEq)]
pub struct DurationMillisecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    DurationMillisecondEncoderBuilder,
    Encoder::DurationMillisecond,
    DurationMillisecondEncoder,
    PostgresType::Interval,
    |dt: &DataType| matches!(dt, DataType::Duration(TimeUnit::Millisecond))
);

#[derive(Debug, Clone, PartialEq)]
pub struct DurationSecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    DurationSecondEncoderBuilder,
    Encoder::DurationSecond,
    DurationSecondEncoder,
    PostgresType::Interval,
    |dt: &DataType| matches!(dt, DataType::Duration(TimeUnit::Second))
);

#[derive(Debug, Clone, PartialEq)]
enum StringOutputType {
    Text,
    Json,
    Jsonb,
}

impl StringOutputType {
    pub fn from_postgres_type(tp: PostgresType, field: &Field) -> Result<Self, ErrorKind> {
        match tp {
            PostgresType::Text => Ok(StringOutputType::Text),
            PostgresType::Json => Ok(StringOutputType::Json),
            PostgresType::Jsonb => Ok(StringOutputType::Jsonb),
            other => Err(ErrorKind::EncodingNotSupported {
                field: field.name().clone(),
                tp: other,
                allowed: vec![PostgresType::Text, PostgresType::Json, PostgresType::Jsonb],
            }),
        }
    }
    pub fn postgres_datatype(&self) -> PostgresType {
        match self {
            StringOutputType::Text => PostgresType::Text,
            StringOutputType::Json => PostgresType::Json,
            StringOutputType::Jsonb => PostgresType::Jsonb,
        }
    }
}

macro_rules! impl_encoder_builder_with_variable_output {
    ($struct_name:ident, $enum_name:expr, $encoder_name:ident, $check_data_type:expr) => {
        impl $struct_name {
            pub fn new(field: Arc<Field>) -> Result<Self, ErrorKind> {
                if !$check_data_type(field.data_type()) {
                    return Err(ErrorKind::FieldTypeNotSupported {
                        encoder: stringify!($struct_name).to_string(),
                        tp: field.data_type().clone(),
                        field: field.name().clone(),
                    });
                }
                Ok(Self {
                    field,
                    output: StringOutputType::Text,
                })
            }
            pub fn new_with_output(
                field: Arc<Field>,
                output: PostgresType,
            ) -> Result<Self, ErrorKind> {
                let output = StringOutputType::from_postgres_type(output, &field)?;
                Ok(Self { field, output })
            }
        }
        impl BuildEncoder for $struct_name {
            fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
                let field = self.field.name();
                let arr = downcast_checked(arr, &field)?;
                Ok($enum_name($encoder_name {
                    field: self.field.name().clone(),
                    arr,
                    output: self.output.clone(),
                }))
            }
            fn schema(&self) -> Column {
                Column {
                    name: self.field.name().clone(),
                    data_type: self.output.postgres_datatype().clone(),
                    nullable: self.field.is_nullable(),
                }
            }
            fn field(&self) -> Arc<Field> {
                self.field.clone()
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq)]
pub struct StringEncoderBuilder {
    field: Arc<Field>,
    output: StringOutputType,
}
impl_encoder_builder_with_variable_output!(
    StringEncoderBuilder,
    Encoder::String,
    StringEncoder,
    |dt: &DataType| matches!(dt, DataType::Utf8)
);

#[derive(Debug, Clone, PartialEq)]
pub struct LargeStringEncoderBuilder {
    field: Arc<Field>,
    output: StringOutputType,
}

impl_encoder_builder_with_variable_output!(
    LargeStringEncoderBuilder,
    Encoder::LargeString,
    LargeStringEncoder,
    |dt: &DataType| matches!(dt, DataType::LargeUtf8)
);

#[derive(Debug, Clone, PartialEq)]
pub struct StringViewEncoderBuilder {
    field: Arc<Field>,
    output: StringOutputType,
}

impl_encoder_builder_with_variable_output!(
    StringViewEncoderBuilder,
    Encoder::StringView,
    StringViewEncoder,
    |dt: &DataType| matches!(dt, DataType::Utf8View)
);

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    BinaryEncoderBuilder,
    Encoder::Binary,
    BinaryEncoder,
    PostgresType::Bytea,
    |dt: &DataType| matches!(dt, DataType::Binary)
);

#[derive(Debug, Clone, PartialEq)]
pub struct LargeBinaryEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    LargeBinaryEncoderBuilder,
    Encoder::LargeBinary,
    LargeBinaryEncoder,
    PostgresType::Bytea,
    |dt: &DataType| matches!(dt, DataType::LargeBinary)
);

macro_rules! impl_list_encoder_builder {
    ($struct_name:ident, $enum_name:expr, $encoder_name:ident) => {
        impl $struct_name {
            pub fn new(field: Arc<Field>) -> Result<Self, ErrorKind> {
                match &field.data_type() {
                    DataType::List(inner) => {
                        let inner_encoder_builder = EncoderBuilder::try_new(inner.clone())?;
                        Ok(Self {
                            field,
                            inner_encoder_builder: Arc::new(inner_encoder_builder),
                        })
                    }
                    _ => Err(ErrorKind::type_unsupported(
                        &field.name(),
                        field.data_type(),
                        format!("{:?} is not a list type", field.data_type()).as_str(),
                    )),
                }
            }
            pub fn new_with_inner(
                field: Arc<Field>,
                inner_encoder_builder: EncoderBuilder,
            ) -> Result<Self, ErrorKind> {
                Ok(Self {
                    field,
                    inner_encoder_builder: Arc::new(inner_encoder_builder),
                })
            }
        }
        impl BuildEncoder for $struct_name {
            fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
                let field = self.field.name().clone();
                let arr = downcast_checked(arr, &field)?;
                let inner_encoder_builder = self.inner_encoder_builder.clone();
                Ok($enum_name($encoder_name {
                    arr,
                    field,
                    inner_encoder_builder,
                }))
            }
            fn schema(&self) -> Column {
                Column {
                    name: self.field.name().clone(),
                    data_type: PostgresType::List(Box::new(
                        self.inner_encoder_builder.schema().clone(),
                    )),
                    nullable: self.field.is_nullable(),
                }
            }
            fn field(&self) -> Arc<Field> {
                self.field.clone()
            }
        }

        impl $struct_name {
            pub fn inner_encoder_builder(&self) -> EncoderBuilder {
                (*self.inner_encoder_builder).clone()
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListEncoderBuilder {
    field: Arc<Field>,
    inner_encoder_builder: Arc<EncoderBuilder>,
}

impl_list_encoder_builder!(ListEncoderBuilder, Encoder::List, ListEncoder);

#[derive(Debug, Clone, PartialEq)]
pub struct LargeListEncoderBuilder {
    field: Arc<Field>,
    inner_encoder_builder: Arc<EncoderBuilder>,
}
impl_list_encoder_builder!(
    LargeListEncoderBuilder,
    Encoder::LargeList,
    LargeListEncoder
);

#[derive(Debug, Clone, PartialEq)]
pub struct StructEncoderBuilder {
    field: Arc<Field>,
    field_encoder_builders: Vec<EncoderBuilder>,
}

impl StructEncoderBuilder {
    pub fn new(field: Arc<Field>) -> Result<Self, ErrorKind> {
        if let DataType::Struct(fields) = field.data_type() {
            let field_encoder_builders = fields
                .iter()
                .map(|f| EncoderBuilder::try_new(f.clone()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Self {
                field,
                field_encoder_builders,
            })
        } else {
            Err(ErrorKind::FieldTypeNotSupported {
                encoder: "StructEncoder".to_string(),
                tp: field.data_type().clone(),
                field: field.name().clone(),
            })
        }
    }
}

impl BuildEncoder for StructEncoderBuilder {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
        let arr: &'a arrow_array::StructArray = downcast_checked(arr, self.field.name())?;

        // Build encoders for each field at build time and collect OIDs
        let mut field_encoders = Vec::new();
        let mut field_oids = Vec::new();

        for (field, encoder_builder) in arr.columns().iter().zip(&self.field_encoder_builders) {
            let encoder = encoder_builder.try_new(field)?;
            let oid = encoder_builder.schema().data_type.oid().unwrap();
            field_encoders.push(encoder);
            field_oids.push(oid);
        }

        Ok(Encoder::Struct(StructEncoder {
            arr,
            field: self.field.name().to_string(),
            field_encoders,
            field_oids,
        }))
    }

    fn schema(&self) -> Column {
        Column {
            name: self.field.name().clone(),
            data_type: PostgresType::UserDefined {
                fields: self
                    .field_encoder_builders
                    .iter()
                    .map(|builder| Box::new(builder.schema()))
                    .collect(),
            },
            nullable: self.field.is_nullable(),
        }
    }

    fn field(&self) -> Arc<Field> {
        self.field.clone()
    }
}

impl StructEncoderBuilder {
    pub fn inner_encoder_builder(&self) -> Vec<EncoderBuilder> {
        // Return a clone of the inner encoder builders
        self.field_encoder_builders.to_vec()
    }
}

#[enum_dispatch(BuildEncoder)]
#[derive(Debug, Clone, PartialEq)]
pub enum EncoderBuilder {
    Boolean(BooleanEncoderBuilder),
    UInt8(UInt8EncoderBuilder),
    UInt16(UInt16EncoderBuilder),
    UInt32(UInt32EncoderBuilder),
    UInt64(UInt64EncoderBuilder),
    Int8(Int8EncoderBuilder),
    Int16(Int16EncoderBuilder),
    Int32(Int32EncoderBuilder),
    Int64(Int64EncoderBuilder),
    Float16(Float16EncoderBuilder),
    Float32(Float32EncoderBuilder),
    Float64(Float64EncoderBuilder),
    Decimal32(Decimal32EncoderBuilder),
    Decimal64(Decimal64EncoderBuilder),
    Decimal128(Decimal128EncoderBuilder),
    TimestampMicrosecond(TimestampMicrosecondEncoderBuilder),
    TimestampMillisecond(TimestampMillisecondEncoderBuilder),
    TimestampSecond(TimestampSecondEncoderBuilder),
    Date32(Date32EncoderBuilder),
    Time32Millisecond(Time32MillisecondEncoderBuilder),
    Time32Second(Time32SecondEncoderBuilder),
    Time64Microsecond(Time64MicrosecondEncoderBuilder),
    DurationMicrosecond(DurationMicrosecondEncoderBuilder),
    DurationMillisecond(DurationMillisecondEncoderBuilder),
    DurationSecond(DurationSecondEncoderBuilder),
    String(StringEncoderBuilder),
    LargeString(LargeStringEncoderBuilder),
    StringView(StringViewEncoderBuilder),
    Binary(BinaryEncoderBuilder),
    LargeBinary(LargeBinaryEncoderBuilder),
    List(ListEncoderBuilder),
    LargeList(LargeListEncoderBuilder),
    Struct(StructEncoderBuilder),
}

impl EncoderBuilder {
    pub fn try_new(field: Arc<Field>) -> Result<Self, ErrorKind> {
        let data_type = field.data_type();
        let res = match data_type {
            DataType::Boolean => Self::Boolean(BooleanEncoderBuilder { field }),
            DataType::UInt8 => Self::UInt8(UInt8EncoderBuilder { field }),
            DataType::UInt16 => Self::UInt16(UInt16EncoderBuilder { field }),
            DataType::UInt32 => Self::UInt32(UInt32EncoderBuilder { field }),
            DataType::UInt64 => Self::UInt64(UInt64EncoderBuilder { field }),
            // Note that rust-postgres encodes int8 to CHAR by default
            DataType::Int8 => Self::Int8(Int8EncoderBuilder {
                field,
                output: PostgresType::Int2,
            }),
            DataType::Int16 => Self::Int16(Int16EncoderBuilder { field }),
            DataType::Int32 => Self::Int32(Int32EncoderBuilder { field }),
            DataType::Int64 => Self::Int64(Int64EncoderBuilder { field }),
            DataType::Float16 => Self::Float16(Float16EncoderBuilder { field }),
            DataType::Float32 => Self::Float32(Float32EncoderBuilder { field }),
            DataType::Float64 => Self::Float64(Float64EncoderBuilder { field }),
            DataType::Decimal32(_, _) => Self::Decimal32(Decimal32EncoderBuilder { field }),
            DataType::Decimal64(_, _) => Self::Decimal64(Decimal64EncoderBuilder { field }),
            DataType::Decimal128(_, _) => Self::Decimal128(Decimal128EncoderBuilder { field }),
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Nanosecond => {
                    return Err(ErrorKind::type_unsupported(
                        field.name(),
                        data_type,
                        "Postgres does not support ns precision; convert to us",
                    ))
                }
                TimeUnit::Microsecond => {
                    Self::TimestampMicrosecond(TimestampMicrosecondEncoderBuilder { field })
                }
                TimeUnit::Millisecond => {
                    Self::TimestampMillisecond(TimestampMillisecondEncoderBuilder { field })
                }
                TimeUnit::Second => Self::TimestampSecond(TimestampSecondEncoderBuilder { field }),
            },
            DataType::Date32 => Self::Date32(Date32EncoderBuilder { field }),
            DataType::Time32(unit) => match unit {
                TimeUnit::Millisecond => {
                    Self::Time32Millisecond(Time32MillisecondEncoderBuilder { field })
                }
                TimeUnit::Second => Self::Time32Second(Time32SecondEncoderBuilder { field }),
                _ => unreachable!(),
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Nanosecond => {
                    return Err(ErrorKind::type_unsupported(
                        field.name(),
                        data_type,
                        "Postgres does not support ns precision; convert to us",
                    ))
                }
                TimeUnit::Microsecond => {
                    Self::Time64Microsecond(Time64MicrosecondEncoderBuilder { field })
                }
                _ => unreachable!(),
            },
            DataType::Duration(unit) => match unit {
                TimeUnit::Nanosecond => {
                    return Err(ErrorKind::type_unsupported(
                        field.name(),
                        data_type,
                        "Postgres does not support ns precision; convert to us",
                    ))
                }
                TimeUnit::Microsecond => {
                    Self::DurationMicrosecond(DurationMicrosecondEncoderBuilder { field })
                }
                TimeUnit::Millisecond => {
                    Self::DurationMillisecond(DurationMillisecondEncoderBuilder { field })
                }
                TimeUnit::Second => Self::DurationSecond(DurationSecondEncoderBuilder { field }),
            },
            DataType::Utf8 => Self::String(StringEncoderBuilder {
                field,
                output: StringOutputType::Text,
            }),
            DataType::LargeUtf8 => Self::LargeString(LargeStringEncoderBuilder {
                field,
                output: StringOutputType::Text,
            }),
            DataType::Utf8View => Self::StringView(StringViewEncoderBuilder {
                field,
                output: StringOutputType::Text,
            }),
            DataType::Binary => Self::Binary(BinaryEncoderBuilder { field }),
            DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                Self::LargeBinary(LargeBinaryEncoderBuilder { field })
            }
            DataType::List(inner) => {
                if matches!(
                    inner.data_type(),
                    DataType::List(_) | DataType::LargeList(_)
                ) {
                    return Err(ErrorKind::type_unsupported(
                        field.name(),
                        data_type,
                        "nested lists are not supported",
                    ));
                }
                let inner = Self::try_new(inner.clone())?;
                Self::List(ListEncoderBuilder {
                    field,
                    inner_encoder_builder: Arc::new(inner),
                })
            }
            DataType::LargeList(inner) | DataType::FixedSizeList(inner, _) => {
                if matches!(
                    inner.data_type(),
                    DataType::List(_) | DataType::LargeList(_)
                ) {
                    return Err(ErrorKind::type_unsupported(
                        field.name(),
                        data_type,
                        "nested lists are not supported",
                    ));
                }
                let inner = Self::try_new(inner.clone())?;
                Self::LargeList(LargeListEncoderBuilder {
                    field,
                    inner_encoder_builder: Arc::new(inner),
                })
            }
            DataType::Struct(inner) => {
                let field_encoder_builders = inner
                    .iter()
                    .map(|f| EncoderBuilder::try_new(f.clone()))
                    .collect::<Result<Vec<_>, _>>()?;
                Self::Struct(StructEncoderBuilder {
                    field,
                    field_encoder_builders,
                })
            }
            _ => {
                return Err(ErrorKind::type_unsupported(
                    field.name(),
                    data_type,
                    "unknown type",
                ))
            }
        };
        Ok(res)
    }
}

#[cfg(test)]
mod decimal_tests {
    use super::*;
    use arrow_array::{Decimal128Array, Decimal32Array, Decimal64Array};

    /// The decoded contents of a Postgres binary NUMERIC field.
    #[derive(Debug, PartialEq, Eq)]
    struct Numeric {
        weight: i16,
        sign: i16,
        dscale: i16,
        digits: Vec<i16>,
    }

    impl Numeric {
        /// Render the numeric the way Postgres does (`get_str_from_var`), i.e. the string a
        /// `SELECT` of the loaded value returns. This is what pins the *value* down: a wrong
        /// `weight` or a dropped digit group shows up here even though the bytes are
        /// self-consistent.
        fn to_pg_string(&self) -> String {
            let digit_at = |exponent: i32| -> i16 {
                let idx = self.weight as i32 - exponent;
                if idx < 0 || idx as usize >= self.digits.len() {
                    0
                } else {
                    self.digits[idx as usize]
                }
            };
            let mut out = String::new();
            if self.sign == 0x4000 {
                out.push('-');
            }
            if self.weight < 0 {
                out.push('0');
            } else {
                for exponent in (0..=self.weight as i32).rev() {
                    let group = digit_at(exponent).to_string();
                    if exponent == self.weight as i32 {
                        out.push_str(&group);
                    } else {
                        out.push_str(&format!("{group:0>4}"));
                    }
                }
            }
            if self.dscale > 0 {
                out.push('.');
                let mut fractional = String::new();
                let mut exponent = -1;
                while fractional.len() < self.dscale as usize {
                    fractional.push_str(&format!("{:0>4}", digit_at(exponent)));
                    exponent -= 1;
                }
                fractional.truncate(self.dscale as usize);
                out.push_str(&fractional);
            }
            out
        }
    }

    fn decode(buf: &[u8]) -> Numeric {
        let read_i16 = |at: usize| i16::from_be_bytes([buf[at], buf[at + 1]]);
        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let ndigits = read_i16(4);
        assert_eq!(len, 8 + 2 * ndigits as i32, "length prefix does not match");
        assert_eq!(buf.len(), 4 + len as usize, "trailing bytes in the buffer");
        let digits: Vec<i16> = (0..ndigits as usize)
            .map(|i| read_i16(12 + 2 * i))
            .collect();
        // Postgres' canonical form: no leading and no trailing zero groups, and every group is a
        // valid base-10000 digit (`numeric_recv` rejects anything else outright).
        assert!(digits.iter().all(|d| (0..10_000).contains(d)), "{digits:?}");
        assert_ne!(digits.first(), Some(&0), "leading zero group: {digits:?}");
        assert_ne!(digits.last(), Some(&0), "trailing zero group: {digits:?}");
        Numeric {
            weight: read_i16(6),
            sign: read_i16(8),
            dscale: read_i16(10),
            digits,
        }
    }

    macro_rules! encode_fn {
        ($name:ident, $int:ty, $encode:ident) => {
            fn $name(value: $int, scale: i8) -> Numeric {
                let mut buf = BytesMut::new();
                $encode(value, scale, &mut buf);
                decode(&buf)
            }
        };
    }
    encode_fn!(enc32, i32, encode_decimal_32);
    encode_fn!(enc64, i64, encode_decimal_64);
    encode_fn!(enc128, i128, encode_decimal_128);

    /// 38 nines, the largest magnitude a `Decimal128(38, _)` can hold.
    const MAX_PRECISION_38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

    /// The headline corruption from #79: the most significant base-10000 group of the fractional
    /// part is zero, so the old encoder dropped it and shifted every following digit four decimal
    /// places towards the point, silently storing `1.0001`.
    #[test]
    fn leading_zero_fractional_group_is_not_dropped() {
        let numeric = enc64(100_000_001, 8);
        assert_eq!(
            numeric,
            Numeric {
                weight: 0,
                sign: 0,
                dscale: 8,
                digits: vec![1, 0, 1],
            }
        );
        assert_eq!(numeric.to_pg_string(), "1.00000001");
    }

    /// The other two corruptions reported in #79, both pure fractions whose leading group is
    /// zero: the old encoding was off by 10^4 and 10^8 respectively.
    #[test]
    fn pure_fractions_with_leading_zero_groups() {
        let numeric = enc64(6_538_030, 14);
        assert_eq!(
            numeric,
            Numeric {
                weight: -2,
                sign: 0,
                dscale: 14,
                digits: vec![6, 5380, 3000],
            }
        );
        assert_eq!(numeric.to_pg_string(), "0.00000006538030");

        let numeric = enc64(1, 10);
        assert_eq!(
            numeric,
            Numeric {
                weight: -3,
                sign: 0,
                dscale: 10,
                digits: vec![100],
            }
        );
        assert_eq!(numeric.to_pg_string(), "0.0000000001");
    }

    /// Every value of the shape `1 * 10^-scale` puts a run of zero groups between the decimal
    /// point and the single significant digit, which is exactly what used to be mis-weighted.
    #[test]
    fn single_digit_at_every_scale() {
        let expected = |scale: i8| -> String {
            if scale == 0 {
                "1".to_string()
            } else {
                format!("0.{}1", "0".repeat(scale as usize - 1))
            }
        };
        for scale in 0..=9i8 {
            assert_eq!(enc32(1, scale).to_pg_string(), expected(scale), "{scale}");
        }
        for scale in 0..=18i8 {
            assert_eq!(enc64(1, scale).to_pg_string(), expected(scale), "{scale}");
        }
        for scale in 0..=38i8 {
            assert_eq!(enc128(1, scale).to_pg_string(), expected(scale), "{scale}");
        }
    }

    /// Used to panic with "attempt to multiply with overflow" once the padded fractional part no
    /// longer fitted the backing integer (scale >= 9 for i32, >= 17 for i64, >= 37 for i128).
    #[test]
    fn large_scales_do_not_overflow() {
        assert_eq!(enc32(999_999_999, 9).to_pg_string(), "0.999999999");
        assert_eq!(
            enc64(99_999_999_999_999_999, 17).to_pg_string(),
            "0.99999999999999999"
        );
        assert_eq!(enc64(i64::MAX, 18).to_pg_string(), "9.223372036854775807");
        assert_eq!(
            enc128(MAX_PRECISION_38, 37).to_pg_string(),
            "9.9999999999999999999999999999999999999"
        );
        assert_eq!(
            enc128(MAX_PRECISION_38, 38).to_pg_string(),
            "0.99999999999999999999999999999999999999"
        );
        assert_eq!(
            enc128(1, 38).to_pg_string(),
            "0.00000000000000000000000000000000000001"
        );
        // The most negative value of each type has no positive counterpart; negating it used to
        // be the obvious hazard.
        assert_eq!(enc32(i32::MIN, 0).to_pg_string(), i32::MIN.to_string());
        assert_eq!(enc64(i64::MIN, 0).to_pg_string(), i64::MIN.to_string());
        assert_eq!(enc128(i128::MIN, 0).to_pg_string(), i128::MIN.to_string());
    }

    /// Arrow permits negative scales, meaning `value * 10^-scale`. They used to underflow in
    /// `byte_size_hint`; they are now encoded exactly, with `dscale = 0`.
    #[test]
    fn negative_scales() {
        let numeric = enc64(123, -2);
        assert_eq!(
            numeric,
            Numeric {
                weight: 1,
                sign: 0,
                dscale: 0,
                digits: vec![1, 2300],
            }
        );
        assert_eq!(numeric.to_pg_string(), "12300");

        // A shift by a whole number of base-10000 groups: the trailing zero groups are not
        // emitted at all, they only move `weight`.
        let numeric = enc32(1, -8);
        assert_eq!(
            numeric,
            Numeric {
                weight: 2,
                sign: 0,
                dscale: 0,
                digits: vec![1],
            }
        );
        assert_eq!(numeric.to_pg_string(), "100000000");

        assert_eq!(enc32(-7, -1).to_pg_string(), "-70");
        assert_eq!(enc32(999_999_999, -9).to_pg_string(), "999999999000000000");
        assert_eq!(
            enc128(MAX_PRECISION_38, -38).to_pg_string(),
            format!("{MAX_PRECISION_38}{}", "0".repeat(38))
        );
    }

    #[test]
    fn zero_is_canonical() {
        for scale in [-4i8, -1, 0, 1, 6, 38] {
            assert_eq!(
                enc128(0, scale),
                Numeric {
                    weight: 0,
                    sign: 0,
                    dscale: scale.max(0) as i16,
                    digits: vec![],
                },
                "scale {scale}"
            );
        }
        assert_eq!(enc32(0, 3).to_pg_string(), "0.000");
    }

    #[test]
    fn signs_and_trailing_zeros() {
        assert_eq!(enc32(-123_450_000, 6).to_pg_string(), "-123.450000");
        assert_eq!(enc32(123_450_000, 6).to_pg_string(), "123.450000");
        assert_eq!(enc32(123_000_000, 6).to_pg_string(), "123.000000");
        assert_eq!(enc32(1_000, 6).to_pg_string(), "0.001000");
        // `UInt64Encoder` encodes through the 128 bit path with scale 0.
        assert_eq!(
            enc128(u64::MAX as i128, 0).to_pg_string(),
            u64::MAX.to_string()
        );
    }

    /// Arrow renders a zero with a negative scale by appending the zeros literally (`0` at scale
    /// `-9` becomes `"0000000000"`); Postgres renders the same value as `"0"`. Normalise the
    /// integer part so the two can be compared.
    fn strip_leading_zeros(rendered: &str) -> String {
        let (sign, rest) = match rendered.strip_prefix('-') {
            Some(rest) => ("-", rest),
            None => ("", rendered),
        };
        let (integer, fractional) = match rest.split_once('.') {
            Some((integer, fractional)) => (integer, Some(fractional)),
            None => (rest, None),
        };
        let trimmed = integer.trim_start_matches('0');
        let integer = if trimmed.is_empty() { "0" } else { trimmed };
        let sign = if integer == "0" && fractional.is_none_or(|f| f.bytes().all(|b| b == b'0')) {
            ""
        } else {
            sign
        };
        match fractional {
            Some(fractional) => format!("{sign}{integer}.{fractional}"),
            None => format!("{sign}{integer}"),
        }
    }

    /// Cross-check a sweep of values against Arrow's own rendering of the same decimal, for every
    /// scale (negative ones included) that Arrow accepts for the type.
    macro_rules! cross_check_with_arrow {
        ($name:ident, $arr:ty, $int:ty, $encode:ident, $precision:expr) => {
            #[test]
            fn $name() {
                // The extremes are the largest magnitude the precision allows; Arrow's own
                // renderer misbehaves past that, and Postgres would reject the column anyway.
                let extreme = (10 as $int).pow($precision as u32 - 1) * 9
                    + (10 as $int).pow($precision as u32 - 1)
                    - 1;
                let values: Vec<$int> = vec![
                    0,
                    1,
                    -1,
                    7,
                    10,
                    9_999,
                    10_000,
                    10_001,
                    100_000_001,
                    123_456_789,
                    extreme,
                    -extreme,
                ];
                for scale in -$precision..=$precision {
                    let arr = <$arr>::from(values.clone())
                        .with_precision_and_scale($precision as u8, scale)
                        .unwrap();
                    for row in 0..arr.len() {
                        let mut buf = BytesMut::new();
                        $encode(arr.value(row), scale, &mut buf);
                        assert_eq!(
                            decode(&buf).to_pg_string(),
                            strip_leading_zeros(&arr.value_as_string(row)),
                            "value {} at scale {scale}",
                            arr.value(row)
                        );
                    }
                }
            }
        };
    }
    cross_check_with_arrow!(
        cross_check_decimal32,
        Decimal32Array,
        i32,
        encode_decimal_32,
        9i8
    );
    cross_check_with_arrow!(
        cross_check_decimal64,
        Decimal64Array,
        i64,
        encode_decimal_64,
        18i8
    );
    cross_check_with_arrow!(
        cross_check_decimal128,
        Decimal128Array,
        i128,
        encode_decimal_128,
        38i8
    );

    /// `byte_size_hint` used to compute `precision - scale` in `usize`, which underflowed (and
    /// panicked) for the negative scales Arrow allows. Encoding a whole array of them has to work
    /// end to end too.
    #[test]
    fn byte_size_hint_handles_negative_scales() {
        for (precision, scale) in [
            (9u8, -9i8),
            (9, -1),
            (9, 0),
            (9, 4),
            (9, 9),
            (1, -1),
            (1, 1),
        ] {
            let max = 10_i64.pow(precision as u32) - 1;
            let arr = Decimal64Array::from(vec![max, -max, 0, 1])
                .with_precision_and_scale(precision, scale)
                .unwrap();
            let encoder = Decimal64Encoder { arr: &arr };
            assert_eq!(
                encoder.byte_size_hint().unwrap(),
                arr.len() * (8 + 2 * numeric_group_count_hint(precision)),
                "({precision}, {scale})"
            );
            let mut buf = BytesMut::new();
            for row in 0..arr.len() {
                encoder.encode(row, &mut buf).unwrap();
            }
            // Every digit group the values actually needed fits in the hinted group count.
            let mut rest = &buf[..];
            while !rest.is_empty() {
                let len = 4 + i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
                let numeric = decode(&rest[..len]);
                assert!(
                    numeric.digits.len() <= numeric_group_count_hint(precision),
                    "{numeric:?} exceeds the hinted group count for ({precision}, {scale})"
                );
                rest = &rest[len..];
            }
        }
    }
}
