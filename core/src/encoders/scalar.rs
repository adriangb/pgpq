//! Scalar encoders: everything that turns one Arrow value into one fixed-size Postgres datum, plus
//! the values Postgres can only represent as a `NUMERIC`.
//!
//! There are exactly two encoders here — [`FixedSizeEncoder`] and [`NumericEncoder`] — and one
//! builder for each. What differs between, say, a `Date32` column and a `Duration(Second)` column
//! is described by a *conversion*: a zero-sized marker type implementing [`FixedSizeConversion`]
//! or [`NumericConversion`], which names the Arrow array to read, the Postgres type to declare and
//! how to write one value. The public per-type names (`Date32Encoder`, `Date32EncoderBuilder`, …)
//! are type aliases over those two pairs; see [`super`] for the aliases and the dispatch enums.

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::types::ArrowPrimitiveType;
use arrow_array::{
    Array, ArrowNativeTypeOp, BooleanArray, Date32Array, Decimal32Array, Decimal64Array,
    Decimal128Array, DurationMicrosecondArray, DurationMillisecondArray, DurationSecondArray,
    Float16Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    PrimitiveArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, TimeUnit};
use bytes::BytesMut;

use super::numeric::{
    encode_decimal_32, encode_decimal_64, encode_decimal_128, numeric_group_count_hint,
};
use super::{BuildEncoder, Encode, Encoder, downcast_checked, put};
use crate::error::ErrorKind;
use crate::pg_schema::{Column, PostgresType, TypeSize};

/// The one thing the generic encoders need from an Arrow array beyond [`Array`] itself: reading
/// the value at a row.
///
/// [`Array`] deliberately does not expose this (the value type differs per array), and the
/// inherent `value` methods cannot be called generically, so it is spelled out here.
pub trait ValueArray: Array + 'static {
    type Value;
    fn value_at(&self, row: usize) -> Self::Value;
}

impl<T: ArrowPrimitiveType> ValueArray for PrimitiveArray<T> {
    type Value = T::Native;
    fn value_at(&self, row: usize) -> Self::Value {
        self.value(row)
    }
}

impl ValueArray for BooleanArray {
    type Value = bool;
    fn value_at(&self, row: usize) -> bool {
        self.value(row)
    }
}

#[inline]
const fn type_size_fixed(size: TypeSize) -> usize {
    match size {
        TypeSize::Fixed(v) => v,
        _ => panic!("attempted to extract a fixed size for a variable sized type"),
    }
}

// ---------------------------------------------------------------------------------------------
// Fixed size scalars
// ---------------------------------------------------------------------------------------------

/// A null field: the length prefix `-1` and nothing else.
const NULL_FIELD: [u8; 4] = (-1i32).to_be_bytes();

/// Assemble one wire field: the four byte length prefix followed by `payload`.
///
/// `N` is the size of the *whole* field, so it is always `P + 4` — the two cannot be spelled with
/// one parameter until `generic_const_exprs` is stable, so the relation is asserted at
/// monomorphisation time instead and a wrong `N` in an impl below is a compile error.
#[inline(always)]
const fn wire_field<const P: usize, const N: usize>(payload: [u8; P]) -> [u8; N] {
    const {
        assert!(
            N == P + 4,
            "a fixed size field is its payload plus a four byte length prefix"
        )
    };
    let mut out = [0u8; N];
    let len = (P as i32).to_be_bytes();
    let mut i = 0;
    while i < 4 {
        out[i] = len[i];
        i += 1;
    }
    while i < N {
        out[i] = payload[i - 4];
        i += 1;
    }
    out
}

/// How one Arrow type maps onto one fixed-width Postgres type.
///
/// The wire encoding of every such column is the same — an `i32` byte count followed by that many
/// bytes, or `-1` for a null — so a conversion only has to say *which* array it reads, *which*
/// Postgres type the column is declared as and how to serialise a single value.
///
/// `N` is the encoded size of one non-null field, length prefix included; it is what lets
/// [`FixedSizeEncoder`] hand a whole field to [`put`] in one call rather than writing the prefix
/// and the payload separately. Splitting those two writes back apart costs ~50% on the taxi
/// benchmark, which is why the conversion returns an array instead of taking the buffer.
pub trait FixedSizeConversion<const N: usize>:
    std::fmt::Debug + Clone + PartialEq + 'static
{
    /// The Arrow array this conversion reads.
    type Array: ValueArray;
    /// Name reported when a builder is handed a field it cannot encode. Kept equal to the public
    /// builder alias so the error reads the way it did before these types were generic.
    const ENCODER_NAME: &'static str;
    /// The Postgres type the column is declared as. Its [`PostgresType::size`] is `N - 4`, which
    /// [`FixedSizeEncoder::new`] asserts.
    const POSTGRES_TYPE: PostgresType;
    /// Whether a field of this Arrow type can be encoded by this conversion.
    fn accepts(data_type: &DataType) -> bool;
    /// Serialise one non-null value as a complete field — use [`wire_field`] to put the length
    /// prefix in front of the payload.
    fn field(value: <Self::Array as ValueArray>::Value) -> Result<[u8; N], ErrorKind>;
}

/// Encoder for any [`FixedSizeConversion`].
#[derive(Debug)]
pub struct FixedSizeEncoder<'a, const N: usize, C: FixedSizeConversion<N>> {
    arr: &'a C::Array,
}

impl<'a, const N: usize, C: FixedSizeConversion<N>> FixedSizeEncoder<'a, N, C> {
    pub(super) fn new(arr: &'a C::Array) -> Self {
        // Not a `const` assertion only because materialising `C::POSTGRES_TYPE` in a const block
        // would have to drop it, and `PostgresType` is not `Copy`. This runs once per column per
        // batch in debug builds, so every test exercises it.
        debug_assert_eq!(
            N,
            4 + type_size_fixed(C::POSTGRES_TYPE.size()),
            "the encoded field size and the declared Postgres type disagree"
        );
        Self { arr }
    }
}

impl<const N: usize, C: FixedSizeConversion<N>> Encode for FixedSizeEncoder<'_, N, C> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), ErrorKind> {
        if self.arr.is_null(row) {
            put(buf, NULL_FIELD);
        } else {
            put(buf, C::field(self.arr.value_at(row))?);
        }
        Ok(())
    }

    fn byte_size_hint(&self) -> Result<usize, ErrorKind> {
        let item_count = self.arr.len();
        let null_count = self.arr.null_count();
        Ok((item_count - null_count) * N + null_count * NULL_FIELD.len())
    }
}

/// Builder for any [`FixedSizeConversion`].
#[derive(Debug, Clone, PartialEq)]
pub struct FixedSizeEncoderBuilder<const N: usize, C: FixedSizeConversion<N>> {
    field: Arc<Field>,
    conversion: PhantomData<C>,
}

impl<const N: usize, C: FixedSizeConversion<N>> FixedSizeEncoderBuilder<N, C> {
    pub fn new(field: Arc<Field>) -> Result<Self, ErrorKind> {
        if !C::accepts(field.data_type()) {
            return Err(ErrorKind::FieldTypeNotSupported {
                encoder: C::ENCODER_NAME.to_string(),
                tp: field.data_type().clone(),
                field: field.name().clone(),
            });
        }
        Ok(Self::unchecked(field))
    }

    /// Build without checking the field's Arrow type, for callers that have already matched on it.
    pub(super) fn unchecked(field: Arc<Field>) -> Self {
        Self {
            field,
            conversion: PhantomData,
        }
    }
}

impl<const N: usize, C> BuildEncoder for FixedSizeEncoderBuilder<N, C>
where
    C: FixedSizeConversion<N>,
    // Satisfied by the `From` impls `enum_dispatch` derives for every `Encoder` variant; the
    // bound is what lets one generic builder produce the right variant without a per-type match.
    for<'a> FixedSizeEncoder<'a, N, C>: Into<Encoder<'a>>,
{
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
        Ok(FixedSizeEncoder::<N, C>::new(downcast_checked(arr, self.field.name())?).into())
    }

    fn schema(&self) -> Column {
        Column {
            name: self.field.name().clone(),
            data_type: C::POSTGRES_TYPE,
            nullable: self.field.is_nullable(),
        }
    }

    fn field(&self) -> Arc<Field> {
        self.field.clone()
    }
}

// ---------------------------------------------------------------------------------------------
// NUMERIC scalars
// ---------------------------------------------------------------------------------------------

/// How one Arrow type maps onto a Postgres `NUMERIC`.
///
/// `NUMERIC` is variable width and its encoding depends on the array's precision and scale rather
/// than on a single value, so — unlike [`FixedSizeConversion`] — these methods take the array.
pub trait NumericConversion: std::fmt::Debug + Clone + PartialEq + 'static {
    /// The Arrow array this conversion reads.
    type Array: Array + 'static;
    /// Name reported when a builder is handed a field it cannot encode.
    const ENCODER_NAME: &'static str;
    /// Whether a field of this Arrow type can be encoded by this conversion.
    fn accepts(data_type: &DataType) -> bool;
    /// Write one non-null value, length prefix included.
    fn write(arr: &Self::Array, row: usize, buf: &mut BytesMut);
    /// Upper bound on the number of base-10000 digit groups a value of this array can occupy.
    fn max_digit_groups(arr: &Self::Array) -> usize;
}

/// Encoder for any [`NumericConversion`].
#[derive(Debug)]
pub struct NumericEncoder<'a, C: NumericConversion> {
    arr: &'a C::Array,
}

impl<'a, C: NumericConversion> NumericEncoder<'a, C> {
    pub(super) fn new(arr: &'a C::Array) -> Self {
        Self { arr }
    }
}

impl<C: NumericConversion> Encode for NumericEncoder<'_, C> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), ErrorKind> {
        if self.arr.is_null(row) {
            put(buf, (-1i32).to_be_bytes());
        } else {
            C::write(self.arr, row, buf);
        }
        Ok(())
    }

    fn byte_size_hint(&self) -> Result<usize, ErrorKind> {
        // `encode_decimal_*` writes the four byte field length in front of the eight byte NUMERIC
        // header and the digit groups, so a whole field is `12 + 2 * groups`. Charging every row
        // for one keeps this exact-or-over: a null row only costs its length prefix.
        Ok(self.arr.len() * (12 + 2 * C::max_digit_groups(self.arr)))
    }
}

/// Builder for any [`NumericConversion`].
#[derive(Debug, Clone, PartialEq)]
pub struct NumericEncoderBuilder<C: NumericConversion> {
    field: Arc<Field>,
    conversion: PhantomData<C>,
}

impl<C: NumericConversion> NumericEncoderBuilder<C> {
    pub fn new(field: Arc<Field>) -> Result<Self, ErrorKind> {
        if !C::accepts(field.data_type()) {
            return Err(ErrorKind::FieldTypeNotSupported {
                encoder: C::ENCODER_NAME.to_string(),
                tp: field.data_type().clone(),
                field: field.name().clone(),
            });
        }
        Ok(Self::unchecked(field))
    }

    /// Build without checking the field's Arrow type, for callers that have already matched on it.
    pub(super) fn unchecked(field: Arc<Field>) -> Self {
        Self {
            field,
            conversion: PhantomData,
        }
    }
}

impl<C> BuildEncoder for NumericEncoderBuilder<C>
where
    C: NumericConversion,
    for<'a> NumericEncoder<'a, C>: Into<Encoder<'a>>,
{
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
        Ok(NumericEncoder::<C>::new(downcast_checked(arr, self.field.name())?).into())
    }

    fn schema(&self) -> Column {
        Column {
            name: self.field.name().clone(),
            data_type: PostgresType::Numeric,
            nullable: self.field.is_nullable(),
        }
    }

    fn field(&self) -> Arc<Field> {
        self.field.clone()
    }
}

// ---------------------------------------------------------------------------------------------
// `Int8`, the one scalar whose Postgres type is caller-selectable
// ---------------------------------------------------------------------------------------------

/// The Postgres types an `Int8` column may be encoded as.
///
/// Both are two bytes wide and both take the same `i16` payload, so only the declared column type
/// changes. (`rust-postgres` encodes `i8` as `CHAR` by default, hence the choice.)
const INT8_OUTPUTS: [PostgresType; 2] = [PostgresType::Char, PostgresType::Int2];

#[derive(Debug, Clone, PartialEq)]
pub struct Int8EncoderBuilder {
    field: Arc<Field>,
    output: PostgresType,
}

impl Int8EncoderBuilder {
    pub fn new(field: Arc<Field>) -> Result<Self, ErrorKind> {
        if !Int8Conversion::accepts(field.data_type()) {
            return Err(ErrorKind::FieldTypeNotSupported {
                encoder: "Int8EncoderBuilder".to_string(),
                tp: field.data_type().clone(),
                field: field.name().clone(),
            });
        }
        Ok(Self::unchecked(field))
    }

    pub fn new_with_output(field: Arc<Field>, output: PostgresType) -> Result<Self, ErrorKind> {
        if !INT8_OUTPUTS.contains(&output) {
            return Err(ErrorKind::unsupported_encoding(
                field.name(),
                &output,
                &INT8_OUTPUTS,
            ));
        }
        Ok(Self { field, output })
    }

    pub(super) fn unchecked(field: Arc<Field>) -> Self {
        Self {
            field,
            output: PostgresType::Int2,
        }
    }
}

impl BuildEncoder for Int8EncoderBuilder {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
        Ok(Encoder::Int8(FixedSizeEncoder::<6, Int8Conversion>::new(
            downcast_checked(arr, self.field.name())?,
        )))
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

// ---------------------------------------------------------------------------------------------
// The conversion table
//
// The `N` on every impl is the encoded size of one field: the Postgres type's width plus the four
// byte length prefix. `FixedSizeEncoder::new` refuses to compile if the two disagree.
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BooleanConversion;
impl FixedSizeConversion<5> for BooleanConversion {
    type Array = BooleanArray;
    const ENCODER_NAME: &'static str = "BooleanEncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Bool;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Boolean)
    }
    fn field(value: bool) -> Result<[u8; 5], ErrorKind> {
        Ok(wire_field([u8::from(value)]))
    }
}

/// Postgres has no unsigned integers, so every unsigned Arrow type is widened to the next
/// signed type that can hold it. `UInt64` has no such type and becomes a `NUMERIC`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UInt8Conversion;
impl FixedSizeConversion<6> for UInt8Conversion {
    type Array = UInt8Array;
    const ENCODER_NAME: &'static str = "UInt8EncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Int2;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::UInt8)
    }
    fn field(value: u8) -> Result<[u8; 6], ErrorKind> {
        Ok(wire_field(i16::from(value).to_be_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UInt16Conversion;
impl FixedSizeConversion<8> for UInt16Conversion {
    type Array = UInt16Array;
    const ENCODER_NAME: &'static str = "UInt16EncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Int4;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::UInt16)
    }
    fn field(value: u16) -> Result<[u8; 8], ErrorKind> {
        Ok(wire_field(i32::from(value).to_be_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UInt32Conversion;
impl FixedSizeConversion<12> for UInt32Conversion {
    type Array = UInt32Array;
    const ENCODER_NAME: &'static str = "UInt32EncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Int8;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::UInt32)
    }
    fn field(value: u32) -> Result<[u8; 12], ErrorKind> {
        Ok(wire_field(i64::from(value).to_be_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Int8Conversion;
impl FixedSizeConversion<6> for Int8Conversion {
    type Array = Int8Array;
    const ENCODER_NAME: &'static str = "Int8EncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Int2;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Int8)
    }
    fn field(value: i8) -> Result<[u8; 6], ErrorKind> {
        Ok(wire_field(i16::from(value).to_be_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Int16Conversion;
impl FixedSizeConversion<6> for Int16Conversion {
    type Array = Int16Array;
    const ENCODER_NAME: &'static str = "Int16EncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Int2;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Int16)
    }
    fn field(value: i16) -> Result<[u8; 6], ErrorKind> {
        Ok(wire_field(value.to_be_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Int32Conversion;
impl FixedSizeConversion<8> for Int32Conversion {
    type Array = Int32Array;
    const ENCODER_NAME: &'static str = "Int32EncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Int4;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Int32)
    }
    fn field(value: i32) -> Result<[u8; 8], ErrorKind> {
        Ok(wire_field(value.to_be_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Int64Conversion;
impl FixedSizeConversion<12> for Int64Conversion {
    type Array = Int64Array;
    const ENCODER_NAME: &'static str = "Int64EncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Int8;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Int64)
    }
    fn field(value: i64) -> Result<[u8; 12], ErrorKind> {
        Ok(wire_field(value.to_be_bytes()))
    }
}

/// Postgres has no half precision float; `float4` is the narrowest that holds every `f16`
/// exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Float16Conversion;
impl FixedSizeConversion<8> for Float16Conversion {
    type Array = Float16Array;
    const ENCODER_NAME: &'static str = "Float16EncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Float4;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Float16)
    }
    fn field(value: <Float16Array as ValueArray>::Value) -> Result<[u8; 8], ErrorKind> {
        Ok(wire_field(f32::from(value).to_be_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Float32Conversion;
impl FixedSizeConversion<8> for Float32Conversion {
    type Array = Float32Array;
    const ENCODER_NAME: &'static str = "Float32EncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Float4;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Float32)
    }
    fn field(value: f32) -> Result<[u8; 8], ErrorKind> {
        Ok(wire_field(value.to_be_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Float64Conversion;
impl FixedSizeConversion<12> for Float64Conversion {
    type Array = Float64Array;
    const ENCODER_NAME: &'static str = "Float64EncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Float8;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Float64)
    }
    fn field(value: f64) -> Result<[u8; 12], ErrorKind> {
        Ok(wire_field(value.to_be_bytes()))
    }
}

/// Microseconds between Postgres' epoch (2000-01-01) and Arrow's / the UNIX epoch (1970-01-01).
const PG_BASE_TIMESTAMP_OFFSET_US: i64 = 946_684_800_000_000;
/// The same offset in milliseconds.
const PG_BASE_TIMESTAMP_OFFSET_MS: i64 = 946_684_800_000;
/// The same offset in seconds.
const PG_BASE_TIMESTAMP_OFFSET_S: i64 = 946_684_800;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampMicrosecondConversion;
impl FixedSizeConversion<12> for TimestampMicrosecondConversion {
    type Array = TimestampMicrosecondArray;
    const ENCODER_NAME: &'static str = "TimestampMicrosecondEncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Timestamp;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, _))
    }
    fn field(value: i64) -> Result<[u8; 12], ErrorKind> {
        // Rebase from microseconds since 1970-01-01 to microseconds since 2000-01-01.
        let value = value.checked_sub(PG_BASE_TIMESTAMP_OFFSET_US).ok_or_else(|| ErrorKind::Encode {
            reason: "Underflow converting microseconds since 1970-01-01 (Arrow) to microseconds since 2000-01-01 (Postgres)".to_string(),
        })?;
        Ok(wire_field(value.to_be_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampMillisecondConversion;
impl FixedSizeConversion<12> for TimestampMillisecondConversion {
    type Array = TimestampMillisecondArray;
    const ENCODER_NAME: &'static str = "TimestampMillisecondEncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Timestamp;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Timestamp(TimeUnit::Millisecond, _))
    }
    fn field(value: i64) -> Result<[u8; 12], ErrorKind> {
        let value = value.checked_sub(PG_BASE_TIMESTAMP_OFFSET_MS).ok_or_else(|| ErrorKind::Encode {
            reason: "Underflow converting milliseconds since 1970-01-01 (Arrow) to microseconds since 2000-01-01 (Postgres)".to_string(),
        })?;
        let value = value.checked_mul(1_000).ok_or_else(|| ErrorKind::Encode {
            reason: "Overflow converting milliseconds to microseconds".to_string(),
        })?;
        Ok(wire_field(value.to_be_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampSecondConversion;
impl FixedSizeConversion<12> for TimestampSecondConversion {
    type Array = TimestampSecondArray;
    const ENCODER_NAME: &'static str = "TimestampSecondEncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Timestamp;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Timestamp(TimeUnit::Second, _))
    }
    fn field(value: i64) -> Result<[u8; 12], ErrorKind> {
        let value = value.checked_sub(PG_BASE_TIMESTAMP_OFFSET_S).ok_or_else(|| ErrorKind::Encode {
            reason: "Underflow converting seconds since 1970-01-01 (Arrow) to microseconds since 2000-01-01 (Postgres)".to_string(),
        })?;
        let value = value
            .checked_mul(1_000_000)
            .ok_or_else(|| ErrorKind::Encode {
                reason: "Overflow converting seconds to microseconds".to_string(),
            })?;
        Ok(wire_field(value.to_be_bytes()))
    }
}

/// Days between Postgres' epoch (2000-01-01) and Arrow's / the UNIX epoch (1970-01-01).
const PG_BASE_DATE_OFFSET: i32 = 10_957;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date32Conversion;
impl FixedSizeConversion<8> for Date32Conversion {
    type Array = Date32Array;
    const ENCODER_NAME: &'static str = "Date32EncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Date;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Date32)
    }
    fn field(value: i32) -> Result<[u8; 8], ErrorKind> {
        let value = value.checked_sub(PG_BASE_DATE_OFFSET).ok_or_else(|| ErrorKind::Encode {
            reason: "Underflow converting days since 1970-01-01 (Arrow) to days since 2000-01-01 (Postgres)".to_string(),
        })?;
        Ok(wire_field(value.to_be_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Time32MillisecondConversion;
impl FixedSizeConversion<12> for Time32MillisecondConversion {
    type Array = Time32MillisecondArray;
    const ENCODER_NAME: &'static str = "Time32MillisecondEncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Time;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Time32(TimeUnit::Millisecond))
    }
    fn field(value: i32) -> Result<[u8; 12], ErrorKind> {
        let value = (value as i64)
            .checked_mul(NUM_US_PER_MS)
            .ok_or_else(|| ErrorKind::Encode {
                reason: "Overflow converting milliseconds to microseconds".to_string(),
            })?;
        Ok(wire_field(value.to_be_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Time32SecondConversion;
impl FixedSizeConversion<12> for Time32SecondConversion {
    type Array = Time32SecondArray;
    const ENCODER_NAME: &'static str = "Time32SecondEncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Time;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Time32(TimeUnit::Second))
    }
    fn field(value: i32) -> Result<[u8; 12], ErrorKind> {
        let value = (value as i64)
            .checked_mul(NUM_US_PER_S)
            .ok_or_else(|| ErrorKind::Encode {
                reason: "Overflow converting seconds to microseconds".to_string(),
            })?;
        Ok(wire_field(value.to_be_bytes()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Time64MicrosecondConversion;
impl FixedSizeConversion<12> for Time64MicrosecondConversion {
    type Array = Time64MicrosecondArray;
    const ENCODER_NAME: &'static str = "Time64MicrosecondEncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Time;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Time64(TimeUnit::Microsecond))
    }
    fn field(value: i64) -> Result<[u8; 12], ErrorKind> {
        Ok(wire_field(value.to_be_bytes()))
    }
}

const NUM_US_PER_MS: i64 = 1_000;
const NUM_US_PER_S: i64 = 1_000_000;

/// Postgres' `interval`: microseconds, then days, then months. pgpq only ever emits the
/// microsecond component, so an Arrow duration never turns into a calendar-aware interval and the
/// trailing eight bytes are always zero.
#[inline]
fn duration_payload(duration_us: i64) -> [u8; 16] {
    let mut interval = [0u8; 16];
    interval[..8].copy_from_slice(&duration_us.to_be_bytes());
    interval
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationMicrosecondConversion;
impl FixedSizeConversion<20> for DurationMicrosecondConversion {
    type Array = DurationMicrosecondArray;
    const ENCODER_NAME: &'static str = "DurationMicrosecondEncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Interval;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Duration(TimeUnit::Microsecond))
    }
    fn field(value: i64) -> Result<[u8; 20], ErrorKind> {
        Ok(wire_field(duration_payload(value)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationMillisecondConversion;
impl FixedSizeConversion<20> for DurationMillisecondConversion {
    type Array = DurationMillisecondArray;
    const ENCODER_NAME: &'static str = "DurationMillisecondEncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Interval;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Duration(TimeUnit::Millisecond))
    }
    fn field(value: i64) -> Result<[u8; 20], ErrorKind> {
        let value = value
            .mul_checked(NUM_US_PER_MS)
            .map_err(|_| ErrorKind::Encode {
                reason: "Overflow encoding millisecond duration as microseconds".to_string(),
            })?;
        Ok(wire_field(duration_payload(value)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationSecondConversion;
impl FixedSizeConversion<20> for DurationSecondConversion {
    type Array = DurationSecondArray;
    const ENCODER_NAME: &'static str = "DurationSecondEncoderBuilder";
    const POSTGRES_TYPE: PostgresType = PostgresType::Interval;
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Duration(TimeUnit::Second))
    }
    fn field(value: i64) -> Result<[u8; 20], ErrorKind> {
        let value = value
            .mul_checked(NUM_US_PER_S)
            .map_err(|_| ErrorKind::Encode {
                reason: "Overflow encoding seconds duration as microseconds".to_string(),
            })?;
        Ok(wire_field(duration_payload(value)))
    }
}

// ---------------------------------------------------------------------------------------------
// The NUMERIC conversions
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal32Conversion;
impl NumericConversion for Decimal32Conversion {
    type Array = Decimal32Array;
    const ENCODER_NAME: &'static str = "Decimal32EncoderBuilder";
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Decimal32(_, _))
    }
    fn write(arr: &Self::Array, row: usize, buf: &mut BytesMut) {
        encode_decimal_32(arr.value(row), arr.scale(), buf);
    }
    fn max_digit_groups(arr: &Self::Array) -> usize {
        // Derived from the precision alone: the scale only shifts where the base-10000 group
        // boundaries fall, and Arrow allows it to be negative.
        numeric_group_count_hint(arr.precision())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal64Conversion;
impl NumericConversion for Decimal64Conversion {
    type Array = Decimal64Array;
    const ENCODER_NAME: &'static str = "Decimal64EncoderBuilder";
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Decimal64(_, _))
    }
    fn write(arr: &Self::Array, row: usize, buf: &mut BytesMut) {
        encode_decimal_64(arr.value(row), arr.scale(), buf);
    }
    fn max_digit_groups(arr: &Self::Array) -> usize {
        numeric_group_count_hint(arr.precision())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal128Conversion;
impl NumericConversion for Decimal128Conversion {
    type Array = Decimal128Array;
    const ENCODER_NAME: &'static str = "Decimal128EncoderBuilder";
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Decimal128(_, _))
    }
    fn write(arr: &Self::Array, row: usize, buf: &mut BytesMut) {
        encode_decimal_128(arr.value(row), arr.scale(), buf);
    }
    fn max_digit_groups(arr: &Self::Array) -> usize {
        numeric_group_count_hint(arr.precision())
    }
}

/// Postgres has no unsigned 64 bit integer, so a `UInt64` is promoted to a `NUMERIC` with no
/// decimal places rather than to a (too narrow) `int8`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UInt64Conversion;
impl NumericConversion for UInt64Conversion {
    type Array = UInt64Array;
    const ENCODER_NAME: &'static str = "UInt64EncoderBuilder";
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::UInt64)
    }
    fn write(arr: &Self::Array, row: usize, buf: &mut BytesMut) {
        encode_decimal_128(arr.value(row) as i128, 0, buf);
    }
    fn max_digit_groups(_arr: &Self::Array) -> usize {
        // `u64::MAX` is 20 decimal digits, which never spans more than 5 base-10000 groups.
        5
    }
}
