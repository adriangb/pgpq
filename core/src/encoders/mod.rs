//! Arrow -> Postgres binary `COPY` encoders.
//!
//! Two families of types make up this module:
//!
//! * an [`EncoderBuilder`] is bound to a schema [`Field`] and knows what Postgres column that
//!   field maps to ([`BuildEncoder::schema`]); it is cheap to hold on to and is what callers
//!   override when they want, say, a `Utf8` column written as `jsonb`;
//! * an [`Encoder`] is bound to one Arrow [`Array`] — one column of one batch — and writes
//!   individual rows ([`Encode::encode`]).
//!
//! Both are enums dispatched with `enum_dispatch`, so the hot loop in
//! [`crate::ArrowToPostgresBinaryEncoder::write_batch`] stays monomorphic. The variants are thin
//! aliases: most of them are instantiations of a handful of generic encoders parameterised by a
//! *conversion* (see [`scalar`]), and only the genuinely different shapes — binary, strings,
//! lists, structs — have their own types.

mod nested;
mod numeric;
mod scalar;
mod text;

use std::any::type_name;
use std::sync::Arc;

use arrow_array::{Array, GenericStringArray, StringViewArray};
use arrow_schema::{DataType, Field, TimeUnit};
use bytes::BytesMut;
use enum_dispatch::enum_dispatch;

use crate::error::ErrorKind;
use crate::pg_schema::Column;

pub use nested::{
    GenericListEncoder, GenericListEncoderBuilder, StructEncoder, StructEncoderBuilder,
};
pub use scalar::{
    BooleanConversion, Date32Conversion, Decimal128Conversion, Decimal32Conversion,
    Decimal64Conversion, DurationMicrosecondConversion, DurationMillisecondConversion,
    DurationSecondConversion, FixedSizeConversion, FixedSizeEncoder, FixedSizeEncoderBuilder,
    Float16Conversion, Float32Conversion, Float64Conversion, Int16Conversion, Int32Conversion,
    Int64Conversion, Int8Conversion, Int8EncoderBuilder, NumericConversion, NumericEncoder,
    NumericEncoderBuilder, Time32MillisecondConversion, Time32SecondConversion,
    Time64MicrosecondConversion, TimestampMicrosecondConversion, TimestampMillisecondConversion,
    TimestampSecondConversion, UInt16Conversion, UInt32Conversion, UInt64Conversion,
    UInt8Conversion, ValueArray,
};
pub use text::{
    GenericBinaryEncoder, GenericBinaryEncoderBuilder, GenericStrArray, GenericStrEncoder,
    LargeStringConversion, StrConversion, StrEncoderBuilder, StringConversion,
    StringViewConversion,
};

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

#[enum_dispatch]
pub trait BuildEncoder: std::fmt::Debug + PartialEq {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind>;
    fn schema(&self) -> Column;
    fn field(&self) -> Arc<Field>;
}

// ---------------------------------------------------------------------------------------------
// The per-type names
//
// Every alias below names one (Arrow type -> Postgres type) mapping. Adding a type is an alias
// plus a conversion impl plus an enum variant; the encoding logic itself is shared.
// ---------------------------------------------------------------------------------------------

pub type BooleanEncoder<'a> = FixedSizeEncoder<'a, BooleanConversion>;
pub type UInt8Encoder<'a> = FixedSizeEncoder<'a, UInt8Conversion>;
pub type UInt16Encoder<'a> = FixedSizeEncoder<'a, UInt16Conversion>;
pub type UInt32Encoder<'a> = FixedSizeEncoder<'a, UInt32Conversion>;
pub type UInt64Encoder<'a> = NumericEncoder<'a, UInt64Conversion>;
pub type Int8Encoder<'a> = FixedSizeEncoder<'a, Int8Conversion>;
pub type Int16Encoder<'a> = FixedSizeEncoder<'a, Int16Conversion>;
pub type Int32Encoder<'a> = FixedSizeEncoder<'a, Int32Conversion>;
pub type Int64Encoder<'a> = FixedSizeEncoder<'a, Int64Conversion>;
pub type Float16Encoder<'a> = FixedSizeEncoder<'a, Float16Conversion>;
pub type Float32Encoder<'a> = FixedSizeEncoder<'a, Float32Conversion>;
pub type Float64Encoder<'a> = FixedSizeEncoder<'a, Float64Conversion>;
pub type Decimal32Encoder<'a> = NumericEncoder<'a, Decimal32Conversion>;
pub type Decimal64Encoder<'a> = NumericEncoder<'a, Decimal64Conversion>;
pub type Decimal128Encoder<'a> = NumericEncoder<'a, Decimal128Conversion>;
pub type TimestampMicrosecondEncoder<'a> = FixedSizeEncoder<'a, TimestampMicrosecondConversion>;
pub type TimestampMillisecondEncoder<'a> = FixedSizeEncoder<'a, TimestampMillisecondConversion>;
pub type TimestampSecondEncoder<'a> = FixedSizeEncoder<'a, TimestampSecondConversion>;
pub type Date32Encoder<'a> = FixedSizeEncoder<'a, Date32Conversion>;
pub type Time32MillisecondEncoder<'a> = FixedSizeEncoder<'a, Time32MillisecondConversion>;
pub type Time32SecondEncoder<'a> = FixedSizeEncoder<'a, Time32SecondConversion>;
pub type Time64MicrosecondEncoder<'a> = FixedSizeEncoder<'a, Time64MicrosecondConversion>;
pub type DurationMicrosecondEncoder<'a> = FixedSizeEncoder<'a, DurationMicrosecondConversion>;
pub type DurationMillisecondEncoder<'a> = FixedSizeEncoder<'a, DurationMillisecondConversion>;
pub type DurationSecondEncoder<'a> = FixedSizeEncoder<'a, DurationSecondConversion>;
pub type BinaryEncoder<'a> = GenericBinaryEncoder<'a, i32>;
pub type LargeBinaryEncoder<'a> = GenericBinaryEncoder<'a, i64>;
pub type StringEncoder<'a> = GenericStrEncoder<'a, GenericStringArray<i32>>;
pub type LargeStringEncoder<'a> = GenericStrEncoder<'a, GenericStringArray<i64>>;
pub type StringViewEncoder<'a> = GenericStrEncoder<'a, StringViewArray>;
pub type ListEncoder<'a> = GenericListEncoder<'a, i32>;
pub type LargeListEncoder<'a> = GenericListEncoder<'a, i64>;

pub type BooleanEncoderBuilder = FixedSizeEncoderBuilder<BooleanConversion>;
pub type UInt8EncoderBuilder = FixedSizeEncoderBuilder<UInt8Conversion>;
pub type UInt16EncoderBuilder = FixedSizeEncoderBuilder<UInt16Conversion>;
pub type UInt32EncoderBuilder = FixedSizeEncoderBuilder<UInt32Conversion>;
pub type UInt64EncoderBuilder = NumericEncoderBuilder<UInt64Conversion>;
pub type Int16EncoderBuilder = FixedSizeEncoderBuilder<Int16Conversion>;
pub type Int32EncoderBuilder = FixedSizeEncoderBuilder<Int32Conversion>;
pub type Int64EncoderBuilder = FixedSizeEncoderBuilder<Int64Conversion>;
pub type Float16EncoderBuilder = FixedSizeEncoderBuilder<Float16Conversion>;
pub type Float32EncoderBuilder = FixedSizeEncoderBuilder<Float32Conversion>;
pub type Float64EncoderBuilder = FixedSizeEncoderBuilder<Float64Conversion>;
pub type Decimal32EncoderBuilder = NumericEncoderBuilder<Decimal32Conversion>;
pub type Decimal64EncoderBuilder = NumericEncoderBuilder<Decimal64Conversion>;
pub type Decimal128EncoderBuilder = NumericEncoderBuilder<Decimal128Conversion>;
pub type TimestampMicrosecondEncoderBuilder =
    FixedSizeEncoderBuilder<TimestampMicrosecondConversion>;
pub type TimestampMillisecondEncoderBuilder =
    FixedSizeEncoderBuilder<TimestampMillisecondConversion>;
pub type TimestampSecondEncoderBuilder = FixedSizeEncoderBuilder<TimestampSecondConversion>;
pub type Date32EncoderBuilder = FixedSizeEncoderBuilder<Date32Conversion>;
pub type Time32MillisecondEncoderBuilder = FixedSizeEncoderBuilder<Time32MillisecondConversion>;
pub type Time32SecondEncoderBuilder = FixedSizeEncoderBuilder<Time32SecondConversion>;
pub type Time64MicrosecondEncoderBuilder = FixedSizeEncoderBuilder<Time64MicrosecondConversion>;
pub type DurationMicrosecondEncoderBuilder = FixedSizeEncoderBuilder<DurationMicrosecondConversion>;
pub type DurationMillisecondEncoderBuilder = FixedSizeEncoderBuilder<DurationMillisecondConversion>;
pub type DurationSecondEncoderBuilder = FixedSizeEncoderBuilder<DurationSecondConversion>;
pub type StringEncoderBuilder = StrEncoderBuilder<StringConversion>;
pub type LargeStringEncoderBuilder = StrEncoderBuilder<LargeStringConversion>;
pub type StringViewEncoderBuilder = StrEncoderBuilder<StringViewConversion>;
pub type BinaryEncoderBuilder = GenericBinaryEncoderBuilder<i32>;
pub type LargeBinaryEncoderBuilder = GenericBinaryEncoderBuilder<i64>;
pub type ListEncoderBuilder = GenericListEncoderBuilder<i32>;
pub type LargeListEncoderBuilder = GenericListEncoderBuilder<i64>;

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
    /// Pick the default encoder for `field`.
    ///
    /// The Arrow type has already been matched here, so the builders are constructed without
    /// re-checking it; that also keeps the two lenient mappings below (`FixedSizeBinary` and
    /// `FixedSizeList`, which fall through to their large variants) working the way they always
    /// have — the mismatch surfaces as a column type error when a batch is encoded.
    pub fn try_new(field: Arc<Field>) -> Result<Self, ErrorKind> {
        let data_type = field.data_type();
        let res = match data_type {
            DataType::Boolean => Self::Boolean(BooleanEncoderBuilder::unchecked(field)),
            DataType::UInt8 => Self::UInt8(UInt8EncoderBuilder::unchecked(field)),
            DataType::UInt16 => Self::UInt16(UInt16EncoderBuilder::unchecked(field)),
            DataType::UInt32 => Self::UInt32(UInt32EncoderBuilder::unchecked(field)),
            DataType::UInt64 => Self::UInt64(UInt64EncoderBuilder::unchecked(field)),
            // Note that rust-postgres encodes int8 to CHAR by default
            DataType::Int8 => Self::Int8(Int8EncoderBuilder::unchecked(field)),
            DataType::Int16 => Self::Int16(Int16EncoderBuilder::unchecked(field)),
            DataType::Int32 => Self::Int32(Int32EncoderBuilder::unchecked(field)),
            DataType::Int64 => Self::Int64(Int64EncoderBuilder::unchecked(field)),
            DataType::Float16 => Self::Float16(Float16EncoderBuilder::unchecked(field)),
            DataType::Float32 => Self::Float32(Float32EncoderBuilder::unchecked(field)),
            DataType::Float64 => Self::Float64(Float64EncoderBuilder::unchecked(field)),
            DataType::Decimal32(_, _) => Self::Decimal32(Decimal32EncoderBuilder::unchecked(field)),
            DataType::Decimal64(_, _) => Self::Decimal64(Decimal64EncoderBuilder::unchecked(field)),
            DataType::Decimal128(_, _) => {
                Self::Decimal128(Decimal128EncoderBuilder::unchecked(field))
            }
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Nanosecond => {
                    return Err(ErrorKind::type_unsupported(
                        field.name(),
                        data_type,
                        "Postgres does not support ns precision; convert to us",
                    ))
                }
                TimeUnit::Microsecond => {
                    Self::TimestampMicrosecond(TimestampMicrosecondEncoderBuilder::unchecked(field))
                }
                TimeUnit::Millisecond => {
                    Self::TimestampMillisecond(TimestampMillisecondEncoderBuilder::unchecked(field))
                }
                TimeUnit::Second => {
                    Self::TimestampSecond(TimestampSecondEncoderBuilder::unchecked(field))
                }
            },
            DataType::Date32 => Self::Date32(Date32EncoderBuilder::unchecked(field)),
            DataType::Time32(unit) => match unit {
                TimeUnit::Millisecond => {
                    Self::Time32Millisecond(Time32MillisecondEncoderBuilder::unchecked(field))
                }
                TimeUnit::Second => {
                    Self::Time32Second(Time32SecondEncoderBuilder::unchecked(field))
                }
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
                    Self::Time64Microsecond(Time64MicrosecondEncoderBuilder::unchecked(field))
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
                    Self::DurationMicrosecond(DurationMicrosecondEncoderBuilder::unchecked(field))
                }
                TimeUnit::Millisecond => {
                    Self::DurationMillisecond(DurationMillisecondEncoderBuilder::unchecked(field))
                }
                TimeUnit::Second => {
                    Self::DurationSecond(DurationSecondEncoderBuilder::unchecked(field))
                }
            },
            DataType::Utf8 => Self::String(StringEncoderBuilder::unchecked(field)),
            DataType::LargeUtf8 => Self::LargeString(LargeStringEncoderBuilder::unchecked(field)),
            DataType::Utf8View => Self::StringView(StringViewEncoderBuilder::unchecked(field)),
            DataType::Binary => Self::Binary(BinaryEncoderBuilder::unchecked(field)),
            DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                Self::LargeBinary(LargeBinaryEncoderBuilder::unchecked(field))
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
                Self::List(ListEncoderBuilder::unchecked(field, inner))
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
                Self::LargeList(LargeListEncoderBuilder::unchecked(field, inner))
            }
            DataType::Struct(inner) => {
                let field_encoder_builders = inner
                    .iter()
                    .map(|f| EncoderBuilder::try_new(f.clone()))
                    .collect::<Result<Vec<_>, _>>()?;
                Self::Struct(StructEncoderBuilder::unchecked(
                    field,
                    field_encoder_builders,
                ))
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
