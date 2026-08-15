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

use arrow_array::{
    Array, FixedSizeBinaryArray, FixedSizeListArray, GenericBinaryArray, GenericListArray,
    GenericStringArray, StringViewArray,
};
use arrow_schema::{DataType, Field, TimeUnit};
use bytes::BytesMut;
use enum_dispatch::enum_dispatch;

use crate::error::ErrorKind;
use crate::pg_schema::Column;

pub use nested::{
    GenericListArrayValues, GenericListEncoder, GenericListEncoderBuilder, StructEncoder,
    StructEncoderBuilder,
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
    GenericBinArray, GenericBinaryEncoder, GenericBinaryEncoderBuilder, GenericStrArray,
    GenericStrEncoder, LargeStringConversion, StrConversion, StrEncoderBuilder, StringConversion,
    StringViewConversion,
};

/// Append one fixed-size, already big-endian field to `buf`.
///
/// Every fixed-width write in this module goes through here, and both halves of the signature are
/// load-bearing — together they are worth ~30% of the encoding time on the NYC taxi benchmark:
///
/// * **`extend_from_slice`, not [`bytes::BufMut::put_i32`] and friends.** Every `put_*` bottoms
///   out in `<BytesMut as BufMut>::put_slice`, which carries no `#[inline]`, so from another crate
///   it stays an out-of-line call whose `copy_nonoverlapping` has a *runtime* length: a call into
///   `memcpy` to move four bytes. Profiling put ~60% of samples in `put_slice` plus the `memmove`
///   it called. The inherent `extend_from_slice` *is* `#[inline]`, so with `N` a constant the
///   length folds away and the copy becomes a store. This is why nothing below uses `BufMut`.
/// * **`#[inline(never)]`.** `extend_from_slice` expands to a capacity check, a store, a length
///   bump *and* a cold call to `BytesMut::reserve_inner`. Letting that expand at the ~40 call
///   sites measured 45% *slower* than one out-of-line copy per width (`inline(always)`: +18% over
///   the `put_i32` baseline, no attribute at all: +25%, this: -28%) — the cold call forces every
///   `encode` into a large stack frame and spills the hot values around it. One tiny monomorphic
///   function per width keeps the copy size constant without that cost.
#[inline(never)]
pub(crate) fn put<const N: usize>(buf: &mut BytesMut, bytes: [u8; N]) {
    buf.extend_from_slice(&bytes);
}

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
pub type BinaryEncoder<'a> = GenericBinaryEncoder<'a, GenericBinaryArray<i32>>;
pub type LargeBinaryEncoder<'a> = GenericBinaryEncoder<'a, GenericBinaryArray<i64>>;
pub type FixedSizeBinaryEncoder<'a> = GenericBinaryEncoder<'a, FixedSizeBinaryArray>;
pub type StringEncoder<'a> = GenericStrEncoder<'a, GenericStringArray<i32>>;
pub type LargeStringEncoder<'a> = GenericStrEncoder<'a, GenericStringArray<i64>>;
pub type StringViewEncoder<'a> = GenericStrEncoder<'a, StringViewArray>;
pub type ListEncoder<'a> = GenericListEncoder<'a, GenericListArray<i32>>;
pub type LargeListEncoder<'a> = GenericListEncoder<'a, GenericListArray<i64>>;
pub type FixedSizeListEncoder<'a> = GenericListEncoder<'a, FixedSizeListArray>;

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
pub type BinaryEncoderBuilder = GenericBinaryEncoderBuilder<GenericBinaryArray<i32>>;
pub type LargeBinaryEncoderBuilder = GenericBinaryEncoderBuilder<GenericBinaryArray<i64>>;
pub type FixedSizeBinaryEncoderBuilder = GenericBinaryEncoderBuilder<FixedSizeBinaryArray>;
pub type ListEncoderBuilder = GenericListEncoderBuilder<GenericListArray<i32>>;
pub type LargeListEncoderBuilder = GenericListEncoderBuilder<GenericListArray<i64>>;
pub type FixedSizeListEncoderBuilder = GenericListEncoderBuilder<FixedSizeListArray>;

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
    FixedSizeBinary(FixedSizeBinaryEncoder<'a>),
    String(StringEncoder<'a>),
    LargeString(LargeStringEncoder<'a>),
    StringView(StringViewEncoder<'a>),
    List(ListEncoder<'a>),
    LargeList(LargeListEncoder<'a>),
    FixedSizeList(FixedSizeListEncoder<'a>),
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
    FixedSizeBinary(FixedSizeBinaryEncoderBuilder),
    List(ListEncoderBuilder),
    LargeList(LargeListEncoderBuilder),
    FixedSizeList(FixedSizeListEncoderBuilder),
    Struct(StructEncoderBuilder),
}

impl EncoderBuilder {
    /// Pick the default encoder for `field`.
    ///
    /// The Arrow type has already been matched here, so the builders are constructed without
    /// re-checking it.
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
            DataType::LargeBinary => Self::LargeBinary(LargeBinaryEncoderBuilder::unchecked(field)),
            DataType::FixedSizeBinary(_) => {
                Self::FixedSizeBinary(FixedSizeBinaryEncoderBuilder::unchecked(field))
            }
            DataType::List(inner) => {
                let inner = Self::list_element(&field, inner)?;
                Self::List(ListEncoderBuilder::unchecked(field, inner))
            }
            DataType::LargeList(inner) => {
                let inner = Self::list_element(&field, inner)?;
                Self::LargeList(LargeListEncoderBuilder::unchecked(field, inner))
            }
            DataType::FixedSizeList(inner, _) => {
                let inner = Self::list_element(&field, inner)?;
                Self::FixedSizeList(FixedSizeListEncoderBuilder::unchecked(field, inner))
            }
            DataType::Struct(inner) => {
                let field_encoder_builders = inner
                    .iter()
                    .map(|f| EncoderBuilder::try_new(f.clone()))
                    .collect::<Result<Vec<_>, _>>()?;
                let builder = StructEncoderBuilder::unchecked(field, field_encoder_builders);
                // A composite whose fields have no OID cannot be encoded at all, so say so here
                // rather than once per batch.
                builder.field_oids()?;
                Self::Struct(builder)
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

    /// The encoder for the element type of a list column.
    ///
    /// Postgres has no array-of-arrays type (`int4[][]` is the same one dimensional `int4[]`), so
    /// a list of lists — in any of Arrow's three list layouts — is rejected here rather than
    /// silently flattened.
    fn list_element(field: &Field, inner: &Arc<Field>) -> Result<Self, ErrorKind> {
        if matches!(
            inner.data_type(),
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
        ) {
            return Err(ErrorKind::type_unsupported(
                field.name(),
                field.data_type(),
                "nested lists are not supported",
            ));
        }
        Self::try_new(inner.clone())
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{FixedSizeBinaryArray, FixedSizeListArray, Int32Array};
    use arrow_schema::Fields;

    use super::*;
    use crate::pg_schema::PostgresType;

    fn field(name: &str, data_type: DataType) -> Arc<Field> {
        Arc::new(Field::new(name, data_type, true))
    }

    fn int32_item() -> Arc<Field> {
        field("item", DataType::Int32)
    }

    /// One row of an int32 array field, encoded on its own.
    fn encode_one(builder: &EncoderBuilder, array: &dyn Array, row: usize) -> BytesMut {
        let mut buf = BytesMut::new();
        builder
            .try_new(array)
            .expect("building the encoder")
            .encode(row, &mut buf)
            .expect("encoding");
        buf
    }

    // -----------------------------------------------------------------------------------------
    // FixedSizeBinary
    // -----------------------------------------------------------------------------------------

    #[test]
    fn fixed_size_binary_is_bytea() {
        let field = field("b", DataType::FixedSizeBinary(3));
        let builder = EncoderBuilder::try_new(field).unwrap();
        assert!(matches!(builder, EncoderBuilder::FixedSizeBinary(_)));
        assert_eq!(builder.schema().data_type, PostgresType::Bytea);

        let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [Some(b"abc"), None].into_iter(),
            3,
        )
        .unwrap();
        // A `bytea` datum is a length prefix and that many bytes; a null is `-1`.
        assert_eq!(&encode_one(&builder, &array, 0)[..], b"\0\0\0\x03abc");
        assert_eq!(&encode_one(&builder, &array, 1)[..], b"\xff\xff\xff\xff");
    }

    #[test]
    fn fixed_size_binary_builder_rejects_other_types() {
        let err = FixedSizeBinaryEncoderBuilder::new(field("b", DataType::Binary)).unwrap_err();
        assert!(
            matches!(err, ErrorKind::FieldTypeNotSupported { ref encoder, .. }
                     if encoder == "FixedSizeBinaryEncoderBuilder"),
            "{err:?}"
        );
    }

    // -----------------------------------------------------------------------------------------
    // FixedSizeList
    // -----------------------------------------------------------------------------------------

    #[test]
    fn fixed_size_list_is_an_array() {
        let field = field("l", DataType::FixedSizeList(int32_item(), 2));
        let builder = EncoderBuilder::try_new(field).unwrap();
        assert!(matches!(builder, EncoderBuilder::FixedSizeList(_)));
        assert!(matches!(builder.schema().data_type, PostgresType::List(_)));

        let values = Arc::new(Int32Array::from(vec![1, 2])) as Arc<dyn Array>;
        let array = FixedSizeListArray::new(int32_item(), 2, values, None);
        // ndim, has-nulls flag, element OID (int4), length, lower bound, then the two elements.
        assert_eq!(
            &encode_one(&builder, &array, 0)[..],
            b"\0\0\0\x24\
              \0\0\0\x01\0\0\0\0\0\0\0\x17\0\0\0\x02\0\0\0\x01\
              \0\0\0\x04\0\0\0\x01\0\0\0\x04\0\0\0\x02"
        );
    }

    #[test]
    fn fixed_size_list_builder_rejects_other_types() {
        let err =
            FixedSizeListEncoderBuilder::new(field("l", DataType::List(int32_item()))).unwrap_err();
        assert!(matches!(err, ErrorKind::TypeNotSupported { .. }), "{err:?}");
    }

    #[test]
    fn nested_lists_are_rejected_in_every_layout() {
        let inner = DataType::List(int32_item());
        for outer in [
            DataType::List(field("item", inner.clone())),
            DataType::LargeList(field("item", inner.clone())),
            DataType::FixedSizeList(field("item", inner.clone()), 2),
            DataType::FixedSizeList(field("item", DataType::FixedSizeList(int32_item(), 2)), 2),
        ] {
            let err = EncoderBuilder::try_new(field("l", outer.clone())).unwrap_err();
            assert!(
                matches!(err, ErrorKind::TypeNotSupported { ref msg, .. }
                         if msg == "nested lists are not supported"),
                "{outer:?}: {err:?}"
            );
        }
    }

    // -----------------------------------------------------------------------------------------
    // Composite fields
    // -----------------------------------------------------------------------------------------

    fn struct_of(fields: Vec<Field>) -> Arc<Field> {
        field("s", DataType::Struct(Fields::from(fields)))
    }

    #[test]
    fn struct_with_a_list_field_uses_the_array_type_oid() {
        let builder = EncoderBuilder::try_new(struct_of(vec![
            Field::new("num", DataType::Int32, true),
            Field::new("nums", DataType::List(int32_item()), true),
            Field::new(
                "texts",
                DataType::LargeList(field("item", DataType::Utf8)),
                true,
            ),
        ]))
        .unwrap();
        let EncoderBuilder::Struct(builder) = builder else {
            panic!("expected a struct builder")
        };
        // int4, _int4, _text
        assert_eq!(builder.field_oids().unwrap(), vec![23, 1007, 1009]);
    }

    #[test]
    fn struct_with_a_list_of_structs_field_is_unsupported() {
        let element = field(
            "item",
            DataType::Struct(Fields::from(vec![Field::new("num", DataType::Int32, true)])),
        );
        let err = EncoderBuilder::try_new(struct_of(vec![Field::new(
            "structs",
            DataType::List(element),
            true,
        )]))
        .unwrap_err();
        assert!(
            matches!(err, ErrorKind::TypeNotSupported { ref msg, .. }
                     if msg.contains("has no Postgres OID")),
            "{err:?}"
        );
    }

    #[test]
    fn struct_with_a_nested_struct_field_is_still_supported() {
        // Nested composites keep the placeholder OID they have always used; only the array case
        // gained a real one.
        let inner = Field::new(
            "inner",
            DataType::Struct(Fields::from(vec![Field::new("num", DataType::Int32, true)])),
            true,
        );
        let builder = EncoderBuilder::try_new(struct_of(vec![inner])).unwrap();
        assert!(matches!(builder, EncoderBuilder::Struct(_)));
    }
}
