use arrow_schema::{DataType, TimeUnit};
use enum_dispatch::enum_dispatch;
use std::{any::type_name, convert::identity, sync::Arc};

use arrow_array::{self, Array, ArrowNativeTypeOp, OffsetSizeTrait};
use bytes::{BufMut, BytesMut};

use crate::error::{Error, ErrorKind};

#[inline]
fn downcast_checked<'a, T: 'static>(arr: &'a dyn Array, field: &str) -> Result<&'a T, Error> {
    match arr.as_any().downcast_ref::<T>() {
        Some(v) => Ok(v),
        None => Err(Error::mismatched_column_type(
            field,
            type_name::<T>(),
            arr.data_type(),
        )),
    }
}

#[enum_dispatch]
pub(crate) trait Encode {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error>;
    fn size_hint(&self) -> Result<usize, Error>;
}

#[enum_dispatch(Encode)]
#[derive(Debug, Clone)]
pub(crate) enum Encoder<'a> {
    Boolean(BooleanEncoder<'a>),
    UInt8(UInt8Encoder<'a>),
    UInt16(UInt16Encoder<'a>),
    UInt32(UInt32Encoder<'a>),
    Int8(Int8Encoder<'a>),
    Int16(Int16Encoder<'a>),
    Int32(Int32Encoder<'a>),
    Int64(Int64Encoder<'a>),
    Float16(Float16Encoder<'a>),
    Float32(Float32Encoder<'a>),
    Float64(Float64Encoder<'a>),
    TimestampMicrosecond(TimestampMicrosecondEncoder<'a>),
    TimestampMillisecond(TimestampMillisecondEncoder<'a>),
    TimestampSecond(TimestampSecondEncoder<'a>),
    Date32(Date32Encoder<'a>),
    Date64(Date64Encoder<'a>),
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
    List(ListEncoder<'a>),
    LargeList(LargeListEncoder<'a>),
}

macro_rules! impl_encode {
    ($struct_name:ident, $field_size:expr, $transform:expr, $write:expr) => {
        impl<'a> Encode for $struct_name<'a> {
            fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error> {
                if self.arr.is_null(row) {
                    buf.put_i32(-1)
                } else {
                    buf.put_i32($field_size);
                    let v = self.arr.value(row);
                    let tv = $transform(v);
                    $write(buf, tv);
                }
                Ok(())
            }
            fn size_hint(&self) -> Result<usize, Error> {
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
            fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error> {
                if self.arr.is_null(row) {
                    buf.put_i32(-1)
                } else {
                    buf.put_i32($field_size);
                    let v = self.arr.value(row);
                    let tv = $transform(&self.field, v)?;
                    $write(buf, tv);
                }
                Ok(())
            }
            fn size_hint(&self) -> Result<usize, Error> {
                let null_count = self.arr.null_count();
                let item_count = self.arr.len();
                Ok((item_count - null_count) * $field_size + item_count)
            }
        }
    };
}

#[derive(Debug, Clone)]
pub(crate) struct BooleanEncoder<'a> {
    arr: &'a arrow_array::BooleanArray,
}
impl_encode!(BooleanEncoder, 1, u8::from, BufMut::put_u8);

#[derive(Debug, Clone)]
pub(crate) struct UInt8Encoder<'a> {
    arr: &'a arrow_array::UInt8Array,
}
impl_encode!(UInt8Encoder, 2, i16::from, BufMut::put_i16);

#[derive(Debug, Clone)]
pub(crate) struct UInt16Encoder<'a> {
    arr: &'a arrow_array::UInt16Array,
}
impl_encode!(UInt16Encoder, 4, i32::from, BufMut::put_i32);

#[derive(Debug, Clone)]
pub(crate) struct UInt32Encoder<'a> {
    arr: &'a arrow_array::UInt32Array,
}
impl_encode!(UInt32Encoder, 8, i64::from, BufMut::put_i64);

#[derive(Debug, Clone)]
pub(crate) struct Int8Encoder<'a> {
    arr: &'a arrow_array::Int8Array,
}
impl_encode!(Int8Encoder, 2, i16::from, BufMut::put_i16);

#[derive(Debug, Clone)]
pub(crate) struct Int16Encoder<'a> {
    arr: &'a arrow_array::Int16Array,
}
impl_encode!(Int16Encoder, 2, identity, BufMut::put_i16);

#[derive(Debug, Clone)]
pub(crate) struct Int32Encoder<'a> {
    arr: &'a arrow_array::Int32Array,
}
impl_encode!(Int32Encoder, 4, identity, BufMut::put_i32);

#[derive(Debug, Clone)]
pub(crate) struct Int64Encoder<'a> {
    arr: &'a arrow_array::Int64Array,
}
impl_encode!(Int64Encoder, 8, identity, BufMut::put_i64);

#[derive(Debug, Clone)]
pub(crate) struct Float16Encoder<'a> {
    arr: &'a arrow_array::Float16Array,
}
impl_encode!(Float16Encoder, 4, f32::from, BufMut::put_f32);

#[derive(Debug, Clone)]
pub(crate) struct Float32Encoder<'a> {
    arr: &'a arrow_array::Float32Array,
}
impl_encode!(Float32Encoder, 4, identity, BufMut::put_f32);

#[derive(Debug, Clone)]
pub(crate) struct Float64Encoder<'a> {
    arr: &'a arrow_array::Float64Array,
}
impl_encode!(Float64Encoder, 8, identity, BufMut::put_f64);

const ONE_S_TO_MS: i64 = 1_000;
const ONE_S_TO_US: i64 = 1_000_000;

// Postgres starts counting on Jan 1st 2000
// This is Jan 1st 2000 relative to the UNIX Epoch in us
const POSTGRES_BASE_TIMESTAMP_S: i64 = 946_684_800;
const POSTGRES_BASE_TIMESTAMP_MS: i64 = POSTGRES_BASE_TIMESTAMP_S * ONE_S_TO_MS;
const POSTGRES_BASE_TIMESTAMP_US: i64 = POSTGRES_BASE_TIMESTAMP_S * ONE_S_TO_US;

const NUM_US_PER_MS: i64 = 1_000;
const NUM_US_PER_S: i64 = 1_000_000;

#[inline]
fn adjust_timestamp(val: i64, offset: i64) -> Result<i64, Error> {
    val.sub_checked(offset).map_err(|_| {
        Error::new(
            ErrorKind::Encode {
                reason: "Value too large to transmit".to_string(),
            },
            None,
        )
    })
}

#[derive(Debug, Clone)]
pub(crate) struct TimestampMicrosecondEncoder<'a> {
    arr: &'a arrow_array::TimestampMicrosecondArray,
    field: String,
}
impl_encode_fallible!(
    TimestampMicrosecondEncoder,
    8,
    |_: &str, v: i64| adjust_timestamp(v, POSTGRES_BASE_TIMESTAMP_US),
    BufMut::put_i64
);

#[derive(Debug, Clone)]
pub(crate) struct TimestampMillisecondEncoder<'a> {
    arr: &'a arrow_array::TimestampMillisecondArray,
    field: String,
}
impl_encode_fallible!(
    TimestampMillisecondEncoder,
    8,
    |_: &str, v: i64| {
        let v = adjust_timestamp(v, POSTGRES_BASE_TIMESTAMP_MS)?;
        match v.mul_checked(NUM_US_PER_MS) {
            Ok(v) => Ok(v),
            Err(_) => Err(Error::new(
                ErrorKind::Encode {
                    reason: "Overflow encoding millisecond timestamp as microseconds".to_string(),
                },
                None,
            )),
        }
    },
    BufMut::put_i64
);

#[derive(Debug, Clone)]
pub(crate) struct TimestampSecondEncoder<'a> {
    arr: &'a arrow_array::TimestampSecondArray,
    field: String,
}
impl_encode_fallible!(
    TimestampSecondEncoder,
    8,
    |_: &str, v: i64| {
        let v = adjust_timestamp(v, POSTGRES_BASE_TIMESTAMP_S)?;
        match v.mul_checked(NUM_US_PER_S) {
            Ok(v) => Ok(v),
            Err(_) => Err(Error::new(
                ErrorKind::Encode {
                    reason: "Overflow encoding seconds timestamp as microseconds".to_string(),
                },
                None,
            )),
        }
    },
    BufMut::put_i64
);

#[derive(Debug, Clone)]
pub(crate) struct Date32Encoder<'a> {
    arr: &'a arrow_array::Date32Array,
}
impl_encode!(Date32Encoder, 4, identity, BufMut::put_i32);

#[derive(Debug, Clone)]
pub(crate) struct Date64Encoder<'a> {
    arr: &'a arrow_array::Date64Array,
    field: String,
}
impl_encode_fallible!(
    Date64Encoder,
    4,
    |_field: &str, v: i64| {
        i32::try_from(v).map_err(|_| {
            Error::new(
                ErrorKind::Encode {
                    reason: "overflow converting 64 bit date to 32 bit date".to_string(),
                },
                None,
            )
        })
    },
    BufMut::put_i32
);

#[derive(Debug, Clone)]
pub(crate) struct Time32MillisecondEncoder<'a> {
    arr: &'a arrow_array::Time32MillisecondArray,
}
impl_encode!(
    Time32MillisecondEncoder,
    8,
    |v| (v as i64) * NUM_US_PER_MS,
    BufMut::put_i64
);

#[derive(Debug, Clone)]
pub(crate) struct Time32SecondEncoder<'a> {
    arr: &'a arrow_array::Time32SecondArray,
}
impl_encode!(
    Time32SecondEncoder,
    8,
    |v| (v as i64) * NUM_US_PER_S,
    BufMut::put_i64
);

#[inline]
fn write_duration(buf: &mut BytesMut, duration_us: i64) {
    buf.put_i64(duration_us);
    buf.put_i32(0); // days
    buf.put_i32(0); // months
}

#[derive(Debug, Clone)]
pub(crate) struct Time64MicrosecondEncoder<'a> {
    arr: &'a arrow_array::Time64MicrosecondArray,
}
impl_encode!(Time64MicrosecondEncoder, 8, identity, BufMut::put_i64);

#[derive(Debug, Clone)]
pub(crate) struct DurationMicrosecondEncoder<'a> {
    arr: &'a arrow_array::DurationMicrosecondArray,
}
impl_encode!(DurationMicrosecondEncoder, 16, identity, write_duration);

#[derive(Debug, Clone)]
pub(crate) struct DurationMillisecondEncoder<'a> {
    arr: &'a arrow_array::DurationMillisecondArray,
    field: String,
}
impl_encode_fallible!(
    DurationMillisecondEncoder,
    16,
    |_: &str, v: i64| v.mul_checked(NUM_US_PER_MS).map_err(|_| Error::new(
        ErrorKind::Encode {
            reason: "Overflow encoding millisecond Duration as microseconds".to_string()
        },
        None
    )),
    write_duration
);

#[derive(Debug, Clone)]
pub(crate) struct DurationSecondEncoder<'a> {
    arr: &'a arrow_array::DurationSecondArray,
    field: String,
}
impl_encode_fallible!(
    DurationSecondEncoder,
    16,
    |_: &str, v: i64| v.mul_checked(NUM_US_PER_S).map_err(|_| Error::new(
        ErrorKind::Encode {
            reason: "Overflow encoding second Duration as microseconds".to_string()
        },
        None
    )),
    write_duration
);

#[derive(Debug, Clone)]
pub(crate) struct GenericBinaryEncoder<'a, T: OffsetSizeTrait> {
    arr: &'a arrow_array::GenericBinaryArray<T>,
    field: String,
}

impl<'a, T: OffsetSizeTrait> Encode for GenericBinaryEncoder<'a, T> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error> {
        if self.arr.is_null(row) {
            buf.put_i32(-1);
        } else {
            let v = self.arr.value(row);
            let len = v.len();
            match i32::try_from(len) {
                Ok(l) => buf.put_i32(l),
                Err(_) => return Err(Error::field_too_large(&self.field, len)),
            }
            buf.extend_from_slice(v);
        }
        Ok(())
    }
    fn size_hint(&self) -> Result<usize, Error> {
        let mut total = 0;
        for row in 0..self.arr.len() {
            total += self.arr.value(row).len();
        }
        Ok(total)
    }
}

type BinaryEncoder<'a> = GenericBinaryEncoder<'a, i32>;
type LargeBinaryEncoder<'a> = GenericBinaryEncoder<'a, i64>;

#[derive(Debug, Clone)]
pub(crate) struct GenericStringEncoder<'a, T: OffsetSizeTrait> {
    arr: &'a arrow_array::GenericStringArray<T>,
    field: String,
}

impl<'a, T: OffsetSizeTrait> Encode for GenericStringEncoder<'a, T> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error> {
        if self.arr.is_null(row) {
            buf.put_i32(-1);
        } else {
            let v = self.arr.value(row).as_bytes();
            let len = v.len();
            match i32::try_from(len) {
                Ok(l) => buf.put_i32(l),
                Err(_) => return Err(Error::field_too_large(&self.field, len)),
            }
            buf.extend_from_slice(v);
        }
        Ok(())
    }
    fn size_hint(&self) -> Result<usize, Error> {
        let mut total = 0;
        for row in 0..self.arr.len() {
            total += self.arr.value(row).len();
        }
        Ok(total)
    }
}

type StringEncoder<'a> = GenericStringEncoder<'a, i32>;
type LargeStringEncoder<'a> = GenericStringEncoder<'a, i64>;

#[derive(Debug, Clone)]
pub(crate) struct GenericListEncoder<'a, T: OffsetSizeTrait> {
    arr: &'a arrow_array::GenericListArray<T>,
    field: String,
    inner_encoder_builder: Arc<EncoderBuilder>,
}

impl<'a, T: OffsetSizeTrait> Encode for GenericListEncoder<'a, T> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error> {
        if self.arr.is_null(row) {
            buf.put_i32(-1);
        } else {
            let val = self.arr.value(row);
            let inner_encoder = self.inner_encoder_builder.try_new(&val)?;
            let size = inner_encoder.size_hint()?;
            match i32::try_from(size) {
                Ok(v) => buf.put_i32(v),
                Err(_) => return Err(Error::field_too_large(&self.field, size)),
            };
            for inner_row in 0..val.len() {
                inner_encoder.encode(inner_row, buf)?
            }
        }
        Ok(())
    }
    fn size_hint(&self) -> Result<usize, Error> {
        let mut total = 0;
        for row in 0..self.arr.len() {
            if !self.arr.is_null(row) {
                let val = self.arr.value(row);
                let inner_encoder = self.inner_encoder_builder.try_new(&val)?;
                let size = inner_encoder.size_hint()?;
                total += size;
            }
        }
        Ok(total)
    }
}

type ListEncoder<'a> = GenericListEncoder<'a, i32>;
type LargeListEncoder<'a> = GenericListEncoder<'a, i64>;

#[enum_dispatch]
pub(crate) trait BuildEncoder {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error>;
}

macro_rules! impl_encoder_builder_stateless {
    ($struct_name:ident, $enum_name:expr, $encoder_name:ident) => {
        impl BuildEncoder for $struct_name {
            fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
                Ok($enum_name($encoder_name {
                    arr: downcast_checked(arr, &self.field)?,
                }))
            }
        }
    };
}

macro_rules! impl_encoder_builder_stateless_with_field {
    ($struct_name:ident, $enum_name:expr, $encoder_name:ident) => {
        impl BuildEncoder for $struct_name {
            fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
                let field = self.field.clone();
                let arr = downcast_checked(arr, &self.field)?;
                Ok($enum_name($encoder_name { arr, field }))
            }
        }
    };
}

#[derive(Debug, Clone)]
pub(crate) struct BooleanEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(BooleanEncoderBuilder, Encoder::Boolean, BooleanEncoder);

#[derive(Debug, Clone)]
pub(crate) struct UInt8EncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(UInt8EncoderBuilder, Encoder::UInt8, UInt8Encoder);

#[derive(Debug, Clone)]
pub(crate) struct UInt16EncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(UInt16EncoderBuilder, Encoder::UInt16, UInt16Encoder);

#[derive(Debug, Clone)]
pub(crate) struct UInt32EncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(UInt32EncoderBuilder, Encoder::UInt32, UInt32Encoder);

#[derive(Debug, Clone)]
pub(crate) struct Int8EncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(Int8EncoderBuilder, Encoder::Int8, Int8Encoder);

#[derive(Debug, Clone)]
pub(crate) struct Int16EncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(Int16EncoderBuilder, Encoder::Int16, Int16Encoder);

#[derive(Debug, Clone)]
pub(crate) struct Int32EncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(Int32EncoderBuilder, Encoder::Int32, Int32Encoder);

#[derive(Debug, Clone)]
pub(crate) struct Int64EncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(Int64EncoderBuilder, Encoder::Int64, Int64Encoder);

#[derive(Debug, Clone)]
pub(crate) struct Float16EncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(Float16EncoderBuilder, Encoder::Float16, Float16Encoder);

#[derive(Debug, Clone)]
pub(crate) struct Float32EncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(Float32EncoderBuilder, Encoder::Float32, Float32Encoder);

#[derive(Debug, Clone)]
pub(crate) struct Float64EncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(Float64EncoderBuilder, Encoder::Float64, Float64Encoder);

#[derive(Debug, Clone)]
pub(crate) struct TimestampMicrosecondEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless_with_field!(
    TimestampMicrosecondEncoderBuilder,
    Encoder::TimestampMicrosecond,
    TimestampMicrosecondEncoder
);

#[derive(Debug, Clone)]
pub(crate) struct TimestampMillisecondEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless_with_field!(
    TimestampMillisecondEncoderBuilder,
    Encoder::TimestampMillisecond,
    TimestampMillisecondEncoder
);

#[derive(Debug, Clone)]
pub(crate) struct TimestampSecondEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless_with_field!(
    TimestampSecondEncoderBuilder,
    Encoder::TimestampSecond,
    TimestampSecondEncoder
);

#[derive(Debug, Clone)]
pub(crate) struct Date32EncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(Date32EncoderBuilder, Encoder::Date32, Date32Encoder);

#[derive(Debug, Clone)]
pub(crate) struct Date64EncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless_with_field!(Date64EncoderBuilder, Encoder::Date64, Date64Encoder);

#[derive(Debug, Clone)]
pub(crate) struct Time32MillisecondEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(
    Time32MillisecondEncoderBuilder,
    Encoder::Time32Millisecond,
    Time32MillisecondEncoder
);

#[derive(Debug, Clone)]
pub(crate) struct Time32SecondEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(
    Time32SecondEncoderBuilder,
    Encoder::Time32Second,
    Time32SecondEncoder
);

#[derive(Debug, Clone)]
pub(crate) struct Time64MicrosecondEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(
    Time64MicrosecondEncoderBuilder,
    Encoder::Time64Microsecond,
    Time64MicrosecondEncoder
);

#[derive(Debug, Clone)]
pub(crate) struct DurationMicrosecondEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless!(
    DurationMicrosecondEncoderBuilder,
    Encoder::DurationMicrosecond,
    DurationMicrosecondEncoder
);

#[derive(Debug, Clone)]
pub(crate) struct DurationMillisecondEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless_with_field!(
    DurationMillisecondEncoderBuilder,
    Encoder::DurationMillisecond,
    DurationMillisecondEncoder
);

#[derive(Debug, Clone)]
pub(crate) struct DurationSecondEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless_with_field!(
    DurationSecondEncoderBuilder,
    Encoder::DurationSecond,
    DurationSecondEncoder
);

#[derive(Debug, Clone)]
pub(crate) struct StringEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless_with_field!(StringEncoderBuilder, Encoder::String, StringEncoder);

#[derive(Debug, Clone)]
pub(crate) struct LargeStringEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless_with_field!(
    LargeStringEncoderBuilder,
    Encoder::LargeString,
    LargeStringEncoder
);

#[derive(Debug, Clone)]
pub(crate) struct BinaryEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless_with_field!(BinaryEncoderBuilder, Encoder::Binary, BinaryEncoder);

#[derive(Debug, Clone)]
pub(crate) struct LargeBinaryEncoderBuilder {
    field: String,
}
impl_encoder_builder_stateless_with_field!(
    LargeBinaryEncoderBuilder,
    Encoder::LargeBinary,
    LargeBinaryEncoder
);

#[derive(Debug, Clone)]
pub(crate) struct ListEncoderBuilder {
    field: String,
    inner_encoder_builder: Arc<EncoderBuilder>,
}

impl BuildEncoder for ListEncoderBuilder {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
        let arr = downcast_checked(arr, &self.field)?;
        let field = self.field.clone();
        let inner_encoder_builder = self.inner_encoder_builder.clone();
        Ok(Encoder::List(ListEncoder {
            arr,
            field,
            inner_encoder_builder,
        }))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LargeListEncoderBuilder {
    field: String,
    inner_encoder_builder: Arc<EncoderBuilder>,
}

impl BuildEncoder for LargeListEncoderBuilder {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
        let arr = downcast_checked(arr, &self.field)?;
        let field = self.field.clone();
        let inner_encoder_builder = self.inner_encoder_builder.clone();
        Ok(Encoder::LargeList(LargeListEncoder {
            arr,
            field,
            inner_encoder_builder,
        }))
    }
}

#[enum_dispatch(BuildEncoder)]
#[derive(Debug, Clone)]
pub(crate) enum EncoderBuilder {
    Boolean(BooleanEncoderBuilder),
    UInt8(UInt8EncoderBuilder),
    UInt16(UInt16EncoderBuilder),
    UInt32(UInt32EncoderBuilder),
    Int8(Int8EncoderBuilder),
    Int16(Int16EncoderBuilder),
    Int32(Int32EncoderBuilder),
    Int64(Int64EncoderBuilder),
    Float16(Float16EncoderBuilder),
    Float32(Float32EncoderBuilder),
    Float64(Float64EncoderBuilder),
    TimestampMicrosecond(TimestampMicrosecondEncoderBuilder),
    TimestampMillisecond(TimestampMillisecondEncoderBuilder),
    TimestampSecond(TimestampSecondEncoderBuilder),
    Date32(Date32EncoderBuilder),
    Date64(Date64EncoderBuilder),
    Time32Millisecond(Time32MillisecondEncoderBuilder),
    Time32Second(Time32SecondEncoderBuilder),
    Time64Microsecond(Time64MicrosecondEncoderBuilder),
    DurationMicrosecond(DurationMicrosecondEncoderBuilder),
    DurationMillisecond(DurationMillisecondEncoderBuilder),
    DurationSecond(DurationSecondEncoderBuilder),
    String(StringEncoderBuilder),
    LargeString(LargeStringEncoderBuilder),
    Binary(BinaryEncoderBuilder),
    LargeBinary(LargeBinaryEncoderBuilder),
    List(ListEncoderBuilder),
    LargeList(LargeListEncoderBuilder),
}

impl EncoderBuilder {
    pub(crate) fn try_new(data_type: &DataType, field: &str) -> Result<Self, Error> {
        let field = field.to_string();
        let res = match data_type {
            DataType::Boolean => Self::Boolean(BooleanEncoderBuilder { field }),
            DataType::UInt8 => Self::UInt8(UInt8EncoderBuilder { field }),
            DataType::UInt16 => Self::UInt16(UInt16EncoderBuilder { field }),
            DataType::UInt32 => Self::UInt32(UInt32EncoderBuilder { field }),
            DataType::Int8 => Self::Int8(Int8EncoderBuilder { field }),
            DataType::Int16 => Self::Int16(Int16EncoderBuilder { field }),
            DataType::Int32 => Self::Int32(Int32EncoderBuilder { field }),
            DataType::Int64 => Self::Int64(Int64EncoderBuilder { field }),
            DataType::Float16 => Self::Float16(Float16EncoderBuilder { field }),
            DataType::Float32 => Self::Float32(Float32EncoderBuilder { field }),
            DataType::Float64 => Self::Float64(Float64EncoderBuilder { field }),
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Nanosecond => {
                    return Err(Error::type_unsupported(
                        &field,
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
            DataType::Date64 => Self::Date64(Date64EncoderBuilder { field }),
            DataType::Time32(unit) => match unit {
                TimeUnit::Millisecond => {
                    Self::Time32Millisecond(Time32MillisecondEncoderBuilder { field })
                }
                TimeUnit::Second => Self::Time32Second(Time32SecondEncoderBuilder { field }),
                _ => unreachable!(),
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Nanosecond => {
                    return Err(Error::type_unsupported(
                        &field,
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
                    return Err(Error::type_unsupported(
                        &field,
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
            DataType::Utf8 => Self::String(StringEncoderBuilder { field }),
            DataType::LargeUtf8 => Self::LargeString(LargeStringEncoderBuilder { field }),
            DataType::Binary => Self::Binary(BinaryEncoderBuilder { field }),
            DataType::LargeBinary => Self::LargeBinary(LargeBinaryEncoderBuilder { field }),
            DataType::List(inner) => {
                let inner = Self::try_new(inner.data_type(), "field")?;
                Self::List(ListEncoderBuilder {
                    field,
                    inner_encoder_builder: Arc::new(inner),
                })
            }
            DataType::LargeList(inner) => {
                let inner = Self::try_new(inner.data_type(), "field")?;
                Self::LargeList(LargeListEncoderBuilder {
                    field,
                    inner_encoder_builder: Arc::new(inner),
                })
            }
            _ => return Err(Error::type_unsupported(&field, data_type, "unknown type")),
        };
        Ok(res)
    }
}
