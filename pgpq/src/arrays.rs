use std::any::Any;


use arrow_array::{Array, ListArray, FixedSizeListArray, LargeListArray};
use arrow_schema::{DataType, TimeUnit, Field};

use crate::error::Error;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ArrowArray<'a> {
    Boolean(&'a arrow_array::BooleanArray),
    Int8(&'a arrow_array::Int8Array),
    Int16(&'a arrow_array::Int16Array),
    Int32(&'a arrow_array::Int32Array),
    Int64(&'a arrow_array::Int64Array),
    UInt8(&'a arrow_array::UInt8Array),
    UInt16(&'a arrow_array::UInt16Array),
    UInt32(&'a arrow_array::UInt32Array),
    Float16(&'a arrow_array::Float16Array),
    Float32(&'a arrow_array::Float32Array),
    Float64(&'a arrow_array::Float64Array),
    TimestampNanosecond(&'a arrow_array::TimestampNanosecondArray, bool),
    TimestampMillisecond(&'a arrow_array::TimestampMillisecondArray, bool),
    TimestampMicrosecond(&'a arrow_array::TimestampMicrosecondArray, bool),
    TimestampSecond(&'a arrow_array::TimestampSecondArray, bool),
    Date32(&'a arrow_array::Date32Array),
    Date64(&'a arrow_array::Date64Array),
    Time32Millisecond(&'a arrow_array::Time32MillisecondArray),
    Time32Second(&'a arrow_array::Time32SecondArray),
    Time64Nanosecond(&'a arrow_array::Time64NanosecondArray),
    Time64Microsecond(&'a arrow_array::Time64MicrosecondArray),
    DurationNanosecond(&'a arrow_array::DurationNanosecondArray),
    DurationMicrosecond(&'a arrow_array::DurationMicrosecondArray),
    DurationMillisecond(&'a arrow_array::DurationMillisecondArray),
    DurationSecond(&'a arrow_array::DurationSecondArray),
    Binary(&'a arrow_array::BinaryArray),
    LargeBinary(&'a arrow_array::LargeBinaryArray),
    String(&'a arrow_array::StringArray),
    LargeStringArray(&'a arrow_array::LargeStringArray),
    List(&'a arrow_array::ListArray, &'a DataType),
    FixedSizeList(&'a arrow_array::FixedSizeListArray, &'a DataType),
    LargeList(&'a arrow_array::LargeListArray, &'a DataType),
}

#[inline]
fn downcast_checked<'a, T: 'static>(
    arr: &'a dyn Array,
    field: &str,
    expected: &DataType,
) -> Result<&'a T, Error> {
    arr.as_any()
        .downcast_ref::<T>()
        .ok_or(Error::mismatched_column_type(
            field,
            expected,
            arr.data_type(),
        ))
}

#[inline]
fn downcast_list<'a, L>(inner: &'a Field, arr: &'a dyn Array, field: &str, outer_type: &DataType) -> Result<&'a L, Error>
where L: Any + 'a
{
    let arr:&'a L = match inner.data_type() {
        DataType::Boolean => downcast_checked(arr, field, outer_type)?,
        DataType::Int8 => downcast_checked(arr, field, outer_type)?,
        DataType::Int16 => downcast_checked(arr, field, outer_type)?,
        DataType::Int32 => downcast_checked(arr, field, outer_type)?,
        DataType::Int64 => downcast_checked(arr, field, outer_type)?,
        DataType::UInt8 => downcast_checked(arr, field, outer_type)?,
        DataType::UInt16 => downcast_checked(arr, field, outer_type)?,
        DataType::UInt32 => downcast_checked(arr, field, outer_type)?,
        DataType::Float16 => downcast_checked(arr, field, outer_type)?,
        DataType::Float32 => downcast_checked(arr, field, outer_type)?,
        DataType::Float64 => downcast_checked(arr, field, outer_type)?,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => downcast_checked(arr, field, outer_type)?,
        DataType::Timestamp(TimeUnit::Microsecond, _) => downcast_checked(arr, field, outer_type)?,
        DataType::Timestamp(TimeUnit::Millisecond, _) => downcast_checked(arr, field, outer_type)?,
        DataType::Timestamp(TimeUnit::Second, _) => downcast_checked(arr, field, outer_type)?,
        DataType::Date32 => downcast_checked(arr, field, outer_type)?,
        DataType::Date64 => downcast_checked(arr, field, outer_type)?,
        DataType::Time32(TimeUnit::Millisecond) => downcast_checked(arr, field, outer_type)?,
        DataType::Time32(TimeUnit::Second) => downcast_checked(arr, field, outer_type)?,
        DataType::Time64(TimeUnit::Nanosecond) => downcast_checked(arr, field, outer_type)?,
        DataType::Time64(TimeUnit::Microsecond) => downcast_checked(arr, field, outer_type)?,
        DataType::Duration(TimeUnit::Nanosecond) => downcast_checked(arr, field, outer_type)?,
        DataType::Duration(TimeUnit::Microsecond) => downcast_checked(arr, field, outer_type)?,
        DataType::Duration(TimeUnit::Millisecond) => downcast_checked(arr, field, outer_type)?,
        DataType::Duration(TimeUnit::Second) => downcast_checked(arr, field, outer_type)?,
        DataType::Binary => downcast_checked(arr, field, outer_type)?,
        DataType::LargeBinary => downcast_checked(arr, field, outer_type)?,
        DataType::Utf8 => downcast_checked(arr, field, outer_type)?,
        DataType::LargeUtf8 => downcast_checked(arr, field, outer_type)?,
        _ => return Err(Error::type_unsupported(field, outer_type)), // show the use the full type not the inner item type
    };
    Ok(arr)
}

#[inline]
pub(crate) fn downcast_array<'a>(
    type_: &'a DataType,
    arr: &'a dyn Array,
    field: &str,
) -> Result<ArrowArray<'a>, Error> {
    let arr = match type_ {
        DataType::Boolean => ArrowArray::Boolean(downcast_checked(arr, field, type_)?),
        DataType::Int8 => ArrowArray::Int8(downcast_checked(arr, field, type_)?),
        DataType::Int16 => ArrowArray::Int16(downcast_checked(arr, field, type_)?),
        DataType::Int32 => ArrowArray::Int32(downcast_checked(arr, field, type_)?),
        DataType::Int64 => ArrowArray::Int64(downcast_checked(arr, field, type_)?),
        DataType::UInt8 => ArrowArray::UInt8(downcast_checked(arr, field, type_)?),
        DataType::UInt16 => ArrowArray::UInt16(downcast_checked(arr, field, type_)?),
        DataType::UInt32 => ArrowArray::UInt32(downcast_checked(arr, field, type_)?),
        DataType::Float16 => ArrowArray::Float16(downcast_checked(arr, field, type_)?),
        DataType::Float32 => ArrowArray::Float32(downcast_checked(arr, field, type_)?),
        DataType::Float64 => ArrowArray::Float64(downcast_checked(arr, field, type_)?),
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => ArrowArray::TimestampNanosecond(downcast_checked(arr, field, type_)?, tz.is_some()),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => ArrowArray::TimestampMicrosecond(downcast_checked(arr, field, type_)?, tz.is_some()),
        DataType::Timestamp(TimeUnit::Millisecond, tz) => ArrowArray::TimestampMillisecond(downcast_checked(arr, field, type_)?, tz.is_some()),
        DataType::Timestamp(TimeUnit::Second, tz) => ArrowArray::TimestampSecond(downcast_checked(arr, field, type_)?, tz.is_some()),
        DataType::Date32 => ArrowArray::Date32(downcast_checked(arr, field, type_)?),
        DataType::Date64 => ArrowArray::Date64(downcast_checked(arr, field, type_)?),
        DataType::Time32(TimeUnit::Millisecond) => ArrowArray::Time32Millisecond(downcast_checked(arr, field, type_)?),
        DataType::Time32(TimeUnit::Second) => ArrowArray::Time32Second(downcast_checked(arr, field, type_)?),
        DataType::Time64(TimeUnit::Nanosecond) => ArrowArray::Time64Nanosecond(downcast_checked(arr, field, type_)?),
        DataType::Time64(TimeUnit::Microsecond) => ArrowArray::Time64Microsecond(downcast_checked(arr, field, type_)?),
        DataType::Duration(TimeUnit::Nanosecond) => ArrowArray::DurationNanosecond(downcast_checked(arr, field, type_)?),
        DataType::Duration(TimeUnit::Microsecond) => ArrowArray::DurationMicrosecond(downcast_checked(arr, field, type_)?),
        DataType::Duration(TimeUnit::Millisecond) => ArrowArray::DurationMillisecond(downcast_checked(arr, field, type_)?),
        DataType::Duration(TimeUnit::Second) => ArrowArray::DurationSecond(downcast_checked(arr, field, type_)?),
        DataType::Binary => ArrowArray::Binary(downcast_checked(arr, field, type_)?),
        DataType::LargeBinary => ArrowArray::LargeBinary(downcast_checked(arr, field, type_)?),
        DataType::Utf8 => ArrowArray::String(downcast_checked(arr, field, type_)?),
        DataType::LargeUtf8 => ArrowArray::LargeStringArray(downcast_checked(arr, field, type_)?),
        DataType::List(inner) => {
            let il: &ListArray = downcast_list(inner, arr, field, type_)?;
            ArrowArray::List(il, inner.data_type())
        },
        DataType::FixedSizeList(inner, _) => {
            let il: &FixedSizeListArray = downcast_list(inner, arr, field, type_)?;
            ArrowArray::FixedSizeList(il, inner.data_type())
        },
        DataType::LargeList(inner) => {
            let il: &LargeListArray = downcast_list(inner, arr, field, type_)?;
            ArrowArray::LargeList(il, inner.data_type())
        },
        tp => return Err(Error::type_unsupported(field, tp)),
    };
    Ok(arr)
}
