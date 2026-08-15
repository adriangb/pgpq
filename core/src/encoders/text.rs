//! Variable-length scalar encoders: `bytea`, and the three Arrow string layouts.
//!
//! These stay bespoke rather than folding into [`super::scalar`]: their length prefix is a
//! property of the value, they can overflow Postgres' `i32` field length (so they carry the field
//! name for the error), and the string encoders additionally choose between `text`, `json` and
//! `jsonb` output.

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::{
    Array, FixedSizeBinaryArray, GenericBinaryArray, GenericStringArray, OffsetSizeTrait,
    StringViewArray,
};
use arrow_schema::{DataType, Field};
use bytes::{BufMut, BytesMut};

use super::{downcast_checked, BuildEncoder, Encode, Encoder};
use crate::error::ErrorKind;
use crate::pg_schema::{Column, PostgresType};

// ---------------------------------------------------------------------------------------------
// Binary
// ---------------------------------------------------------------------------------------------

/// Reading a `&[u8]` out of any of Arrow's binary layouts, and which Arrow type that layout is.
///
/// The wire encoding is the same for all of them — Postgres' `bytea` is just a length prefix and
/// that many bytes — so this is all the generic encoder and builder below need to know. Note that
/// `FixedSizeBinary`, unlike the two offset-based layouts, has no offsets buffer at all: every
/// value is `byte_width` bytes at a fixed stride, which `value` hides.
pub trait GenericBinArray: Array + 'static {
    /// Name reported when a builder is handed a field it cannot encode. Kept equal to the public
    /// builder alias so the error names the type the caller asked for.
    const ENCODER_NAME: &'static str;
    /// Whether a field of this Arrow type can be encoded by this layout.
    fn accepts(data_type: &DataType) -> bool;
    fn value(&self, row: usize) -> &[u8];
}

impl<T: OffsetSizeTrait> GenericBinArray for GenericBinaryArray<T> {
    /// `Binary` for 32 bit offsets, `LargeBinary` for 64 bit ones.
    const ENCODER_NAME: &'static str = if T::IS_LARGE {
        "LargeBinaryEncoderBuilder"
    } else {
        "BinaryEncoderBuilder"
    };
    fn accepts(data_type: &DataType) -> bool {
        if T::IS_LARGE {
            matches!(data_type, DataType::LargeBinary)
        } else {
            matches!(data_type, DataType::Binary)
        }
    }
    fn value(&self, row: usize) -> &[u8] {
        self.value(row)
    }
}

impl GenericBinArray for FixedSizeBinaryArray {
    const ENCODER_NAME: &'static str = "FixedSizeBinaryEncoderBuilder";
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::FixedSizeBinary(_))
    }
    fn value(&self, row: usize) -> &[u8] {
        self.value(row)
    }
}

#[derive(Debug)]
pub struct GenericBinaryEncoder<'a, T: GenericBinArray> {
    arr: &'a T,
    field: String,
}

impl<T: GenericBinArray> Encode for GenericBinaryEncoder<'_, T> {
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

/// Builder for any [`GenericBinArray`].
///
/// `Debug`, `Clone` and `PartialEq` are written out rather than derived: a derive would put a
/// `T: Debug + Clone + PartialEq` bound on the *array* type, which the builder never holds — the
/// `PhantomData` is a `fn() -> T` precisely so that it constrains nothing.
pub struct GenericBinaryEncoderBuilder<T: GenericBinArray> {
    field: Arc<Field>,
    array: PhantomData<fn() -> T>,
}

impl<T: GenericBinArray> std::fmt::Debug for GenericBinaryEncoderBuilder<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(T::ENCODER_NAME)
            .field("field", &self.field)
            .finish()
    }
}

impl<T: GenericBinArray> Clone for GenericBinaryEncoderBuilder<T> {
    fn clone(&self) -> Self {
        Self::unchecked(self.field.clone())
    }
}

impl<T: GenericBinArray> PartialEq for GenericBinaryEncoderBuilder<T> {
    fn eq(&self, other: &Self) -> bool {
        self.field == other.field
    }
}

impl<T: GenericBinArray> GenericBinaryEncoderBuilder<T> {
    pub fn new(field: Arc<Field>) -> Result<Self, ErrorKind> {
        if !T::accepts(field.data_type()) {
            return Err(ErrorKind::FieldTypeNotSupported {
                encoder: T::ENCODER_NAME.to_string(),
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
            array: PhantomData,
        }
    }
}

impl<T> BuildEncoder for GenericBinaryEncoderBuilder<T>
where
    T: GenericBinArray,
    for<'a> GenericBinaryEncoder<'a, T>: Into<Encoder<'a>>,
{
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
        let field = self.field.name();
        Ok(GenericBinaryEncoder::<T> {
            arr: downcast_checked(arr, field)?,
            field: field.to_string(),
        }
        .into())
    }

    fn schema(&self) -> Column {
        Column {
            name: self.field.name().clone(),
            data_type: PostgresType::Bytea,
            nullable: self.field.is_nullable(),
        }
    }

    fn field(&self) -> Arc<Field> {
        self.field.clone()
    }
}

// ---------------------------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------------------------

/// Reading a `&str` out of any of Arrow's string layouts.
pub trait GenericStrArray: Array + 'static {
    fn value(&self, row: usize) -> &str;
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

/// Which Arrow string layout a builder reads, and what to call it in errors.
pub trait StrConversion: std::fmt::Debug + Clone + PartialEq + 'static {
    type Array: GenericStrArray;
    const ENCODER_NAME: &'static str;
    fn accepts(data_type: &DataType) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringConversion;
impl StrConversion for StringConversion {
    type Array = GenericStringArray<i32>;
    const ENCODER_NAME: &'static str = "StringEncoderBuilder";
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Utf8)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LargeStringConversion;
impl StrConversion for LargeStringConversion {
    type Array = GenericStringArray<i64>;
    const ENCODER_NAME: &'static str = "LargeStringEncoderBuilder";
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::LargeUtf8)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringViewConversion;
impl StrConversion for StringViewConversion {
    type Array = StringViewArray;
    const ENCODER_NAME: &'static str = "StringViewEncoderBuilder";
    fn accepts(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Utf8View)
    }
}

/// The Postgres types a string column may be written as.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum StringOutputType {
    Text,
    Json,
    Jsonb,
}

impl StringOutputType {
    fn from_postgres_type(tp: PostgresType, field: &Field) -> Result<Self, ErrorKind> {
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

    fn postgres_datatype(&self) -> PostgresType {
        match self {
            StringOutputType::Text => PostgresType::Text,
            StringOutputType::Json => PostgresType::Json,
            StringOutputType::Jsonb => PostgresType::Jsonb,
        }
    }
}

#[derive(Debug)]
pub struct GenericStrEncoder<'a, T: GenericStrArray> {
    arr: &'a T,
    field: String,
    output: StringOutputType,
}

impl<T: GenericStrArray> Encode for GenericStrEncoder<'_, T> {
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

#[derive(Debug, Clone, PartialEq)]
pub struct StrEncoderBuilder<C: StrConversion> {
    field: Arc<Field>,
    output: StringOutputType,
    conversion: PhantomData<C>,
}

impl<C: StrConversion> StrEncoderBuilder<C> {
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

    pub fn new_with_output(field: Arc<Field>, output: PostgresType) -> Result<Self, ErrorKind> {
        let output = StringOutputType::from_postgres_type(output, &field)?;
        Ok(Self {
            field,
            output,
            conversion: PhantomData,
        })
    }

    /// Build without checking the field's Arrow type, for callers that have already matched on it.
    pub(super) fn unchecked(field: Arc<Field>) -> Self {
        Self {
            field,
            output: StringOutputType::Text,
            conversion: PhantomData,
        }
    }
}

impl<C> BuildEncoder for StrEncoderBuilder<C>
where
    C: StrConversion,
    for<'a> GenericStrEncoder<'a, C::Array>: Into<Encoder<'a>>,
{
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
        let field = self.field.name();
        Ok(GenericStrEncoder::<C::Array> {
            arr: downcast_checked(arr, field)?,
            field: field.clone(),
            output: self.output.clone(),
        }
        .into())
    }

    fn schema(&self) -> Column {
        Column {
            name: self.field.name().clone(),
            data_type: self.output.postgres_datatype(),
            nullable: self.field.is_nullable(),
        }
    }

    fn field(&self) -> Arc<Field> {
        self.field.clone()
    }
}
