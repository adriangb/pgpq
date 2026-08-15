//! Nested encoders: Postgres arrays (from Arrow lists) and composite types (from Arrow structs).
//!
//! Both build inner encoders per row (lists) or per batch (structs) and patch a length prefix back
//! into the buffer once the children have been written, so neither fits the scalar shape.

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, GenericListArray, OffsetSizeTrait, StructArray,
};
use arrow_schema::{DataType, Field};
use bytes::BytesMut;

use super::{downcast_checked, put, BuildEncoder, Encode, Encoder, EncoderBuilder};
use crate::error::ErrorKind;
use crate::pg_schema::{Column, PostgresType};

// ---------------------------------------------------------------------------------------------
// Lists
// ---------------------------------------------------------------------------------------------

/// One Arrow list layout: how to read the values of a row, and which Arrow type it is.
///
/// All three of Arrow's list layouts become the same thing in Postgres — a one dimensional array —
/// and the only thing that differs is how a row's slice of the child array is located: from an
/// `i32` or `i64` offsets buffer, or (for `FixedSizeList`) from a constant stride, with no offsets
/// buffer at all.
pub trait GenericListArrayValues: Array + 'static {
    /// Name reported when a builder is handed a field it cannot encode. Kept equal to the public
    /// builder alias so the error names the type the caller asked for.
    const ENCODER_NAME: &'static str;
    /// The element field of `data_type`, if `data_type` is the list type this layout reads.
    fn inner_field(data_type: &DataType) -> Option<&Arc<Field>>;
    /// The values of row `row`, as an array of exactly that row's length.
    fn value(&self, row: usize) -> ArrayRef;
}

impl<T: OffsetSizeTrait> GenericListArrayValues for GenericListArray<T> {
    const ENCODER_NAME: &'static str = if T::IS_LARGE {
        "LargeListEncoderBuilder"
    } else {
        "ListEncoderBuilder"
    };
    fn inner_field(data_type: &DataType) -> Option<&Arc<Field>> {
        match (T::IS_LARGE, data_type) {
            (false, DataType::List(inner)) => Some(inner),
            (true, DataType::LargeList(inner)) => Some(inner),
            _ => None,
        }
    }
    fn value(&self, row: usize) -> ArrayRef {
        self.value(row)
    }
}

impl GenericListArrayValues for FixedSizeListArray {
    const ENCODER_NAME: &'static str = "FixedSizeListEncoderBuilder";
    fn inner_field(data_type: &DataType) -> Option<&Arc<Field>> {
        match data_type {
            DataType::FixedSizeList(inner, _) => Some(inner),
            _ => None,
        }
    }
    fn value(&self, row: usize) -> ArrayRef {
        self.value(row)
    }
}

#[derive(Debug)]
pub struct GenericListEncoder<'a, T: GenericListArrayValues> {
    arr: &'a T,
    field: String,
    inner_encoder_builder: Arc<EncoderBuilder>,
}

impl<T: GenericListArrayValues> Encode for GenericListEncoder<'_, T> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), ErrorKind> {
        if self.arr.is_null(row) {
            put(buf, (-1i32).to_be_bytes());
        } else {
            let val = self.arr.value(row);
            let inner_encoder = self.inner_encoder_builder.try_new(&val)?;

            // Postgres checks the element OID against the column's element type, so it has to be
            // the real one. It is only `None` for a hand built encoder whose element is itself an
            // array; `EncoderBuilder::try_new` rejects those up front.
            let inner_type = self.inner_encoder_builder.schema().data_type;
            let inner_tp_oid = inner_type.oid().ok_or_else(|| ErrorKind::Encode {
                reason: format!(
                    "element type {inner_type:?} of array column {} has no Postgres OID",
                    self.field
                ),
            })?;

            let base_idx = buf.len();
            put(buf, (0i32).to_be_bytes()); // the total number of bytes this element takes up, insert later
            put(buf, (1i32).to_be_bytes()); // num dimensions, we only support 1
                                            // nulls flag, true if any item is null
            put(buf, ((val.null_count() != 0) as i32).to_be_bytes());
            put(buf, (inner_tp_oid as i32).to_be_bytes());
            // put the dimension length
            put(buf, (val.len() as i32).to_be_bytes());
            // put the dimension lower bound, always 1
            put(buf, (1i32).to_be_bytes());

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

/// Builder for any [`GenericListArrayValues`].
///
/// See [`super::GenericBinaryEncoderBuilder`] for why the three impls below are written out rather
/// than derived.
pub struct GenericListEncoderBuilder<T: GenericListArrayValues> {
    field: Arc<Field>,
    inner_encoder_builder: Arc<EncoderBuilder>,
    array: PhantomData<fn() -> T>,
}

impl<T: GenericListArrayValues> std::fmt::Debug for GenericListEncoderBuilder<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(T::ENCODER_NAME)
            .field("field", &self.field)
            .field("inner_encoder_builder", &self.inner_encoder_builder)
            .finish()
    }
}

impl<T: GenericListArrayValues> Clone for GenericListEncoderBuilder<T> {
    fn clone(&self) -> Self {
        Self {
            field: self.field.clone(),
            inner_encoder_builder: self.inner_encoder_builder.clone(),
            array: PhantomData,
        }
    }
}

impl<T: GenericListArrayValues> PartialEq for GenericListEncoderBuilder<T> {
    fn eq(&self, other: &Self) -> bool {
        self.field == other.field && self.inner_encoder_builder == other.inner_encoder_builder
    }
}

impl<T: GenericListArrayValues> GenericListEncoderBuilder<T> {
    pub fn new(field: Arc<Field>) -> Result<Self, ErrorKind> {
        match T::inner_field(field.data_type()) {
            Some(inner) => {
                let inner_encoder_builder = EncoderBuilder::try_new(inner.clone())?;
                Ok(Self::unchecked(field, inner_encoder_builder))
            }
            None => Err(ErrorKind::type_unsupported(
                field.name(),
                field.data_type(),
                format!("{:?} is not a list type", field.data_type()).as_str(),
            )),
        }
    }

    pub fn new_with_inner(
        field: Arc<Field>,
        inner_encoder_builder: EncoderBuilder,
    ) -> Result<Self, ErrorKind> {
        Ok(Self::unchecked(field, inner_encoder_builder))
    }

    /// Build without checking the field's Arrow type, for callers that have already matched on it.
    pub(super) fn unchecked(field: Arc<Field>, inner_encoder_builder: EncoderBuilder) -> Self {
        Self {
            field,
            inner_encoder_builder: Arc::new(inner_encoder_builder),
            array: PhantomData,
        }
    }

    pub fn inner_encoder_builder(&self) -> EncoderBuilder {
        (*self.inner_encoder_builder).clone()
    }
}

impl<T> BuildEncoder for GenericListEncoderBuilder<T>
where
    T: GenericListArrayValues,
    for<'a> GenericListEncoder<'a, T>: Into<Encoder<'a>>,
{
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
        let field = self.field.name().clone();
        Ok(GenericListEncoder::<T> {
            arr: downcast_checked(arr, &field)?,
            field,
            inner_encoder_builder: self.inner_encoder_builder.clone(),
        }
        .into())
    }

    fn schema(&self) -> Column {
        Column {
            name: self.field.name().clone(),
            data_type: PostgresType::List(Box::new(self.inner_encoder_builder.schema())),
            nullable: self.field.is_nullable(),
        }
    }

    fn field(&self) -> Arc<Field> {
        self.field.clone()
    }
}

// ---------------------------------------------------------------------------------------------
// Structs
// ---------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct StructEncoder<'a> {
    arr: &'a StructArray,
    field: String,
    field_encoders: Vec<Encoder<'a>>,
    field_oids: Vec<u32>,
}

impl Encode for StructEncoder<'_> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), ErrorKind> {
        if self.arr.is_null(row) {
            put(buf, (-1i32).to_be_bytes());
        } else {
            let base_idx = buf.len();
            put(buf, (0i32).to_be_bytes()); // Placeholder for the total size

            // Put the number of fields
            put(buf, (self.field_encoders.len() as i32).to_be_bytes());

            for (encoder, oid) in self.field_encoders.iter().zip(&self.field_oids) {
                put(buf, (*oid).to_be_bytes());
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
            let builder = Self::unchecked(field, field_encoder_builders);
            // Fail at schema time rather than once per batch.
            builder.field_oids()?;
            Ok(builder)
        } else {
            Err(ErrorKind::FieldTypeNotSupported {
                encoder: "StructEncoder".to_string(),
                tp: field.data_type().clone(),
                field: field.name().clone(),
            })
        }
    }

    /// Build without checking the field's Arrow type, for callers that have already matched on it.
    pub(super) fn unchecked(
        field: Arc<Field>,
        field_encoder_builders: Vec<EncoderBuilder>,
    ) -> Self {
        Self {
            field,
            field_encoder_builders,
        }
    }

    /// The OID Postgres expects for each field of the composite type.
    ///
    /// Postgres' `record_recv` compares the OID written for every field against the composite's
    /// declared column type, so these have to be real: an array field needs the array type's OID
    /// (`_int4` = 1007 for `int4[]`), not the element's. Types whose OID is only known to the
    /// server — an array of composites, or an array of arrays — have no answer here, and are
    /// reported as unsupported rather than encoded with something Postgres would reject.
    pub(super) fn field_oids(&self) -> Result<Vec<u32>, ErrorKind> {
        self.field_encoder_builders
            .iter()
            .map(|builder| {
                let column = builder.schema();
                column.data_type.oid().ok_or_else(|| {
                    ErrorKind::type_unsupported(
                        self.field.name(),
                        self.field.data_type(),
                        &format!(
                            "field {} maps to {:?}, which has no Postgres OID; \
                             a composite type cannot contain an array of composites or of arrays",
                            column.name, column.data_type
                        ),
                    )
                })
            })
            .collect()
    }

    pub fn inner_encoder_builder(&self) -> Vec<EncoderBuilder> {
        // Return a clone of the inner encoder builders
        self.field_encoder_builders.to_vec()
    }
}

impl BuildEncoder for StructEncoderBuilder {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
        let arr: &'a StructArray = downcast_checked(arr, self.field.name())?;

        // Build encoders for each field at build time and collect OIDs
        let field_oids = self.field_oids()?;
        let mut field_encoders = Vec::with_capacity(self.field_encoder_builders.len());

        for (field, encoder_builder) in arr.columns().iter().zip(&self.field_encoder_builders) {
            field_encoders.push(encoder_builder.try_new(field)?);
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
