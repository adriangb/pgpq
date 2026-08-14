//! Nested encoders: Postgres arrays (from Arrow lists) and composite types (from Arrow structs).
//!
//! Both build inner encoders per row (lists) or per batch (structs) and patch a length prefix back
//! into the buffer once the children have been written, so neither fits the scalar shape.

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::{Array, GenericListArray, OffsetSizeTrait, StructArray};
use arrow_schema::{DataType, Field};
use bytes::{BufMut, BytesMut};

use super::{downcast_checked, BuildEncoder, Encode, Encoder, EncoderBuilder};
use crate::error::ErrorKind;
use crate::pg_schema::{Column, PostgresType};

// ---------------------------------------------------------------------------------------------
// Lists
// ---------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct GenericListEncoder<'a, T: OffsetSizeTrait> {
    arr: &'a GenericListArray<T>,
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

#[derive(Debug, Clone, PartialEq)]
pub struct GenericListEncoderBuilder<T: OffsetSizeTrait> {
    field: Arc<Field>,
    inner_encoder_builder: Arc<EncoderBuilder>,
    offset: PhantomData<T>,
}

impl<T: OffsetSizeTrait> GenericListEncoderBuilder<T> {
    /// `List` for 32 bit offsets, `LargeList` for 64 bit ones.
    fn inner_field(data_type: &DataType) -> Option<&Arc<Field>> {
        match (T::IS_LARGE, data_type) {
            (false, DataType::List(inner)) => Some(inner),
            (true, DataType::LargeList(inner)) => Some(inner),
            _ => None,
        }
    }

    pub fn new(field: Arc<Field>) -> Result<Self, ErrorKind> {
        match Self::inner_field(field.data_type()) {
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
            offset: PhantomData,
        }
    }

    pub fn inner_encoder_builder(&self) -> EncoderBuilder {
        (*self.inner_encoder_builder).clone()
    }
}

impl<T> BuildEncoder for GenericListEncoderBuilder<T>
where
    T: OffsetSizeTrait,
    for<'a> GenericListEncoder<'a, T>: Into<Encoder<'a>>,
{
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
        let field = self.field.name().clone();
        Ok(GenericListEncoder {
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
            Ok(Self::unchecked(field, field_encoder_builders))
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

    pub fn inner_encoder_builder(&self) -> Vec<EncoderBuilder> {
        // Return a clone of the inner encoder builders
        self.field_encoder_builders.to_vec()
    }
}

impl BuildEncoder for StructEncoderBuilder {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, ErrorKind> {
        let arr: &'a StructArray = downcast_checked(arr, self.field.name())?;

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
