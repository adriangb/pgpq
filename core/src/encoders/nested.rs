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

use super::{BuildEncoder, Encode, Encoder, EncoderBuilder, downcast_checked, put};
use crate::error::ErrorKind;
use crate::pg_schema::{Column, PostgresType};

/// Bytes every field spends on its `i32` length prefix, null or not.
const LENGTH_PREFIX: usize = 4;
/// The five `i32`s that precede an array's elements: dimension count, null flag, element oid,
/// dimension length and dimension lower bound.
const ARRAY_HEADER: usize = 5 * 4;
/// The `i32` field count that precedes a composite's fields, and the `u32` oid in front of each.
const COMPOSITE_HEADER: usize = 4;
const COMPOSITE_FIELD_HEADER: usize = 4;

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
        // A null row is just the `-1` length prefix; a present one is that prefix plus the array
        // header `encode` writes above (dimension count, null flag, element oid, dimension length
        // and lower bound) plus the elements themselves.
        let mut total = LENGTH_PREFIX * self.arr.len();
        for row in 0..self.arr.len() {
            if !self.arr.is_null(row) {
                let val = self.arr.value(row);
                let inner_encoder = self.inner_encoder_builder.try_new(&val)?;
                total += ARRAY_HEADER + inner_encoder.byte_size_hint()?;
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
    /// The element builder, mutably, so a caller can walk into it.
    ///
    /// Uses `Arc::make_mut`, so a builder shared with a clone is copied first rather than mutated
    /// underneath it.
    pub(crate) fn inner_encoder_builder_mut(&mut self) -> &mut EncoderBuilder {
        Arc::make_mut(&mut self.inner_encoder_builder)
    }

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
        // The children's hints cover the whole column, but the length prefix, the field count and
        // each field's oid are written once *per row*.
        let per_row =
            LENGTH_PREFIX + COMPOSITE_HEADER + COMPOSITE_FIELD_HEADER * self.field_encoders.len();
        let mut total = per_row * self.arr.len();
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
    /// This composite's own OID in the target database, if the caller supplied one.
    ///
    /// Only consulted when this composite is a *field of another composite*, which is the one
    /// place a composite's OID goes on the wire. A top-level composite column never needs it:
    /// binary COPY declares no column types.
    oid: Option<u32>,
}

impl StructEncoderBuilder {
    pub fn new(field: Arc<Field>) -> Result<Self, ErrorKind> {
        if let DataType::Struct(fields) = field.data_type() {
            let field_encoder_builders = fields
                .iter()
                .map(|f| EncoderBuilder::try_new(f.clone()))
                .collect::<Result<Vec<_>, _>>()?;
            let builder = Self::unchecked(field, field_encoder_builders);
            // Fail at schema time rather than once per batch — but only for shapes that can
            // never work. A nested composite whose OID has not been supplied yet is not one of
            // those: the caller sets it with `with_oid` after building the tree.
            builder.check_field_types_encodable()?;
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
            oid: None,
        }
    }

    /// Declare this composite's OID in the database being loaded.
    ///
    /// Required only when the composite is nested inside another composite. Postgres allocates
    /// composite OIDs when the type is created, so the value has to come from the server:
    ///
    /// ```sql
    /// select oid from pg_type where typname = 'my_struct_t';
    /// ```
    ///
    /// [`ArrowToPostgresBinaryEncoder::with_composite_oids`] applies a whole map of these at
    /// once, keyed by the type name the generated DDL uses.
    ///
    /// [`ArrowToPostgresBinaryEncoder::with_composite_oids`]: crate::ArrowToPostgresBinaryEncoder::with_composite_oids
    pub fn with_oid(mut self, oid: u32) -> Self {
        self.oid = Some(oid);
        self
    }

    /// [`Self::with_oid`] for a builder held behind a `&mut`, as the tree walk in
    /// [`ArrowToPostgresBinaryEncoder::with_composite_oids`] has.
    ///
    /// [`ArrowToPostgresBinaryEncoder::with_composite_oids`]: crate::ArrowToPostgresBinaryEncoder::with_composite_oids
    pub(crate) fn set_oid(&mut self, oid: u32) {
        self.oid = Some(oid);
    }

    /// This composite's declared OID, if one has been supplied.
    pub fn oid(&self) -> Option<u32> {
        self.oid
    }

    /// The builders for this composite's fields, so a caller can walk the tree.
    pub(crate) fn field_encoder_builders_mut(&mut self) -> &mut Vec<EncoderBuilder> {
        &mut self.field_encoder_builders
    }

    /// The name the generated DDL gives this composite's type.
    pub(crate) fn type_name(&self) -> String {
        format!("{}_t", self.field.name())
    }

    /// Reject field types that can never carry an OID, whatever the caller does.
    ///
    /// A composite field's OID goes on the wire, and Postgres' `record_recv` checks it. Some
    /// types have no answer at all — an array of composites, or an array of arrays, neither of
    /// which Postgres has a distinct type for — and those are rejected when the builder is
    /// created rather than once per batch. A *composite* field is fine here even with no OID yet:
    /// the caller supplies it with [`Self::with_oid`] after the tree exists.
    pub(super) fn check_field_types_encodable(&self) -> Result<(), ErrorKind> {
        for builder in &self.field_encoder_builders {
            let column = builder.schema();
            let missing_oid = column.data_type.oid().is_none();
            let is_composite = matches!(column.data_type, PostgresType::UserDefined { .. });
            if missing_oid && !is_composite {
                return Err(ErrorKind::type_unsupported(
                    self.field.name(),
                    self.field.data_type(),
                    &format!(
                        "field {} maps to {:?}, which has no Postgres OID; \
                         a composite type cannot contain an array of composites or of arrays",
                        column.name, column.data_type
                    ),
                ));
            }
        }
        Ok(())
    }

    /// The OID Postgres expects for each field of the composite type.
    ///
    /// Postgres' `record_recv` compares the OID written for every field against the composite's
    /// declared column type, so these have to be real: an array field needs the array type's OID
    /// (`_int4` = 1007 for `int4[]`), not the element's.
    ///
    /// A composite field's OID is allocated by the server, so pgpq can only know it if the caller
    /// says so — see [`Self::with_oid`] and
    /// [`ArrowToPostgresBinaryEncoder::with_composite_oids`]. Encoding without it would put a
    /// guess on the wire, which is exactly the bug this replaced.
    ///
    /// [`ArrowToPostgresBinaryEncoder::with_composite_oids`]: crate::ArrowToPostgresBinaryEncoder::with_composite_oids
    pub(super) fn field_oids(&self) -> Result<Vec<u32>, ErrorKind> {
        self.field_encoder_builders
            .iter()
            .map(|builder| {
                let column = builder.schema();
                if let Some(oid) = column.data_type.oid() {
                    return Ok(oid);
                }
                let reason = match &column.data_type {
                    PostgresType::UserDefined { .. } => format!(
                        "field {} is a composite type whose OID in the target database is \
                         unknown; supply it with `with_composite_oids` (look it up with \
                         `select oid from pg_type where typname = '{}_t'`)",
                        column.name, column.name
                    ),
                    other => format!(
                        "field {} maps to {other:?}, which has no Postgres OID; \
                         a composite type cannot contain an array of composites or of arrays",
                        column.name
                    ),
                };
                Err(ErrorKind::type_unsupported(
                    self.field.name(),
                    self.field.data_type(),
                    &reason,
                ))
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
                oid: self.oid,
            },
            nullable: self.field.is_nullable(),
        }
    }

    fn field(&self) -> Arc<Field> {
        self.field.clone()
    }
}
