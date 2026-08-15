use std::collections::{HashMap, HashSet};

use arrow_array::RecordBatch;
use arrow_schema::Fields;
use arrow_schema::Schema;
use bytes::{BufMut, BytesMut};
use error::ErrorKind;

pub mod encoders;
pub mod error;
pub mod pg_schema;

use crate::encoders::{BuildEncoder, Encode, EncoderBuilder};
use crate::pg_schema::PostgresSchema;

const HEADER_MAGIC_BYTES: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// Where an [`ArrowToPostgresBinaryEncoder`] is in the `header -> batches -> footer` sequence.
///
/// Reported by [`ErrorKind::EncoderStateError`] when a call arrives out of order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncoderState {
    /// Nothing has been written yet; the header comes next.
    Created,
    /// The header has been written; batches and then the footer may follow.
    Encoding,
    /// The footer has been written; nothing more may be encoded.
    Finished,
}

impl std::fmt::Display for EncoderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            EncoderState::Created => "created",
            EncoderState::Encoding => "encoding",
            EncoderState::Finished => "finished",
        };
        f.write_str(name)
    }
}

/// Set composite OIDs from `oids` on `builder` and everything nested inside it, recording which
/// keys were used.
///
/// Only composites are walked: a Postgres array of composites has no OID of its own, so
/// `EncoderBuilder::try_new` rejects that shape outright and a list can never hold one.
fn apply_composite_oids(
    builder: &mut EncoderBuilder,
    oids: &HashMap<String, u32>,
    applied: &mut HashSet<String>,
) {
    match builder {
        EncoderBuilder::Struct(struct_builder) => {
            let name = struct_builder.type_name();
            if let Some(oid) = oids.get(&name) {
                applied.insert(name);
                struct_builder.set_oid(*oid);
            }
            for inner in struct_builder.field_encoder_builders_mut() {
                apply_composite_oids(inner, oids, applied);
            }
        }
        // An array of composites writes its element type's OID into the array header, so the
        // element needs one too.
        EncoderBuilder::List(list) => {
            apply_composite_oids(list.inner_encoder_builder_mut(), oids, applied)
        }
        EncoderBuilder::LargeList(list) => {
            apply_composite_oids(list.inner_encoder_builder_mut(), oids, applied)
        }
        EncoderBuilder::FixedSizeList(list) => {
            apply_composite_oids(list.inner_encoder_builder_mut(), oids, applied)
        }
        _ => {}
    }
}

#[derive(Debug)]
pub struct ArrowToPostgresBinaryEncoder {
    fields: Fields,
    state: EncoderState,
    encoder_builders: Vec<EncoderBuilder>,
}

/// Build the default encoder for every field of `fields`, paired with the field's name.
///
/// The names come back with the builders because that is the key
/// [`ArrowToPostgresBinaryEncoder::try_new_with_encoders`] expects: the usual flow is to take the
/// defaults, replace the few columns that need a non-default Postgres type (a `Utf8` column
/// written as `jsonb`, say) and hand the whole set back.
///
/// This fails on the first field pgpq cannot encode. No per-field `Result` is returned because
/// every [`ErrorKind`] already names the field it is about.
pub fn build_encoders(
    fields: &arrow_schema::Fields,
) -> Result<Vec<(String, EncoderBuilder)>, ErrorKind> {
    fields
        .iter()
        .map(|f| Ok((f.name().clone(), EncoderBuilder::try_new(f.clone())?)))
        .collect()
}

impl ArrowToPostgresBinaryEncoder {
    /// Creates a new writer which will write rows of the provided types to the provided sink.
    pub fn try_new(schema: &Schema) -> Result<Self, ErrorKind> {
        let fields = schema.fields();
        let encoder_builders = build_encoders(fields)?
            .into_iter()
            .map(|(_, encoder)| encoder)
            .collect();

        Ok(ArrowToPostgresBinaryEncoder {
            fields: fields.clone(),
            state: EncoderState::Created,
            encoder_builders,
        })
    }

    pub fn try_new_with_encoders(
        schema: &Schema,
        encoders: &HashMap<String, EncoderBuilder>,
    ) -> Result<Self, ErrorKind> {
        let mut encoders = encoders.clone();
        let maybe_encoder_builders: Result<Vec<EncoderBuilder>, ErrorKind> = schema
            .fields()
            .iter()
            .map(|f| {
                encoders.remove(f.name()).map_or_else(
                    || {
                        Err(ErrorKind::EncoderMissing {
                            field: f.name().to_string(),
                        })
                    },
                    Ok,
                )
            })
            .collect();
        if !encoders.is_empty() {
            return Err(ErrorKind::UnknownFields {
                fields: encoders.keys().cloned().collect(),
            });
        }
        Ok(ArrowToPostgresBinaryEncoder {
            fields: schema.fields.clone(),
            state: EncoderState::Created,
            encoder_builders: maybe_encoder_builders?,
        })
    }

    /// The composite type names this encoder needs OIDs for, outermost first.
    ///
    /// These are the names [`PostgresSchema::ddl`] creates, so the usual flow is: run the DDL,
    /// look these names up in `pg_type`, and hand the result to [`Self::with_composite_oids`].
    ///
    /// Every composite is listed, including top-level struct columns that do not strictly need an
    /// OID — supplying one for those is harmless, and leaving them out of the list would make it
    /// a poor answer to "which types did my DDL create?".
    pub fn composite_type_names(&self) -> Vec<String> {
        fn walk(builder: &EncoderBuilder, out: &mut Vec<String>) {
            match builder {
                EncoderBuilder::Struct(s) => {
                    out.push(s.type_name());
                    for inner in s.field_builders() {
                        walk(inner, out);
                    }
                }
                EncoderBuilder::List(l) => walk(l.inner_builder(), out),
                EncoderBuilder::LargeList(l) => walk(l.inner_builder(), out),
                EncoderBuilder::FixedSizeList(l) => walk(l.inner_builder(), out),
                _ => {}
            }
        }

        let mut out = Vec::new();
        for builder in &self.encoder_builders {
            walk(builder, &mut out);
        }
        out
    }

    /// Supply the OIDs of the composite types this encoder will write, keyed by the type name
    /// the generated DDL uses (`<field>_t`).
    ///
    /// Postgres allocates a composite type's OID when the type is created, so pgpq cannot know
    /// it. The OID matters in exactly one place — a composite nested inside another composite
    /// writes its OID into the outer composite's field header, and `record_recv` checks it — so
    /// this is only required for nested structs. Everything else encodes without it.
    ///
    /// The usual flow is: run [`PostgresSchema::ddl`], then ask the server what it created.
    ///
    /// ```sql
    /// select typname, oid from pg_type where typname in ('inner_t', 'outer_t');
    /// ```
    ///
    /// Names that do not match any composite in this encoder are reported rather than ignored, so
    /// a typo in a type name fails here instead of silently leaving the wrong OID in place.
    pub fn with_composite_oids(mut self, oids: &HashMap<String, u32>) -> Result<Self, ErrorKind> {
        let mut applied: HashSet<String> = HashSet::new();
        for builder in &mut self.encoder_builders {
            apply_composite_oids(builder, oids, &mut applied);
        }
        let unknown: Vec<String> = oids
            .keys()
            .filter(|name| !applied.contains(*name))
            .cloned()
            .collect();
        if !unknown.is_empty() {
            let mut unknown = unknown;
            unknown.sort();
            return Err(ErrorKind::UnknownFields { fields: unknown });
        }
        Ok(self)
    }

    pub fn schema(&self) -> PostgresSchema {
        PostgresSchema {
            columns: self
                .encoder_builders
                .iter()
                .map(|builder| builder.schema())
                .collect(),
        }
    }

    /// Fail unless the encoder is in `expected`.
    ///
    /// The `header -> batches -> footer` sequence is a caller-visible contract, so breaking it is
    /// an error value rather than a panic: a library has no business aborting its host process
    /// over a misuse it can describe.
    fn expect_state(&self, expected: EncoderState) -> Result<(), ErrorKind> {
        if self.state != expected {
            return Err(ErrorKind::EncoderStateError {
                expected,
                actual: self.state,
            });
        }
        Ok(())
    }

    pub fn write_header(&mut self, out: &mut BytesMut) -> Result<(), ErrorKind> {
        self.expect_state(EncoderState::Created)?;
        out.put(HEADER_MAGIC_BYTES);
        out.put_i32(0); // flags
        out.put_i32(0); // header extension
        self.state = EncoderState::Encoding;
        Ok(())
    }

    pub fn write_batch(
        &mut self,
        batch: &RecordBatch,
        buf: &mut BytesMut,
    ) -> Result<(), ErrorKind> {
        self.expect_state(EncoderState::Encoding)?;
        if batch.num_columns() != self.fields.len() {
            return Err(ErrorKind::ColumnCountMismatch {
                expected: self.fields.len(),
                actual: batch.num_columns(),
            });
        }
        let n_rows = batch.num_rows();
        let n_cols = batch.num_columns();

        let encoders = batch
            .columns()
            .iter()
            .zip(&self.encoder_builders)
            .map(|(col, builder)| builder.try_new(col))
            .collect::<Result<Vec<_>, _>>()?;

        // Every row is prefixed with the same column count; render it once.
        let row_header = (n_cols as i16).to_be_bytes();

        let mut required_size: usize = n_rows * row_header.len();
        for encoder in &encoders {
            required_size += encoder.byte_size_hint()?
        }
        buf.reserve(required_size);

        for row in 0..n_rows {
            encoders::put(buf, row_header);
            for encoder in &encoders {
                encoder.encode(row, buf)?
            }
        }
        Ok(())
    }

    pub fn write_footer(&mut self, out: &mut BytesMut) -> Result<(), ErrorKind> {
        self.expect_state(EncoderState::Encoding)?;
        out.put_i16(-1);
        self.state = EncoderState::Finished;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::{encoders::StringEncoderBuilder, pg_schema::Column};

    use super::*;
    use arrow_array::{Int8Array, Int32Array, StringArray};
    use arrow_schema::{DataType, Field};

    fn make_test_data() -> RecordBatch {
        let int32_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let int8_array = Int8Array::from(vec![1, 2, 3, 4, 5]);
        let string_array = StringArray::from(vec!["a", "b", "c", "d", "e"]);
        let json_array = StringArray::from(vec!["\"a\"", "[]", "{\"f\":123}", "1", "{}"]);

        let schema = Schema::new(vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("int8", DataType::Int8, false),
            Field::new("string", DataType::Utf8, false),
            Field::new("json", DataType::Utf8, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(int32_array),
                Arc::new(int8_array),
                Arc::new(string_array),
                Arc::new(json_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_build_with_encoders() {
        let batch = make_test_data();
        let encoders = build_encoders(batch.schema().fields()).unwrap();
        let encoders: HashMap<String, EncoderBuilder> = encoders
            .into_iter()
            .map(|(field_name, encoder)| match field_name.as_str() {
                "json" => (
                    field_name.to_string(),
                    EncoderBuilder::String(
                        StringEncoderBuilder::new_with_output(
                            Arc::new(batch.schema().field_with_name("json").unwrap().clone()),
                            pg_schema::PostgresType::Jsonb,
                        )
                        .unwrap(),
                    ),
                ),
                field_name => (field_name.to_string(), encoder),
            })
            .collect();
        let encoder = ArrowToPostgresBinaryEncoder::try_new_with_encoders(
            &batch.schema(),
            &encoders.into_iter().collect(),
        )
        .unwrap();
        let schema = encoder.schema();
        assert_eq!(
            schema.columns,
            vec![
                Column {
                    name: "int32".to_string(),
                    data_type: pg_schema::PostgresType::Int4,
                    nullable: false,
                },
                Column {
                    name: "int8".to_string(),
                    data_type: pg_schema::PostgresType::Int2,
                    nullable: false,
                },
                Column {
                    name: "string".to_string(),
                    data_type: pg_schema::PostgresType::Text,
                    nullable: false,
                },
                Column {
                    name: "json".to_string(),
                    data_type: pg_schema::PostgresType::Jsonb,
                    nullable: false,
                }
            ]
        )
    }

    /// A list of lists has no Postgres equivalent (arrays are flat), so `try_new` must reject it
    /// rather than build an encoder that produces nonsense.
    ///
    /// Neither generative suite reaches this: the proptest strategies and the fuzz target both
    /// draw list *elements* from the scalar types only, so this `Err` is covered here instead.
    #[test]
    fn nested_lists_are_rejected() {
        let inner = Arc::new(Field::new("item", DataType::Int32, true));
        for outer in [
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(inner.clone()),
                true,
            ))),
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::LargeList(inner.clone()),
                true,
            ))),
            DataType::LargeList(Arc::new(Field::new(
                "item",
                DataType::List(inner.clone()),
                true,
            ))),
            DataType::LargeList(Arc::new(Field::new(
                "item",
                DataType::LargeList(inner.clone()),
                true,
            ))),
        ] {
            let schema = Schema::new(vec![Field::new("nested", outer.clone(), true)]);
            let err = ArrowToPostgresBinaryEncoder::try_new(&schema)
                .err()
                .unwrap_or_else(|| panic!("{outer:?} was accepted"));
            assert!(
                matches!(&err, ErrorKind::TypeNotSupported { msg, .. } if msg == "nested lists are not supported"),
                "{outer:?}: {err:?}"
            );
        }
    }

    /// Every way of breaking the `header -> batches -> footer` contract has to come back as an
    /// `Err`; these used to be `assert!`s, i.e. a process abort inside a library.
    mod misuse {
        use super::*;

        fn encoder() -> ArrowToPostgresBinaryEncoder {
            ArrowToPostgresBinaryEncoder::try_new(&make_test_data().schema()).unwrap()
        }

        #[test]
        fn batch_before_header() {
            let mut encoder = encoder();
            let err = encoder
                .write_batch(&make_test_data(), &mut BytesMut::new())
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    ErrorKind::EncoderStateError {
                        expected: EncoderState::Encoding,
                        actual: EncoderState::Created,
                    }
                ),
                "{err:?}"
            );
        }

        #[test]
        fn footer_before_header() {
            let mut encoder = encoder();
            let err = encoder.write_footer(&mut BytesMut::new()).unwrap_err();
            assert!(
                matches!(
                    err,
                    ErrorKind::EncoderStateError {
                        expected: EncoderState::Encoding,
                        actual: EncoderState::Created,
                    }
                ),
                "{err:?}"
            );
        }

        #[test]
        fn double_header() {
            let mut buf = BytesMut::new();
            let mut encoder = encoder();
            encoder.write_header(&mut buf).unwrap();
            let err = encoder.write_header(&mut buf).unwrap_err();
            assert!(
                matches!(
                    err,
                    ErrorKind::EncoderStateError {
                        expected: EncoderState::Created,
                        actual: EncoderState::Encoding,
                    }
                ),
                "{err:?}"
            );
        }

        #[test]
        fn batch_after_footer() {
            let mut buf = BytesMut::new();
            let mut encoder = encoder();
            encoder.write_header(&mut buf).unwrap();
            encoder.write_footer(&mut buf).unwrap();
            let err = encoder
                .write_batch(&make_test_data(), &mut buf)
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    ErrorKind::EncoderStateError {
                        expected: EncoderState::Encoding,
                        actual: EncoderState::Finished,
                    }
                ),
                "{err:?}"
            );
        }

        #[test]
        fn double_footer() {
            let mut buf = BytesMut::new();
            let mut encoder = encoder();
            encoder.write_header(&mut buf).unwrap();
            encoder.write_footer(&mut buf).unwrap();
            let err = encoder.write_footer(&mut buf).unwrap_err();
            assert!(
                matches!(
                    err,
                    ErrorKind::EncoderStateError {
                        expected: EncoderState::Encoding,
                        actual: EncoderState::Finished,
                    }
                ),
                "{err:?}"
            );
        }

        #[test]
        fn wrong_column_count() {
            let batch = make_test_data();
            let mut buf = BytesMut::new();
            let mut encoder = encoder();
            encoder.write_header(&mut buf).unwrap();

            // A batch built from a prefix of the schema: right types, too few columns.
            let narrow_schema = Schema::new(vec![batch.schema().field(0).clone()]);
            let narrow =
                RecordBatch::try_new(Arc::new(narrow_schema), vec![batch.column(0).clone()])
                    .unwrap();

            let err = encoder.write_batch(&narrow, &mut buf).unwrap_err();
            assert!(
                matches!(
                    err,
                    ErrorKind::ColumnCountMismatch {
                        expected: 4,
                        actual: 1
                    }
                ),
                "{err:?}"
            );
        }
    }
}
