use std::collections::HashMap;

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

        let mut required_size: usize = 0;
        for encoder in &encoders {
            required_size += encoder.byte_size_hint()?
        }
        buf.reserve(required_size);

        for row in 0..n_rows {
            buf.put_i16(n_cols as i16);
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
