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

#[derive(Debug, PartialEq)]
enum EncoderState {
    Created,
    Encoding,
    Finished,
}

#[derive(Debug)]
pub struct ArrowToPostgresBinaryEncoder {
    fields: Fields,
    state: EncoderState,
    encoder_builders: Vec<EncoderBuilder>,
}

pub fn build_encoders(
    fields: &arrow_schema::Fields,
) -> Vec<(String, Result<EncoderBuilder, ErrorKind>)> {
    fields
        .iter()
        .map(|f| (f.name().clone(), EncoderBuilder::try_new(f.clone())))
        .collect()
}

impl ArrowToPostgresBinaryEncoder {
    /// Creates a new writer which will write rows of the provided types to the provided sink.
    pub fn try_new(schema: &Schema) -> Result<Self, ErrorKind> {
        let fields = schema.fields();

        let maybe_encoder_builders: Result<Vec<EncoderBuilder>, ErrorKind> = build_encoders(fields)
            .into_iter()
            .map(|(_, maybe_encoder)| maybe_encoder)
            .collect();

        Ok(ArrowToPostgresBinaryEncoder {
            fields: fields.clone(),
            state: EncoderState::Created,
            encoder_builders: maybe_encoder_builders?,
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
                .zip(&self.fields)
                .map(|(builder, field)| (field.name().clone(), builder.schema()))
                .collect(),
        }
    }

    pub fn write_header(&mut self, out: &mut BytesMut) {
        assert_eq!(self.state, EncoderState::Created);
        out.put(HEADER_MAGIC_BYTES);
        out.put_i32(0); // flags
        out.put_i32(0); // header extension
        self.state = EncoderState::Encoding;
    }

    pub fn write_batch(
        &mut self,
        batch: &RecordBatch,
        buf: &mut BytesMut,
    ) -> Result<(), ErrorKind> {
        assert_eq!(self.state, EncoderState::Encoding);
        assert!(
            batch.num_columns() == self.fields.len(),
            "expected {} values but got {}",
            self.fields.len(),
            batch.num_columns(),
        );
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
            required_size += encoder.size_hint()?
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
        assert_eq!(self.state, EncoderState::Encoding);
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
    use arrow_array::{Int32Array, Int8Array, StringArray};
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
        let encoders = build_encoders(batch.schema().fields());
        let encoders: HashMap<String, EncoderBuilder> = encoders
            .into_iter()
            .map(|(field_name, maybe_enc)| match field_name.as_str() {
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
                field_name => (field_name.to_string(), maybe_enc.unwrap()),
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
                (
                    "int32".to_owned(),
                    Column {
                        data_type: pg_schema::PostgresType::Int4,
                        nullable: false,
                    }
                ),
                (
                    "int8".to_owned(),
                    Column {
                        data_type: pg_schema::PostgresType::Int2,
                        nullable: false,
                    },
                ),
                (
                    "string".to_owned(),
                    Column {
                        data_type: pg_schema::PostgresType::Text,
                        nullable: false,
                    },
                ),
                (
                    "json".to_owned(),
                    Column {
                        data_type: pg_schema::PostgresType::Jsonb,
                        nullable: false,
                    },
                ),
            ]
        )
    }
}
