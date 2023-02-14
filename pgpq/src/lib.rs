mod error;

use crate::error::Error;
use arrow_array::RecordBatch;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema;
use bytes::{BufMut, BytesMut};

mod encoders;

use crate::encoders::{BuildEncoder, Encode, EncoderBuilder};

pub const HEADER_MAGIC_BYTES: &[u8] = b"PGCOPY\n\xff\r\n\0";

#[derive(Debug, PartialEq)]
enum EncoderState {
    Created,
    Encoding,
    Finished,
}

#[derive(Debug)]
pub struct ArrowToPostgresBinaryEncoder {
    fields: Vec<ArrowField>,
    state: EncoderState,
    encoder_builder: Vec<EncoderBuilder>,
}

impl ArrowToPostgresBinaryEncoder {
    /// Creates a new writer which will write rows of the provided types to the provided sink.
    pub fn try_new(schema: &Schema) -> Result<ArrowToPostgresBinaryEncoder, Error> {
        let fields = schema.fields.to_vec();

        let encoder_builders_result: Result<Vec<EncoderBuilder>, Error> = fields
            .iter()
            .map(|f| EncoderBuilder::try_new(f.data_type(), f.name()))
            .collect();

        Ok(ArrowToPostgresBinaryEncoder {
            fields: fields.to_vec(),
            state: EncoderState::Created,
            encoder_builder: encoder_builders_result?,
        })
    }

    pub fn write_header(&mut self, out: &mut BytesMut) {
        assert_eq!(self.state, EncoderState::Created);
        out.put(HEADER_MAGIC_BYTES);
        out.put_i32(0); // flags
        out.put_i32(0); // header extension
        self.state = EncoderState::Encoding;
    }

    pub fn write_batch(&mut self, batch: &RecordBatch, buf: &mut BytesMut) -> Result<(), Error> {
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
            .zip(&self.encoder_builder)
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

    pub fn write_footer(&mut self, out: &mut BytesMut) -> Result<(), Error> {
        assert_eq!(self.state, EncoderState::Encoding);
        out.put_i16(-1);
        self.state = EncoderState::Finished;
        Ok(())
    }
}
