use std::collections::HashMap;

use encoders::EncoderBuilder;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3::Python;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use bytes::BytesMut;

mod encoders;
mod pg_schema;
mod utils;

#[pyclass(module = "pgpq._pgpq")]
#[derive(Debug)]
struct ArrowToPostgresBinaryEncoder {
    encoder: pgpq::ArrowToPostgresBinaryEncoder,
    buf: BytesMut,
    empty: Py<PyAny>,
}

const BUFF_SIZE: usize = 1024 * 1024;

#[pymethods]
impl ArrowToPostgresBinaryEncoder {
    #[new]
    fn new(py: Python, pyschema: &PyAny) -> PyResult<Self> {
        // TODO: error handling
        let encoder = pgpq::ArrowToPostgresBinaryEncoder::try_new(
            &ArrowSchema::from_pyarrow(pyschema).unwrap(),
        )
        .unwrap();
        Ok(Self {
            encoder,
            buf: BytesMut::with_capacity(BUFF_SIZE),
            empty: PyBytes::new(py, &vec![][..]).into(),
        })
    }
    #[staticmethod]
    fn infer_encoder(py: Python, py_field: &PyAny) -> PyResult<EncoderBuilder> {
        EncoderBuilder::try_new(py, py_field)
    }
    #[staticmethod]
    fn new_with_encoders(py: Python, py_schema: &PyAny, py_encoders: &PyDict) -> PyResult<Self> {
        // TODO: error handling
        let mut encoders: HashMap<String, pgpq::encoders::EncoderBuilder> = HashMap::new();
        for item in py_encoders.items() {
            let (name, py_builder): (String, crate::encoders::EncoderBuilder) = item.extract()?;
            let encoder: pgpq::encoders::EncoderBuilder = py_builder.into();
            encoders.insert(name, encoder);
        }
        let schema = &ArrowSchema::from_pyarrow(py_schema).unwrap();
        let encoder =
            pgpq::ArrowToPostgresBinaryEncoder::try_new_with_encoders(schema, &encoders).unwrap();
        Ok(Self {
            encoder,
            buf: BytesMut::with_capacity(BUFF_SIZE),
            empty: PyBytes::new(py, &vec![][..]).into(),
        })
    }
    fn write_header(&mut self, py: Python) -> Py<PyAny> {
        self.encoder.write_header(&mut self.buf);
        PyBytes::new(py, &self.buf.split()[..]).into()
    }
    fn write_batch(&mut self, py_batch: &PyAny) -> Py<PyAny> {
        let batch = &RecordBatch::from_pyarrow(py_batch).unwrap();
        self.encoder.write_batch(batch, &mut self.buf).unwrap();

        if self.buf.len() > BUFF_SIZE {
            Python::with_gil(|py| PyBytes::new(py, &self.buf.split()[..]).into())
        } else {
            self.empty.clone()
        }
    }
    fn finish(&mut self) -> &[u8] {
        self.encoder.write_footer(&mut self.buf).unwrap();
        &self.buf[..]
    }
    fn schema(&self) -> crate::pg_schema::PostgresSchema {
        self.encoder.schema().into()
    }
}

#[pymodule]
fn _pgpq(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ArrowToPostgresBinaryEncoder>()?;
    m.add_class::<crate::encoders::Int8EncoderBuilder>()?;
    m.add_class::<crate::encoders::ListEncoderBuilder>()?;
    m.add_class::<crate::pg_schema::Char>()?;
    m.add_class::<crate::pg_schema::Int2>()?;

    m.add_class::<crate::encoders::BooleanEncoderBuilder>()?;
    m.add_class::<crate::encoders::UInt8EncoderBuilder>()?;
    m.add_class::<crate::encoders::UInt16EncoderBuilder>()?;
    m.add_class::<crate::encoders::UInt32EncoderBuilder>()?;
    m.add_class::<crate::encoders::Int8EncoderBuilder>()?;
    m.add_class::<crate::encoders::Int16EncoderBuilder>()?;
    m.add_class::<crate::encoders::Int32EncoderBuilder>()?;
    m.add_class::<crate::encoders::Int64EncoderBuilder>()?;
    m.add_class::<crate::encoders::Float16EncoderBuilder>()?;
    m.add_class::<crate::encoders::Float32EncoderBuilder>()?;
    m.add_class::<crate::encoders::Float64EncoderBuilder>()?;
    m.add_class::<crate::encoders::TimestampMicrosecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::TimestampMillisecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::TimestampSecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::Date32EncoderBuilder>()?;
    m.add_class::<crate::encoders::Time32MillisecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::Time32SecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::Time64MicrosecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::DurationMicrosecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::DurationMillisecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::DurationSecondEncoderBuilder>()?;
    m.add_class::<crate::encoders::StringEncoderBuilder>()?;
    m.add_class::<crate::encoders::LargeStringEncoderBuilder>()?;
    m.add_class::<crate::encoders::BinaryEncoderBuilder>()?;
    m.add_class::<crate::encoders::LargeBinaryEncoderBuilder>()?;
    m.add_class::<crate::encoders::ListEncoderBuilder>()?;
    m.add_class::<crate::encoders::LargeListEncoderBuilder>()?;

    m.add_class::<crate::pg_schema::Bool>()?;
    m.add_class::<crate::pg_schema::Bytea>()?;
    m.add_class::<crate::pg_schema::Int8>()?;
    m.add_class::<crate::pg_schema::Int2>()?;
    m.add_class::<crate::pg_schema::Int4>()?;
    m.add_class::<crate::pg_schema::Char>()?;
    m.add_class::<crate::pg_schema::Text>()?;
    m.add_class::<crate::pg_schema::Jsonb>()?;
    m.add_class::<crate::pg_schema::Float4>()?;
    m.add_class::<crate::pg_schema::Float8>()?;
    m.add_class::<crate::pg_schema::Date>()?;
    m.add_class::<crate::pg_schema::Time>()?;
    m.add_class::<crate::pg_schema::Timestamp>()?;
    m.add_class::<crate::pg_schema::Interval>()?;
    m.add_class::<crate::pg_schema::List>()?;
    m.add_class::<crate::pg_schema::Column>()?;
    m.add_class::<crate::pg_schema::PostgresSchema>()?;
    Ok(())
}
